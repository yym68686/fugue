package appmail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

type reelStore struct {
	bindings []model.AppMailBinding
	apps     map[string]model.App
	failRead map[string]error
	failSave map[string]error
	saves    []model.AppMailBinding
}

func (s *reelStore) ListAppMailBindings(after string, limit int) ([]model.AppMailBinding, error) {
	var page []model.AppMailBinding
	for _, binding := range s.bindings {
		if binding.AppID > after && len(page) < limit {
			page = append(page, binding)
		}
	}
	return page, nil
}

func (s *reelStore) GetAppMetadata(id string) (model.App, error) {
	if err := s.failRead[id]; err != nil {
		return model.App{}, err
	}
	if app, ok := s.apps[id]; ok {
		return app, nil
	}
	return model.App{}, store.ErrNotFound
}

func (s *reelStore) SaveAppMailBinding(binding model.AppMailBinding, expected uint64) (model.AppMailBinding, error) {
	if expected != binding.Generation {
		return model.AppMailBinding{}, store.ErrConflict
	}
	if err := s.failSave[binding.AppID]; err != nil {
		return model.AppMailBinding{}, err
	}
	binding.Generation++
	s.saves = append(s.saves, binding)
	return binding, nil
}

type syncBindingFunc func(context.Context, model.AppMailBinding) (json.RawMessage, error)

func (fn syncBindingFunc) Sync(ctx context.Context, b model.AppMailBinding) (json.RawMessage, error) {
	return fn(ctx, b)
}

func testBinding(id string) model.AppMailBinding {
	return model.AppMailBinding{AppID: id, TenantID: "tenant", ProjectID: "project", Hostname: id + ".apps.example.test", Generation: 9, State: "active"}
}

func testApp(b model.AppMailBinding) model.App {
	return model.App{ID: b.AppID, TenantID: b.TenantID, ProjectID: b.ProjectID, Route: &model.AppRoute{Hostname: b.Hostname}}
}

func TestPassRevokesOnlyInvalidOwnersAndDoesNotStarveLaterBindings(t *testing.T) {
	st := &reelStore{apps: map[string]model.App{}, failRead: map[string]error{"a": errors.New("temporary metadata failure")}, failSave: map[string]error{"b": store.ErrConflict}}
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		st.bindings = append(st.bindings, testBinding(id))
	}
	st.apps["c"] = testApp(st.bindings[2])
	st.apps["d"] = testApp(st.bindings[3])
	foreign := testApp(st.bindings[4])
	foreign.ProjectID = "transferred"
	st.apps["e"] = foreign
	st.bindings[5].State = "revoked"
	var got []string
	provider := syncBindingFunc(func(_ context.Context, b model.AppMailBinding) (json.RawMessage, error) {
		got = append(got, fmt.Sprintf("%s %s %d", b.AppID, b.State, b.Generation))
		if b.AppID == "c" {
			return nil, errors.New("provider temporarily unavailable")
		}
		return nil, nil
	})
	r := Reconciler{Store: st, Provider: provider, BaseDomain: "apps.example.test", Log: log.New(io.Discard, "", 0)}
	after, err := r.Pass(context.Background(), "")
	if err != nil || after != "" {
		t.Fatalf("completed sweep: after=%q err=%v", after, err)
	}
	if want := []string{"c active 9", "d active 9", "e revoked 10", "f revoked 9"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("provider received %v, want %v", got, want)
	}
	if len(st.saves) != 1 || st.saves[0].AppID != "e" {
		t.Fatalf("unexpected revocations: %+v", st.saves)
	}
}

func TestPassReturnsResumeCursorWithoutStartingNextBindingAfterCancellation(t *testing.T) {
	st := &reelStore{apps: map[string]model.App{}}
	for _, id := range []string{"a", "b", "c"} {
		b := testBinding(id)
		st.bindings = append(st.bindings, b)
		st.apps[id] = testApp(b)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var got []string
	provider := syncBindingFunc(func(_ context.Context, b model.AppMailBinding) (json.RawMessage, error) {
		got = append(got, b.AppID)
		cancel()
		return nil, nil
	})
	r := Reconciler{Store: st, Provider: provider, BaseDomain: "apps.example.test", Log: log.New(io.Discard, "", 0)}
	after, err := r.Pass(ctx, "a")
	if !errors.Is(err, context.Canceled) || after != "b" || !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("canceled pass: after=%q err=%v calls=%v", after, err, got)
	}
	after, err = r.Pass(context.Background(), after)
	if err != nil || after != "" || !reflect.DeepEqual(got, []string{"b", "c"}) {
		t.Fatalf("resumed pass: after=%q err=%v calls=%v", after, err, got)
	}
}
