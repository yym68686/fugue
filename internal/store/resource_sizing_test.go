package store

import (
	"context"
	"errors"
	"testing"

	"fugue/internal/model"
)

func TestAutomaticResourceSizingRejectsGenericOperationAdmission(t *testing.T) {
	s := &Store{}
	for _, actor := range []string{model.OperationRequestedByRightSizing, model.OperationRequestedByRightSizingDownscale} {
		op := model.Operation{
			TenantID: "tenant", AppID: "app", Type: model.OperationTypeDeploy,
			RequestedByType: model.ActorTypeSystem, RequestedByID: actor,
		}
		if _, err := s.CreateOperation(op); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("generic deployment admitted actor %q: %v", actor, err)
		}
		for _, resize := range []string{model.OperationTypeAppCPUResize, model.OperationTypeAppMemoryResize} {
			op.Type = resize
			if _, err := s.CreateAppRequestResizeOperation(op, model.AppSpec{}); !errors.Is(err, ErrInvalidInput) {
				t.Errorf("generic request resize %q admitted actor %q: %v", resize, actor, err)
			}
		}
	}
}

func TestPGAutomaticResourceSizingUsesHistoricalScanLock(t *testing.T) {
	const fixture = "automatic-resource-sizing-lock"
	legacy := newStoreTestDatabase(t, fixture)
	current := newStoreTestDatabase(t, fixture)
	if got, err := legacy.WithAdvisoryLock(t.Context(), "background:right-sizing:auto-apply", func() error {
		acquired, err := current.RunAutomaticResourceSizing(func() error {
			t.Error("current writer entered while legacy held the lock")
			return nil
		})
		if acquired || err != nil {
			t.Errorf("contending current writer acquired=%v err=%v", acquired, err)
		}
		return nil
	}); !got || err != nil {
		t.Fatalf("legacy lock acquired=%v err=%v", got, err)
	}
	sentinel := errors.New("scan failed")
	if got, err := current.RunAutomaticResourceSizing(func() error {
		if got, err := legacy.WithAdvisoryLock(t.Context(), "different-job", nil); !got || err != nil {
			t.Errorf("unrelated job acquired=%v err=%v", got, err)
		}
		return sentinel
	}); !got || !errors.Is(err, sentinel) {
		t.Fatalf("current scan acquired=%v err=%v", got, err)
	}
	if got, err := legacy.WithAdvisoryLock(context.Background(), "background:right-sizing:auto-apply", nil); !got || err != nil {
		t.Fatalf("legacy could not reacquire after failed scan: %v %v", got, err)
	}
}
