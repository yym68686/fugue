package edgegroupfront

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const (
	testActivationGroup  = "edge-group-country-us"
	testActivationCommit = "0123456789abcdef0123456789abcdef01234567"
	testActivationDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

func TestActivationCASInitializesPromotesAndRollsBackOneGroup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activation.json")
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	initial, err := ApplyActivationCAS(path, activationRequest(ActivationOperationInit, 0, "a", "a", "bundle-a", 0), now)
	if err != nil {
		t.Fatal(err)
	}
	if initial.PreviousExists || initial.Current.Generation != 1 || initial.Current.ActiveSlot != "a" || initial.Current.GroupID != testActivationGroup {
		t.Fatalf("unexpected initial activation: %+v", initial)
	}
	promoted, err := ApplyActivationCAS(path, activationRequest(ActivationOperationPromote, 1, "a", "b", "bundle-b", 0), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !promoted.PreviousExists || promoted.Previous == nil || promoted.Previous.Generation != 1 || promoted.Current.Generation != 2 || promoted.Current.ActiveSlot != "b" {
		t.Fatalf("unexpected promotion receipt: %+v", promoted)
	}
	rolledBack, err := ApplyActivationCAS(path, activationRequest(ActivationOperationRollback, 2, "b", "a", "bundle-a", 2), now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.Current.Generation != 3 || rolledBack.Current.ActiveSlot != "a" || rolledBack.Current.RollbackOfGeneration != 2 {
		t.Fatalf("unexpected rollback receipt: %+v", rolledBack)
	}
	state, exists, err := ReadActivationState(path)
	if err != nil || !exists || state != rolledBack.Current {
		t.Fatalf("persisted state=%+v exists=%t err=%v, want %+v", state, exists, err, rolledBack.Current)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("activation state mode=%v err=%v", info.Mode().Perm(), err)
	}
}

func TestActivationCASAllowsExactlyOneConcurrentGroupTransition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activation.json")
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	if _, err := ApplyActivationCAS(path, activationRequest(ActivationOperationInit, 0, "a", "a", "bundle-a", 0), now); err != nil {
		t.Fatal(err)
	}
	request := activationRequest(ActivationOperationPromote, 1, "a", "b", "bundle-b", 0)
	var wait sync.WaitGroup
	results := make(chan error, 2)
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := ApplyActivationCAS(path, request, now.Add(time.Minute))
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	succeeded, conflicted := 0, 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrActivationCASConflict):
			conflicted++
		default:
			t.Fatalf("unexpected concurrent CAS error: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("concurrent CAS succeeded=%d conflicted=%d", succeeded, conflicted)
	}
}

func TestReadActiveSlotRejectsAnotherGroupAndPreservesLastGoodActivation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activation.json")
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	if _, err := ApplyActivationCAS(path, activationRequest(ActivationOperationInit, 0, "b", "b", "bundle-b", 0), now); err != nil {
		t.Fatal(err)
	}
	if slot, state, err := readActiveSlot(path, testActivationGroup); err != nil || slot != "b" || state == nil || state.Generation != 1 {
		t.Fatalf("read activation slot=%q state=%+v err=%v", slot, state, err)
	}
	if _, _, err := readActiveSlot(path, "edge-group-country-de"); err == nil {
		t.Fatal("front accepted another group's activation state")
	}
	service := NewService(Config{}, nil)
	cfg := Config{EdgeGroupID: testActivationGroup, ActiveSlotFile: path, DefaultSlot: "a", RequireActivationState: true,
		Slots: map[string]SlotTargets{"a": {}, "b": {}}}
	if slot := service.activeSlot(cfg); slot != "b" {
		t.Fatalf("front did not load activation state, slot=%q", slot)
	}
	if err := os.WriteFile(path, []byte("a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if slot := service.activeSlot(cfg); slot != "b" {
		t.Fatalf("plain slot file bypassed required activation CAS, slot=%q", slot)
	}
}

func TestActivationHealthBindsFrontToExactWorkerAndBundle(t *testing.T) {
	service := NewService(Config{}, nil)
	service.lastActivation = &ActivationState{
		Schema: ActivationStateSchemaV1, GroupID: testActivationGroup, Generation: 7, ActiveSlot: "b", PreviousSlot: "a",
		BundleGeneration: "bundle-b", WorkerSourceCommit: testActivationCommit, WorkerImageDigest: testActivationDigest,
		Authority: ActivationAuthority, Operation: ActivationOperationPromote, Reason: "promote tested group slot", UpdatedAt: time.Now().UTC(),
	}
	recorder := httptest.NewRecorder()
	service.writeActivationHealth(recorder, "b", "127.0.0.1:28443")
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["active_slot"] != "b" || payload["activation_generation"] != float64(7) || payload["bundle_generation"] != "bundle-b" ||
		payload["worker_source_commit"] != testActivationCommit || payload["worker_image_digest"] != testActivationDigest || payload["route_authority"] != ActivationAuthority {
		t.Fatalf("front health is not bound to activation state: %v", payload)
	}
}

func TestActiveSlotWithActivationRefreshesMetadataAfterCASSwitch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activation.json")
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	if _, err := ApplyActivationCAS(path, activationRequest(ActivationOperationInit, 0, "a", "a", "bundle-a", 0), now); err != nil {
		t.Fatal(err)
	}
	service := NewService(Config{}, nil)
	service.lastActivation = &ActivationState{ActiveSlot: "a", Generation: 1, GroupID: testActivationGroup}
	cfg := Config{EdgeGroupID: testActivationGroup, ActiveSlotFile: path, DefaultSlot: "a", RequireActivationState: true,
		Slots: map[string]SlotTargets{"a": {}, "b": {}}}
	if _, err := ApplyActivationCAS(path, activationRequest(ActivationOperationPromote, 1, "a", "b", "bundle-b", 0), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	slot, activation := service.activeSlotWithActivation(cfg)
	if slot != "b" || activation == nil || activation.ActiveSlot != "b" || activation.Generation != 2 || activation.BundleGeneration != "bundle-b" {
		t.Fatalf("active slot evidence was not refreshed atomically: slot=%q activation=%+v", slot, activation)
	}
}

func TestFrontFailsClosedBeforeBindingPortsWithoutRequiredActivationState(t *testing.T) {
	service := NewService(Config{
		HTTPListenAddr: "127.0.0.1:0", HTTPMode: HTTPModeRedirect,
		EdgeGroupID: testActivationGroup, ActiveSlotFile: filepath.Join(t.TempDir(), "missing.json"), DefaultSlot: "a",
		RequireActivationState: true,
		Slots:                  map[string]SlotTargets{"a": {}, "b": {}},
	}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := service.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("front unexpectedly started without its required group activation state")
	}
}

func activationRequest(operation string, expectedGeneration uint64, expectedSlot, targetSlot, bundle string, rollbackOf uint64) ActivationCASRequest {
	return ActivationCASRequest{
		GroupID: testActivationGroup, ExpectedGeneration: expectedGeneration, ExpectedSlot: expectedSlot, TargetSlot: targetSlot,
		BundleGeneration: bundle, WorkerSourceCommit: testActivationCommit, WorkerImageDigest: testActivationDigest,
		Operation: operation, RollbackOfGeneration: rollbackOf, Reason: "edge group activation test",
	}
}
