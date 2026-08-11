package releaseguardian

import (
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
)

const (
	testSHA     = "1111111111111111111111111111111111111111"
	testDigest  = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	otherDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func testLayer(state HealthState, reason string, now time.Time) LayerHealth {
	return LayerHealth{State: state, Reason: reason, EvidenceDigest: testDigest, ObservedAt: now.UTC().Format(time.RFC3339Nano)}
}

func testHealth(local, dependency, route HealthState, now time.Time) HealthSnapshot {
	return HealthSnapshot{Local: testLayer(local, "local", now), Dependency: testLayer(dependency, "dependency", now), Route: testLayer(route, "route", now)}
}

func TestClassifySeparatesLocalDependencyAndRouteFailures(t *testing.T) {
	now := time.Unix(10, 0).UTC()
	tests := []struct {
		name     string
		health   HealthSnapshot
		state    ReleaseState
		rollback bool
	}{
		{"healthy", testHealth(HealthHealthy, HealthHealthy, HealthHealthy, now), StateStable, false},
		{"local", testHealth(HealthDegraded, HealthHealthy, HealthHealthy, now), StateRollbackPending, true},
		{"dependency", testHealth(HealthHealthy, HealthDegraded, HealthHealthy, now), StateDegraded, false},
		{"route", testHealth(HealthHealthy, HealthHealthy, HealthDegraded, now), StateDegraded, false},
		{"unknown", testHealth(HealthHealthy, HealthUnknown, HealthHealthy, now), StateRecoveryRequired, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := Classify(testDigest, testDigest, test.health)
			if decision.State != test.state || decision.RollbackEligible != test.rollback {
				t.Fatalf("decision=%+v", decision)
			}
		})
	}
	rollout := Classify(testDigest, otherDigest, testHealth(HealthHealthy, HealthHealthy, HealthHealthy, now))
	if rollout.State != StateRolloutPending || !rollout.RolloutEligible || rollout.RollbackEligible {
		t.Fatalf("rollout decision=%+v", rollout)
	}
	waiting := Classify(testDigest, otherDigest, testHealth(HealthHealthy, HealthHealthy, HealthUnknown, now))
	if waiting.State != StateRolloutPending || waiting.RolloutEligible || waiting.RollbackEligible ||
		!strings.Contains(waiting.Reason, "waiting for complete health evidence") {
		t.Fatalf("waiting decision=%+v", waiting)
	}
}

type fakeStore struct {
	snapshot Snapshot
	status   ReleaseStatus
	lkgCAS   int
}

func (store *fakeStore) Load(context.Context, Key) (Snapshot, error) { return store.snapshot, nil }
func (store *fakeStore) UpdateStatus(_ context.Context, _ Snapshot, status ReleaseStatus) error {
	store.status = status
	return nil
}
func (store *fakeStore) SetDesiredToLKG(context.Context, Snapshot) error {
	store.lkgCAS++
	return nil
}

type fakeExecutor struct {
	rollouts  int
	repairs   int
	rollbacks int
}

func (executor *fakeExecutor) Rollout(context.Context, Snapshot) (ExecutionReceipt, error) {
	executor.rollouts++
	return ExecutionReceipt{Status: "verified", Reason: "rollout verified", RecordDigest: otherDigest, ReceiptDigest: testDigest}, nil
}

func (executor *fakeExecutor) Repair(_ context.Context, snapshot Snapshot) (ExecutionReceipt, error) {
	executor.repairs++
	return ExecutionReceipt{Status: "verified", Reason: "stable forward repaired", RecordDigest: snapshot.Record.RecordDigest, ReceiptDigest: testDigest}, nil
}

func (executor *fakeExecutor) Rollback(context.Context, Snapshot) (ExecutionReceipt, error) {
	executor.rollbacks++
	return ExecutionReceipt{Status: "compensated", Reason: "LKG restored", RecordDigest: otherDigest, ReceiptDigest: testDigest}, nil
}

func testSnapshot(t *testing.T, health HealthSnapshot, current string) Snapshot {
	t.Helper()
	key := Key{Component: "edge-control", Group: "edge-pool-a"}
	record, err := NewReleaseRecord(key, testSHA, testDigest, testDigest, otherDigest, testDigest)
	if err != nil {
		t.Fatal(err)
	}
	return Snapshot{
		Key: key, Record: record,
		Desired:             DesiredRelease{APIVersion: APIVersion, Kind: DesiredReleaseKind, Component: key.Component, Group: key.Group, RecordDigest: record.RecordDigest, Generation: 1},
		CurrentRecordDigest: current, LastSuccessfulLKG: otherDigest, Health: health, StatusResourceVersion: "1",
		Bundle:                 ExecutionBundle{Prepared: declarativerelease.ExecutionPlan{Component: key.Component}},
		LKGMonitorRecordDigest: testDigest, Managed: true,
	}
}

func TestShadowNeverExecutesProductionMutation(t *testing.T) {
	now := time.Unix(20, 0).UTC()
	store := &fakeStore{snapshot: testSnapshot(t, testHealth(HealthDegraded, HealthHealthy, HealthDegraded, now), "")}
	executor := &fakeExecutor{}
	controller, err := NewController(ModeShadow, store, executor)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return now }
	if err := controller.Reconcile(context.Background(), store.snapshot.Key); err != nil {
		t.Fatal(err)
	}
	if executor.rollouts != 0 || executor.rollbacks != 0 {
		t.Fatalf("shadow mutations rollout=%d rollback=%d", executor.rollouts, executor.rollbacks)
	}
	if store.status.State != StateRolloutPending || !strings.HasPrefix(store.status.Reason, "shadow: ") {
		t.Fatalf("status=%+v", store.status)
	}
}

func TestWriteModeRollsBackOnlyLocalFailure(t *testing.T) {
	now := time.Unix(30, 0).UTC()
	snapshot := testSnapshot(t, testHealth(HealthDegraded, HealthHealthy, HealthHealthy, now), "")
	snapshot.CurrentRecordDigest = snapshot.Record.RecordDigest
	store := &fakeStore{snapshot: snapshot}
	executor := &fakeExecutor{}
	controller, err := NewController(ModeWrite, store, executor)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return now }
	if err := controller.Reconcile(context.Background(), snapshot.Key); err != nil {
		t.Fatal(err)
	}
	if executor.rollouts != 0 || executor.rollbacks != 1 || store.status.State != StateLKGStable || store.status.RollbackReceiptDigest != testDigest {
		t.Fatalf("executor=%+v status=%+v", executor, store.status)
	}
}

func TestWriteModeRepairsCurrentStableRecordInsteadOfWalkingItsOlderLKG(t *testing.T) {
	now := time.Unix(35, 0).UTC()
	snapshot := testSnapshot(t, testHealth(HealthDegraded, HealthHealthy, HealthHealthy, now), "")
	snapshot.Health.Local.Reason = "Deployment release identity differs from the stable record"
	snapshot.CurrentRecordDigest = snapshot.Record.RecordDigest
	snapshot.LastSuccessfulLKG = snapshot.Record.RecordDigest
	store := &fakeStore{snapshot: snapshot}
	executor := &fakeExecutor{}
	controller, err := NewController(ModeWrite, store, executor)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return now }
	if err := controller.Reconcile(context.Background(), snapshot.Key); err != nil {
		t.Fatal(err)
	}
	if executor.repairs != 1 || executor.rollbacks != 0 || store.lkgCAS != 0 || store.status.State != StateStable ||
		store.status.CurrentRecordDigest != snapshot.Record.RecordDigest || store.status.TargetRecordDigest != snapshot.Record.RecordDigest {
		t.Fatalf("executor=%+v store=%+v status=%+v", executor, store, store.status)
	}
}

func TestWriteModeRollsBackAStableRecordWithRuntimeFailure(t *testing.T) {
	now := time.Unix(36, 0).UTC()
	snapshot := testSnapshot(t, testHealth(HealthDegraded, HealthHealthy, HealthHealthy, now), "")
	snapshot.Health.Local.Reason = "workload container restarted or lacks immutable image identity"
	snapshot.CurrentRecordDigest = snapshot.Record.RecordDigest
	snapshot.LastSuccessfulLKG = snapshot.Record.RecordDigest
	store := &fakeStore{snapshot: snapshot}
	executor := &fakeExecutor{}
	controller, err := NewController(ModeWrite, store, executor)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return now }
	if err := controller.Reconcile(context.Background(), snapshot.Key); err != nil {
		t.Fatal(err)
	}
	if executor.repairs != 0 || executor.rollbacks != 1 || store.lkgCAS != 1 || store.status.State != StateLKGStable {
		t.Fatalf("executor=%+v store=%+v status=%+v", executor, store, store.status)
	}
}

func TestDependencyFailureCannotRollbackHealthyComponent(t *testing.T) {
	now := time.Unix(40, 0).UTC()
	snapshot := testSnapshot(t, testHealth(HealthHealthy, HealthDegraded, HealthHealthy, now), "")
	snapshot.CurrentRecordDigest = snapshot.Record.RecordDigest
	store := &fakeStore{snapshot: snapshot}
	executor := &fakeExecutor{}
	controller, err := NewController(ModeWrite, store, executor)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return now }
	if err := controller.Reconcile(context.Background(), snapshot.Key); err != nil {
		t.Fatal(err)
	}
	if executor.rollbacks != 0 || store.status.State != StateDegraded {
		t.Fatalf("executor=%+v status=%+v", executor, store.status)
	}
}
