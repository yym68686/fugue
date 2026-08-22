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
	for _, test := range []struct {
		name   string
		health HealthSnapshot
		state  ReleaseState
	}{
		{"local degraded", testHealth(HealthDegraded, HealthHealthy, HealthHealthy, now), StateRecoveryRequired},
		{"dependency degraded", testHealth(HealthHealthy, HealthDegraded, HealthHealthy, now), StateDegraded},
		{"route degraded", testHealth(HealthHealthy, HealthHealthy, HealthDegraded, now), StateDegraded},
	} {
		t.Run("pending "+test.name, func(t *testing.T) {
			decision := Classify(testDigest, otherDigest, test.health)
			if decision.State != test.state || decision.RolloutEligible || decision.RollbackEligible {
				t.Fatalf("decision=%+v", decision)
			}
		})
	}
	settledInactive := Classify(testDigest, testDigest, HealthSnapshot{
		Local:      testLayer(HealthDegraded, "health daemonset/edge-worker-a rollout is incomplete desired=1 updated=1 ready=0 available=0", now),
		Dependency: testLayer(HealthHealthy, "", now), Route: testLayer(HealthHealthy, "", now),
	})
	if settledInactive.State != StateStable {
		t.Fatalf("healthy serving LKG with inactive candidate was not settled: %+v", settledInactive)
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
	repair    *ExecutionReceipt
}

func (executor *fakeExecutor) Rollout(context.Context, Snapshot) (ExecutionReceipt, error) {
	executor.rollouts++
	return ExecutionReceipt{Status: "verified", Reason: "rollout verified", RecordDigest: otherDigest, ReceiptDigest: testDigest}, nil
}

func (executor *fakeExecutor) Repair(_ context.Context, snapshot Snapshot) (ExecutionReceipt, error) {
	executor.repairs++
	if executor.repair != nil {
		return *executor.repair, nil
	}
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
	if store.status.State != StateRecoveryRequired || !strings.HasPrefix(store.status.Reason, "shadow: ") {
		t.Fatalf("status=%+v", store.status)
	}
}

func TestWriteModeReconcilesUnprovenLKGBeforeRetryingCandidate(t *testing.T) {
	now := time.Unix(25, 0).UTC()
	snapshot := testSnapshot(t, testHealth(HealthHealthy, HealthHealthy, HealthHealthy, now), otherDigest)
	previous, err := (ReleaseStatus{
		Component: snapshot.Key.Component, Group: snapshot.Key.Group, State: StateRecoveryRequired,
		CurrentRecordDigest: otherDigest, TargetRecordDigest: snapshot.Record.RecordDigest,
		LastSuccessfulLKG: otherDigest, Health: snapshot.Health, Reason: "lkg-unproven: service probe failed",
		RolloutReceiptDigest: testDigest, ObservedAt: now.Format(time.RFC3339Nano),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.PreviousStatus = &previous
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
	if executor.rollouts != 0 || executor.rollbacks != 0 || store.lkgCAS != 1 || store.status.State != StateLKGStable ||
		store.status.CurrentRecordDigest != otherDigest || store.status.TargetRecordDigest != otherDigest ||
		store.status.RolloutReceiptDigest != testDigest {
		t.Fatalf("executor=%+v store=%+v status=%+v", executor, store, store.status)
	}
}

func TestWriteModeRollsOutExactDegradedPredecessorRepair(t *testing.T) {
	now := time.Unix(23, 0).UTC()
	snapshot := testSnapshot(t, testHealth(HealthHealthy, HealthHealthy, HealthDegraded, now), otherDigest)
	snapshot.Bundle.Prepared = declarativerelease.ExecutionPlan{
		Component: snapshot.Key.Component, ConfigSHA: snapshot.Record.ConfigSHA, DegradedPredecessor: true,
		Forward: declarativerelease.TargetIdentity{ConfigSHA: snapshot.Record.ConfigSHA},
	}
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
	if executor.rollouts != 1 || executor.rollbacks != 0 || store.lkgCAS != 0 ||
		store.status.State != StateVerifying || store.status.CurrentRecordDigest != snapshot.Record.RecordDigest {
		t.Fatalf("degraded predecessor repair was not rolled out: executor=%+v store=%+v status=%+v", executor, store, store.status)
	}
}

func TestDegradedPredecessorRolloutRequiresExactRecordBindings(t *testing.T) {
	now := time.Unix(23, 0).UTC()
	exact := testSnapshot(t, testHealth(HealthHealthy, HealthHealthy, HealthDegraded, now), otherDigest)
	exact.Bundle.Prepared = declarativerelease.ExecutionPlan{
		Component: exact.Key.Component, ConfigSHA: exact.Record.ConfigSHA, DegradedPredecessor: true,
		Forward: declarativerelease.TargetIdentity{ConfigSHA: exact.Record.ConfigSHA},
	}
	if !degradedPredecessorRolloutEligible(exact) {
		t.Fatal("exact degraded predecessor repair was rejected")
	}
	for name, mutate := range map[string]func(*Snapshot){
		"component": func(snapshot *Snapshot) { snapshot.Bundle.Prepared.Component = "other" },
		"config":    func(snapshot *Snapshot) { snapshot.Bundle.Prepared.ConfigSHA = strings.Repeat("2", 40) },
		"forward":   func(snapshot *Snapshot) { snapshot.Bundle.Prepared.Forward.ConfigSHA = strings.Repeat("2", 40) },
		"desired":   func(snapshot *Snapshot) { snapshot.Desired.RecordDigest = testDigest },
		"current":   func(snapshot *Snapshot) { snapshot.CurrentRecordDigest = testDigest },
		"lkg":       func(snapshot *Snapshot) { snapshot.LastSuccessfulLKG = testDigest },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := exact
			mutate(&candidate)
			if degradedPredecessorRolloutEligible(candidate) {
				t.Fatal("identity-drifted degraded predecessor repair was accepted")
			}
		})
	}
}

func TestWriteModeKeepsVerifiedRolloutPendingUntilTargetCanaryArrives(t *testing.T) {
	now := time.Unix(24, 0).UTC()
	snapshot := testSnapshot(t, testHealth(HealthHealthy, HealthHealthy, HealthUnknown, now), "")
	snapshot.CurrentRecordDigest = snapshot.Record.RecordDigest
	previous, err := (ReleaseStatus{
		Component: snapshot.Key.Component, Group: snapshot.Key.Group, State: StateVerifying,
		CurrentRecordDigest: snapshot.Record.RecordDigest, TargetRecordDigest: snapshot.Record.RecordDigest,
		LastSuccessfulLKG: snapshot.LastSuccessfulLKG, Health: snapshot.Health, Reason: "rollout verified",
		RolloutReceiptDigest: testDigest, ObservedAt: now.Format(time.RFC3339Nano),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.PreviousStatus = &previous
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
	if executor.rollouts != 0 || executor.repairs != 0 || executor.rollbacks != 0 || store.status.State != StateVerifying ||
		store.status.RolloutReceiptDigest != testDigest || !strings.Contains(store.status.Reason, "target-bound route evidence") {
		t.Fatalf("executor=%+v status=%+v", executor, store.status)
	}

	snapshot.PreviousStatus = &store.status
	snapshot.Health.Route = testLayer(HealthHealthy, "", now)
	store.snapshot = snapshot
	if err := controller.Reconcile(context.Background(), snapshot.Key); err != nil {
		t.Fatal(err)
	}
	if store.status.State != StateStable || executor.rollouts != 0 || executor.rollbacks != 0 {
		t.Fatalf("healthy target did not converge: executor=%+v status=%+v", executor, store.status)
	}
}

func TestWriteModeDoesNotTreatUnrelatedUnknownRouteAsVerification(t *testing.T) {
	now := time.Unix(24, 0).UTC()
	snapshot := testSnapshot(t, testHealth(HealthHealthy, HealthHealthy, HealthUnknown, now), "")
	snapshot.CurrentRecordDigest = snapshot.Record.RecordDigest
	store := &fakeStore{snapshot: snapshot}
	executor := &fakeExecutor{}
	controller, _ := NewController(ModeWrite, store, executor)
	controller.now = func() time.Time { return now }
	if err := controller.Reconcile(context.Background(), snapshot.Key); err != nil {
		t.Fatal(err)
	}
	if store.status.State != StateRecoveryRequired {
		t.Fatalf("unbound unknown route was accepted: %+v", store.status)
	}
}

func TestWriteModeFencesUnprovenLKGUntilFreshHealth(t *testing.T) {
	now := time.Unix(26, 0).UTC()
	snapshot := testSnapshot(t, testHealth(HealthDegraded, HealthHealthy, HealthHealthy, now), otherDigest)
	previous, err := (ReleaseStatus{
		Component: snapshot.Key.Component, Group: snapshot.Key.Group, State: StateRecoveryRequired,
		CurrentRecordDigest: otherDigest, TargetRecordDigest: snapshot.Record.RecordDigest,
		LastSuccessfulLKG: otherDigest, Health: snapshot.Health, Reason: "lkg-unproven",
		RolloutReceiptDigest: testDigest, ObservedAt: now.Format(time.RFC3339Nano),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.PreviousStatus = &previous
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
	if executor.rollouts != 0 || executor.rollbacks != 0 || store.lkgCAS != 0 || store.status.State != StateRecoveryRequired ||
		!strings.HasPrefix(store.status.Reason, "lkg-unproven: ") || !strings.Contains(store.status.Reason, "fenced") {
		t.Fatalf("executor=%+v store=%+v status=%+v", executor, store, store.status)
	}
	snapshot.PreviousStatus = &store.status
	store.snapshot = snapshot
	if err := controller.Reconcile(context.Background(), snapshot.Key); err != nil {
		t.Fatal(err)
	}
	if executor.rollouts != 0 || executor.rollbacks != 0 || store.lkgCAS != 0 || store.status.State != StateRecoveryRequired {
		t.Fatalf("second reconcile retried the fenced candidate: executor=%+v store=%+v status=%+v", executor, store, store.status)
	}
}

func TestWriteModeRestoresFencedLKGWhenCandidateRolloutIsIncomplete(t *testing.T) {
	now := time.Unix(28, 0).UTC()
	snapshot := testSnapshot(t, testHealth(HealthDegraded, HealthHealthy, HealthHealthy, now), otherDigest)
	snapshot.Health.Local.Reason = "health daemonset/edge-worker-a rollout is incomplete desired=1 updated=1 ready=0 available=0 generation=7 observed=7"
	previous, err := (ReleaseStatus{
		Component: snapshot.Key.Component, Group: snapshot.Key.Group, State: StateRecoveryRequired,
		CurrentRecordDigest: otherDigest, TargetRecordDigest: snapshot.Record.RecordDigest,
		LastSuccessfulLKG: otherDigest, Health: snapshot.Health, Reason: "rollout result is unknown: stale execution plan",
		ObservedAt: now.Format(time.RFC3339Nano),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.PreviousStatus = &previous
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
	if executor.rollbacks != 1 || store.lkgCAS != 1 || store.status.State != StateLKGStable ||
		store.status.TargetRecordDigest != snapshot.CurrentRecordDigest {
		t.Fatalf("executor=%+v store=%+v status=%+v", executor, store, store.status)
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

func TestWriteModeRecordsAnExactButDegradedLKGAsCurrentRecoveryState(t *testing.T) {
	now := time.Unix(37, 0).UTC()
	snapshot := testSnapshot(t, testHealth(HealthDegraded, HealthHealthy, HealthHealthy, now), "")
	snapshot.Health.Local.Reason = "Deployment release identity differs from the stable record"
	snapshot.CurrentRecordDigest = snapshot.Record.RecordDigest
	snapshot.LastSuccessfulLKG = snapshot.Record.RecordDigest
	store := &fakeStore{snapshot: snapshot}
	executor := &fakeExecutor{repair: &ExecutionReceipt{
		Status: "recovery-required", Reason: "continuous-repair-lkg-unproven",
		RecordDigest: snapshot.Record.LKGRecordDigest, ReceiptDigest: testDigest,
	}}
	controller, err := NewController(ModeWrite, store, executor)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return now }
	if err := controller.Reconcile(context.Background(), snapshot.Key); err != nil {
		t.Fatal(err)
	}
	if executor.repairs != 1 || store.lkgCAS != 1 || store.status.State != StateRecoveryRequired ||
		store.status.CurrentRecordDigest != snapshot.Record.LKGRecordDigest || store.status.TargetRecordDigest != snapshot.Record.LKGRecordDigest {
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
