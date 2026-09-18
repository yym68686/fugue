package releaseguardian

import (
	"context"
	"errors"
	"fugue/internal/declarativerelease"
	"testing"
	"time"
)

func interruptedCommitSnapshot(t *testing.T, now time.Time) Snapshot {
	s := testSnapshot(t, testHealth(HealthDegraded, HealthHealthy, HealthHealthy, now), otherDigest)
	s.Health.Local.Reason = "DaemonSet release identity differs from the stable record"
	s.Bundle.Prepared.ConfigSHA = s.Record.ConfigSHA
	s.Bundle.Release = declarativerelease.PlanRelease{ComponentID: s.Key.Component, Delivery: &declarativerelease.Delivery{Writer: "guardian", Group: s.Key.Group}, Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{GroupID: "group-a"}}}
	s.PreviousStatus = &ReleaseStatus{Component: s.Key.Component, Group: s.Key.Group, State: StateRecoveryRequired, CurrentRecordDigest: s.CurrentRecordDigest, TargetRecordDigest: s.Desired.RecordDigest, LastSuccessfulLKG: s.LastSuccessfulLKG, Reason: "desired rollout is fenced because the current component is degraded; local=DaemonSet release identity differs from the stable record"}
	return s
}

func TestCommittedRecoveryRequiresExactCandidateAndHealthyServingLayers(t *testing.T) {
	now := time.Now().UTC()
	if !CommittedMonitorRecoveryEligible(interruptedCommitSnapshot(t, now)) {
		t.Fatal("interrupted publication cannot be reconciled")
	}
	for name, change := range map[string]func(*Snapshot){
		"unmanaged":               func(s *Snapshot) { s.Managed = false },
		"missing candidate":       func(s *Snapshot) { s.DesiredRecordMissing = true },
		"different desired":       func(s *Snapshot) { s.Desired.RecordDigest = otherDigest },
		"different predecessor":   func(s *Snapshot) { s.CurrentRecordDigest = testDigest },
		"changed LKG":             func(s *Snapshot) { s.LastSuccessfulLKG = testDigest },
		"changed bundle":          func(s *Snapshot) { s.Bundle.Prepared.ConfigSHA = "other" },
		"wrong writer":            func(s *Snapshot) { s.Bundle.Release.Delivery.Writer = "direct" },
		"no transition":           func(s *Snapshot) { s.Bundle.Release.Transition = nil },
		"missing status":          func(s *Snapshot) { s.PreviousStatus = nil },
		"foreign status":          func(s *Snapshot) { s.PreviousStatus.Group = "foreign" },
		"different failed target": func(s *Snapshot) { s.PreviousStatus.TargetRecordDigest = otherDigest },
		"receipt exists":          func(s *Snapshot) { s.PreviousStatus.RolloutReceiptDigest = testDigest },
		"rollback exists":         func(s *Snapshot) { s.PreviousStatus.RollbackReceiptDigest = testDigest },
		"unrelated failure":       func(s *Snapshot) { s.PreviousStatus.Reason = "ordinary failure" },
		"route degraded":          func(s *Snapshot) { s.Health.Route.State = HealthDegraded },
		"dependency unknown":      func(s *Snapshot) { s.Health.Dependency.State = HealthUnknown },
		"wrong local reason":      func(s *Snapshot) { s.Health.Local.Reason = "crashloop" },
	} {
		t.Run(name, func(t *testing.T) {
			s := interruptedCommitSnapshot(t, now)
			change(&s)
			if CommittedMonitorRecoveryEligible(s) {
				t.Fatal("unproven recovery admitted")
			}
		})
	}
}

type committedRecoveryExecutor struct {
	fakeExecutor
	calls  int
	result ExecutionReceipt
	err    error
}

func (e *committedRecoveryExecutor) AdoptCommitted(context.Context, Snapshot) (ExecutionReceipt, error) {
	e.calls++
	return e.result, e.err
}

func TestInterruptedCommitRecoveryNeverRollsOrOverwritesFreshMetadata(t *testing.T) {
	for _, scenario := range []string{"verified", "read failure", "invalid receipt", "shadow"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now().UTC()
			s := interruptedCommitSnapshot(t, now)
			store := &fakeStore{snapshot: s}
			exec := &committedRecoveryExecutor{result: ExecutionReceipt{Status: "verified", RecordDigest: s.Record.RecordDigest, ReceiptDigest: testDigest}}
			mode := ModeWrite
			switch scenario {
			case "read failure":
				exec.err = errors.New("current authority unavailable")
			case "invalid receipt":
				exec.result.RecordDigest = otherDigest
			case "shadow":
				mode = ModeShadow
			}
			controller, err := NewController(mode, store, exec)
			if err != nil {
				t.Fatal(err)
			}
			defer controller.queue.ShutDown()
			controller.now = func() time.Time { return now }
			err = controller.Reconcile(context.Background(), s.Key)
			if exec.rollouts != 0 || exec.repairs != 0 || exec.rollbacks != 0 || store.lkgCAS != 0 {
				t.Fatal("metadata recovery mutated traffic")
			}
			if scenario == "shadow" {
				if exec.calls != 0 {
					t.Fatal("shadow attempted recovery")
				}
				return
			}
			if exec.calls != 1 || store.status.State != "" {
				t.Fatal("recovery overwrote metadata using stale snapshot")
			}
			if (err == nil) != (scenario == "verified") {
				t.Fatalf("wrong recovery result: %v", err)
			}
			if scenario == "verified" && controller.queue.Len() != 1 {
				t.Fatal("did not schedule fresh status read")
			}
		})
	}
}

func TestCommittedRecoveryResumesExactVerifiedMonitorBeforeDesiredCAS(t *testing.T) {
	now := time.Now().UTC()
	key := Key{Component: "edge-control-de", Group: "de"}
	data, _, artifact, _ := guardianStableFixture(t, key, now)
	canonical, _, err := canonicalStableReleaseRecord(key, data)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	for _, name := range executionFileNames {
		files[name] = []byte(data[name])
	}
	plan, _, prepared, _, _, _, err := decodeStableRecord(data)
	if err != nil {
		t.Fatal(err)
	}
	release, _ := releaseForComponent(plan, key.Component)
	bundle := ExecutionBundle{Prepared: prepared, Files: files, Release: release}
	bundle.Release.Delivery = &declarativerelease.Delivery{Writer: "guardian", Group: key.Group}
	bundle.Release.Transition = &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{GroupID: "group-a"}}
	record, err := NewReleaseRecord(key, bundle.Prepared.ConfigSHA, artifact.TopDigest, bundle.Prepared.Forward.ManifestDigest, otherDigest, canonical.HealthContractDigest)
	if err != nil {
		t.Fatal(err)
	}
	s := Snapshot{Key: key, Managed: true, Record: record, Bundle: bundle, CurrentMonitorData: data, CurrentRecordDigest: canonical.RecordDigest, LastSuccessfulLKG: canonical.RecordDigest, Desired: DesiredRelease{Component: key.Component, Group: key.Group, RecordDigest: record.RecordDigest}, Health: testHealth(HealthHealthy, HealthHealthy, HealthHealthy, now)}
	if !CommittedMonitorRecoveryEligible(s) {
		t.Fatal("exact persisted monitor cannot finish Desired CAS")
	}
	s.CurrentRecordDigest = record.RecordDigest
	s.LastSuccessfulLKG = record.RecordDigest
	if !CommittedMonitorRecoveryEligible(s) {
		t.Fatal("published candidate alias cannot finish Desired CAS")
	}
	s.Record = canonical
	s.Desired.RecordDigest = canonical.RecordDigest
	s.CurrentRecordDigest = canonical.RecordDigest
	s.LastSuccessfulLKG = canonical.RecordDigest
	if CommittedMonitorRecoveryEligible(s) {
		t.Fatal("settled canonical record attempted recovery again")
	}
	s.Record = record
	s.Desired.RecordDigest = record.RecordDigest
	s.CurrentRecordDigest = record.RecordDigest
	s.LastSuccessfulLKG = record.RecordDigest
	s.Bundle.Files["forward.json"] = []byte(`{}`)
	if CommittedMonitorRecoveryEligible(s) {
		t.Fatal("different execution accepted")
	}
}
