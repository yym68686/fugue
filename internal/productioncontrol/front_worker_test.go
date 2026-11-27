package productioncontrol

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
)

func frontWorkerFixture(t *testing.T, now time.Time, node string, localGeneration uint64) FrontWorkerShadow {
	t.Helper()
	const group, namespace = "edge-group-test", "fixture-system"
	uid := func(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", i) }
	j, err := (edgegroupfront.NodeWorkerJournal{
		Schema:    edgegroupfront.NodeWorkerJournalSchema,
		Recipient: edgegroupfront.NativeRecipient{Group: group, Node: node, PodUID: uid(1), Incarnation: digest([]byte(node))},
		NodeUID:   uid(2), ServiceUID: uid(3), PredecessorWorkerUID: uid(4), CandidateWorkerUID: uid(5),
		PredecessorProcess: digest([]byte("retained")), CandidateProcess: digest([]byte("candidate")),
		CandidateComponent: "test-candidate", ReleaseDigest: digest([]byte("test release")),
		Expected: edgegroupfront.ActivationState{Schema: edgegroupfront.ActivationStateSchemaV1,
			GroupID: group, Generation: localGeneration, ActiveSlot: "b", BundleGeneration: "test-old.p1.r1",
			WorkerSourceCommit: strings.Repeat("a", 40), WorkerImageDigest: digest([]byte("old")),
			Authority: edgegroupfront.ActivationAuthority, Operation: edgegroupfront.ActivationOperationInit,
			Reason: "test predecessor", UpdatedAt: now.Add(-time.Minute).UTC()},
		Candidate: edgegroupfront.ActivationRepairRelease{Slot: "a", BundleGeneration: "test-new.p2.r1",
			WorkerSourceCommit: strings.Repeat("b", 40), WorkerImageDigest: digest([]byte("new"))},
		CreatedAt: now.Add(-time.Second).UTC(),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	selection, err := j.SelectionPlan()
	if err != nil {
		t.Fatal(err)
	}
	return FrontWorkerShadow{Schema: FrontWorkerShadowSchema, Namespace: namespace, GroupFence: FrontWorkerGroupFenceName(group),
		Component: "test-worker", LeaseName: "test-worker-lease", Journal: j, Selection: selection, ObservedAt: now.UTC(),
		Rollback: FrontWorkerRollbackObservation{RetainedWorkerUID: j.PredecessorWorkerUID, RetainedTarget: "192.0.2.7:28443",
			CanaryHost: "example.test", CanaryPath: "/ready", CanaryBodyDigest: digest([]byte("test canary"))}}
}

func TestFrontWorkerApprovalBindsPlanAndReobservedPreconditions(t *testing.T) {
	now := time.Unix(1780000000, 0)
	shadow := frontWorkerFixture(t, now, "fixture-node-a", 42)
	head := Version{7, digest([]byte("group head"))}
	s, err := DraftFrontWorker("cluster-a", "handoff-a", head, shadow)
	if err != nil {
		t.Fatal(err)
	}
	if s.Intent.ResourceID != "front-worker/fixture-system/edge-group-test" || s.Intent.Generation != 7 ||
		s.Intent.TargetDigest != s.Intent.PayloadDigest || s.Intent.RollbackDigest != head.Digest {
		t.Fatalf("invalid group binding: %+v", s.Intent)
	}
	if p, err := InspectFrontWorker(s); err != nil || p != shadow {
		t.Fatalf("cannot review approved shadow: %+v %v", p, err)
	}
	fresh := shadow
	fresh.ObservedAt = now.Add(time.Hour).UTC()
	if err := MatchFreshFrontWorkerShadow(shadow, fresh); err != nil {
		t.Fatalf("fresh observation could not authenticate older, approved inputs: %v", err)
	}
	for name, mutate := range map[string]func(*FrontWorkerShadow){
		"wrong listener": func(p *FrontWorkerShadow) { p.Rollback.RetainedTarget = "192.0.2.8:28443" },
		"wrong canary":   func(p *FrontWorkerShadow) { p.Rollback.CanaryPath = "/other" },
		"other lease":    func(p *FrontWorkerShadow) { p.LeaseName = "other-lease" },
		"receipt":        func(p *FrontWorkerShadow) { p.Journal.ReleaseDigest = digest([]byte("other")) },
		"retirement":     func(p *FrontWorkerShadow) { p.Retirement = &FrontWorkerRetirement{DaemonSet: "new-effect"} },
		"earlier":        func(p *FrontWorkerShadow) { p.ObservedAt = now.Add(-time.Second).UTC() },
	} {
		t.Run(name, func(t *testing.T) {
			other := fresh
			mutate(&other)
			if err := MatchFreshFrontWorkerShadow(shadow, other); err == nil {
				t.Fatal("different live shadow escaped the approval")
			}
		})
	}
	shadow.Selection.RollbackDigest = shadow.Selection.AdvanceDigest
	if _, err := DraftFrontWorker("cluster-a", "handoff-a", head, shadow); err == nil {
		t.Fatal("approved a false rollback projection")
	}
	aliased := s
	aliased.Intent.ResourceID += "/fixture-node-a"
	if _, err := InspectFrontWorker(aliased); err == nil {
		t.Fatal("node-specific resource alias escaped the group fence")
	}
	aliased = s
	aliased.Intent.Operation = Maintenance
	if err := aliased.Intent.Validate(); err == nil {
		t.Fatal("another operation could claim the Front/Worker resource")
	}
}

func testPostgresFrontWorkerApproval(t *testing.T, ctx context.Context, first, second *Journal) {
	t.Helper()
	_, _, private, trusted, now := approvalFixture(t)
	left, err := NewJournal(first.db, trusted)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewJournal(second.db, trusted)
	if err != nil {
		t.Fatal(err)
	}
	left.now = func() time.Time { return now }
	right.now = left.now
	snapshot := []byte(`{"group":"edge-group-fixture","serving":["fixture-node-a","fixture-node-b"]}`)
	head, err := FrontWorkerBaselineVersion(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	stage := func(node string, local uint64) (Submission, string, string) {
		t.Helper()
		shadow := frontWorkerFixture(t, now, node, local)
		s, err := DraftFrontWorker("cluster-a", node, head, shadow)
		if err != nil {
			t.Fatal(err)
		}
		r, err := left.RecordIntent(ctx, "operator-a", s.Intent, s.Payload)
		if err != nil {
			t.Fatal(err)
		}
		a, err := SignApproval(s.Intent, "owner-a", private, now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		signed, err := left.RecordApproval(ctx, a)
		if err != nil {
			t.Fatal(err)
		}
		return s, r.Digest, signed.Digest
	}
	s, intent, approval := stage("fixture-node-a", 42)
	_, other, otherApproval := stage("fixture-node-b", 15)
	if _, err := right.ReadApprovedFrontWorker(ctx, s.Intent.ClusterID, intent, approval); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unanchored group returned approval: %v", err)
	}
	pub, observer, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	source := memoryWitnesses{}
	scope := s.Intent.scope()
	trust := map[string]TrustedWitness{"observer": {scope, pub}}
	if _, err := NewWitnessedExecutionJournal(left, source, trust); err == nil {
		t.Fatal("Front/Worker group accepted a signature-only evidence verifier")
	}
	liveAuthority := digest([]byte("live boundary"))
	seen := ExecutionWitness{Kind: BaselineWitness, KeyID: "observer", ClusterID: scope.ClusterID, ResourceID: scope.ResourceID,
		Generation: head.Generation, VerifiedDigest: head.Digest, ObservationDigest: digest([]byte("live group")),
		AuthorityDigest: liveAuthority, ObservedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix()}
	w, err := NewObservedExecutionJournal(left, source, trust, func(_ context.Context, signed ExecutionWitness) error {
		if signed.AuthorityDigest != liveAuthority {
			return errors.New("provider boundary changed")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	wrong := seen
	wrong.AuthorityDigest = digest([]byte("stale boundary"))
	baseline := Baseline{scope, head, fixtureWitness(t, observer, wrong, source)}
	if created, err := w.AnchorFrontWorkerBaseline(ctx, "observer-a", baseline, func() []byte { return snapshot }); err == nil || created {
		t.Fatal("Front/Worker group accepted a signed baseline with stale provider authority")
	}
	baseline.EvidenceDigest = fixtureWitness(t, observer, seen, source)
	if created, err := w.AnchorResource(ctx, "observer-a", baseline); err == nil || created {
		t.Fatal("Front/Worker group accepted a digest without a saved baseline snapshot")
	}
	if created, err := w.AnchorFrontWorkerBaseline(ctx, "observer-a", baseline, func() []byte { return []byte(`{}`) }); err == nil || created {
		t.Fatal("Front/Worker group accepted a snapshot with a different verified digest")
	}
	if created, err := w.AnchorFrontWorkerBaseline(ctx, "observer-a", baseline, func() []byte { return snapshot }); err != nil || !created {
		t.Fatalf("cannot seed test group baseline: %t %v", created, err)
	}
	if got, err := right.ReadFrontWorkerBaselineSnapshot(ctx, baseline.Scope); err != nil || string(got) != string(snapshot) {
		t.Fatalf("baseline snapshot did not survive reopen: %s %v", got, err)
	}
	if p, err := right.ReadApprovedFrontWorker(ctx, s.Intent.ClusterID, intent, approval); err != nil || p.Journal.Expected.Generation != 42 {
		t.Fatalf("stored approval after reopen: %+v %v", p, err)
	}
	if _, err := right.ReadApprovedFrontWorker(ctx, s.Intent.ClusterID, intent, otherApproval); err == nil {
		t.Fatal("accepted another node's approval")
	}
	right.now = func() time.Time { return now.Add(time.Minute) }
	if _, err := right.ReadApprovedFrontWorker(ctx, s.Intent.ClusterID, intent, approval); err == nil {
		t.Fatal("accepted expired approval")
	}
	right.now = left.now
	c, created, err := left.ReserveExecution(ctx, "worker-a", intent, approval)
	if err != nil || !created {
		t.Fatalf("first group claim: %t %v", created, err)
	}
	if replay, created, err := right.ReserveExecution(ctx, "worker-a", intent, approval); err != nil || created || replay != c {
		t.Fatalf("repeat claim: %+v %t %v", replay, created, err)
	}
	if _, created, err := right.ReserveExecution(ctx, "worker-b", other, otherApproval); !errors.Is(err, ErrResourceBusy) || created {
		t.Fatalf("second node claimed the same Front group: %t %v", created, err)
	}
	if _, err := right.ReadApprovedFrontWorker(ctx, s.Intent.ClusterID, other, otherApproval); !errors.Is(err, ErrResourceBusy) {
		t.Fatalf("approved shadow did not see the open group claim: %v", err)
	}
	during, err := right.ReadResource(ctx, c.Scope)
	if err != nil || during.LastVerified != head || during.Pending != intent {
		t.Fatalf("unresolved operation displaced the verified head: %+v %v", during, err)
	}
	seen.Kind, seen.Outcome, seen.Fence = TerminalWitness, Applied, c.Fence
	seen.IntentDigest, seen.ApprovalDigest = c.IntentDigest, c.ApprovalDigest
	seen.Generation, seen.VerifiedDigest = head.Generation+1, s.Intent.TargetDigest
	result := ExecutionResult{Applied, Version{43, s.Intent.TargetDigest}, fixtureWitness(t, observer, seen, source)}
	if _, err := w.CompleteExecution(ctx, "observer-a", c, result); err == nil {
		t.Fatal("Node generation was accepted as the group ledger generation")
	}
	result.Verified.Generation = head.Generation + 1
	if created, err := w.CompleteExecution(ctx, "observer-a", c, result); err != nil || !created {
		t.Fatalf("could not settle the group claim: %t %v", created, err)
	}
	if got, err := right.ReadFrontWorkerBaselineSnapshot(ctx, baseline.Scope); err != nil || string(got) != string(snapshot) {
		t.Fatalf("group settlement replaced the initial snapshot: %s %v", got, err)
	}
	if _, err := right.ReadApprovedFrontWorker(ctx, s.Intent.ClusterID, other, otherApproval); !errors.Is(err, ErrResourceDrift) {
		t.Fatalf("a superseded group predecessor remained dispatchable: %v", err)
	}
}
