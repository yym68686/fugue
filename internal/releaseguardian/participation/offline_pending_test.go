package participation

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
)

type pendingHoldFixture struct {
	submission productioncontrol.Submission
	claim      productioncontrol.Claim
	terminal   *productioncontrol.ExecutionResult
	head       productioncontrol.ResourceState
	topology   productioncontrol.VerifiedParticipationTopology
	claimErr   error
}

func (f *pendingHoldFixture) ReadIntent(context.Context, string) (productioncontrol.Intent, []byte, error) {
	return f.submission.Intent, f.submission.Payload, nil
}

func (f *pendingHoldFixture) ReadExecution(context.Context, productioncontrol.Scope, string) (productioncontrol.Claim, *productioncontrol.ExecutionResult, error) {
	return f.claim, f.terminal, f.claimErr
}

func (f *pendingHoldFixture) ReadResource(context.Context, productioncontrol.Scope) (productioncontrol.ResourceState, error) {
	return f.head, nil
}

func (f *pendingHoldFixture) ReadParticipationTopology(context.Context, string) (productioncontrol.VerifiedParticipationTopology, error) {
	return f.topology, nil
}

func TestPendingOfflineHoldDoesNotConfuseObservationWithSettlement(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	topology := offlineFixtureTopology(now)
	s, n, _ := draftFixtureOfflineHold(t, topology, now)
	intent, err := s.Intent.Digest()
	if err != nil {
		t.Fatal(err)
	}
	scope := HoldResource("fixture", n.Name)
	predecessor := productioncontrol.Version{Generation: s.Intent.Generation, Digest: s.Intent.CurrentDigest}
	claim := productioncontrol.Claim{Scope: scope, IntentDigest: intent, ApprovalDigest: digest([]byte("hold approval")), Fence: 1}
	base := pendingHoldFixture{
		submission: s, claim: claim, topology: topology,
		head: productioncontrol.ResourceState{Baseline: productioncontrol.Baseline{Scope: scope}, LastVerified: predecessor, LastFence: 1, Pending: intent},
	}
	for _, tc := range []struct {
		name           string
		before, during func(*pendingHoldFixture)
		err            error
		calls          int
	}{
		{name: "pending", calls: 1},
		{name: "unclaimed intent", before: func(f *pendingHoldFixture) { f.claimErr = sql.ErrNoRows }},
		{name: "already settled", before: func(f *pendingHoldFixture) { f.terminal = &productioncontrol.ExecutionResult{} }},
		{name: "wrong fence", before: func(f *pendingHoldFixture) { f.head.LastFence++ }},
		{name: "wrong last verified version", before: func(f *pendingHoldFixture) { f.head.LastVerified.Generation++ }},
		{name: "wrong intent", before: func(f *pendingHoldFixture) { f.submission.Intent.RequestID = "different" }},
		{name: "racing settlement", during: func(f *pendingHoldFixture) { f.terminal = &productioncontrol.ExecutionResult{} }, calls: 1},
		{name: "racing journal head", during: func(f *pendingHoldFixture) { f.head.Pending = "" }, calls: 1},
		{name: "changed topology approval", during: func(f *pendingHoldFixture) { f.topology.ApprovalDigest = digest([]byte("another")) }, calls: 1},
		{name: "changed claim approval", during: func(f *pendingHoldFixture) { f.claim.ApprovalDigest = digest([]byte("another")) }, calls: 1},
		{name: "unavailable provider", err: errors.New("provider unavailable"), calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := base
			if tc.before != nil {
				tc.before(&f)
			}
			calls := 0
			p := OfflineHoldReadback{State: OfflineHoldTargetVisible, Node: ObjectVersion{Name: n.Name, UID: string(n.UID), ResourceVersion: "11"},
				ExpectedResourceVersion: n.ResourceVersion, PredecessorDigest: s.Intent.CurrentDigest, TargetDigest: s.Intent.TargetDigest, ObservedDigest: s.Intent.TargetDigest}
			got, err := observePendingOfflineHold(context.Background(), &f, scope.ClusterID, intent,
				func(_ context.Context, actual productioncontrol.VerifiedParticipationTopology, submission productioncontrol.Submission) (OfflineHoldReadback, error) {
					calls++
					if actual.IntentDigest != topology.IntentDigest || submission.Intent != s.Intent {
						t.Fatal("provider read used a different journal submission")
					}
					if tc.during != nil {
						tc.during(&f)
					}
					return p, tc.err
				})
			if calls != tc.calls {
				t.Fatalf("provider call count=%d, want %d", calls, tc.calls)
			}
			if tc.before != nil || tc.during != nil || tc.err != nil {
				if err == nil || got != (PendingOfflineHoldReadback{}) {
					t.Fatalf("returned an unproven report: %+v, %v", got, err)
				}
				return
			}
			if err != nil || got.Provider != p || got.Fence != claim.Fence || got.IntentDigest != intent ||
				got.ApprovalDigest != claim.ApprovalDigest || got.LastVerified != predecessor || got.Topology != topology.Version {
				t.Fatalf("incorrect pending observation: %+v, %v", got, err)
			}
		})
	}
}
