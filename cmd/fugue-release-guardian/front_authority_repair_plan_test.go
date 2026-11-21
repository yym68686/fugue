package main

import (
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
)

func partialFrontRollbackFixture() (releaseguardian.CurrentAuthority, []frontRepairNode, []frontRepairWitness) {
	digest := func(c string) string { return "sha256:" + strings.Repeat(c, 64) }
	before := releaseguardian.CurrentAuthority{
		APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind, GroupID: "edge-group-fixture",
		CurrentRecordDigest: digest("1"), CurrentWorkerSlot: releaseguardian.AuthoritySlotB, CurrentFrontGeneration: 30,
		CurrentBundleGeneration: "committed.p41.r3", CurrentWorkerSourceSHA: strings.Repeat("b", 40), CurrentWorkerImageDigest: digest("b"),
		PreviousRecordDigest: digest("2"), PreviousWorkerSlot: releaseguardian.AuthoritySlotA, PreviousFrontGeneration: 29,
		PreviousBundleGeneration: "earlier.p38.r3", PreviousWorkerSourceSHA: strings.Repeat("a", 40), PreviousWorkerImageDigest: digest("a"),
		AuthorityEpoch: 9,
	}
	cohort := []frontRepairNode{{Name: "edge-c", UID: "uid-node-c", Admitted: true}, {Name: "edge-b", UID: "uid-node-b"}, {Name: "edge-a", UID: "uid-node-a"}}
	var observed []frontRepairWitness
	for _, node := range cohort {
		state := edgegroupfront.ActivationState{
			GroupID: before.GroupID, Schema: edgegroupfront.ActivationStateSchemaV1, Authority: edgegroupfront.ActivationAuthority,
			Generation: before.CurrentFrontGeneration, PreviousSlot: string(before.PreviousWorkerSlot), ActiveSlot: string(before.CurrentWorkerSlot),
			BundleGeneration: before.CurrentBundleGeneration, WorkerSourceCommit: before.CurrentWorkerSourceSHA, WorkerImageDigest: before.CurrentWorkerImageDigest,
			Operation: edgegroupfront.ActivationOperationPromote, UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Reason: "test authority fixture",
		}
		if node.Admitted {
			state.Generation++
			state.PreviousSlot, state.ActiveSlot = state.ActiveSlot, state.PreviousSlot
			state.Operation, state.RollbackOfGeneration = edgegroupfront.ActivationOperationRollback, before.CurrentFrontGeneration
			state.WorkerSourceCommit, state.WorkerImageDigest = before.PreviousWorkerSourceSHA, before.PreviousWorkerImageDigest
		}
		observed = append(observed, frontRepairWitness{frontRepairMember: frontRepairMember{Node: node, FrontPodUID: node.Name + "-front-uid", TargetWorkerPodUID: node.Name + "-target-uid",
			Activation: state, TargetWorkerSlot: before.CurrentWorkerSlot, TargetWorkerSourceSHA: before.CurrentWorkerSourceSHA, TargetWorkerImage: before.CurrentWorkerImageDigest}, LiveFront: liveFrontRepairFixture(state),
			TargetWorkerHealth: authorityWorkerHealth{Healthy: true, Status: "ok", EdgeGroupID: before.GroupID, BundleVersion: "live.p43.r3",
				CaddyAppliedVersion: "live.p43.r3", PublicationSequence: 43, ServingGeneration: "live"}})
	}
	return before, cohort, observed
}

func liveFrontRepairFixture(state edgegroupfront.ActivationState) observedFront {
	live := observedFrontFromActivation(state)
	live.Status = "ok"
	return live
}

func TestPartialFrontRollbackRepairPlanBindsBothCodeAndPublication(t *testing.T) {
	before, cohort, observed := partialFrontRollbackFixture()
	plan, err := planPartialFrontRollbackRepair(before, "pointer-uid", "101", cohort, observed)
	if err != nil || plan.Before != before || plan.TargetGeneration != before.CurrentFrontGeneration+2 ||
		len(plan.Members) != len(cohort) || plan.Members[0].Node.Name != "edge-a" || !exactSHA256Digest(plan.Digest) {
		t.Fatalf("unexpected plan: %+v, error=%v", plan, err)
	}
	if observed[0].Node.Name != "edge-c" || before.CurrentBundleGeneration == before.PreviousBundleGeneration {
		t.Fatal("invalid test fixture or plan modified its input")
	}
	reordered := []frontRepairWitness{observed[2], observed[0], observed[1]}
	replayed, err := planPartialFrontRollbackRepair(before, "pointer-uid", "101", cohort, reordered)
	if err != nil || !reflect.DeepEqual(replayed, plan) {
		t.Fatalf("evidence reordering changed the plan: %+v, error=%v", replayed, err)
	}
	newRV, err := planPartialFrontRollbackRepair(before, "pointer-uid", "102", cohort, observed)
	if err != nil || newRV.Digest == plan.Digest {
		t.Fatalf("authority resourceVersion is not bound: %q, error=%v", newRV.Digest, err)
	}
	observed[0].Activation.BundleGeneration = before.PreviousBundleGeneration
	observed[0].LiveFront = liveFrontRepairFixture(observed[0].Activation)
	previousPublication, err := planPartialFrontRollbackRepair(before, "pointer-uid", "101", cohort, observed)
	if err != nil || previousPublication.Digest == plan.Digest {
		t.Fatalf("exact previous publication should be accepted with a different digest: %q, error=%v", previousPublication.Digest, err)
	}
}

func TestPartialFrontRollbackRepairPlanRejectsUnprovenState(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*releaseguardian.CurrentAuthority, []frontRepairNode, []frontRepairWitness)
	}{
		{"rollback generation advanced twice", func(_ *releaseguardian.CurrentAuthority, _ []frontRepairNode, w []frontRepairWitness) {
			w[0].Activation.Generation++
		}},
		{"rollback has another source", func(_ *releaseguardian.CurrentAuthority, _ []frontRepairNode, w []frontRepairWitness) {
			w[0].Activation.WorkerSourceCommit = strings.Repeat("c", 40)
		}},
		{"rollback names another publication", func(_ *releaseguardian.CurrentAuthority, _ []frontRepairNode, w []frontRepairWitness) {
			w[0].Activation.BundleGeneration = "unknown.p42.r3"
		}},
		{"rollback of a different activation", func(_ *releaseguardian.CurrentAuthority, _ []frontRepairNode, w []frontRepairWitness) {
			w[0].Activation.RollbackOfGeneration--
		}},
		{"wrong rollback direction", func(_ *releaseguardian.CurrentAuthority, _ []frontRepairNode, w []frontRepairWitness) {
			w[0].Activation.PreviousSlot = "a"
		}},
		{"target Worker is the other slot", func(b *releaseguardian.CurrentAuthority, _ []frontRepairNode, w []frontRepairWitness) {
			w[0].TargetWorkerSlot = b.PreviousWorkerSlot
		}},
		{"target Worker has wrong runtime digest", func(b *releaseguardian.CurrentAuthority, _ []frontRepairNode, w []frontRepairWitness) {
			w[0].TargetWorkerImage = b.PreviousWorkerImageDigest
		}},
		{"ordinary node is not admitted for initialization", func(_ *releaseguardian.CurrentAuthority, _ []frontRepairNode, w []frontRepairWitness) {
			w[1].Activation.Operation, w[1].Activation.PreviousSlot = edgegroupfront.ActivationOperationInit, ""
		}},
		{"a physical node was replaced", func(_ *releaseguardian.CurrentAuthority, _ []frontRepairNode, w []frontRepairWitness) {
			w[0].Node.UID = "replacement"
		}},
		{"an unlabelled witness replaces a member", func(_ *releaseguardian.CurrentAuthority, _ []frontRepairNode, w []frontRepairWitness) {
			w[0].Node = frontRepairNode{}
		}},
		{"reuse a Pod on two nodes", func(_ *releaseguardian.CurrentAuthority, _ []frontRepairNode, w []frontRepairWitness) {
			w[0].TargetWorkerPodUID = w[1].TargetWorkerPodUID
		}},
		{"duplicate approved node uid", func(_ *releaseguardian.CurrentAuthority, n []frontRepairNode, _ []frontRepairWitness) {
			n[0].UID = n[1].UID
		}},
		{"all already converged", func(_ *releaseguardian.CurrentAuthority, _ []frontRepairNode, w []frontRepairWitness) {
			w[0].Activation = w[1].Activation
		}},
		{"case is not a partial rollback", func(_ *releaseguardian.CurrentAuthority, _ []frontRepairNode, w []frontRepairWitness) {
			w[1].Activation, w[2].Activation = w[0].Activation, w[0].Activation
		}},
		{"exhausted activation counter", func(b *releaseguardian.CurrentAuthority, _ []frontRepairNode, _ []frontRepairWitness) {
			b.CurrentFrontGeneration = math.MaxUint64 - 2
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			before, cohort, observed := partialFrontRollbackFixture()
			test.mutate(&before, cohort, observed)
			for i := range observed {
				observed[i].LiveFront = liveFrontRepairFixture(observed[i].Activation)
			}
			plan, err := planPartialFrontRollbackRepair(before, "pointer-uid", "101", cohort, observed)
			if err == nil || plan.Digest != "" {
				t.Fatalf("unproven state yielded a plan: %+v, error=%v", plan, err)
			}
		})
	}
	before, cohort, observed := partialFrontRollbackFixture()
	observed[0].LiveFront.Generation--
	if _, err := planPartialFrontRollbackRepair(before, "pointer-uid", "101", cohort, observed); err == nil {
		t.Fatal("live Front may not lag the durable activation")
	}
	if _, err := planPartialFrontRollbackRepair(before, "pointer-uid", "101", cohort, observed[:2]); err == nil {
		t.Fatal("missing cohort member was accepted")
	}
}

func TestPartialFrontRollbackRepairPlanRequiresPreparedWorkerBeforeSwitch(t *testing.T) {
	before, cohort, observed := partialFrontRollbackFixture()
	currentHealth := observed[0].TargetWorkerHealth
	observed[0].TargetWorkerHealth.Status = "degraded" // inactive LKG, not ready to switch
	plan, err := planPartialFrontRollbackRepair(before, "pointer-uid", "101", cohort, observed)
	if err != nil {
		t.Fatalf("inactive LKG prevented sealing the predecessor: %v", err)
	}
	now := observed[0].Activation.UpdatedAt.Add(time.Minute)
	p := edgegroupfront.PublicationPreparation{Schema: edgegroupfront.PublicationPreparationSchema,
		Previous: observed[0].Activation, TargetSlot: string(before.CurrentWorkerSlot), BundleGeneration: before.CurrentBundleGeneration,
		WorkerSourceCommit: before.CurrentWorkerSourceSHA, WorkerImageDigest: before.CurrentWorkerImageDigest,
		CreatedAt: now, ExpiresAt: now.Add(edgegroupfront.PublicationPreparationLifetime)}
	verify := func(w []frontRepairWitness, rv string, preparations map[string]edgegroupfront.PublicationPreparation, at time.Time) error {
		return verifyFrontRepairPrepared(plan, before, "pointer-uid", rv, cohort, w, preparations, at)
	}
	preparations := map[string]edgegroupfront.PublicationPreparation{observed[0].Node.Name: p}
	if err := verify(observed, "101", preparations, now); err == nil {
		t.Fatal("switch was permitted on degraded standby even with preparation")
	}
	observed[0].TargetWorkerHealth = currentHealth
	if err := verify(observed, "101", preparations, now.Add(time.Second)); err != nil {
		t.Fatalf("prepared Worker never became eligible: %v", err)
	}
	if err := verify(observed, "101", nil, now); err == nil {
		t.Fatal("missing publication preparation was accepted")
	}
	if err := verify(observed, "101", preparations, p.ExpiresAt.Add(-5*time.Second)); err == nil {
		t.Fatal("publication preparation too close to expiry was accepted")
	}
	if err := verify(observed, "102", preparations, now); err == nil {
		t.Fatal("changed authority CAS was accepted")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*frontRepairWitness)
	}{
		{"replaced target Worker", func(w *frontRepairWitness) { w.TargetWorkerPodUID = "replacement-pod-uid" }},
		{"target is unhealthy", func(w *frontRepairWitness) { w.TargetWorkerHealth.Healthy = false }},
		{"unapplied route", func(w *frontRepairWitness) { w.TargetWorkerHealth.CaddyAppliedVersion = "" }},
		{"another applied publication", func(w *frontRepairWitness) {
			h := &w.TargetWorkerHealth
			h.PublicationSequence, h.BundleVersion, h.CaddyAppliedVersion = 44, "live.p44.r3", "live.p44.r3"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := append([]frontRepairWitness(nil), observed...)
			tc.mutate(&changed[0])
			if err := verify(changed, "101", preparations, now); err == nil {
				t.Fatal("changed or unready target Worker was accepted")
			}
		})
	}
}

func TestPartialFrontRollbackRepairOnlyPreparesDivergedMemberWithoutSwitch(t *testing.T) {
	before, cohort, observed := partialFrontRollbackFixture()
	path := filepath.Join(t.TempDir(), "activation.json")
	timebase := observed[0].Activation.UpdatedAt
	init := edgegroupfront.ActivationCASRequest{GroupID: before.GroupID, ExpectedSlot: string(before.PreviousWorkerSlot), TargetSlot: string(before.PreviousWorkerSlot),
		BundleGeneration: before.PreviousBundleGeneration, WorkerSourceCommit: before.PreviousWorkerSourceSHA, WorkerImageDigest: before.PreviousWorkerImageDigest,
		InitialGeneration: before.PreviousFrontGeneration, Operation: edgegroupfront.ActivationOperationInit, Reason: "initialize test Front"}
	if _, err := edgegroupfront.ApplyActivationCAS(path, init, timebase); err != nil {
		t.Fatal(err)
	}
	promote := edgegroupfront.ActivationCASRequest{GroupID: before.GroupID, ExpectedGeneration: before.PreviousFrontGeneration,
		ExpectedSlot: string(before.PreviousWorkerSlot), TargetSlot: string(before.CurrentWorkerSlot), BundleGeneration: before.CurrentBundleGeneration,
		WorkerSourceCommit: before.CurrentWorkerSourceSHA, WorkerImageDigest: before.CurrentWorkerImageDigest,
		Operation: edgegroupfront.ActivationOperationPromote, Reason: "activate committed test Worker"}
	if _, err := edgegroupfront.ApplyActivationCAS(path, promote, timebase.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	rollback := init
	rollback.InitialGeneration, rollback.ExpectedGeneration = 0, before.CurrentFrontGeneration
	rollback.ExpectedSlot, rollback.BundleGeneration = string(before.CurrentWorkerSlot), before.CurrentBundleGeneration
	rollback.Operation, rollback.RollbackOfGeneration, rollback.Reason = edgegroupfront.ActivationOperationRollback, before.CurrentFrontGeneration, "simulate partial rollback for repair"
	old, err := edgegroupfront.ApplyActivationCAS(path, rollback, timebase.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	observed[0].Activation, observed[0].LiveFront = old.Current, liveFrontRepairFixture(old.Current)
	plan, err := planPartialFrontRollbackRepair(before, "pointer-uid", "101", cohort, observed)
	if err != nil {
		t.Fatal(err)
	}
	requests, err := frontRepairPublicationRequests(plan)
	request, found := requests[observed[0].Node.Name]
	if err != nil || len(requests) != 1 || !found {
		t.Fatalf("only the divergent member should be prepared: %+v, error=%v", requests, err)
	}
	now := timebase.Add(3 * time.Second)
	prepared, err := edgegroupfront.PreparePublication(path, request.request, now)
	if err != nil {
		t.Fatal(err)
	}
	preparedAgain, err := edgegroupfront.PreparePublication(path, request.request, now.Add(time.Second))
	if err != nil || preparedAgain != prepared {
		t.Fatalf("preparation replay extended or changed the grant: error=%v", err)
	}
	stored, exists, err := edgegroupfront.ReadActivationState(path)
	if err != nil || !exists || stored != old.Current {
		t.Fatalf("preparation changed the serving pointer: %+v, error=%v", stored, err)
	}
	if err := verifyFrontRepairPrepared(plan, before, "pointer-uid", "101", cohort, observed,
		map[string]edgegroupfront.PublicationPreparation{observed[0].Node.Name: prepared}, now.Add(time.Second)); err != nil {
		t.Fatalf("exact current route and real preparation were not accepted: %v", err)
	}
}
