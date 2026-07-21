package compositecoordinator

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"fugue/internal/releasedomain"
)

func TestCompositeCoordinatorCommitsOnlyAfterSerialObservation(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	record, err := NewRecord("composite-release-1", coordinatorPlanFixture(t), now)
	if err != nil {
		t.Fatal(err)
	}
	transitions := []Transition{
		{Kind: TransitionBeginApply},
		{Kind: TransitionBeginObservation, EvidenceDigest: coordinatorDigest("1")},
		{Kind: TransitionCompleteObservation, EvidenceDigest: coordinatorDigest("2")},
		{Kind: TransitionBeginObservation, EvidenceDigest: coordinatorDigest("3")},
		{Kind: TransitionCompleteObservation, EvidenceDigest: coordinatorDigest("4")},
	}
	for index, transition := range transitions {
		now = now.Add(time.Second)
		record, err = ApplyTransition(record, transition, now)
		if err != nil {
			t.Fatalf("transition %d: %v", index, err)
		}
	}
	if record.State != StateCommitted || record.CurrentStep != 2 || record.Revision != 6 ||
		record.Steps[0].State != StepCompleted || record.Steps[1].State != StepCompleted {
		t.Fatalf("committed record = %#v", record)
	}
	if _, err := ApplyTransition(record, Transition{Kind: TransitionBeginRevert, Reason: "late"}, now.Add(time.Second)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("committed record accepted rollback: %v", err)
	}
}

func TestCompositeCoordinatorRevertsCurrentAndCompletedStepsInReverseOrder(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	record, err := NewRecord("composite-release-2", coordinatorPlanFixture(t), now)
	if err != nil {
		t.Fatal(err)
	}
	for index, transition := range []Transition{
		{Kind: TransitionBeginApply},
		{Kind: TransitionBeginObservation, EvidenceDigest: coordinatorDigest("1")},
		{Kind: TransitionCompleteObservation, EvidenceDigest: coordinatorDigest("2")},
		{Kind: TransitionBeginObservation, EvidenceDigest: coordinatorDigest("3")},
		{Kind: TransitionBeginRevert, Reason: "second domain unhealthy"},
		{Kind: TransitionCompleteRevert, EvidenceDigest: coordinatorDigest("4")},
		{Kind: TransitionCompleteRevert, EvidenceDigest: coordinatorDigest("5")},
	} {
		now = now.Add(time.Second)
		record, err = ApplyTransition(record, transition, now)
		if err != nil {
			t.Fatalf("transition %d (%s): %v", index, transition.Kind, err)
		}
	}
	if record.State != StateReverted || record.CurrentStep != -1 || record.RollbackStartStep != 1 ||
		record.Steps[0].State != StepReverted || record.Steps[1].State != StepReverted {
		t.Fatalf("reverted record = %#v", record)
	}
}

func TestCompositeCoordinatorFreezesWhenReverseProofCannotComplete(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	record, err := NewRecord("composite-release-3", coordinatorPlanFixture(t), now)
	if err != nil {
		t.Fatal(err)
	}
	for _, transition := range []Transition{
		{Kind: TransitionBeginApply},
		{Kind: TransitionBeginRevert, Reason: "apply uncertain"},
		{Kind: TransitionFreeze, Reason: "reverse evidence unavailable"},
	} {
		now = now.Add(time.Second)
		record, err = ApplyTransition(record, transition, now)
		if err != nil {
			t.Fatal(err)
		}
	}
	if record.State != StateFrozen || record.Steps[0].State != StepFrozen || record.FreezeReason == "" {
		t.Fatalf("frozen record = %#v", record)
	}
}

func TestCompositeCoordinatorRejectsProgressWithoutDurableEvidence(t *testing.T) {
	record, err := NewRecord("composite-release-4", coordinatorPlanFixture(t), time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	record.State = StateApplying
	record.Steps[0].State = StepApplying
	record.Digest = recordDigest(record)
	if err := VerifyRecord(record); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("applying state without startedAt was accepted: %v", err)
	}
}

func TestCompositeCoordinatorRejectsUnknownDurableJSON(t *testing.T) {
	record, err := NewRecord("composite-release-5", coordinatorPlanFixture(t), time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded[:1], append([]byte(`"unknown":true,`), encoded[1:]...)...)
	var decoded Record
	if err := json.Unmarshal(encoded, &decoded); err == nil {
		t.Fatal("unknown durable record field was accepted")
	}
}

func TestCompositeCoordinatorRejectsDuplicateDurableJSON(t *testing.T) {
	record, err := NewRecord("composite-release-6", coordinatorPlanFixture(t), time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded[:1], append([]byte(`"state":"frozen",`), encoded[1:]...)...)
	var decoded Record
	if err := json.Unmarshal(encoded, &decoded); err == nil {
		t.Fatal("duplicate durable record field was accepted")
	}
}

func coordinatorPlanFixture(t *testing.T) releasedomain.CompositeReleasePlan {
	t.Helper()
	plan, err := releasedomain.NewCompositeReleasePlan(releasedomain.CompositeReleasePlan{
		BaseCommit:                "1111111111111111111111111111111111111111",
		TargetCommit:              "2222222222222222222222222222222222222222",
		ImageActivationPlanDigest: coordinatorDigest("a"), Generation: "7", FencingEpoch: "11",
		BaseVersions: []releasedomain.DomainVersion{
			{Domain: releasedomain.DomainAuthoritativeDNS, Version: coordinatorDigest("b")},
			{Domain: releasedomain.DomainControlPlane, Version: coordinatorDigest("c")},
		},
		TargetVersions: []releasedomain.DomainVersion{
			{Domain: releasedomain.DomainAuthoritativeDNS, Version: coordinatorDigest("d")},
			{Domain: releasedomain.DomainControlPlane, Version: coordinatorDigest("e")},
		},
		Steps: []releasedomain.CompositeReleaseStep{
			coordinatorStep("authoritative-dns", releasedomain.DomainAuthoritativeDNS, "control_plane_release_adapter_authoritative_dns", "b", "d"),
			coordinatorStep("control-plane", releasedomain.DomainControlPlane, "control_plane_release_adapter_control_plane", "c", "e"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func coordinatorStep(id string, domain releasedomain.Domain, adapter, base, target string) releasedomain.CompositeReleaseStep {
	depends := []string{}
	if id == "control-plane" {
		depends = []string{"authoritative-dns"}
	}
	return releasedomain.CompositeReleaseStep{
		ID: id, Domain: domain, Adapter: adapter, DependsOn: depends,
		ActivationIDs: []string{"activate-" + id}, BaseVersion: coordinatorDigest(base), TargetVersion: coordinatorDigest(target),
		ForwardRenderedDigest: coordinatorDigest("f"), ReverseRenderedDigest: coordinatorDigest("0"),
		Observation: releasedomain.CompositeObservationPolicy{
			HealthEvidenceDigest: coordinatorDigest("9"), MinimumSamples: "5", WindowSeconds: "120",
		},
		RollbackBudgetSeconds: "300",
	}
}

func coordinatorDigest(digit string) string {
	value := ""
	for len(value) < 64 {
		value += digit
	}
	return "sha256:" + value[:64]
}
