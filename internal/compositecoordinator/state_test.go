package compositecoordinator

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"fugue/internal/releasecontract"
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

func TestCompositeCoordinatorRejectsCaseInsensitiveDurableJSONFields(t *testing.T) {
	record, err := NewRecord("composite-release-7", coordinatorPlanFixture(t), time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	for name, occurrence := range map[string]int{"top-level": 1, "nested-plan": 2} {
		modified := replaceJSONFieldOccurrence(t, encoded, []byte(`"kind":`), []byte(`"Kind":`), occurrence)
		var decoded Record
		if err := json.Unmarshal(modified, &decoded); err == nil {
			t.Fatalf("%s case-insensitive durable field was accepted", name)
		}
	}
}

func TestCompositeCoordinatorRejectsMalformedUnicodeAndInvalidDigestOnDecode(t *testing.T) {
	record, err := NewRecord("composite-release-8", coordinatorPlanFixture(t), time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	malformed := bytes.Replace(encoded, []byte(`"composite-release-8"`), []byte(`"\ud800"`), 1)
	var decoded Record
	if err := json.Unmarshal(malformed, &decoded); err == nil {
		t.Fatal("malformed durable Unicode was accepted")
	}
	invalidDigest := bytes.Replace(encoded, []byte(record.Digest), []byte(coordinatorDigest("8")), 1)
	if err := json.Unmarshal(invalidDigest, &decoded); err == nil {
		t.Fatal("durable record with a mismatched digest was accepted")
	}
}

func TestCompositeCoordinatorCopiesPlanOwnership(t *testing.T) {
	plan := coordinatorPlanFixture(t)
	record, err := NewRecord("composite-release-9", plan, time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	plan.BaseVersions[0].Version = coordinatorDigest("8")
	plan.TargetVersions[0].Version = coordinatorDigest("7")
	plan.Steps[0].ID = "mutated"
	plan.Steps[1].DependsOn[0] = "mutated"
	if err := VerifyRecord(record); err != nil || record.Steps[0].ID != "authoritative-dns" || record.Plan.Steps[1].DependsOn[0] != "authoritative-dns" {
		t.Fatalf("record retained caller-owned plan memory: %#v err=%v", record, err)
	}
}

func replaceJSONFieldOccurrence(t *testing.T, data, old, replacement []byte, occurrence int) []byte {
	t.Helper()
	result := append([]byte(nil), data...)
	offset := 0
	for index := 1; index <= occurrence; index++ {
		found := bytes.Index(result[offset:], old)
		if found < 0 {
			t.Fatalf("field occurrence %d was not found", occurrence)
		}
		offset += found
		if index == occurrence {
			return append(append(append([]byte(nil), result[:offset]...), replacement...), result[offset+len(old):]...)
		}
		offset += len(old)
	}
	return nil
}

func coordinatorPlanFixture(t *testing.T) releasecontract.CompositeReleasePlan {
	t.Helper()
	plan, err := releasecontract.NewCompositeReleasePlan(releasecontract.CompositeReleasePlan{
		BaseCommit:                "1111111111111111111111111111111111111111",
		TargetCommit:              "2222222222222222222222222222222222222222",
		ImageActivationPlanDigest: coordinatorDigest("a"), Generation: "7", FencingEpoch: "11",
		BaseVersions: []releasecontract.DomainVersion{
			{Domain: releasecontract.DomainAuthoritativeDNS, Version: coordinatorDigest("b")},
			{Domain: releasecontract.DomainControlPlane, Version: coordinatorDigest("c")},
		},
		TargetVersions: []releasecontract.DomainVersion{
			{Domain: releasecontract.DomainAuthoritativeDNS, Version: coordinatorDigest("d")},
			{Domain: releasecontract.DomainControlPlane, Version: coordinatorDigest("e")},
		},
		Steps: []releasecontract.CompositeReleaseStep{
			coordinatorStep("authoritative-dns", releasecontract.DomainAuthoritativeDNS, "control_plane_release_adapter_authoritative_dns", "b", "d"),
			coordinatorStep("control-plane", releasecontract.DomainControlPlane, "control_plane_release_adapter_control_plane", "c", "e"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func coordinatorStep(id string, domain releasecontract.Domain, adapter, base, target string) releasecontract.CompositeReleaseStep {
	depends := []string{}
	if id == "control-plane" {
		depends = []string{"authoritative-dns"}
	}
	return releasecontract.CompositeReleaseStep{
		ID: id, Domain: domain, Adapter: adapter, DependsOn: depends,
		ActivationIDs: []string{"activate-" + id}, BaseVersion: coordinatorDigest(base), TargetVersion: coordinatorDigest(target),
		ForwardRenderedDigest: coordinatorDigest("f"), ReverseRenderedDigest: coordinatorDigest("0"),
		Observation: releasecontract.CompositeObservationPolicy{
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
