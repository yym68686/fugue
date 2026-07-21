package compositecoordinator

import (
	"bytes"
	"testing"
	"time"
)

func TestControlledNoopRecoveryDrillReversesCurrentThenCompletedDomain(t *testing.T) {
	record := noopAuthorizationRecord(t, "composite-drill-1")
	authorization := noopAuthorizationForRecord(t, record)
	final, result, err := RunControlledNoopRecoveryDrill(
		record, authorization, record.Plan.Steps[1].ID, record.UpdatedAt.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf("run drill: %v", err)
	}
	if err := VerifyRecoveryDrillResult(record, authorization, final, result); err != nil {
		t.Fatalf("verify drill: %v", err)
	}
	if result.ProductionWrite || result.FinalState != StateReverted || final.State != StateReverted {
		t.Fatalf("drill crossed write boundary or did not revert: result=%#v final=%#v", result, final)
	}
	wanted := []string{
		"apply-noop:" + record.Plan.Steps[0].ID,
		"observe-noop:" + record.Plan.Steps[0].ID,
		"apply-noop:" + record.Plan.Steps[1].ID,
		"observe-noop:" + record.Plan.Steps[1].ID,
		"induce-failure:" + record.Plan.Steps[1].ID,
		"reverse-noop:" + record.Plan.Steps[1].ID,
		"reverse-noop:" + record.Plan.Steps[0].ID,
	}
	for index, event := range result.Events {
		if event.Action+":"+event.StepID != wanted[index] {
			t.Fatalf("event %d = %#v, want %q", index, event, wanted[index])
		}
	}
}

func TestControlledNoopRecoveryDrillRejectsWrongAuthorizationAndFailureStep(t *testing.T) {
	record := noopAuthorizationRecord(t, "composite-drill-2")
	other := noopAuthorizationRecord(t, "composite-drill-3")
	otherAuthorization := noopAuthorizationForRecord(t, other)
	if _, _, err := RunControlledNoopRecoveryDrill(
		record, otherAuthorization, record.Plan.Steps[1].ID, record.UpdatedAt.Add(time.Minute),
	); err == nil {
		t.Fatal("authorization for another record started the drill")
	}
	authorization := noopAuthorizationForRecord(t, record)
	if _, _, err := RunControlledNoopRecoveryDrill(
		record, authorization, record.Plan.Steps[0].ID, record.UpdatedAt.Add(time.Minute),
	); err == nil {
		t.Fatal("failure before completing the first domain was accepted")
	}
}

func TestControlledNoopRecoveryDrillEvidenceDetectsMutation(t *testing.T) {
	record := noopAuthorizationRecord(t, "composite-drill-4")
	authorization := noopAuthorizationForRecord(t, record)
	final, result, err := RunControlledNoopRecoveryDrill(
		record, authorization, record.Plan.Steps[1].ID, record.UpdatedAt.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf("run drill: %v", err)
	}
	mutated := result
	mutated.Events = append([]RecoveryDrillEvent(nil), result.Events...)
	mutated.Events[5].StepID = record.Plan.Steps[0].ID
	mutated.Digest = DigestRecoveryDrillResult(mutated)
	if err := VerifyRecoveryDrillResult(record, authorization, final, mutated); err == nil {
		t.Fatal("re-signed reverse-order mutation verified")
	}
	mutated = result
	mutated.ProductionWrite = true
	mutated.Digest = DigestRecoveryDrillResult(mutated)
	if err := VerifyRecoveryDrillResult(record, authorization, final, mutated); err == nil {
		t.Fatal("re-signed production-write mutation verified")
	}
	mutatedFinal := final
	mutatedFinal.Revision++
	mutatedFinal.Digest = recordDigest(mutatedFinal)
	mutated = result
	mutated.FinalRecordDigest = mutatedFinal.Digest
	mutated.Digest = DigestRecoveryDrillResult(mutated)
	if err := VerifyRecoveryDrillResult(record, authorization, mutatedFinal, mutated); err == nil {
		t.Fatal("re-signed final revision mutation verified")
	}
}

func noopAuthorizationForRecord(t *testing.T, record Record) NoopAuthorization {
	t.Helper()
	encoded := noopAuthorizationEnvelope(t, record)
	authorization, err := DecodeAndAuthorizeNoop(record, bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("authorize noop: %v", err)
	}
	return authorization
}
