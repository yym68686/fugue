package compositecoordinator

import (
	"errors"
	"testing"
	"time"
)

func TestDurableNoopWorkerCommitsSeriallyWithoutProductionWrite(t *testing.T) {
	record := noopAuthorizationRecord(t, "composite-durable-noop-1")
	authorization := noopAuthorizationForRecord(t, record)
	store := newDurableNoopTestStore(record)

	final, result, err := RunDurableNoop(store, record.ID, authorization, "")
	if err != nil {
		t.Fatalf("run durable noop: %v", err)
	}
	if err := VerifyDurableNoopRunResult(record, authorization, final, result); err != nil {
		t.Fatalf("verify durable noop: %v", err)
	}
	if final.State != StateCommitted || result.ProductionWrite || len(store.transitions) != 5 {
		t.Fatalf("unexpected durable result: state=%s write=%t transitions=%d", final.State, result.ProductionWrite, len(store.transitions))
	}
	wanted := []TransitionKind{
		TransitionBeginApply,
		TransitionBeginObservation, TransitionCompleteObservation,
		TransitionBeginObservation, TransitionCompleteObservation,
	}
	for index := range wanted {
		if store.transitions[index].Kind != wanted[index] {
			t.Fatalf("transition %d = %s, want %s", index, store.transitions[index].Kind, wanted[index])
		}
	}
}

func TestDurableNoopWorkerPersistsAutomaticReverseOrder(t *testing.T) {
	record := noopAuthorizationRecord(t, "composite-durable-noop-2")
	authorization := noopAuthorizationForRecord(t, record)
	store := newDurableNoopTestStore(record)

	final, result, err := RunDurableNoop(store, record.ID, authorization, record.Plan.Steps[1].ID)
	if err != nil {
		t.Fatalf("run durable noop recovery: %v", err)
	}
	if err := VerifyDurableNoopRunResult(record, authorization, final, result); err != nil {
		t.Fatalf("verify durable noop recovery: %v", err)
	}
	if final.State != StateReverted || result.ProductionWrite || len(store.transitions) != 7 {
		t.Fatalf("unexpected durable recovery: state=%s write=%t transitions=%d", final.State, result.ProductionWrite, len(store.transitions))
	}
	if result.Events[len(result.Events)-2].StepID != record.Plan.Steps[1].ID ||
		result.Events[len(result.Events)-1].StepID != record.Plan.Steps[0].ID {
		t.Fatalf("recovery was not current-then-completed: %#v", result.Events)
	}
	if store.gets != 1 {
		t.Fatalf("worker re-read an unbound record: gets=%d", store.gets)
	}
}

func TestDurableNoopWorkerRejectsUnboundInputsBeforeAdvance(t *testing.T) {
	record := noopAuthorizationRecord(t, "composite-durable-noop-3")
	other := noopAuthorizationRecord(t, "composite-durable-noop-4")
	store := newDurableNoopTestStore(record)

	if _, _, err := RunDurableNoop(store, record.ID, noopAuthorizationForRecord(t, other), ""); err == nil {
		t.Fatal("authorization for another record started the durable worker")
	}
	if len(store.transitions) != 0 {
		t.Fatalf("unauthorized worker advanced %d transitions", len(store.transitions))
	}
	if _, _, err := RunDurableNoop(store, record.ID, noopAuthorizationForRecord(t, record), "missing-step"); err == nil {
		t.Fatal("unknown failure step started the durable worker")
	}
	if len(store.transitions) != 0 {
		t.Fatalf("invalid failure step advanced %d transitions", len(store.transitions))
	}
	if _, _, err := RunDurableNoop(nil, record.ID, noopAuthorizationForRecord(t, record), ""); err == nil {
		t.Fatal("nil store started the durable worker")
	}
}

func TestDurableNoopWorkerFailsClosedOnCASOrEvidenceMutation(t *testing.T) {
	record := noopAuthorizationRecord(t, "composite-durable-noop-5")
	authorization := noopAuthorizationForRecord(t, record)
	store := newDurableNoopTestStore(record)
	store.failAt = 2
	if _, _, err := RunDurableNoop(store, record.ID, authorization, ""); !errors.Is(err, ErrDurableNoopRun) {
		t.Fatalf("CAS failure = %v, want durable fail-closed", err)
	}
	if len(store.transitions) != 1 {
		t.Fatalf("worker continued after CAS failure: transitions=%d", len(store.transitions))
	}
	store = newDurableNoopTestStore(record)
	store.substituteAt = 2
	if _, _, err := RunDurableNoop(store, record.ID, authorization, ""); !errors.Is(err, ErrDurableNoopRun) {
		t.Fatalf("substituted store transition = %v, want durable fail-closed", err)
	}
	if len(store.transitions) != 2 {
		t.Fatalf("worker continued after substituted store transition: transitions=%d", len(store.transitions))
	}

	store = newDurableNoopTestStore(record)
	final, result, err := RunDurableNoop(store, record.ID, authorization, "")
	if err != nil {
		t.Fatalf("run durable noop: %v", err)
	}
	mutated := result
	mutated.Events = append([]DurableNoopRunEvent(nil), result.Events...)
	mutated.Events[0].StepID = record.Plan.Steps[1].ID
	mutated.Digest = DigestDurableNoopRunResult(mutated)
	if err := VerifyDurableNoopRunResult(record, authorization, final, mutated); err == nil {
		t.Fatal("re-signed event mutation verified")
	}
	mutated = result
	mutated.ProductionWrite = true
	mutated.Digest = DigestDurableNoopRunResult(mutated)
	if err := VerifyDurableNoopRunResult(record, authorization, final, mutated); err == nil {
		t.Fatal("re-signed production-write mutation verified")
	}
}

type durableNoopTestStore struct {
	record       Record
	now          time.Time
	gets         int
	advances     int
	failAt       int
	substituteAt int
	transitions  []Transition
}

func newDurableNoopTestStore(record Record) *durableNoopTestStore {
	return &durableNoopTestStore{record: record, now: record.UpdatedAt}
}

func (store *durableNoopTestStore) GetCompositeReleaseTransaction(id string) (Record, error) {
	store.gets++
	if id != store.record.ID {
		return Record{}, errors.New("not found")
	}
	return store.record, nil
}

func (store *durableNoopTestStore) AdvanceCompositeReleaseTransaction(
	id string,
	expectedRevision int64,
	expectedPlanDigest string,
	expectedFencingEpoch string,
	transition Transition,
) (Record, error) {
	store.advances++
	if store.failAt > 0 && store.advances == store.failAt {
		return Record{}, errors.New("controlled CAS conflict")
	}
	if id != store.record.ID || expectedRevision != store.record.Revision ||
		expectedPlanDigest != store.record.Plan.Digest || expectedFencingEpoch != store.record.Plan.FencingEpoch {
		return Record{}, errors.New("CAS mismatch")
	}
	store.now = store.now.Add(time.Second)
	applied := transition
	if store.substituteAt > 0 && store.advances == store.substituteAt {
		applied.EvidenceDigest = coordinatorDigest("9")
	}
	next, err := ApplyTransition(store.record, applied, store.now)
	if err != nil {
		return Record{}, err
	}
	store.record = next
	store.transitions = append(store.transitions, applied)
	return next, nil
}
