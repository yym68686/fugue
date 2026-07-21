package compositecoordinator

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"fugue/internal/releasecontract"
)

func TestDecodeAndAuthorizeNoopBindsExactPreparedRecord(t *testing.T) {
	record := noopAuthorizationRecord(t, "composite-noop-1")
	encoded := noopAuthorizationEnvelope(t, record)
	authorization, err := DecodeAndAuthorizeNoop(record, bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("authorize noop: %v", err)
	}
	if authorization.Mode() != releasecontract.CompositeTransactionModeNoop ||
		authorization.RecordID() != record.ID || authorization.RecordRevision() != record.Revision ||
		authorization.PlanDigest() != record.Plan.Digest || authorization.EnvelopeDigest() == "" {
		t.Fatalf("unexpected authorization: %#v", authorization)
	}
	if err := authorization.Verify(record); err != nil {
		t.Fatalf("verify authorization: %v", err)
	}
	typeOfAuthorization := reflect.TypeOf(authorization)
	for index := 0; index < typeOfAuthorization.NumField(); index++ {
		if typeOfAuthorization.Field(index).IsExported() {
			t.Fatalf("authorization field %q is exported", typeOfAuthorization.Field(index).Name)
		}
	}
}

func TestNoopAuthorizationRejectsRecordAndCredentialMutation(t *testing.T) {
	record := noopAuthorizationRecord(t, "composite-noop-2")
	encoded := noopAuthorizationEnvelope(t, record)
	authorization, err := DecodeAndAuthorizeNoop(record, bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("authorize noop: %v", err)
	}

	other := noopAuthorizationRecord(t, "composite-noop-3")
	if err := authorization.Verify(other); err == nil {
		t.Fatal("authorization verified against a different record")
	}
	mutated := authorization
	mutated.envelopeDigest = coordinatorDigest("e")
	if err := mutated.Verify(record); err == nil {
		t.Fatal("mutated authorization verified")
	}
	mutated = authorization
	mutated.seal[0] ^= 1
	if err := mutated.Verify(record); err == nil {
		t.Fatal("authorization with mutated seal verified")
	}
}

func TestDecodeAndAuthorizeNoopRejectsNonPreparedRecord(t *testing.T) {
	record := noopAuthorizationRecord(t, "composite-noop-4")
	encoded := noopAuthorizationEnvelope(t, record)
	applying, err := ApplyTransition(record, Transition{Kind: TransitionBeginApply}, record.UpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatalf("begin apply: %v", err)
	}
	if _, err := DecodeAndAuthorizeNoop(applying, bytes.NewReader(encoded)); err == nil {
		t.Fatal("non-prepared record was authorized")
	}
}

func TestDecodeAndAuthorizeNoopRejectsReSignedUntrustedRecordBinding(t *testing.T) {
	record := noopAuthorizationRecord(t, "composite-noop-5")
	binding := releasecontract.CompositeTransactionBindingForRecord(record.ID, coordinatorDigest("e"), record.Revision, record.Plan)
	envelope, err := releasecontract.NewCompositeTransactionEnvelope(record.Plan, binding)
	if err != nil {
		t.Fatalf("new mismatched envelope: %v", err)
	}
	encoded, err := releasecontract.MarshalCompositeTransactionEnvelope(envelope)
	if err != nil {
		t.Fatalf("marshal mismatched envelope: %v", err)
	}
	if _, err := DecodeAndAuthorizeNoop(record, bytes.NewReader(encoded)); err == nil {
		t.Fatal("re-signed untrusted record binding was authorized")
	}
}

func noopAuthorizationRecord(t *testing.T, id string) Record {
	t.Helper()
	record, err := NewRecord(id, coordinatorPlanFixture(t), time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("new record: %v", err)
	}
	return record
}

func noopAuthorizationEnvelope(t *testing.T, record Record) []byte {
	t.Helper()
	binding := releasecontract.CompositeTransactionBindingForRecord(record.ID, record.Digest, record.Revision, record.Plan)
	envelope, err := releasecontract.NewCompositeTransactionEnvelope(record.Plan, binding)
	if err != nil {
		t.Fatalf("new envelope: %v", err)
	}
	encoded, err := releasecontract.MarshalCompositeTransactionEnvelope(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return encoded
}
