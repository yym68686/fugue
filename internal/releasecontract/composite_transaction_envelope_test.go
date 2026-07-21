package releasecontract

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestCompositeTransactionEnvelopeRoundTripBindsExactNoopRecord(t *testing.T) {
	plan, err := NewCompositeReleasePlan(compositePlanFixture())
	if err != nil {
		t.Fatalf("new plan: %v", err)
	}
	binding := compositeTransactionBindingFixture(plan)
	envelope, err := NewCompositeTransactionEnvelope(plan, binding)
	if err != nil {
		t.Fatalf("new envelope: %v", err)
	}
	encoded, err := MarshalCompositeTransactionEnvelope(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	decoded, err := DecodeAndVerifyCompositeTransactionEnvelope(bytes.NewReader(encoded), binding)
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if !reflect.DeepEqual(decoded, envelope) || decoded.Mode != CompositeTransactionModeNoop || decoded.Digest != DigestCompositeTransactionEnvelope(decoded) {
		t.Fatalf("decoded envelope drifted: %#v", decoded)
	}

	plan.BaseVersions[0].Version = contractDigest("f")
	plan.Steps[0].ActivationIDs[0] = "caller-mutated"
	if decoded.Plan.BaseVersions[0].Version == plan.BaseVersions[0].Version || decoded.Plan.Steps[0].ActivationIDs[0] == plan.Steps[0].ActivationIDs[0] {
		t.Fatal("envelope retained caller-owned plan slices")
	}
}

func TestCompositeTransactionEnvelopeRejectsTrustedBindingMismatchAfterResign(t *testing.T) {
	plan, _ := NewCompositeReleasePlan(compositePlanFixture())
	binding := compositeTransactionBindingFixture(plan)
	envelope, err := NewCompositeTransactionEnvelope(plan, binding)
	if err != nil {
		t.Fatalf("new envelope: %v", err)
	}
	envelope.CoordinatorRecordDigest = contractDigest("e")
	envelope.Digest = DigestCompositeTransactionEnvelope(envelope)
	encoded, err := MarshalCompositeTransactionEnvelope(envelope)
	if err != nil {
		t.Fatalf("marshal re-signed envelope: %v", err)
	}
	if _, err := DecodeAndVerifyCompositeTransactionEnvelope(bytes.NewReader(encoded), binding); err == nil {
		t.Fatal("re-signed record binding mismatch was accepted")
	}
}

func TestCompositeTransactionEnvelopeStrictJSONBoundary(t *testing.T) {
	plan, _ := NewCompositeReleasePlan(compositePlanFixture())
	binding := compositeTransactionBindingFixture(plan)
	envelope, _ := NewCompositeTransactionEnvelope(plan, binding)
	encoded, _ := MarshalCompositeTransactionEnvelope(envelope)

	mutate := func(update func(map[string]any)) []byte {
		t.Helper()
		var root map[string]any
		if err := json.Unmarshal(encoded, &root); err != nil {
			t.Fatal(err)
		}
		update(root)
		result, err := json.Marshal(root)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	cases := [][]byte{
		mutate(func(root map[string]any) { delete(root, "mode") }),
		mutate(func(root map[string]any) { root["unknown"] = true }),
		mutate(func(root map[string]any) { root["Mode"] = root["mode"]; delete(root, "mode") }),
		bytes.Replace(encoded, []byte(`"mode":"noop"`), []byte(`"mode":"noop","mode":"noop"`), 1),
		bytes.Replace(encoded, []byte(`"mode":"noop"`), []byte(`"mode":"\ud800"`), 1),
		append(append([]byte(nil), encoded...), []byte(`{}`)...),
	}
	for index, data := range cases {
		if _, err := DecodeAndVerifyCompositeTransactionEnvelope(bytes.NewReader(data), binding); err == nil {
			t.Fatalf("strict case %d was accepted", index)
		}
	}
	if _, err := DecodeAndVerifyCompositeTransactionEnvelope(nil, binding); err == nil || !strings.Contains(err.Error(), "reader is nil") {
		t.Fatalf("nil reader error = %v", err)
	}
}

func TestCompositeTransactionEnvelopeRejectsNonCanonicalIdentity(t *testing.T) {
	plan, _ := NewCompositeReleasePlan(compositePlanFixture())
	binding := compositeTransactionBindingFixture(plan)
	tests := []func(*CompositeTransactionBinding){
		func(value *CompositeTransactionBinding) { value.RecordID = "Bad" },
		func(value *CompositeTransactionBinding) { value.RecordDigest = "sha256:ABC" },
		func(value *CompositeTransactionBinding) { value.RecordRevision = "01" },
		func(value *CompositeTransactionBinding) { value.Generation = "0" },
		func(value *CompositeTransactionBinding) { value.FencingEpoch = " 11" },
	}
	for index, mutate := range tests {
		candidate := binding
		mutate(&candidate)
		if _, err := NewCompositeTransactionEnvelope(plan, candidate); err == nil {
			t.Fatalf("invalid binding case %d was accepted", index)
		}
	}
}

func compositeTransactionBindingFixture(plan CompositeReleasePlan) CompositeTransactionBinding {
	return CompositeTransactionBinding{
		RecordID: "compositerelease_1", RecordDigest: contractDigest("d"), RecordRevision: "1",
		PlanDigest: plan.Digest, ImageActivationPlanDigest: plan.ImageActivationPlanDigest,
		Generation: plan.Generation, FencingEpoch: plan.FencingEpoch,
	}
}
