package releasedomain

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestPublicDataPlaneSecretFreeRenderBindsLookupAndLeaksNoPayload(t *testing.T) {
	fixture := newPublicDataPlaneAdoptionFixture(t)
	spec, err := LoadOwnership(bytes.NewReader(fixture.ownership))
	if err != nil {
		t.Fatal(err)
	}
	canonical, witness, err := CanonicalizePublicDataPlaneSecretFreeManifest(
		fixture.rawBase, spec, "fugue-system", fixture.secretHMACKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	encodedWitness := mustJSON(t, witness)
	for _, secret := range []string{
		"workload-signing-secret", "bundle-signing-secret", "edge-tls-secret",
		"postgres-secret", "platform-signing-secret",
	} {
		if bytes.Contains(canonical, []byte(secret)) || bytes.Contains(encodedWitness, []byte(secret)) {
			t.Fatalf("secret payload %q leaked into canonical evidence", secret)
		}
	}
	if !bytes.Contains(canonical, []byte(secretRedactedStringDataValue)) ||
		!strings.HasPrefix(witness.PayloadHMAC, "hmac-sha256:") {
		t.Fatalf("secret-free evidence is not redacted and HMAC-bound")
	}
	lookup, err := DecodePublicDataPlaneSecretLookupWitness(fixture.input.SecretLookupWitness)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyPublicDataPlaneSecretWitnessBinding(lookup, witness); err != nil {
		t.Fatalf("lookup/render binding failed: %v", err)
	}
}

func TestPublicDataPlaneSecretLookupFailsClosedAndDriftInvalidatesPrewrite(t *testing.T) {
	fixture := newPublicDataPlaneAdoptionFixture(t)
	names := PublicDataPlaneSecretLookupNames{
		Config: "fugue-fugue-config", ControlPlaneDB: "fugue-fugue-control-plane-postgres-app",
		PlatformIdentity: "fugue-fugue-platform-component-identity",
	}
	var base map[string]any
	if err := json.Unmarshal(fixture.lookupSnapshot, &base); err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(map[string]any){
		"absent": func(document map[string]any) {
			document["items"] = document["items"].([]any)[:2]
		},
		"foreign": func(document map[string]any) {
			metadata := document["items"].([]any)[0].(map[string]any)["metadata"].(map[string]any)
			metadata["labels"].(map[string]any)["app.kubernetes.io/managed-by"] = "foreign"
		},
		"deleting": func(document map[string]any) {
			metadata := document["items"].([]any)[0].(map[string]any)["metadata"].(map[string]any)
			metadata["deletionTimestamp"] = "2026-08-01T00:00:00Z"
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			document := cloneJSONDocument(t, base)
			mutate(document)
			if _, err := BuildPublicDataPlaneSecretLookupWitness(
				mustJSON(t, document), "fugue", "fugue-system", names,
			); err == nil {
				t.Fatal("unsafe lookup evidence was accepted")
			}
		})
	}

	plan, restore, err := BuildPublicDataPlaneAdoptionPlan(fixture.input)
	if err != nil {
		t.Fatal(err)
	}
	drifted := cloneJSONDocument(t, base)
	metadata := drifted["items"].([]any)[0].(map[string]any)["metadata"].(map[string]any)
	metadata["uid"] = "replacement-uid"
	metadata["resourceVersion"] = "replacement-rv"
	witness, err := BuildPublicDataPlaneSecretLookupWitness(
		mustJSON(t, drifted), "fugue", "fugue-system", names,
	)
	if err != nil {
		t.Fatal(err)
	}
	prewrite := fixture.input
	prewrite.SecretLookupWitness = mustJSON(t, witness)
	if err := VerifyPublicDataPlaneAdoptionPrewrite(plan, restore, prewrite); err == nil {
		t.Fatal("prewrite accepted Secret UID/resourceVersion drift")
	}
}

func TestPublicDataPlaneSecretRenderDriftFailsBeforeApply(t *testing.T) {
	fixture := newPublicDataPlaneAdoptionFixture(t)
	plan, _, err := BuildPublicDataPlaneAdoptionPlan(fixture.input)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := NewPublicDataPlaneAdoptionTransaction(plan)
	if err != nil {
		t.Fatal(err)
	}
	drifted := bytes.Replace(fixture.rawBase, []byte("postgres-secret"), []byte("replacement-secret"), 1)
	if bytes.Equal(drifted, fixture.rawBase) {
		t.Fatal("test Secret mutation did not apply")
	}
	if _, err := RenderPublicDataPlaneAdoptionTransactionTarget(
		drifted, fixture.ownership, "fugue-system", envelope, fixture.secretHMACKey,
	); err == nil {
		t.Fatal("apply post-renderer accepted Secret payload drift")
	} else if strings.Contains(err.Error(), "postgres-secret") || strings.Contains(err.Error(), "replacement-secret") {
		t.Fatal("Secret payload leaked through a fail-closed error")
	}

	spec, err := LoadOwnership(bytes.NewReader(fixture.ownership))
	if err != nil {
		t.Fatal(err)
	}
	_, driftedWitness, err := CanonicalizePublicDataPlaneSecretFreeManifest(
		drifted, spec, "fugue-system", fixture.secretHMACKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	input := fixture.input
	input.RepeatedSecretRenderWitness = mustJSON(t, driftedWitness)
	if _, _, err := BuildPublicDataPlaneAdoptionPlan(input); err == nil {
		t.Fatal("plan accepted two server render rounds with different Secret bytes")
	}
}
