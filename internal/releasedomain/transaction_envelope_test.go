package releasedomain

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func executableTransactionPlan(domain Domain) Plan {
	return BuildPlan(PlanInput{
		Files:    fileDomains(domain),
		Rendered: renderedDomains(domain),
		Digests:  stableDigestEvidence(),
	})
}

func encodedTransactionEnvelope(t *testing.T, domain Domain) (Plan, TransactionEnvelope, []byte) {
	t.Helper()
	plan := executableTransactionPlan(domain)
	envelope, err := NewTransactionEnvelope(plan, plan.PlanDigest, domain)
	if err != nil {
		t.Fatalf("new transaction envelope: %v", err)
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal transaction envelope: %v", err)
	}
	return plan, envelope, encoded
}

func TestTransactionEnvelopeAuthorizesOnlyFrozenSingleDomain(t *testing.T) {
	plan, envelope, encoded := encodedTransactionEnvelope(t, DomainNodeLocal)
	authorization, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(encoded), plan.PlanDigest, DomainNodeLocal)
	if err != nil {
		t.Fatalf("decode transaction envelope: %v", err)
	}
	if got := authorization.Domain(); got != DomainNodeLocal {
		t.Fatalf("authorization domain = %q", got)
	}
	if got := authorization.PlanDigest(); got != plan.PlanDigest {
		t.Fatalf("authorization plan digest = %q", got)
	}
	if err := authorization.Verify(); err != nil {
		t.Fatalf("verify authorization: %v", err)
	}
	if envelope.APIVersion != TransactionEnvelopeAPIVersion || envelope.Kind != TransactionEnvelopeKind {
		t.Fatalf("unexpected envelope identity: %#v", envelope)
	}

	// The constructor rebuilds the plan instead of retaining mutable caller
	// slices as the persisted authorization input.
	plan.Domains[0] = DomainBackup
	plan.Files.Domains[0] = DomainBackup
	plan.Rendered.Domains[0] = DomainBackup
	if envelope.Plan.Domains[0] != DomainNodeLocal || envelope.Plan.Files.Domains[0] != DomainNodeLocal || envelope.Plan.Rendered.Domains[0] != DomainNodeLocal {
		t.Fatalf("transaction envelope retained caller-owned domain slices: %#v", envelope.Plan)
	}
}

func TestTransactionAuthorizationRejectsZeroValueAndMutation(t *testing.T) {
	if err := (TransactionAuthorization{}).Verify(); err == nil {
		t.Fatal("zero transaction authorization verified")
	}
	plan, _, encoded := encodedTransactionEnvelope(t, DomainNodeLocal)
	authorization, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(encoded), plan.PlanDigest, DomainNodeLocal)
	if err != nil {
		t.Fatalf("decode transaction envelope: %v", err)
	}

	mutatedDomain := authorization
	mutatedDomain.domain = DomainBackup
	if err := mutatedDomain.Verify(); err == nil {
		t.Fatal("authorization with mutated domain verified")
	}
	mutatedDigest := authorization
	mutatedDigest.planDigest = "sha256:" + strings.Repeat("0", 64)
	if err := mutatedDigest.Verify(); err == nil {
		t.Fatal("authorization with mutated digest verified")
	}
	mutatedSeal := authorization
	mutatedSeal.seal[0] ^= 0xff
	if err := mutatedSeal.Verify(); err == nil {
		t.Fatal("authorization with mutated seal verified")
	}
}

func TestTransactionAuthorizationExposesOnlyBoundAccessors(t *testing.T) {
	typeOfAuthorization := reflect.TypeOf(TransactionAuthorization{})
	for index := 0; index < typeOfAuthorization.NumField(); index++ {
		if typeOfAuthorization.Field(index).PkgPath == "" {
			t.Fatalf("authorization field %q is exported", typeOfAuthorization.Field(index).Name)
		}
	}
	for _, methodType := range []reflect.Type{typeOfAuthorization, reflect.PointerTo(typeOfAuthorization)} {
		methods := make([]string, 0, methodType.NumMethod())
		for index := 0; index < methodType.NumMethod(); index++ {
			methods = append(methods, methodType.Method(index).Name)
		}
		if got, want := strings.Join(methods, ","), "Domain,PlanDigest,Verify"; got != want {
			t.Fatalf("exported %s methods = %q, want %q", methodType, got, want)
		}
	}
}

func TestNewTransactionEnvelopeRejectsNonExecutablePlans(t *testing.T) {
	valid := executableTransactionPlan(DomainNodeLocal)
	zero := BuildPlan(PlanInput{
		Files:    FileClassification{Domains: []Domain{}, AllNonRuntime: true},
		Rendered: RenderedClassification{Domains: []Domain{}},
		Digests:  stableDigestEvidence(),
	})
	multiple := BuildPlan(PlanInput{
		Files:    fileDomains(DomainNodeLocal, DomainBackup),
		Rendered: renderedDomains(DomainNodeLocal, DomainBackup),
		Digests:  stableDigestEvidence(),
	})
	unknown := BuildPlan(PlanInput{
		Files:    fileDomains(DomainNodeLocal),
		Rendered: renderedDomains(DomainBackup),
		Digests:  stableDigestEvidence(),
	})
	allNonRuntime := valid
	allNonRuntime.Files.AllNonRuntime = true
	allNonRuntime.PlanDigest = computePlanDigest(allNonRuntime)
	rootUnknown := valid
	rootUnknown.Unknown = []Evidence{{Source: "fixture", Subject: "unknown"}}
	rootUnknown.PlanDigest = computePlanDigest(rootUnknown)
	nestedUnknown := valid
	nestedUnknown.Files.Unknown = []Evidence{{Source: "fixture", Subject: "unknown"}}
	nestedUnknown.PlanDigest = computePlanDigest(nestedUnknown)
	renderedUnknown := valid
	renderedUnknown.Rendered.Unknown = []Evidence{{Source: "fixture", Subject: "unknown"}}
	renderedUnknown.PlanDigest = computePlanDigest(renderedUnknown)

	tests := []struct {
		name   string
		plan   Plan
		digest string
		domain Domain
	}{
		{name: "zero", plan: zero, digest: zero.PlanDigest, domain: DomainNodeLocal},
		{name: "multiple", plan: multiple, digest: multiple.PlanDigest, domain: DomainNodeLocal},
		{name: "unknown", plan: unknown, digest: unknown.PlanDigest, domain: DomainNodeLocal},
		{name: "digest mismatch", plan: valid, digest: "sha256:" + strings.Repeat("0", 64), domain: DomainNodeLocal},
		{name: "domain mismatch", plan: valid, digest: valid.PlanDigest, domain: DomainBackup},
		{name: "unknown domain", plan: valid, digest: valid.PlanDigest, domain: Domain("not-a-domain")},
		{name: "all non-runtime", plan: allNonRuntime, digest: allNonRuntime.PlanDigest, domain: DomainNodeLocal},
		{name: "root unknown", plan: rootUnknown, digest: rootUnknown.PlanDigest, domain: DomainNodeLocal},
		{name: "nested unknown", plan: nestedUnknown, digest: nestedUnknown.PlanDigest, domain: DomainNodeLocal},
		{name: "rendered unknown", plan: renderedUnknown, digest: renderedUnknown.PlanDigest, domain: DomainNodeLocal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewTransactionEnvelope(test.plan, test.digest, test.domain); err == nil {
				t.Fatal("constructor accepted non-executable transaction plan")
			}
		})
	}
}

func TestDecodeTransactionEnvelopeStrictJSONBoundary(t *testing.T) {
	plan, _, encoded := encodedTransactionEnvelope(t, DomainNodeLocal)

	missingRoot := mutateTransactionEnvelopeJSON(t, encoded, func(root map[string]any) {
		delete(root, "expectedDomain")
	})
	missingNested := mutateTransactionEnvelopeJSON(t, encoded, func(root map[string]any) {
		delete(root["plan"].(map[string]any), "files")
	})
	nullPlan := mutateTransactionEnvelopeJSON(t, encoded, func(root map[string]any) {
		root["plan"] = nil
	})
	nullNested := mutateTransactionEnvelopeJSON(t, encoded, func(root map[string]any) {
		root["plan"].(map[string]any)["files"] = nil
	})
	invalidRawUTF8 := bytes.Replace(encoded, []byte("node-local"), []byte{'b', 'a', 'd', '-', 0xff}, 1)
	isolatedSurrogate := bytes.Replace(encoded, []byte("node-local"), []byte(`bad-\ud800`), 1)

	tests := []struct {
		name string
		data []byte
	}{
		{name: "duplicate root field", data: append([]byte(`{"apiVersion":"duplicate",`), encoded[1:]...)},
		{name: "duplicate nested field", data: bytes.Replace(encoded, []byte(`"plan":{`), []byte(`"plan":{"kind":"duplicate",`), 1)},
		{name: "unknown root field", data: append([]byte(`{"extra":true,`), encoded[1:]...)},
		{name: "unknown nested field", data: bytes.Replace(encoded, []byte(`"plan":{`), []byte(`"plan":{"extra":true,`), 1)},
		{name: "missing root field", data: missingRoot},
		{name: "missing nested field", data: missingNested},
		{name: "case alias", data: bytes.Replace(encoded, []byte(`"apiVersion"`), []byte(`"APIVersion"`), 1)},
		{name: "null root", data: []byte("null")},
		{name: "null plan", data: nullPlan},
		{name: "null nested object", data: nullNested},
		{name: "invalid raw utf8", data: invalidRawUTF8},
		{name: "isolated unicode surrogate", data: isolatedSurrogate},
		{name: "trailing value", data: append(append([]byte(nil), encoded...), []byte("\n{}")...)},
		{name: "unsupported api version", data: mutateTransactionEnvelopeJSON(t, encoded, func(root map[string]any) { root["apiVersion"] = "other/v1" })},
		{name: "unsupported kind", data: mutateTransactionEnvelopeJSON(t, encoded, func(root map[string]any) { root["kind"] = "OtherEnvelope" })},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(test.data), plan.PlanDigest, DomainNodeLocal); err == nil {
				t.Fatal("strict decoder accepted invalid transaction envelope")
			}
		})
	}
}

func TestDecodeTransactionEnvelopeRejectsReSignedNestedIdentity(t *testing.T) {
	for _, field := range []string{"apiVersion", "kind"} {
		t.Run(field, func(t *testing.T) {
			plan := executableTransactionPlan(DomainNodeLocal)
			switch field {
			case "apiVersion":
				plan.APIVersion = "other/v1"
			case "kind":
				plan.Kind = "OtherPlan"
			}
			plan.PlanDigest = computePlanDigest(plan)
			if err := VerifyPlanDigest(plan); err != nil {
				t.Fatalf("re-signed nested identity control: %v", err)
			}
			envelope := TransactionEnvelope{
				APIVersion:     TransactionEnvelopeAPIVersion,
				Kind:           TransactionEnvelopeKind,
				PlanDigest:     plan.PlanDigest,
				ExpectedDomain: DomainNodeLocal,
				Plan:           plan,
			}
			encoded, err := json.Marshal(envelope)
			if err != nil {
				t.Fatalf("marshal re-signed nested identity: %v", err)
			}
			if _, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(encoded), plan.PlanDigest, DomainNodeLocal); err == nil {
				t.Fatal("decoder accepted a re-signed unsupported nested identity")
			}
		})
	}
}

func TestDecodeTransactionEnvelopeBindsThreeDigestAndDomainSources(t *testing.T) {
	plan, _, encoded := encodedTransactionEnvelope(t, DomainNodeLocal)
	otherDigest := "sha256:" + strings.Repeat("0", 64)
	backupPlan := executableTransactionPlan(DomainBackup)
	validNestedDomainMismatch := TransactionEnvelope{
		APIVersion:     TransactionEnvelopeAPIVersion,
		Kind:           TransactionEnvelopeKind,
		PlanDigest:     backupPlan.PlanDigest,
		ExpectedDomain: DomainNodeLocal,
		Plan:           backupPlan,
	}
	validNestedDomainMismatchJSON, err := json.Marshal(validNestedDomainMismatch)
	if err != nil {
		t.Fatalf("marshal valid nested-domain mismatch: %v", err)
	}

	tests := []struct {
		name          string
		data          []byte
		trustedDigest string
		trustedDomain Domain
	}{
		{name: "trusted digest differs", data: encoded, trustedDigest: otherDigest, trustedDomain: DomainNodeLocal},
		{name: "envelope digest differs", data: mutateTransactionEnvelopeJSON(t, encoded, func(root map[string]any) { root["planDigest"] = otherDigest }), trustedDigest: plan.PlanDigest, trustedDomain: DomainNodeLocal},
		{name: "nested digest differs", data: mutateTransactionEnvelopeJSON(t, encoded, func(root map[string]any) { root["plan"].(map[string]any)["planDigest"] = otherDigest }), trustedDigest: plan.PlanDigest, trustedDomain: DomainNodeLocal},
		{name: "trusted domain differs", data: encoded, trustedDigest: plan.PlanDigest, trustedDomain: DomainBackup},
		{name: "envelope domain differs", data: mutateTransactionEnvelopeJSON(t, encoded, func(root map[string]any) { root["expectedDomain"] = string(DomainBackup) }), trustedDigest: plan.PlanDigest, trustedDomain: DomainNodeLocal},
		{name: "nested domain differs", data: validNestedDomainMismatchJSON, trustedDigest: backupPlan.PlanDigest, trustedDomain: DomainNodeLocal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(test.data), test.trustedDigest, test.trustedDomain); err == nil {
				t.Fatal("decoder accepted an unbound transaction envelope")
			}
		})
	}

	for _, field := range []string{"domains", "files.domains", "rendered.domains"} {
		t.Run("re-signed "+field+" differs", func(t *testing.T) {
			mutated := executableTransactionPlan(DomainNodeLocal)
			switch field {
			case "domains":
				mutated.Domains = []Domain{DomainBackup}
			case "files.domains":
				mutated.Files.Domains = []Domain{DomainBackup}
			case "rendered.domains":
				mutated.Rendered.Domains = []Domain{DomainBackup}
			}
			mutated.PlanDigest = computePlanDigest(mutated)
			if err := VerifyPlanDigest(mutated); err != nil {
				t.Fatalf("re-signed domain mutation control: %v", err)
			}
			envelope := TransactionEnvelope{
				APIVersion:     TransactionEnvelopeAPIVersion,
				Kind:           TransactionEnvelopeKind,
				PlanDigest:     mutated.PlanDigest,
				ExpectedDomain: DomainNodeLocal,
				Plan:           mutated,
			}
			encodedMutation, err := json.Marshal(envelope)
			if err != nil {
				t.Fatalf("marshal re-signed domain mutation: %v", err)
			}
			if _, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(encodedMutation), mutated.PlanDigest, DomainNodeLocal); err == nil {
				t.Fatal("decoder accepted a re-signed mismatched domain slice")
			}
		})
	}
}

func TestDecodeTransactionEnvelopeRejectsDigestAliases(t *testing.T) {
	plan, _, encoded := encodedTransactionEnvelope(t, DomainNodeLocal)
	tests := []string{
		"",
		"sha256:abcd",
		"SHA256:" + strings.Repeat("0", 64),
		"sha256:" + strings.Repeat("A", 64),
		"sha256:" + strings.Repeat("g", 64),
		"sha256:" + strings.Repeat("0", 63),
		"sha256:" + strings.Repeat("0", 65),
		"sha256:" + strings.Repeat("0", 63) + "\xff",
	}
	for _, digest := range tests {
		t.Run(strings.ReplaceAll(digest, "/", "_"), func(t *testing.T) {
			if _, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(encoded), digest, DomainNodeLocal); err == nil {
				t.Fatalf("decoder accepted non-canonical trusted digest %q", digest)
			}
		})
	}

	uppercaseEnvelope := mutateTransactionEnvelopeJSON(t, encoded, func(root map[string]any) {
		root["planDigest"] = "sha256:" + strings.Repeat("A", 64)
	})
	if _, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(uppercaseEnvelope), plan.PlanDigest, DomainNodeLocal); err == nil {
		t.Fatal("decoder accepted uppercase envelope digest")
	}
	uppercaseNested := mutateTransactionEnvelopeJSON(t, encoded, func(root map[string]any) {
		root["plan"].(map[string]any)["planDigest"] = "sha256:" + strings.Repeat("A", 64)
	})
	if _, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(uppercaseNested), plan.PlanDigest, DomainNodeLocal); err == nil {
		t.Fatal("decoder accepted uppercase nested plan digest")
	}
}

func TestDecodeTransactionEnvelopeRebuildsPlanAgainstSelfDigestForgery(t *testing.T) {
	plan := executableTransactionPlan(DomainNodeLocal)
	plan.SelectedDomain = DomainBackup
	plan.Domains = []Domain{DomainBackup}
	plan.Files.Domains = []Domain{DomainBackup}
	plan.Rendered.Domains = []Domain{DomainBackup}
	// The underlying evidence remains node-local. Re-signing the untrusted
	// derived fields must not turn them into a valid backup authorization.
	plan.PlanDigest = computePlanDigest(plan)
	envelope := TransactionEnvelope{
		APIVersion:     TransactionEnvelopeAPIVersion,
		Kind:           TransactionEnvelopeKind,
		PlanDigest:     plan.PlanDigest,
		ExpectedDomain: DomainBackup,
		Plan:           plan,
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal forged envelope: %v", err)
	}
	if err := VerifyPlanDigest(plan); err != nil {
		t.Fatalf("forgery control must have a valid self digest: %v", err)
	}
	if _, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(encoded), plan.PlanDigest, DomainBackup); err == nil {
		t.Fatal("decoder accepted self-digest forgery of the derived release domain")
	}
}

func TestDecodeTransactionEnvelopeRequiresEmptyUnknownEvidence(t *testing.T) {
	plan, _, encoded := encodedTransactionEnvelope(t, DomainNodeLocal)
	explicitEmpty := mutateTransactionEnvelopeJSON(t, encoded, func(root map[string]any) {
		nested := root["plan"].(map[string]any)
		nested["unknown"] = []any{}
		nested["files"].(map[string]any)["unknown"] = []any{}
		nested["rendered"].(map[string]any)["unknown"] = []any{}
	})
	if _, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(explicitEmpty), plan.PlanDigest, DomainNodeLocal); err != nil {
		t.Fatalf("decoder rejected explicitly empty unknown arrays: %v", err)
	}

	nonEmpty := mutateTransactionEnvelopeJSON(t, encoded, func(root map[string]any) {
		root["plan"].(map[string]any)["unknown"] = []any{map[string]any{"source": "fixture", "subject": "unknown"}}
	})
	if _, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(nonEmpty), plan.PlanDigest, DomainNodeLocal); err == nil {
		t.Fatal("decoder accepted non-empty unknown evidence")
	}
}

func TestDecodeTransactionEnvelopeReaderAndSizeBoundary(t *testing.T) {
	plan, _, encoded := encodedTransactionEnvelope(t, DomainNodeLocal)
	t.Run("nil reader", func(t *testing.T) {
		if _, err := DecodeAndVerifyTransactionEnvelope(nil, plan.PlanDigest, DomainNodeLocal); err == nil || !strings.Contains(err.Error(), "reader is nil") {
			t.Fatalf("nil reader error = %v", err)
		}
	})
	t.Run("typed nil reader", func(t *testing.T) {
		var reader *bytes.Reader
		if _, err := DecodeAndVerifyTransactionEnvelope(reader, plan.PlanDigest, DomainNodeLocal); err == nil || !strings.Contains(err.Error(), "reader is nil") {
			t.Fatalf("typed nil reader error = %v", err)
		}
	})
	t.Run("reader error", func(t *testing.T) {
		readErr := errors.New("fixture read failure")
		if _, err := DecodeAndVerifyTransactionEnvelope(errorReader{err: readErr}, plan.PlanDigest, DomainNodeLocal); !errors.Is(err, readErr) {
			t.Fatalf("reader error = %v, want wrapped %v", err, readErr)
		}
	})
	t.Run("exact limit", func(t *testing.T) {
		exact := append(append([]byte(nil), encoded...), bytes.Repeat([]byte(" "), maxTransactionEnvelopeBytes-len(encoded))...)
		if _, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(exact), plan.PlanDigest, DomainNodeLocal); err != nil {
			t.Fatalf("decode exact-limit envelope: %v", err)
		}
	})
	t.Run("over limit", func(t *testing.T) {
		over := append(append([]byte(nil), encoded...), bytes.Repeat([]byte(" "), maxTransactionEnvelopeBytes-len(encoded)+1)...)
		if _, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(over), plan.PlanDigest, DomainNodeLocal); err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("over-limit envelope error = %v", err)
		}
	})
}

func mutateTransactionEnvelopeJSON(t *testing.T, encoded []byte, mutate func(map[string]any)) []byte {
	t.Helper()
	var root map[string]any
	if err := json.Unmarshal(encoded, &root); err != nil {
		t.Fatalf("decode transaction envelope mutation fixture: %v", err)
	}
	mutate(root)
	mutated, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("encode transaction envelope mutation fixture: %v", err)
	}
	return mutated
}
