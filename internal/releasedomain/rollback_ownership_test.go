package releasedomain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type rollbackOwnershipFixture struct {
	input RollbackOwnershipInput
	plan  Plan
}

func newRollbackOwnershipFixture(t *testing.T, domain Domain) rollbackOwnershipFixture {
	t.Helper()
	if _, err := ParseDomain(string(domain)); err != nil {
		t.Fatalf("fixture domain: %v", err)
	}

	objectRules := make([]ObjectRule, 0, len(KnownDomains()))
	for _, knownDomain := range KnownDomains() {
		objectRules = append(objectRules, ObjectRule{
			ID:        "fixture-" + string(knownDomain),
			Domain:    knownDomain,
			Version:   "v1",
			Kind:      "ConfigMap",
			Scope:     ScopeNamespaced,
			Namespace: "${releaseNamespace}",
			Name:      "fixture-" + string(knownDomain),
			RequiredLabels: map[string]string{
				"test.fugue.dev/domain": string(knownDomain),
			},
		})
	}
	spec := &OwnershipSpec{
		APIVersion:       OwnershipAPIVersion,
		Kind:             OwnershipKind,
		Domains:          KnownDomains(),
		RequiredBindings: []string{"releaseName", "releaseNamespace"},
		ObjectRules:      objectRules,
	}
	if err := spec.Validate(); err != nil {
		t.Fatalf("validate fixture ownership: %v", err)
	}
	ownership, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal fixture ownership: %v", err)
	}
	manifest := func(version string) []byte {
		raw := []byte(fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: fixture-%s
  namespace: fugue-system
  labels:
    test.fugue.dev/domain: %s
data:
  version: %s
`, domain, domain, version))
		canonical, canonicalErr := CanonicalizeRenderedManifest(raw, spec, "fugue-system")
		if canonicalErr != nil {
			t.Fatalf("canonicalize fixture manifest: %v", canonicalErr)
		}
		return canonical
	}
	baseManifest := manifest("base")
	targetManifest := manifest("target")
	bindings := map[string]string{
		"releaseName":      "fugue",
		"releaseNamespace": "fugue-system",
	}
	classificationContext, err := NewClassificationContextEvidence("fugue-system", bindings, false)
	if err != nil {
		t.Fatalf("fixture classification context: %v", err)
	}
	rendered := ClassifyRendered(baseManifest, targetManifest, spec, RenderedOptions{
		DefaultNamespace: "fugue-system",
		Bindings:         bindings,
	})
	plan := BuildPlan(PlanInput{
		Files: FileClassification{
			Domains:  []Domain{domain},
			Evidence: []Evidence{{Source: "changed-file", Subject: "fixture", Domains: []Domain{domain}}},
		},
		Rendered: rendered,
		Digests: DigestEvidence{
			Base:                   "41",
			Target:                 "42",
			Live:                   "41",
			BaseManifest:           rollbackDigestBytes(baseManifest),
			TargetManifest:         rollbackDigestBytes(targetManifest),
			RepeatedTargetManifest: rollbackDigestBytes(targetManifest),
			Ownership:              rollbackDigestBytes(ownership),
			ChangedFiles:           rollbackDigestBytes([]byte("changed-file-evidence")),
			ClassificationContext:  classificationContext,
		},
	})
	if plan.Result != OutcomeSingle || plan.SelectedDomain != domain {
		t.Fatalf("fixture plan = %#v", plan)
	}
	envelope, err := NewTransactionEnvelope(plan, plan.PlanDigest, domain)
	if err != nil {
		t.Fatalf("new fixture transaction envelope: %v", err)
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal fixture transaction envelope: %v", err)
	}
	transaction, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(encoded), plan.PlanDigest, domain)
	if err != nil {
		t.Fatalf("decode fixture transaction envelope: %v", err)
	}
	return rollbackOwnershipFixture{
		plan: plan,
		input: RollbackOwnershipInput{
			Transaction: transaction,
			Binding: ExecutionBinding{
				ReleaseName:       "fugue",
				ReleaseNamespace:  "fugue-system",
				BaseRevision:      "41",
				TargetRevision:    "42",
				UpgradeArgvDigest: rollbackDigestBytes([]byte("helm\x00upgrade\x00fugue\x00")),
				HooksPolicy:       HooksPolicyNoHooks,
			},
			Ownership:              ownership,
			BaseManifest:           baseManifest,
			TargetManifest:         targetManifest,
			RepeatedTargetManifest: append([]byte(nil), targetManifest...),
		},
	}
}

func TestVerifyRollbackOwnershipSealsExactExecutionBoundary(t *testing.T) {
	fixture := newRollbackOwnershipFixture(t, DomainNodeLocal)
	authorization, err := VerifyRollbackOwnership(fixture.input)
	if err != nil {
		t.Fatalf("verify rollback ownership: %v", err)
	}
	if err := authorization.Verify(); err != nil {
		t.Fatalf("verify execution authorization: %v", err)
	}
	if authorization.Domain() != DomainNodeLocal || authorization.PlanDigest() != fixture.plan.PlanDigest {
		t.Fatalf("authorization identity = %q %q", authorization.Domain(), authorization.PlanDigest())
	}
	if got := authorization.Binding(); !reflect.DeepEqual(got, fixture.input.Binding) {
		t.Fatalf("authorization binding = %#v, want %#v", got, fixture.input.Binding)
	}
	evidence := authorization.Evidence()
	if evidence.Domain != DomainNodeLocal ||
		evidence.OwnershipDigest != fixture.plan.Digests.Ownership ||
		evidence.ClassificationContextDigest != fixture.plan.Digests.ClassificationContext.Digest ||
		evidence.BaseManifestDigest != fixture.plan.Digests.BaseManifest ||
		evidence.TargetManifestDigest != fixture.plan.Digests.TargetManifest ||
		evidence.RepeatedTargetManifestDigest != fixture.plan.Digests.RepeatedTargetManifest ||
		evidence.ForwardEvidenceDigest != evidence.ReverseEvidenceDigest {
		t.Fatalf("authorization evidence = %#v", evidence)
	}

	repeated, err := VerifyRollbackOwnership(fixture.input)
	if err != nil {
		t.Fatalf("repeat rollback ownership proof: %v", err)
	}
	if authorization.seal != repeated.seal {
		t.Fatal("identical proof produced a different execution seal")
	}

	// The sealed result retains no mutable input. Destroy every caller-owned
	// artifact after authorization and ensure the bounded seal remains valid.
	for _, artifact := range [][]byte{
		fixture.input.Ownership,
		fixture.input.BaseManifest,
		fixture.input.TargetManifest,
		fixture.input.RepeatedTargetManifest,
	} {
		for index := range artifact {
			artifact[index] ^= 0xff
		}
	}
	if err := authorization.Verify(); err != nil {
		t.Fatalf("authorization retained mutable proof input: %v", err)
	}
}

func TestExecutionAuthorizationIsOpaqueAndEveryBoundFieldIsSealed(t *testing.T) {
	fixture := newRollbackOwnershipFixture(t, DomainNodeLocal)
	authorization, err := VerifyRollbackOwnership(fixture.input)
	if err != nil {
		t.Fatalf("verify rollback ownership: %v", err)
	}
	typeOfAuthorization := reflect.TypeOf(ExecutionAuthorization{})
	for index := 0; index < typeOfAuthorization.NumField(); index++ {
		field := typeOfAuthorization.Field(index)
		if field.PkgPath == "" {
			t.Fatalf("authorization field %q is exported", field.Name)
		}
		if field.Type == reflect.TypeOf(Plan{}) || field.Type.Kind() == reflect.Slice || field.Type.Kind() == reflect.Map {
			t.Fatalf("authorization retains mutable or raw field %q of type %s", field.Name, field.Type)
		}
	}
	methods := make([]string, 0, typeOfAuthorization.NumMethod())
	for index := 0; index < typeOfAuthorization.NumMethod(); index++ {
		methods = append(methods, typeOfAuthorization.Method(index).Name)
	}
	if got, want := strings.Join(methods, ","), "Binding,Domain,Evidence,PlanDigest,Verify"; got != want {
		t.Fatalf("exported execution authorization methods = %q, want %q", got, want)
	}

	tests := []struct {
		name   string
		mutate func(*ExecutionAuthorization)
	}{
		{name: "domain", mutate: func(value *ExecutionAuthorization) { value.domain = DomainBackup }},
		{name: "plan digest", mutate: func(value *ExecutionAuthorization) { value.planDigest = "sha256:" + strings.Repeat("0", 64) }},
		{name: "release name", mutate: func(value *ExecutionAuthorization) { value.binding.ReleaseName = "other" }},
		{name: "release namespace", mutate: func(value *ExecutionAuthorization) { value.binding.ReleaseNamespace = "other-system" }},
		{name: "base revision", mutate: func(value *ExecutionAuthorization) { value.binding.BaseRevision = "other-base" }},
		{name: "target revision", mutate: func(value *ExecutionAuthorization) { value.binding.TargetRevision = "other-target" }},
		{name: "upgrade argv digest", mutate: func(value *ExecutionAuthorization) {
			value.binding.UpgradeArgvDigest = "sha256:" + strings.Repeat("0", 64)
		}},
		{name: "hooks policy", mutate: func(value *ExecutionAuthorization) { value.binding.HooksPolicy = "hooks" }},
		{name: "evidence domain", mutate: func(value *ExecutionAuthorization) { value.evidence.Domain = DomainBackup }},
		{name: "ownership digest", mutate: func(value *ExecutionAuthorization) {
			value.evidence.OwnershipDigest = "sha256:" + strings.Repeat("0", 64)
		}},
		{name: "context digest", mutate: func(value *ExecutionAuthorization) {
			value.evidence.ClassificationContextDigest = "sha256:" + strings.Repeat("0", 64)
		}},
		{name: "base manifest digest", mutate: func(value *ExecutionAuthorization) {
			value.evidence.BaseManifestDigest = "sha256:" + strings.Repeat("0", 64)
		}},
		{name: "target manifest digest", mutate: func(value *ExecutionAuthorization) {
			value.evidence.TargetManifestDigest = "sha256:" + strings.Repeat("0", 64)
		}},
		{name: "repeated target digest", mutate: func(value *ExecutionAuthorization) {
			value.evidence.RepeatedTargetManifestDigest = "sha256:" + strings.Repeat("0", 64)
		}},
		{name: "forward evidence digest", mutate: func(value *ExecutionAuthorization) {
			value.evidence.ForwardEvidenceDigest = "sha256:" + strings.Repeat("0", 64)
		}},
		{name: "reverse evidence digest", mutate: func(value *ExecutionAuthorization) {
			value.evidence.ReverseEvidenceDigest = "sha256:" + strings.Repeat("0", 64)
		}},
		{name: "seal", mutate: func(value *ExecutionAuthorization) { value.seal[0] ^= 0xff }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := authorization
			test.mutate(&mutated)
			if err := mutated.Verify(); err == nil {
				t.Fatal("mutated execution authorization verified")
			}
		})
	}
	if err := (ExecutionAuthorization{}).Verify(); err == nil {
		t.Fatal("zero execution authorization verified")
	}
}

func TestExecutionBindingRequiresAdjacentCanonicalHelmRevisions(t *testing.T) {
	fixture := newRollbackOwnershipFixture(t, DomainNodeLocal)
	tests := []struct {
		name   string
		base   string
		target string
	}{
		{name: "empty base", base: "", target: "1"},
		{name: "zero base", base: "0", target: "1"},
		{name: "leading zero base", base: "01", target: "2"},
		{name: "negative base", base: "-1", target: "1"},
		{name: "non decimal base", base: "one", target: "2"},
		{name: "empty target", base: "1", target: ""},
		{name: "zero target", base: "1", target: "0"},
		{name: "leading zero target", base: "1", target: "02"},
		{name: "non adjacent target", base: "41", target: "43"},
		{name: "same target", base: "41", target: "41"},
		{name: "base overflow", base: "2147483647", target: "2147483648"},
		{name: "target overflow", base: "2147483646", target: "2147483648"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := fixture.input
			input.Binding.BaseRevision = test.base
			input.Binding.TargetRevision = test.target
			if _, err := VerifyRollbackOwnership(input); err == nil {
				t.Fatal("non-canonical or non-adjacent revision pair was authorized")
			}
		})
	}

	input := fixture.input
	input.Binding.BaseRevision = "2147483646"
	input.Binding.TargetRevision = "2147483647"
	input.Transaction.plan.Digests.Base = input.Binding.BaseRevision
	input.Transaction.plan.Digests.Live = input.Binding.BaseRevision
	input.Transaction.plan.Digests.Target = input.Binding.TargetRevision
	input.Transaction.plan.PlanDigest = computePlanDigest(input.Transaction.plan)
	input.Transaction.planDigest = input.Transaction.plan.PlanDigest
	input.Transaction.seal = transactionAuthorizationSeal(input.Transaction.planDigest, input.Transaction.domain)
	if _, err := VerifyRollbackOwnership(input); err != nil {
		t.Fatalf("maximum adjacent revision pair was rejected: %v", err)
	}
}

func TestVerifyRollbackOwnershipRejectsTamperAndBindingMismatch(t *testing.T) {
	newInput := func() RollbackOwnershipInput {
		return newRollbackOwnershipFixture(t, DomainNodeLocal).input
	}
	tests := []struct {
		name   string
		mutate func(*RollbackOwnershipInput)
	}{
		{name: "zero transaction", mutate: func(input *RollbackOwnershipInput) { input.Transaction = TransactionAuthorization{} }},
		{name: "ownership bytes", mutate: func(input *RollbackOwnershipInput) { input.Ownership = append(input.Ownership, '\n') }},
		{name: "base manifest", mutate: func(input *RollbackOwnershipInput) { input.BaseManifest[0] ^= 1 }},
		{name: "target manifest", mutate: func(input *RollbackOwnershipInput) { input.TargetManifest[0] ^= 1 }},
		{name: "repeated target manifest", mutate: func(input *RollbackOwnershipInput) { input.RepeatedTargetManifest[0] ^= 1 }},
		{name: "release name", mutate: func(input *RollbackOwnershipInput) { input.Binding.ReleaseName = "other" }},
		{name: "release namespace", mutate: func(input *RollbackOwnershipInput) { input.Binding.ReleaseNamespace = "other-system" }},
		{name: "base revision", mutate: func(input *RollbackOwnershipInput) { input.Binding.BaseRevision = "other-base" }},
		{name: "target revision", mutate: func(input *RollbackOwnershipInput) { input.Binding.TargetRevision = "other-target" }},
		{name: "missing argv digest", mutate: func(input *RollbackOwnershipInput) { input.Binding.UpgradeArgvDigest = "" }},
		{name: "uppercase argv digest", mutate: func(input *RollbackOwnershipInput) {
			input.Binding.UpgradeArgvDigest = "sha256:" + strings.Repeat("A", 64)
		}},
		{name: "hooks enabled", mutate: func(input *RollbackOwnershipInput) { input.Binding.HooksPolicy = "hooks" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := newInput()
			test.mutate(&input)
			if _, err := VerifyRollbackOwnership(input); err == nil {
				t.Fatal("tampered rollback ownership input authorized execution")
			}
		})
	}
}

func TestVerifyRollbackOwnershipRejectsNonCanonicalManifest(t *testing.T) {
	fixture := newRollbackOwnershipFixture(t, DomainNodeLocal)
	// This is semantically the same base object, but JSON is not the canonical
	// YAML representation. Build an otherwise valid transaction around its
	// exact bytes so the rejection specifically exercises canonicality.
	nonCanonical := []byte(`{"kind":"ConfigMap","apiVersion":"v1","metadata":{"namespace":"fugue-system","name":"fixture-node-local","labels":{"test.fugue.dev/domain":"node-local"}},"data":{"version":"base"}}`)
	plan := fixture.plan
	plan.Digests.BaseManifest = rollbackDigestBytes(nonCanonical)
	plan = BuildPlan(PlanInput{Files: plan.Files, Rendered: plan.Rendered, Digests: plan.Digests})
	envelope, err := NewTransactionEnvelope(plan, plan.PlanDigest, DomainNodeLocal)
	if err != nil {
		t.Fatalf("new non-canonical fixture envelope: %v", err)
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal non-canonical fixture envelope: %v", err)
	}
	transaction, err := DecodeAndVerifyTransactionEnvelope(bytes.NewReader(encoded), plan.PlanDigest, DomainNodeLocal)
	if err != nil {
		t.Fatalf("decode non-canonical fixture envelope: %v", err)
	}
	fixture.input.Transaction = transaction
	fixture.input.BaseManifest = nonCanonical
	if _, err := VerifyRollbackOwnership(fixture.input); err == nil || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("non-canonical manifest error = %v", err)
	}
}

func TestVerifyRollbackOwnershipEnforcesNoHooksInputsAndContext(t *testing.T) {
	t.Run("classification context cannot ignore hooks", func(t *testing.T) {
		fixture := newRollbackOwnershipFixture(t, DomainNodeLocal)
		context, err := NewClassificationContextEvidence(
			"fugue-system",
			map[string]string{"releaseName": "fugue", "releaseNamespace": "fugue-system"},
			true,
		)
		if err != nil {
			t.Fatalf("new ignore-hooks context: %v", err)
		}
		plan := fixture.plan
		plan.Digests.ClassificationContext = context
		plan = BuildPlan(PlanInput{Files: plan.Files, Rendered: plan.Rendered, Digests: plan.Digests})
		envelope, err := NewTransactionEnvelope(plan, plan.PlanDigest, DomainNodeLocal)
		if err != nil {
			t.Fatalf("new ignore-hooks envelope: %v", err)
		}
		encoded, err := json.Marshal(envelope)
		if err != nil {
			t.Fatalf("marshal ignore-hooks envelope: %v", err)
		}
		fixture.input.Transaction, err = DecodeAndVerifyTransactionEnvelope(bytes.NewReader(encoded), plan.PlanDigest, DomainNodeLocal)
		if err != nil {
			t.Fatalf("decode ignore-hooks envelope: %v", err)
		}
		if _, err := VerifyRollbackOwnership(fixture.input); err == nil || !strings.Contains(err.Error(), "no-hooks") {
			t.Fatalf("ignore-hooks context error = %v", err)
		}
	})

	t.Run("canonical manifests cannot retain hooks", func(t *testing.T) {
		fixture := newRollbackOwnershipFixture(t, DomainNodeLocal)
		spec, err := LoadOwnership(bytes.NewReader(fixture.input.Ownership))
		if err != nil {
			t.Fatalf("load fixture ownership: %v", err)
		}
		hook := []byte(`apiVersion: v1
kind: ConfigMap
metadata:
  name: unchanged-test-hook
  namespace: fugue-system
  annotations:
    helm.sh/hook: test
data:
  value: unchanged
`)
		withHook := func(manifest []byte) []byte {
			combined := append(append(append([]byte(nil), manifest...), []byte("---\n")...), hook...)
			canonical, canonicalErr := CanonicalizeRenderedManifest(combined, spec, "fugue-system")
			if canonicalErr != nil {
				t.Fatalf("canonicalize hook fixture: %v", canonicalErr)
			}
			return canonical
		}
		base := withHook(fixture.input.BaseManifest)
		target := withHook(fixture.input.TargetManifest)
		bindings := fixture.plan.Digests.ClassificationContext.BindingMap()
		rendered := ClassifyRendered(base, target, spec, RenderedOptions{
			DefaultNamespace: "fugue-system",
			Bindings:         bindings,
		})
		plan := BuildPlan(PlanInput{
			Files:    fixture.plan.Files,
			Rendered: rendered,
			Digests: DigestEvidence{
				Base:                   fixture.plan.Digests.Base,
				Target:                 fixture.plan.Digests.Target,
				Live:                   fixture.plan.Digests.Live,
				BaseManifest:           rollbackDigestBytes(base),
				TargetManifest:         rollbackDigestBytes(target),
				RepeatedTargetManifest: rollbackDigestBytes(target),
				Ownership:              fixture.plan.Digests.Ownership,
				ChangedFiles:           fixture.plan.Digests.ChangedFiles,
				ClassificationContext:  fixture.plan.Digests.ClassificationContext,
			},
		})
		if plan.Result != OutcomeSingle {
			t.Fatalf("hook fixture plan = %#v", plan)
		}
		envelope, err := NewTransactionEnvelope(plan, plan.PlanDigest, DomainNodeLocal)
		if err != nil {
			t.Fatalf("new hook fixture envelope: %v", err)
		}
		encoded, err := json.Marshal(envelope)
		if err != nil {
			t.Fatalf("marshal hook fixture envelope: %v", err)
		}
		fixture.input.Transaction, err = DecodeAndVerifyTransactionEnvelope(bytes.NewReader(encoded), plan.PlanDigest, DomainNodeLocal)
		if err != nil {
			t.Fatalf("decode hook fixture envelope: %v", err)
		}
		fixture.input.BaseManifest = base
		fixture.input.TargetManifest = target
		fixture.input.RepeatedTargetManifest = append([]byte(nil), target...)
		if _, err := VerifyRollbackOwnership(fixture.input); err == nil || !strings.Contains(err.Error(), "contains a Helm hook") {
			t.Fatalf("hook manifest error = %v", err)
		}
	})
}

func TestRollbackOwnershipPairRejectsReverseZeroMultipleUnknownAndMismatch(t *testing.T) {
	valid := renderedDomains(DomainNodeLocal)
	tests := []struct {
		name    string
		reverse RenderedClassification
	}{
		{name: "zero", reverse: RenderedClassification{Domains: []Domain{}, Evidence: []Evidence{}}},
		{name: "multiple", reverse: renderedDomains(DomainNodeLocal, DomainBackup)},
		{name: "unknown", reverse: RenderedClassification{
			Domains:  []Domain{DomainNodeLocal},
			Evidence: []Evidence{{Source: "rendered-object", Subject: "fixture", Domains: []Domain{DomainNodeLocal}}},
			Unknown:  []Evidence{{Source: "rendered-object", Subject: "fixture", Reason: "unknown"}},
		}},
		{name: "other domain", reverse: renderedDomains(DomainBackup)},
		{name: "different evidence", reverse: RenderedClassification{
			Domains:  []Domain{DomainNodeLocal},
			Evidence: []Evidence{{Source: "rendered-object", Subject: "other", Domains: []Domain{DomainNodeLocal}}},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := verifyExactRollbackOwnershipPair(valid, valid, test.reverse, DomainNodeLocal); err == nil {
				t.Fatal("invalid reverse ownership evidence was accepted")
			}
		})
	}
	if err := verifyExactRollbackOwnershipPair(renderedDomains(DomainBackup), valid, valid, DomainNodeLocal); err == nil {
		t.Fatal("forward evidence not authorized by transaction was accepted")
	}
}

func TestTransactionAuthorizationPlanSnapshotIsSealed(t *testing.T) {
	fixture := newRollbackOwnershipFixture(t, DomainNodeLocal)
	transaction := fixture.input.Transaction
	transaction.plan.Rendered.Domains[0] = DomainBackup
	if err := transaction.Verify(); err == nil {
		t.Fatal("mutated transaction plan snapshot verified")
	}
}
