package releasedomain

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

func stableDigestEvidence() DigestEvidence {
	context, err := NewClassificationContextEvidence("fugue-system", map[string]string{
		"releaseNamespace": "fugue-system",
	}, false)
	if err != nil {
		panic(err)
	}
	return DigestEvidence{
		Base: "opaque-base", Target: "opaque-target", Live: "opaque-base",
		BaseManifest: "sha256:base", TargetManifest: "sha256:target", RepeatedTargetManifest: "sha256:target",
		Ownership: "sha256:ownership", ChangedFiles: "sha256:files", ClassificationContext: context,
	}
}

func fileDomains(domains ...Domain) FileClassification {
	classification := FileClassification{Domains: domains}
	if len(domains) > 0 {
		classification.Evidence = []Evidence{{Source: "changed-file", Subject: "fixture", Domains: domains}}
	}
	return classification
}

func renderedDomains(domains ...Domain) RenderedClassification {
	classification := RenderedClassification{Domains: domains}
	if len(domains) > 0 {
		classification.Evidence = []Evidence{{Source: "rendered-object", Subject: "fixture", Domains: domains}}
	}
	return classification
}

func TestPlannerOutcomes(t *testing.T) {
	tests := []struct {
		name         string
		input        PlanInput
		want         Outcome
		wantSelected Domain
	}{
		{
			name: "zero",
			input: PlanInput{
				Files:    FileClassification{Domains: []Domain{}, AllNonRuntime: true},
				Rendered: RenderedClassification{Domains: []Domain{}}, Digests: stableDigestEvidence(),
			},
			want: OutcomeZero,
		},
		{
			name: "single",
			input: PlanInput{
				Files:    fileDomains(DomainNodeLocal),
				Rendered: renderedDomains(DomainNodeLocal), Digests: stableDigestEvidence(),
			},
			want: OutcomeSingle, wantSelected: DomainNodeLocal,
		},
		{
			name: "multiple",
			input: PlanInput{
				Files:    fileDomains(DomainAuthoritativeDNS, DomainNodeLocal),
				Rendered: renderedDomains(DomainNodeLocal, DomainAuthoritativeDNS), Digests: stableDigestEvidence(),
			},
			want: OutcomeMultiple,
		},
		{
			name: "mismatched-evidence",
			input: PlanInput{
				Files:    fileDomains(DomainNodeLocal),
				Rendered: renderedDomains(DomainControlPlane), Digests: stableDigestEvidence(),
			},
			want: OutcomeUnknown,
		},
		{
			name: "classifier-unknown",
			input: PlanInput{
				Files:    FileClassification{Unknown: []Evidence{{Source: "changed-file", Subject: "shared", Reason: "unknown"}}},
				Rendered: RenderedClassification{}, Digests: stableDigestEvidence(),
			},
			want: OutcomeUnknown,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := BuildPlan(test.input)
			if plan.Result != test.want || plan.SelectedDomain != test.wantSelected {
				t.Fatalf("plan result = %s selected=%s unknown=%#v", plan.Result, plan.SelectedDomain, plan.Unknown)
			}
			if err := VerifyPlanDigest(plan); err != nil {
				t.Fatalf("verify digest: %v", err)
			}
			if plan.NoWriteRequired() != (test.want == OutcomeZero) {
				t.Fatalf("NoWriteRequired = %v", plan.NoWriteRequired())
			}
			if plan.SingleDomainDispatchAllowed() != (test.want == OutcomeSingle) {
				t.Fatalf("SingleDomainDispatchAllowed = %v", plan.SingleDomainDispatchAllowed())
			}
		})
	}
}

func TestZeroRequiresEveryFileProvenNonRuntime(t *testing.T) {
	plan := BuildPlan(PlanInput{
		Files:    FileClassification{Domains: []Domain{}, AllNonRuntime: false},
		Rendered: RenderedClassification{Domains: []Domain{}}, Digests: stableDigestEvidence(),
	})
	if plan.Result != OutcomeUnknown {
		t.Fatalf("result = %s", plan.Result)
	}
}

func TestDigestAndLookupDriftFailClosed(t *testing.T) {
	digests := stableDigestEvidence()
	digests.Live = "different-live"
	digests.RepeatedTargetManifest = "different-render"
	plan := BuildPlan(PlanInput{
		Files:    fileDomains(DomainNodeLocal),
		Rendered: renderedDomains(DomainNodeLocal), Digests: digests,
	})
	if plan.Result != OutcomeUnknown || len(plan.Unknown) != 2 {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestClassificationContextEvidenceFailsClosed(t *testing.T) {
	valid := stableDigestEvidence().ClassificationContext
	tests := []struct {
		name    string
		context ClassificationContextEvidence
	}{
		{name: "missing", context: ClassificationContextEvidence{}},
		{name: "empty namespace", context: func() ClassificationContextEvidence {
			context := valid
			context.DefaultNamespace = ""
			context.Digest = classificationContextDigest(context)
			return context
		}()},
		{name: "namespace binding mismatch", context: func() ClassificationContextEvidence {
			context := valid
			context.DefaultNamespace = "other-system"
			context.Digest = classificationContextDigest(context)
			return context
		}()},
		{name: "invalid digest", context: func() ClassificationContextEvidence {
			context := valid
			context.Digest = "sha256:invalid"
			return context
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			digests := stableDigestEvidence()
			digests.ClassificationContext = test.context
			plan := BuildPlan(PlanInput{
				Files:    fileDomains(DomainNodeLocal),
				Rendered: renderedDomains(DomainNodeLocal),
				Digests:  digests,
			})
			if plan.Result != OutcomeUnknown {
				t.Fatalf("result = %s, want unknown", plan.Result)
			}
			found := false
			for _, evidence := range plan.Unknown {
				if evidence.Source == "planner" && evidence.Subject == "classification-context" {
					found = true
					if strings.TrimSpace(evidence.Reason) == "" {
						t.Fatal("classification-context evidence has no reason")
					}
				}
			}
			if !found {
				t.Fatalf("missing classification-context evidence: %#v", plan.Unknown)
			}
		})
	}
}

func TestPlanDigestBindsAllEvidence(t *testing.T) {
	input := PlanInput{
		Files:    fileDomains(DomainBackup),
		Rendered: renderedDomains(DomainBackup), Digests: stableDigestEvidence(),
	}
	first := BuildPlan(input)
	second := BuildPlan(input)
	if first.PlanDigest != second.PlanDigest {
		t.Fatalf("digest is not deterministic: %s != %s", first.PlanDigest, second.PlanDigest)
	}
	first.Digests.Target = "mutated"
	if err := VerifyPlanDigest(first); err == nil {
		t.Fatal("expected mutation to invalidate plan digest")
	}
}

func TestPlannerRejectsUnknownDomainValue(t *testing.T) {
	plan := BuildPlan(PlanInput{
		Files:    FileClassification{Domains: []Domain{"not-a-domain"}},
		Rendered: RenderedClassification{Domains: []Domain{}}, Digests: stableDigestEvidence(),
	})
	if plan.Result != OutcomeUnknown || len(plan.Unknown) == 0 {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestPlannerRejectsDeclaredDomainWithoutEvidence(t *testing.T) {
	plan := BuildPlan(PlanInput{
		Files:    FileClassification{Domains: []Domain{DomainNodeLocal}},
		Rendered: renderedDomains(DomainNodeLocal), Digests: stableDigestEvidence(),
	})
	if plan.Result != OutcomeUnknown || len(plan.Unknown) == 0 {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestPlannerRejectsInvalidUTF8InPersistedInputs(t *testing.T) {
	t.Run("opaque digest", func(t *testing.T) {
		digests := stableDigestEvidence()
		digests.Target = "target-\xff"
		plan := BuildPlan(PlanInput{
			Files: fileDomains(DomainNodeLocal), Rendered: renderedDomains(DomainNodeLocal), Digests: digests,
		})
		assertBlockedPlanIsByteSafe(t, plan)
		if plan.Digests.Target != "" {
			t.Fatalf("invalid digest was persisted as %q", plan.Digests.Target)
		}
	})

	t.Run("external evidence", func(t *testing.T) {
		files := fileDomains(DomainNodeLocal)
		files.Evidence[0].Subject = "fixture-\xff"
		plan := BuildPlan(PlanInput{
			Files: files, Rendered: renderedDomains(DomainNodeLocal), Digests: stableDigestEvidence(),
		})
		if plan.Result != OutcomeUnknown {
			t.Fatalf("result = %s, want unknown", plan.Result)
		}
	})

	t.Run("file classifier unknown evidence", func(t *testing.T) {
		files := fileDomains(DomainNodeLocal)
		files.Unknown = []Evidence{{
			Source: "changed-file", Subject: "fixture", Reason: "unknown-\xff",
			Paths: []string{"valid", "invalid-\xfe"},
		}}
		assertBlockedPlanIsByteSafe(t, BuildPlan(PlanInput{
			Files: files, Rendered: renderedDomains(DomainNodeLocal), Digests: stableDigestEvidence(),
		}))
	})

	t.Run("rendered classifier unknown evidence", func(t *testing.T) {
		rendered := renderedDomains(DomainNodeLocal)
		rendered.Unknown = []Evidence{{
			Source: "rendered-object", Subject: "fixture-\xff", Reason: "unknown",
			Domains: []Domain{DomainNodeLocal, Domain("invalid-\xfe")},
		}}
		assertBlockedPlanIsByteSafe(t, BuildPlan(PlanInput{
			Files: fileDomains(DomainNodeLocal), Rendered: rendered, Digests: stableDigestEvidence(),
		}))
	})

	t.Run("classification context", func(t *testing.T) {
		digests := stableDigestEvidence()
		digests.ClassificationContext.Bindings[0].Value = "namespace-\xff"
		assertBlockedPlanIsByteSafe(t, BuildPlan(PlanInput{
			Files: fileDomains(DomainNodeLocal), Rendered: renderedDomains(DomainNodeLocal), Digests: digests,
		}))
	})
}

func assertBlockedPlanIsByteSafe(t *testing.T, plan Plan) {
	t.Helper()
	if plan.Result != OutcomeUnknown {
		t.Fatalf("result = %s, want unknown", plan.Result)
	}
	if invalid := invalidUTF8Paths(reflect.ValueOf(plan), ""); len(invalid) != 0 {
		t.Fatalf("persisted plan contains invalid UTF-8: %v", invalid)
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal blocked plan: %v", err)
	}
	if !utf8.Valid(encoded) {
		t.Fatalf("blocked plan JSON is not valid UTF-8: %q", encoded)
	}
	if bytes.Contains(encoded, []byte("\xef\xbf\xbd")) {
		t.Fatalf("blocked plan silently contains a replacement rune: %s", encoded)
	}
	if err := VerifyPlanDigest(plan); err != nil {
		t.Fatalf("verify blocked plan digest: %v", err)
	}
}

func TestDecodeAndVerifyPlanStrictBoundary(t *testing.T) {
	plan := BuildPlan(PlanInput{
		Files: fileDomains(DomainNodeLocal), Rendered: renderedDomains(DomainNodeLocal), Digests: stableDigestEvidence(),
	})
	encoded, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	decoded, err := DecodeAndVerifyPlan(bytes.NewReader(encoded), plan.PlanDigest)
	if err != nil {
		t.Fatalf("decode valid plan with opaque SSoT digests: %v", err)
	}
	if decoded.Digests.Base != "opaque-base" || decoded.Digests.Target != "opaque-target" {
		t.Fatalf("opaque SSoT digests were normalized: %#v", decoded.Digests)
	}

	invalidRawUTF8 := bytes.Replace(encoded, []byte("opaque-target"), []byte{'b', 'a', 'd', '-', 0xff}, 1)
	requiredEvidenceNull := mutatePlanJSON(t, encoded, func(root map[string]any) {
		root["files"].(map[string]any)["evidence"] = nil
	})
	requiredBindingsNull := mutatePlanJSON(t, encoded, func(root map[string]any) {
		digests := root["digests"].(map[string]any)
		digests["classificationContext"].(map[string]any)["bindings"] = nil
	})
	optionalUnknownNull := mutatePlanJSON(t, encoded, func(root map[string]any) {
		root["unknown"] = nil
	})
	optionalNestedUnknownNull := mutatePlanJSON(t, encoded, func(root map[string]any) {
		root["files"].(map[string]any)["unknown"] = nil
	})
	optionalEvidencePathsNull := mutatePlanJSON(t, encoded, func(root map[string]any) {
		files := root["files"].(map[string]any)
		files["evidence"].([]any)[0].(map[string]any)["paths"] = nil
	})
	tests := []struct {
		name     string
		data     []byte
		expected string
	}{
		{name: "missing external digest", data: encoded, expected: ""},
		{name: "invalid external digest utf8", data: encoded, expected: "digest-\xff"},
		{name: "external digest mismatch", data: encoded, expected: "sha256:not-the-plan"},
		{name: "duplicate root field", data: append([]byte(`{"apiVersion":"release-domain-plan.fugue.dev/v1",`), encoded[1:]...), expected: plan.PlanDigest},
		{name: "duplicate nested field", data: bytes.Replace(encoded, []byte(`"digests": {`), []byte(`"digests": {"base":"duplicate",`), 1), expected: plan.PlanDigest},
		{name: "unknown root field", data: append([]byte(`{"extra":true,`), encoded[1:]...), expected: plan.PlanDigest},
		{name: "unknown nested field", data: bytes.Replace(encoded, []byte(`"digests": {`), []byte(`"digests": {"extra":true,`), 1), expected: plan.PlanDigest},
		{name: "case alias", data: bytes.Replace(encoded, []byte(`"apiVersion"`), []byte(`"APIVersion"`), 1), expected: plan.PlanDigest},
		{name: "invalid raw utf8", data: invalidRawUTF8, expected: plan.PlanDigest},
		{name: "isolated unicode surrogate", data: bytes.Replace(encoded, []byte("opaque-target"), []byte(`bad-\ud800`), 1), expected: plan.PlanDigest},
		{name: "trailing value", data: append(append([]byte(nil), encoded...), []byte("\n{}")...), expected: plan.PlanDigest},
		{name: "null root", data: []byte("null"), expected: plan.PlanDigest},
		{name: "required evidence null", data: requiredEvidenceNull, expected: plan.PlanDigest},
		{name: "required bindings null", data: requiredBindingsNull, expected: plan.PlanDigest},
		{name: "optional root unknown null", data: optionalUnknownNull, expected: plan.PlanDigest},
		{name: "optional nested unknown null", data: optionalNestedUnknownNull, expected: plan.PlanDigest},
		{name: "optional evidence paths null", data: optionalEvidencePathsNull, expected: plan.PlanDigest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeAndVerifyPlan(bytes.NewReader(test.data), test.expected); err == nil {
				t.Fatal("strict decoder accepted invalid plan")
			}
		})
	}
}

func TestStrictDecoderRejectsOptionalNullDigestAlias(t *testing.T) {
	plan := BuildPlan(PlanInput{
		Files: fileDomains(DomainNodeLocal), Rendered: renderedDomains(DomainNodeLocal), Digests: stableDigestEvidence(),
	})
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}
	withNull := mutatePlanJSON(t, encoded, func(root map[string]any) {
		root["files"].(map[string]any)["unknown"] = nil
	})

	var permissivelyDecoded Plan
	if err := json.Unmarshal(withNull, &permissivelyDecoded); err != nil {
		t.Fatalf("permissive decode control: %v", err)
	}
	if err := VerifyPlanDigest(permissivelyDecoded); err != nil {
		t.Fatalf("control mutation should preserve the legacy semantic digest: %v", err)
	}
	if _, err := DecodeAndVerifyPlan(bytes.NewReader(withNull), plan.PlanDigest); err == nil {
		t.Fatal("strict decoder accepted an explicit null alias for an omitted slice")
	}
}

func TestBuildPlanCanonicalizesRequiredEmptyArrays(t *testing.T) {
	plan := BuildPlan(PlanInput{
		Files: FileClassification{AllNonRuntime: true}, Rendered: RenderedClassification{}, Digests: stableDigestEvidence(),
	})
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal zero-domain plan: %v", err)
	}
	for _, forbidden := range [][]byte{
		[]byte(`"domains":null`),
		[]byte(`"evidence":null`),
		[]byte(`"bindings":null`),
	} {
		if bytes.Contains(encoded, forbidden) {
			t.Fatalf("zero-domain plan contains non-canonical required slice %s: %s", forbidden, encoded)
		}
	}
	if _, err := DecodeAndVerifyPlan(bytes.NewReader(encoded), plan.PlanDigest); err != nil {
		t.Fatalf("strict decode canonical zero-domain plan: %v", err)
	}
}

func mutatePlanJSON(t *testing.T, encoded []byte, mutate func(map[string]any)) []byte {
	t.Helper()
	var root map[string]any
	if err := json.Unmarshal(encoded, &root); err != nil {
		t.Fatalf("decode plan mutation fixture: %v", err)
	}
	mutate(root)
	mutated, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("encode plan mutation fixture: %v", err)
	}
	return mutated
}

func TestDecodeAndVerifyPlanRejectsMutatedOrUnsupportedPlan(t *testing.T) {
	plan := BuildPlan(PlanInput{
		Files: fileDomains(DomainNodeLocal), Rendered: renderedDomains(DomainNodeLocal), Digests: stableDigestEvidence(),
	})
	tests := []struct {
		name   string
		mutate func(*Plan)
	}{
		{name: "unsupported api version", mutate: func(plan *Plan) { plan.APIVersion = "other/v1" }},
		{name: "unsupported kind", mutate: func(plan *Plan) { plan.Kind = "OtherPlan" }},
		{name: "digest-bound field", mutate: func(plan *Plan) { plan.Digests.Target = "other-target" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := plan
			test.mutate(&mutated)
			encoded, err := json.Marshal(mutated)
			if err != nil {
				t.Fatalf("marshal mutated plan: %v", err)
			}
			if _, err := DecodeAndVerifyPlan(bytes.NewReader(encoded), plan.PlanDigest); err == nil {
				t.Fatal("strict decoder accepted mutated plan")
			}
		})
	}
}

func TestVerifyPlanDigestRejectsInvalidUTF8BeforeHashing(t *testing.T) {
	plan := BuildPlan(PlanInput{
		Files: fileDomains(DomainNodeLocal), Rendered: renderedDomains(DomainNodeLocal), Digests: stableDigestEvidence(),
	})
	plan.Digests.Target = "target-\xff"
	if err := VerifyPlanDigest(plan); err == nil || !strings.Contains(err.Error(), "not valid UTF-8") {
		t.Fatalf("VerifyPlanDigest invalid UTF-8 error = %v", err)
	}
}
