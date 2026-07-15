package releasedomain

import (
	"strings"
	"testing"
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
		if plan.Result != OutcomeUnknown {
			t.Fatalf("result = %s, want unknown", plan.Result)
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
}
