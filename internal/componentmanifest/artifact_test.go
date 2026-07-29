package componentmanifest

import (
	"reflect"
	"testing"
)

const (
	testBaseCommit   = "1111111111111111111111111111111111111111"
	testTargetCommit = "2222222222222222222222222222222222222222"
)

func TestShadowArtifactEnvelopeRoundTrip(t *testing.T) {
	manifest, changePlan, coordinationPlan := testShadowPlans(t)
	envelope, err := BuildShadowArtifactEnvelope(manifest, changePlan, coordinationPlan, testBaseCommit, testTargetCommit)
	if err != nil {
		t.Fatalf("BuildShadowArtifactEnvelope() error = %v", err)
	}
	content, err := envelope.Content()
	if err != nil {
		t.Fatalf("Content() error = %v", err)
	}
	decoded, err := DecodeShadowArtifactContent(content)
	if err != nil {
		t.Fatalf("DecodeShadowArtifactContent() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, envelope) {
		t.Fatalf("round trip changed envelope:\n got  %+v\n want %+v", decoded, envelope)
	}
	content["targetCommit"] = testBaseCommit
	if decoded.TargetCommit != testTargetCommit {
		t.Fatal("stored content mutation changed the decoded envelope")
	}
}

func TestShadowArtifactEnvelopeRejectsUnsafeOrUnboundContent(t *testing.T) {
	for name, mutate := range map[string]func(*ShadowArtifactEnvelope){
		"invalid base": func(envelope *ShadowArtifactEnvelope) {
			envelope.BaseCommit = "main"
		},
		"same commits": func(envelope *ShadowArtifactEnvelope) {
			envelope.TargetCommit = envelope.BaseCommit
		},
		"mutation allowed": func(envelope *ShadowArtifactEnvelope) {
			envelope.CoordinationPlan.ProductionMutationAllowed = true
		},
		"wrong change digest": func(envelope *ShadowArtifactEnvelope) {
			envelope.ChangePlan.PlanDigest = "sha256:invalid"
		},
		"wrong coordination input": func(envelope *ShadowArtifactEnvelope) {
			envelope.CoordinationPlan.ChangePlanDigest = "sha256:invalid"
		},
		"forged ownership": func(envelope *ShadowArtifactEnvelope) {
			envelope.ChangePlan.ImpactedComponents[0].OwnershipMode = "independent"
			envelope.ChangePlan.DispatchMode = DispatchModeCoordinated
			envelope.ChangePlan.RequiresLegacyRelease = false
			envelope.ChangePlan.PlanDigest = envelope.ChangePlan.Digest()
		},
	} {
		t.Run(name, func(t *testing.T) {
			manifest, changePlan, coordinationPlan := testShadowPlans(t)
			envelope := ShadowArtifactEnvelope{
				SchemaVersion:    ShadowArtifactSchemaVersionV1,
				BaseCommit:       testBaseCommit,
				TargetCommit:     testTargetCommit,
				Manifest:         manifest,
				ChangePlan:       changePlan,
				CoordinationPlan: coordinationPlan,
			}
			mutate(&envelope)
			if err := envelope.Validate(); err == nil {
				t.Fatal("Validate() unexpectedly succeeded")
			}
		})
	}
}

func TestDecodeShadowArtifactContentRejectsUnknownFields(t *testing.T) {
	manifest, changePlan, coordinationPlan := testShadowPlans(t)
	envelope, err := BuildShadowArtifactEnvelope(manifest, changePlan, coordinationPlan, testBaseCommit, testTargetCommit)
	if err != nil {
		t.Fatalf("BuildShadowArtifactEnvelope() error = %v", err)
	}
	content, err := envelope.Content()
	if err != nil {
		t.Fatalf("Content() error = %v", err)
	}
	content["productionMutationAllowed"] = true
	if _, err := DecodeShadowArtifactContent(content); err == nil {
		t.Fatal("DecodeShadowArtifactContent() accepted an unknown mutation field")
	}
}

func testShadowPlans(t *testing.T) (Manifest, ChangePlan, ShadowCoordinationPlan) {
	t.Helper()
	manifest := loadRepositoryManifest(t)
	changePlan, err := PlanChanges(manifest, []string{"cmd/fugue-image-cache/main.go"})
	if err != nil {
		t.Fatalf("PlanChanges() error = %v", err)
	}
	coordinationPlan, err := BuildShadowCoordinationPlan(changePlan)
	if err != nil {
		t.Fatalf("BuildShadowCoordinationPlan() error = %v", err)
	}
	return manifest, changePlan, coordinationPlan
}
