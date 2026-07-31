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

func TestShadowArtifactIdentityIsGitBoundAndLaneLocal(t *testing.T) {
	manifest, changePlan, coordinationPlan := testShadowPlans(t)
	first, err := BuildShadowArtifactEnvelope(manifest, changePlan, coordinationPlan, testBaseCommit, testTargetCommit)
	if err != nil {
		t.Fatalf("BuildShadowArtifactEnvelope() error = %v", err)
	}
	identity, err := first.ArtifactIdentity()
	if err != nil {
		t.Fatalf("ArtifactIdentity() error = %v", err)
	}
	if got, want := identity.ScopeType, ShadowArtifactScopeType; got != want {
		t.Fatalf("scope type = %q, want %q", got, want)
	}
	if got, want := identity.ScopeKey, ShadowArtifactScopeType+":"+testBaseCommit+".."+testTargetCommit; got != want {
		t.Fatalf("scope key = %q, want %q", got, want)
	}
	if got, want := identity.Generation, "git-"+testTargetCommit; got != want {
		t.Fatalf("generation = %q, want %q", got, want)
	}

	content, err := first.Content()
	if err != nil {
		t.Fatalf("Content() error = %v", err)
	}
	if err := ValidateArtifactBinding(content, identity.ScopeType, identity.ScopeKey, identity.ScopeKey, identity.Generation); err != nil {
		t.Fatalf("ValidateArtifactBinding() error = %v", err)
	}
	tests := map[string]struct {
		scopeType      string
		scopeObjectKey string
		scopeKey       string
		generation     string
	}{
		"scope type":       {scopeType: "global", scopeObjectKey: identity.ScopeKey, scopeKey: identity.ScopeKey, generation: identity.Generation},
		"scope object key": {scopeType: identity.ScopeType, scopeObjectKey: ShadowArtifactScopeType + ":other", scopeKey: identity.ScopeKey, generation: identity.Generation},
		"scope key":        {scopeType: identity.ScopeType, scopeObjectKey: identity.ScopeKey, scopeKey: ShadowArtifactScopeType + ":other", generation: identity.Generation},
		"generation":       {scopeType: identity.ScopeType, scopeObjectKey: identity.ScopeKey, scopeKey: identity.ScopeKey, generation: "git-3333333333333333333333333333333333333333"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if err := ValidateArtifactBinding(content, test.scopeType, test.scopeObjectKey, test.scopeKey, test.generation); err == nil {
				t.Fatal("ValidateArtifactBinding() unexpectedly succeeded")
			}
		})
	}

	second, err := BuildShadowArtifactEnvelope(
		manifest,
		changePlan,
		coordinationPlan,
		"3333333333333333333333333333333333333333",
		testTargetCommit,
	)
	if err != nil {
		t.Fatalf("second BuildShadowArtifactEnvelope() error = %v", err)
	}
	secondIdentity, err := second.ArtifactIdentity()
	if err != nil {
		t.Fatalf("second ArtifactIdentity() error = %v", err)
	}
	if secondIdentity.ScopeKey == identity.ScopeKey {
		t.Fatal("different exact Git comparisons shared one platform release lane")
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
