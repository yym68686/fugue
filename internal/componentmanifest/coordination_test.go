package componentmanifest

import (
	"reflect"
	"testing"
)

func TestBuildRepositoryImageShadowCoordination(t *testing.T) {
	manifest := loadRepositoryManifest(t)
	changePlan, err := PlanChanges(manifest, []string{"cmd/fugue-image-cache/main.go"})
	if err != nil {
		t.Fatalf("PlanChanges() error = %v", err)
	}
	plan, err := BuildShadowCoordinationPlan(changePlan)
	if err != nil {
		t.Fatalf("BuildShadowCoordinationPlan() error = %v", err)
	}
	wantKeys := []string{
		"lane/image-plane",
		"legacy-release/fugue",
		"resource/legacy-fugue-helm-release",
		"resource/registry",
	}
	if got := coordinationKeys(plan.Scopes); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("coordination keys = %v, want %v", got, wantKeys)
	}
	if got, want := plan.RecoveryLanes, []string{"image-plane"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("recovery lanes = %v, want %v", got, want)
	}
	if !plan.ObservationOnly || plan.ProductionMutationAllowed {
		t.Fatalf("shadow plan can mutate production: %+v", plan)
	}
	if err := plan.VerifyDigest(); err != nil {
		t.Fatalf("VerifyDigest() error = %v", err)
	}
}

func TestBuildIndependentShadowCoordinationStillDoesNotAuthorizeMutation(t *testing.T) {
	changePlan, err := PlanChanges(minimalIndependentManifest(), []string{"cmd/worker/main.go"})
	if err != nil {
		t.Fatalf("PlanChanges() error = %v", err)
	}
	plan, err := BuildShadowCoordinationPlan(changePlan)
	if err != nil {
		t.Fatalf("BuildShadowCoordinationPlan() error = %v", err)
	}
	if got, want := coordinationKeys(plan.Scopes), []string{"lane/worker"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("coordination keys = %v, want %v", got, want)
	}
	if plan.DispatchMode != DispatchModeIndependent || plan.ProductionMutationAllowed {
		t.Fatalf("unexpected independent shadow plan: %+v", plan)
	}
}

func TestBuildShadowCoordinationRejectsMutatedInput(t *testing.T) {
	changePlan, err := PlanChanges(minimalIndependentManifest(), []string{"cmd/worker/main.go"})
	if err != nil {
		t.Fatalf("PlanChanges() error = %v", err)
	}
	changePlan.DispatchMode = DispatchModeShadow
	if _, err := BuildShadowCoordinationPlan(changePlan); err == nil {
		t.Fatal("BuildShadowCoordinationPlan() accepted a mutated input plan")
	}
}

func TestShadowCoordinationDigestDetectsMutation(t *testing.T) {
	changePlan, err := PlanChanges(minimalIndependentManifest(), []string{"cmd/worker/main.go"})
	if err != nil {
		t.Fatalf("PlanChanges() error = %v", err)
	}
	plan, err := BuildShadowCoordinationPlan(changePlan)
	if err != nil {
		t.Fatalf("BuildShadowCoordinationPlan() error = %v", err)
	}
	plan.ProductionMutationAllowed = true
	if err := plan.VerifyDigest(); err == nil {
		t.Fatal("VerifyDigest() accepted production authorization tampering")
	}
}

func coordinationKeys(scopes []CoordinationScope) []string {
	keys := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		keys = append(keys, scope.Key)
	}
	return keys
}
