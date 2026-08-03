package componentmanifest

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPlanRepositoryImageChangeIsShadowOnly(t *testing.T) {
	manifest := loadRepositoryManifest(t)
	plan, err := PlanChanges(manifest, []string{"cmd/fugue-image-cache/main.go"})
	if err != nil {
		t.Fatalf("PlanChanges() error = %v", err)
	}
	if plan.DispatchMode != DispatchModeShadow {
		t.Fatalf("dispatch mode = %q, want %q", plan.DispatchMode, DispatchModeShadow)
	}
	if !plan.RequiresLegacyRelease {
		t.Fatal("shadow change must remain bound to the legacy release until cutover")
	}
	if got, want := impactIDs(plan.ImpactedComponents), []string{"image-plane"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("impacted components = %v, want %v", got, want)
	}
	if got, want := plan.ValidationOnlyComponents, []string{"backup-storage", "control-plane", "edge-dns", "operator-cli"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("validation-only components = %v, want %v", got, want)
	}
	if got, want := resourceIDs(plan.SharedResources), []string{"legacy-fugue-helm-release", "registry"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("shared resources = %v, want %v", got, want)
	}
	if err := plan.VerifyDigest(); err != nil {
		t.Fatalf("VerifyDigest() error = %v", err)
	}
}

func TestPlanRepositoryEdgeControlBoundaryIsShadowOnly(t *testing.T) {
	manifest := loadRepositoryManifest(t)
	paths := []string{
		"cmd/fugue-edge-control/main.go",
		"internal/edgecontrol/boundary.go",
		"Dockerfile.edge-control",
		"deploy/helm/fugue-edge-control/values.yaml",
		"scripts/test_edge_control_image.sh",
		"scripts/deploy_edge_control_shadow.sh",
		"scripts/test_deploy_edge_control_shadow.sh",
		".github/workflows/publish-edge-control-image.yml",
		".github/workflows/deploy-edge-control-shadow.yml",
	}
	plan, err := PlanChanges(manifest, paths)
	if err != nil {
		t.Fatalf("PlanChanges() error = %v", err)
	}
	if plan.DispatchMode != DispatchModeShadow || !plan.RequiresLegacyRelease {
		t.Fatalf("edge-control boundary plan = mode %q legacy=%v", plan.DispatchMode, plan.RequiresLegacyRelease)
	}
	if got, want := impactIDs(plan.ImpactedComponents), []string{"edge-control"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("impacted components = %v, want %v", got, want)
	}
	if got, want := resourceIDs(plan.SharedResources), []string{"registry"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("shared resources = %v, want %v", got, want)
	}
	coordination, err := BuildShadowCoordinationPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if !coordination.ObservationOnly || coordination.ProductionMutationAllowed {
		t.Fatalf("edge-control boundary can mutate production: %+v", coordination)
	}
}

func TestPlanRepositoryReleaseControlOwnsMigrationPlanner(t *testing.T) {
	manifest := loadRepositoryManifest(t)
	for _, changedPath := range []string{
		"cmd/fugue-component-plan/main.go",
		"docs/architecture/component-ownership-v1.yaml",
		"internal/componentmanifest/artifact.go",
	} {
		plan, err := PlanChanges(manifest, []string{changedPath})
		if err != nil {
			t.Fatalf("PlanChanges(%q) error = %v", changedPath, err)
		}
		if got, want := impactIDs(plan.ImpactedComponents), []string{"release-control"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("PlanChanges(%q) impacted components = %v, want %v", changedPath, got, want)
		}
		if plan.DispatchMode != DispatchModeShadow || !plan.RequiresLegacyRelease {
			t.Fatalf("PlanChanges(%q) = mode %q legacy=%v", changedPath, plan.DispatchMode, plan.RequiresLegacyRelease)
		}
	}
}

func TestPlanRepositorySharedChangeFailsSafeToLegacy(t *testing.T) {
	manifest := loadRepositoryManifest(t)
	plan, err := PlanChanges(manifest, []string{"internal/model/model.go"})
	if err != nil {
		t.Fatalf("PlanChanges() error = %v", err)
	}
	if plan.DispatchMode != DispatchModeLegacyShared || !plan.RequiresLegacyRelease {
		t.Fatalf("shared plan = mode %q legacy=%v", plan.DispatchMode, plan.RequiresLegacyRelease)
	}
	if len(plan.ImpactedComponents) != len(manifest.Components) {
		t.Fatalf("impacted components = %d, want %d", len(plan.ImpactedComponents), len(manifest.Components))
	}
	if len(plan.ChangedPaths) != 1 || !plan.ChangedPaths[0].Shared {
		t.Fatalf("unexpected changed-path evidence: %+v", plan.ChangedPaths)
	}
}

func TestPlanFoundationSliceIsCoveredAndFailsClosed(t *testing.T) {
	manifest := loadRepositoryManifest(t)
	paths := []string{
		"cmd/fugue-component-plan/main.go",
		"cmd/fugue-component-plan/main_test.go",
		"docs/architecture/component-ownership-v1.md",
		"docs/architecture/component-ownership-v1.yaml",
		"docs/architecture/microservices-migration-acceptance-v1.md",
		"internal/componentmanifest/artifact.go",
		"internal/componentmanifest/artifact_test.go",
		"internal/componentmanifest/coordination.go",
		"internal/componentmanifest/coordination_test.go",
		"internal/componentmanifest/manifest.go",
		"internal/componentmanifest/manifest_test.go",
		"internal/componentmanifest/planner.go",
		"internal/componentmanifest/planner_test.go",
	}
	plan, err := PlanChanges(manifest, paths)
	if err != nil {
		t.Fatalf("PlanChanges() error = %v", err)
	}
	if plan.DispatchMode != DispatchModeShadow || !plan.RequiresLegacyRelease {
		t.Fatalf("foundation plan = mode %q legacy=%v", plan.DispatchMode, plan.RequiresLegacyRelease)
	}
	if len(plan.ChangedPaths) != len(paths) {
		t.Fatalf("changed paths = %d, want %d", len(plan.ChangedPaths), len(paths))
	}
	if got, want := impactIDs(plan.ImpactedComponents), []string{"release-control"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("impacted components = %v, want %v", got, want)
	}
	coordination, err := BuildShadowCoordinationPlan(plan)
	if err != nil {
		t.Fatalf("BuildShadowCoordinationPlan() error = %v", err)
	}
	if !coordination.ObservationOnly || coordination.ProductionMutationAllowed {
		t.Fatalf("foundation coordination can mutate production: %+v", coordination)
	}
}

func TestPlanShadowPersistenceSliceIsCoveredAndFailsClosed(t *testing.T) {
	manifest := loadRepositoryManifest(t)
	paths := []string{
		"cmd/fugue-component-plan/main.go",
		"cmd/fugue-component-plan/main_test.go",
		"docs/architecture/component-ownership-v1.md",
		"docs/architecture/component-ownership-v1.yaml",
		"docs/architecture/microservices-migration-acceptance-v1.md",
		"internal/api/component_release_plan_test.go",
		"internal/api/platform_state.go",
		"internal/api/resilience_explain.go",
		"internal/apispec/spec_gen.go",
		"internal/componentmanifest/artifact.go",
		"internal/componentmanifest/artifact_test.go",
		"internal/componentmanifest/planner_test.go",
		"internal/model/platform_state.go",
		"internal/platformcontrol/registry.go",
		"internal/platformcontrol/registry_test.go",
		"internal/platformsafety/kernel.go",
		"internal/platformsafety/kernel_test.go",
		"internal/releasecontrol/component_plan_http_store.go",
		"internal/releasecontrol/component_plan_http_store_test.go",
		"internal/releasecontrol/component_plan_reconciler.go",
		"internal/releasecontrol/component_plan_reconciler_test.go",
		"internal/store/platform_state.go",
		"openapi/openapi.yaml",
	}
	plan, err := PlanChanges(manifest, paths)
	if err != nil {
		t.Fatalf("PlanChanges() error = %v", err)
	}
	if plan.DispatchMode != DispatchModeLegacyShared || !plan.RequiresLegacyRelease {
		t.Fatalf("shadow persistence plan = mode %q legacy=%v", plan.DispatchMode, plan.RequiresLegacyRelease)
	}
	if len(plan.ChangedPaths) != len(paths) {
		t.Fatalf("changed paths = %d, want %d", len(plan.ChangedPaths), len(paths))
	}
	if len(plan.ImpactedComponents) != len(manifest.Components) {
		t.Fatalf("impacted components = %d, want %d", len(plan.ImpactedComponents), len(manifest.Components))
	}
	sharedPaths := map[string]struct{}{
		"internal/api/component_release_plan_test.go": {},
		"internal/api/platform_state.go":              {},
		"internal/api/resilience_explain.go":          {},
		"internal/apispec/spec_gen.go":                {},
		"internal/platformsafety/kernel.go":           {},
		"internal/platformsafety/kernel_test.go":      {},
		"internal/store/platform_state.go":            {},
	}
	for _, changedPath := range plan.ChangedPaths {
		if _, mustBeShared := sharedPaths[changedPath.Path]; mustBeShared && !changedPath.Shared {
			t.Fatalf("cross-boundary path %q is not shared", changedPath.Path)
		}
	}
	coordination, err := BuildShadowCoordinationPlan(plan)
	if err != nil {
		t.Fatalf("BuildShadowCoordinationPlan() error = %v", err)
	}
	if !coordination.ObservationOnly || coordination.ProductionMutationAllowed {
		t.Fatalf("shadow persistence coordination can mutate production: %+v", coordination)
	}
}

func TestPlanRejectsUnknownDuplicateAndNonCanonicalPaths(t *testing.T) {
	manifest := loadRepositoryManifest(t)
	for name, paths := range map[string][]string{
		"unknown":       {"unowned/runtime.go"},
		"duplicate":     {"cmd/fugue-api/main.go", "cmd/fugue-api/main.go"},
		"absolute":      {"/cmd/fugue-api/main.go"},
		"dot component": {"cmd/fugue-api/./main.go"},
		"backslash":     {`cmd\fugue-api\main.go`},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := PlanChanges(manifest, paths); err == nil {
				t.Fatal("PlanChanges() unexpectedly succeeded")
			}
		})
	}
}

func TestPlanCanAuthorizeIndependentOrCoordinatedModes(t *testing.T) {
	manifest := minimalIndependentManifest()
	plan, err := PlanChanges(manifest, []string{"cmd/worker/main.go"})
	if err != nil {
		t.Fatalf("PlanChanges() error = %v", err)
	}
	if plan.DispatchMode != DispatchModeIndependent {
		t.Fatalf("dispatch mode = %q, want %q", plan.DispatchMode, DispatchModeIndependent)
	}

	manifest.SharedResources = []SharedResource{{
		ID: "registry", Owner: "release-control", ConflictMode: "mediated", Consumers: []string{"worker"},
	}}
	plan, err = PlanChanges(manifest, []string{"cmd/worker/main.go"})
	if err != nil {
		t.Fatalf("PlanChanges() coordinated error = %v", err)
	}
	if plan.DispatchMode != DispatchModeCoordinated {
		t.Fatalf("dispatch mode = %q, want %q", plan.DispatchMode, DispatchModeCoordinated)
	}
	if len(plan.SharedResources) != 1 || !plan.SharedResources[0].RequiresCoordinator {
		t.Fatalf("unexpected coordinated resources: %+v", plan.SharedResources)
	}
}

func TestPlanDigestIsDeterministicAndTamperEvident(t *testing.T) {
	manifest := minimalIndependentManifest()
	first, err := PlanChanges(manifest, []string{"cmd/worker/main.go", "internal/release/main.go"})
	if err != nil {
		t.Fatalf("first PlanChanges() error = %v", err)
	}
	second, err := PlanChanges(manifest, []string{"internal/release/main.go", "cmd/worker/main.go"})
	if err != nil {
		t.Fatalf("second PlanChanges() error = %v", err)
	}
	if first.PlanDigest != second.PlanDigest {
		t.Fatalf("digest is order-dependent: %s != %s", first.PlanDigest, second.PlanDigest)
	}
	if !strings.HasPrefix(first.PlanDigest, "sha256:") || len(first.PlanDigest) != len("sha256:")+64 {
		t.Fatalf("unexpected plan digest %q", first.PlanDigest)
	}
	first.DispatchMode = DispatchModeShadow
	if err := first.VerifyDigest(); err == nil {
		t.Fatal("VerifyDigest() accepted a mutated plan")
	}
}

func TestChangePlanValidateRejectsRehashedMalformedShape(t *testing.T) {
	for name, mutate := range map[string]func(*ChangePlan){
		"invalid component": func(plan *ChangePlan) {
			plan.ImpactedComponents[0].ID = "../worker"
			plan.ChangedPaths[0].Components[0] = "../worker"
		},
		"unsorted paths": func(plan *ChangePlan) {
			plan.ChangedPaths = append(plan.ChangedPaths, plan.ChangedPaths[0])
		},
		"unjustified component": func(plan *ChangePlan) {
			plan.ImpactedComponents = append(plan.ImpactedComponents, ComponentImpact{
				ID: "zombie", ReleaseLane: "zombie", OwnershipMode: "independent",
			})
		},
		"unsafe dispatch": func(plan *ChangePlan) {
			plan.DispatchMode = DispatchModeLegacyShared
			plan.RequiresLegacyRelease = true
		},
		"invalid manifest digest": func(plan *ChangePlan) {
			plan.ManifestDigest = "sha256:invalid"
		},
	} {
		t.Run(name, func(t *testing.T) {
			plan, err := PlanChanges(minimalIndependentManifest(), []string{"cmd/worker/main.go"})
			if err != nil {
				t.Fatalf("PlanChanges() error = %v", err)
			}
			mutate(&plan)
			plan.PlanDigest = plan.Digest()
			if err := plan.Validate(); err == nil {
				t.Fatal("Validate() accepted a malformed rehashed plan")
			}
		})
	}
}

func loadRepositoryManifest(t *testing.T) Manifest {
	t.Helper()
	manifestPath := filepath.Join("..", "..", "docs", "architecture", "component-ownership-v1.yaml")
	file, err := os.Open(manifestPath)
	if err != nil {
		t.Fatalf("open repository manifest: %v", err)
	}
	defer file.Close()
	manifest, err := Load(file)
	if err != nil {
		t.Fatalf("load repository manifest: %v", err)
	}
	return manifest
}

func minimalIndependentManifest() Manifest {
	return Manifest{
		APIVersion:     APIVersion,
		Kind:           Kind,
		MigrationPhase: "lane-shadow",
		LegacyRelease:  "fugue",
		Components: []Component{
			{
				ID: "release-control", Description: "release coordinator",
				RuntimeKinds: []string{"coordinator"}, OwnershipMode: "independent",
				SourceRoots: []string{"internal/release"}, ArtifactKinds: []string{"release-control"},
				ReleaseLane: "release-control", Coordinator: "release-control",
				OwnedState: []string{"release-ledger"}, Contracts: []string{"release-intent@v1"},
				FailureBoundary: "release-control", LKGPolicy: "required",
			},
			{
				ID: "worker", Description: "independent worker",
				RuntimeKinds: []string{"deployment"}, OwnershipMode: "independent",
				SourceRoots: []string{"cmd/worker"}, ArtifactKinds: []string{"worker"},
				ReleaseLane: "worker", Coordinator: "release-control",
				OwnedState: []string{"worker-status"}, Contracts: []string{"worker@v1"},
				FailureBoundary: "worker", LKGPolicy: "required",
			},
		},
	}
}

func impactIDs(impacts []ComponentImpact) []string {
	ids := make([]string, 0, len(impacts))
	for _, impact := range impacts {
		ids = append(ids, impact.ID)
	}
	return ids
}

func resourceIDs(impacts []ResourceImpact) []string {
	ids := make([]string, 0, len(impacts))
	for _, impact := range impacts {
		ids = append(ids, impact.ID)
	}
	return ids
}
