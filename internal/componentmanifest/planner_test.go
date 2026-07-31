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
	for _, changedPath := range []string{
		"cmd/fugue-image-cache/main.go",
		"deploy/helm/fugue-image-plane/templates/daemonset.yaml",
	} {
		plan, err := PlanChanges(manifest, []string{changedPath})
		if err != nil {
			t.Fatalf("PlanChanges(%q) error = %v", changedPath, err)
		}
		if plan.DispatchMode != DispatchModeShadow {
			t.Fatalf("PlanChanges(%q) dispatch mode = %q, want %q", changedPath, plan.DispatchMode, DispatchModeShadow)
		}
		if !plan.RequiresLegacyRelease {
			t.Fatalf("PlanChanges(%q) must remain bound to the legacy release until cutover", changedPath)
		}
		if got, want := impactIDs(plan.ImpactedComponents), []string{"image-plane"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("PlanChanges(%q) impacted components = %v, want %v", changedPath, got, want)
		}
		if got, want := plan.ValidationOnlyComponents, []string{"backup-storage", "control-plane", "edge-dns", "operator-cli"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("PlanChanges(%q) validation-only components = %v, want %v", changedPath, got, want)
		}
		if got, want := resourceIDs(plan.SharedResources), []string{"legacy-fugue-helm-release", "registry"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("PlanChanges(%q) shared resources = %v, want %v", changedPath, got, want)
		}
		if err := plan.VerifyDigest(); err != nil {
			t.Fatalf("PlanChanges(%q) VerifyDigest() error = %v", changedPath, err)
		}
	}
}

func TestPlanRepositoryReleaseControlOwnsMigrationPlanner(t *testing.T) {
	manifest := loadRepositoryManifest(t)
	for _, changedPath := range []string{
		"cmd/fugue-component-plan/main.go",
		"deploy/helm/fugue-release-control/templates/deployment.yaml",
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

func TestPlanRepositoryBackupObserverChartIsShadowOnly(t *testing.T) {
	manifest := loadRepositoryManifest(t)
	for _, changedPath := range []string{
		".github/workflows/validate-backup-observation-api.yml",
		".github/workflows/validate-backup-release-candidate.yml",
		".github/workflows/validate-backup-observation-store.yml",
		".github/workflows/validate-backup-observer-identity.yml",
		"cmd/fugue-backup-release-plan/main.go",
		"deploy/helm/fugue-backup-observer/templates/deployment.yaml",
		"internal/api/backup_observation.go",
		"internal/backupadapter/legacy.go",
		"internal/backupidentity/identity.go",
		"internal/backuprelease/candidate.go",
		"internal/store/backup_observation.go",
	} {
		plan, err := PlanChanges(manifest, []string{changedPath})
		if err != nil {
			t.Fatalf("PlanChanges(%q) error = %v", changedPath, err)
		}
		if got, want := impactIDs(plan.ImpactedComponents), []string{"backup-storage"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("PlanChanges(%q) impacted components = %v, want %v", changedPath, got, want)
		}
		if plan.DispatchMode != DispatchModeShadow || !plan.RequiresLegacyRelease {
			t.Fatalf("PlanChanges(%q) = mode %q legacy=%v, want shadow/legacy", changedPath, plan.DispatchMode, plan.RequiresLegacyRelease)
		}
		if got, want := resourceIDs(plan.SharedResources), []string{"control-plane-postgres", "legacy-fugue-helm-release", "r2-backup-bucket"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("PlanChanges(%q) shared resources = %v, want %v", changedPath, got, want)
		}
		if err := plan.VerifyDigest(); err != nil {
			t.Fatalf("PlanChanges(%q) digest: %v", changedPath, err)
		}
	}
}

func TestPlanRepositoryBackupDryRunGuardChartIsShadowOnly(t *testing.T) {
	manifest := loadRepositoryManifest(t)
	for _, changedPath := range []string{
		".github/workflows/validate-backup-materializer-dry-run-guard-chart.yml",
		"deploy/helm/fugue-backup-materializer-dry-run-guard/templates/guard.yaml",
		"internal/backupmaterializer/dryrunguard/guard.go",
	} {
		plan, err := PlanChanges(manifest, []string{changedPath})
		if err != nil {
			t.Fatalf("PlanChanges(%q) error = %v", changedPath, err)
		}
		if got, want := impactIDs(plan.ImpactedComponents), []string{"backup-storage"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("PlanChanges(%q) impacted components = %v, want %v", changedPath, got, want)
		}
		if plan.DispatchMode != DispatchModeShadow || !plan.RequiresLegacyRelease {
			t.Fatalf("PlanChanges(%q) = mode %q legacy=%v, want shadow/legacy", changedPath, plan.DispatchMode, plan.RequiresLegacyRelease)
		}
		if got, want := resourceIDs(plan.SharedResources), []string{"control-plane-postgres", "legacy-fugue-helm-release", "r2-backup-bucket"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("PlanChanges(%q) shared resources = %v, want %v", changedPath, got, want)
		}
		if err := plan.VerifyDigest(); err != nil {
			t.Fatalf("PlanChanges(%q) digest: %v", changedPath, err)
		}
	}
}

func TestPlanRepositorySharedChangeFailsSafeToLegacy(t *testing.T) {
	manifest := loadRepositoryManifest(t)
	for _, changedPath := range []string{"internal/model/model.go", "internal/auth/auth.go", "cmd/fugue-openapi-gen/main.go"} {
		plan, err := PlanChanges(manifest, []string{changedPath})
		if err != nil {
			t.Fatalf("PlanChanges(%q) error = %v", changedPath, err)
		}
		if plan.DispatchMode != DispatchModeLegacyShared || !plan.RequiresLegacyRelease {
			t.Fatalf("PlanChanges(%q) shared plan = mode %q legacy=%v", changedPath, plan.DispatchMode, plan.RequiresLegacyRelease)
		}
		if len(plan.ImpactedComponents) != len(manifest.Components) {
			t.Fatalf("PlanChanges(%q) impacted components = %d, want %d", changedPath, len(plan.ImpactedComponents), len(manifest.Components))
		}
		if len(plan.ChangedPaths) != 1 || !plan.ChangedPaths[0].Shared {
			t.Fatalf("PlanChanges(%q) changed-path evidence: %+v", changedPath, plan.ChangedPaths)
		}
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
