package declarativerelease

import (
	"reflect"
	"strings"
	"testing"
)

const testSHA1 = "1111111111111111111111111111111111111111"
const testSHA2 = "2222222222222222222222222222222222222222"
const testDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func testRegistry() Registry {
	return Registry{
		APIVersion: RegistryAPIVersion,
		Kind:       RegistryKind,
		Components: []Component{
			{
				ID: "api", Family: "control-plane",
				IntentPath:   "deploy/releases/api/intent.json",
				ManifestPath: "deploy/releases/api/deployment.json",
				SourceRoots:  []string{"Dockerfile.api", "cmd/fugue-api", "internal/api"},
				Artifact:     Artifact{Repository: "ghcr.io/example/fugue-api", Dockerfile: "Dockerfile.api", Context: ".", BuildPackage: "./cmd/fugue-api"},
				Workload:     Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api", FieldManager: "fugue-api-declarative", Replicas: 2, RolloutMode: "rolling"},
				Health:       []HealthProbe{{Type: "deployment", Name: "fugue-fugue-api"}, {Type: "service-http", Name: "fugue-fugue", Port: "http", Path: "/healthz", Expected: "ok"}},
				Concurrency:  "fugue-production-api",
			},
			{
				ID: "telemetry", Family: "observability",
				IntentPath:   "deploy/releases/telemetry/intent.json",
				ManifestPath: "deploy/releases/telemetry/deployment.json",
				SourceRoots:  []string{"Dockerfile.telemetry-agent", "cmd/fugue-telemetry-agent", "internal/observability"},
				Artifact:     Artifact{Repository: "ghcr.io/example/fugue-telemetry-agent", Dockerfile: "Dockerfile.telemetry-agent", Context: ".", BuildPackage: "./cmd/fugue-telemetry-agent"},
				Workload:     Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-telemetry-agent", Container: "telemetry-agent", FieldManager: "fugue-telemetry-declarative", Replicas: 1, RolloutMode: "rolling"},
				Health:       []HealthProbe{{Type: "deployment", Name: "fugue-fugue-telemetry-agent"}},
				Concurrency:  "fugue-production-telemetry",
			},
		},
	}
}

func TestRegistryAndIntentAreStrict(t *testing.T) {
	registry := testRegistry()
	if err := registry.Validate(); err != nil {
		t.Fatalf("valid registry: %v", err)
	}
	intent := Intent{APIVersion: IntentAPIVersion, Kind: IntentKind, Component: "api", Generation: 1, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: testSHA1, ExpectedPreviousManifestSHA: testSHA1, ExpectedPreviousOCIRevision: testSHA1, ExpectedPreviousImageDigest: testDigest, Rollback: "previous-git-lkg"}
	if err := intent.Validate(); err != nil {
		t.Fatalf("valid intent: %v", err)
	}
	if _, err := DecodeIntent(strings.NewReader(`{"apiVersion":"release.fugue.dev/v2","kind":"ProductionComponentIntent","component":"api","generation":1,"expectedPreviousConfigSha":"` + testSHA1 + `","expectedPreviousManifestSha":"` + testSHA1 + `","expectedPreviousOciRevision":"` + testSHA1 + `","expectedPreviousImageDigest":"` + testDigest + `","rollback":"previous-git-lkg","unknown":true}`)); err == nil {
		t.Fatal("unknown intent field was accepted")
	}
	if _, err := DecodeIntent(strings.NewReader(`{"apiVersion":"release.fugue.dev/v2","kind":"ProductionComponentIntent","component":"api","generation":1,"expectedPreviousConfigSha":"` + testSHA1 + `","expectedPreviousManifestSha":"` + testSHA1 + `","expectedPreviousOciRevision":"` + testSHA1 + `","expectedPreviousImageDigest":"` + testDigest + `","rollback":"previous-git-lkg"}`)); err == nil {
		t.Fatal("intent without explicit predecessor presence was accepted")
	}
	registry.Components[1].Concurrency = registry.Components[0].Concurrency
	if err := registry.Validate(); err == nil {
		t.Fatal("non-component-scoped concurrency was accepted")
	}
}

func TestRegistryArtifactTargetsAreStrictAndContainPrimary(t *testing.T) {
	registry := testRegistry()
	registry.Components[0].ArtifactTargets = []ArtifactTarget{
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api", ContainerType: "container"},
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "bootstrap", ContainerType: "init-container"},
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api-worker", Container: "api-worker", ContainerType: "container"},
	}
	if err := registry.Validate(); err != nil {
		t.Fatalf("valid multi-workload artifact targets: %v", err)
	}

	registry.Components[0].ArtifactTargets[0], registry.Components[0].ArtifactTargets[1] = registry.Components[0].ArtifactTargets[1], registry.Components[0].ArtifactTargets[0]
	if err := registry.Validate(); err == nil || !strings.Contains(err.Error(), "strictly identity ordered") {
		t.Fatalf("unordered artifact targets were accepted: %v", err)
	}
	registry = testRegistry()
	registry.Components[0].ArtifactTargets = []ArtifactTarget{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api-worker", Container: "api-worker", ContainerType: "container"}}
	if err := registry.Validate(); err == nil || !strings.Contains(err.Error(), "omit the primary") {
		t.Fatalf("artifact targets without primary were accepted: %v", err)
	}
	registry.Components[0].ArtifactTargets[0].ContainerType = "sidecar"
	if err := registry.Validate(); err == nil || !strings.Contains(err.Error(), "containerType") {
		t.Fatalf("invalid artifact target container type was accepted: %v", err)
	}
}

func TestEdgeGroupABTransitionIsStrictAndArtifactBound(t *testing.T) {
	component := Component{
		ID: "edge-worker-us", Family: "edge", IntentPath: "deploy/releases/edge-worker-us/intent.json", ManifestPath: "deploy/releases/edge-worker-us/resources.json",
		SourceRoots: []string{"Dockerfile.edge", "cmd/fugue-edge"}, Artifact: Artifact{Repository: "ghcr.io/example/fugue-edge", Dockerfile: "Dockerfile.edge", Context: ".", BuildPackage: "./cmd/fugue-edge"},
		ArtifactTargets: []ArtifactTarget{
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "fugue-fugue-edge-country-us-front", Container: "edge-front", ContainerType: "container"},
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "fugue-fugue-edge-country-us-worker-a", Container: "edge", ContainerType: "container"},
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "fugue-fugue-edge-country-us-worker-b", Container: "edge", ContainerType: "container"},
		},
		Workload:   Workload{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "fugue-fugue-edge-country-us-front", Container: "edge-front", FieldManager: "fugue-edge-worker-us-declarative", RolloutMode: "on-delete"},
		Transition: &Transition{Type: "edge-group-ab", EdgeGroupAB: &EdgeGroupABTransition{GroupID: "edge-group-country-us", FrontName: "fugue-fugue-edge-country-us-front", WorkerAName: "fugue-fugue-edge-country-us-worker-a", WorkerBName: "fugue-fugue-edge-country-us-worker-b", WorkerContainer: "edge", ActivationStatePath: "/var/lib/fugue-edge-front/activation.json", CASBinary: "/usr/local/bin/fugue-edge-front-cas", ExpectedNodes: 1, SoakSeconds: 180}},
		Health:     []HealthProbe{{Type: "daemonset", Name: "fugue-fugue-edge-country-us-front"}}, Concurrency: "fugue-production-edge-worker-us",
	}
	registry := Registry{APIVersion: RegistryAPIVersion, Kind: RegistryKind, Components: []Component{component}}
	if err := registry.Validate(); err != nil {
		t.Fatalf("valid edge group transition: %v", err)
	}
	registry.Components[0].Transition.EdgeGroupAB.WorkerBName = "another-worker"
	if err := registry.Validate(); err == nil || !strings.Contains(err.Error(), "not artifact-bound") {
		t.Fatalf("unbound edge worker was accepted: %v", err)
	}
	registry.Components[0].Transition.EdgeGroupAB.WorkerBName = "fugue-fugue-edge-country-us-worker-b"
	registry.Components[0].Transition.EdgeGroupAB.CASBinary = "/bin/sh"
	if err := registry.Validate(); err == nil || !strings.Contains(err.Error(), "identity is invalid") {
		t.Fatalf("arbitrary transition command was accepted: %v", err)
	}
}

func TestBuildPlanRequiresSameCommitIntent(t *testing.T) {
	registry := testRegistry()
	if _, err := BuildPlan(registry, testSHA1, testSHA2, []string{"internal/api/server.go"}); err == nil || !strings.Contains(err.Error(), "same-commit production intent") {
		t.Fatalf("runtime-only change did not fail closed: %v", err)
	}
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{
		"internal/api/server.go",
		"deploy/releases/api/intent.json",
	})
	if err != nil {
		t.Fatalf("build release plan: %v", err)
	}
	if len(plan.Releases) != 1 || plan.Releases[0].ComponentID != "api" {
		t.Fatalf("unexpected release plan: %+v", plan)
	}
	if len(plan.Releases[0].ChangedPaths) != 1 || plan.Releases[0].ChangedPaths[0] != "internal/api/server.go" {
		t.Fatalf("unexpected API changed paths: %+v", plan.Releases[0].ChangedPaths)
	}
}

func TestBuildPlanTreatsGoTestsAsNonRuntime(t *testing.T) {
	registry := testRegistry()
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{
		"internal/api/server_test.go",
		"cmd/fugue-api/main_test.go",
	})
	if err != nil {
		t.Fatalf("test-only plan: %v", err)
	}
	if len(plan.Releases) != 0 {
		t.Fatalf("Go tests selected runtime releases: %+v", plan.Releases)
	}
}

func TestBuildPlanRejectsMultiComponentProductionAtom(t *testing.T) {
	registry := testRegistry()
	_, err := BuildPlan(registry, testSHA1, testSHA2, []string{
		"cmd/fugue-api/main.go",
		"cmd/fugue-telemetry-agent/main.go",
		"deploy/releases/api/intent.json",
		"deploy/releases/telemetry/intent.json",
	})
	if err == nil || !strings.Contains(err.Error(), "multiple production intents") {
		t.Fatalf("multi-component runtime atom was accepted: %v", err)
	}
}

func TestBuildPlanSerializesGroupsThatShareOneArtifact(t *testing.T) {
	registry := testRegistry()
	registry.Components[0].SourceRoots = append(registry.Components[0].SourceRoots, "internal/shared-runtime")
	registry.Components[1].SourceRoots = append(registry.Components[1].SourceRoots, "internal/shared-runtime")
	registry.Components[1].Artifact = registry.Components[0].Artifact

	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{
		"internal/shared-runtime/runtime.go",
		"deploy/releases/api/intent.json",
	})
	if err != nil {
		t.Fatalf("select first shared-artifact group: %v", err)
	}
	if len(plan.Releases) != 1 || plan.Releases[0].ComponentID != "api" {
		t.Fatalf("shared artifact was not serialized to API: %+v", plan.Releases)
	}

	plan, err = BuildPlan(registry, testSHA1, testSHA2, []string{"deploy/releases/telemetry/intent.json"})
	if err != nil {
		t.Fatalf("select successor shared-artifact group: %v", err)
	}
	if len(plan.Releases) != 1 || plan.Releases[0].ComponentID != "telemetry" || len(plan.Releases[0].ChangedPaths) != 0 {
		t.Fatalf("intent-only successor was not isolated: %+v", plan.Releases)
	}
}

func TestBuildPlanSerializesSharedCodeAcrossDistinctArtifacts(t *testing.T) {
	registry := testRegistry()
	registry.Components[0].SourceRoots = append(registry.Components[0].SourceRoots, "internal/shared-runtime")
	registry.Components[1].SourceRoots = append(registry.Components[1].SourceRoots, "internal/shared-runtime")
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{
		"internal/shared-runtime/runtime.go",
		"deploy/releases/api/intent.json",
	})
	if err != nil {
		t.Fatalf("select first shared-source component: %v", err)
	}
	if len(plan.Releases) != 1 || plan.Releases[0].ComponentID != "api" ||
		!reflect.DeepEqual(plan.Releases[0].ChangedPaths, []string{"internal/shared-runtime/runtime.go"}) {
		t.Fatalf("shared source was not serialized to the selected intent: %+v", plan.Releases)
	}
}

func TestManifestChangeAlsoRequiresSameCommitIntent(t *testing.T) {
	registry := testRegistry()
	if _, err := BuildPlan(registry, testSHA1, testSHA2, []string{"deploy/releases/api/deployment.json"}); err == nil || !strings.Contains(err.Error(), "same-commit production intent") {
		t.Fatalf("manifest-only change did not fail closed: %v", err)
	}
}

func TestBindIntentsLocksPlanAndConsecutiveProductionAtom(t *testing.T) {
	registry := testRegistry()
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{
		"cmd/fugue-api/main.go",
		"deploy/releases/api/intent.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	current := Intent{APIVersion: IntentAPIVersion, Kind: IntentKind, Component: "api", Generation: 2, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: testSHA1, ExpectedPreviousManifestSHA: testSHA1, ExpectedPreviousOCIRevision: testSHA1, ExpectedPreviousImageDigest: testDigest, Rollback: "previous-git-lkg"}
	previous := current
	previous.Generation = 1
	previous.ExpectedPreviousConfigSHA = "3333333333333333333333333333333333333333"
	previous.ExpectedPreviousManifestSHA = previous.ExpectedPreviousConfigSHA
	previous.ExpectedPreviousOCIRevision = previous.ExpectedPreviousConfigSHA
	bound, err := BindIntents(registry, plan, map[string]Intent{"api": current}, map[string]Intent{"api": previous}, map[string]string{"api": testSHA1})
	if err != nil {
		t.Fatalf("bind intents: %v", err)
	}
	if !digestPattern.MatchString(bound.PlanDigest) || !digestPattern.MatchString(bound.Releases[0].IntentDigest) {
		t.Fatalf("bound plan is missing canonical digests: %+v", bound)
	}
	if bound.Releases[0].Artifact.Repository != "ghcr.io/example/fugue-api" || bound.Releases[0].Concurrency != "fugue-production-api" {
		t.Fatalf("bound plan lost static release mechanics: %+v", bound.Releases[0])
	}
	current.Generation = 4
	if _, err := BindIntents(registry, plan, map[string]Intent{"api": current}, map[string]Intent{"api": previous}, map[string]string{"api": testSHA1}); err == nil {
		t.Fatal("non-consecutive production atom was accepted")
	}
}

func TestBindIntentsUsesPriorComponentAtomAcrossUnrelatedCommits(t *testing.T) {
	registry := testRegistry()
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{"cmd/fugue-api/main.go", "deploy/releases/api/intent.json"})
	if err != nil {
		t.Fatal(err)
	}
	priorComponentSHA := "3333333333333333333333333333333333333333"
	previous := Intent{APIVersion: IntentAPIVersion, Kind: IntentKind, Component: "api", Generation: 1, ExpectedPreviousPresent: false, Rollback: "previous-git-lkg"}
	current := Intent{APIVersion: IntentAPIVersion, Kind: IntentKind, Component: "api", Generation: 2, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: priorComponentSHA, ExpectedPreviousManifestSHA: priorComponentSHA, ExpectedPreviousOCIRevision: priorComponentSHA, ExpectedPreviousImageDigest: testDigest, Rollback: "previous-git-lkg"}
	if _, err := BindIntents(registry, plan, map[string]Intent{"api": current}, map[string]Intent{"api": previous}, map[string]string{"api": priorComponentSHA}); err != nil {
		t.Fatalf("unrelated commits incorrectly broke the component predecessor binding: %v", err)
	}
	if _, err := BindIntents(registry, plan, map[string]Intent{"api": current}, map[string]Intent{"api": previous}, map[string]string{"api": testSHA1}); err == nil {
		t.Fatal("wrong previous component atom was accepted")
	}
}

func TestBindIntentsAllowsOneConsecutiveAttemptAgainstTheSameLKG(t *testing.T) {
	registry := testRegistry()
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{"deploy/releases/api/intent.json"})
	if err != nil {
		t.Fatal(err)
	}
	previous := Intent{APIVersion: IntentAPIVersion, Kind: IntentKind, Component: "api", Generation: 1, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: testSHA1, ExpectedPreviousManifestSHA: testSHA1, ExpectedPreviousOCIRevision: testSHA1, ExpectedPreviousImageDigest: testDigest, Rollback: "previous-git-lkg"}
	current := previous
	current.Generation = 2
	bound, err := BindIntents(registry, plan, map[string]Intent{"api": current}, map[string]Intent{"api": previous}, map[string]string{"api": "3333333333333333333333333333333333333333"})
	if err != nil {
		t.Fatalf("consecutive attempt against the same LKG was rejected: %v", err)
	}
	if len(bound.Releases) != 1 || !bound.Releases[0].RetrySameLKG {
		t.Fatalf("same-LKG attempt was not bound explicitly: %+v", bound.Releases)
	}
	current.ExpectedPreviousImageDigest = "sha256:" + strings.Repeat("c", 64)
	if _, err := BindIntents(registry, plan, map[string]Intent{"api": current}, map[string]Intent{"api": previous}, map[string]string{"api": "3333333333333333333333333333333333333333"}); err == nil {
		t.Fatal("attempt with a changed LKG was accepted")
	}
}

func TestBindIntentsAllowsExplicitFailedAtomSupersession(t *testing.T) {
	registry := testRegistry()
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{"deploy/releases/api/intent.json"})
	if err != nil {
		t.Fatal(err)
	}
	failedSHA := "3333333333333333333333333333333333333333"
	previous := Intent{APIVersion: IntentAPIVersion, Kind: IntentKind, Component: "api", Generation: 2, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: testSHA1, ExpectedPreviousManifestSHA: testSHA1, ExpectedPreviousOCIRevision: testSHA1, ExpectedPreviousImageDigest: testDigest, Rollback: "previous-git-lkg"}
	current := Intent{APIVersion: IntentAPIVersion, Kind: IntentKind, Component: "api", Generation: 3, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: testSHA1, ExpectedPreviousManifestSHA: testSHA1, ExpectedPreviousOCIRevision: testSHA1, ExpectedPreviousImageDigest: testDigest, SupersedesFailedConfigSHA: failedSHA, Rollback: "previous-git-lkg"}
	bound, err := BindIntents(registry, plan, map[string]Intent{"api": current}, map[string]Intent{"api": previous}, map[string]string{"api": failedSHA})
	if err != nil {
		t.Fatalf("explicit failed atom supersession was rejected: %v", err)
	}
	if got := bound.Releases[0]; got.SupersedesFailedConfigSHA != failedSHA || got.RetrySameLKG {
		t.Fatalf("failed atom supersession was not bound exactly: %+v", got)
	}
	current.SupersedesFailedConfigSHA = testSHA2
	if _, err := BindIntents(registry, plan, map[string]Intent{"api": current}, map[string]Intent{"api": previous}, map[string]string{"api": failedSHA}); err == nil {
		t.Fatal("wrong failed atom identity was accepted")
	}
}

func TestBindIntentsAllowsAbsentLKGRetry(t *testing.T) {
	registry := testRegistry()
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{"deploy/releases/api/intent.json"})
	if err != nil {
		t.Fatal(err)
	}
	previous := Intent{APIVersion: IntentAPIVersion, Kind: IntentKind, Component: "api", Generation: 1, ExpectedPreviousPresent: false, Rollback: "previous-git-lkg"}
	current := previous
	current.Generation = 2
	bound, err := BindIntents(registry, plan, map[string]Intent{"api": current}, map[string]Intent{"api": previous}, map[string]string{"api": testSHA1})
	if err != nil {
		t.Fatalf("absent-LKG retry was rejected: %v", err)
	}
	if len(bound.Releases) != 1 || !bound.Releases[0].RetrySameLKG || bound.Releases[0].ExpectedPreviousPresent {
		t.Fatalf("absent-LKG retry was not preserved: %+v", bound.Releases)
	}
	current.ExpectedPreviousConfigSHA = testSHA1
	if _, err := BindIntents(registry, plan, map[string]Intent{"api": current}, map[string]Intent{"api": previous}, map[string]string{"api": testSHA1}); err == nil {
		t.Fatal("absent-LKG retry with a predecessor identity was accepted")
	}
}
