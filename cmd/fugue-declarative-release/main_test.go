package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"fugue/internal/declarativerelease"
)

func TestInitialBootstrapFailedAtomHistoricalRootIsExact(t *testing.T) {
	const failed = "1111111111111111111111111111111111111111"
	const lkg = "2222222222222222222222222222222222222222"
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-front"}
	component := declarativerelease.Component{
		ID: "edge-client-test", MigrationState: "adopting", BootstrapLKGPath: "deploy/releases/edge-client-test/lkg.json",
		Workload:         declarativerelease.Workload{APIVersion: identity.APIVersion, Kind: identity.Kind, Namespace: identity.Namespace, Name: identity.Name, Container: "front"},
		BootstrapRuntime: &declarativerelease.BootstrapRuntime{Resource: identity, Container: "front", ImageDigest: "sha256:" + strings.Repeat("a", 64), OCIRevision: strings.Repeat("3", 40)},
		OwnershipAdoption: &declarativerelease.OwnershipAdoption{LegacyFieldManager: "helm", Resources: []declarativerelease.OwnershipAdoptionScope{{
			Identity: identity, Fields: []string{"/spec/template/spec/containers[name=front]/image"},
		}}},
	}
	registry := declarativerelease.Registry{Components: []declarativerelease.Component{component}}
	prior := declarativerelease.Intent{APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind, Component: component.ID,
		Generation: 1, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: lkg, ExpectedPreviousManifestSHA: lkg,
		ExpectedPreviousOCIRevision: lkg, ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("b", 64), Rollback: "previous-git-lkg"}
	current := prior
	current.Generation = 2
	current.SupersedesFailedConfigSHA = failed
	if !initialBootstrapFailedAtomRecoversSameLKG(registry, component.ID, current, prior, failed) {
		t.Fatal("exact failed initial bootstrap successor was rejected")
	}
	for name, mutate := range map[string]func(*declarativerelease.Intent, *declarativerelease.Intent, *declarativerelease.Registry){
		"missing prior": func(_ *declarativerelease.Intent, prior *declarativerelease.Intent, _ *declarativerelease.Registry) {
			prior.Generation = 0
		},
		"changed LKG": func(current *declarativerelease.Intent, _ *declarativerelease.Intent, _ *declarativerelease.Registry) {
			current.ExpectedPreviousImageDigest = "sha256:" + strings.Repeat("c", 64)
		},
		"changed scope": func(_ *declarativerelease.Intent, _ *declarativerelease.Intent, registry *declarativerelease.Registry) {
			registry.Components[0].OwnershipAdoption = nil
		},
		"ordinary successor": func(current *declarativerelease.Intent, _ *declarativerelease.Intent, _ *declarativerelease.Registry) {
			current.SupersedesFailedConfigSHA = ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			gotCurrent, gotPrior, gotRegistry := current, prior, registry
			gotRegistry.Components = append([]declarativerelease.Component(nil), registry.Components...)
			mutate(&gotCurrent, &gotPrior, &gotRegistry)
			if initialBootstrapFailedAtomRecoversSameLKG(gotRegistry, component.ID, gotCurrent, gotPrior, failed) {
				t.Fatal("invalid failed initial bootstrap successor was accepted")
			}
		})
	}
}

func TestLoadLKGManifestUsesBootstrapOnlyForExplicitAdoption(t *testing.T) {
	root := t.TempDir()
	previousDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previousDirectory) })
	want := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[],"kind":"ComponentResourceSet"}`)
	writeFile(t, "deploy/releases/telemetry/lkg.json", want)
	release := declarativerelease.PlanRelease{
		IntentGeneration: 3, ExpectedPreviousPresent: true, RetrySameLKG: true,
		BootstrapLKGPath:          "deploy/releases/telemetry/lkg.json",
		ExpectedPreviousConfigSHA: strings.Repeat("1", 40), ManifestPath: "deploy/releases/telemetry/resources.json",
		MigrationState: "adopting",
	}
	got, err := loadLKGManifest(release)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("load retry LKG: got=%s err=%v", got, err)
	}
	release.MigrationState = "independent"
	if _, err := loadLKGManifest(release); err == nil || !strings.Contains(err.Error(), "previous production registry") {
		t.Fatalf("independent same-LKG retry fell back to bootstrap LKG: %v", err)
	}
}

func TestLoadLKGManifestUsesReceiptWhenProductionSourcePredatesRegistry(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	previousDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previousDirectory) })
	release := declarativerelease.PlanRelease{
		ComponentID: "image-cache", MigrationState: "independent", ExpectedPreviousPresent: true, RetrySameLKG: true,
		AdoptionReceiptPath:         "deploy/releases/image-cache/adoption-receipt.json",
		ExpectedPreviousConfigSHA:   "e8f3781e3c9282e9daf24842c10cef3eab9f5497",
		ExpectedPreviousManifestSHA: "e8f3781e3c9282e9daf24842c10cef3eab9f5497",
		ExpectedPreviousOCIRevision: "e8f3781e3c9282e9daf24842c10cef3eab9f5497",
		ExpectedPreviousImageDigest: "sha256:18bf0bcc6d3b69a73aed8118acbb98b508216977ddf5b4c4d0d9f6ee3c5494d4",
		Workload:                    declarativerelease.Workload{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "fugue-fugue-image-cache", Container: "image-cache"},
	}
	got, err := loadLKGManifest(release)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("deploy/releases/image-cache/lkg.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("receipt-bound historical LKG bytes changed")
	}
}

func TestReceiptBoundLKGFallbackFailsClosed(t *testing.T) {
	t.Run("historical registry exists but is invalid", func(t *testing.T) {
		release := receiptBoundImageCacheFixture(t)
		fakeBin := filepath.Join(t.TempDir(), "bin")
		writeFile(t, filepath.Join(fakeBin, "git"), []byte("#!/bin/sh\nprintf '{'\n"))
		if err := os.Chmod(filepath.Join(fakeBin, "git"), 0o755); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
		if _, err := loadLKGManifest(release); err == nil || !strings.Contains(err.Error(), "decode previous production registry") ||
			strings.Contains(err.Error(), "receipt-bound LKG fallback") {
			t.Fatalf("invalid historical registry did not fail closed: %v", err)
		}
	})

	tests := map[string]func(t *testing.T, release *declarativerelease.PlanRelease){
		"receipt is invalid": func(t *testing.T, _ *declarativerelease.PlanRelease) {
			writeFile(t, "deploy/releases/image-cache/adoption-receipt.json", []byte(`{}`))
		},
		"receipt identity drifted": func(_ *testing.T, release *declarativerelease.PlanRelease) {
			release.ExpectedPreviousImageDigest = "sha256:" + strings.Repeat("9", 64)
		},
		"receipt path escapes component": func(_ *testing.T, release *declarativerelease.PlanRelease) {
			release.AdoptionReceiptPath = "deploy/releases/image-cache/../other/adoption-receipt.json"
		},
		"receipt path is removed": func(_ *testing.T, release *declarativerelease.PlanRelease) {
			release.AdoptionReceiptPath = ""
		},
		"LKG template identity drifted": func(t *testing.T, _ *declarativerelease.PlanRelease) {
			raw, err := os.ReadFile("deploy/releases/image-cache/lkg.json")
			if err != nil {
				t.Fatal(err)
			}
			raw = bytes.Replace(raw, []byte("e8f3781e3c9282e9daf24842c10cef3eab9f5497"), []byte(strings.Repeat("9", 40)), 1)
			writeFile(t, "deploy/releases/image-cache/lkg.json", raw)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			release := receiptBoundImageCacheFixture(t)
			mutate(t, &release)
			if _, err := loadReceiptBoundLKGManifest(release); err == nil {
				t.Fatal("invalid receipt-bound LKG fallback was accepted")
			}
		})
	}
}

func receiptBoundImageCacheFixture(t *testing.T) declarativerelease.PlanRelease {
	t.Helper()
	sourceRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for _, name := range []string{
		"deploy/releases/components.json",
		"deploy/releases/edge-groups.json",
		"deploy/releases/image-cache/adoption-receipt.json",
		"deploy/releases/image-cache/lkg.json",
	} {
		raw, err := os.ReadFile(filepath.Join(sourceRoot, name))
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(root, name), raw)
	}
	previousDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previousDirectory) })
	return declarativerelease.PlanRelease{
		ComponentID: "image-cache", MigrationState: "independent", ExpectedPreviousPresent: true, RetrySameLKG: true,
		AdoptionReceiptPath:         "deploy/releases/image-cache/adoption-receipt.json",
		ExpectedPreviousConfigSHA:   "e8f3781e3c9282e9daf24842c10cef3eab9f5497",
		ExpectedPreviousManifestSHA: "e8f3781e3c9282e9daf24842c10cef3eab9f5497",
		ExpectedPreviousOCIRevision: "e8f3781e3c9282e9daf24842c10cef3eab9f5497",
		ExpectedPreviousImageDigest: "sha256:18bf0bcc6d3b69a73aed8118acbb98b508216977ddf5b4c4d0d9f6ee3c5494d4",
		Workload:                    declarativerelease.Workload{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "fugue-fugue-image-cache", Container: "image-cache"},
	}
}

func TestLoadLKGManifestUsesTheHistoricalRegistryManifestPath(t *testing.T) {
	root := t.TempDir()
	previousDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	componentsRaw, err := os.ReadFile(filepath.Join(previousDirectory, "../..", "deploy/releases/components.json"))
	if err != nil {
		t.Fatal(err)
	}
	edgeRaw, err := os.ReadFile(filepath.Join(previousDirectory, "../..", "deploy/releases/edge-groups.json"))
	if err != nil {
		t.Fatal(err)
	}
	edge, err := declarativerelease.DecodeEdgeGroupRegistry(bytes.NewReader(edgeRaw))
	if err != nil {
		t.Fatal(err)
	}
	const oldPath = "internal/edge/component/previous-worker-us.json"
	for index := range edge.Groups {
		if edge.Groups[index].Worker.ID == "edge-worker-us" {
			edge.Groups[index].Worker.ManifestPath = oldPath
		}
	}
	edgeRaw, err = declarativerelease.CanonicalJSON(edge)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previousDirectory) })
	runGit(t, "init", "--initial-branch=main")
	runGit(t, "config", "user.email", "release@example.test")
	runGit(t, "config", "user.name", "Release Test")
	writeFile(t, "deploy/releases/components.json", componentsRaw)
	writeFile(t, "deploy/releases/edge-groups.json", append(edgeRaw, '\n'))
	want := []byte(`{"historical":"worker-us"}`)
	writeFile(t, oldPath, want)
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "historical registry")
	revisionRaw, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	revision := strings.TrimSpace(string(revisionRaw))
	release := declarativerelease.PlanRelease{ComponentID: "edge-worker-us", ExpectedPreviousPresent: true,
		ExpectedPreviousConfigSHA: revision, ManifestPath: "internal/edge/component/current-worker-us.json", MigrationState: "independent"}
	got, err := loadLKGManifest(release)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("load historical manifest path: got=%s err=%v", got, err)
	}
}

func TestHistoricalEdgeGroupReaderProjectsExactLegacyManifestPath(t *testing.T) {
	const revision = "4d9db8b777ce4644df2d064aeca3df2718c46602"
	raw, err := exec.Command("git", "show", revision+":deploy/releases/edge-groups.json").Output()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := declarativerelease.DecodeEdgeGroupRegistry(bytes.NewReader(raw)); err == nil {
		t.Fatal("current Edge group decoder accepted historical adoption metadata")
	}
	paths, err := decodeHistoricalEdgeGroupManifestPaths(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if got := paths["edge-control-de"]; got != "internal/edgecontrol/component/resources.authority.group.json" {
		t.Fatalf("historical DE Edge Control manifest path=%q", got)
	}
	if got, err := historicalComponentManifestPath(revision, "edge-control-de"); err != nil || got != paths["edge-control-de"] {
		t.Fatalf("historical component manifest path=%q err=%v", got, err)
	}

	mutations := map[string]func(map[string]any){
		"unknown field": func(root map[string]any) { root["unknown"] = true },
		"duplicate group": func(root map[string]any) {
			groups := root["groups"].([]any)
			root["groups"] = append(groups, groups[0])
		},
		"wrong component ID": func(root map[string]any) {
			groups := root["groups"].([]any)
			group := groups[0].(map[string]any)
			group["control"].(map[string]any)["id"] = "edge-control-wrong"
		},
		"empty manifest path": func(root map[string]any) {
			groups := root["groups"].([]any)
			group := groups[0].(map[string]any)
			group["control"].(map[string]any)["manifestPath"] = ""
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			var value map[string]any
			if err := json.Unmarshal(raw, &value); err != nil {
				t.Fatal(err)
			}
			mutate(value)
			candidate, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeHistoricalEdgeGroupManifestPaths(bytes.NewReader(candidate)); err == nil {
				t.Fatal("invalid historical Edge group registry was accepted")
			}
		})
	}
}

func TestPlanCommandBindsFirstProductionAtom(t *testing.T) {
	root := t.TempDir()
	previousDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previousDirectory) })
	runGit(t, "init", "--initial-branch=main")
	runGit(t, "config", "user.email", "release@example.test")
	runGit(t, "config", "user.name", "Release Test")
	writeFile(t, "go.mod", []byte("module example.test/release\n\ngo 1.22\n"))

	registry := declarativerelease.Registry{
		APIVersion: declarativerelease.RegistryAPIVersion,
		Kind:       declarativerelease.RegistryKind,
		Components: []declarativerelease.Component{{
			ID: "api", Family: "control-plane",
			IntentPath: "deploy/releases/api/intent.json", ManifestPath: "deploy/releases/api/deployment.json",
			SourceRoots: []string{"Dockerfile.api", "cmd/fugue-api"},
			Artifact:    declarativerelease.Artifact{Repository: "ghcr.io/example/fugue-api", Dockerfile: "Dockerfile.api", Context: ".", BuildPackage: "./cmd/fugue-api"},
			Workload:    declarativerelease.Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api", FieldManager: "fugue-api-declarative", Replicas: 2, RolloutMode: "rolling"},
			Health:      []declarativerelease.HealthProbe{{Type: "deployment", Name: "fugue-fugue-api"}},
			Concurrency: "fugue-production-api", MigrationState: "independent",
		}},
	}
	writeJSON(t, "deploy/releases/components.json", registry)
	writeFile(t, "Dockerfile.api", []byte("FROM scratch\n"))
	writeFile(t, "cmd/fugue-api/main.go", []byte("package main\nfunc main() {}\n"))
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "base")
	base := runGit(t, "rev-parse", "HEAD")

	writeFile(t, "cmd/fugue-api/main.go", []byte("package main\nfunc main() { println(\"v2\") }\n"))
	intent := declarativerelease.Intent{
		APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind,
		Component: "api", Generation: 1, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: base,
		ExpectedPreviousManifestSHA: base,
		ExpectedPreviousOCIRevision: base,
		ExpectedPreviousImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Rollback:                    "previous-git-lkg",
	}
	writeJSON(t, "deploy/releases/api/intent.json", intent)
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "release api")
	head := runGit(t, "rev-parse", "HEAD")
	writeFile(t, "changed.txt", []byte("cmd/fugue-api/main.go\ndeploy/releases/api/intent.json\n"))

	var output bytes.Buffer
	if err := run([]string{"plan", "deploy/releases/components.json", base, head, "changed.txt"}, &output); err != nil {
		t.Fatalf("plan command: %v", err)
	}
	var plan declarativerelease.Plan
	if err := json.Unmarshal(output.Bytes(), &plan); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if plan.BaseSHA != base || plan.HeadSHA != head || len(plan.Releases) != 1 || plan.Releases[0].ComponentID != "api" {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	if plan.Releases[0].IntentGeneration != 1 || plan.Releases[0].ExpectedPreviousImageDigest != intent.ExpectedPreviousImageDigest {
		t.Fatalf("intent was not bound: %+v", plan.Releases[0])
	}
	writeFile(t, "README.md", []byte("unrelated after runtime atom\n"))
	runGit(t, "add", "README.md")
	runGit(t, "commit", "-m", "second pushed commit")
	multiHead := runGit(t, "rev-parse", "HEAD")
	if err := run([]string{"plan", "deploy/releases/components.json", base, multiHead, "changed.txt"}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "one direct-child commit") {
		t.Fatalf("multi-commit runtime push was accepted: %v", err)
	}
}

func TestComponentDependencyGraphIncludesTransitiveRepositoryPackages(t *testing.T) {
	file, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	registry, decodeErr := declarativerelease.DecodeRegistry(file)
	closeErr := file.Close()
	if decodeErr != nil || closeErr != nil {
		t.Fatalf("decode registry: decode=%v close=%v", decodeErr, closeErr)
	}
	for index := range registry.Components {
		if registry.Components[index].ID == "api" {
			registry.Components[index].MigrationState = "independent"
			registry.Components[index].OwnershipAdoption = nil
		}
	}
	expanded, err := expandComponentDependencyRoots(registry)
	if err != nil {
		t.Fatal(err)
	}
	foundAPIHTTP := false
	for _, component := range expanded.Components {
		if !sort.StringsAreSorted(component.SourceRoots) {
			t.Fatalf("%s expanded roots are not canonical", component.ID)
		}
		if component.ID == "api" {
			for _, root := range component.SourceRoots {
				if root == "internal/httpx" {
					foundAPIHTTP = true
				}
			}
		}
	}
	if !foundAPIHTTP {
		t.Fatal("API transitive internal/httpx dependency was omitted from the impact graph")
	}
}

func TestReconcileRefusesToReplaceAnExistingCanonicalTerminalReceipt(t *testing.T) {
	result := declarativerelease.ExecutionResult{
		APIVersion: declarativerelease.ExecutionPlanAPIVersion, Kind: declarativerelease.ExecutionResultKind,
		Component: "api", ConfigSHA: strings.Repeat("a", 40), ExecutionPlanDigest: "sha256:" + strings.Repeat("b", 64),
		Status: "recovery-required", Reason: "lease-release-unproven",
	}
	unsigned, err := declarativerelease.CanonicalJSON(result)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(unsigned)
	result.ReceiptDigest = fmt.Sprintf("sha256:%x", digest)
	filename := filepath.Join(t.TempDir(), "result.json")
	writeJSON(t, filename, result)
	err = run([]string{"reconcile", filepath.Join(t.TempDir(), "missing-plan"), filename}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "already emitted a canonical terminal receipt") {
		t.Fatalf("canonical executor result was replaced: %v", err)
	}
}

func TestEveryCanonicalExecutorResultFinalizesItsHeldComponentLease(t *testing.T) {
	for _, status := range []string{"verified", "compensated", "failed-no-write", "recovery-required"} {
		if !finalizeComponentLeaseStatus(status) {
			t.Fatalf("terminal status %q was not marked for lease finalization", status)
		}
	}
}

func TestPlanCommandBindsSuccessorToLastComponentIntentCommit(t *testing.T) {
	root := t.TempDir()
	previousDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previousDirectory) })
	runGit(t, "init", "--initial-branch=main")
	runGit(t, "config", "user.email", "release@example.test")
	runGit(t, "config", "user.name", "Release Test")
	writeFile(t, "go.mod", []byte("module example.test/release\n\ngo 1.22\n"))

	registry := declarativerelease.Registry{APIVersion: declarativerelease.RegistryAPIVersion, Kind: declarativerelease.RegistryKind, Components: []declarativerelease.Component{{
		ID: "api", Family: "control-plane", IntentPath: "deploy/releases/api/intent.json", ManifestPath: "deploy/releases/api/deployment.json",
		SourceRoots: []string{"Dockerfile.api", "cmd/fugue-api"}, Artifact: declarativerelease.Artifact{Repository: "ghcr.io/example/fugue-api", Dockerfile: "Dockerfile.api", Context: ".", BuildPackage: "./cmd/fugue-api"},
		Workload: declarativerelease.Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api", FieldManager: "fugue-api-declarative", Replicas: 2, RolloutMode: "rolling"},
		Health:   []declarativerelease.HealthProbe{{Type: "deployment", Name: "fugue-fugue-api"}}, Concurrency: "fugue-production-api", MigrationState: "independent",
	}}}
	writeJSON(t, "deploy/releases/components.json", registry)
	writeFile(t, "Dockerfile.api", []byte("FROM scratch\n"))
	writeFile(t, "cmd/fugue-api/main.go", []byte("package main\nfunc main() {}\n"))
	writeJSON(t, "deploy/releases/api/intent.json", declarativerelease.Intent{APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind, Component: "api", Generation: 1, ExpectedPreviousPresent: false, Rollback: "previous-git-lkg"})
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "first api atom")
	firstAtom := runGit(t, "rev-parse", "HEAD")

	writeFile(t, "README.md", []byte("unrelated\n"))
	runGit(t, "add", "README.md")
	runGit(t, "commit", "-m", "unrelated change")
	base := runGit(t, "rev-parse", "HEAD")
	writeFile(t, "cmd/fugue-api/main.go", []byte("package main\nfunc main() { println(\"v2\") }\n"))
	writeJSON(t, "deploy/releases/api/intent.json", declarativerelease.Intent{APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind, Component: "api", Generation: 2, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: firstAtom, ExpectedPreviousManifestSHA: firstAtom, ExpectedPreviousOCIRevision: firstAtom, ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("a", 64), Rollback: "previous-git-lkg"})
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "second api atom")
	head := runGit(t, "rev-parse", "HEAD")
	writeFile(t, "changed.txt", []byte("cmd/fugue-api/main.go\ndeploy/releases/api/intent.json\n"))

	var output bytes.Buffer
	if err := run([]string{"plan", "deploy/releases/components.json", base, head, "changed.txt"}, &output); err != nil {
		t.Fatalf("successor plan across unrelated commits: %v", err)
	}
	var plan declarativerelease.Plan
	if err := json.Unmarshal(output.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Releases) != 1 || plan.Releases[0].ExpectedPreviousConfigSHA != firstAtom {
		t.Fatalf("successor was not bound to its own prior atom: %+v", plan.Releases)
	}
}

func TestPlanCommandRejectsExtraArguments(t *testing.T) {
	if err := run([]string{"plan", "a", "b", "c", "d", "extra"}, &bytes.Buffer{}); err == nil {
		t.Fatal("extra argument was accepted")
	}
}

func TestReceiptCommandMaterializesCanonicalArtifact(t *testing.T) {
	plan := declarativerelease.Plan{
		APIVersion: declarativerelease.IntentAPIVersion, Kind: "ProductionReleasePlan",
		BaseSHA: "1111111111111111111111111111111111111111", HeadSHA: "2222222222222222222222222222222222222222",
		Releases: []declarativerelease.PlanRelease{{
			ComponentID: "api", IntentPath: "deploy/releases/api/intent.json",
			IntentDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", IntentGeneration: 1, ExpectedPreviousPresent: true,
			ExpectedPreviousConfigSHA:   "1111111111111111111111111111111111111111",
			ExpectedPreviousManifestSHA: "1111111111111111111111111111111111111111",
			ExpectedPreviousOCIRevision: "1111111111111111111111111111111111111111",
			ExpectedPreviousImageDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			ManifestPath:                "deploy/releases/api/deployment.json",
			Artifact:                    declarativerelease.Artifact{Repository: "ghcr.io/example/fugue-api", Dockerfile: "Dockerfile.api", Context: ".", BuildPackage: "./cmd/fugue-api"},
			Workload:                    declarativerelease.Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api", FieldManager: "fugue-api-declarative", Replicas: 2, RolloutMode: "rolling"},
			Health:                      []declarativerelease.HealthProbe{{Type: "deployment", Name: "fugue-fugue-api"}}, Concurrency: "fugue-production-api",
		}},
	}
	unsigned, err := declarativerelease.CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestBytes(unsigned)
	verification := declarativerelease.RegistryVerification{
		Image:          "ghcr.io/example/fugue-api@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		IndexDigest:    "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ManifestDigest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		ConfigDigest:   "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		OCIRevision:    plan.HeadSHA, Platform: "linux/amd64", Verification: "registry_manifest_config_and_layer_get",
		BlobCount: 2, LayerProbeCount: 1, RequestCount: 6, TotalLayerBytes: 100,
	}
	root := t.TempDir()
	planPath := filepath.Join(root, "plan.json")
	verificationPath := filepath.Join(root, "verification.json")
	writeAbsoluteJSON(t, planPath, plan)
	writeAbsoluteJSON(t, verificationPath, verification)
	var output bytes.Buffer
	if err := run([]string{"receipt", planPath, "api", verificationPath}, &output); err != nil {
		t.Fatalf("receipt command: %v", err)
	}
	if _, err := declarativerelease.DecodeArtifactReceipt(bytes.NewReader(bytes.TrimSpace(output.Bytes()))); err != nil {
		t.Fatalf("receipt output is invalid: %v", err)
	}
}

func TestEmitGitHubOutputUsesCanonicalSingleComponentMatrix(t *testing.T) {
	plan := declarativerelease.Plan{
		APIVersion: declarativerelease.IntentAPIVersion, Kind: "ProductionReleasePlan",
		BaseSHA: "1111111111111111111111111111111111111111", HeadSHA: "2222222222222222222222222222222222222222",
		Releases: []declarativerelease.PlanRelease{
			{ComponentID: "api", IntentDigest: "sha256:" + strings.Repeat("a", 64), IntentGeneration: 1, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: strings.Repeat("1", 40), ExpectedPreviousManifestSHA: strings.Repeat("1", 40), ExpectedPreviousOCIRevision: strings.Repeat("1", 40), ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("b", 64), Concurrency: "fugue-production-api", Artifact: declarativerelease.Artifact{Repository: "ghcr.io/example/fugue-api"}, Workload: declarativerelease.Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api", FieldManager: "fugue-api-declarative", Replicas: 2, RolloutMode: "rolling"}},
		},
	}
	unsigned, err := declarativerelease.CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestBytes(unsigned)
	root := t.TempDir()
	planPath := filepath.Join(root, "plan.json")
	outputPath := filepath.Join(root, "github-output")
	writeAbsoluteJSON(t, planPath, plan)
	if err := os.WriteFile(outputPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runEmitGitHubOutput([]string{"emit-github-output", planPath, outputPath}); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	apiLane := sha256.Sum256([]byte("ghcr.io/example/fugue-api"))
	want := fmt.Sprintf(
		"release_count=1\nrelease_matrix={\"include\":[{\"build_lane\":\"%x\",\"component\":\"api\"}]}\nrelease_components=[\"api\"]\nedge_control_count=0\nedge_control_matrix={\"include\":[]}\nedge_worker_count=0\nedge_worker_matrix={\"include\":[]}\n",
		apiLane[:8],
	)
	if string(content) != want {
		t.Fatalf("unexpected GitHub output:\n%s", content)
	}
}

func writeJSON(t *testing.T, name string, value any) {
	t.Helper()
	content, err := declarativerelease.CanonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, name, append(content, '\n'))
}

func writeFile(t *testing.T, name string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeAbsoluteJSON(t *testing.T, name string, value any) {
	t.Helper()
	content, err := declarativerelease.CanonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, append(content, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func digestBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return fmt.Sprintf("sha256:%x", sum)
}

func runGit(t *testing.T, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return string(bytes.TrimSpace(output))
}
