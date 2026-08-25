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
			Concurrency: "fugue-production-api",
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

func TestProductionRuntimeDiffDoesNotImportAnotherComponentIntent(t *testing.T) {
	root := t.TempDir()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previous) })
	runGit(t, "init", "--initial-branch=main")
	runGit(t, "config", "user.email", "release@example.test")
	runGit(t, "config", "user.name", "Release Test")
	writeFile(t, "go.mod", []byte("module example.test/release\n\ngo 1.22\n"))
	writeFile(t, "shared/value.go", []byte("package shared\nconst Value = 1\n"))
	writeFile(t, "cmd/a/main.go", []byte("package main\nimport _ \"example.test/release/shared\"\nfunc main(){}\n"))
	writeFile(t, "cmd/b/main.go", []byte("package main\nimport _ \"example.test/release/shared\"\nfunc main(){}\n"))
	registry := declarativerelease.Registry{APIVersion: declarativerelease.RegistryAPIVersion, Kind: declarativerelease.RegistryKind}
	for _, id := range []string{"a", "b"} {
		registry.Components = append(registry.Components, declarativerelease.Component{ID: id, Family: "test", IntentPath: "deploy/" + id + "/intent.json", ManifestPath: "deploy/" + id + "/resources.json",
			SourceRoots: []string{"cmd/" + id}, Artifact: declarativerelease.Artifact{Repository: "ghcr.io/example/" + id, Dockerfile: "Dockerfile." + id, Context: ".", BuildPackage: "./cmd/" + id},
			Workload: declarativerelease.Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "test", Name: id, Container: id, FieldManager: id, Replicas: 1, RolloutMode: "rolling"},
			Health:   []declarativerelease.HealthProbe{{Type: "deployment", Name: id}}, Concurrency: "fugue-production-" + id})
		writeFile(t, "Dockerfile."+id, []byte("FROM scratch\n"))
		writeFile(t, "deploy/"+id+"/resources.json", []byte("{}\n"))
	}
	writeJSON(t, "deploy/releases/components.json", registry)
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "base")
	base := runGit(t, "rev-parse", "HEAD")
	writeFile(t, "shared/value.go", []byte("package shared\nconst Value = 2\n"))
	writeJSON(t, "deploy/a/intent.json", declarativerelease.Intent{APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind, Component: "a", Generation: 1,
		ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: base, ExpectedPreviousManifestSHA: base, ExpectedPreviousOCIRevision: base,
		ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("a", 64), Rollback: "previous-git-lkg"})
	writeFile(t, "deploy/b/intent.json", []byte("old unrelated intent\n"))
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "release a")
	head := runGit(t, "rev-parse", "HEAD")
	expanded, err := expandComponentDependencyRoots(registry)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := addProductionRuntimeChanges(expanded, head, []string{"shared/value.go", "deploy/a/intent.json"})
	if err != nil {
		t.Fatal(err)
	}
	if containsPath(paths, "deploy/b/intent.json") || containsPath(paths, "deploy/b/resources.json") {
		t.Fatalf("selected component runtime diff imported another lane: %v", paths)
	}
	if _, err := addProductionRuntimeChanges(expanded, head, []string{"deploy/a/intent.json", "deploy/b/intent.json"}); err == nil ||
		!strings.Contains(err.Error(), "different artifacts") {
		t.Fatalf("different artifact intents were accepted: %v", err)
	}

	expanded.Components[1].Artifact = expanded.Components[0].Artifact
	writeJSON(t, "deploy/b/intent.json", declarativerelease.Intent{APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind, Component: "b", Generation: 1,
		ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: base, ExpectedPreviousManifestSHA: base, ExpectedPreviousOCIRevision: base,
		ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("b", 64), Rollback: "previous-git-lkg"})
	paths, err = addProductionRuntimeChanges(expanded, head, []string{"shared/value.go", "deploy/a/intent.json", "deploy/b/intent.json"})
	if err != nil {
		t.Fatalf("shared artifact intents were rejected: %v", err)
	}
	if !containsPath(paths, "deploy/a/intent.json") || !containsPath(paths, "deploy/b/intent.json") || !containsPath(paths, "shared/value.go") {
		t.Fatalf("shared artifact runtime diff lost a selected lane: %v", paths)
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
		Health:   []declarativerelease.HealthProbe{{Type: "deployment", Name: "fugue-fugue-api"}}, Concurrency: "fugue-production-api",
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

func TestPlanCommandAcceptsMergeCommitAsLivePredecessor(t *testing.T) {
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
		Health:   []declarativerelease.HealthProbe{{Type: "deployment", Name: "fugue-fugue-api"}}, Concurrency: "fugue-production-api",
	}}}
	writeJSON(t, "deploy/releases/components.json", registry)
	writeFile(t, "Dockerfile.api", []byte("FROM scratch\n"))
	writeFile(t, "cmd/fugue-api/main.go", []byte("package main\nfunc main() {}\n"))
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "base")

	runGit(t, "checkout", "-b", "release-v1")
	writeFile(t, "cmd/fugue-api/main.go", []byte("package main\nfunc main() { println(\"v1\") }\n"))
	writeJSON(t, "deploy/releases/api/intent.json", declarativerelease.Intent{
		APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind, Component: "api",
		Generation: 1, ExpectedPreviousPresent: false, Rollback: "previous-git-lkg",
	})
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "release api v1")
	runGit(t, "checkout", "main")
	runGit(t, "merge", "--no-ff", "release-v1", "-m", "merge api v1")
	liveMerge := runGit(t, "rev-parse", "HEAD")

	runGit(t, "checkout", "-b", "release-v2")
	writeFile(t, "cmd/fugue-api/main.go", []byte("package main\nfunc main() { println(\"v2\") }\n"))
	writeJSON(t, "deploy/releases/api/intent.json", declarativerelease.Intent{
		APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind, Component: "api",
		Generation: 2, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: liveMerge,
		ExpectedPreviousManifestSHA: liveMerge, ExpectedPreviousOCIRevision: liveMerge,
		ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("a", 64), Rollback: "previous-git-lkg",
	})
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "release api v2")
	head := runGit(t, "rev-parse", "HEAD")
	writeFile(t, "changed.txt", []byte("cmd/fugue-api/main.go\ndeploy/releases/api/intent.json\n"))

	var output bytes.Buffer
	if err := run([]string{"plan", "deploy/releases/components.json", liveMerge, head, "changed.txt"}, &output); err != nil {
		t.Fatalf("plan after merge release: %v", err)
	}
	var plan declarativerelease.Plan
	if err := json.Unmarshal(output.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Releases) != 1 || plan.Releases[0].ExpectedPreviousConfigSHA != liveMerge {
		t.Fatalf("merge predecessor was not preserved: %+v", plan.Releases)
	}
}

func TestPlanCommandAcceptsMergeCommitAsSupersededFailedAtom(t *testing.T) {
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
		Health:   []declarativerelease.HealthProbe{{Type: "deployment", Name: "fugue-fugue-api"}}, Concurrency: "fugue-production-api",
	}}}
	writeJSON(t, "deploy/releases/components.json", registry)
	writeFile(t, "Dockerfile.api", []byte("FROM scratch\n"))
	writeFile(t, "cmd/fugue-api/main.go", []byte("package main\nfunc main() {}\n"))
	writeJSON(t, "deploy/releases/api/intent.json", declarativerelease.Intent{
		APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind, Component: "api",
		Generation: 1, ExpectedPreviousPresent: false, Rollback: "previous-git-lkg",
	})
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "release api lkg")
	lkg := runGit(t, "rev-parse", "HEAD")

	runGit(t, "checkout", "-b", "failed-v2")
	writeFile(t, "cmd/fugue-api/main.go", []byte("package main\nfunc main() { println(\"failed\") }\n"))
	writeJSON(t, "deploy/releases/api/intent.json", declarativerelease.Intent{
		APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind, Component: "api",
		Generation: 2, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: lkg,
		ExpectedPreviousManifestSHA: lkg, ExpectedPreviousOCIRevision: lkg,
		ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("a", 64), Rollback: "previous-git-lkg",
	})
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "release failed api v2")
	runGit(t, "checkout", "main")
	runGit(t, "merge", "--no-ff", "failed-v2", "-m", "merge failed api v2")
	failedMerge := runGit(t, "rev-parse", "HEAD")
	_, priorProductionAtom, found, err := loadGitIntent(failedMerge, "deploy/releases/api/intent.json")
	if err != nil || !found || priorProductionAtom != failedMerge {
		t.Fatalf("failed merge was not resolved as the prior production atom: found=%v atom=%q err=%v", found, priorProductionAtom, err)
	}

	runGit(t, "checkout", "-b", "recovery-v3")
	writeFile(t, "cmd/fugue-api/main.go", []byte("package main\nfunc main() { println(\"recovered\") }\n"))
	writeJSON(t, "deploy/releases/api/intent.json", declarativerelease.Intent{
		APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind, Component: "api",
		Generation: 3, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: lkg,
		ExpectedPreviousManifestSHA: lkg, ExpectedPreviousOCIRevision: lkg,
		ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("a", 64), SupersedesFailedConfigSHA: failedMerge,
		Rollback: "previous-git-lkg",
	})
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "recover api v3")
	head := runGit(t, "rev-parse", "HEAD")
	writeFile(t, "changed.txt", []byte("cmd/fugue-api/main.go\ndeploy/releases/api/intent.json\n"))

	var output bytes.Buffer
	if err := run([]string{"plan", "deploy/releases/components.json", failedMerge, head, "changed.txt"}, &output); err != nil {
		t.Fatalf("plan after failed merge release: %v", err)
	}
	var plan declarativerelease.Plan
	if err := json.Unmarshal(output.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Releases) != 1 || plan.Releases[0].ExpectedPreviousConfigSHA != lkg ||
		plan.Releases[0].SupersedesFailedConfigSHA != failedMerge || plan.Releases[0].RetrySameLKG {
		t.Fatalf("failed merge successor was not preserved: %+v", plan.Releases)
	}
}

func TestExactIntentAtomOrMergeRejectsUnrelatedSingleParent(t *testing.T) {
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
	writeFile(t, "deploy/releases/api/intent.json", []byte("{}\n"))
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "intent")
	writeFile(t, "README.md", []byte("unrelated\n"))
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "unrelated")

	if isExactIntentAtomOrMerge(runGit(t, "rev-parse", "HEAD"), "deploy/releases/api/intent.json") {
		t.Fatal("unrelated single-parent commit was accepted as a production intent atom")
	}
}

func TestExactIntentAtomOrMergeAcceptsProductionMerge(t *testing.T) {
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
	writeFile(t, "deploy/releases/api/intent.json", []byte("{}\n"))
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "bootstrap intent")
	runGit(t, "checkout", "-b", "release")
	writeFile(t, "deploy/releases/api/intent.json", []byte("{\"generation\":2}\n"))
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "release intent")
	runGit(t, "checkout", "main")
	runGit(t, "merge", "--no-ff", "release", "-m", "merge release intent")
	merge := runGit(t, "rev-parse", "HEAD")
	if !isExactIntentAtomOrMerge(merge, "deploy/releases/api/intent.json") {
		t.Fatal("production merge intent atom was rejected")
	}
}

func TestPlanCommandIncludesRuntimeChangesSinceProductionOCIRevision(t *testing.T) {
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
		Health:   []declarativerelease.HealthProbe{{Type: "deployment", Name: "fugue-fugue-api"}}, Concurrency: "fugue-production-api",
	}}}
	writeJSON(t, "deploy/releases/components.json", registry)
	writeFile(t, "Dockerfile.api", []byte("FROM scratch\n"))
	writeFile(t, "cmd/fugue-api/main.go", []byte("package main\nfunc main() {}\n"))
	writeJSON(t, "deploy/releases/api/intent.json", declarativerelease.Intent{APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind, Component: "api", Generation: 1, ExpectedPreviousPresent: false, Rollback: "previous-git-lkg"})
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "first production api atom")
	production := runGit(t, "rev-parse", "HEAD")

	writeFile(t, "cmd/fugue-api/main.go", []byte("package main\nfunc main() { println(\"latent\") }\n"))
	runGit(t, "add", "cmd/fugue-api/main.go")
	runGit(t, "commit", "-m", "latent api runtime change")
	base := runGit(t, "rev-parse", "HEAD")
	writeFile(t, "README.md", []byte("release the accumulated API runtime delta\n"))
	writeJSON(t, "deploy/releases/api/intent.json", declarativerelease.Intent{APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind, Component: "api", Generation: 2, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: production, ExpectedPreviousManifestSHA: production, ExpectedPreviousOCIRevision: production, ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("a", 64), Rollback: "previous-git-lkg"})
	runGit(t, "add", ".")
	runGit(t, "commit", "-m", "release accumulated api runtime")
	head := runGit(t, "rev-parse", "HEAD")
	writeFile(t, "changed.txt", []byte("README.md\ndeploy/releases/api/intent.json\n"))

	var output bytes.Buffer
	if err := run([]string{"plan", "deploy/releases/components.json", base, head, "changed.txt"}, &output); err != nil {
		t.Fatalf("plan command: %v", err)
	}
	var plan declarativerelease.Plan
	if err := json.Unmarshal(output.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Releases) != 1 || plan.Releases[0].ComponentID != "api" ||
		!containsPath(plan.Releases[0].ChangedPaths, "cmd/fugue-api/main.go") {
		t.Fatalf("production runtime delta was omitted: %+v", plan.Releases)
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
	content := emitGitHubOutputForComponent(t, "api", "fugue-production-api", "ghcr.io/example/fugue-api")
	apiLane := sha256.Sum256([]byte("ghcr.io/example/fugue-api"))
	want := fmt.Sprintf(
		"release_count=1\nrelease_matrix={\"include\":[{\"build_lane\":\"%x\",\"component\":\"api\"}]}\nrelease_components=[\"api\"]\nedge_client_count=0\nedge_client_matrix={\"include\":[]}\nedge_control_count=0\nedge_control_matrix={\"include\":[]}\nedge_worker_count=0\nedge_worker_matrix={\"include\":[]}\n",
		apiLane[:8],
	)
	if string(content) != want {
		t.Fatalf("unexpected GitHub output:\n%s", content)
	}
}

func TestEmitGitHubOutputRoutesDataDefinedEdgeClient(t *testing.T) {
	content := emitGitHubOutputForComponent(t, "edge-client-gamma", "fugue-production-edge-client-gamma", "ghcr.io/example/fugue-edge")
	edgeLane := sha256.Sum256([]byte("ghcr.io/example/fugue-edge"))
	want := fmt.Sprintf(
		"release_count=1\nrelease_matrix={\"include\":[{\"build_lane\":\"%x\",\"component\":\"edge-client-gamma\"}]}\nrelease_components=[\"edge-client-gamma\"]\nedge_client_count=1\nedge_client_matrix={\"include\":[{\"component\":\"edge-client-gamma\",\"concurrency\":\"fugue-production-edge-client-gamma\"}]}\nedge_control_count=0\nedge_control_matrix={\"include\":[]}\nedge_worker_count=0\nedge_worker_matrix={\"include\":[]}\n",
		edgeLane[:8],
	)
	if string(content) != want {
		t.Fatalf("unexpected Edge client GitHub output:\n%s", content)
	}
}

func emitGitHubOutputForComponent(t *testing.T, component, concurrency, repository string) []byte {
	t.Helper()
	plan := declarativerelease.Plan{
		APIVersion: declarativerelease.IntentAPIVersion, Kind: "ProductionReleasePlan",
		BaseSHA: "1111111111111111111111111111111111111111", HeadSHA: "2222222222222222222222222222222222222222",
		Releases: []declarativerelease.PlanRelease{
			{ComponentID: component, IntentDigest: "sha256:" + strings.Repeat("a", 64), IntentGeneration: 1, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: strings.Repeat("1", 40), ExpectedPreviousManifestSHA: strings.Repeat("1", 40), ExpectedPreviousOCIRevision: strings.Repeat("1", 40), ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("b", 64), Concurrency: concurrency, Artifact: declarativerelease.Artifact{Repository: repository}, Workload: declarativerelease.Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api", FieldManager: "fugue-api-declarative", Replicas: 2, RolloutMode: "rolling"}},
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
	return content
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
