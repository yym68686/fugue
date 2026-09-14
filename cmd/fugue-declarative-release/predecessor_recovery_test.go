package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"fugue/internal/declarativerelease"
)

func TestPlanRecoveryRequiresExplicitFailedAtomAndLivePredecessor(t *testing.T) {
	previousDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previousDirectory) })
	runGit(t, "init", "--initial-branch=main")
	runGit(t, "config", "user.email", "release@example.test")
	runGit(t, "config", "user.name", "Release Test")
	intentPath := "deploy/releases/api/intent.json"
	registry := declarativerelease.Registry{APIVersion: declarativerelease.RegistryAPIVersion, Kind: declarativerelease.RegistryKind, Components: []declarativerelease.Component{{
		ID: "api", Family: "control-plane", IntentPath: intentPath, ManifestPath: "deploy/releases/api/deployment.json", SourceRoots: []string{"Dockerfile.api", "cmd/fugue-api"},
		Artifact: declarativerelease.Artifact{Repository: "ghcr.io/example/fugue-api", Dockerfile: "Dockerfile.api", Context: ".", BuildPackage: "./cmd/fugue-api"},
		Workload: declarativerelease.Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api", FieldManager: "fugue-api-declarative", Replicas: 2, RolloutMode: "rolling"}, Health: []declarativerelease.HealthProbe{{Type: "deployment", Name: "fugue-fugue-api"}}, Concurrency: "fugue-production-api",
	}}}
	writeJSON(t, "deploy/releases/components.json", registry)
	writeFile(t, "go.mod", []byte("module example.test/release\n\ngo 1.22\n"))
	writeFile(t, "Dockerfile.api", []byte("FROM scratch\n"))
	writeFile(t, "cmd/fugue-api/main.go", []byte("package main\nfunc main() {}\n"))
	intent := declarativerelease.Intent{APIVersion: declarativerelease.IntentAPIVersion, Kind: declarativerelease.IntentKind, Component: "api", Generation: 1, Rollback: "previous-git-lkg"}
	commit := func(message string) string {
		writeJSON(t, intentPath, intent)
		runGit(t, "add", ".")
		runGit(t, "commit", "-m", message)
		return runGit(t, "rev-parse", "HEAD")
	}
	first := commit("first verified release")
	intent.Generation = 2
	intent.ExpectedPreviousPresent = true
	intent.ExpectedPreviousConfigSHA = first
	intent.ExpectedPreviousManifestSHA = first
	intent.ExpectedPreviousOCIRevision = first
	intent.ExpectedPreviousImageDigest = "sha256:" + strings.Repeat("a", 64)
	live := commit("second verified release")
	// A later failed plan retained the obsolete predecessor. The live authority
	// remains at generation 2; correcting the plan must not imply a rollback.
	intent.Generation = 3
	failed := commit("failed preflight with obsolete predecessor")
	for _, test := range []struct {
		name, supersedes string
		pass             bool
	}{
		{"implicit_ancestor", "", false},
		{"explicit_failed", failed, true},
		{"wrong_failed", live, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			runGit(t, "checkout", "-b", test.name, failed)
			intent.Generation = 4
			intent.ExpectedPreviousConfigSHA = live
			intent.ExpectedPreviousManifestSHA = live
			intent.ExpectedPreviousOCIRevision = live
			intent.ExpectedPreviousImageDigest = "sha256:" + strings.Repeat("b", 64)
			intent.SupersedesFailedConfigSHA = test.supersedes
			head := commit(test.name)
			writeFile(t, "changed.txt", []byte(intentPath+"\n"))
			var output bytes.Buffer
			err := run([]string{"plan", "deploy/releases/components.json", failed, head, "changed.txt"}, &output)
			if !test.pass {
				if err == nil {
					t.Fatal("older ancestor was treated as an ordinary predecessor")
				}
				return
			}
			if err != nil {
				t.Fatal("explicit failed preflight recovery rejected", err)
			}
			var plan declarativerelease.Plan
			if err := json.Unmarshal(output.Bytes(), &plan); err != nil {
				t.Fatal(err)
			}
			if len(plan.Releases) != 1 || plan.Releases[0].ExpectedPreviousConfigSHA != live || plan.Releases[0].SupersedesFailedConfigSHA != failed || plan.Releases[0].RetrySameLKG {
				t.Fatalf("recovery binding changed: %+v", plan.Releases)
			}
		})
	}
}
