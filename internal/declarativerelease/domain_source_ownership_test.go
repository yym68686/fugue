package declarativerelease

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func migratedDomainRegistry(t *testing.T) Registry {
	t.Helper()
	f, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	registry, err := DecodeRegistry(f)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestMigratedDomainReleaseSourceOwnership(t *testing.T) {
	registry := migratedDomainRegistry(t)
	for _, tc := range []struct {
		path string
		want []string
	}{
		{"internal/appimages/remote_registry.go", []string{"api", "controller"}},
		{"internal/imagecacheevidence/evidence.go", []string{"api", "controller", "image-cache", "observation-worker", "ssh-control"}},
		{"internal/imagecachekeys/keys.go", []string{"api", "controller", "observation-worker", "ssh-control"}},
		{"internal/operationqueue/selection.go", []string{"controller"}},
		{"internal/operationview/repository.go", []string{"api"}},
		{"internal/operationview/contract/contract.go", []string{"api", "controller", "observation-worker", "ssh-control"}},
		{"internal/imagetracking/poller.go", []string{"api", "controller"}},
		{"internal/nodehostpolicy/host_journald_policy.go", []string{"api"}},
		{"internal/pgimage/catalog_receipt_gen.go", []string{"api", "controller", "observation-worker", "ssh-control"}},
		{"internal/pgimage/witness.go", []string{"api", "controller", "observation-worker", "ssh-control"}},
		{"internal/pgimagemigration/execute.go", []string{"controller"}},
	} {
		var got []string
		for _, component := range registry.Components {
			for _, root := range component.SourceRoots {
				if pathMatchesRoot(tc.path, root) {
					got = append(got, component.ID)
					break
				}
			}
		}
		slices.Sort(got)
		if !slices.Equal(got, tc.want) {
			t.Errorf("source owners for %s = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestMigratedDomainBuildGraphMatchesSourceOwnership(t *testing.T) {
	registry := migratedDomainRegistry(t)
	packages := make(map[string]bool)
	for _, component := range registry.Components {
		packages[component.Artifact.BuildPackage] = true
	}
	args := []string{"list", "-json=ImportPath,Deps"}
	for pkg := range packages {
		args = append(args, pkg)
	}
	slices.Sort(args[2:])
	// These domains have been extracted from larger release roots. Check that
	// the path classifier selects exactly their actual artifact consumers.
	sources := map[string]string{
		"internal/appimages":                  "remote_registry.go",
		"internal/backupusage":                "reconcile.go",
		"internal/imagebilling":               "scheduler.go",
		"internal/imagebilling/contract":      "repository.go",
		"internal/imagecacheevidence":         "evidence.go",
		"internal/imagecachekeys":             "keys.go",
		"internal/imagetracking":              "poller.go",
		"internal/nodehostpolicy":             "host_journald_policy.go",
		"internal/operationcancel":            "cancel.go",
		"internal/operationqueue":             "selection.go",
		"internal/operationview":              "repository.go",
		"internal/operationview/contract":     "contract.go",
		"internal/pgimage":                    "catalog_receipt_gen.go",
		"internal/pgimagemigration":           "execute.go",
		"internal/releaseretirement":          "scheduler.go",
		"internal/releaseretirement/contract": "repository.go",
	}
	for _, arch := range []string{"amd64", "arm64"} {
		t.Run(arch, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), "go", args...)
			cmd.Dir = "../.."
			cmd.Env = append(cmd.Environ(), "GOOS=linux", "GOARCH="+arch, "CGO_ENABLED=0")
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			raw, err := cmd.Output()
			if err != nil {
				t.Fatalf("cannot list artifact dependencies: %v: %s", err, &stderr)
			}
			graph := make(map[string][]string)
			dec := json.NewDecoder(bytes.NewReader(raw))
			for {
				var pkg struct {
					ImportPath string
					Deps       []string
				}
				if err := dec.Decode(&pkg); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				graph[pkg.ImportPath] = pkg.Deps
			}
			for _, component := range registry.Components {
				command := "fugue/" + strings.TrimPrefix(component.Artifact.BuildPackage, "./")
				deps, ok := graph[command]
				if !ok {
					t.Fatalf("missing build graph for %s", command)
				}
				for pkg, file := range sources {
					source := pkg + "/" + file
					covered := slices.ContainsFunc(component.SourceRoots, func(root string) bool {
						return pathMatchesRoot(source, root)
					})
					if linked := slices.Contains(deps, "fugue/"+pkg); linked != covered {
						t.Errorf("%s: %s linked=%v, source %s covered=%v", component.ID, pkg, linked, source, covered)
					}
				}
			}
		})
	}
}
