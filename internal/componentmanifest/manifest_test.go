package componentmanifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRepositoryManifestValidates(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "architecture", "component-ownership-v1.yaml")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open repository manifest: %v", err)
	}
	defer file.Close()
	manifest, err := Load(file)
	if err != nil {
		t.Fatalf("load repository manifest: %v", err)
	}
	if len(manifest.Components) != 7 {
		t.Fatalf("components = %d, want 7", len(manifest.Components))
	}
}

func TestLoadManifest(t *testing.T) {
	manifest, err := Load(strings.NewReader(`
apiVersion: component.fugue.dev/v1
kind: ComponentOwnershipManifest
migrationPhase: foundation
legacyRelease: fugue
sharedSourceRoots:
  - go.mod
components:
  - id: release-control
    description: durable release coordinator
    runtimeKinds: [deployment, coordinator]
    ownershipMode: transitional-shared
    sourceRoots: [internal/platformcontrol]
    artifactKinds: [release-control]
    releaseLane: release-control
    coordinator: release-control
    ownedState: [release-ledger]
    contracts: [release-intent@v1]
    dependencies: []
    failureBoundary: release-control
    lkgPolicy: required
  - id: image-plane
    description: image inventory and replica control
    runtimeKinds: [deployment, daemonset]
    ownershipMode: transitional-shared
    sourceRoots: [cmd/fugue-image-cache]
    artifactKinds: [image-cache]
    releaseLane: image-plane
    coordinator: release-control
    ownedState: [image-inventory]
    contracts: [image-availability@v1]
    dependencies: []
    failureBoundary: image-plane
    lkgPolicy: required
sharedResources:
  - id: registry
    owner: image-plane
    conflictMode: mediated
    consumers: [release-control]
`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(manifest.Components) != 2 || manifest.Components[1].ID != "image-plane" {
		t.Fatalf("unexpected components: %+v", manifest.Components)
	}
}

func TestManifestRejectsAmbiguousOwnership(t *testing.T) {
	base := `
apiVersion: component.fugue.dev/v1
kind: ComponentOwnershipManifest
migrationPhase: foundation
legacyRelease: fugue
components:
  - id: release-control
    description: coordinator
    runtimeKinds: [coordinator]
    ownershipMode: transitional-shared
    sourceRoots: [internal/platformcontrol]
    artifactKinds: [release-control]
    releaseLane: release-control
    coordinator: release-control
    ownedState: [release-ledger]
    contracts: [release-intent@v1]
    dependencies: []
    failureBoundary: release-control
    lkgPolicy: required
  - id: image-plane
    description: image service
    runtimeKinds: [daemonset]
    ownershipMode: transitional-shared
    sourceRoots: [cmd/fugue-image-cache]
    artifactKinds: [image-cache]
    releaseLane: image-plane
    coordinator: release-control
    ownedState: [image-inventory]
    contracts: [image-availability@v1]
    dependencies: []
    failureBoundary: image-plane
    lkgPolicy: required
`
	for name, input := range map[string]string{
		"duplicate artifact":  strings.Replace(base, "artifactKinds: [image-cache]", "artifactKinds: [release-control]", 1),
		"unknown coordinator": strings.Replace(base, "coordinator: release-control\n    ownedState: [image-inventory]", "coordinator: missing\n    ownedState: [image-inventory]", 1),
		"bad contract":        strings.Replace(base, "image-availability@v1", "image-availability", 1),
		"overlapping source":  strings.Replace(base, "sourceRoots: [cmd/fugue-image-cache]", "sourceRoots: [internal/platformcontrol/subpackage]", 1),
		"duplicate dependency": strings.Replace(
			base,
			"dependencies: []\n    failureBoundary: image-plane",
			"dependencies: [release-control, release-control]\n    failureBoundary: image-plane",
			1,
		),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(strings.NewReader(input)); err == nil {
				t.Fatal("Load() unexpectedly succeeded")
			}
		})
	}
}

func TestManifestRejectsDependencyCycle(t *testing.T) {
	input := `
apiVersion: component.fugue.dev/v1
kind: ComponentOwnershipManifest
migrationPhase: foundation
legacyRelease: fugue
components:
  - id: release-control
    description: coordinator
    runtimeKinds: [coordinator]
    ownershipMode: transitional-shared
    sourceRoots: [internal/platformcontrol]
    artifactKinds: [release-control]
    releaseLane: release-control
    coordinator: release-control
    ownedState: [release-ledger]
    contracts: [release-intent@v1]
    dependencies: [image-plane]
    failureBoundary: release-control
    lkgPolicy: required
  - id: image-plane
    description: image service
    runtimeKinds: [daemonset]
    ownershipMode: transitional-shared
    sourceRoots: [cmd/fugue-image-cache]
    artifactKinds: [image-cache]
    releaseLane: image-plane
    coordinator: release-control
    ownedState: [image-inventory]
    contracts: [image-availability@v1]
    dependencies: [release-control]
    failureBoundary: image-plane
    lkgPolicy: required
`
	if _, err := Load(strings.NewReader(input)); err == nil {
		t.Fatal("Load() unexpectedly accepted a dependency cycle")
	}
}
