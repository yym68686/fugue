package declarativerelease

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestProductionRegistryNamesEveryRuntimeLane(t *testing.T) {
	filename := "../../deploy/releases/components.json"
	raw, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("read production component registry: %v", err)
	}
	file, err := os.Open(filename)
	if err != nil {
		t.Fatalf("open production component registry: %v", err)
	}
	defer file.Close()
	registry, err := DecodeRegistry(file)
	if err != nil {
		t.Fatalf("decode production component registry: %v", err)
	}
	edgeRaw, err := os.ReadFile(filepath.Join("../..", registry.EdgeGroupRegistryPath))
	if err != nil {
		t.Fatalf("read edge group registry: %v", err)
	}
	edgeRegistry, err := DecodeEdgeGroupRegistry(bytes.NewReader(edgeRaw))
	if err != nil {
		t.Fatalf("decode edge group registry: %v", err)
	}
	registry, err = MergeEdgeGroupRegistry(registry, edgeRegistry)
	if err != nil {
		t.Fatalf("merge edge group registry: %v", err)
	}
	got := make([]string, 0, len(registry.Components))
	for _, component := range registry.Components {
		got = append(got, component.ID)
	}
	want := []string{"api", "controller"}
	for _, group := range edgeRegistry.Groups {
		want = append(want, group.Control.ID, group.Worker.ID)
	}
	want = append(want, "image-cache", "schema", "telemetry")
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime lane inventory mismatch: got=%v want=%v", got, want)
	}
	byID := make(map[string]Component, len(registry.Components))
	for _, component := range registry.Components {
		byID[component.ID] = component
	}
	schema := byID["schema"]
	if schema.ManifestPath != "deploy/releases/schema/deployment.json" || schema.Workload.Kind != "Deployment" ||
		schema.Workload.Name != "fugue-schema-migrator" || schema.Workload.Replicas != 1 || schema.Workload.RolloutMode != "rolling" {
		t.Fatalf("schema lane is not a repeatable declarative Deployment: %+v", schema)
	}
	for _, group := range edgeRegistry.Groups {
		for _, component := range []Component{byID[group.Control.ID], byID[group.Worker.ID]} {
			if !strings.HasPrefix(component.ManifestPath, "internal/edge") {
				t.Fatalf("%s still depends on a legacy manifest path: %s", component.ID, component.ManifestPath)
			}
			for _, root := range component.SourceRoots {
				if strings.HasPrefix(root, "deploy/helm/") {
					t.Fatalf("%s still depends on a legacy Chart source root: %s", component.ID, root)
				}
			}
		}
	}
	for _, group := range edgeRegistry.Groups {
		component := byID[group.Worker.ID]
		if !containsString(component.SourceRoots, "internal/observability/config.go") || containsString(component.SourceRoots, "internal/observability") {
			t.Fatalf("%s does not isolate its exact observability config dependency: %v", component.ID, component.SourceRoots)
		}
	}
	for _, group := range edgeRegistry.Groups {
		componentID := group.Worker.ID
		caddyTargets := 0
		for _, target := range byID[componentID].ArtifactTargets {
			if target.Container == "caddy" && target.ContainerType == "container" {
				caddyTargets++
			}
		}
		if caddyTargets != 0 {
			t.Fatalf("%s incorrectly binds external Caddy containers to its Edge artifact", componentID)
		}
		lkgRaw, err := os.ReadFile(filepath.Join("../..", byID[componentID].BootstrapLKGPath))
		if err != nil {
			t.Fatalf("read %s semantic LKG: %v", componentID, err)
		}
		lkg, err := DecodeResourceSet(bytes.NewReader(lkgRaw))
		if err != nil {
			t.Fatalf("decode %s semantic LKG: %v", componentID, err)
		}
		intentRaw, err := os.ReadFile(filepath.Join("../..", byID[componentID].IntentPath))
		if err != nil && (!os.IsNotExist(err) || byID[componentID].MigrationState != "pending") {
			t.Fatalf("read %s bootstrap intent: %v", componentID, err)
		}
		if err == nil {
			intent, decodeErr := DecodeIntent(bytes.NewReader(intentRaw))
			if decodeErr != nil {
				t.Fatalf("decode %s bootstrap intent: %v", componentID, decodeErr)
			}
			release := PlanRelease{
				ComponentID: componentID, IntentGeneration: intent.Generation,
				ExpectedPreviousConfigSHA: intent.ExpectedPreviousConfigSHA, ExpectedPreviousManifestSHA: intent.ExpectedPreviousManifestSHA,
				ExpectedPreviousOCIRevision: intent.ExpectedPreviousOCIRevision, ExpectedPreviousImageDigest: intent.ExpectedPreviousImageDigest,
				Artifact: byID[componentID].Artifact, Workload: byID[componentID].Workload,
			}
			if identityErr := validateBootstrapLKGIdentity(lkg, release); identityErr != nil {
				t.Fatalf("validate %s semantic LKG: %v", componentID, identityErr)
			}
		}
		identities, err := ResourceSetIdentities(lkgRaw)
		if err != nil || len(identities) != 3 || identities[0].Name != group.Worker.Transition.EdgeGroupAB.FrontName ||
			identities[1].Name != group.Worker.Transition.EdgeGroupAB.WorkerAName || identities[2].Name != group.Worker.Transition.EdgeGroupAB.WorkerBName {
			t.Fatalf("%s semantic LKG resources are not group-local: %+v err=%v", componentID, identities, err)
		}
	}
	baseFile, err := os.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	baseRegistry, err := DecodeRegistry(baseFile)
	_ = baseFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := CanonicalJSON(baseRegistry)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, append(canonical, '\n')) {
		t.Fatal("production component registry is not canonical JSON")
	}
	edgeCanonical, err := CanonicalJSON(edgeRegistry)
	if err != nil || !bytes.Equal(edgeRaw, append(edgeCanonical, '\n')) {
		t.Fatal("edge group registry is not canonical JSON")
	}
	toolingPlan, err := BuildPlan(
		registry,
		"1111111111111111111111111111111111111111",
		"2222222222222222222222222222222222222222",
		[]string{".github/workflows/ci.yml", "cmd/fugue-declarative-release/main.go", "internal/declarativerelease/spec.go"},
	)
	if err != nil || len(toolingPlan.Releases) != 0 {
		t.Fatalf("registry activated a tooling-only migration commit: plan=%+v err=%v", toolingPlan, err)
	}
	for _, component := range registry.Components {
		if component.MigrationState != "pending" {
			continue
		}
		_, err := BuildPlan(
			registry,
			"1111111111111111111111111111111111111111",
			"2222222222222222222222222222222222222222",
			[]string{component.IntentPath},
		)
		if err == nil || !strings.Contains(err.Error(), "is not migrated to the declarative release entrypoint") {
			t.Fatalf("pending %s intent was not fail-closed: %v", component.ID, err)
		}
	}
	for _, component := range registry.Components {
		if component.MigrationState != "independent" {
			continue
		}
		manifestRaw, err := os.ReadFile(filepath.Join("../..", component.ManifestPath))
		if err != nil {
			t.Fatalf("read %s manifest: %v", component.ID, err)
		}
		manifestRaw, err = MaterializeManifestTemplate(manifestRaw, component.ManifestVariables)
		if err != nil {
			t.Fatalf("materialize %s manifest: %v", component.ID, err)
		}
		resourceSet, err := DecodeResourceSet(bytes.NewReader(manifestRaw))
		if err != nil {
			t.Fatalf("decode %s manifest: %v", component.ID, err)
		}
		if _, err := resourceSet.Primary(component.Workload); err != nil {
			t.Fatalf("validate %s primary workload: %v", component.ID, err)
		}
		intentRaw, err := os.ReadFile(filepath.Join("../..", component.IntentPath))
		if err != nil {
			t.Fatalf("read %s intent: %v", component.ID, err)
		}
		intent, err := DecodeIntent(bytes.NewReader(intentRaw))
		if err != nil || intent.Component != component.ID {
			t.Fatalf("decode %s intent: intent=%+v err=%v", component.ID, intent, err)
		}
		canonicalIntent, err := CanonicalJSON(intent)
		if err != nil || !bytes.Equal(intentRaw, append(canonicalIntent, '\n')) {
			t.Fatalf("%s intent is not canonical JSON", component.ID)
		}
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
