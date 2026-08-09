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
	want := []string{"api", "controller", "edge-client-de", "edge-client-us"}
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
	imageCache := byID["image-cache"]
	if imageCache.MigrationState != "independent" || imageCache.Workload.PreservedUnavailable != 1 ||
		imageCache.AdoptionReceiptPath != "deploy/releases/image-cache/adoption-receipt.json" || imageCache.OwnershipAdoption != nil ||
		imageCache.BootstrapLKGPath != "" {
		t.Fatalf("image-cache lane did not retire adoption while preserving its single offline node: %+v", imageCache)
	}
	for filename, contract := range map[string]struct {
		strategy    string
		unavailable int64
	}{
		"../../deploy/releases/image-cache/lkg.json":       {strategy: "OnDelete"},
		"../../deploy/releases/image-cache/daemonset.json": {strategy: "RollingUpdate", unavailable: 2},
	} {
		raw, err := os.ReadFile(filename)
		if err != nil {
			t.Fatalf("read image-cache resource set %s: %v", filename, err)
		}
		resourceSet, err := DecodeResourceSet(bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("decode image-cache resource set %s: %v", filename, err)
		}
		workload := imageCache.Workload
		if contract.strategy == "OnDelete" {
			workload.RolloutMode = "on-delete"
		}
		primary, err := resourceSet.Primary(workload)
		if err != nil {
			t.Fatalf("select image-cache primary from %s: %v", filename, err)
		}
		spec, err := objectField(primary, "spec")
		if err != nil {
			t.Fatal(err)
		}
		updateStrategy, err := objectField(spec, "updateStrategy")
		if err != nil || stringField(updateStrategy, "type") != contract.strategy {
			t.Fatalf("image-cache strategy in %s = %+v, %v", filename, updateStrategy, err)
		}
		if contract.unavailable == 0 {
			if _, present := updateStrategy["rollingUpdate"]; present {
				t.Fatalf("bootstrap LKG must retain the observed OnDelete strategy: %+v", updateStrategy)
			}
			if _, present := spec["minReadySeconds"]; present {
				t.Fatal("bootstrap LKG invented minReadySeconds absent from the live predecessor")
			}
			template, err := objectField(spec, "template")
			if err != nil {
				t.Fatal(err)
			}
			podSpec, err := objectField(template, "spec")
			if err != nil {
				t.Fatal(err)
			}
			containers, ok := podSpec["containers"].([]any)
			if !ok || len(containers) != 1 {
				t.Fatalf("bootstrap LKG image-cache containers are invalid: %+v", podSpec["containers"])
			}
			container, ok := containers[0].(map[string]any)
			if !ok || stringField(container, "name") != "image-cache" {
				t.Fatalf("bootstrap LKG image-cache container is invalid: %+v", containers[0])
			}
			wantImage := imageCache.Artifact.Repository + "@sha256:18bf0bcc6d3b69a73aed8118acbb98b508216977ddf5b4c4d0d9f6ee3c5494d4"
			if stringField(container, "image") != wantImage {
				t.Fatalf("bootstrap LKG image is not the declared immutable predecessor: %q", stringField(container, "image"))
			}
			templateMetadata, err := objectField(template, "metadata")
			if err != nil {
				t.Fatal(err)
			}
			annotations := ensureReadStringMap(templateMetadata, "annotations")
			wantSource := "e8f3781e3c9282e9daf24842c10cef3eab9f5497"
			if annotations["fugue.pro/source-commit"] != wantSource || annotations["fugue.pro/oci-revision"] != wantSource {
				t.Fatalf("bootstrap LKG source identity is not the declared predecessor: %+v", annotations)
			}
			for _, field := range []string{"livenessProbe", "readinessProbe", "startupProbe"} {
				if _, present := container[field]; present {
					t.Fatalf("bootstrap LKG invented %s absent from the live predecessor", field)
				}
			}
			continue
		}
		rollingUpdate, err := objectField(updateStrategy, "rollingUpdate")
		gotUnavailable, ok := integerField(rollingUpdate["maxUnavailable"])
		if err != nil || !ok || gotUnavailable != contract.unavailable {
			t.Fatalf("forward image-cache rollout does not reserve one offline plus one active slot: %+v", updateStrategy)
		}
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
		if byID[componentID].BootstrapLKGPath != "" {
			t.Fatalf("%s retains a legacy bootstrap LKG", componentID)
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
