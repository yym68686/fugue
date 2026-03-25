package sourceimport

import "testing"

func TestBuilderWorkloadProfileForStatefulDockerfileIsHeavy(t *testing.T) {
	t.Parallel()

	if got := builderWorkloadProfileFor("dockerfile", true); got != builderWorkloadProfileHeavy {
		t.Fatalf("expected stateful dockerfile build to be heavy, got %q", got)
	}
	if got := builderWorkloadProfileFor("dockerfile", false); got != builderWorkloadProfileLight {
		t.Fatalf("expected stateless dockerfile build to be light, got %q", got)
	}
}

func TestBuildArchiveKanikoJobObjectAppliesLightBuilderPolicy(t *testing.T) {
	t.Parallel()

	jobObject, err := buildArchiveKanikoJobObject("fugue-system", "build-demo", dockerfileBuildRequest{
		ArchiveDownloadURL: "https://example.com/archive.tar.gz",
		DockerfilePath:     "Dockerfile",
		BuildContextDir:    ".",
		ImageRef:           "10.128.0.2:30500/fugue-apps/demo:git-abc123",
		WorkloadProfile:    builderWorkloadProfileLight,
	})
	if err != nil {
		t.Fatalf("build archive kaniko job object: %v", err)
	}

	podSpec := jobObject["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	if _, ok := podSpec["affinity"]; ok {
		t.Fatalf("expected light builder job to avoid large-node affinity")
	}

	workspace := builderEmptyDirVolume(t, podSpec, "workspace")
	if got := workspace["sizeLimit"]; got != "2Gi" {
		t.Fatalf("expected workspace sizeLimit 2Gi, got %#v", got)
	}

	container := podSpec["containers"].([]map[string]any)[0]
	requests := builderResourceValues(t, container, "requests")
	if got := requests["ephemeral-storage"]; got != "1Gi" {
		t.Fatalf("expected light ephemeral-storage request 1Gi, got %q", got)
	}
	initContainer := podSpec["initContainers"].([]map[string]any)[0]
	limits := builderResourceValues(t, initContainer, "limits")
	if got := limits["memory"]; got != "2Gi" {
		t.Fatalf("expected init container memory limit 2Gi, got %q", got)
	}
}

func TestBuildBuildpacksJobObjectAppliesHeavyBuilderPolicy(t *testing.T) {
	t.Parallel()

	jobObject, err := buildBuildpacksJobObject("fugue-system", "build-demo", buildpacksBuildRequest{
		ArchiveDownloadURL: "https://example.com/archive.tar.gz",
		SourceDir:          ".",
		ImageRef:           "10.128.0.2:30500/fugue-apps/demo:git-abc123",
		WorkloadProfile:    builderWorkloadProfileHeavy,
	})
	if err != nil {
		t.Fatalf("build buildpacks job object: %v", err)
	}

	podSpec := jobObject["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	affinity := podSpec["affinity"].(map[string]any)
	nodeAffinity := affinity["nodeAffinity"].(map[string]any)
	preferred := nodeAffinity["preferredDuringSchedulingIgnoredDuringExecution"].([]map[string]any)
	preference := preferred[0]["preference"].(map[string]any)
	matchExpressions := preference["matchExpressions"].([]map[string]any)
	if got := matchExpressions[0]["key"]; got != defaultBuilderLargeNodeLabelKey {
		t.Fatalf("expected large node label key %q, got %#v", defaultBuilderLargeNodeLabelKey, got)
	}
	if got := matchExpressions[0]["values"].([]string)[0]; got != defaultBuilderLargeNodeLabelValue {
		t.Fatalf("expected large node label value %q, got %q", defaultBuilderLargeNodeLabelValue, got)
	}

	workspace := builderEmptyDirVolume(t, podSpec, "workspace")
	if got := workspace["sizeLimit"]; got != "4Gi" {
		t.Fatalf("expected heavy workspace sizeLimit 4Gi, got %#v", got)
	}
	dockerData := builderEmptyDirVolume(t, podSpec, "docker-data")
	if got := dockerData["sizeLimit"]; got != "8Gi" {
		t.Fatalf("expected docker-data sizeLimit 8Gi, got %#v", got)
	}

	container := podSpec["containers"].([]map[string]any)[0]
	requests := builderResourceValues(t, container, "requests")
	if got := requests["cpu"]; got != "750m" {
		t.Fatalf("expected heavy cpu request 750m, got %q", got)
	}
	initContainer := podSpec["initContainers"].([]map[string]any)[0]
	limits := builderResourceValues(t, initContainer, "limits")
	if got := limits["ephemeral-storage"]; got != "8Gi" {
		t.Fatalf("expected heavy init ephemeral-storage limit 8Gi, got %q", got)
	}
}

func builderEmptyDirVolume(t *testing.T, podSpec map[string]any, name string) map[string]any {
	t.Helper()

	volumes, ok := podSpec["volumes"].([]map[string]any)
	if !ok {
		t.Fatalf("pod spec is missing volumes")
	}
	for _, volume := range volumes {
		if volume["name"] == name {
			emptyDir, ok := volume["emptyDir"].(map[string]any)
			if !ok {
				t.Fatalf("volume %q is missing emptyDir", name)
			}
			return emptyDir
		}
	}
	t.Fatalf("volume %q not found", name)
	return nil
}

func builderResourceValues(t *testing.T, container map[string]any, key string) map[string]string {
	t.Helper()

	resources, ok := container["resources"].(map[string]any)
	if !ok {
		t.Fatalf("container is missing resources")
	}
	values, ok := resources[key].(map[string]string)
	if !ok {
		t.Fatalf("resources[%q] is missing or has unexpected type", key)
	}
	return values
}
