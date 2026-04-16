package sourceimport

import (
	"reflect"
	"strings"
	"testing"
)

func TestBuilderWorkloadProfileForDockerfileIsHeavy(t *testing.T) {
	t.Parallel()

	if got := builderWorkloadProfileFor("dockerfile", true); got != builderWorkloadProfileHeavy {
		t.Fatalf("expected stateful dockerfile build to be heavy, got %q", got)
	}
	if got := builderWorkloadProfileFor("dockerfile", false); got != builderWorkloadProfileHeavy {
		t.Fatalf("expected stateless dockerfile build to be heavy, got %q", got)
	}
}

func TestDefaultBuilderPodPolicyWaitsForQueuedCapacity(t *testing.T) {
	t.Parallel()

	if got := defaultBuilderPodPolicy().SelectionTimeoutSeconds; got != 600 {
		t.Fatalf("expected default builder selection timeout 600s, got %d", got)
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
	if _, ok := podSpec["affinity"]; ok {
		t.Fatalf("expected heavy builder policy to avoid generic node affinity")
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

func TestBuildBuildpacksJobObjectInjectsSourceOverlayBeforeBuild(t *testing.T) {
	t.Parallel()

	jobObject, err := buildBuildpacksJobObject("fugue-system", "build-demo", buildpacksBuildRequest{
		ArchiveDownloadURL: "https://example.com/archive.tar.gz",
		SourceDir:          ".",
		ImageRef:           "10.128.0.2:30500/fugue-apps/demo:git-abc123",
		SourceOverlayFiles: []sourceOverlayFile{
			{
				RelativePath:  "requirements.txt",
				Content:       "fastapi\n",
				OnlyIfMissing: true,
			},
		},
		WorkloadProfile: builderWorkloadProfileHeavy,
	})
	if err != nil {
		t.Fatalf("build buildpacks job object: %v", err)
	}

	podSpec := jobObject["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	initContainers := podSpec["initContainers"].([]map[string]any)
	if len(initContainers) != 2 {
		t.Fatalf("expected download and overlay init containers, got %d", len(initContainers))
	}
	if got := initContainers[1]["name"]; got != "source-overlay" {
		t.Fatalf("expected source-overlay init container, got %#v", got)
	}
	command := initContainers[1]["command"].([]string)
	if !strings.Contains(command[2], "requirements.txt") {
		t.Fatalf("expected overlay command to mention requirements.txt, got %q", command[2])
	}
}

func TestBuildBuildpacksJobObjectUsesHostNetworkForPackBuild(t *testing.T) {
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
	container := podSpec["containers"].([]map[string]any)[0]
	command := container["command"].([]string)
	if !strings.Contains(command[2], "--network") || !strings.Contains(command[2], defaultBuildpacksContainerNetwork) {
		t.Fatalf("expected pack build command to use host network, got %q", command[2])
	}
}

func TestBuildBuildpacksJobObjectAddsAptAndLanguageBuildpacksWhenRequested(t *testing.T) {
	t.Parallel()

	jobObject, err := buildBuildpacksJobObject("fugue-system", "build-demo", buildpacksBuildRequest{
		ArchiveDownloadURL:  "https://example.com/archive.tar.gz",
		SourceDir:           ".",
		ImageRef:            "10.128.0.2:30500/fugue-apps/demo:git-abc123",
		DetectedProvider:    "python",
		IncludeAptBuildpack: true,
		WorkloadProfile:     builderWorkloadProfileHeavy,
	})
	if err != nil {
		t.Fatalf("build buildpacks job object: %v", err)
	}

	podSpec := jobObject["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	container := podSpec["containers"].([]map[string]any)[0]
	command := container["command"].([]string)
	if !strings.Contains(command[2], defaultPaketoAptBuildpack) {
		t.Fatalf("expected pack build command to include apt buildpack, got %q", command[2])
	}
	if !strings.Contains(command[2], "paketo-buildpacks/python") {
		t.Fatalf("expected pack build command to include python buildpack, got %q", command[2])
	}
}

func TestBuildNixpacksJobObjectUsesCompatibleShellOptions(t *testing.T) {
	t.Parallel()

	jobObject, err := buildNixpacksJobObject("fugue-system", "build-demo", nixpacksBuildRequest{
		ArchiveDownloadURL: "https://example.com/archive.tar.gz",
		SourceDir:          ".",
		ImageRef:           "10.128.0.2:30500/fugue-apps/demo:git-abc123",
		WorkloadProfile:    builderWorkloadProfileLight,
	})
	if err != nil {
		t.Fatalf("build nixpacks job object: %v", err)
	}

	podSpec := jobObject["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	initContainers := podSpec["initContainers"].([]map[string]any)
	command := initContainers[len(initContainers)-1]["command"].([]string)
	if strings.Contains(command[2], "pipefail") {
		t.Fatalf("expected nixpacks init script to avoid pipefail, got %q", command[2])
	}
}

func TestBuildNixpacksJobObjectPassesInferredSystemPackages(t *testing.T) {
	t.Parallel()

	jobObject, err := buildNixpacksJobObject("fugue-system", "build-demo", nixpacksBuildRequest{
		ArchiveDownloadURL: "https://example.com/archive.tar.gz",
		SourceDir:          ".",
		ImageRef:           "10.128.0.2:30500/fugue-apps/demo:git-abc123",
		SystemPackages:     []string{"ffmpeg", "git"},
		WorkloadProfile:    builderWorkloadProfileLight,
	})
	if err != nil {
		t.Fatalf("build nixpacks job object: %v", err)
	}

	podSpec := jobObject["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	initContainers := podSpec["initContainers"].([]map[string]any)
	command := initContainers[len(initContainers)-1]["command"].([]string)
	if !strings.Contains(command[2], "--apt 'ffmpeg' 'git'") {
		t.Fatalf("expected nixpacks command to include inferred apt packages, got %q", command[2])
	}
}

func TestBuildArchiveKanikoJobObjectAppliesBuilderTolerations(t *testing.T) {
	t.Parallel()

	jobObject, err := buildArchiveKanikoJobObject("fugue-system", "build-demo", dockerfileBuildRequest{
		ArchiveDownloadURL: "https://example.com/archive.tar.gz",
		DockerfilePath:     "Dockerfile",
		BuildContextDir:    ".",
		ImageRef:           "10.128.0.2:30500/fugue-apps/demo:git-abc123",
		PodPolicy: BuilderPodPolicy{
			Tolerations: []BuilderToleration{
				{
					Key:      "dedicated",
					Operator: "Equal",
					Value:    "builders",
					Effect:   "NoSchedule",
				},
			},
		},
		WorkloadProfile: builderWorkloadProfileLight,
	})
	if err != nil {
		t.Fatalf("build archive kaniko job object: %v", err)
	}

	podSpec := jobObject["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	tolerations, ok := podSpec["tolerations"].([]map[string]any)
	if !ok {
		t.Fatalf("expected pod tolerations to be present")
	}
	expected := []map[string]any{
		{
			"key":      "dedicated",
			"operator": "Equal",
			"value":    "builders",
			"effect":   "NoSchedule",
		},
	}
	if !reflect.DeepEqual(tolerations, expected) {
		t.Fatalf("expected tolerations %v, got %v", expected, tolerations)
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
