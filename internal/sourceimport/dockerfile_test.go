package sourceimport

import (
	"strings"
	"testing"
)

func TestBuildJobNameDifferentiatesMultiServiceUploads(t *testing.T) {
	t.Parallel()

	gateway := buildJobName(dockerfileBuildRequest{
		SourceLabel: "argus",
		CommitSHA:   "abcdef1234567890",
		ImageRef:    "registry.example/fugue-apps/argus-gateway:upload-abc123",
	})
	runtime := buildJobName(dockerfileBuildRequest{
		SourceLabel: "argus",
		CommitSHA:   "abcdef1234567890",
		ImageRef:    "registry.example/fugue-apps/argus-runtime:upload-abc123",
	})

	if gateway == runtime {
		t.Fatalf("expected per-service builder jobs, got identical name %q", gateway)
	}
}

func TestBuildJobNameDifferentiatesRepeatedImportOperations(t *testing.T) {
	t.Parallel()

	first := buildJobName(dockerfileBuildRequest{
		SourceLabel: "argus",
		CommitSHA:   "abcdef1234567890",
		ImageRef:    "registry.example/fugue-apps/argus-gateway:upload-abc123",
		JobLabels: map[string]string{
			"fugue.pro/operation-id": "op_1",
		},
	})
	second := buildJobName(dockerfileBuildRequest{
		SourceLabel: "argus",
		CommitSHA:   "abcdef1234567890",
		ImageRef:    "registry.example/fugue-apps/argus-gateway:upload-abc123",
		JobLabels: map[string]string{
			"fugue.pro/operation-id": "op_2",
		},
	})

	if first == second {
		t.Fatalf("expected distinct builder jobs per import operation, got identical name %q", first)
	}
}

func TestBuildKanikoJobObjectPushesToBuilderRegistryWhenConfigured(t *testing.T) {
	t.Setenv("FUGUE_BUILDER_REGISTRY_PUSH_BASE", "127.0.0.1:5000")

	jobObject, err := buildKanikoJobObject("fugue-system", "build-demo", dockerfileBuildRequest{
		RepoURL:         "https://github.com/example/demo",
		Branch:          "main",
		CommitSHA:       "abcdef1234567890",
		DockerfilePath:  "Dockerfile",
		BuildContextDir: ".",
		ImageRef:        "fugue-fugue-registry.fugue-system.svc.cluster.local:5000/fugue-apps/demo:git-abcdef123456",
		DestinationImageRef: builderDestinationImageRef(
			"fugue-fugue-registry.fugue-system.svc.cluster.local:5000/fugue-apps/demo:git-abcdef123456",
			"fugue-fugue-registry.fugue-system.svc.cluster.local:5000",
		),
	})
	if err != nil {
		t.Fatalf("build kaniko job object: %v", err)
	}

	podSpec := jobObject["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	if got := podSpec["hostNetwork"]; got != true {
		t.Fatalf("hostNetwork = %#v, want true", got)
	}
	if got := podSpec["dnsPolicy"]; got != "ClusterFirstWithHostNet" {
		t.Fatalf("dnsPolicy = %#v, want ClusterFirstWithHostNet", got)
	}
	containers := podSpec["containers"].([]map[string]any)
	args := containers[0]["args"].([]string)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--destination=127.0.0.1:5000/fugue-apps/demo:git-abcdef123456") {
		t.Fatalf("expected builder-local destination, got args: %v", args)
	}
}
