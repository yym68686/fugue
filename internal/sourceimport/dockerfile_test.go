package sourceimport

import "testing"

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
