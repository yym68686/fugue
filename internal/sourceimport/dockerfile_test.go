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
