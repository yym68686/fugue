package controller

import (
	"context"
	"testing"
)

func TestMarkRegistryGCNeededUsesConfiguredRecorder(t *testing.T) {
	t.Parallel()

	var got string
	service := &Service{
		requestRegistryGC: func(_ context.Context, reason string) error {
			got = reason
			return nil
		},
	}
	service.markRegistryGCNeeded(context.Background(), "stale manifest deleted")
	if got != "stale manifest deleted" {
		t.Fatalf("expected GC request reason to be recorded, got %q", got)
	}
}

func TestLiveManagedImageRefsFromWorkloadProtectsContainerAndEnvDigests(t *testing.T) {
	t.Parallel()

	service := &Service{
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.pull.example",
	}
	workload := map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"initContainers": []any{
						map[string]any{
							"image": "registry.pull.example/fugue-apps/bootstrap@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
						},
					},
					"containers": []any{
						map[string]any{
							"image": "registry.pull.example/fugue-apps/demo@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
							"env": []any{
								map[string]any{
									"value": "registry.pull.example/fugue-apps/sidecar:current",
								},
							},
						},
					},
				},
			},
		},
	}

	refs := service.liveManagedImageRefsFromWorkload(workload)
	for _, want := range []string{
		"registry.push.example/fugue-apps/bootstrap@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"registry.push.example/fugue-apps/demo@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"registry.push.example/fugue-apps/sidecar:current",
	} {
		if _, exists := refs[want]; !exists {
			t.Fatalf("expected workload keep set to contain %q, got %+v", want, refs)
		}
	}
}

func TestManagedImageDigestInUseMatchesTagCandidateToWorkloadDigest(t *testing.T) {
	t.Parallel()

	const digest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	service := &Service{
		resolveManagedImageDigestRef: func(context.Context, string) (string, error) {
			return "registry.push.example/fugue-apps/demo@" + digest, nil
		},
	}
	inUse, err := service.managedImageDigestInUse(
		context.Background(),
		"registry.push.example/fugue-apps/demo:old",
		map[string]struct{}{
			"registry.push.example/fugue-apps/demo@" + digest: {},
		},
	)
	if err != nil {
		t.Fatalf("check digest use: %v", err)
	}
	if !inUse {
		t.Fatal("expected tag candidate resolving to current workload digest to be protected")
	}
}
