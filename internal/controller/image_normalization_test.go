package controller

import (
	"testing"

	"fugue/internal/model"
)

func TestNormalizeManagedAppRuntimeImageRefsMapsLegacyManagedImage(t *testing.T) {
	t.Parallel()

	svc := &Service{
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.fugue.internal:5000",
	}
	app := model.App{
		Spec: model.AppSpec{
			Image: "10.128.0.2:30500/fugue-apps/demo@sha256:abc123",
		},
		Source: &model.AppSource{
			Type:             model.AppSourceTypeGitHubPublic,
			RepoURL:          "https://github.com/example/demo",
			CommitSHA:        "abcdef123456",
			ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-abcdef123456",
		},
	}

	got, changed := svc.normalizeManagedAppRuntimeImageRefs(app)
	if !changed {
		t.Fatal("expected legacy runtime image ref to be normalized")
	}
	want := "registry.fugue.internal:5000/fugue-apps/demo@sha256:abc123"
	if got.Spec.Image != want {
		t.Fatalf("expected image %q, got %q", want, got.Spec.Image)
	}
}

func TestNormalizeManagedAppRuntimeImageRefsKeepsExternalDockerImage(t *testing.T) {
	t.Parallel()

	svc := &Service{
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.fugue.internal:5000",
	}
	imageRef := "ghcr.io/example/fugue-apps/demo@sha256:abc123"
	app := model.App{
		Spec: model.AppSpec{
			Image: imageRef,
		},
		Source: &model.AppSource{
			Type:     model.AppSourceTypeDockerImage,
			ImageRef: imageRef,
		},
	}

	got, changed := svc.normalizeManagedAppRuntimeImageRefs(app)
	if changed {
		t.Fatal("expected external docker image to remain unchanged")
	}
	if got.Spec.Image != imageRef {
		t.Fatalf("expected image %q, got %q", imageRef, got.Spec.Image)
	}
}
