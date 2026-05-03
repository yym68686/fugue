package appimages

import (
	"context"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestExcessManagedImageRefsDeletesOldestExistingStaleImagesBeyondLimit(t *testing.T) {
	t.Parallel()

	const (
		pushBase = "registry.push.example"
		pullBase = "registry.pull.example"
	)

	currentReadyAt := time.Date(2026, 4, 5, 10, 0, 0, 0, time.UTC)
	oldCompletedAt := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	newCompletedAt := time.Date(2026, 4, 4, 10, 0, 0, 0, time.UTC)

	app := model.App{
		ID:   "app-limit",
		Name: "demo",
		Spec: model.AppSpec{
			Image:            pullBase + "/fugue-apps/example-demo:git-current",
			ImageMirrorLimit: 1,
		},
		Source: &model.AppSource{
			Type:             model.AppSourceTypeGitHubPublic,
			ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-current",
		},
		Status: model.AppStatus{
			CurrentReleaseReadyAt: &currentReadyAt,
		},
	}
	ops := []model.Operation{
		{
			AppID: "app-limit",
			DesiredSpec: &model.AppSpec{
				Image: pullBase + "/fugue-apps/example-demo:git-old",
			},
			DesiredSource: &model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-old",
			},
			CompletedAt: &oldCompletedAt,
		},
		{
			AppID: "app-limit",
			DesiredSpec: &model.AppSpec{
				Image: pullBase + "/fugue-apps/example-demo:git-new",
			},
			DesiredSource: &model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-new",
			},
			CompletedAt: &newCompletedAt,
		},
	}

	got, err := ExcessManagedImageRefs(
		context.Background(),
		func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			return true, map[string]int64{"sha256:" + imageRef: 1}, nil
		},
		app,
		ops,
		pushBase,
		pullBase,
		app.Spec.ImageMirrorLimit,
	)
	if err != nil {
		t.Fatalf("excess managed image refs: %v", err)
	}

	want := []string{
		pushBase + "/fugue-apps/example-demo:git-old",
		pushBase + "/fugue-apps/example-demo:git-new",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected excess refs %v, got %v", want, got)
	}
}

func TestManagedImageRefForSourceGitHubOmitsBlankOptionalSuffix(t *testing.T) {
	t.Parallel()

	got := ManagedImageRefForSource(model.App{Name: "demo"}, &model.AppSource{
		Type:      model.AppSourceTypeGitHubPublic,
		RepoURL:   "https://github.com/Example/Demo",
		CommitSHA: "abcdef1234567890",
	}, "", "registry.push.example", "registry.pull.example")
	want := "registry.push.example/fugue-apps/example-demo:git-abcdef123456"
	if got != want {
		t.Fatalf("expected github managed image ref %q, got %q", want, got)
	}
}

func TestManagedImageRefForSourceMapsLegacyFugueAppsRuntimeHost(t *testing.T) {
	t.Parallel()

	got := ManagedImageRefForSource(
		model.App{Name: "demo"},
		&model.AppSource{Type: model.AppSourceTypeDockerImage},
		"10.128.0.2:30500/fugue-apps/example-demo@sha256:abc123",
		"registry.push.example",
		"registry.fugue.internal:5000",
	)
	want := "registry.push.example/fugue-apps/example-demo@sha256:abc123"
	if got != want {
		t.Fatalf("expected legacy runtime host to map to managed ref %q, got %q", want, got)
	}
}

func TestNormalizeRuntimeImageRefForSourceMapsLegacyFugueAppsRuntimeHost(t *testing.T) {
	t.Parallel()

	got := NormalizeRuntimeImageRefForSource(
		"10.128.0.2:30500/fugue-apps/example-demo@sha256:abc123",
		&model.AppSource{
			Type:             model.AppSourceTypeUpload,
			ResolvedImageRef: "registry.push.example/fugue-apps/example-demo:upload-abcdef123456",
		},
		"registry.push.example",
		"registry.fugue.internal:5000",
	)
	want := "registry.fugue.internal:5000/fugue-apps/example-demo@sha256:abc123"
	if got != want {
		t.Fatalf("expected normalized runtime image ref %q, got %q", want, got)
	}
}

func TestNormalizeRuntimeImageRefForSourceDoesNotRewriteExternalDockerImage(t *testing.T) {
	t.Parallel()

	imageRef := "ghcr.io/example/fugue-apps/demo@sha256:abc123"
	got := NormalizeRuntimeImageRefForSource(
		imageRef,
		&model.AppSource{
			Type:     model.AppSourceTypeDockerImage,
			ImageRef: imageRef,
		},
		"registry.push.example",
		"registry.fugue.internal:5000",
	)
	if got != imageRef {
		t.Fatalf("expected external docker image to remain %q, got %q", imageRef, got)
	}
}

func TestManagedImageRefForSourceUploadOmitsBlankOptionalSuffix(t *testing.T) {
	t.Parallel()

	got := ManagedImageRefForSource(model.App{Name: "Demo App"}, &model.AppSource{
		Type:          model.AppSourceTypeUpload,
		ArchiveSHA256: "abcdef1234567890",
	}, "", "registry.push.example", "registry.pull.example")
	want := "registry.push.example/fugue-apps/demo-app:upload-abcdef123456"
	if got != want {
		t.Fatalf("expected upload managed image ref %q, got %q", want, got)
	}
}

func TestManagedImageRefForSourceUploadAvoidsDuplicateServiceSuffix(t *testing.T) {
	t.Parallel()

	got := ManagedImageRefForSource(model.App{Name: "argus-runtime"}, &model.AppSource{
		Type:            model.AppSourceTypeUpload,
		ArchiveSHA256:   "abcdef1234567890",
		ImageNameSuffix: "runtime",
	}, "", "registry.push.example", "registry.pull.example")
	want := "registry.push.example/fugue-apps/argus-runtime:upload-abcdef123456"
	if got != want {
		t.Fatalf("expected upload managed image ref %q, got %q", want, got)
	}
}

func TestExcessManagedImageRefsIgnoresMissingStaleImages(t *testing.T) {
	t.Parallel()

	const (
		pushBase = "registry.push.example"
		pullBase = "registry.pull.example"
	)

	currentReadyAt := time.Date(2026, 4, 5, 10, 0, 0, 0, time.UTC)
	oldCompletedAt := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	midCompletedAt := time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC)
	missingCompletedAt := time.Date(2026, 4, 4, 10, 0, 0, 0, time.UTC)

	app := model.App{
		ID:   "app-limit-missing",
		Name: "demo",
		Spec: model.AppSpec{
			Image:            pullBase + "/fugue-apps/example-demo:git-current",
			ImageMirrorLimit: 2,
		},
		Source: &model.AppSource{
			Type:             model.AppSourceTypeGitHubPublic,
			ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-current",
		},
		Status: model.AppStatus{
			CurrentReleaseReadyAt: &currentReadyAt,
		},
	}
	ops := []model.Operation{
		{
			AppID: "app-limit-missing",
			DesiredSpec: &model.AppSpec{
				Image: pullBase + "/fugue-apps/example-demo:git-old",
			},
			DesiredSource: &model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-old",
			},
			CompletedAt: &oldCompletedAt,
		},
		{
			AppID: "app-limit-missing",
			DesiredSpec: &model.AppSpec{
				Image: pullBase + "/fugue-apps/example-demo:git-mid",
			},
			DesiredSource: &model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-mid",
			},
			CompletedAt: &midCompletedAt,
		},
		{
			AppID: "app-limit-missing",
			DesiredSpec: &model.AppSpec{
				Image: pullBase + "/fugue-apps/example-demo:git-missing",
			},
			DesiredSource: &model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-missing",
			},
			CompletedAt: &missingCompletedAt,
		},
	}

	existingRefs := map[string]bool{
		pushBase + "/fugue-apps/example-demo:git-current": true,
		pushBase + "/fugue-apps/example-demo:git-mid":     true,
		pushBase + "/fugue-apps/example-demo:git-old":     true,
		pushBase + "/fugue-apps/example-demo:git-missing": false,
	}

	got, err := ExcessManagedImageRefs(
		context.Background(),
		func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			return existingRefs[imageRef], nil, nil
		},
		app,
		ops,
		pushBase,
		pullBase,
		app.Spec.ImageMirrorLimit,
	)
	if err != nil {
		t.Fatalf("excess managed image refs: %v", err)
	}

	want := []string{
		pushBase + "/fugue-apps/example-demo:git-old",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected excess refs %v, got %v", want, got)
	}
}

func TestDeletableManagedImageRefsOnlyReturnsTargetExclusiveRefs(t *testing.T) {
	t.Parallel()

	const (
		pushBase = "registry.push.example"
		pullBase = "registry.pull.example"
	)

	targetApp := model.App{
		ID:   "app-target",
		Name: "demo",
		Spec: model.AppSpec{
			Image: pullBase + "/fugue-apps/example-demo:git-current",
		},
		Source: &model.AppSource{
			Type:             model.AppSourceTypeGitHubPublic,
			ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-current",
		},
	}
	targetOps := []model.Operation{
		{
			AppID: "app-target",
			DesiredSpec: &model.AppSpec{
				Image: pullBase + "/fugue-apps/example-demo:git-shared-old",
			},
			DesiredSource: &model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-shared-old",
			},
		},
		{
			AppID: "app-target",
			DesiredSpec: &model.AppSpec{
				Image: pullBase + "/fugue-apps/example-demo:git-target-only",
			},
			DesiredSource: &model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-target-only",
			},
		},
	}

	remainingApps := []model.App{
		{
			ID:   "app-current-shared",
			Name: "prod",
			Spec: model.AppSpec{
				Image: pullBase + "/fugue-apps/example-demo:git-current",
			},
			Source: &model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-current",
			},
		},
		{
			ID:   "app-history-shared",
			Name: "staging",
			Spec: model.AppSpec{
				Image: pullBase + "/fugue-apps/example-demo:git-other",
			},
			Source: &model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-other",
			},
		},
	}
	remainingOps := []model.Operation{
		{
			AppID: "app-history-shared",
			DesiredSpec: &model.AppSpec{
				Image: pullBase + "/fugue-apps/example-demo:git-shared-old",
			},
			DesiredSource: &model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-shared-old",
			},
		},
	}

	got := DeletableManagedImageRefs(
		targetApp,
		targetOps,
		remainingApps,
		remainingOps,
		pushBase,
		pullBase,
	)

	want := []string{
		pushBase + "/fugue-apps/example-demo:git-target-only",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected deletable refs %v, got %v", want, got)
	}
}

func TestDeletableManagedImageRefsReturnsAllRefsWhenNoOtherAppsReferenceThem(t *testing.T) {
	t.Parallel()

	const (
		pushBase = "registry.push.example"
		pullBase = "registry.pull.example"
	)

	targetApp := model.App{
		ID:   "app-target",
		Name: "demo",
		Spec: model.AppSpec{
			Image: pullBase + "/fugue-apps/example-demo:git-current",
		},
		Source: &model.AppSource{
			Type:             model.AppSourceTypeGitHubPublic,
			ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-current",
		},
	}
	targetOps := []model.Operation{
		{
			AppID: "app-target",
			DesiredSpec: &model.AppSpec{
				Image: pullBase + "/fugue-apps/example-demo:git-old",
			},
			DesiredSource: &model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-old",
			},
		},
	}

	got := DeletableManagedImageRefs(
		targetApp,
		targetOps,
		nil,
		nil,
		pushBase,
		pullBase,
	)

	want := []string{
		pushBase + "/fugue-apps/example-demo:git-current",
		pushBase + "/fugue-apps/example-demo:git-old",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected deletable refs %v, got %v", want, got)
	}
}
