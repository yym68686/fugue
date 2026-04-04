package appimages

import (
	"reflect"
	"testing"

	"fugue/internal/model"
)

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
