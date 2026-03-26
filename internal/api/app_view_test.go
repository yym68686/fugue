package api

import (
	"testing"

	"fugue/internal/model"
)

func TestBuildAppTechStackUsesDetectedStack(t *testing.T) {
	t.Parallel()

	stack := buildAppTechStack(model.App{
		Source: &model.AppSource{
			BuildStrategy:    model.AppBuildStrategyDockerfile,
			DetectedProvider: model.AppBuildStrategyDockerfile,
			DetectedStack:    "nextjs",
		},
	})

	if len(stack) != 1 {
		t.Fatalf("expected exactly one tech stack entry, got %+v", stack)
	}
	if stack[0].Kind != "stack" || stack[0].Slug != "nextjs" || stack[0].Name != "Next.js" {
		t.Fatalf("expected nextjs stack entry, got %+v", stack[0])
	}
}

func TestBuildAppTechStackFallsBackToLegacyProvider(t *testing.T) {
	t.Parallel()

	stack := buildAppTechStack(model.App{
		Source: &model.AppSource{
			DetectedProvider: "python",
		},
	})

	if len(stack) != 1 {
		t.Fatalf("expected one legacy provider stack entry, got %+v", stack)
	}
	if stack[0].Kind != "stack" || stack[0].Slug != "python" || stack[0].Name != "Python" {
		t.Fatalf("expected python stack entry, got %+v", stack[0])
	}
}
