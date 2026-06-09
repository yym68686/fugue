package model

import "testing"

func TestApplyAppSpecDefaultsEnablesRightSizingByDefault(t *testing.T) {
	t.Parallel()

	spec := AppSpec{}
	ApplyAppSpecDefaults(&spec)

	if spec.RightSizing == nil || spec.RightSizing.Mode != AppRightSizingModeAuto {
		t.Fatalf("expected automatic right-sizing by default, got %+v", spec.RightSizing)
	}
}

func TestApplyAppSpecDefaultsPreservesExplicitDisabledRightSizing(t *testing.T) {
	t.Parallel()

	spec := AppSpec{
		RightSizing: &AppRightSizingSpec{Mode: AppRightSizingModeDisabled},
	}
	ApplyAppSpecDefaults(&spec)

	if spec.RightSizing == nil || spec.RightSizing.Mode != AppRightSizingModeDisabled {
		t.Fatalf("expected explicit disabled right-sizing to be preserved, got %+v", spec.RightSizing)
	}
}

func TestDefaultPostgresMemoryLimitIncludesHeadroom(t *testing.T) {
	t.Parallel()

	if got := DefaultPostgresMemoryLimitMebibytes(1024); got != 1536 {
		t.Fatalf("expected 1024Mi request to default to 1536Mi limit, got %dMi", got)
	}
}
