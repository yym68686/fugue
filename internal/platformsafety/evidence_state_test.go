package platformsafety

import (
	"testing"

	"fugue/internal/model"
)

func TestResolveInvariantEvidenceState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		explicitState string
		legacyPass    bool
		want          string
		wantError     bool
	}{
		{name: "legacy pass", legacyPass: true, want: model.InvariantEvidenceStatePass},
		{name: "legacy non-pass is unknown", want: model.InvariantEvidenceStateUnknown},
		{name: "explicit pass", explicitState: model.InvariantEvidenceStatePass, legacyPass: true, want: model.InvariantEvidenceStatePass},
		{name: "explicit fail", explicitState: model.InvariantEvidenceStateFail, want: model.InvariantEvidenceStateFail},
		{name: "explicit unknown", explicitState: model.InvariantEvidenceStateUnknown, want: model.InvariantEvidenceStateUnknown},
		{name: "explicit stale", explicitState: model.InvariantEvidenceStateStale, want: model.InvariantEvidenceStateStale},
		{name: "trim explicit state", explicitState: "  stale  ", want: model.InvariantEvidenceStateStale},
		{name: "pass conflicts with legacy false", explicitState: model.InvariantEvidenceStatePass, wantError: true},
		{name: "non-pass conflicts with legacy true", explicitState: model.InvariantEvidenceStateFail, legacyPass: true, wantError: true},
		{name: "unsupported state", explicitState: "degraded", wantError: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := ResolveInvariantEvidenceState(test.explicitState, test.legacyPass)
			if test.wantError {
				if err == nil {
					t.Fatalf("expected fail-closed conflict, got state %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve evidence state: %v", err)
			}
			if got != test.want {
				t.Fatalf("state = %q, want %q", got, test.want)
			}
		})
	}
}
