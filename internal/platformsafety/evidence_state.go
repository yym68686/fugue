package platformsafety

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

// ResolveInvariantEvidenceState provides a fail-closed bridge while callers
// migrate from legacy pass booleans to explicit four-state evidence.
func ResolveInvariantEvidenceState(explicitState string, legacyPass bool) (string, error) {
	state := strings.TrimSpace(explicitState)
	if state == "" {
		if legacyPass {
			return model.InvariantEvidenceStatePass, nil
		}
		return model.InvariantEvidenceStateUnknown, nil
	}

	switch state {
	case model.InvariantEvidenceStatePass:
		if !legacyPass {
			return "", fmt.Errorf("explicit evidence state %q conflicts with legacy pass=false", state)
		}
	case model.InvariantEvidenceStateFail, model.InvariantEvidenceStateUnknown, model.InvariantEvidenceStateStale:
		if legacyPass {
			return "", fmt.Errorf("explicit evidence state %q conflicts with legacy pass=true", state)
		}
	default:
		return "", fmt.Errorf("unsupported evidence state %q", state)
	}
	return state, nil
}
