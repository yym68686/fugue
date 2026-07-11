package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

type platformArtifactVerificationEvidenceStateOptions struct {
	ConsumerConvergence        string
	LocalProbe                 string
	PublicSynthetic            string
	WatchWindow                string
	BaselineMonotonic          string
	DatabaseRollbackCompatible string
}

type platformArtifactVerifyLKGWireRequest struct {
	FencingToken    int64                                           `json:"fencing_token"`
	Reason          string                                          `json:"reason"`
	AllowInitialLKG bool                                            `json:"allow_initial_lkg,omitempty"`
	Evidence        platformArtifactVerificationEvidenceWireRequest `json:"evidence"`
}

type platformArtifactVerificationEvidenceWireRequest struct {
	ConsumerConvergence        *bool    `json:"consumer_convergence,omitempty"`
	ConsumerConvergenceState   string   `json:"consumer_convergence_state,omitempty"`
	LocalProbe                 *bool    `json:"local_probe,omitempty"`
	LocalProbeState            string   `json:"local_probe_state,omitempty"`
	PublicSynthetic            *bool    `json:"public_synthetic,omitempty"`
	PublicSyntheticState       string   `json:"public_synthetic_state,omitempty"`
	WatchWindow                *bool    `json:"watch_window,omitempty"`
	WatchWindowState           string   `json:"watch_window_state,omitempty"`
	BaselineMonotonic          *bool    `json:"baseline_monotonic,omitempty"`
	BaselineMonotonicState     string   `json:"baseline_monotonic_state,omitempty"`
	DatabaseRollbackCompatible *bool    `json:"database_rollback_compatible,omitempty"`
	DatabaseRollbackState      string   `json:"database_rollback_compatible_state,omitempty"`
	ExpectedConsumerSetID      string   `json:"expected_consumer_set_id,omitempty"`
	EvidenceRefs               []string `json:"evidence_refs,omitempty"`
}

func buildPlatformArtifactVerifyLKGWireRequest(
	req model.PlatformArtifactVerifyLKGRequest,
	states platformArtifactVerificationEvidenceStateOptions,
	legacyFlagChanged func(string) bool,
) (platformArtifactVerifyLKGWireRequest, error) {
	evidence := platformArtifactVerificationEvidenceWireRequest{
		ConsumerConvergenceState: strings.TrimSpace(states.ConsumerConvergence),
		LocalProbeState:          strings.TrimSpace(states.LocalProbe),
		PublicSyntheticState:     strings.TrimSpace(states.PublicSynthetic),
		WatchWindowState:         strings.TrimSpace(states.WatchWindow),
		BaselineMonotonicState:   strings.TrimSpace(states.BaselineMonotonic),
		DatabaseRollbackState:    strings.TrimSpace(states.DatabaseRollbackCompatible),
		ExpectedConsumerSetID:    strings.TrimSpace(req.Evidence.ExpectedConsumerSetID),
		EvidenceRefs:             append([]string(nil), req.Evidence.EvidenceRefs...),
	}
	evidence.ConsumerConvergence = changedBoolPointer(req.Evidence.ConsumerConvergence, legacyFlagChanged("consumer-convergence"))
	evidence.LocalProbe = changedBoolPointer(req.Evidence.LocalProbe, legacyFlagChanged("local-probe"))
	evidence.PublicSynthetic = changedBoolPointer(req.Evidence.PublicSynthetic, legacyFlagChanged("public-synthetic"))
	evidence.WatchWindow = changedBoolPointer(req.Evidence.WatchWindow, legacyFlagChanged("watch-window"))
	evidence.BaselineMonotonic = changedBoolPointer(req.Evidence.BaselineMonotonic, legacyFlagChanged("baseline-monotonic"))
	evidence.DatabaseRollbackCompatible = changedBoolPointer(req.Evidence.DatabaseRollbackCompatible, legacyFlagChanged("database-rollback-compatible"))

	for _, item := range []struct {
		name   string
		state  string
		legacy *bool
	}{
		{"consumer_convergence", evidence.ConsumerConvergenceState, evidence.ConsumerConvergence},
		{"local_probe", evidence.LocalProbeState, evidence.LocalProbe},
		{"public_synthetic", evidence.PublicSyntheticState, evidence.PublicSynthetic},
		{"watch_window", evidence.WatchWindowState, evidence.WatchWindow},
		{"baseline_monotonic", evidence.BaselineMonotonicState, evidence.BaselineMonotonic},
		{"database_rollback_compatible", evidence.DatabaseRollbackState, evidence.DatabaseRollbackCompatible},
	} {
		if err := validatePlatformVerificationEvidenceInput(item.state, item.legacy); err != nil {
			return platformArtifactVerifyLKGWireRequest{}, fmt.Errorf("%s: %w", item.name, err)
		}
	}

	return platformArtifactVerifyLKGWireRequest{
		FencingToken:    req.FencingToken,
		Reason:          req.Reason,
		AllowInitialLKG: req.AllowInitialLKG,
		Evidence:        evidence,
	}, nil
}

func changedBoolPointer(value, changed bool) *bool {
	if !changed {
		return nil
	}
	copy := value
	return &copy
}

func validatePlatformVerificationEvidenceInput(state string, legacy *bool) error {
	if legacy != nil {
		_, err := platformsafety.ResolveInvariantEvidenceState(state, *legacy)
		return err
	}
	if state == "" {
		return nil
	}
	_, err := platformsafety.ResolveInvariantEvidenceState(state, state == model.InvariantEvidenceStatePass)
	return err
}
