package api

import (
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

type platformArtifactVerifyLKGHTTPRequest struct {
	FencingToken    int64                                           `json:"fencing_token"`
	Reason          string                                          `json:"reason"`
	AllowInitialLKG bool                                            `json:"allow_initial_lkg,omitempty"`
	Evidence        platformArtifactVerificationEvidenceHTTPRequest `json:"evidence"`
}

type platformArtifactVerificationEvidenceHTTPRequest struct {
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

type platformVerificationEvidenceState struct {
	Name  string
	State string
}

func (req platformArtifactVerifyLKGHTTPRequest) modelRequest() (model.PlatformArtifactVerifyLKGRequest, []platformVerificationEvidenceState, error) {
	evidence, states, err := req.Evidence.modelEvidence()
	if err != nil {
		return model.PlatformArtifactVerifyLKGRequest{}, nil, err
	}
	return model.PlatformArtifactVerifyLKGRequest{
		FencingToken:    req.FencingToken,
		Reason:          req.Reason,
		AllowInitialLKG: req.AllowInitialLKG,
		Evidence:        evidence,
	}, states, nil
}

func (req platformArtifactVerificationEvidenceHTTPRequest) modelEvidence() (model.PlatformArtifactVerificationEvidence, []platformVerificationEvidenceState, error) {
	evidence := model.PlatformArtifactVerificationEvidence{
		ExpectedConsumerSetID: strings.TrimSpace(req.ExpectedConsumerSetID),
		EvidenceRefs:          append([]string(nil), req.EvidenceRefs...),
	}
	states := make([]platformVerificationEvidenceState, 0, 6)
	resolve := func(name, explicitState string, legacyPass *bool, target *bool) error {
		state, err := resolveOptionalInvariantEvidenceState(explicitState, legacyPass)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		*target = state == model.InvariantEvidenceStatePass
		states = append(states, platformVerificationEvidenceState{Name: name, State: state})
		return nil
	}

	for _, item := range []struct {
		name          string
		explicitState string
		legacyPass    *bool
		target        *bool
	}{
		{"consumer_convergence", req.ConsumerConvergenceState, req.ConsumerConvergence, &evidence.ConsumerConvergence},
		{"local_probe", req.LocalProbeState, req.LocalProbe, &evidence.LocalProbe},
		{"public_synthetic", req.PublicSyntheticState, req.PublicSynthetic, &evidence.PublicSynthetic},
		{"watch_window", req.WatchWindowState, req.WatchWindow, &evidence.WatchWindow},
		{"baseline_monotonic", req.BaselineMonotonicState, req.BaselineMonotonic, &evidence.BaselineMonotonic},
		{"database_rollback_compatible", req.DatabaseRollbackState, req.DatabaseRollbackCompatible, &evidence.DatabaseRollbackCompatible},
	} {
		if err := resolve(item.name, item.explicitState, item.legacyPass, item.target); err != nil {
			return model.PlatformArtifactVerificationEvidence{}, nil, err
		}
	}
	return evidence, states, nil
}

func resolveOptionalInvariantEvidenceState(explicitState string, legacyPass *bool) (string, error) {
	if legacyPass != nil {
		return platformsafety.ResolveInvariantEvidenceState(explicitState, *legacyPass)
	}
	state := strings.TrimSpace(explicitState)
	if state == "" {
		return platformsafety.ResolveInvariantEvidenceState("", false)
	}
	return platformsafety.ResolveInvariantEvidenceState(state, state == model.InvariantEvidenceStatePass)
}

func nonPassingPlatformVerificationEvidence(states []platformVerificationEvidenceState) []string {
	nonPassing := make([]string, 0, len(states))
	for _, evidence := range states {
		if evidence.State != model.InvariantEvidenceStatePass {
			nonPassing = append(nonPassing, evidence.Name+"="+evidence.State)
		}
	}
	return nonPassing
}
