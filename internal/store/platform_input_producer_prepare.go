package store

import (
	"encoding/json"

	"fugue/internal/model"
)

// These are the original producer method arguments, not a caller-supplied
// release guard. The same builders below serve both the business methods and
// cold preparation. A ticket does not confer producer or mutation authority.
type ProducerInputPreparationIntent struct {
	Action                string                                  `json:"action"`
	PolicyReleaseID       string                                  `json:"policy_release_id"`
	PreviousReleaseID     string                                  `json:"previous_release_id,omitempty"`
	PreviousFullReleaseID string                                  `json:"previous_full_release_id,omitempty"`
	BaselineArtifactID    string                                  `json:"baseline_artifact_id,omitempty"`
	FailedReleaseID       string                                  `json:"failed_release_id,omitempty"`
	CanaryRuleRef         string                                  `json:"canary_rule_ref,omitempty"`
	Rollback              *model.PlatformArtifactRollbackRequest  `json:"rollback,omitempty"`
	Verification          *model.PlatformArtifactVerifyLKGRequest `json:"verification,omitempty"`
}

func producedShadowIntent(id, policy, previous string) (model.PlatformArtifactReleaseRequest, *platformProducerReleaseGuard) {
	return model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", Reason: "versioned platform configuration producer", IdempotencyKey: "producer/" + policy + "/" + id},
		&platformProducerReleaseGuard{PolicyReleaseID: policy, PreviousReleaseID: previous}
}

func producedTrafficIntent(id, policy, channel, previous, full, baseline, canary string) (model.PlatformArtifactReleaseRequest, *platformProducerReleaseGuard, error) {
	if channel != model.PlatformArtifactReleaseChannelGray && channel != model.PlatformArtifactReleaseChannelFull {
		return model.PlatformArtifactReleaseRequest{}, nil, ErrInvalidInput
	}
	return model.PlatformArtifactReleaseRequest{ReleaseChannel: channel, CanaryRuleRef: canary, Reason: "versioned platform configuration serving producer", IdempotencyKey: "producer/" + policy + "/" + channel + "/" + id},
		&platformProducerReleaseGuard{PolicyReleaseID: policy, PreviousReleaseID: previous, PreviousFullReleaseID: full, BaselineArtifactID: baseline, Phase: channel}, nil
}

func producedRollbackIntent(failed, policy, full, baseline string, req model.PlatformArtifactRollbackRequest) (*platformProducerReleaseGuard, error) {
	if req.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || req.SoftOverride || req.ForcePublish || req.KernelBreakGlass != nil {
		return nil, ErrInvalidInput
	}
	return &platformProducerReleaseGuard{PolicyReleaseID: policy, PreviousReleaseID: failed, PreviousFullReleaseID: full, BaselineArtifactID: baseline, FailedReleaseID: failed, Phase: "rollback"}, nil
}

func producedVerificationIntent(release, policy, baseline, phase string, req model.PlatformArtifactVerifyLKGRequest) (*platformProducerReleaseGuard, error) {
	if req.AllowInitialLKG || phase != "verify" && phase != "verify-recovery" {
		return nil, ErrInvalidInput
	}
	return &platformProducerReleaseGuard{PolicyReleaseID: policy, PreviousReleaseID: release, PreviousFullReleaseID: release, BaselineArtifactID: baseline, Phase: phase}, nil
}

func (c PublicationInputPreparationContract) producerIntent() (model.PlatformArtifactReleaseRequest, *platformProducerReleaseGuard, error) {
	p := c.Producer
	if p == nil || p.PolicyReleaseID == "" {
		return model.PlatformArtifactReleaseRequest{}, nil, ErrInvalidInput
	}
	for _, value := range []string{p.PolicyReleaseID, p.PreviousReleaseID, p.PreviousFullReleaseID, p.BaselineArtifactID, p.FailedReleaseID, p.CanaryRuleRef} {
		if len(value) > 512 {
			return model.PlatformArtifactReleaseRequest{}, nil, ErrInvalidInput
		}
	}
	raw, err := json.Marshal(p)
	if err != nil || len(raw) > 65536 {
		return model.PlatformArtifactReleaseRequest{}, nil, ErrInvalidInput
	}
	var req model.PlatformArtifactReleaseRequest
	var guard *platformProducerReleaseGuard
	switch p.Action {
	case "shadow":
		if c.Mode != "release" || p.PreviousFullReleaseID != "" || p.BaselineArtifactID != "" || p.FailedReleaseID != "" || p.CanaryRuleRef != "" || p.Rollback != nil || p.Verification != nil {
			return req, nil, ErrInvalidInput
		}
		req, guard = producedShadowIntent(c.ArtifactID, p.PolicyReleaseID, p.PreviousReleaseID)
	case "gray", "full":
		if c.Mode != "release" || p.PreviousFullReleaseID == "" || p.BaselineArtifactID == "" || p.FailedReleaseID != "" || p.Rollback != nil || p.Verification != nil {
			return req, nil, ErrInvalidInput
		}
		req, guard, err = producedTrafficIntent(c.ArtifactID, p.PolicyReleaseID, p.Action, p.PreviousReleaseID, p.PreviousFullReleaseID, p.BaselineArtifactID, p.CanaryRuleRef)
	case "rollback":
		if c.Mode != "rollback" || p.PreviousReleaseID != "" || p.PreviousFullReleaseID == "" || p.BaselineArtifactID == "" || p.FailedReleaseID == "" || p.CanaryRuleRef != "" || p.Rollback == nil || p.Verification != nil || p.Rollback.ToGeneration != c.RollbackGeneration {
			return req, nil, ErrInvalidInput
		}
		guard, err = producedRollbackIntent(p.FailedReleaseID, p.PolicyReleaseID, p.PreviousFullReleaseID, p.BaselineArtifactID, *p.Rollback)
	case "verify", "verify-recovery":
		if c.Mode != "lkg" || p.PreviousReleaseID != "" || p.PreviousFullReleaseID != "" || p.BaselineArtifactID == "" || p.FailedReleaseID != "" || p.CanaryRuleRef != "" || p.Rollback != nil || p.Verification == nil {
			return req, nil, ErrInvalidInput
		}
		guard, err = producedVerificationIntent(c.ReleaseID, p.PolicyReleaseID, p.BaselineArtifactID, p.Action, *p.Verification)
	default:
		return req, nil, ErrInvalidInput
	}
	// The native contract carries the exact request constructed by the real
	// producer workflow, including reason/idempotency and all original evidence.
	actual, _ := json.Marshal(c.Request)
	expected, _ := json.Marshal(req)
	if err != nil || string(actual) != string(expected) {
		return req, nil, ErrInvalidInput
	}
	return req, guard, nil
}
