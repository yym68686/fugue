package managedssh

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"
)

var ErrCandidateUnverified = errors.New("managed SSH candidate lacks fresh bound serving evidence")

type RuntimeRelease struct {
	Image        string `json:"image"`
	HelperDigest string `json:"helper_digest"`
	Architecture string `json:"architecture"`
}

type CandidateEvidence struct {
	ArtifactDigest      string    `json:"artifact_digest"`
	IdentityID          string    `json:"identity_id"`
	Fingerprint         string    `json:"fingerprint"`
	ServerPodUID        string    `json:"server_pod_uid"`
	TargetPodUID        string    `json:"target_pod_uid"`
	TargetContainerID   string    `json:"target_container_id"`
	KeyExchangeVerified bool      `json:"key_exchange_verified"`
	RuntimeVerified     bool      `json:"runtime_verified"`
	ObservedAt          time.Time `json:"observed_at"`
	ExpiresAt           time.Time `json:"expires_at"`
}

type ServingSlot struct {
	ID                string             `json:"id"`
	Namespace         string             `json:"namespace"`
	Release           RuntimeRelease     `json:"release"`
	Artifact          Artifact           `json:"artifact"`
	LoadedArtifact    *Artifact          `json:"loaded_artifact,omitempty"`
	LastValidEvidence *CandidateEvidence `json:"last_valid_evidence,omitempty"`
	PendingApply      *ApplyProgress     `json:"pending_apply,omitempty"`
	Evidence          *CandidateEvidence `json:"evidence,omitempty"`
	CreatedAt         time.Time          `json:"created_at"`
	DrainingSince     *time.Time         `json:"draining_since,omitempty"`
	RetiringAt        *time.Time         `json:"retiring_at,omitempty"`
	RetiredAt         *time.Time         `json:"retired_at,omitempty"`
}

type ControlState struct {
	Schema            string        `json:"schema"`
	TenantID          string        `json:"tenant_id"`
	AppID             string        `json:"app_id"`
	IdentityID        string        `json:"identity_id"`
	Revision          uint64        `json:"revision"`
	Generation        uint64        `json:"generation"`
	IntentGeneration  uint64        `json:"intent_generation"`
	RouteGeneration   uint64        `json:"route_generation"`
	Active            *ServingSlot  `json:"active,omitempty"`
	Candidate         *ServingSlot  `json:"candidate,omitempty"`
	Draining          []ServingSlot `json:"draining,omitempty"`
	Retired           []ServingSlot `json:"retired,omitempty"`
	ActivationActor   string        `json:"activation_actor,omitempty"`
	ActivatedAt       *time.Time    `json:"activated_at,omitempty"`
	LegacyRouteDigest string        `json:"legacy_route_digest,omitempty"`
	RollbackHold      bool          `json:"rollback_hold"`
}

var imageDigestPattern = regexp.MustCompile(`^[^\s]+@sha256:[a-f0-9]{64}$`)
var sha256DigestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func ValidateRuntimeRelease(release RuntimeRelease) error {
	if !imageDigestPattern.MatchString(release.Image) || !sha256DigestPattern.MatchString(release.HelperDigest) || (release.Architecture != "amd64" && release.Architecture != "arm64") {
		return ErrConfiguration
	}
	return nil
}

func SlotID(identityID string, target Target, release RuntimeRelease) string {
	raw, _ := json.Marshal(struct {
		IdentityID string
		Target     Target
		Release    RuntimeRelease
	}{identityID, target, release})
	digest := sha256.Sum256(raw)
	return "ssh-" + hex.EncodeToString(digest[:16])
}

func (slot ServingSlot) Verified(now time.Time) bool {
	return slot.VerificationFailure(now) == ""
}

func (slot ServingSlot) VerificationFailure(now time.Time) string {
	evidence := slot.Evidence
	return evidenceFailure(slot.Artifact, evidence, now)
}

func evidenceFailure(artifact Artifact, evidence *CandidateEvidence, now time.Time) string {
	configuration := artifact.Configuration
	switch {
	case evidence == nil:
		return "evidence_missing"
	case evidence.ArtifactDigest != artifact.Digest:
		return "artifact_digest_mismatch"
	case evidence.IdentityID != configuration.Identity.ID || evidence.Fingerprint != configuration.Identity.Fingerprint:
		return "host_identity_mismatch"
	case evidence.ServerPodUID == "":
		return "server_pod_missing"
	case evidence.TargetPodUID != configuration.Target.PodUID || evidence.TargetContainerID != configuration.Target.ContainerID:
		return "target_identity_mismatch"
	case !evidence.KeyExchangeVerified:
		return "key_exchange_unverified"
	case !evidence.RuntimeVerified:
		return "runtime_unverified"
	case evidence.ObservedAt.IsZero():
		return "observation_time_missing"
	case evidence.ObservedAt.After(now.Add(5 * time.Second)):
		return "observation_after_verification_clock"
	case !evidence.ExpiresAt.After(now):
		return "observation_expired"
	case !evidence.ExpiresAt.After(evidence.ObservedAt) || evidence.ExpiresAt.After(evidence.ObservedAt.Add(2*time.Minute)):
		return "observation_lifetime_invalid"
	case !configuration.ValidUntil.After(evidence.ExpiresAt):
		return "observation_exceeds_authorization"
	default:
		return ""
	}
}

func (state ControlState) Validate() error {
	if state.Schema != "fugue.managed-ssh-control/v1" || !safeToken(state.TenantID) || !safeToken(state.AppID) || !safeToken(state.IdentityID) || state.Revision == 0 || state.Generation == 0 || state.IntentGeneration == 0 || len(state.Draining) > 8 || len(state.Retired) > 64 {
		return ErrConfiguration
	}
	if (state.Active == nil) != (state.RouteGeneration == 0) {
		return ErrConfiguration
	}
	if state.Active != nil && (state.ActivatedAt == nil || state.ActivatedAt.IsZero() || state.ActivationActor == "" || state.LegacyRouteDigest == "") {
		return ErrConfiguration
	}
	seen := map[string]bool{}
	for _, serving := range []*ServingSlot{state.Active, state.Candidate} {
		if serving != nil && (serving.RetiringAt != nil || serving.RetiredAt != nil || serving.Artifact.Configuration.Draining) {
			return ErrConfiguration
		}
	}
	for _, slot := range state.Slots() {
		configuration := slot.Artifact.Configuration
		if seen[slot.ID] || slot.ID != SlotID(state.IdentityID, configuration.Target, slot.Release) || slot.Namespace == "" || slot.CreatedAt.IsZero() || ValidateRuntimeRelease(slot.Release) != nil || configuration.Identity.ID != state.IdentityID || configuration.Identity.TenantID != state.TenantID || configuration.Identity.AppID != state.AppID || configuration.Target.TenantID != state.TenantID || configuration.Target.AppID != state.AppID || configuration.Generation == 0 || configuration.Generation > state.Generation || slot.Artifact.Schema != "fugue.managed-ssh-artifact/v1" || slot.Artifact.Signature == "" || !sha256DigestPattern.MatchString(slot.Artifact.Digest) {
			return ErrConfiguration
		}
		raw, err := json.Marshal(configuration)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(raw)
		if slot.Artifact.Digest != "sha256:"+hex.EncodeToString(digest[:]) {
			return ErrArtifact
		}
		if err := slot.validateConvergence(); err != nil {
			return err
		}
		seen[slot.ID] = true
	}
	if len(seen) == 0 {
		return ErrConfiguration
	}
	for _, slot := range state.Retired {
		if seen[slot.ID] || slot.RetiredAt == nil || slot.RetiringAt == nil || slot.RetiredAt.Before(*slot.RetiringAt) || !slot.Artifact.Configuration.Draining {
			return ErrConfiguration
		}
		seen[slot.ID] = true
	}
	return nil
}

func (state ControlState) Slots() []ServingSlot {
	slots := []ServingSlot{}
	if state.Active != nil {
		slots = append(slots, *state.Active)
	}
	if state.Candidate != nil {
		slots = append(slots, *state.Candidate)
	}
	slots = append(slots, state.Draining...)
	return slots
}

func (state *ControlState) Slot(identifier string) *ServingSlot {
	if state.Active != nil && state.Active.ID == identifier {
		return state.Active
	}
	if state.Candidate != nil && state.Candidate.ID == identifier {
		return state.Candidate
	}
	for index := range state.Draining {
		if state.Draining[index].ID == identifier {
			return &state.Draining[index]
		}
	}
	return nil
}

func (state ControlState) ActivateCandidate(digest, fingerprint, legacyRouteDigest, actor string, now time.Time) (ControlState, error) {
	if state.Validate() != nil || state.Candidate == nil || state.Candidate.Artifact.Digest != digest || state.Candidate.Artifact.Configuration.Identity.Fingerprint != fingerprint || actor == "" || legacyRouteDigest == "" || (state.LegacyRouteDigest != "" && state.LegacyRouteDigest != legacyRouteDigest) || len(state.Draining) >= 8 {
		return state, ErrConfiguration
	}
	if !state.Candidate.Verified(now) {
		return state, ErrCandidateUnverified
	}
	state.Draining = append([]ServingSlot(nil), state.Draining...)
	if state.Active != nil {
		previous := *state.Active
		previous.DrainingSince = &now
		state.Draining = append(state.Draining, previous)
	}
	promoted := *state.Candidate
	state.Active, state.Candidate = &promoted, nil
	state.RouteGeneration++
	state.Revision++
	state.ActivationActor, state.ActivatedAt, state.LegacyRouteDigest = actor, &now, legacyRouteDigest
	state.RollbackHold = false
	return state, nil
}

func (state ControlState) RollbackSlot(identifier, digest, fingerprint, actor string, now time.Time) (ControlState, error) {
	if state.Validate() != nil || state.Active == nil || actor == "" {
		return state, ErrConfiguration
	}
	for index, slot := range state.Draining {
		if slot.ID != identifier {
			continue
		}
		if slot.RetiringAt != nil || slot.Artifact.Configuration.Draining {
			return state, ErrConfiguration
		}
		if slot.Artifact.Digest != digest || slot.Artifact.Configuration.Identity.Fingerprint != fingerprint {
			return state, ErrConfiguration
		}
		if !slot.Verified(now) {
			return state, ErrCandidateUnverified
		}
		retained := make([]ServingSlot, 0, len(state.Draining))
		retained = append(retained, state.Draining[:index]...)
		retained = append(retained, state.Draining[index+1:]...)
		previous := *state.Active
		previous.DrainingSince = &now
		state.Draining = append(retained, previous)
		slot.DrainingSince = nil
		state.Active = &slot
		state.RouteGeneration++
		state.Revision++
		state.ActivationActor, state.ActivatedAt, state.RollbackHold = actor, &now, true
		return state, nil
	}
	return state, ErrConfiguration
}
