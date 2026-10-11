package managedssh

import "time"

type SlotStatus struct {
	ID             string             `json:"id"`
	Namespace      string             `json:"namespace"`
	Image          string             `json:"image"`
	HelperDigest   string             `json:"helper_digest"`
	Architecture   string             `json:"architecture"`
	ArtifactDigest string             `json:"artifact_digest"`
	Generation     uint64             `json:"generation"`
	Target         Target             `json:"target"`
	Evidence       *CandidateEvidence `json:"evidence,omitempty"`
	Verified       bool               `json:"verified"`
	ValidUntil     time.Time          `json:"valid_until"`
	CreatedAt      time.Time          `json:"created_at"`
	DrainingSince  *time.Time         `json:"draining_since,omitempty"`
	RetiringAt     *time.Time         `json:"retiring_at,omitempty"`
	RetiredAt      *time.Time         `json:"retired_at,omitempty"`
}

type ControlStatus struct {
	IdentityID       string       `json:"identity_id"`
	Revision         uint64       `json:"revision"`
	Generation       uint64       `json:"generation"`
	IntentGeneration uint64       `json:"intent_generation"`
	RouteGeneration  uint64       `json:"route_generation"`
	RollbackHold     bool         `json:"rollback_hold"`
	Active           *SlotStatus  `json:"active,omitempty"`
	Candidate        *SlotStatus  `json:"candidate,omitempty"`
	Draining         []SlotStatus `json:"draining"`
	Retired          []SlotStatus `json:"retired"`
	ActivationActor  string       `json:"activation_actor,omitempty"`
	ActivatedAt      *time.Time   `json:"activated_at,omitempty"`
}

func (state ControlState) PublicStatus(now time.Time) ControlStatus {
	status := ControlStatus{IdentityID: state.IdentityID, Revision: state.Revision, Generation: state.Generation, IntentGeneration: state.IntentGeneration, RouteGeneration: state.RouteGeneration, Draining: []SlotStatus{}, ActivationActor: state.ActivationActor, ActivatedAt: state.ActivatedAt}
	status.RollbackHold = state.RollbackHold
	status.Retired = []SlotStatus{}
	if state.Active != nil {
		slot := state.Active.PublicStatus(now)
		status.Active = &slot
	}
	if state.Candidate != nil {
		slot := state.Candidate.PublicStatus(now)
		status.Candidate = &slot
	}
	for _, slot := range state.Draining {
		status.Draining = append(status.Draining, slot.PublicStatus(now))
	}
	for _, slot := range state.Retired {
		status.Retired = append(status.Retired, slot.PublicStatus(now))
	}
	return status
}

func (slot ServingSlot) PublicStatus(now time.Time) SlotStatus {
	return SlotStatus{ID: slot.ID, Namespace: slot.Namespace, Image: slot.Release.Image, HelperDigest: slot.Release.HelperDigest, Architecture: slot.Release.Architecture, ArtifactDigest: slot.Artifact.Digest, Generation: slot.Artifact.Configuration.Generation, Target: slot.Artifact.Configuration.Target, Evidence: slot.Evidence, Verified: slot.RetiringAt == nil && slot.RetiredAt == nil && slot.Verified(now), ValidUntil: slot.Artifact.Configuration.ValidUntil, CreatedAt: slot.CreatedAt, DrainingSince: slot.DrainingSince, RetiringAt: slot.RetiringAt, RetiredAt: slot.RetiredAt}
}
