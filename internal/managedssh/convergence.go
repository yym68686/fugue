package managedssh

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"time"
)

// ApplyTimeout bounds asynchronous volume projection, across retries and restarts.
const ApplyTimeout = 2 * time.Minute

type ApplyProgress struct {
	StartedAt time.Time `json:"started_at"`
	Deadline  time.Time `json:"deadline"`
}

// BeginApply preserves the last bound observation, independently of desired state.
// Superseding an in-flight authorization must never extend its waiting deadline.
func (slot *ServingSlot) BeginApply(artifact Artifact, now time.Time) {
	if slot.LoadedArtifact == nil && slot.Evidence != nil && slot.Verified(slot.Evidence.ObservedAt) {
		loaded, evidence := slot.Artifact, *slot.Evidence
		slot.LoadedArtifact, slot.LastValidEvidence = &loaded, &evidence
	}
	if slot.PendingApply == nil {
		slot.PendingApply = &ApplyProgress{StartedAt: now, Deadline: now.Add(ApplyTimeout)}
	}
	slot.Artifact = artifact
}

// ServingFailure may accept a known old artifact. Verified deliberately never does.
func (slot ServingSlot) ServingFailure(now time.Time) string {
	if slot.Verified(now) {
		return ""
	}
	if slot.Evidence == nil {
		return "evidence_missing"
	}
	if slot.LoadedArtifact == nil || slot.Evidence.ArtifactDigest != slot.LoadedArtifact.Digest {
		return "unknown_loaded_digest"
	}
	if failure := evidenceFailure(*slot.LoadedArtifact, slot.Evidence, now); failure != "" {
		return failure
	}
	if !authorizationCovers(slot.Artifact.Configuration, slot.LoadedArtifact.Configuration, now) {
		return "loaded_authorization_revoked_or_expired"
	}
	if slot.PendingApply == nil || !slot.PendingApply.Deadline.After(now) {
		return "apply_deadline_exceeded"
	}
	return ""
}

func (slot ServingSlot) Serving(now time.Time) bool {
	return slot.ServingFailure(now) == ""
}

// AcceptEvidence is called only after the runtime has bound the receipt to the
// live Pod, Service endpoint, SSH host key and target execution identity.
func (slot *ServingSlot) AcceptEvidence(evidence CandidateEvidence, now time.Time) string {
	slot.Evidence = &evidence
	if failure := slot.ServingFailure(now); failure != "" {
		return failure
	}
	if slot.Verified(now) {
		loaded := slot.Artifact
		slot.LoadedArtifact = &loaded
		slot.PendingApply = nil
	}
	positive := evidence
	slot.LastValidEvidence = &positive
	return ""
}

// A previous grant may remain usable only while every permission it grants is
// still authorized. Conservative equality for non-key policy also rejects any
// target, host identity, forwarding, environment or resource-policy change.
func authorizationCovers(desired, loaded Configuration, now time.Time) bool {
	if !desired.ValidUntil.After(now) || !loaded.ValidUntil.After(now) {
		return false
	}
	keys := make(map[string]KeyGrant, len(desired.Keys))
	for _, key := range desired.Keys {
		keys[key.ID] = key
	}
	for _, old := range loaded.Keys {
		current, ok := keys[old.ID]
		if !ok || current.PublicKey != old.PublicKey || !current.ExpiresAt.After(now) || !old.ExpiresAt.After(now) {
			return false
		}
	}
	desired.Generation, desired.ValidUntil, desired.Keys = loaded.Generation, loaded.ValidUntil, nil
	loaded.Keys = nil
	return reflect.DeepEqual(desired, loaded)
}

func (slot ServingSlot) validateConvergence() error {
	if pending := slot.PendingApply; pending != nil && (pending.StartedAt.IsZero() || !pending.Deadline.After(pending.StartedAt) || pending.Deadline.Sub(pending.StartedAt) > ApplyTimeout) {
		return ErrConfiguration
	}
	if loaded := slot.LoadedArtifact; loaded != nil {
		configuration := loaded.Configuration
		if loaded.Schema != slot.Artifact.Schema || loaded.Signature == "" || configuration.Generation == 0 || configuration.Generation > slot.Artifact.Configuration.Generation || configuration.Target != slot.Artifact.Configuration.Target || configuration.Identity != slot.Artifact.Configuration.Identity {
			return ErrArtifact
		}
		raw, err := json.Marshal(configuration)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(raw)
		if loaded.Digest != "sha256:"+hex.EncodeToString(digest[:]) {
			return ErrArtifact
		}
	}
	if evidence := slot.LastValidEvidence; evidence != nil && (slot.LoadedArtifact == nil || evidenceFailure(*slot.LoadedArtifact, evidence, evidence.ObservedAt) != "") {
		return ErrArtifact
	}
	return nil
}
