package platformproducer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"fugue/internal/model"
)

const InputDeferralScope = "platform-config-input-deferral"
const InputDeferralSchema = "fugue.platform.producer.input-deferral/v1"
const MaxInputDeferralBytes = 32768

// InputDeferral is a separately signed, finite new-work restriction. It never
// asserts that already admitted work returned, or that a fleet is covered. The
// original policy remains selected, including its pending recovery and quality
// bindings. No timer, process boot or observation may renew Request.ExpiresAt.
type InputDeferral struct {
	SchemaVersion string                 `json:"schema_version"`
	Generation    string                 `json:"generation"`
	Action        string                 `json:"action"`
	Request       CaptureDeferralRequest `json:"request"`
	RequestDigest string                 `json:"request_digest"`
}

func DecodeInputDeferral(a model.PlatformArtifact) (InputDeferral, error) {
	var p InputDeferral
	raw, err := json.Marshal(a.Content)
	if err != nil || len(raw) > MaxInputDeferralBytes {
		return p, fmt.Errorf("input deferral content exceeds bound or is invalid")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return p, err
	}
	digest, err := p.Request.Digest()
	if err != nil || digest != p.RequestDigest || p.SchemaVersion != InputDeferralSchema ||
		a.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || a.ScopeKey != InputDeferralScope ||
		p.Generation != a.Generation || p.Action != "defer" && p.Action != "release" {
		return p, fmt.Errorf("input deferral identity or request invalid")
	}
	expected := p.Request.OperationID
	if p.Action == "release" {
		expected += ":release"
	}
	if p.Generation != expected {
		return p, fmt.Errorf("input deferral generation must identify the immutable operation and action")
	}
	for _, binding := range p.Request.Authorities {
		if len(binding.Policy.ArtifactID) > 256 || len(binding.Policy.ReleaseID) > 256 || binding.Policy.FencingToken > 9007199254740991 {
			return p, fmt.Errorf("input deferral authority identity exceeds bound")
		}
	}
	return p, nil
}

func (p InputDeferral) IdempotencyKey() string {
	return "producer-input-deferral:" + p.Action + ":" + p.Request.OperationID
}

// Defers uses the database observation time. A read failure or a clock before
// the signed start is uncertainty, not permission for a new allocation. Expiry
// removes only this optional gate; it grants no retry, replay, freshness or pin.
func (p InputDeferral) Defers(at time.Time, scope string, policy model.PlatformPublicationPrecondition) (bool, error) {
	if p.Action == "release" {
		return false, nil // a terminal release stays OFF even during clock uncertainty
	}
	if at.Before(p.Request.RequestedAt) {
		return false, fmt.Errorf("input deferral clock precedes signed request")
	}
	if !at.Before(p.Request.ExpiresAt) {
		return false, nil
	}
	for _, binding := range p.Request.Authorities {
		if binding.Scope == scope {
			if binding.Policy != policy {
				return false, fmt.Errorf("input deferral authority changed")
			}
			return true, nil
		}
	}
	return false, nil
}

// ValidateInputDeferralTransition runs under the durable selection barrier.
// Consumed operation keys are also retained by the existing release ledger.
// Returning an old idempotent receipt never republishes its superseded release.
func ValidateInputDeferralTransition(next InputDeferral, previous *InputDeferral, at time.Time) error {
	if next.Action == "release" {
		if previous == nil || previous.RequestDigest != next.RequestDigest || previous.Request.OperationID != next.Request.OperationID {
			return fmt.Errorf("input deferral release does not match selected operation")
		}
		return nil
	}
	if at.Before(next.Request.RequestedAt) {
		return fmt.Errorf("input deferral start is in the future")
	}
	if !at.Before(next.Request.ExpiresAt) {
		return fmt.Errorf("input deferral already expired")
	}
	if previous != nil {
		if previous.Request.OperationID == next.Request.OperationID {
			if previous.Action == "release" || previous.RequestDigest != next.RequestDigest {
				return fmt.Errorf("input deferral operation is terminal or changed")
			}
		} else if previous.Action == "defer" && at.Before(previous.Request.ExpiresAt) {
			return fmt.Errorf("input deferral cannot replace an unexpired operation")
		}
	}
	return nil
}
