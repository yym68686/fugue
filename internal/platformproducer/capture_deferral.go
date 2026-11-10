package platformproducer

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

const (
	CaptureDeferralSchema        = "fugue.platform.producer.capture-deferral/v1"
	CaptureDeferralControlSchema = "fugue.platform.producer.capture-deferral-control/v1"
	MaxCaptureDeferral           = 30 * time.Minute
	maxDeferredScopes            = 32
)

// CaptureDeferralAuthority binds a control to an exact signed producer policy.
// It does not replace that policy or change the authority used by recovery.
type CaptureDeferralAuthority struct {
	Scope  string                                `json:"scope"`
	Policy model.PlatformPublicationPrecondition `json:"policy"`
}

type CaptureDeferralRequest struct {
	SchemaVersion string                     `json:"schema_version"`
	OperationID   string                     `json:"operation_id"`
	RequestedAt   time.Time                  `json:"requested_at"`
	ExpiresAt     time.Time                  `json:"expires_at"`
	Authorities   []CaptureDeferralAuthority `json:"authorities"`
}

// A control targets the complete immutable original request, not an operation
// name alone. Observe may advance a drain or persist an expiry; release only
// removes this optional gate. Neither action renews or creates an operation.
type CaptureDeferralControlRequest struct {
	SchemaVersion string `json:"schema_version"`
	OperationID   string `json:"operation_id"`
	RequestDigest string `json:"request_digest"`
	Action        string `json:"action"`
}

func (request CaptureDeferralControlRequest) Validate() error {
	if request.SchemaVersion != CaptureDeferralControlSchema || !validCaptureDeferralOperationID(request.OperationID) || !ValidDigest(request.RequestDigest) || request.Action != "observe" && request.Action != "release" {
		return fmt.Errorf("capture deferral control target or action invalid")
	}
	return nil
}

// Digest lets a caller address the original request after a lost prepare
// response. Computing it grants no permission to prepare, observe or release.
func (request CaptureDeferralRequest) Digest() (string, error) {
	return captureDeferralDigest(request)
}

// CaptureDeferralRecord is a value for a durable, authenticated control adapter.
// This state machine itself performs no I/O or authorization. The
// adapter MUST serialize capture admission and this record's CAS, retain the
// terminal operation tombstone, and reject a second operation for a held scope.
// Without that adapter this type is not a production control path.
type CaptureDeferralRecord struct {
	Request       CaptureDeferralRequest `json:"request"`
	RequestDigest string                 `json:"request_digest"`
	Phase         string                 `json:"phase"`
	ObservedAt    time.Time              `json:"observed_at"`
	ActivatedAt   *time.Time             `json:"activated_at,omitempty"`
	EndedAt       *time.Time             `json:"ended_at,omitempty"`
}

// CaptureDeferralObservation must come from the same serialized admission view.
// Every requested scope must be present in both maps; missing is never zero.
// InFlight counts only already admitted new-input work, never recovery, reader,
// heartbeat, no-new-input observations, or exit/audit work.
type CaptureDeferralObservation struct {
	At          time.Time
	Authorities map[string]model.PlatformPublicationPrecondition
	InFlight    map[string]uint64
}

func validCaptureDeferralOperationID(id string) bool {
	return id != "" && len(id) <= 128 && strings.TrimSpace(id) == id && !strings.ContainsAny(id, "\x00\r\n\t ")
}

func captureDeferralDigest(request CaptureDeferralRequest) (string, error) {
	if request.SchemaVersion != CaptureDeferralSchema || !validCaptureDeferralOperationID(request.OperationID) {
		return "", fmt.Errorf("invalid capture deferral identity")
	}
	if request.RequestedAt.IsZero() || request.ExpiresAt.IsZero() || !request.ExpiresAt.After(request.RequestedAt) || request.ExpiresAt.Sub(request.RequestedAt) > MaxCaptureDeferral {
		return "", fmt.Errorf("capture deferral exceeds its absolute time bound")
	}
	if len(request.Authorities) == 0 || len(request.Authorities) > maxDeferredScopes {
		return "", fmt.Errorf("capture deferral scope count invalid")
	}
	request.Authorities = slices.Clone(request.Authorities)
	slices.SortFunc(request.Authorities, func(a, b CaptureDeferralAuthority) int { return strings.Compare(a.Scope, b.Scope) })
	for i, authority := range request.Authorities {
		scope := authority.Scope
		if scope != Scope {
			cell := strings.TrimPrefix(scope, Scope+":")
			canonical, err := PolicyScopeForTarget(platformconfig.AuthorityCellScope(cell))
			if err != nil || canonical != scope {
				return "", fmt.Errorf("capture deferral scope invalid")
			}
		}
		p := authority.Policy
		if i > 0 && request.Authorities[i-1].Scope == scope || p.ArtifactID == "" || p.ReleaseID == "" || p.FencingToken <= 0 || !ValidDigest(p.ContentHash) {
			return "", fmt.Errorf("capture deferral requires unique exact policy bindings")
		}
	}
	// Normalize time zones and discard process-local monotonic clock readings.
	// A restart must read the original absolute deadline, never now + duration.
	request.RequestedAt, request.ExpiresAt = request.RequestedAt.UTC(), request.ExpiresAt.UTC()
	return platformconfig.Digest(request)
}

// PrepareCaptureDeferral starts a one-shot drain without canceling existing
// work. previous must be the durable lookup for OperationID, including expired
// tombstones. The fixed expiry includes drain time; activation never renews it.
func PrepareCaptureDeferral(request CaptureDeferralRequest, previous *CaptureDeferralRecord, observation CaptureDeferralObservation) (CaptureDeferralRecord, error) {
	if previous != nil {
		return CaptureDeferralRecord{}, fmt.Errorf("capture deferral operation already consumed")
	}
	digest, err := captureDeferralDigest(request)
	if err != nil {
		return CaptureDeferralRecord{}, err
	}
	if observation.At.Before(request.RequestedAt) || !observation.At.Before(request.ExpiresAt) {
		return CaptureDeferralRecord{}, fmt.Errorf("capture deferral request outside its time window")
	}
	if err := checkCaptureDeferralObservation(request, observation); err != nil {
		return CaptureDeferralRecord{}, err
	}
	request.Authorities = slices.Clone(request.Authorities)
	record := CaptureDeferralRecord{Request: request, RequestDigest: digest, Phase: "draining", ObservedAt: observation.At}
	activateCaptureDeferral(&record, observation)
	return record, nil
}

func checkCaptureDeferralObservation(request CaptureDeferralRequest, observation CaptureDeferralObservation) error {
	for _, authority := range request.Authorities {
		current, present := observation.Authorities[authority.Scope]
		_, counted := observation.InFlight[authority.Scope]
		if !present || current != authority.Policy || !counted {
			return fmt.Errorf("capture deferral exact authority or drain observation unavailable")
		}
	}
	return nil
}

func activateCaptureDeferral(record *CaptureDeferralRecord, observation CaptureDeferralObservation) {
	for _, authority := range record.Request.Authorities {
		if observation.InFlight[authority.Scope] != 0 {
			return
		}
	}
	record.Phase = "active"
	at := observation.At
	record.ActivatedAt = &at
}

func (record CaptureDeferralRecord) validate() error {
	digest, err := captureDeferralDigest(record.Request)
	if err != nil || digest != record.RequestDigest || record.ObservedAt.Before(record.Request.RequestedAt) {
		return fmt.Errorf("capture deferral record invalid")
	}
	if record.ActivatedAt != nil && (record.ActivatedAt.Before(record.Request.RequestedAt) || !record.ActivatedAt.Before(record.Request.ExpiresAt) || record.ActivatedAt.After(record.ObservedAt)) {
		return fmt.Errorf("capture deferral activation invalid")
	}
	switch record.Phase {
	case "draining", "active":
		if record.EndedAt != nil || !record.ObservedAt.Before(record.Request.ExpiresAt) || (record.Phase == "active") != (record.ActivatedAt != nil) {
			return fmt.Errorf("capture deferral live phase invalid")
		}
	case "expired", "released", "invalidated":
		if record.EndedAt == nil || !record.EndedAt.Equal(record.ObservedAt) || record.Phase == "expired" && record.ObservedAt.Before(record.Request.ExpiresAt) {
			return fmt.Errorf("capture deferral terminal phase invalid")
		}
	default:
		return fmt.Errorf("capture deferral phase invalid")
	}
	return nil
}

// AdvanceCaptureDeferral must be committed by the adapter's original-version
// CAS before its admission decision is used. Terminal records never reactivate.
// Authority drift or clock regression ends this control instead of extending
// an availability restriction. Normal signature/resource/TTL guards still run.
func AdvanceCaptureDeferral(record CaptureDeferralRecord, observation CaptureDeferralObservation) (CaptureDeferralRecord, error) {
	if err := record.validate(); err != nil {
		return record, err
	}
	if record.EndedAt != nil {
		return record, nil
	}
	if observation.At.Before(record.ObservedAt) {
		return endCaptureDeferral(record, "invalidated", record.ObservedAt), nil
	}
	if !observation.At.Before(record.Request.ExpiresAt) {
		return endCaptureDeferral(record, "expired", observation.At), nil
	}
	if err := checkCaptureDeferralObservation(record.Request, observation); err != nil {
		return endCaptureDeferral(record, "invalidated", observation.At), nil
	}
	if record.Phase == "active" {
		for _, authority := range record.Request.Authorities {
			if observation.InFlight[authority.Scope] != 0 {
				return record, fmt.Errorf("new input admitted through active capture deferral")
			}
		}
	}
	record.ObservedAt = observation.At
	if record.Phase == "draining" {
		activateCaptureDeferral(&record, observation)
	}
	return record, nil
}

// Validate checks a persisted record before an adapter uses any of its scopes
// or time bounds in database queries. It grants no transition or admission.
func (record CaptureDeferralRecord) Validate() error { return record.validate() }

func endCaptureDeferral(record CaptureDeferralRecord, phase string, at time.Time) CaptureDeferralRecord {
	record.Phase, record.ObservedAt = phase, at
	record.EndedAt = &at
	return record
}

// ReleaseCaptureDeferral removes only this optional-work gate. It grants no
// replay, capture, publication, TTL renewal, resource or migration authority.
func ReleaseCaptureDeferral(record CaptureDeferralRecord, at time.Time) (CaptureDeferralRecord, error) {
	if err := record.validate(); err != nil {
		return record, err
	}
	if record.EndedAt != nil {
		return record, nil
	}
	if at.Before(record.ObservedAt) {
		return record, fmt.Errorf("capture deferral release time regressed")
	}
	if !at.Before(record.Request.ExpiresAt) {
		return endCaptureDeferral(record, "expired", at), nil
	}
	return endCaptureDeferral(record, "released", at), nil
}

// DefersNewInput is deliberately limited to new-input admission. Call it only
// after Advance's durable CAS, and after pending safety recovery/timeout and
// no-new-input observations. false removes this control alone; it is not an
// authorization result. No policy Mode, evidence timestamp or TTL is modified.
func (record CaptureDeferralRecord) DefersNewInput(scope string, policy model.PlatformPublicationPrecondition, at time.Time) (bool, error) {
	if err := record.validate(); err != nil {
		return false, err
	}
	if record.EndedAt != nil {
		return false, nil
	}
	if at.Before(record.ObservedAt) {
		return false, fmt.Errorf("capture deferral admission time regressed")
	}
	// Never keep an optional-work gate past the absolute deadline, even if the
	// adapter has not yet persisted the terminal observation. Its next CAS must
	// retain the expired tombstone before any later control can be admitted.
	if !at.Before(record.Request.ExpiresAt) {
		return false, nil
	}
	for _, authority := range record.Request.Authorities {
		if authority.Scope == scope {
			if authority.Policy != policy {
				return false, fmt.Errorf("capture deferral policy binding changed")
			}
			return true, nil
		}
	}
	return false, nil
}
