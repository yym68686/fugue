package platformproducer

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

const MaxOfflineRecoveryEvidenceBytes = 64 * 4096

const OfflineRecoveryFact = "offline_edge_recovery"
const OfflineRecoveryMetadata = "producer_offline_recovery"

type OfflineEdgeRecoveryPolicy struct {
	Hostname             string                  `json:"hostname"`
	Path                 string                  `json:"path"`
	ProbeIntervalSeconds int                     `json:"probe_interval_seconds"`
	ProbeTimeoutSeconds  int                     `json:"probe_timeout_seconds"`
	SuccessThreshold     int                     `json:"success_threshold"`
	FailureThreshold     int                     `json:"failure_threshold"`
	FreshnessSeconds     int                     `json:"freshness_seconds"`
	Capacity             *CapacityRecoveryPolicy `json:"capacity,omitempty"`
}

func (p OfflineEdgeRecoveryPolicy) Validate() error {
	u, err := url.ParseRequestURI(p.Path)
	if p.Hostname == "" || len(p.Hostname) > 253 || strings.ToLower(p.Hostname) != p.Hostname || !strings.Contains(p.Hostname, ".") || len(p.Path) > 1024 || !strings.HasPrefix(p.Path, "/") || strings.HasPrefix(p.Path, "//") || err != nil || u.RawQuery != "" || u.Fragment != "" || u.Host != "" {
		return fmt.Errorf("invalid recovery probe target")
	}
	for _, label := range strings.Split(p.Hostname, ".") {
		if !offlineIdentity.MatchString(label) || len(label) > 63 || strings.HasSuffix(label, "-") {
			return fmt.Errorf("invalid recovery probe hostname")
		}
	}
	if p.ProbeIntervalSeconds < 300 || p.ProbeIntervalSeconds > 3600 || p.ProbeTimeoutSeconds < 1 || p.ProbeTimeoutSeconds > 5 || p.SuccessThreshold < 2 || p.SuccessThreshold > 5 || p.FailureThreshold < 1 || p.FailureThreshold > 3 || p.FreshnessSeconds < 600 || p.FreshnessSeconds > 10800 || p.FreshnessSeconds < 2*p.ProbeIntervalSeconds+p.ProbeTimeoutSeconds {
		return fmt.Errorf("recovery probe bounds invalid")
	}
	if p.Capacity != nil && (p.Capacity.Validate() != nil || p.Capacity.MinimumStableSeconds > p.FreshnessSeconds) {
		return fmt.Errorf("capacity recovery policy is invalid or exceeds observation continuity")
	}
	return nil
}

// This is a rebuildable observation. Only a signed candidate can pin it for
// historical membership; this checkpoint is never serving authority.
type OfflineRecoveryObservation struct {
	EdgeID            string                    `json:"edge_id"`
	DeclarationDigest string                    `json:"declaration_digest"`
	LastAttempt       time.Time                 `json:"last_attempt"`
	LastSuccess       time.Time                 `json:"last_success"`
	ProofValidUntil   time.Time                 `json:"proof_valid_until"`
	Successes         int                       `json:"successes"`
	Failures          int                       `json:"failures"`
	Admitted          bool                      `json:"admitted"`
	FirstSuccess      time.Time                 `json:"first_success,omitzero"`
	LastResult        string                    `json:"last_result,omitempty"`
	Capacity          *CapacityRecoveryEvidence `json:"capacity,omitempty"`
}

func OfflineRecoveryKey(hold OfflineEdge) string {
	d, _ := platformconfig.Digest(hold)
	return "edge-offline-recovery/" + d
}
func OfflineRecoveryDigest(hold OfflineEdge) string { d, _ := platformconfig.Digest(hold); return d }

func (o OfflineRecoveryObservation) Ready(hold OfflineEdge, at time.Time) bool {
	p := hold.Recovery
	if p != nil && p.Capacity != nil && (o.Successes != p.SuccessThreshold || o.Failures != 0 || o.LastResult != "capacity_verified" || !o.Capacity.Passes(*p.Capacity, hold.EdgeID) || !o.Capacity.CheckedAt.Equal(o.LastSuccess) || o.FirstSuccess.IsZero() || o.FirstSuccess.After(o.LastSuccess) || o.LastSuccess.Sub(o.FirstSuccess) < time.Duration(p.Capacity.MinimumStableSeconds)*time.Second) {
		return false
	}
	return p != nil && p.Validate() == nil && o.EdgeID == hold.EdgeID && o.DeclarationDigest == OfflineRecoveryDigest(hold) && o.Admitted && o.Successes >= 0 && o.Successes <= p.SuccessThreshold && o.Failures >= 0 && o.Failures < p.FailureThreshold && !at.IsZero() && !o.LastAttempt.IsZero() && !o.LastAttempt.After(at) && !o.LastSuccess.IsZero() && !o.LastSuccess.After(o.LastAttempt) && at.Sub(o.LastSuccess) <= time.Duration(p.FreshnessSeconds)*time.Second && at.Before(o.ProofValidUntil)
}

func (o OfflineRecoveryObservation) Record(hold OfflineEdge, at time.Time, passed bool, validUntil time.Time) OfflineRecoveryObservation {
	return o.RecordCapacity(hold, at, passed, validUntil, nil, "")
}

func (o OfflineRecoveryObservation) RecordCapacity(hold OfflineEdge, at time.Time, passed bool, validUntil time.Time, capacity *CapacityRecoveryEvidence, result string) OfflineRecoveryObservation {
	p := hold.Recovery
	if p == nil {
		return OfflineRecoveryObservation{}
	}
	if p.Capacity != nil {
		// Replaying one outcome cannot manufacture consecutive observations.
		if o.EdgeID == hold.EdgeID && o.DeclarationDigest == OfflineRecoveryDigest(hold) && !at.After(o.LastAttempt) {
			return o
		}
		passed = passed && result == "capacity_verified" && capacity.Passes(*p.Capacity, hold.EdgeID) && capacity.CheckedAt.Equal(at)
		if !sameCapacityCode(o.Capacity, capacity) {
			o.Successes, o.FirstSuccess, o.Admitted = 0, time.Time{}, false
		}
		if passed && o.EdgeID == hold.EdgeID && o.DeclarationDigest == OfflineRecoveryDigest(hold) && at.Sub(o.LastAttempt) < time.Duration(p.ProbeIntervalSeconds)*time.Second {
			return o
		}
	}
	if o.EdgeID != hold.EdgeID || o.DeclarationDigest != OfflineRecoveryDigest(hold) || o.LastAttempt.After(at) || !o.LastAttempt.IsZero() && at.Sub(o.LastAttempt) > time.Duration(p.FreshnessSeconds)*time.Second {
		o = OfflineRecoveryObservation{}
	}
	wasReady := o.Ready(hold, at)
	if o.LastSuccess.IsZero() || o.LastSuccess.After(at) || at.Sub(o.LastSuccess) > time.Duration(p.FreshnessSeconds)*time.Second {
		o.Successes = 0
		o.FirstSuccess = time.Time{}
	}
	o.EdgeID, o.DeclarationDigest = hold.EdgeID, OfflineRecoveryDigest(hold)
	o.LastAttempt = at
	o.LastResult = result
	if passed && validUntil.After(at) {
		if p.Capacity != nil && o.Successes == 0 {
			o.FirstSuccess = at
		}
		o.Successes = min(o.Successes+1, p.SuccessThreshold)
		o.Failures = 0
		o.LastSuccess, o.ProofValidUntil = at, validUntil
		o.Admitted = wasReady || o.Successes >= p.SuccessThreshold
		if p.Capacity != nil {
			copy := *capacity
			o.Capacity = &copy
			o.Admitted = o.Admitted && at.Sub(o.FirstSuccess) >= time.Duration(p.Capacity.MinimumStableSeconds)*time.Second
		}
	} else {
		o.Successes = 0
		o.FirstSuccess = time.Time{}
		o.Failures = min(o.Failures+1, p.FailureThreshold)
		o.Admitted = wasReady && o.Failures < p.FailureThreshold
		if p.Capacity != nil {
			o.Capacity, o.Admitted = nil, false
		}
	}
	return o
}

// EffectiveOfflineHolds always validates every pinned identity, including
// recovered identities, before using fresh runtime evidence.
func EffectiveOfflineHolds(nodes []model.EdgeNode, holds []OfflineEdge, observations []OfflineRecoveryObservation, at time.Time) ([]OfflineEdge, error) {
	if err := ValidateOfflineEdges(holds); err != nil {
		return nil, err
	}
	byID := map[string]model.EdgeNode{}
	for _, n := range nodes {
		if _, ok := byID[n.ID]; ok {
			return nil, fmt.Errorf("ambiguous endpoint inventory")
		}
		byID[n.ID] = n
	}
	ready := map[string]OfflineRecoveryObservation{}
	for _, o := range observations {
		if _, ok := ready[o.EdgeID]; ok {
			return nil, fmt.Errorf("duplicate recovery observation")
		}
		ready[o.EdgeID] = o
	}
	out := []OfflineEdge{}
	for _, h := range holds {
		n, ok := byID[h.EdgeID]
		if !ok || n.EdgeGroupID != h.EdgeGroupID || n.PublicIPv4 != h.PublicIPv4 {
			return nil, fmt.Errorf("offline endpoint identity changed or absent")
		}
		if !ready[h.EdgeID].Ready(h, at) {
			out = append(out, h)
		}
	}
	return out, nil
}

// PublicationOfflineHolds consumes only the exact parent's signed evidence.
// Missing metadata preserves legacy indefinite holds. Never consult today's
// checkpoint or clock when replaying an existing full/LKG publication.
func PublicationOfflineHolds(parent model.PlatformArtifact, holds []OfflineEdge) ([]OfflineEdge, error) {
	raw := parent.Metadata[OfflineRecoveryMetadata]
	if raw == "" {
		return holds, nil
	}
	var evidence struct {
		CapturedAt   time.Time                    `json:"captured_at"`
		Observations []OfflineRecoveryObservation `json:"observations"`
	}
	if len(raw) > MaxOfflineRecoveryEvidenceBytes || json.Unmarshal([]byte(raw), &evidence) != nil || evidence.CapturedAt.IsZero() || evidence.CapturedAt.After(parent.Provenance.SignedAt) || len(evidence.Observations) == 0 || len(evidence.Observations) > len(holds) {
		return nil, fmt.Errorf("invalid pinned recovery evidence")
	}
	known := map[string]OfflineEdge{}
	for _, h := range holds {
		known[h.EdgeID] = h
	}
	recovered := map[string]bool{}
	for _, o := range evidence.Observations {
		h, ok := known[o.EdgeID]
		if !ok || recovered[o.EdgeID] || !o.Ready(h, evidence.CapturedAt) {
			return nil, fmt.Errorf("unbound recovery evidence")
		}
		recovered[o.EdgeID] = true
	}
	out := []OfflineEdge{}
	for _, h := range holds {
		if !recovered[h.EdgeID] {
			out = append(out, h)
		}
	}
	return out, nil
}
