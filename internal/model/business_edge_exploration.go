package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"
)

const BusinessEdgeExplorationVersion = "business-edge-exploration-v1"

type BusinessEdgeExplorationState struct {
	IntentID           string            `json:"intent_id"`
	PolicyDigest       string            `json:"policy_digest"`
	BasisPoints        map[string]int    `json:"basis_points"`
	Stopped            map[string]string `json:"stopped"`
	LastAdjustedAt     time.Time         `json:"last_adjusted_at"`
	LastEvaluatedAt    time.Time         `json:"last_evaluated_at"`
	LastEvidenceBucket time.Time         `json:"last_evidence_bucket"`
	EvidenceValidUntil time.Time         `json:"evidence_valid_until"`
}

type DNSBusinessAllocation struct {
	Policy        BusinessEdgeExplorationPolicy `json:"policy"`
	State         BusinessEdgeExplorationState  `json:"state"`
	ValidUntil    time.Time                     `json:"valid_until"`
	ReceiptDigest string                        `json:"receipt_digest"`
}

func ValidateDNSBusinessAllocation(allocation *DNSBusinessAllocation, capturedAt time.Time) error {
	if allocation == nil || ValidateBusinessEdgeExplorationPolicy(allocation.Policy) != nil {
		return errors.New("invalid signed business allocation policy")
	}
	policy, state := allocation.Policy, allocation.State
	raw, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(raw)
	if state.IntentID != policy.IntentID || state.PolicyDigest != "sha256:"+hex.EncodeToString(digest[:]) ||
		!state.LastEvaluatedAt.Equal(capturedAt) || state.LastAdjustedAt.IsZero() || state.LastAdjustedAt.After(capturedAt) ||
		state.LastEvidenceBucket.After(state.LastAdjustedAt) || allocation.ValidUntil.IsZero() || allocation.ValidUntil.After(policy.ExpiresAt) ||
		allocation.ValidUntil.After(capturedAt.Add(time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second)) ||
		state.EvidenceValidUntil.IsZero() || state.EvidenceValidUntil.After(policy.ExpiresAt) ||
		state.EvidenceValidUntil.After(capturedAt.Add(time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second)) {
		return errors.New("signed business allocation identity or time is inconsistent")
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(allocation.ReceiptDigest, "sha256:"))
	if err != nil || len(decoded) != 32 || len(allocation.ReceiptDigest) != 71 || allocation.ReceiptDigest != strings.ToLower(allocation.ReceiptDigest) || !strings.HasPrefix(allocation.ReceiptDigest, "sha256:") {
		return errors.New("invalid business allocation evidence digest")
	}
	candidates := map[string]bool{}
	for _, edgeID := range policy.CandidateEdgeIDs {
		candidates[edgeID] = true
	}
	for edgeID, reason := range state.Stopped {
		if !candidates[edgeID] || (reason != "candidate_not_route_ready" && reason != "observed_transport_stop_threshold") || state.BasisPoints[edgeID] != 0 {
			return errors.New("invalid signed business stop")
		}
	}
	total := 0
	for edgeID, weight := range state.BasisPoints {
		if !candidates[edgeID] || weight < 0 || weight > 10000 {
			return errors.New("signed business allocation has an undeclared weight")
		}
		total += weight
	}
	nonBaseline := 10000 - state.BasisPoints[policy.BaselineEdgeID]
	if total != 10000 || nonBaseline > policy.MaximumCandidateBasisPoints ||
		policy.Mode == "observe" && nonBaseline > policy.ExplorationBasisPoints || policy.Mode == "paused" && nonBaseline != 0 ||
		nonBaseline > policy.ExplorationBasisPoints && allocation.ValidUntil.After(state.EvidenceValidUntil) {
		return errors.New("signed business allocation exceeds declared budget or evidence lifetime")
	}
	return nil
}

func CloneDNSBusinessAllocation(allocation *DNSBusinessAllocation) *DNSBusinessAllocation {
	if allocation == nil {
		return nil
	}
	cloned := *allocation
	cloned.Policy = CloneBusinessEdgeExplorationPolicy(allocation.Policy)
	cloned.State.BasisPoints = map[string]int{}
	for edgeID, weight := range allocation.State.BasisPoints {
		cloned.State.BasisPoints[edgeID] = weight
	}
	cloned.State.Stopped = map[string]string{}
	for edgeID, reason := range allocation.State.Stopped {
		cloned.State.Stopped[edgeID] = reason
	}
	return &cloned
}

type BusinessDNSPublicationAnchor struct {
	Digest       string `json:"digest"`
	FencingToken int64  `json:"fencing_token"`
}

func CloneBusinessEdgeExplorationPolicy(policy BusinessEdgeExplorationPolicy) BusinessEdgeExplorationPolicy {
	cloned := policy
	cloned.CandidateEdgeIDs = append([]string(nil), policy.CandidateEdgeIDs...)
	if policy.InitialDNSPublications != nil {
		cloned.InitialDNSPublications = make(map[string]BusinessDNSPublicationAnchor, len(policy.InitialDNSPublications))
		for nodeID, anchor := range policy.InitialDNSPublications {
			cloned.InitialDNSPublications[nodeID] = anchor
		}
	}
	return cloned
}

type BusinessEdgeExplorationPolicy struct {
	InitialDNSPublications           map[string]BusinessDNSPublicationAnchor `json:"initial_dns_publications"`
	Version                          string                                  `json:"version"`
	IntentID                         string                                  `json:"intent_id"`
	Mode                             string                                  `json:"mode"`
	BaselineEdgeID                   string                                  `json:"baseline_edge_id"`
	CandidateEdgeIDs                 []string                                `json:"candidate_edge_ids"`
	StartsAt                         time.Time                               `json:"starts_at"`
	ExpiresAt                        time.Time                               `json:"expires_at"`
	ExplorationBasisPoints           int                                     `json:"exploration_basis_points"`
	MaximumCandidateBasisPoints      int                                     `json:"maximum_candidate_basis_points"`
	MaximumStepBasisPoints           int                                     `json:"maximum_step_basis_points"`
	BucketSeconds                    int                                     `json:"bucket_seconds"`
	RequiredBuckets                  int                                     `json:"required_buckets"`
	MinimumConnectionsPerBucket      int                                     `json:"minimum_connections_per_bucket"`
	CooldownSeconds                  int                                     `json:"cooldown_seconds"`
	EvidenceMaxAgeSeconds            int                                     `json:"evidence_max_age_seconds"`
	MinimumCohortCoverageBasisPoints int                                     `json:"minimum_cohort_coverage_basis_points"`
	RTTAdvantageRatio                float64                                 `json:"rtt_advantage_ratio"`
	DeliveryAdvantageRatio           float64                                 `json:"delivery_advantage_ratio"`
	MaximumRTTRegressionRatio        float64                                 `json:"maximum_rtt_regression_ratio"`
	MaximumDeliveryRegressionRatio   float64                                 `json:"maximum_delivery_regression_ratio"`
	MaximumRetransmissionIncrease    float64                                 `json:"maximum_retransmission_increase"`
	StopRTTMS                        float64                                 `json:"stop_rtt_ms"`
	StopRetransmissionRate           float64                                 `json:"stop_retransmission_rate"`
}

func (policy *BusinessEdgeExplorationPolicy) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 24 {
		return errors.New("business exploration requires complete explicit intent")
	}
	for _, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return errors.New("business exploration fields cannot be null")
		}
	}
	type plain BusinessEdgeExplorationPolicy
	var decoded plain
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	if err := ValidateBusinessEdgeExplorationPolicy(BusinessEdgeExplorationPolicy(decoded)); err != nil {
		return err
	}
	*policy = BusinessEdgeExplorationPolicy(decoded)
	return nil
}

func ValidateBusinessEdgeExplorationPolicy(policy BusinessEdgeExplorationPolicy) error {
	validIdentity := func(value string) bool {
		return value != "" && len(value) <= 128 && !strings.ContainsAny(value, " \t\r\n\x00")
	}
	if len(policy.InitialDNSPublications) < 1 || len(policy.InitialDNSPublications) > 32 {
		return errors.New("business exploration requires bounded initial DNS publication bindings")
	}
	for nodeID, anchor := range policy.InitialDNSPublications {
		initialDigest, err := hex.DecodeString(strings.TrimPrefix(anchor.Digest, "sha256:"))
		if !validIdentity(nodeID) || err != nil || len(initialDigest) != 32 || len(anchor.Digest) != 71 || !strings.HasPrefix(anchor.Digest, "sha256:") || anchor.Digest != strings.ToLower(anchor.Digest) || anchor.FencingToken <= 0 {
			return errors.New("business exploration requires exact initial DNS publication binding for each DNS node")
		}
	}
	if policy.Version != BusinessEdgeExplorationVersion || !validIdentity(policy.IntentID) ||
		(policy.Mode != "observe" && policy.Mode != "adaptive" && policy.Mode != "paused") ||
		!validIdentity(policy.BaselineEdgeID) || len(policy.CandidateEdgeIDs) < 2 || len(policy.CandidateEdgeIDs) > 32 ||
		policy.StartsAt.IsZero() || !policy.ExpiresAt.After(policy.StartsAt) || policy.ExpiresAt.Sub(policy.StartsAt) > 7*24*time.Hour {
		return errors.New("invalid business exploration identity, mode or lease")
	}
	seen := map[string]bool{}
	for _, edgeID := range policy.CandidateEdgeIDs {
		if !validIdentity(edgeID) || seen[edgeID] {
			return errors.New("invalid or duplicate business exploration candidate")
		}
		seen[edgeID] = true
	}
	if !seen[policy.BaselineEdgeID] || policy.ExplorationBasisPoints < len(seen)-1 || policy.ExplorationBasisPoints > 1000 ||
		policy.MaximumCandidateBasisPoints < policy.ExplorationBasisPoints || policy.MaximumCandidateBasisPoints > 9000 ||
		policy.MaximumStepBasisPoints < 1 || policy.MaximumStepBasisPoints > 1000 ||
		policy.BucketSeconds < 60 || policy.BucketSeconds > 3600 || policy.RequiredBuckets < 2 || policy.RequiredBuckets > 12 ||
		policy.MinimumConnectionsPerBucket < 3 || policy.MinimumConnectionsPerBucket > 10000 ||
		policy.CooldownSeconds < 60 || policy.CooldownSeconds > 86400 || policy.EvidenceMaxAgeSeconds < policy.BucketSeconds || policy.EvidenceMaxAgeSeconds > 86400 ||
		policy.MinimumCohortCoverageBasisPoints < 1 || policy.MinimumCohortCoverageBasisPoints > 10000 {
		return errors.New("business exploration budget or evidence bounds are invalid")
	}
	for _, value := range []float64{policy.RTTAdvantageRatio, policy.DeliveryAdvantageRatio, policy.MaximumRTTRegressionRatio,
		policy.MaximumDeliveryRegressionRatio, policy.MaximumRetransmissionIncrease, policy.StopRetransmissionRate} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
			return errors.New("business exploration ratios must be finite and bounded")
		}
	}
	if policy.RTTAdvantageRatio == 0 || policy.DeliveryAdvantageRatio == 0 || policy.StopRetransmissionRate == 0 ||
		math.IsNaN(policy.StopRTTMS) || math.IsInf(policy.StopRTTMS, 0) || policy.StopRTTMS <= 0 || policy.StopRTTMS > 60000 {
		return errors.New("business exploration requires explicit advantage and stop thresholds")
	}
	return nil
}
