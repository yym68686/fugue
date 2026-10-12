package edgequality

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

type BusinessExplorationState = model.BusinessEdgeExplorationState

type BusinessExplorationInput struct {
	Policy         model.BusinessEdgeExplorationPolicy `json:"policy"`
	CapturedAt     time.Time                           `json:"captured_at"`
	Hostname       string                              `json:"hostname"`
	PathPrefix     string                              `json:"path_prefix"`
	TrafficClass   string                              `json:"traffic_class"`
	Candidates     []Candidate                         `json:"candidates"`
	NetworkSamples []model.EdgeNetworkSample           `json:"network_samples"`
	Previous       *BusinessExplorationState           `json:"previous,omitempty"`
}

type BusinessExplorationWindow struct {
	EdgeID                  string    `json:"edge_id"`
	Cohort                  string    `json:"cohort"`
	StartedAt               time.Time `json:"started_at"`
	LastObservedAt          time.Time `json:"last_observed_at"`
	Connections             int       `json:"connections"`
	AcknowledgedBytesMedian float64   `json:"acknowledged_bytes_median"`
	BusyMicrosecondsMedian  float64   `json:"busy_microseconds_median"`
	RTTMedianMS             float64   `json:"rtt_median_ms"`
	RTTP90MS                float64   `json:"rtt_p90_ms"`
	DeliveryBPS             float64   `json:"delivery_median_bps"`
	DeliveryP10             float64   `json:"delivery_p10_bps"`
	RetransmitP90           float64   `json:"retransmission_p90"`
}

type BusinessExplorationCoverage struct {
	EdgeID                   string         `json:"edge_id"`
	Records                  int            `json:"records"`
	Connections              int            `json:"connections"`
	ConnectionWindows        int            `json:"connection_windows"`
	RTTRecords               int            `json:"rtt_records"`
	AcknowledgedDeltaRecords int            `json:"acknowledged_delta_records"`
	DeliveryRecords          int            `json:"delivery_records"`
	RetransmissionRecords    int            `json:"retransmission_records"`
	LastSampleAt             *time.Time     `json:"last_sample_at,omitempty"`
	Reasons                  map[string]int `json:"reasons"`
}

type BusinessExplorationComparison struct {
	EdgeID              string   `json:"edge_id"`
	Cohorts             []string `json:"cohorts"`
	CoverageBasisPoints int      `json:"coverage_basis_points"`
	CompleteBuckets     int      `json:"complete_buckets"`
	Verdict             string   `json:"verdict"`
	Reasons             []string `json:"reasons"`
}

type BusinessExplorationDecision struct {
	State            BusinessExplorationState        `json:"state"`
	Action           string                          `json:"action"`
	Reasons          []string                        `json:"reasons"`
	Windows          []BusinessExplorationWindow     `json:"windows"`
	Coverage         []BusinessExplorationCoverage   `json:"coverage"`
	Comparisons      []BusinessExplorationComparison `json:"comparisons"`
	Rejected         map[string]int                  `json:"rejected"`
	UsedSampleIDs    []string                        `json:"used_sample_ids"`
	ValidUntil       time.Time                       `json:"valid_until"`
	ServingAuthority bool                            `json:"serving_authority"`
}

type BusinessExplorationReceipt struct {
	Input    BusinessExplorationInput    `json:"input"`
	Decision BusinessExplorationDecision `json:"decision"`
	Digest   string                      `json:"digest"`
}

func BusinessSamplesFromBoundObservations(snapshot Snapshot, candidateIDs []string) []model.EdgeNetworkSample {
	bound := map[string]string{}
	for _, observation := range snapshot.Observations {
		if observation.ClientSource == "public_tcp_info" {
			bound[observation.ID] = observation.EdgeID
		}
	}
	samples := []model.EdgeNetworkSample{}
	for _, sample := range snapshot.NetworkSamples {
		observationID := "network:" + sample.EdgeID + ":" + sample.ID
		if sample.Source == "public_front_tcp_info_v1" && sample.EdgeID != "" && bound[observationID] == sample.EdgeID && slices.Contains(candidateIDs, sample.EdgeID) {
			samples = append(samples, sample)
		}
	}
	return samples
}

func CaptureBusinessExploration(input BusinessExplorationInput) (BusinessExplorationReceipt, error) {
	if input.NetworkSamples == nil {
		input.NetworkSamples = []model.EdgeNetworkSample{}
	}
	decision, err := EvaluateBusinessExploration(input)
	if err != nil {
		return BusinessExplorationReceipt{}, err
	}
	receipt := BusinessExplorationReceipt{Input: input, Decision: decision}
	receipt.Digest, err = businessDigest(receipt)
	return receipt, err
}

func ReplayBusinessExploration(receipt BusinessExplorationReceipt) (BusinessExplorationDecision, error) {
	expected := receipt.Digest
	receipt.Digest = ""
	digest, err := businessDigest(receipt)
	if err != nil || digest != expected {
		return BusinessExplorationDecision{}, errors.New("business exploration receipt digest mismatch")
	}
	decision, err := EvaluateBusinessExploration(receipt.Input)
	if err != nil || !reflect.DeepEqual(decision, receipt.Decision) {
		return BusinessExplorationDecision{}, errors.New("business exploration decision replay mismatch")
	}
	return decision, nil
}

func businessDigest(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func EvaluateBusinessExploration(input BusinessExplorationInput) (BusinessExplorationDecision, error) {
	policy := input.Policy
	if err := model.ValidateBusinessEdgeExplorationPolicy(policy); err != nil {
		return BusinessExplorationDecision{}, err
	}
	if input.CapturedAt.IsZero() || input.Hostname == "" || len(input.Hostname) > 253 || !strings.HasPrefix(input.PathPrefix, "/") ||
		len(input.PathPrefix) > 2048 || (input.TrafficClass != "dynamic_api" && input.TrafficClass != "streaming") ||
		len(input.NetworkSamples) > 32768 || len(input.Candidates) != len(policy.CandidateEdgeIDs) {
		return BusinessExplorationDecision{}, errors.New("invalid business exploration context or input bounds")
	}
	policyDigest, err := businessDigest(policy)
	if err != nil {
		return BusinessExplorationDecision{}, err
	}
	state := BusinessExplorationState{IntentID: policy.IntentID, PolicyDigest: policyDigest, BasisPoints: map[string]int{}, Stopped: map[string]string{}, LastAdjustedAt: input.CapturedAt, LastEvaluatedAt: input.CapturedAt,
		EvidenceValidUntil: minBusinessTime(policy.ExpiresAt, input.CapturedAt.Add(time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second))}
	if previous := input.Previous; previous != nil {
		if err := validateBusinessPrevious(policy, policyDigest, input.CapturedAt, *previous); err != nil {
			return BusinessExplorationDecision{}, err
		}
		state.LastAdjustedAt = previous.LastAdjustedAt
		state.LastEvidenceBucket = previous.LastEvidenceBucket
		state.EvidenceValidUntil = previous.EvidenceValidUntil
		for edgeID, weight := range previous.BasisPoints {
			state.BasisPoints[edgeID] = weight
		}
		for edgeID, reason := range previous.Stopped {
			state.Stopped[edgeID] = reason
		}
	}
	decision := BusinessExplorationDecision{State: state, Action: "hold", Reasons: []string{}, Windows: []BusinessExplorationWindow{},
		Coverage: []BusinessExplorationCoverage{}, Comparisons: []BusinessExplorationComparison{}, Rejected: map[string]int{}, UsedSampleIDs: []string{},
		ValidUntil: minBusinessTime(policy.ExpiresAt, input.CapturedAt.Add(time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second))}
	candidates := map[string]Candidate{}
	for _, candidate := range input.Candidates {
		if !slices.Contains(policy.CandidateEdgeIDs, candidate.EdgeID) || candidates[candidate.EdgeID].EdgeID != "" {
			return decision, errors.New("business exploration candidate set differs from intent")
		}
		candidates[candidate.EdgeID] = candidate
	}
	coverage, windows, used, rejected, err := collectBusinessWindows(input, candidates)
	if err != nil {
		return decision, err
	}
	decision.Coverage, decision.Windows, decision.UsedSampleIDs, decision.Rejected = coverage, windows, used, rejected
	for _, edgeID := range policy.CandidateEdgeIDs {
		candidate := candidates[edgeID]
		if !businessCandidateReady(candidate, input.CapturedAt, policy.EvidenceMaxAgeSeconds) {
			decision.State.Stopped[edgeID] = "candidate_not_route_ready"
		}
	}
	for _, window := range windows {
		if window.Connections >= policy.MinimumConnectionsPerBucket && (window.RTTP90MS > policy.StopRTTMS || window.RetransmitP90 > policy.StopRetransmissionRate) {
			if window.EdgeID != policy.BaselineEdgeID {
				decision.State.Stopped[window.EdgeID] = "observed_transport_stop_threshold"
			}
		}
	}
	baselineStopped := decision.State.Stopped[policy.BaselineEdgeID] != ""
	if baselineStopped {
		decision.State.BasisPoints = map[string]int{}
		decision.Action, decision.Reasons = "withhold", []string{"baseline_not_route_ready_use_independent_verified_failover"}
		return decision, nil
	}
	if policy.Mode == "paused" || input.CapturedAt.Before(policy.StartsAt) || !input.CapturedAt.Before(policy.ExpiresAt) {
		decision.State.BasisPoints = map[string]int{policy.BaselineEdgeID: 10000}
		decision.Action, decision.Reasons = "baseline_only", []string{"intent_paused_or_outside_lease"}
		return decision, nil
	}
	floors := businessInitialAllocation(policy, decision.State.Stopped)
	if input.Previous == nil || input.Previous.LastEvaluatedAt.Before(policy.StartsAt) {
		decision.State.BasisPoints = floors
		decision.State.LastAdjustedAt = input.CapturedAt
		decision.Action = "initialize"
	} else {
		for edgeID := range decision.State.Stopped {
			decision.State.BasisPoints[policy.BaselineEdgeID] += decision.State.BasisPoints[edgeID]
			delete(decision.State.BasisPoints, edgeID)
		}
		if !reflect.DeepEqual(input.Previous.BasisPoints, decision.State.BasisPoints) {
			decision.State.LastAdjustedAt = input.CapturedAt
			decision.Action = "stop_candidates"
		}
	}
	ids := append([]string(nil), policy.CandidateEdgeIDs...)
	sort.Strings(ids)
	for _, edgeID := range ids {
		if edgeID == policy.BaselineEdgeID || decision.State.Stopped[edgeID] != "" {
			continue
		}
		decision.Comparisons = append(decision.Comparisons, compareBusinessWindows(input, windows, coverage, edgeID))
	}
	if policy.Mode == "observe" {
		decision.State.BasisPoints = floors
		decision.Reasons = append(decision.Reasons, "fixed_exploration_only")
		return decision, nil
	}
	activeAdaptive, freshAdaptive := false, true
	adaptiveExpiry := policy.ExpiresAt
	for _, comparison := range decision.Comparisons {
		if decision.State.BasisPoints[comparison.EdgeID] <= floors[comparison.EdgeID] {
			continue
		}
		activeAdaptive = true
		if comparison.Verdict == "insufficient" {
			freshAdaptive = false
		} else {
			adaptiveExpiry = minBusinessTime(adaptiveExpiry, businessComparisonExpiry(input, comparison, windows))
		}
	}
	if activeAdaptive {
		if freshAdaptive {
			decision.State.EvidenceValidUntil = adaptiveExpiry
		}
		if !input.CapturedAt.Before(decision.State.EvidenceValidUntil) {
			decision.State.BasisPoints = floors
			decision.State.LastAdjustedAt = input.CapturedAt
			decision.Action = "evidence_expired"
			decision.Reasons = append(decision.Reasons, "adaptive_evidence_expired_return_to_fixed_exploration")
			return decision, nil
		}
		decision.ValidUntil = minBusinessTime(decision.ValidUntil, decision.State.EvidenceValidUntil)
	}
	if input.Previous == nil || input.CapturedAt.Sub(decision.State.LastAdjustedAt) < time.Duration(policy.CooldownSeconds)*time.Second {
		decision.Reasons = append(decision.Reasons, "cooldown")
		return decision, nil
	}
	latestBucket := time.Unix(input.CapturedAt.Unix()/int64(policy.BucketSeconds)*int64(policy.BucketSeconds)-int64(policy.BucketSeconds), 0).UTC()
	if !latestBucket.After(decision.State.LastEvidenceBucket) {
		decision.Reasons = append(decision.Reasons, "evidence_bucket_already_consumed")
		return decision, nil
	}
	for _, comparison := range decision.Comparisons {
		if comparison.Verdict != "worse" {
			continue
		}
		amount := min(policy.MaximumStepBasisPoints, decision.State.BasisPoints[comparison.EdgeID]-floors[comparison.EdgeID])
		if amount > 0 {
			decision.State.BasisPoints[comparison.EdgeID] -= amount
			decision.State.BasisPoints[policy.BaselineEdgeID] += amount
			decision.State.LastAdjustedAt, decision.Action = input.CapturedAt, "decrease"
			decision.State.LastEvidenceBucket = latestBucket
			decision.State.EvidenceValidUntil = minBusinessTime(adaptiveExpiry, businessComparisonExpiry(input, comparison, windows))
			decision.ValidUntil = minBusinessTime(decision.ValidUntil, decision.State.EvidenceValidUntil)
			return decision, nil
		}
	}
	for _, comparison := range decision.Comparisons {
		if comparison.Verdict != "better" {
			continue
		}
		remaining := policy.MaximumCandidateBasisPoints - (10000 - decision.State.BasisPoints[policy.BaselineEdgeID])
		amount := min(policy.MaximumStepBasisPoints, remaining)
		if amount > 0 {
			decision.State.BasisPoints[comparison.EdgeID] += amount
			decision.State.BasisPoints[policy.BaselineEdgeID] -= amount
			decision.State.LastAdjustedAt, decision.Action = input.CapturedAt, "increase"
			decision.State.LastEvidenceBucket = latestBucket
			decision.State.EvidenceValidUntil = minBusinessTime(adaptiveExpiry, businessComparisonExpiry(input, comparison, windows))
			decision.ValidUntil = minBusinessTime(decision.ValidUntil, decision.State.EvidenceValidUntil)
			return decision, nil
		}
	}
	decision.Reasons = append(decision.Reasons, "no_qualified_step")
	return decision, nil
}

func validateBusinessPrevious(policy model.BusinessEdgeExplorationPolicy, digest string, now time.Time, state BusinessExplorationState) error {
	if state.IntentID != policy.IntentID || state.PolicyDigest != digest || state.LastAdjustedAt.IsZero() || state.LastEvaluatedAt.IsZero() ||
		state.LastAdjustedAt.After(state.LastEvaluatedAt) || state.LastEvaluatedAt.After(now) || state.LastEvidenceBucket.After(state.LastAdjustedAt) ||
		state.EvidenceValidUntil.IsZero() || state.EvidenceValidUntil.After(policy.ExpiresAt) || state.EvidenceValidUntil.After(state.LastEvaluatedAt.Add(time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second)) ||
		len(state.BasisPoints) > len(policy.CandidateEdgeIDs) || len(state.Stopped) > len(policy.CandidateEdgeIDs) {
		return errors.New("business exploration predecessor is not bound to the same intent and time")
	}
	total := 0
	for edgeID, weight := range state.BasisPoints {
		if !slices.Contains(policy.CandidateEdgeIDs, edgeID) || weight < 0 || weight > 10000 {
			return errors.New("invalid previous business allocation")
		}
		total += weight
	}
	for edgeID, reason := range state.Stopped {
		if !slices.Contains(policy.CandidateEdgeIDs, edgeID) || (reason != "candidate_not_route_ready" && reason != "observed_transport_stop_threshold") || state.BasisPoints[edgeID] != 0 {
			return errors.New("invalid previous sticky exploration stop")
		}
	}
	if policy.Mode == "observe" && total == 10000 && 10000-state.BasisPoints[policy.BaselineEdgeID] > policy.ExplorationBasisPoints {
		return errors.New("observe predecessor exceeds fixed exploration budget")
	}
	if !state.LastEvaluatedAt.Before(policy.StartsAt) && state.LastEvaluatedAt.Before(policy.ExpiresAt) && policy.Mode != "paused" && total == 10000 {
		for edgeID, floor := range businessInitialAllocation(policy, state.Stopped) {
			if edgeID != policy.BaselineEdgeID && state.BasisPoints[edgeID] < floor {
				return errors.New("previous business allocation starves an unstopped candidate")
			}
		}
	}
	if total != 10000 && !(total == 0 && state.Stopped[policy.BaselineEdgeID] != "") ||
		total == 10000 && 10000-state.BasisPoints[policy.BaselineEdgeID] > policy.MaximumCandidateBasisPoints {
		return errors.New("previous business allocation exceeds explicit budget")
	}
	return nil
}

func businessInitialAllocation(policy model.BusinessEdgeExplorationPolicy, stopped map[string]string) map[string]int {
	weights := map[string]int{policy.BaselineEdgeID: 10000}
	ids := append([]string(nil), policy.CandidateEdgeIDs...)
	sort.Strings(ids)
	index := 0
	for _, edgeID := range ids {
		if edgeID == policy.BaselineEdgeID {
			continue
		}
		weight := policy.ExplorationBasisPoints / (len(ids) - 1)
		if index < policy.ExplorationBasisPoints%(len(ids)-1) {
			weight++
		}
		index++
		if stopped[edgeID] == "" {
			weights[edgeID] = weight
			weights[policy.BaselineEdgeID] -= weight
		}
	}
	return weights
}

func businessCandidateReady(candidate Candidate, now time.Time, ageSeconds int) bool {
	return candidate.RouteProofVerified && candidate.RouteGeneration != "" && candidate.ProofObservedAt != nil &&
		!candidate.ProofObservedAt.After(now) && now.Sub(*candidate.ProofObservedAt) <= time.Duration(ageSeconds)*time.Second && len(candidate.HardGates) == 0
}

type businessConnection struct {
	sample  model.EdgeNetworkSample
	rtt     float64
	rate    float64
	retrans float64
}

type businessWindowKey struct {
	edgeID string
	cohort string
	start  int64
}

func collectBusinessWindows(input BusinessExplorationInput, candidates map[string]Candidate) ([]BusinessExplorationCoverage, []BusinessExplorationWindow, []string, map[string]int, error) {
	policy := input.Policy
	end := input.CapturedAt.Unix() / int64(policy.BucketSeconds) * int64(policy.BucketSeconds)
	start := end - int64(policy.BucketSeconds*policy.RequiredBuckets)
	grouped := map[businessWindowKey]map[string]businessConnection{}
	coverage := map[string]*BusinessExplorationCoverage{}
	connections := map[string]map[string]bool{}
	connectionWindows := map[string]map[string]bool{}
	rejected := map[string]int{}
	seen := map[string]bool{}
	for edgeID := range candidates {
		coverage[edgeID] = &BusinessExplorationCoverage{EdgeID: edgeID, Reasons: map[string]int{}}
		connections[edgeID] = map[string]bool{}
		connectionWindows[edgeID] = map[string]bool{}
	}
	for _, sample := range input.NetworkSamples {
		if seen[sample.ID] {
			return nil, nil, nil, nil, errors.New("duplicate business evidence identity")
		}
		seen[sample.ID] = true
		entry := coverage[sample.EdgeID]
		reason := ""
		candidate, exists := candidates[sample.EdgeID]
		switch {
		case !exists:
			reason = "outside_candidate_intent"
		case model.ValidateEdgeNetworkSample(sample) != nil:
			reason = "invalid_sample"
		case sample.Source != "public_front_tcp_info_v1":
			reason = "not_passive_business_tcp"
		case sample.Hostname != input.Hostname || sample.PathPrefix != input.PathPrefix || sample.TrafficClass != input.TrafficClass:
			reason = "context_mismatch"
		case sample.RouteDigest != candidate.RouteGeneration || sample.EdgeGroupID != candidate.EdgeGroupID:
			reason = "route_identity_mismatch"
		case sample.ObservedAt.After(input.CapturedAt):
			reason = "future_sample"
		case sample.ObservedAt.Unix() < start:
			reason = "outside_window"
		case sample.ObservedAt.Unix() >= end:
			reason = "incomplete_bucket"
		}
		if reason == "" {
			entry.Records++
			if entry.LastSampleAt == nil || sample.ObservedAt.After(*entry.LastSampleAt) {
				observed := sample.ObservedAt
				entry.LastSampleAt = &observed
			}
			client := sample.ClientNetwork
			connectionID := fmt.Sprintf("%s\x00%s\x00%s", client.ConnectionID, client.Slot, client.StartedAt.UTC().Format(time.RFC3339Nano))
			if client.Backend != nil {
				connectionID += "\x00" + client.Backend.PodUID
			}
			connections[sample.EdgeID][connectionID] = true
			bucketStart := sample.ObservedAt.Unix() / int64(policy.BucketSeconds) * int64(policy.BucketSeconds)
			connectionWindows[sample.EdgeID][fmt.Sprintf("%d\x00%s", bucketStart, connectionID)] = true
			rate, retrans := model.EdgeClientDeliveryMetrics(client)
			if client.RTTMS != nil {
				entry.RTTRecords++
			}
			if client.Delivery != nil && client.DeliveryBaseline != nil {
				entry.AcknowledgedDeltaRecords++
				if client.Delivery.ApplicationLimited {
					entry.Reasons["delivery_application_limited"]++
				}
			} else {
				entry.Reasons["delivery_pair_missing"]++
			}
			if rate != nil {
				entry.DeliveryRecords++
			} else {
				entry.Reasons["delivery_rate_unqualified"]++
			}
			if retrans != nil {
				entry.RetransmissionRecords++
			} else {
				entry.Reasons["retransmission_ratio_unqualified"]++
			}
			if client.RTTMS == nil || rate == nil || retrans == nil {
				reason = "insufficient_transport_metrics"
			} else {
				key := businessWindowKey{edgeID: sample.EdgeID, cohort: client.Scope, start: sample.ObservedAt.Unix() / int64(policy.BucketSeconds) * int64(policy.BucketSeconds)}
				if client.DeliveryBaseline.ObservedAt.Unix() < key.start {
					reason = "delivery_crosses_bucket"
				} else {
					if grouped[key] == nil {
						grouped[key] = map[string]businessConnection{}
					}
					previous, present := grouped[key][connectionID]
					if !present || sample.ObservedAt.After(previous.sample.ObservedAt) || sample.ObservedAt.Equal(previous.sample.ObservedAt) && sample.ID < previous.sample.ID {
						grouped[key][connectionID] = businessConnection{sample: sample, rtt: *client.RTTMS, rate: *rate, retrans: *retrans}
					}
				}
			}
		}
		if reason != "" {
			rejected[reason]++
			if entry != nil {
				entry.Reasons[reason]++
			}
		}
	}
	windows := []BusinessExplorationWindow{}
	used := []string{}
	for key, records := range grouped {
		rtts, rates, retransmits := []float64{}, []float64{}, []float64{}
		acknowledgedBytes, busyMicroseconds := []float64{}, []float64{}
		latest := time.Time{}
		for _, record := range records {
			client := record.sample.ClientNetwork
			acknowledgedBytes = append(acknowledgedBytes, float64(client.Delivery.BytesAcked-client.DeliveryBaseline.BytesAcked))
			busyMicroseconds = append(busyMicroseconds, float64(client.Delivery.BusyMicroseconds-client.DeliveryBaseline.BusyMicroseconds))
			rtts, rates, retransmits = append(rtts, record.rtt), append(rates, record.rate), append(retransmits, record.retrans)
			used = append(used, record.sample.ID)
			if record.sample.ObservedAt.After(latest) {
				latest = record.sample.ObservedAt
			}
		}
		sort.Float64s(rtts)
		sort.Float64s(rates)
		sort.Float64s(retransmits)
		sort.Float64s(acknowledgedBytes)
		sort.Float64s(busyMicroseconds)
		windows = append(windows, BusinessExplorationWindow{EdgeID: key.edgeID, Cohort: key.cohort, StartedAt: time.Unix(key.start, 0).UTC(), LastObservedAt: latest, Connections: len(records),
			AcknowledgedBytesMedian: networkQuantile(acknowledgedBytes, 0.5), BusyMicrosecondsMedian: networkQuantile(busyMicroseconds, 0.5),
			RTTMedianMS: networkQuantile(rtts, 0.5), RTTP90MS: networkQuantile(rtts, 0.9), DeliveryBPS: networkQuantile(rates, 0.5), DeliveryP10: networkQuantile(rates, 0.1), RetransmitP90: networkQuantile(retransmits, 0.9)})
	}
	sort.Slice(windows, func(left, right int) bool {
		first, second := windows[left], windows[right]
		if first.EdgeID != second.EdgeID {
			return first.EdgeID < second.EdgeID
		}
		if first.Cohort != second.Cohort {
			return first.Cohort < second.Cohort
		}
		return first.StartedAt.Before(second.StartedAt)
	})
	sort.Strings(used)
	entries := []BusinessExplorationCoverage{}
	for edgeID, entry := range coverage {
		entry.Connections = len(connections[edgeID])
		entry.ConnectionWindows = len(connectionWindows[edgeID])
		if entry.Records == 0 {
			entry.Reasons["no_usable_business_samples"]++
		}
		entries = append(entries, *entry)
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].EdgeID < entries[right].EdgeID })
	return entries, windows, used, rejected, nil
}

func compareBusinessWindows(input BusinessExplorationInput, windows []BusinessExplorationWindow, coverage []BusinessExplorationCoverage, edgeID string) BusinessExplorationComparison {
	policy := input.Policy
	comparison := BusinessExplorationComparison{EdgeID: edgeID, Cohorts: []string{}, Verdict: "insufficient", Reasons: []string{}}
	end := input.CapturedAt.Unix() / int64(policy.BucketSeconds) * int64(policy.BucketSeconds)
	byKey := map[businessWindowKey]BusinessExplorationWindow{}
	cohorts := map[string]bool{}
	total, matched := map[string]int{}, map[string]int{}
	for _, entry := range coverage {
		total[entry.EdgeID] = entry.ConnectionWindows
	}
	for _, window := range windows {
		if window.EdgeID == policy.BaselineEdgeID || window.EdgeID == edgeID {
			byKey[businessWindowKey{edgeID: window.EdgeID, cohort: window.Cohort, start: window.StartedAt.Unix()}] = window
			cohorts[window.Cohort] = true
		}
	}
	allBetter, allWorse := true, true
	for cohort := range cohorts {
		complete, better, worse := 0, true, true
		cohortCounts := map[string]int{}
		for bucket := 1; bucket <= policy.RequiredBuckets; bucket++ {
			start := end - int64(bucket*policy.BucketSeconds)
			baseline := byKey[businessWindowKey{edgeID: policy.BaselineEdgeID, cohort: cohort, start: start}]
			candidate := byKey[businessWindowKey{edgeID: edgeID, cohort: cohort, start: start}]
			if baseline.Connections < policy.MinimumConnectionsPerBucket || candidate.Connections < policy.MinimumConnectionsPerBucket {
				continue
			}
			if bucket == 1 && (input.CapturedAt.Sub(baseline.LastObservedAt) > time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second || input.CapturedAt.Sub(candidate.LastObservedAt) > time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second) {
				continue
			}
			complete++
			cohortCounts[policy.BaselineEdgeID] += baseline.Connections
			cohortCounts[edgeID] += candidate.Connections
			better = better && businessWindowBetter(candidate, baseline, policy)
			worse = worse && businessWindowBetter(baseline, candidate, policy)
		}
		if complete == policy.RequiredBuckets {
			comparison.Cohorts = append(comparison.Cohorts, cohort)
			for key, count := range cohortCounts {
				matched[key] += count
			}
			allBetter, allWorse = allBetter && better, allWorse && worse
		}
	}
	sort.Strings(comparison.Cohorts)
	if len(comparison.Cohorts) == 0 {
		comparison.Reasons = append(comparison.Reasons, "no_complete_shared_cohort_windows")
		return comparison
	}
	comparison.CompleteBuckets = policy.RequiredBuckets
	comparison.CoverageBasisPoints = min(matched[edgeID]*10000/total[edgeID], matched[policy.BaselineEdgeID]*10000/total[policy.BaselineEdgeID])
	if comparison.CoverageBasisPoints < policy.MinimumCohortCoverageBasisPoints {
		comparison.Reasons = append(comparison.Reasons, "observed_cohort_coverage_below_policy")
		return comparison
	}
	comparison.Verdict = "inconclusive"
	if allBetter {
		comparison.Verdict = "better"
	} else if allWorse {
		comparison.Verdict = "worse"
	} else {
		comparison.Reasons = append(comparison.Reasons, "conflicting_or_insufficient_advantage")
	}
	return comparison
}

func businessWindowBetter(candidate, baseline BusinessExplorationWindow, policy model.BusinessEdgeExplorationPolicy) bool {
	rttGuard := candidate.RTTP90MS <= baseline.RTTP90MS*(1+policy.MaximumRTTRegressionRatio)
	deliveryGuard := candidate.DeliveryP10 >= baseline.DeliveryP10*(1-policy.MaximumDeliveryRegressionRatio)
	retransGuard := candidate.RetransmitP90 <= baseline.RetransmitP90+policy.MaximumRetransmissionIncrease
	rttAdvantage := baseline.RTTMedianMS > 0 && candidate.RTTMedianMS <= baseline.RTTMedianMS*(1-policy.RTTAdvantageRatio)
	deliveryAdvantage := candidate.DeliveryBPS >= baseline.DeliveryBPS*(1+policy.DeliveryAdvantageRatio)
	return rttGuard && deliveryGuard && retransGuard && (rttAdvantage || deliveryAdvantage) && !math.IsNaN(candidate.DeliveryBPS)
}

func businessComparisonExpiry(input BusinessExplorationInput, comparison BusinessExplorationComparison, windows []BusinessExplorationWindow) time.Time {
	latestStart := input.CapturedAt.Unix()/int64(input.Policy.BucketSeconds)*int64(input.Policy.BucketSeconds) - int64(input.Policy.BucketSeconds)
	expires := input.Policy.ExpiresAt
	for _, window := range windows {
		if (window.EdgeID == comparison.EdgeID || window.EdgeID == input.Policy.BaselineEdgeID) && window.StartedAt.Unix() == latestStart && slices.Contains(comparison.Cohorts, window.Cohort) {
			expires = minBusinessTime(expires, window.LastObservedAt.Add(time.Duration(input.Policy.EvidenceMaxAgeSeconds)*time.Second))
		}
	}
	return expires
}

func minBusinessTime(first, second time.Time) time.Time {
	if first.Before(second) {
		return first
	}
	return second
}
