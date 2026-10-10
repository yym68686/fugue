package edgequality

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const Schema = "fugue.physical-edge-quality-shadow/v1"
const MaxObservations = 4096
const ReceiptDigestFormat = "embedded-json-sorted-v1"

type Policy = model.PhysicalEdgeQualityPolicy

func DefaultShadowPolicy() Policy {
	return Policy{Version: "network-only-experiment-v1", WindowSeconds: 1800, BucketSeconds: 300,
		RequiredBuckets: 3, MinimumRecords: 3, CooldownSeconds: 900, EvidenceMaxAgeSeconds: 600, AdvantageMS: 20, AdvantageRatio: 0.15,
		UnknownCostMS: 100, UncertaintyMS: 100, FailureCostMS: 1000, CapacityCostMS: 200,
		ThroughputCostMS: 100, ThroughputTargetBPS: 1 << 20, ProbeIntervalSeconds: 300, ProbeBudgetPerInterval: 1}
}

type Candidate struct {
	EdgeID             string     `json:"edge_id"`
	EdgeGroupID        string     `json:"edge_group_id"`
	RouteGeneration    string     `json:"route_generation"`
	RouteProofVerified bool       `json:"route_proof_verified"`
	ProofObservedAt    *time.Time `json:"proof_observed_at"`
	HardGates          []string   `json:"hard_gates"`
}

type Observation struct {
	ClientProbeRoundID       string             `json:"client_probe_round_id,omitempty"`
	ClientRetransmissionRate *float64           `json:"client_retransmission_rate,omitempty"`
	NodeCapacityID           string             `json:"node_capacity_id,omitempty"`
	ClientCohort             string             `json:"client_cohort,omitempty"`
	CapacitySource           string             `json:"capacity_source,omitempty"`
	RouteWitnessID           string             `json:"route_witness_id,omitempty"`
	ID                       string             `json:"id"`
	EdgeID                   string             `json:"edge_id"`
	Hostname                 string             `json:"hostname"`
	TrafficClass             string             `json:"traffic_class"`
	Scope                    string             `json:"scope"`
	RouteGeneration          string             `json:"route_generation"`
	ObservedAt               time.Time          `json:"observed_at"`
	ClientNetworkMS          *float64           `json:"client_network_ms"`
	ServiceNetworkMS         *float64           `json:"service_network_ms"`
	ClientSource             string             `json:"client_source"`
	ServiceSource            string             `json:"service_source"`
	UploadBPS                *float64           `json:"upload_bps"`
	DownloadBPS              *float64           `json:"download_bps"`
	ClientFailureRate        *float64           `json:"client_failure_rate"`
	ServiceFailureRate       *float64           `json:"service_failure_rate"`
	CapacityUtilization      *float64           `json:"capacity_utilization"`
	Diagnostics              map[string]float64 `json:"diagnostics"`
}

type Snapshot struct {
	DNSHostname         string                        `json:"dns_hostname,omitempty"`
	ClientProbeReports  []model.EdgeClientProbeReport `json:"client_probe_reports,omitempty"`
	NodeCapacitySamples []NodeCapacitySample          `json:"node_capacity_samples,omitempty"`
	Limitations         []string                      `json:"limitations,omitempty"`
	ActualDNSReceipt    json.RawMessage               `json:"actual_dns_receipt,omitempty"`
	NetworkSamples      []model.EdgeNetworkSample     `json:"network_samples,omitempty"`
	Schema              string                        `json:"schema"`
	CapturedAt          time.Time                     `json:"captured_at"`
	Hostname            string                        `json:"hostname"`
	TrafficClass        string                        `json:"traffic_class"`
	Scope               string                        `json:"scope"`
	Policy              Policy                        `json:"policy"`
	CurrentEdgeID       string                        `json:"current_edge_id"`
	LastSwitchAt        *time.Time                    `json:"last_switch_at"`
	Candidates          []Candidate                   `json:"candidates"`
	Observations        []Observation                 `json:"observations"`
	Blockers            []string                      `json:"blockers"`
}

type Metric struct {
	State   string  `json:"state"`
	Value   float64 `json:"value"`
	Records int     `json:"records"`
}

type Assessment struct {
	EdgeID      string            `json:"edge_id"`
	EdgeGroupID string            `json:"edge_group_id"`
	Score       float64           `json:"score"`
	Lower       float64           `json:"lower"`
	Upper       float64           `json:"upper"`
	Metrics     map[string]Metric `json:"metrics"`
	Missing     []string          `json:"missing"`
	HardGates   []string          `json:"hard_gates"`
	Ready       bool              `json:"ready"`
	RecordCount int               `json:"record_count"`
	BucketCount int               `json:"bucket_count"`
}

type Result struct {
	Comparisons      []NetworkComparison `json:"comparisons,omitempty"`
	Mode             string              `json:"mode"`
	DNSUnchanged     bool                `json:"dns_unchanged"`
	PromotionReady   bool                `json:"promotion_ready"`
	Hypothesis       string              `json:"hypothesis"`
	ProposedEdgeID   string              `json:"proposed_edge_id"`
	ProbeEdgeIDs     []string            `json:"probe_edge_ids"`
	ProbeExecuted    bool                `json:"probe_executed"`
	SustainedBuckets int                 `json:"sustained_buckets"`
	Candidates       []Assessment        `json:"candidates"`
	RejectedRecords  map[string]int      `json:"rejected_records"`
	Blockers         []string            `json:"blockers"`
}

type Receipt struct {
	Snapshot     Snapshot `json:"snapshot"`
	Result       Result   `json:"result"`
	Digest       string   `json:"digest"`
	DigestFormat string   `json:"digest_format,omitempty"`
}

func Capture(snapshot Snapshot) (Receipt, error) {
	result, err := Evaluate(snapshot)
	if err != nil {
		return Receipt{}, err
	}
	receipt := Receipt{Snapshot: snapshot, Result: result}
	if len(snapshot.ActualDNSReceipt) > 0 {
		receipt.DigestFormat = ReceiptDigestFormat
	}
	receipt.Digest, err = receiptDigest(receipt)
	return receipt, err
}

func Replay(receipt Receipt) (Result, error) {
	digest, err := receiptDigest(receipt)
	if err != nil || digest != receipt.Digest {
		return Result{}, errors.New("shadow receipt integrity mismatch")
	}
	result, err := Evaluate(receipt.Snapshot)
	if err != nil {
		return Result{}, err
	}
	want, _ := json.Marshal(receipt.Result)
	got, _ := json.Marshal(result)
	if string(want) != string(got) {
		return Result{}, errors.New("shadow replay differs from captured result")
	}
	return result, nil
}

func receiptDigest(receipt Receipt) (string, error) {
	switch receipt.DigestFormat {
	case "":
	case ReceiptDigestFormat:
		if len(receipt.Snapshot.ActualDNSReceipt) > 0 {
			decoder := json.NewDecoder(bytes.NewReader(receipt.Snapshot.ActualDNSReceipt))
			decoder.UseNumber()
			var embedded any
			if err := decoder.Decode(&embedded); err != nil {
				return "", err
			}
			if !json.Valid(receipt.Snapshot.ActualDNSReceipt) {
				return "", errors.New("invalid embedded DNS receipt")
			}
			normalized, err := json.Marshal(embedded)
			if err != nil {
				return "", err
			}
			receipt.Snapshot.ActualDNSReceipt = normalized
		}
	default:
		return "", errors.New("unsupported shadow digest format")
	}
	receipt.Digest = ""
	raw, err := json.Marshal(receipt)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func Evaluate(snapshot Snapshot) (Result, error) {
	if err := validate(snapshot); err != nil {
		return Result{}, err
	}
	if IsNetworkPolicy(snapshot.Policy.Version) {
		return evaluateNetwork(snapshot), nil
	}
	result := Result{Mode: "shadow", DNSUnchanged: true, Hypothesis: "hold", ProposedEdgeID: snapshot.CurrentEdgeID,
		ProbeEdgeIDs: []string{}, Candidates: []Assessment{}, RejectedRecords: map[string]int{},
		Blockers: append([]string{"shadow_only_not_serving_authority"}, snapshot.Blockers...)}
	observations := make(map[string][]Observation)
	nodes := make(map[string]Candidate)
	for _, candidate := range snapshot.Candidates {
		nodes[candidate.EdgeID] = candidate
	}
	seen := make(map[string]bool)
	for _, observation := range snapshot.Observations {
		reason := ""
		candidate, exists := nodes[observation.EdgeID]
		switch {
		case seen[observation.ID]:
			reason = "duplicate_id"
		case !exists:
			reason = "unknown_physical_edge"
		case observation.Hostname != snapshot.Hostname || observation.TrafficClass != snapshot.TrafficClass || observation.Scope != snapshot.Scope:
			reason = "context_mismatch"
		case observation.ObservedAt.After(snapshot.CapturedAt) || !observation.ObservedAt.After(snapshot.CapturedAt.Add(-time.Duration(snapshot.Policy.WindowSeconds)*time.Second)):
			reason = "outside_window"
		case candidate.RouteGeneration == "" || observation.RouteGeneration != candidate.RouteGeneration:
			reason = "route_generation_mismatch"
		}
		seen[observation.ID] = true
		if reason != "" {
			result.RejectedRecords[reason]++
			continue
		}
		observations[observation.EdgeID] = append(observations[observation.EdgeID], observation)
	}
	for _, candidate := range snapshot.Candidates {
		result.Candidates = append(result.Candidates, assess(candidate, observations[candidate.EdgeID], snapshot.Policy, snapshot.CapturedAt))
	}
	sort.Slice(result.Candidates, func(left, right int) bool {
		if result.Candidates[left].Score != result.Candidates[right].Score {
			return result.Candidates[left].Score < result.Candidates[right].Score
		}
		return result.Candidates[left].EdgeID < result.Candidates[right].EdgeID
	})
	var current, best *Assessment
	for index := range result.Candidates {
		candidate := &result.Candidates[index]
		if candidate.EdgeID == snapshot.CurrentEdgeID {
			current = candidate
		}
		if best == nil && candidate.Ready {
			best = candidate
		}
	}
	switch {
	case len(snapshot.Blockers) > 0:
		result.Blockers = append(result.Blockers, "snapshot_incomplete")
	case current == nil:
		result.Blockers = append(result.Blockers, "actual_current_physical_edge_unknown")
	case best == nil:
		result.Blockers = append(result.Blockers, "no_candidate_with_complete_network_evidence")
	case len(current.HardGates) > 0:
		result.Hypothesis = "failover"
		result.ProposedEdgeID = best.EdgeID
	case best.EdgeID == current.EdgeID:
	case !current.Ready:
		result.Blockers = append(result.Blockers, "incumbent_evidence_unknown")
	case snapshot.LastSwitchAt == nil:
		result.Blockers = append(result.Blockers, "last_switch_time_unknown")
	case snapshot.CapturedAt.Sub(*snapshot.LastSwitchAt) < time.Duration(snapshot.Policy.CooldownSeconds)*time.Second:
		result.Blockers = append(result.Blockers, "switch_cooldown")
	default:
		result.SustainedBuckets = sustained(snapshot, nodes[best.EdgeID], nodes[current.EdgeID], observations)
		if advantageous(*best, *current, snapshot.Policy) && result.SustainedBuckets >= snapshot.Policy.RequiredBuckets {
			result.Hypothesis = "switch"
			result.ProposedEdgeID = best.EdgeID
		} else {
			result.Blockers = append(result.Blockers, "advantage_not_sustained_or_intervals_overlap")
		}
	}
	probeCandidates := []string{}
	for _, candidate := range result.Candidates {
		if !candidate.Ready && len(candidate.HardGates) == 0 && proofFresh(nodes[candidate.EdgeID], snapshot.Policy, snapshot.CapturedAt) {
			probeCandidates = append(probeCandidates, candidate.EdgeID)
		}
	}
	sort.Strings(probeCandidates)
	if len(probeCandidates) > 0 {
		offset := int(snapshot.CapturedAt.Unix()/int64(snapshot.Policy.ProbeIntervalSeconds)) % len(probeCandidates)
		for index := 0; index < snapshot.Policy.ProbeBudgetPerInterval && index < len(probeCandidates); index++ {
			result.ProbeEdgeIDs = append(result.ProbeEdgeIDs, probeCandidates[(offset+index)%len(probeCandidates)])
		}
	}
	return result, nil
}

func assess(candidate Candidate, observations []Observation, policy Policy, now time.Time) Assessment {
	assessment := Assessment{EdgeID: candidate.EdgeID, EdgeGroupID: candidate.EdgeGroupID,
		HardGates: append([]string{}, candidate.HardGates...), Metrics: map[string]Metric{}, Missing: []string{}}
	if !proofFresh(candidate, policy, now) {
		assessment.Missing = append(assessment.Missing, "hostname_route_tls_proof")
	}
	buckets := map[int64]bool{}
	latest := map[string]time.Time{}
	for _, observation := range observations {
		assessment.RecordCount++
		buckets[observation.ObservedAt.Unix()/int64(policy.BucketSeconds)] = true
		if observation.ClientSource == "public_tcp_info" || observation.ClientSource == "client_probe" {
			accumulate(assessment.Metrics, latest, "client_network_ms", observation.ClientNetworkMS, observation.ObservedAt)
			accumulate(assessment.Metrics, latest, "upload_bps", observation.UploadBPS, observation.ObservedAt)
			accumulate(assessment.Metrics, latest, "download_bps", observation.DownloadBPS, observation.ObservedAt)
			accumulate(assessment.Metrics, latest, "client_failure_rate", observation.ClientFailureRate, observation.ObservedAt)
		}
		if observation.ServiceSource == "service_endpoint_tcp" {
			accumulate(assessment.Metrics, latest, "service_network_ms", observation.ServiceNetworkMS, observation.ObservedAt)
			accumulate(assessment.Metrics, latest, "service_failure_rate", observation.ServiceFailureRate, observation.ObservedAt)
		}
		accumulate(assessment.Metrics, latest, "capacity_utilization", observation.CapacityUtilization, observation.ObservedAt)
	}
	assessment.BucketCount = len(buckets)
	uncertainty := 0.0
	for _, name := range []string{"client_network_ms", "service_network_ms", "upload_bps", "download_bps", "client_failure_rate", "service_failure_rate", "capacity_utilization"} {
		metric := assessment.Metrics[name]
		cost := policy.UnknownCostMS
		if metric.Records == 0 {
			metric.State = "unknown"
			assessment.Missing = append(assessment.Missing, name)
			uncertainty += policy.UncertaintyMS
		} else {
			metric.State = "observed"
			metric.Value /= float64(metric.Records)
			cost = metric.Value
			switch name {
			case "upload_bps", "download_bps":
				cost = policy.ThroughputCostMS * math.Max(0, 1-metric.Value/policy.ThroughputTargetBPS)
			case "client_failure_rate", "service_failure_rate":
				cost *= policy.FailureCostMS
			case "capacity_utilization":
				cost *= policy.CapacityCostMS
			}
			uncertainty += policy.UncertaintyMS / math.Sqrt(float64(metric.Records))
			if metric.Records < policy.MinimumRecords {
				assessment.Missing = append(assessment.Missing, name+"_sample_count")
			}
			if now.Sub(latest[name]) > time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second {
				assessment.Missing = append(assessment.Missing, name+"_stale")
			}
		}
		assessment.Metrics[name] = metric
		assessment.Score += cost
	}
	assessment.Lower = math.Max(0, assessment.Score-uncertainty)
	assessment.Upper = assessment.Score + uncertainty
	assessment.Ready = len(assessment.Missing) == 0 && len(assessment.HardGates) == 0 && assessment.BucketCount >= policy.RequiredBuckets
	return assessment
}

func proofFresh(candidate Candidate, policy Policy, now time.Time) bool {
	return candidate.RouteProofVerified && candidate.RouteGeneration != "" && candidate.ProofObservedAt != nil &&
		!candidate.ProofObservedAt.After(now) && now.Sub(*candidate.ProofObservedAt) <= time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second
}

func accumulate(metrics map[string]Metric, latest map[string]time.Time, name string, value *float64, observedAt time.Time) {
	if value == nil {
		return
	}
	metric := metrics[name]
	metric.Records++
	metric.Value += *value
	metrics[name] = metric
	if observedAt.After(latest[name]) {
		latest[name] = observedAt
	}
}

func advantageous(challenger, current Assessment, policy Policy) bool {
	return current.Lower-challenger.Upper >= policy.AdvantageMS && challenger.Upper <= current.Lower*(1-policy.AdvantageRatio)
}

func sustained(snapshot Snapshot, challenger, current Candidate, observations map[string][]Observation) int {
	count := 0
	lastBucket := snapshot.CapturedAt.Unix()/int64(snapshot.Policy.BucketSeconds) - 1
	policy := snapshot.Policy
	policy.RequiredBuckets = 1
	for index := 0; index < snapshot.Policy.RequiredBuckets; index++ {
		bucket := lastBucket - int64(index)
		selected := func(edgeID string) []Observation {
			out := []Observation{}
			for _, observation := range observations[edgeID] {
				if observation.ObservedAt.Unix()/int64(policy.BucketSeconds) == bucket {
					out = append(out, observation)
				}
			}
			return out
		}
		policy.EvidenceMaxAgeSeconds = policy.WindowSeconds
		challengerAssessment := assess(challenger, selected(challenger.EdgeID), policy, snapshot.CapturedAt)
		currentAssessment := assess(current, selected(current.EdgeID), policy, snapshot.CapturedAt)
		if !challengerAssessment.Ready || !currentAssessment.Ready || !advantageous(challengerAssessment, currentAssessment, policy) {
			break
		}
		count++
	}
	return count
}

func validate(snapshot Snapshot) error {
	if snapshot.DNSHostname != "" && (snapshot.Policy.Version != DeliveryNetworkPolicyVersion || snapshot.DNSHostname == snapshot.Hostname || len(snapshot.DNSHostname) > 253 || snapshot.DNSHostname != strings.TrimSuffix(strings.ToLower(snapshot.DNSHostname), ".") || strings.ContainsAny(snapshot.DNSHostname, "/:@?# \t\r\n")) {
		return errors.New("invalid explicit DNS alias binding")
	}
	if snapshot.Schema != Schema || snapshot.Hostname == "" || snapshot.TrafficClass == "" || snapshot.Scope == "" || snapshot.CapturedAt.IsZero() || snapshot.CapturedAt.Unix() < 0 {
		return errors.New("invalid shadow schema or context")
	}
	policy := snapshot.Policy
	if IsNetworkPolicy(policy.Version) {
		if err := model.ValidatePhysicalEdgeQualityPolicy(policy); err != nil {
			return err
		}
	}
	if policy.EvidenceMaxAgeSeconds < 60 || policy.EvidenceMaxAgeSeconds > policy.WindowSeconds {
		return errors.New("invalid evidence age bound")
	}
	if policy.Version == "" || policy.WindowSeconds < 60 || policy.WindowSeconds > 86400 || policy.BucketSeconds < 60 || policy.BucketSeconds > policy.WindowSeconds || policy.RequiredBuckets < 2 || policy.RequiredBuckets > 12 || policy.RequiredBuckets*policy.BucketSeconds > policy.WindowSeconds || policy.MinimumRecords < 2 || policy.MinimumRecords > 10000 || policy.CooldownSeconds < 0 || policy.CooldownSeconds > 86400 || policy.ProbeIntervalSeconds < 60 || policy.ProbeIntervalSeconds > 86400 || policy.ProbeBudgetPerInterval < 0 || policy.ProbeBudgetPerInterval > 2 {
		return errors.New("invalid bounded shadow policy")
	}
	for _, value := range []float64{policy.AdvantageMS, policy.AdvantageRatio, policy.UnknownCostMS, policy.UncertaintyMS, policy.FailureCostMS, policy.CapacityCostMS, policy.ThroughputCostMS, policy.ThroughputTargetBPS} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 || value > 1e12 {
			return errors.New("invalid shadow cost")
		}
	}
	if policy.AdvantageRatio >= 1 || len(snapshot.Observations) > MaxObservations || len(snapshot.NetworkSamples) > MaxObservations || len(snapshot.Candidates) > 256 || len(snapshot.NodeCapacitySamples) > 8 || len(snapshot.ClientProbeReports) > 256 {
		return errors.New("shadow input exceeds bounds")
	}
	capacitySamples := map[string]NodeCapacitySample{}
	for _, sample := range snapshot.NodeCapacitySamples {
		if err := ValidateNodeCapacitySample(sample, snapshot.CapturedAt); err != nil {
			return err
		}
		if _, exists := capacitySamples[sample.EdgeID]; exists {
			return errors.New("duplicate physical-node capacity identity")
		}
		capacitySamples[sample.EdgeID] = sample
	}
	networkSamples := map[string]model.EdgeNetworkSample{}
	for _, sample := range snapshot.NetworkSamples {
		if err := model.ValidateEdgeNetworkSample(sample); err != nil {
			return err
		}
		key := sample.EdgeID + "\x00" + sample.ID
		if _, found := networkSamples[key]; found {
			return errors.New("duplicate raw network sample identity")
		}
		networkSamples[key] = sample
	}
	if snapshot.LastSwitchAt != nil && snapshot.LastSwitchAt.After(snapshot.CapturedAt) {
		return errors.New("future switch time")
	}
	seen := map[string]bool{}
	probeObservations, probeErr := ClientProbeObservations(snapshot)
	if probeErr != nil {
		return probeErr
	}
	probesByID := map[string]Observation{}
	for _, observation := range probeObservations {
		probesByID[observation.ID] = observation
	}
	for _, candidate := range snapshot.Candidates {
		if candidate.EdgeID == "" || seen[candidate.EdgeID] {
			return errors.New("missing or duplicate physical edge")
		}
		seen[candidate.EdgeID] = true
	}
	for _, observation := range snapshot.Observations {
		if observation.ID == "" {
			return errors.New("observation id required")
		}
		if observation.ClientProbeRoundID != "" || observation.ClientSource == "authenticated_client_probe" {
			expected, exists := probesByID[observation.ID]
			if !exists || !reflect.DeepEqual(expected, observation) {
				return errors.New("client probe observation differs from complete captured report")
			}
			continue
		}
		if observation.NodeCapacityID != "" {
			sample, found := capacitySamples[observation.EdgeID]
			if !found || !NodeCapacityObservationMatches(snapshot, observation, sample) {
				return errors.New("capacity observation differs from captured physical-node facts")
			}
			continue
		}
		if observation.RouteWitnessID != "" {
			witness, found := networkSamples[observation.EdgeID+"\x00"+observation.RouteWitnessID]
			if observation.CapacitySource != "" {
				if !found || !networkWitnessCapacityMatches(observation, witness, snapshot.CapturedAt) {
					return errors.New("capacity observation lacks matching captured node witness")
				}
				continue
			}
			sample, sampled := networkSamples[observation.EdgeID+"\x00"+strings.TrimPrefix(observation.ID, "network:"+observation.EdgeID+":")]
			if !found || !sampled || observation.ID != "network:"+sample.EdgeID+":"+sample.ID || !model.EdgeNetworkWitnessMatches(sample, witness) || witness.ObservedAt.After(snapshot.CapturedAt) ||
				observation.Hostname != sample.Hostname || observation.TrafficClass != sample.TrafficClass || observation.RouteGeneration != sample.RouteDigest ||
				!observation.ObservedAt.Equal(sample.ObservedAt) || !networkWitnessMeasurementMatches(observation, sample, policy.Version) {
				return errors.New("historical observation lacks matching captured route witness")
			}
		}
		for _, value := range []*float64{observation.ClientNetworkMS, observation.ServiceNetworkMS, observation.UploadBPS, observation.DownloadBPS, observation.ClientFailureRate, observation.ServiceFailureRate, observation.CapacityUtilization, observation.ClientRetransmissionRate} {
			if value != nil && (math.IsNaN(*value) || math.IsInf(*value, 0) || *value < 0 || *value > 1e12) {
				return fmt.Errorf("invalid measurement in %s", observation.ID)
			}
		}
		for _, value := range []*float64{observation.ClientFailureRate, observation.ServiceFailureRate, observation.CapacityUtilization, observation.ClientRetransmissionRate} {
			if value != nil && *value > 1 {
				return errors.New("measurement ratio outside [0,1]")
			}
		}
	}
	return nil
}

func networkWitnessMeasurementMatches(observation Observation, sample model.EdgeNetworkSample, policyVersion string) bool {
	if observation.UploadBPS != nil || observation.ClientFailureRate != nil || observation.CapacityUtilization != nil {
		return false
	}
	var download, retransmission *float64
	if policyVersion == DeliveryNetworkPolicyVersion && sample.Source == "public_front_tcp_info_v1" {
		download, retransmission = model.EdgeClientDeliveryMetrics(sample.ClientNetwork)
	}
	if !reflect.DeepEqual(observation.DownloadBPS, download) || !reflect.DeepEqual(observation.ClientRetransmissionRate, retransmission) {
		return false
	}
	var serviceFailure *float64
	if model.IsBoundedPhysicalNetworkPolicy(policyVersion) && model.EdgeNetworkServiceSource(sample.Source) && sample.ServiceConnectFailed != nil {
		value := 0.0
		if *sample.ServiceConnectFailed {
			value = 1
		}
		serviceFailure = &value
	}
	if !reflect.DeepEqual(observation.ServiceFailureRate, serviceFailure) {
		return false
	}
	if model.EdgeNetworkServiceSource(sample.Source) {
		return observation.ServiceSource == "service_endpoint_tcp" && observation.ClientSource == "" && observation.ClientNetworkMS == nil && reflect.DeepEqual(observation.ServiceNetworkMS, sample.ServiceRTTMS)
	}
	return sample.ClientNetwork != nil && observation.ClientSource == "public_tcp_info" && observation.ServiceSource == "" && observation.ServiceNetworkMS == nil &&
		(observation.ClientCohort == "" || observation.ClientCohort == sample.ClientNetwork.Scope) &&
		(observation.Scope == "global" || observation.Scope == sample.ClientNetwork.Scope) && reflect.DeepEqual(observation.ClientNetworkMS, sample.ClientNetwork.RTTMS)
}

func networkWitnessCapacityMatches(observation Observation, witness model.EdgeNetworkSample, now time.Time) bool {
	if witness.Source != "route_tls_witness_v1" || witness.RouteWitness == nil || witness.RouteWitness.NodeCapacity == nil || witness.ObservedAt.After(now) ||
		observation.ID != "capacity:"+witness.EdgeID+":"+witness.ID || observation.EdgeID != witness.EdgeID || observation.Hostname != witness.Hostname ||
		observation.TrafficClass != witness.TrafficClass || observation.RouteGeneration != witness.RouteDigest || observation.ClientSource != "" || observation.ServiceSource != "" ||
		observation.ClientNetworkMS != nil || observation.ServiceNetworkMS != nil || observation.UploadBPS != nil || observation.DownloadBPS != nil || observation.ClientFailureRate != nil || observation.ServiceFailureRate != nil || observation.ClientRetransmissionRate != nil {
		return false
	}
	capacity := witness.RouteWitness.NodeCapacity
	value, err := model.EdgeNetworkNodeUtilization(capacity)
	return err == nil && observation.CapacitySource == capacity.Source && observation.ObservedAt.Equal(capacity.ObservedAt) &&
		!capacity.CPUObservedAt.After(now) && !capacity.MemoryObservedAt.After(now) && observation.CapacityUtilization != nil && *observation.CapacityUtilization == value
}
