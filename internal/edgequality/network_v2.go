package edgequality

import (
	"math"
	"net/netip"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const NetworkPolicyVersion = model.PhysicalNetworkPolicyVersion
const BoundedNetworkPolicyVersion = model.PhysicalBoundedNetworkPolicyVersion
const DeliveryNetworkPolicyVersion = model.PhysicalDeliveryNetworkPolicyVersion

func IsNetworkPolicy(version string) bool {
	return version == NetworkPolicyVersion || model.IsBoundedPhysicalNetworkPolicy(version)
}

type NetworkComparison struct {
	EdgeID           string  `json:"edge_id"`
	IncumbentEdgeID  string  `json:"incumbent_edge_id"`
	Cohort           string  `json:"cohort"`
	IncumbentLower   float64 `json:"incumbent_lower"`
	ChallengerUpper  float64 `json:"challenger_upper"`
	SustainedBuckets int     `json:"sustained_buckets"`
	Ready            bool    `json:"ready"`
	Advantageous     bool    `json:"advantageous"`
}

func DefaultNetworkPolicy() Policy {
	policy := DefaultShadowPolicy()
	policy.Version = BoundedNetworkPolicyVersion
	policy.UnknownCostMS, policy.UncertaintyMS = 30, 5
	policy.MaximumNodeUtilization = 0.85
	return policy
}

func DefaultDeliveryNetworkPolicy() Policy {
	policy := DefaultNetworkPolicy()
	policy.Version = DeliveryNetworkPolicyVersion
	return policy
}

func validNetworkCohort(value string) bool {
	if !strings.HasPrefix(value, "tcp_peer:") {
		return false
	}
	prefix, err := netip.ParsePrefix(strings.TrimPrefix(value, "tcp_peer:"))
	return err == nil && prefix == prefix.Masked() && !prefix.Addr().Is4In6() &&
		((prefix.Addr().Is4() && prefix.Bits() == 24) || (prefix.Addr().Is6() && prefix.Bits() == 48))
}

func evaluateNetwork(snapshot Snapshot) Result {
	result := Result{Mode: "shadow", DNSUnchanged: true, Hypothesis: "hold", ProposedEdgeID: snapshot.CurrentEdgeID,
		ProbeEdgeIDs: []string{}, Candidates: []Assessment{}, RejectedRecords: map[string]int{},
		Blockers: append([]string{"shadow_only_not_serving_authority"}, snapshot.Blockers...)}
	nodes := map[string]Candidate{}
	observations := map[string][]Observation{}
	seen := map[string]bool{}
	for _, candidate := range snapshot.Candidates {
		nodes[candidate.EdgeID] = candidate
	}
	for _, observation := range snapshot.Observations {
		candidate, exists := nodes[observation.EdgeID]
		reason := ""
		switch {
		case seen[observation.ID]:
			reason = "duplicate_id"
		case !exists:
			reason = "unknown_physical_edge"
		case observation.Hostname != snapshot.Hostname || observation.TrafficClass != snapshot.TrafficClass || observation.Scope != snapshot.Scope:
			reason = "context_mismatch"
		case observation.ObservedAt.After(snapshot.CapturedAt) || !observation.ObservedAt.After(snapshot.CapturedAt.Add(-time.Duration(snapshot.Policy.WindowSeconds)*time.Second)):
			reason = "outside_window"
		case candidate.RouteGeneration == "" || candidate.RouteGeneration != observation.RouteGeneration:
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
		result.Candidates = append(result.Candidates, assessNetwork(candidate, observations[candidate.EdgeID], snapshot.Policy, snapshot.CapturedAt, ""))
	}
	sort.Slice(result.Candidates, func(left, right int) bool {
		if result.Candidates[left].Score != result.Candidates[right].Score {
			return result.Candidates[left].Score < result.Candidates[right].Score
		}
		return result.Candidates[left].EdgeID < result.Candidates[right].EdgeID
	})
	current, currentExists := nodes[snapshot.CurrentEdgeID]
	currentGated := len(current.HardGates) > 0
	var best *Assessment
	for index := range result.Candidates {
		candidate := &result.Candidates[index]
		if candidate.EdgeID == current.EdgeID && len(candidate.HardGates) > 0 {
			currentGated = true
		}
		if best == nil && candidate.Ready {
			best = candidate
		}
	}
	switch {
	case len(snapshot.Blockers) > 0:
		result.Blockers = append(result.Blockers, "snapshot_incomplete")
	case !currentExists:
		result.Blockers = append(result.Blockers, "actual_current_physical_edge_unknown")
	case best == nil:
		result.Blockers = append(result.Blockers, "no_candidate_with_core_network_and_capacity_evidence")
	case currentGated:
		result.Hypothesis, result.ProposedEdgeID = "failover", best.EdgeID
	default:
		selectNetworkChallenger(snapshot, nodes, observations, &result)
	}
	unknown := []string{}
	for _, candidate := range result.Candidates {
		needsCohort := candidate.EdgeID != current.EdgeID && len(networkSharedCohorts(observations[current.EdgeID], observations[candidate.EdgeID])) == 0
		if (len(candidate.Missing) != 0 || needsCohort) && len(candidate.HardGates) == 0 && proofFresh(nodes[candidate.EdgeID], snapshot.Policy, snapshot.CapturedAt) {
			unknown = append(unknown, candidate.EdgeID)
		}
	}
	sort.Strings(unknown)
	if len(unknown) > 0 {
		offset := int(snapshot.CapturedAt.Unix()/int64(snapshot.Policy.ProbeIntervalSeconds)) % len(unknown)
		for index := 0; index < snapshot.Policy.ProbeBudgetPerInterval && index < len(unknown); index++ {
			result.ProbeEdgeIDs = append(result.ProbeEdgeIDs, unknown[(offset+index)%len(unknown)])
		}
	}
	return result
}

func selectNetworkChallenger(snapshot Snapshot, nodes map[string]Candidate, observations map[string][]Observation, result *Result) {
	current := nodes[snapshot.CurrentEdgeID]
	for _, candidate := range result.Candidates {
		if !candidate.Ready || candidate.EdgeID == current.EdgeID {
			continue
		}
		cohorts := networkSharedCohorts(observations[current.EdgeID], observations[candidate.EdgeID])
		if model.IsBoundedPhysicalNetworkPolicy(snapshot.Policy.Version) && len(cohorts) == 0 &&
			(!hasClientNetworkCohort(observations[current.EdgeID]) || !hasClientNetworkCohort(observations[candidate.EdgeID])) {
			cohorts = []string{""}
		}
		if len(cohorts) == 0 || len(cohorts) > 16 {
			continue
		}
		qualified := true
		minimumBuckets := snapshot.Policy.RequiredBuckets
		for _, cohort := range cohorts {
			incumbent := assessNetwork(current, observations[current.EdgeID], snapshot.Policy, snapshot.CapturedAt, cohort)
			challenger := assessNetwork(nodes[candidate.EdgeID], observations[candidate.EdgeID], snapshot.Policy, snapshot.CapturedAt, cohort)
			comparison := NetworkComparison{EdgeID: candidate.EdgeID, IncumbentEdgeID: current.EdgeID, Cohort: cohort,
				IncumbentLower: incumbent.Lower, ChallengerUpper: challenger.Upper, Ready: incumbent.Ready && challenger.Ready}
			comparison.Advantageous = comparison.Ready && networkAdvantageous(challenger, incumbent, snapshot.Policy)
			comparison.SustainedBuckets = networkSustained(snapshot, current, nodes[candidate.EdgeID], observations, cohort)
			if len(result.Comparisons) >= 1024 {
				result.Blockers = append(result.Blockers, "cohort_comparison_limit_reached")
				return
			}
			result.Comparisons = append(result.Comparisons, comparison)
			qualified = qualified && comparison.Advantageous && comparison.SustainedBuckets >= snapshot.Policy.RequiredBuckets
			minimumBuckets = min(minimumBuckets, comparison.SustainedBuckets)
		}
		if !qualified {
			continue
		}
		if snapshot.LastSwitchAt == nil {
			result.Blockers = append(result.Blockers, "last_switch_time_unknown")
			return
		}
		if snapshot.CapturedAt.Sub(*snapshot.LastSwitchAt) < time.Duration(snapshot.Policy.CooldownSeconds)*time.Second {
			result.Blockers = append(result.Blockers, "switch_cooldown")
			return
		}
		result.Hypothesis, result.ProposedEdgeID, result.SustainedBuckets = "switch", candidate.EdgeID, minimumBuckets
		return
	}
	if model.IsBoundedPhysicalNetworkPolicy(snapshot.Policy.Version) {
		result.Blockers = append(result.Blockers, "no_sustained_advantage_in_comparable_network_evidence")
	} else {
		result.Blockers = append(result.Blockers, "no_sustained_advantage_in_common_client_cohorts")
	}
}

func hasClientNetworkCohort(observations []Observation) bool {
	for _, observation := range observations {
		if observation.ClientSource == "public_tcp_info" && observation.ClientNetworkMS != nil && validNetworkCohort(observation.ClientCohort) {
			return true
		}
	}
	return false
}

func networkSharedCohorts(current, challenger []Observation) []string {
	incumbent := map[string]bool{}
	shared := map[string]bool{}
	for _, observation := range current {
		if observation.ClientSource == "public_tcp_info" && observation.ClientNetworkMS != nil && validNetworkCohort(observation.ClientCohort) {
			incumbent[observation.ClientCohort] = true
		}
	}
	for _, observation := range challenger {
		if observation.ClientSource == "public_tcp_info" && observation.ClientNetworkMS != nil && incumbent[observation.ClientCohort] {
			shared[observation.ClientCohort] = true
		}
	}
	cohorts := []string{}
	for cohort := range shared {
		cohorts = append(cohorts, cohort)
	}
	sort.Strings(cohorts)
	return cohorts
}

func assessNetwork(candidate Candidate, observations []Observation, policy Policy, now time.Time, cohort string) Assessment {
	return assessNetworkAt(candidate, observations, policy, now, now, cohort)
}

func assessNetworkAt(candidate Candidate, observations []Observation, policy Policy, now, metricNow time.Time, cohort string) Assessment {
	assessment := Assessment{EdgeID: candidate.EdgeID, EdgeGroupID: candidate.EdgeGroupID, HardGates: append([]string{}, candidate.HardGates...), Metrics: map[string]Metric{}, Missing: []string{}}
	compareClient := !model.IsBoundedPhysicalNetworkPolicy(policy.Version) || cohort != ""
	values := map[string][]float64{}
	latest := map[string]time.Time{}
	latestValue := map[string]float64{}
	buckets := map[string]map[int64]bool{}
	collect := func(name string, value *float64, observed time.Time) {
		if value == nil {
			return
		}
		values[name] = append(values[name], *value)
		if buckets[name] == nil {
			buckets[name] = map[int64]bool{}
		}
		buckets[name][observed.Unix()/int64(policy.BucketSeconds)] = true
		if observed.After(latest[name]) {
			latest[name] = observed
			latestValue[name] = *value
		} else if observed.Equal(latest[name]) {
			latestValue[name] = math.Max(latestValue[name], *value)
		}
	}
	for _, observation := range observations {
		assessment.RecordCount++
		if compareClient && observation.ClientSource == "public_tcp_info" && validNetworkCohort(observation.ClientCohort) && (cohort == "" || cohort == observation.ClientCohort) {
			collect("client_network_ms", observation.ClientNetworkMS, observation.ObservedAt)
			collect("upload_bps", observation.UploadBPS, observation.ObservedAt)
			collect("download_bps", observation.DownloadBPS, observation.ObservedAt)
			collect("client_failure_rate", observation.ClientFailureRate, observation.ObservedAt)
			if policy.Version == DeliveryNetworkPolicyVersion {
				collect("client_retransmission_rate", observation.ClientRetransmissionRate, observation.ObservedAt)
			}
		}
		if observation.ServiceSource == "service_endpoint_tcp" {
			collect("service_network_ms", observation.ServiceNetworkMS, observation.ObservedAt)
			collect("service_failure_rate", observation.ServiceFailureRate, observation.ObservedAt)
		}
		if observation.CapacitySource == "kubelet_node_allocatable_v1" {
			collect("capacity_utilization", observation.CapacityUtilization, observation.ObservedAt)
		}
	}
	coreReady := proofFresh(candidate, policy, now)
	if !coreReady {
		assessment.Missing = append(assessment.Missing, "hostname_route_tls_proof")
	}
	unknownOptional := false
	metricNames := []string{"client_network_ms", "service_network_ms", "upload_bps", "download_bps", "client_failure_rate", "service_failure_rate", "capacity_utilization"}
	if policy.Version == DeliveryNetworkPolicyVersion {
		metricNames = append(metricNames, "client_retransmission_rate")
	}
	for _, name := range metricNames {
		measured := values[name]
		sort.Float64s(measured)
		core := name == "client_network_ms" && compareClient || name == "service_network_ms" || name == "capacity_utilization"
		minimum := policy.MinimumRecords
		if name == "capacity_utilization" {
			minimum = 1
		}
		fresh := len(measured) >= minimum && !latest[name].After(metricNow) && metricNow.Sub(latest[name]) <= time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second
		if name == "capacity_utilization" {
			fresh = fresh && metricNow.Sub(latest[name]) < 2*time.Minute
		}
		if !fresh {
			assessment.Metrics[name] = Metric{State: "unknown", Records: len(measured)}
			assessment.Missing = append(assessment.Missing, name)
			coreReady = coreReady && !core
			unknownOptional = unknownOptional || !core
			continue
		}
		value := networkQuantile(measured, 0.5)
		if model.IsBoundedPhysicalNetworkPolicy(policy.Version) && (name == "client_failure_rate" || name == "service_failure_rate" || name == "client_retransmission_rate") {
			value = 0
			for _, measuredValue := range measured {
				value += measuredValue / float64(len(measured))
			}
		}
		if name == "capacity_utilization" {
			value = latestValue[name]
		}
		assessment.Metrics[name] = Metric{State: "observed", Value: value, Records: len(measured)}
		switch name {
		case "client_network_ms", "service_network_ms":
			assessment.Score += value
			assessment.Lower += math.Max(0, networkQuantile(measured, 0.1)-policy.UncertaintyMS)
			assessment.Upper += networkQuantile(measured, 0.9) + policy.UncertaintyMS
			if len(buckets[name]) < policy.RequiredBuckets {
				coreReady = false
				assessment.Missing = append(assessment.Missing, name+"_sustained_buckets")
			}
		default:
			cost := value * policy.FailureCostMS
			if name == "upload_bps" || name == "download_bps" {
				cost = policy.ThroughputCostMS * math.Max(0, 1-value/policy.ThroughputTargetBPS)
				if policy.Version == DeliveryNetworkPolicyVersion {
					cost = deliveryCost(value, policy)
					assessment.Score += cost
					assessment.Lower += deliveryCost(networkQuantile(measured, 0.9), policy)
					assessment.Upper += deliveryCost(networkQuantile(measured, 0.1), policy)
					continue
				}
			}
			if name == "capacity_utilization" {
				cost = value * policy.CapacityCostMS
				if value >= policy.MaximumNodeUtilization {
					assessment.HardGates = append(assessment.HardGates, "physical_node_capacity_pressure")
				}
			}
			assessment.Score += cost
			assessment.Lower += cost
			assessment.Upper += cost
		}
	}
	if unknownOptional {
		assessment.Score += policy.UnknownCostMS / 2
		assessment.Upper += policy.UnknownCostMS
	}
	assessment.BucketCount = min(len(buckets["client_network_ms"]), len(buckets["service_network_ms"]))
	if !compareClient {
		assessment.BucketCount = len(buckets["service_network_ms"])
	}
	assessment.Ready = coreReady && len(assessment.HardGates) == 0
	return assessment
}

func networkQuantile(values []float64, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := max(0, min(len(values)-1, int(math.Ceil(quantile*float64(len(values))))-1))
	return values[index]
}

func networkSustained(snapshot Snapshot, current, challenger Candidate, observations map[string][]Observation, cohort string) int {
	policy := snapshot.Policy
	policy.RequiredBuckets = 1
	policy.EvidenceMaxAgeSeconds = policy.WindowSeconds
	lastBucket := snapshot.CapturedAt.Unix()/int64(policy.BucketSeconds) - 1
	count := 0
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
		at := time.Unix((bucket+1)*int64(policy.BucketSeconds), 0).Add(-time.Nanosecond).UTC()
		currentAssessment := assessNetworkAt(current, selected(current.EdgeID), policy, snapshot.CapturedAt, at, cohort)
		challengerAssessment := assessNetworkAt(challenger, selected(challenger.EdgeID), policy, snapshot.CapturedAt, at, cohort)
		if !currentAssessment.Ready || !challengerAssessment.Ready || !networkAdvantageous(challengerAssessment, currentAssessment, policy) {
			break
		}
		count++
	}
	return count
}

func deliveryCost(bytesPerSecond float64, policy Policy) float64 {
	return policy.ThroughputCostMS * math.Min(1000, math.Max(0, policy.ThroughputTargetBPS/math.Max(1, bytesPerSecond)-1))
}

func networkAdvantageous(challenger, current Assessment, policy Policy) bool {
	if policy.Version == DeliveryNetworkPolicyVersion {
		for _, metric := range []string{"download_bps", "client_retransmission_rate"} {
			if (challenger.Metrics[metric].State == "observed") != (current.Metrics[metric].State == "observed") {
				return false
			}
		}
	}
	return advantageous(challenger, current, policy)
}
