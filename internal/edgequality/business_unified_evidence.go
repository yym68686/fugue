package edgequality

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"fugue/internal/model"
)

type unifiedConnection struct {
	sample  model.EdgeNetworkSample
	rate    *float64
	retrans *float64
}

type unifiedWindows struct {
	windows []BusinessExplorationWindow
	records map[businessWindowKey]map[string]unifiedConnection
	totals  map[string]map[string]bool
}

func collectUnifiedBusinessWindows(input BusinessExplorationInput, candidates map[string]Candidate, decision *BusinessExplorationDecision) (unifiedWindows, error) {
	policy := input.Policy
	end := input.CapturedAt.Unix() / int64(policy.BucketSeconds) * int64(policy.BucketSeconds)
	start := end - int64(policy.BucketSeconds*policy.RequiredBuckets)
	result := unifiedWindows{records: map[businessWindowKey]map[string]unifiedConnection{}, totals: map[string]map[string]bool{}}
	coverage := map[string]*BusinessExplorationCoverage{}
	connectionWindows := map[string]map[string]bool{}
	for edgeID := range candidates {
		coverage[edgeID] = &BusinessExplorationCoverage{EdgeID: edgeID, Reasons: map[string]int{}}
		result.totals[edgeID], connectionWindows[edgeID] = map[string]bool{}, map[string]bool{}
	}
	seen := map[string]bool{}
	for _, sample := range input.NetworkSamples {
		if seen[sample.ID] {
			return result, errors.New("duplicate business evidence identity")
		}
		seen[sample.ID] = true
		candidate, exists := candidates[sample.EdgeID]
		reason := ""
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
		case sample.ObservedAt.Before(policy.StartsAt) || sample.ObservedAt.Unix() < start:
			reason = "outside_window"
		case sample.ObservedAt.Unix() >= end:
			reason = "incomplete_bucket"
		}
		if reason != "" {
			decision.Rejected[reason]++
			if entry := coverage[sample.EdgeID]; entry != nil {
				entry.Reasons[reason]++
			}
			continue
		}
		client := sample.ClientNetwork
		entry := coverage[sample.EdgeID]
		entry.Records++
		if entry.LastSampleAt == nil || sample.ObservedAt.After(*entry.LastSampleAt) {
			observed := sample.ObservedAt
			entry.LastSampleAt = &observed
		}
		identity := fmt.Sprintf("%s\x00%s\x00%s", client.ConnectionID, client.Slot, client.StartedAt.UTC().Format(time.RFC3339Nano))
		if client.Backend != nil {
			identity += "\x00" + client.Backend.PodUID
		}
		result.totals[sample.EdgeID][identity] = true
		key := businessWindowKey{edgeID: sample.EdgeID, cohort: client.Scope, start: sample.ObservedAt.Unix() / int64(policy.BucketSeconds) * int64(policy.BucketSeconds)}
		connectionWindows[sample.EdgeID][fmt.Sprintf("%d\x00%s", key.start, identity)] = true
		rate, retrans := model.EdgeClientDeliveryMetrics(client)
		if client.RTTMS != nil {
			entry.RTTRecords++
		}
		if client.Delivery != nil && client.DeliveryBaseline != nil {
			entry.AcknowledgedDeltaRecords++
			if client.Delivery.ApplicationLimited {
				entry.Reasons["delivery_application_limited"]++
			}
			if client.DeliveryBaseline.ObservedAt.Unix() < key.start {
				rate, retrans = nil, nil
				entry.Reasons["delivery_crosses_bucket"]++
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
		if result.records[key] == nil {
			result.records[key] = map[string]unifiedConnection{}
		}
		previous, present := result.records[key][identity]
		if !present || sample.ObservedAt.After(previous.sample.ObservedAt) || sample.ObservedAt.Equal(previous.sample.ObservedAt) && sample.ID < previous.sample.ID {
			result.records[key][identity] = unifiedConnection{sample: sample, rate: rate, retrans: retrans}
		}
	}
	for key, records := range result.records {
		window := BusinessExplorationWindow{EdgeID: key.edgeID, Cohort: key.cohort, StartedAt: time.Unix(key.start, 0).UTC(), Connections: len(records), MetricConnections: map[string]int{}}
		rtts, rates, retransmits := []float64{}, []float64{}, []float64{}
		acknowledged, busy := []float64{}, []float64{}
		for _, record := range records {
			client := record.sample.ClientNetwork
			if client.Delivery != nil && client.DeliveryBaseline != nil && client.DeliveryBaseline.ObservedAt.Unix() >= key.start {
				acknowledged = append(acknowledged, float64(client.Delivery.BytesAcked-client.DeliveryBaseline.BytesAcked))
				busy = append(busy, float64(client.Delivery.BusyMicroseconds-client.DeliveryBaseline.BusyMicroseconds))
			}
			if client.RTTMS != nil {
				rtts = append(rtts, *client.RTTMS)
				if record.sample.ObservedAt.After(window.LastObservedAt) {
					window.LastObservedAt = record.sample.ObservedAt
				}
			}
			if record.rate != nil {
				rates = append(rates, *record.rate)
			}
			if record.retrans != nil {
				retransmits = append(retransmits, *record.retrans)
			}
			decision.UsedSampleIDs = append(decision.UsedSampleIDs, record.sample.ID)
		}
		window.MetricConnections["rtt"], window.MetricConnections["delivery"], window.MetricConnections["retransmission"] = len(rtts), len(rates), len(retransmits)
		window.MetricConnections["acknowledged_delta"] = len(acknowledged)
		window.AcknowledgedBytesMedian, window.BusyMicrosecondsMedian = businessPercentile(acknowledged, .5), businessPercentile(busy, .5)
		window.RTTMedianMS, window.RTTP90MS = businessPercentile(rtts, .5), businessPercentile(rtts, .9)
		window.DeliveryBPS, window.DeliveryP10 = businessPercentile(rates, .5), businessPercentile(rates, .1)
		window.RetransmitP90 = businessPercentile(retransmits, .9)
		result.windows = append(result.windows, window)
	}
	sort.Slice(result.windows, func(left, right int) bool {
		first, second := result.windows[left], result.windows[right]
		if first.EdgeID != second.EdgeID {
			return first.EdgeID < second.EdgeID
		}
		if first.Cohort != second.Cohort {
			return first.Cohort < second.Cohort
		}
		return first.StartedAt.Before(second.StartedAt)
	})
	for edgeID, entry := range coverage {
		entry.Connections, entry.ConnectionWindows = len(result.totals[edgeID]), len(connectionWindows[edgeID])
		if entry.Records == 0 {
			entry.Reasons["no_usable_business_samples"]++
		}
		decision.Coverage = append(decision.Coverage, *entry)
	}
	sort.Slice(decision.Coverage, func(left, right int) bool { return decision.Coverage[left].EdgeID < decision.Coverage[right].EdgeID })
	sort.Strings(decision.UsedSampleIDs)
	decision.Windows = result.windows
	return result, nil
}

func compareUnifiedBusiness(input BusinessExplorationInput, data unifiedWindows, primary, edgeID string) BusinessExplorationComparison {
	policy := input.Policy
	comparison := BusinessExplorationComparison{EdgeID: edgeID, Cohorts: []string{}, Verdict: "insufficient", Reasons: []string{}}
	end := input.CapturedAt.Unix() / int64(policy.BucketSeconds) * int64(policy.BucketSeconds)
	byKey, cohorts := map[businessWindowKey]BusinessExplorationWindow{}, map[string]bool{}
	for _, window := range data.windows {
		if window.EdgeID == primary || window.EdgeID == edgeID {
			byKey[businessWindowKey{edgeID: window.EdgeID, cohort: window.Cohort, start: window.StartedAt.Unix()}] = window
			cohorts[window.Cohort] = true
		}
	}
	matched := map[string]map[string]bool{primary: {}, edgeID: {}}
	allBetter, allWorse, guardsComplete := true, true, true
	for cohort := range cohorts {
		connections := map[string]map[string]bool{primary: {}, edgeID: {}}
		complete, better, worse, qualifiedGuards := 0, true, true, true
		for bucket := 1; bucket <= policy.RequiredBuckets; bucket++ {
			start := end - int64(bucket*policy.BucketSeconds)
			baselineKey, candidateKey := businessWindowKey{primary, cohort, start}, businessWindowKey{edgeID, cohort, start}
			baseline, candidate := byKey[baselineKey], byKey[candidateKey]
			if baseline.MetricConnections["rtt"] < policy.MinimumConnectionsPerBucket || candidate.MetricConnections["rtt"] < policy.MinimumConnectionsPerBucket {
				continue
			}
			if bucket == 1 && (input.CapturedAt.Sub(baseline.LastObservedAt) > time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second || input.CapturedAt.Sub(candidate.LastObservedAt) > time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second) {
				continue
			}
			complete++
			for _, key := range []businessWindowKey{baselineKey, candidateKey} {
				for identity, record := range data.records[key] {
					if record.sample.ClientNetwork.RTTMS != nil {
						connections[key.edgeID][identity] = true
					}
				}
			}
			better = better && unifiedWindowBetter(candidate, baseline, policy)
			worse = worse && unifiedWindowBetter(baseline, candidate, policy)
			for _, metric := range []string{"delivery", "retransmission"} {
				qualifiedGuards = qualifiedGuards && baseline.MetricConnections[metric] >= policy.MinimumConnectionsPerBucket && candidate.MetricConnections[metric] >= policy.MinimumConnectionsPerBucket
			}
		}
		if complete != policy.RequiredBuckets || min(len(connections[primary]), len(connections[edgeID])) < policy.Unified.MinimumConnectionsPerCohort {
			continue
		}
		comparison.Cohorts = append(comparison.Cohorts, cohort)
		for _, edge := range []string{primary, edgeID} {
			for identity := range connections[edge] {
				matched[edge][identity] = true
			}
		}
		allBetter, allWorse, guardsComplete = allBetter && better, allWorse && worse, guardsComplete && qualifiedGuards
	}
	sort.Strings(comparison.Cohorts)
	if len(comparison.Cohorts) == 0 {
		comparison.Reasons = append(comparison.Reasons, "no_complete_shared_cohort_windows")
		return comparison
	}
	comparison.CompleteBuckets = policy.RequiredBuckets
	comparison.MinimumConnections = min(len(matched[primary]), len(matched[edgeID]))
	comparison.CoverageBasisPoints = min(len(matched[primary])*10000/len(data.totals[primary]), len(matched[edgeID])*10000/len(data.totals[edgeID]))
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
	if !guardsComplete {
		comparison.Reasons = append(comparison.Reasons, "qualified_rtt_with_other_metrics_incomplete")
	}
	comparison.PromotionQualified = comparison.Verdict == "better" && comparison.MinimumConnections >= policy.Unified.PromotionMinimumConnections && comparison.CoverageBasisPoints >= policy.Unified.PromotionMinimumCoverageBasisPoints
	return comparison
}

func unifiedWindowBetter(candidate, baseline BusinessExplorationWindow, policy model.BusinessEdgeExplorationPolicy) bool {
	if candidate.RTTP90MS > baseline.RTTP90MS*(1+policy.MaximumRTTRegressionRatio) {
		return false
	}
	rttAdvantage := baseline.RTTMedianMS-candidate.RTTMedianMS >= policy.Unified.RTTAdvantageMS && candidate.RTTMedianMS <= baseline.RTTMedianMS*(1-policy.RTTAdvantageRatio)
	deliveryAdvantage := false
	if min(candidate.MetricConnections["delivery"], baseline.MetricConnections["delivery"]) >= policy.MinimumConnectionsPerBucket {
		if candidate.DeliveryP10 < baseline.DeliveryP10*(1-policy.MaximumDeliveryRegressionRatio) {
			return false
		}
		deliveryAdvantage = candidate.DeliveryBPS >= baseline.DeliveryBPS*(1+policy.DeliveryAdvantageRatio)
	}
	if min(candidate.MetricConnections["retransmission"], baseline.MetricConnections["retransmission"]) >= policy.MinimumConnectionsPerBucket && candidate.RetransmitP90 > baseline.RetransmitP90+policy.MaximumRetransmissionIncrease {
		return false
	}
	return rttAdvantage || deliveryAdvantage
}

func unifiedComparisonExpiry(input BusinessExplorationInput, comparison BusinessExplorationComparison, data unifiedWindows, primary string) time.Time {
	latest := input.CapturedAt.Unix()/int64(input.Policy.BucketSeconds)*int64(input.Policy.BucketSeconds) - int64(input.Policy.BucketSeconds)
	expires := input.Policy.ExpiresAt
	for _, window := range data.windows {
		if (window.EdgeID == primary || window.EdgeID == comparison.EdgeID) && window.StartedAt.Unix() == latest && slices.Contains(comparison.Cohorts, window.Cohort) {
			expires = minBusinessTime(expires, window.LastObservedAt.Add(time.Duration(input.Policy.EvidenceMaxAgeSeconds)*time.Second))
		}
	}
	return expires
}
