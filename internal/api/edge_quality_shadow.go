package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
	"k8s.io/apimachinery/pkg/util/validation"
)

func (s *Server) handleGetEdgeQualityShadow(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect edge quality shadow")
		return
	}
	hostname := normalizeExternalAppDomain(r.PathValue("hostname"))
	trafficClass := normalizeEdgeTrafficClass(r.URL.Query().Get("traffic_class"))
	scope, err := parseEdgeQualityRankScope(r.URL.Query().Get("scope"))
	dnsNodeID := strings.TrimSpace(r.URL.Query().Get("dns_node_id"))
	if err != nil || hostname == "" || trafficClass == "" || dnsNodeID != "" && len(validation.IsDNS1123Subdomain(dnsNodeID)) != 0 {
		httpx.WriteError(w, http.StatusBadRequest, "hostname, explicit traffic_class and valid scope required")
		return
	}
	if dnsNodeID != "" && !mustPrincipal(r).HasScope("artifact.read") {
		httpx.WriteError(w, http.StatusForbidden, "artifact.read scope required to bind actual DNS evidence")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	receipt, err := s.capturePhysicalQuality(ctx, hostname, trafficClass, scope, dnsNodeID, edgequality.DefaultNetworkPolicy())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	httpx.WriteJSON(w, http.StatusOK, receipt)
}

func (s *Server) capturePhysicalQuality(ctx context.Context, hostname, trafficClass string, scope edgeQualityRankScope, dnsNodeID string, networkPolicy edgequality.Policy) (edgequality.Receipt, error) {
	now := time.Now().UTC()
	snapshot := edgequality.Snapshot{Schema: edgequality.Schema, CapturedAt: now, Hostname: hostname, TrafficClass: trafficClass,
		Scope: scope.key(), Policy: networkPolicy, Candidates: []edgequality.Candidate{}, Observations: []edgequality.Observation{},
		Blockers: []string{"actual_dns_receipt_not_bound"}, Limitations: []string{"dns_resolver_scope_is_not_terminal_path", "common_tcp_cohorts_do_not_cover_every_terminal", "node_capacity_is_not_link_or_application_capacity", "uncertainty_budget_is_not_statistical_confidence"}}
	nodes, _, err := s.store.ListActiveEdgeNodes("")
	if err != nil {
		return edgequality.Receipt{}, err
	}
	var policy model.EdgeRoutePolicy
	if loaded, loadErr := s.store.GetEdgeRoutePolicy(hostname); loadErr == nil {
		policy = loaded
	} else if !errors.Is(loadErr, store.ErrNotFound) {
		return edgequality.Receipt{}, loadErr
	}
	quarantine := s.activeNodeQuarantineByName()
	for _, node := range nodes {
		legacy := edgeQualityRankCandidateForNode(node, policy, now, quarantine)
		candidate := edgequality.Candidate{EdgeID: legacy.EdgeID, EdgeGroupID: legacy.EdgeGroupID, HardGates: []string{}}
		if edgeQualityRankCandidateHardGated(legacy) {
			candidate.HardGates = append(candidate.HardGates, firstNonEmpty(legacy.ExclusionReason, legacy.Reason, edgeQualityRankGateReason(legacy)))
		}
		snapshot.Candidates = append(snapshot.Candidates, candidate)
	}
	networkSamples, err := s.store.ListEdgeNetworkSamples(ctx, hostname, now.Add(-time.Duration(snapshot.Policy.WindowSeconds)*time.Second), edgequality.MaxObservations)
	if err != nil {
		return edgequality.Receipt{}, err
	}
	for _, sample := range networkSamples {
		if sample.TrafficClass == trafficClass {
			snapshot.NetworkSamples = append(snapshot.NetworkSamples, sample)
		}
	}
	if len(networkSamples) == edgequality.MaxObservations {
		snapshot.Blockers = append(snapshot.Blockers, "network_observation_limit_reached")
	}
	witnesses, err := s.store.ListEdgeNetworkRouteWitnesses(ctx, hostname, now.Add(-time.Duration(snapshot.Policy.WindowSeconds)*time.Second), 256)
	if err != nil {
		return edgequality.Receipt{}, err
	}
	if len(witnesses) == 256 {
		snapshot.Blockers = append(snapshot.Blockers, "route_witness_limit_reached")
	}
	for _, witness := range witnesses {
		if len(snapshot.NetworkSamples) == edgequality.MaxObservations {
			snapshot.Blockers = append(snapshot.Blockers, "network_observation_limit_reached")
			break
		}
		if witness.TrafficClass == trafficClass {
			snapshot.NetworkSamples = append(snapshot.NetworkSamples, witness)
		}
	}
	limitReached := errors.New("shadow observation limit reached")
	visited := 0
	err = s.store.WalkEdgePerformanceSamples(ctx, hostname, now.Add(-time.Duration(snapshot.Policy.WindowSeconds)*time.Second), func(sample model.EdgePerformanceSample) error {
		visited++
		if visited > edgequality.MaxObservations*4 {
			return limitReached
		}
		if normalizeEdgeTrafficClass(sample.TrafficClass) != trafficClass || !edgeQualitySampleMatchesScope(sample, scope) {
			return nil
		}
		if len(snapshot.Observations) >= edgequality.MaxObservations {
			return limitReached
		}
		snapshot.Observations = append(snapshot.Observations, legacyPhysicalEdgeObservation(sample, scope.key()))
		return nil
	})
	if errors.Is(err, limitReached) {
		snapshot.Blockers = append(snapshot.Blockers, "observation_limit_reached")
	} else if err != nil {
		return edgequality.Receipt{}, err
	}
	if dnsNodeID != "" {
		decisions, readErr := s.readPlatformDNSDecisions(ctx, dnsNodeID, hostname, "", 1)
		if readErr != nil || len(decisions.Snapshot.Receipts) != 1 {
			snapshot.Blockers = append(snapshot.Blockers, "actual_dns_backend_unavailable")
		} else {
			s.captureQualityCapacity(ctx, &snapshot, decisions.Snapshot.Receipts[0], nodes)
			snapshot.CapturedAt = time.Now().UTC()
			if err := bindPhysicalQualityDNS(&snapshot, decisions.Snapshot.Receipts[0]); err != nil {
				snapshot.Blockers = append(snapshot.Blockers, "actual_dns_evidence_unbound")
			}
		}
	}
	return edgequality.Capture(snapshot)
}

func bindPhysicalQualityDNS(snapshot *edgequality.Snapshot, receipt dnsserver.DNSDecisionReceipt) error {
	evidence, err := dnsserver.QualityEvidenceFromDNSDecision(receipt, snapshot.CapturedAt, time.Duration(snapshot.Policy.EvidenceMaxAgeSeconds)*time.Second)
	if err != nil {
		return err
	}
	if evidence.Hostname != snapshot.Hostname || evidence.Scope != snapshot.Scope {
		return errors.New("actual DNS answer scope differs from shadow")
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	snapshot.ActualDNSReceipt = raw
	bindPhysicalQualityEvidence(snapshot, evidence)
	return nil
}

func bindPhysicalQualityEvidence(snapshot *edgequality.Snapshot, evidence dnsserver.QualityAnswerEvidence) {
	snapshot.CurrentEdgeID = evidence.EdgeID
	snapshot.LastSwitchAt = nil
	if evidence.PrimarySince != nil {
		primarySince := *evidence.PrimarySince
		snapshot.LastSwitchAt = &primarySince
	}
	blockers := []string{}
	for _, blocker := range snapshot.Blockers {
		if blocker != "actual_dns_receipt_not_bound" {
			blockers = append(blockers, blocker)
		}
	}
	snapshot.Blockers = blockers
	witnesses := map[string][]model.EdgeNetworkSample{}
	for _, sample := range snapshot.NetworkSamples {
		if sample.Source == "route_tls_witness_v1" && !sample.ObservedAt.After(snapshot.CapturedAt) && model.ValidateEdgeNetworkSample(sample) == nil {
			key := sample.EdgeID + "\x00" + sample.BundleVersion
			witnesses[key] = append(witnesses[key], sample)
		}
	}
	for index := range snapshot.Candidates {
		candidate := &snapshot.Candidates[index]
		proofs := []dnsserver.QualityRouteProof{}
		for _, proof := range evidence.Proofs {
			if proof.EdgeID == candidate.EdgeID && proof.EdgeGroupID == candidate.EdgeGroupID && proof.Hostname == snapshot.Hostname {
				proofs = append(proofs, proof)
			}
		}
		if len(proofs) != 1 {
			candidate.HardGates = append(candidate.HardGates, "exact_service_route_proof_not_unique")
			continue
		}
		proof := proofs[0]
		candidate.RouteGeneration = proof.Proof.Digest
		candidate.RouteProofVerified = true
		checkedAt := proof.Proof.CheckedAt
		candidate.ProofObservedAt = &checkedAt
		for _, sample := range snapshot.NetworkSamples {
			if model.ValidateEdgeNetworkSample(sample) != nil || sample.EdgeID != candidate.EdgeID || sample.EdgeGroupID != candidate.EdgeGroupID ||
				sample.Hostname != proof.Hostname || sample.PathPrefix != proof.Path || sample.TrafficClass != snapshot.TrafficClass ||
				sample.RouteDigest != proof.Proof.Digest || sample.ObservedAt.After(snapshot.CapturedAt) {
				continue
			}
			if sample.Source == "route_tls_witness_v1" {
				if observation, ok := physicalQualityCapacityObservation(sample, snapshot.Scope, snapshot.CapturedAt); ok {
					if len(snapshot.Observations) >= edgequality.MaxObservations {
						snapshot.Blockers = append(snapshot.Blockers, "observation_limit_reached")
						break
					}
					snapshot.Observations = append(snapshot.Observations, observation)
				}
				continue
			}
			witnessID := ""
			if sample.BundleVersion != proof.Proof.Version {
				for _, witness := range witnesses[sample.EdgeID+"\x00"+sample.BundleVersion] {
					if model.EdgeNetworkWitnessMatches(sample, witness) && (witnessID == "" || witness.ID < witnessID) {
						witnessID = witness.ID
					}
				}
				if witnessID == "" {
					continue
				}
			}
			if len(snapshot.Observations) >= edgequality.MaxObservations {
				snapshot.Blockers = append(snapshot.Blockers, "observation_limit_reached")
				break
			}
			observation := edgequality.Observation{ID: "network:" + sample.EdgeID + ":" + sample.ID, EdgeID: sample.EdgeID,
				Hostname: sample.Hostname, TrafficClass: sample.TrafficClass, Scope: snapshot.Scope, RouteGeneration: sample.RouteDigest,
				ObservedAt: sample.ObservedAt, RouteWitnessID: witnessID}
			if sample.Source == "service_endpoint_tcp_info_v1" {
				observation.ServiceNetworkMS, observation.ServiceSource = sample.ServiceRTTMS, "service_endpoint_tcp"
			} else if sample.Source == "public_front_tcp_info_v1" && sample.ClientNetwork != nil && (snapshot.Scope == "global" || snapshot.Scope == sample.ClientNetwork.Scope) {
				observation.ClientNetworkMS, observation.ClientSource = sample.ClientNetwork.RTTMS, "public_tcp_info"
				observation.ClientCohort = sample.ClientNetwork.Scope
			} else {
				continue
			}
			snapshot.Observations = append(snapshot.Observations, observation)
		}
		for _, sample := range snapshot.NodeCapacitySamples {
			if !physicalCapacityAddressMatches(sample, evidence) {
				continue
			}
			if observation, ok := edgequality.NodeCapacityObservation(*snapshot, *candidate, sample); ok {
				if len(snapshot.Observations) >= edgequality.MaxObservations {
					snapshot.Blockers = append(snapshot.Blockers, "observation_limit_reached")
					break
				}
				snapshot.Observations = append(snapshot.Observations, observation)
			}
		}
	}
}

func physicalQualityCapacityObservation(sample model.EdgeNetworkSample, scope string, now time.Time) (edgequality.Observation, bool) {
	if sample.Source != "route_tls_witness_v1" || sample.RouteWitness == nil || sample.RouteWitness.NodeCapacity == nil || model.ValidateEdgeNetworkSample(sample) != nil {
		return edgequality.Observation{}, false
	}
	capacity := sample.RouteWitness.NodeCapacity
	value, err := model.EdgeNetworkNodeUtilization(capacity)
	if err != nil || capacity.CPUObservedAt.After(now) || capacity.MemoryObservedAt.After(now) {
		return edgequality.Observation{}, false
	}
	return edgequality.Observation{ID: "capacity:" + sample.EdgeID + ":" + sample.ID, EdgeID: sample.EdgeID, Hostname: sample.Hostname, TrafficClass: sample.TrafficClass,
		Scope: scope, RouteGeneration: sample.RouteDigest, ObservedAt: capacity.ObservedAt, RouteWitnessID: sample.ID,
		CapacitySource: capacity.Source, CapacityUtilization: &value}, true
}

func legacyPhysicalEdgeObservation(sample model.EdgePerformanceSample, scope string) edgequality.Observation {
	return edgequality.Observation{ID: sample.ID, EdgeID: strings.TrimSpace(sample.EdgeID), Hostname: sample.Hostname,
		TrafficClass: normalizeEdgeTrafficClass(sample.TrafficClass), Scope: scope, RouteGeneration: sample.RouteGeneration,
		ObservedAt: sample.SampledAt, ClientSource: "legacy_unverified", ServiceSource: "legacy_unverified",
		Diagnostics: map[string]float64{
			"request_count": float64(sample.SampleCount), "http_error_count": float64(sample.ErrorCount),
			"ttfb_ms": float64(sample.TTFBMS), "upstream_ms": float64(sample.UpstreamMS), "total_ms": float64(sample.TotalMS),
			"origin_response_wait_ms": float64(sample.OriginResponseWaitMS), "origin_connect_ms": float64(sample.OriginConnectMS),
			"origin_endpoint_connect_ms": float64(sample.OriginEndpointConnectMS), "origin_total_ms": float64(sample.OriginTotalMS),
			"client_tcp_rtt_ms": sample.ClientTCPRTTMS, "client_tcp_retrans_rate": sample.ClientTCPRetransRate,
			"client_tcp_rto_rate": sample.ClientTCPRTORate, "upload_effective_bps": float64(sample.UploadEffectiveBPS),
			"response_egress_bps": float64(sample.ResponseEgressBPS), "active_requests": float64(sample.ActiveRequests),
			"streaming_request_count": float64(sample.StreamingRequestCount), "client_cancel_count": float64(sample.ClientCancelCount),
		}}
}
