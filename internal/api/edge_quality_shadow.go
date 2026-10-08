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
	now := time.Now().UTC()
	snapshot := edgequality.Snapshot{Schema: edgequality.Schema, CapturedAt: now, Hostname: hostname, TrafficClass: trafficClass,
		Scope: scope.key(), Policy: edgequality.DefaultShadowPolicy(), Candidates: []edgequality.Candidate{}, Observations: []edgequality.Observation{},
		Blockers: []string{"legacy_samples_lack_segment_provenance", "capacity_limit_unknown", "actual_dns_receipt_not_bound", "client_scope_not_terminal_path"}}
	nodes, _, err := s.store.ListActiveEdgeNodes("")
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	var policy model.EdgeRoutePolicy
	if loaded, loadErr := s.store.GetEdgeRoutePolicy(hostname); loadErr == nil {
		policy = loaded
	} else if !errors.Is(loadErr, store.ErrNotFound) {
		s.writeStoreError(w, loadErr)
		return
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
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	networkSamples, err := s.store.ListEdgeNetworkSamples(ctx, hostname, now.Add(-time.Duration(snapshot.Policy.WindowSeconds)*time.Second), edgequality.MaxObservations)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	for _, sample := range networkSamples {
		if sample.TrafficClass == trafficClass {
			snapshot.NetworkSamples = append(snapshot.NetworkSamples, sample)
		}
	}
	if len(networkSamples) == edgequality.MaxObservations {
		snapshot.Blockers = append(snapshot.Blockers, "network_observation_limit_reached")
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
		s.writeStoreError(w, err)
		return
	}
	if dnsNodeID != "" {
		decisions, readErr := s.readPlatformDNSDecisions(ctx, dnsNodeID, hostname, "", 1)
		if readErr != nil || len(decisions.Snapshot.Receipts) != 1 {
			snapshot.Blockers = append(snapshot.Blockers, "actual_dns_backend_unavailable")
		} else if err := bindPhysicalQualityDNS(&snapshot, decisions.Snapshot.Receipts[0]); err != nil {
			snapshot.Blockers = append(snapshot.Blockers, "actual_dns_evidence_unbound")
		}
	}
	receipt, err := edgequality.Capture(snapshot)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "shadow snapshot evaluation failed")
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	httpx.WriteJSON(w, http.StatusOK, receipt)
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
	blockers := []string{}
	for _, blocker := range snapshot.Blockers {
		if blocker != "actual_dns_receipt_not_bound" {
			blockers = append(blockers, blocker)
		}
	}
	snapshot.Blockers = blockers
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
				sample.RouteDigest != proof.Proof.Digest || sample.BundleVersion != proof.Proof.Version || sample.ObservedAt.After(snapshot.CapturedAt) {
				continue
			}
			if len(snapshot.Observations) >= edgequality.MaxObservations {
				snapshot.Blockers = append(snapshot.Blockers, "observation_limit_reached")
				break
			}
			snapshot.Observations = append(snapshot.Observations, edgequality.Observation{ID: "origin:" + sample.EdgeID + ":" + sample.ID, EdgeID: sample.EdgeID,
				Hostname: sample.Hostname, TrafficClass: sample.TrafficClass, Scope: snapshot.Scope, RouteGeneration: sample.RouteDigest,
				ObservedAt: sample.ObservedAt, ServiceNetworkMS: sample.ServiceRTTMS, ServiceSource: "service_endpoint_tcp"})
		}
	}
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
