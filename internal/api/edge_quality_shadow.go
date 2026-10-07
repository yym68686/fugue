package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handleGetEdgeQualityShadow(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect edge quality shadow")
		return
	}
	hostname := normalizeExternalAppDomain(r.PathValue("hostname"))
	trafficClass := normalizeEdgeTrafficClass(r.URL.Query().Get("traffic_class"))
	scope, err := parseEdgeQualityRankScope(r.URL.Query().Get("scope"))
	if err != nil || hostname == "" || trafficClass == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname, explicit traffic_class and valid scope required")
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
	receipt, err := edgequality.Capture(snapshot)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "shadow snapshot evaluation failed")
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	httpx.WriteJSON(w, http.StatusOK, receipt)
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
