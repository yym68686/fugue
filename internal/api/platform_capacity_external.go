package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"time"

	"fugue/internal/clientmeasurement"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func externalCapacityRecoveryEnabled() bool {
	return os.Getenv("FUGUE_EDGE_CAPACITY_OBSERVER") == "external"
}

type externalCapacityRound struct {
	Round        preparedCapacityRecovery                     `json:"round"`
	Observer     string                                       `json:"observer"`
	ClaimedAt    time.Time                                    `json:"claimed_at"`
	Previous     platformproducer.OfflineRecoveryObservation  `json:"previous"`
	ReportDigest string                                       `json:"report_digest,omitempty"`
	Result       string                                       `json:"result,omitempty"`
	Completed    *platformproducer.OfflineRecoveryObservation `json:"completed,omitempty"`
}

// Resolve authority afresh on both sides of a round. A pending plan is an
// observation capability, never a replacement for the current signed policy.
func (s *Server) externalCapacityHolds(ctx context.Context) ([]platformproducer.OfflineEdge, error) {
	if !externalCapacityRecoveryEnabled() {
		return nil, errors.New("external capacity observation is not configured")
	}
	artifact, _, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil || !found {
		return nil, errors.New("active recovery policy unavailable")
	}
	policy, err := platformproducer.Decode(artifact)
	if err != nil || artifact.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(artifact) != nil || !platformsafety.EvaluateArtifactIntegrity(artifact, s.bundleKeyring()).Pass {
		return nil, errors.New("signed recovery policy unavailable")
	}
	if policy.Mode == "paused" {
		return nil, nil
	}
	if policy.TargetScope != "global" || policy.DNSPolicyArtifactID == "" {
		return nil, nil
	}
	source, err := s.store.GetPlatformArtifact(policy.DNSPolicyArtifactID)
	if err != nil || source.ContentHash != policy.DNSPolicyDigest || source.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(source) != nil || !platformsafety.EvaluateArtifactIntegrity(source, s.bundleKeyring()).Pass {
		return nil, errors.New("signed recovery input unavailable")
	}
	static, err := s.loadStaticPlatformIntent(policy.StaticIntentArtifactID, policy.StaticIntentDigest)
	if err != nil {
		return nil, err
	}
	input, err := platformproducer.DecodeProjectionPolicy(source, static.Consumers, policy.HostedZoneTemplates)
	if err != nil {
		return nil, err
	}
	for _, hold := range input.OfflineEdges {
		if hold.Recovery == nil || hold.Recovery.Capacity == nil {
			continue
		}
		authorized := false
		for _, route := range static.Routes {
			if route.Hostname == hold.Recovery.Hostname && route.Status != model.EdgeRouteStatusDisabled && model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) && hold.Recovery.Capacity.TrafficClass == "streaming" {
				authorized = true
			}
		}
		if !authorized {
			return nil, errors.New("recovery target is not an enabled pinned static route")
		}
	}
	return input.OfflineEdges, nil
}

func (s *Server) claimExternalCapacity(ctx context.Context, hold platformproducer.OfflineEdge, holds []platformproducer.OfflineEdge, observer string) (*preparedCapacityRecovery, error) {
	now := time.Now().UTC()
	var previous platformproducer.OfflineRecoveryObservation
	claimed := false
	key := platformproducer.OfflineRecoveryKey(hold)
	locked, err := s.store.UpdateObservationCheckpoint(ctx, key, func(raw []byte) ([]byte, error) {
		var current platformproducer.OfflineRecoveryObservation
		if len(raw) > 4096 || len(raw) > 0 && json.Unmarshal(raw, &current) != nil {
			return nil, errors.New("recovery checkpoint invalid")
		}
		if !current.LastAttempt.IsZero() && now.Sub(current.LastAttempt) < time.Duration(hold.Recovery.ProbeIntervalSeconds)*time.Second {
			return nil, nil
		}
		previous = current
		current.LastAttempt = now
		claimed = true
		return json.Marshal(current)
	})
	if err != nil || !locked || !claimed {
		return nil, err
	}
	round, failure := s.prepareCapacityRecovery(ctx, hold, holds)
	if round == nil {
		return nil, s.completeExternalCapacity(ctx, hold, externalCapacityRound{ClaimedAt: now, Previous: previous}, failure)
	}
	packet := externalCapacityRound{Round: *round, Observer: observer, ClaimedAt: now, Previous: previous}
	encoded, _ := json.Marshal(packet)
	locked, err = s.store.UpdateObservationCheckpoint(ctx, key+"/external", func([]byte) ([]byte, error) { return encoded, nil })
	if err == nil && !locked {
		err = errors.New("external recovery claim busy")
	}
	return round, err
}

func (s *Server) completeExternalCapacity(ctx context.Context, hold platformproducer.OfflineEdge, pending externalCapacityRound, result capacityRecoveryResult) error {
	at := time.Now().UTC()
	if result.evidence != nil {
		at = result.evidence.CheckedAt
	}
	observed := pending.Previous.RecordCapacity(hold, at, result.reason == "capacity_verified" && result.evidence != nil, result.validUntil, result.evidence, result.reason)
	observed.CodeDetail, observed.CodePlan = result.codeDetail, result.codePlan
	if pending.Completed != nil {
		observed = *pending.Completed
	}
	locked, err := s.store.UpdateObservationCheckpoint(ctx, platformproducer.OfflineRecoveryKey(hold), func(raw []byte) ([]byte, error) {
		var current platformproducer.OfflineRecoveryObservation
		encoded, err := json.Marshal(observed)
		if err != nil {
			return nil, err
		}
		if string(raw) == string(encoded) {
			return nil, nil // Retrying an already completed report never adds a success.
		}
		if len(raw) > 4096 || json.Unmarshal(raw, &current) != nil || !current.LastAttempt.Equal(pending.ClaimedAt) {
			return nil, errors.New("external recovery claim superseded")
		}
		return encoded, nil
	})
	if err == nil && !locked {
		err = errors.New("recovery completion busy")
	}
	return err
}

func (s *Server) recordExternalCapacity(ctx context.Context, holds []platformproducer.OfflineEdge, observer string, report model.EdgeClientProbeReport) (string, error) {
	if len(report.Plan.Permits) != 2 {
		return "", errors.New("exact paired recovery report required")
	}
	var hold *platformproducer.OfflineEdge
	for i := range holds {
		h := &holds[i]
		if h.Recovery != nil && h.Recovery.Capacity != nil && platformproducer.OfflineRecoveryDigest(*h) == report.Plan.Permits[0].ObserverID {
			hold = h
		}
	}
	if hold == nil {
		return "", errors.New("recovery declaration no longer active")
	}
	key := platformproducer.OfflineRecoveryKey(*hold) + "/external"
	raw, err := s.store.ReadObservationCheckpoint(ctx, key)
	var pending externalCapacityRound
	if err != nil || len(raw) > 32768 || json.Unmarshal(raw, &pending) != nil || pending.Observer != observer || !reflect.DeepEqual(pending.Round.Plan, report.Plan) {
		return "", errors.New("report does not match this observer's recovery claim")
	}
	encoded, _ := json.Marshal(report)
	digest := clientmeasurement.Digest(encoded)
	if pending.ReportDigest != "" {
		if pending.ReportDigest == digest {
			return pending.Result, s.completeExternalCapacity(ctx, *hold, pending, capacityRecoveryResult{})
		}
		return "", errors.New("recovery claim already consumed")
	}
	if _, err := clientmeasurement.ValidateReport(report); err != nil {
		return "", err
	}
	now := time.Now().UTC()
	for _, permit := range report.Plan.Permits {
		if clientmeasurement.VerifyPermit(permit, s.clientProbeKeys(), now) != nil {
			return "", errors.New("recovery permit invalid or expired")
		}
	}
	result := s.finishCapacityRecovery(ctx, *hold, pending.Round, report)
	at := time.Now().UTC()
	if result.evidence != nil {
		at = result.evidence.CheckedAt
	}
	completed := pending.Previous.RecordCapacity(*hold, at, result.reason == "capacity_verified" && result.evidence != nil, result.validUntil, result.evidence, result.reason)
	completed.CodeDetail, completed.CodePlan = result.codeDetail, result.codePlan
	pending.Completed = &completed
	consumed := false
	locked, err := s.store.UpdateObservationCheckpoint(ctx, key, func(current []byte) ([]byte, error) {
		if string(current) != string(raw) {
			return nil, errors.New("recovery claim changed")
		}
		pending.ReportDigest, pending.Result = digest, result.reason
		consumed = true
		return json.Marshal(pending)
	})
	if err != nil || !locked || !consumed {
		return "", fmt.Errorf("recovery claim consumption failed: %v", err)
	}
	if err = s.completeExternalCapacity(ctx, *hold, pending, result); err != nil {
		return "", err
	}
	return result.reason, nil
}

func (s *Server) handleIssueEdgeRecoveryClientProbePlan(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() || !p.HasExplicitScope("edge.quality.observe") {
		httpx.WriteError(w, http.StatusForbidden, "explicit edge.quality.observe authority required")
		return
	}
	if r.ContentLength > 0 {
		httpx.WriteError(w, http.StatusBadRequest, "probe plan requests do not accept a body")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	holds, err := s.externalCapacityHolds(ctx)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	for _, hold := range holds {
		if hold.Recovery == nil || hold.Recovery.Capacity == nil {
			continue
		}
		round, err := s.claimExternalCapacity(ctx, hold, holds, clientProbeObserver(p))
		if err != nil {
			httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		if round != nil {
			w.Header().Set("Cache-Control", "private, no-store")
			httpx.WriteJSON(w, http.StatusOK, round.Plan)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleReportEdgeRecoveryClientProbeRound(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() || !p.HasExplicitScope("edge.quality.observe") {
		httpx.WriteError(w, http.StatusForbidden, "explicit edge.quality.observe authority required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 128<<10)
	var report model.EdgeClientProbeReport
	if httpx.DecodeJSON(r, &report) != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid bounded recovery report")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	holds, err := s.externalCapacityHolds(ctx)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	result, err := s.recordExternalCapacity(ctx, holds, clientProbeObserver(p), report)
	if err != nil {
		httpx.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"accepted": true, "recovery_result": result})
}
