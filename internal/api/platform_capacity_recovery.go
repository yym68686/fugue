package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"time"

	"fugue/internal/clientmeasurement"
	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/routeprobe"
)

type capacityRecoveryResult struct {
	codeDetail string
	codePlan   *model.EdgeCodeRecoveryPlan
	evidence   *platformproducer.CapacityRecoveryEvidence
	validUntil time.Time
	reason     string
}

func (s *Server) probeCapacityRecovery(parent context.Context, hold platformproducer.OfflineEdge, holds []platformproducer.OfflineEdge) capacityRecoveryResult {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	fail := func(reason string) capacityRecoveryResult { return capacityRecoveryResult{reason: reason} }
	now := time.Now().UTC()
	node, _, err := s.store.GetEdgeNode(hold.EdgeID)
	if err != nil || node.EdgeGroupID != hold.EdgeGroupID || node.PublicIPv4 != hold.PublicIPv4 || !edgeNodeRouteServingCapable(node, now) || node.Draining {
		return fail("route_unavailable")
	}
	readCode := s.offlineCapacityCode
	if readCode == nil {
		readCode = s.readCapacityCode
	}
	before, err := readCode(ctx, node)
	if err != nil {
		return capacityCodeFailureResult(err)
	}
	if !before.Valid() {
		return capacityCodeFailureResult(codeObservationError("code_identity_conflict", "code_binding"))
	}
	probe := s.offlineEdgeRecoveryProbe
	if probe == nil {
		probe = routeprobe.Probe
	}
	getProof := func(node model.EdgeNode) (routeprobe.Proof, error) {
		started := time.Now().UTC()
		proof, err := probe(ctx, hold.Recovery.Hostname, hold.Recovery.Path, node.PublicIPv4, "", time.Duration(hold.Recovery.ProbeTimeoutSeconds)*time.Second)
		checked := time.Now().UTC()
		if err != nil || proof.EdgeID != node.ID || proof.GroupID != node.EdgeGroupID || proof.State != "" || !platformproducer.ValidDigest(proof.Digest) || proof.Version == "" || proof.CheckedAt.Before(started) || proof.CheckedAt.After(checked) || !proof.ValidUntil.After(checked) {
			return routeprobe.Proof{}, errors.New("fresh route proof unavailable")
		}
		return proof, nil
	}
	proof, err := getProof(node)
	if err != nil {
		return fail("route_unavailable")
	}
	var reference model.EdgeNode
	var referenceProof routeprobe.Proof
	for _, id := range hold.Recovery.Capacity.ReferenceEdgeIDs {
		if ctx.Err() != nil {
			break
		}
		// A recovery subject cannot serve as another subject's control sample.
		if slices.ContainsFunc(holds, func(h platformproducer.OfflineEdge) bool { return h.EdgeID == id }) {
			continue
		}
		candidate, _, err := s.store.GetEdgeNode(id)
		if err != nil || candidate.ID == node.ID || candidate.PublicIPv4 == node.PublicIPv4 || !edgeNodeRouteServingCapable(candidate, time.Now().UTC()) || candidate.Draining {
			continue
		}
		p, err := getProof(candidate)
		if err == nil {
			reference, referenceProof = candidate, p
			break
		}
	}
	if reference.ID == "" {
		return fail("reference_unavailable")
	}
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		return fail("evidence_unavailable")
	}
	encodedSeed := hex.EncodeToString(seed)
	targets := []string{node.ID, reference.ID}
	slices.Sort(targets)
	plan := model.EdgeClientProbePlan{Schema: "fugue.client-probe-plan/v1", RoundID: model.NewID("capacity_recovery"), ObserverLabel: "platform-capacity-recovery"}
	now = time.Now().UTC()
	for i, n := range []model.EdgeNode{node, reference} {
		p := []routeprobe.Proof{proof, referenceProof}[i]
		permit, err := clientmeasurement.SignPermit(model.EdgeClientProbePermit{Schema: "fugue.client-probe-permit/v1", RoundID: plan.RoundID, AttemptID: model.NewID("capacity_attempt"), ObserverID: platformproducer.OfflineRecoveryDigest(hold), Hostname: hold.Recovery.Hostname, Path: hold.Recovery.Path, TrafficClass: hold.Recovery.Capacity.TrafficClass, EdgeID: n.ID, EdgeGroupID: n.EdgeGroupID, Address: n.PublicIPv4, RouteDigest: p.Digest, BundleVersion: p.Version, IssuedAt: now, ExpiresAt: now.Add(time.Minute), BodyBytes: clientmeasurement.BodyBytes, BodySeed: encodedSeed, BodySHA256: clientmeasurement.Digest(clientmeasurement.Payload(encodedSeed)), TargetEdgeIDs: targets}, s.clientProbeKeys())
		if err != nil || clientmeasurement.ValidatePermit(permit) != nil {
			return fail("evidence_unavailable")
		}
		plan.Permits = append(plan.Permits, permit)
	}
	measure := s.offlineCapacityMeasure
	if measure == nil {
		measure = clientmeasurement.MeasureRound
	}
	measureCtx, stop := context.WithTimeout(ctx, time.Duration(hold.Recovery.Capacity.MaxElapsedMilliseconds)*time.Millisecond)
	report, err := measure(measureCtx, plan)
	stop()
	if err != nil || !reflect.DeepEqual(report.Plan, plan) {
		return fail("capacity_unavailable")
	}
	after, err := readCode(ctx, node)
	if err != nil {
		return capacityCodeFailureResult(err)
	}
	if before != after {
		return capacityCodeFailureResult(codeObservationError("code_identity_conflict", "observation_changed"))
	}
	checked := time.Now().UTC()
	evidence, reason := s.evaluateCapacityRound(hold, reference.ID, report, before, checked)
	// Retain a bounded audit ring separately from the compact signed projection;
	// raw reports never enter every DNS consumer's retained configuration graph.
	if reason == "capacity_verified" || reason == "capacity_below_policy" {
		if err := s.retainCapacityRound(ctx, hold, report, before, checked); err != nil {
			return fail("evidence_unavailable")
		}
	}
	validUntil := proof.ValidUntil
	if referenceProof.ValidUntil.Before(validUntil) {
		validUntil = referenceProof.ValidUntil
	}
	if !validUntil.After(checked) {
		return fail("route_unavailable")
	}
	return capacityRecoveryResult{evidence: evidence, validUntil: validUntil, reason: reason}
}

func (s *Server) evaluateCapacityRound(hold platformproducer.OfflineEdge, reference string, report model.EdgeClientProbeReport, code platformproducer.CapacityCodeIdentity, at time.Time) (*platformproducer.CapacityRecoveryEvidence, string) {
	if hold.Recovery == nil || hold.Recovery.Capacity == nil || !code.Valid() || len(report.Plan.Permits) != 2 || report.Plan.Permits[0].Address == report.Plan.Permits[1].Address {
		return nil, "capacity_unavailable"
	}
	if _, err := clientmeasurement.ValidateReport(report); err != nil {
		return nil, "capacity_unavailable"
	}
	keys := s.clientProbeKeys()
	var held *platformproducer.CapacityRecoveryEvidence
	raw, _ := json.Marshal(report)
	result := "capacity_verified"
	for _, permit := range report.Plan.Permits {
		if permit.ObserverID != platformproducer.OfflineRecoveryDigest(hold) || permit.Hostname != hold.Recovery.Hostname || permit.Path != hold.Recovery.Path || permit.TrafficClass != hold.Recovery.Capacity.TrafficClass || (permit.EdgeID != hold.EdgeID && permit.EdgeID != reference) || clientmeasurement.VerifyPermit(permit, keys, at) != nil {
			return nil, "capacity_unavailable"
		}
		i := slices.IndexFunc(report.Outcomes, func(o model.EdgeClientProbeOutcome) bool { return o.AttemptID == permit.AttemptID })
		outcome := report.Outcomes[i]
		if outcome.StartedAt.Before(permit.IssuedAt) || outcome.CompletedAt.After(at) || outcome.Attestation != nil && clientmeasurement.VerifyAttestation(*outcome.Attestation, permit, keys) != nil {
			return nil, "capacity_unavailable"
		}
		if outcome.Failure != "" || outcome.HTTPStatus != 200 || outcome.Attestation == nil {
			result = "capacity_below_policy"
			continue
		}
		elapsed := outcome.CompletedAt.Sub(outcome.StartedAt).Seconds() * 1000
		e := &platformproducer.CapacityRecoveryEvidence{CheckedAt: at, ReportDigest: clientmeasurement.Digest(raw), ReferenceEdgeID: reference, Bytes: outcome.BytesReceived, ElapsedMilliseconds: elapsed, BytesPerSecond: float64(outcome.BytesReceived) * 1000 / elapsed, Code: code, RouteDigest: permit.RouteDigest}
		if !e.Passes(*hold.Recovery.Capacity, hold.EdgeID) {
			result = "capacity_below_policy"
		}
		if permit.EdgeID == hold.EdgeID {
			backend := outcome.Attestation.ClientNetwork.Backend
			if permit.EdgeGroupID != hold.EdgeGroupID || permit.Address != hold.PublicIPv4 || backend.PodUID != code.FrontPodUID || backend.ServiceUID != code.ServiceUID || outcome.Attestation.ClientNetwork.Slot != code.Slot {
				return nil, "code_not_current"
			}
			held = e
		}
	}
	if result != "capacity_verified" || held == nil {
		return nil, "capacity_below_policy"
	}
	return held, result
}

type retainedCapacityRound struct {
	CheckedAt time.Time                             `json:"checked_at"`
	Code      platformproducer.CapacityCodeIdentity `json:"code"`
	Report    model.EdgeClientProbeReport           `json:"report"`
}

func (s *Server) retainCapacityRound(ctx context.Context, hold platformproducer.OfflineEdge, report model.EdgeClientProbeReport, code platformproducer.CapacityCodeIdentity, at time.Time) error {
	locked, err := s.store.UpdateObservationCheckpoint(ctx, platformproducer.OfflineRecoveryKey(hold)+"/capacity-rounds", func(raw []byte) ([]byte, error) {
		var rounds []retainedCapacityRound
		if len(raw) > 1<<20 || len(raw) > 0 && json.Unmarshal(raw, &rounds) != nil || len(rounds) > 8 {
			return nil, errors.New("capacity evidence history invalid")
		}
		if len(rounds) == 8 {
			rounds = rounds[1:]
		}
		rounds = append(rounds, retainedCapacityRound{CheckedAt: at, Code: code, Report: report})
		encoded, err := json.Marshal(rounds)
		if err != nil || len(encoded) > 1<<20 {
			return nil, errors.New("capacity evidence history exceeds bound")
		}
		return encoded, nil
	})
	if err == nil && !locked {
		return errors.New("capacity evidence history busy")
	}
	return err
}
