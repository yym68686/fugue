package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/clientmeasurement"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/routeprobe"
)

func capacityRecoveryFixture(t *testing.T) (*Server, platformproducer.Policy, platformproducer.OfflineEdge, platformproducer.ProjectionPolicyInput) {
	t.Helper()
	s, p, h, input := offlineRecoveryFixture(t)
	h.Recovery.Capacity = &platformproducer.CapacityRecoveryPolicy{Version: platformproducer.CapacityRecoveryVersion, TrafficClass: "streaming", ReferenceEdgeIDs: []string{"edge-b"}, MinimumBytesPerSecond: 262144, MaxElapsedMilliseconds: 5000, MinimumStableSeconds: 600}
	input.OfflineEdges = []platformproducer.OfflineEdge{h}
	input.Generation += "-capacity"
	raw, _ := json.Marshal(input)
	var content map[string]any
	json.Unmarshal(raw, &content)
	source, err := s.store.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "global"}, Generation: input.Generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	source, err = s.store.ValidatePlatformArtifact(source.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	p.DNSPolicyArtifactID, p.DNSPolicyDigest = source.ID, source.ContentHash
	_, _, err = s.store.UpdateEdgeHeartbeat(model.EdgeNode{ID: "edge-b", EdgeGroupID: "edge-group-a", PublicIPv4: "9.9.9.9", Healthy: true, Status: model.EdgeHealthHealthy})
	if err != nil {
		t.Fatal(err)
	}
	s.offlineCapacityCode = func(context.Context, model.EdgeNode) (platformproducer.CapacityCodeIdentity, error) {
		return testCapacityCode(), nil
	}
	s.offlineEdgeRecoveryProbe = func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
		id := "edge-a"
		if address == "9.9.9.9" {
			id = "edge-b"
		}
		at := time.Now().UTC()
		return routeprobe.Proof{EdgeID: id, GroupID: "edge-group-a", Digest: "sha256:" + strings.Repeat("a", 64), Version: "bundle-a", CheckedAt: at, ValidUntil: at.Add(time.Hour)}, nil
	}
	return s, p, h, input
}

func capacityReport(t *testing.T, s *Server, plan model.EdgeClientProbePlan, started, completed time.Time) model.EdgeClientProbeReport {
	t.Helper()
	report := model.EdgeClientProbeReport{Schema: "fugue.client-probe-report/v1", Plan: plan}
	for _, permit := range plan.Permits {
		rtt := 30.0
		sample := model.EdgeClientNetworkSample{ConnectionID: "connection-" + permit.AttemptID, Slot: "a", Scope: "tcp_peer:203.0.113.0/24", StartedAt: started, ObservedAt: started, TCPInfoAvailable: true, RTTMS: &rtt, Backend: &model.EdgeClientNetworkBackend{Namespace: "system", PodName: "front", PodUID: "pod-a", PodVersion: "10", ServiceName: "public", ServiceUID: "service-a", ServiceVersion: "20", EndpointsDigest: "sha256:" + strings.Repeat("b", 64)}}
		attestation, err := clientmeasurement.SignAttestation(model.EdgeClientProbeAttestation{Schema: "fugue.client-probe-attestation/v1", AttemptID: permit.AttemptID, EdgeID: permit.EdgeID, EdgeGroupID: permit.EdgeGroupID, RouteDigest: permit.RouteDigest, BundleVersion: permit.BundleVersion, ObservedAt: sample.ObservedAt, ClientNetwork: sample}, s.clientProbeKeys())
		if err != nil {
			t.Fatal(err)
		}
		report.Outcomes = append(report.Outcomes, model.EdgeClientProbeOutcome{AttemptID: permit.AttemptID, StartedAt: started, CompletedAt: completed, BytesReceived: clientmeasurement.BodyBytes, BodySHA256: permit.BodySHA256, BodySeconds: completed.Sub(started).Seconds() / 2, HTTPStatus: 200, Attestation: &attestation})
	}
	return report
}

func TestCapacityRecoveryRoundRequiresBothSignaturesFullElapsedAndPhysicalIdentity(t *testing.T) {
	s, _, h, _ := capacityRecoveryFixture(t)
	raw := clientReportFixture(t, s)
	for i := range raw.Plan.Permits {
		p := raw.Plan.Permits[i]
		p.ObserverID = platformproducer.OfflineRecoveryDigest(h)
		p.Hostname = h.Recovery.Hostname
		p.Path = h.Recovery.Path
		p.EdgeGroupID = h.EdgeGroupID
		p.Address = h.PublicIPv4
		if p.EdgeID == "edge-b" {
			p.Address = "9.9.9.9"
		}
		raw.Plan.Permits[i], _ = clientmeasurement.SignPermit(p, s.clientProbeKeys())
	}
	for _, scenario := range []string{"valid", "forged_permit", "forged_attestation", "revoked_key", "foreign_observer", "wrong_class", "held_slow_headers", "reference_slow_headers", "partial_body", "foreign_front", "foreign_service", "wrong_socket_slot", "future", "duplicate_edge", "missing_reference", "reference_failed"} {
		t.Run(scenario, func(t *testing.T) {
			issued := raw.Plan.Permits[0].IssuedAt
			report := capacityReport(t, s, raw.Plan, issued.Add(time.Second), issued.Add(3*time.Second))
			code := testCapacityCode()
			revoked := s.bundleRevokedKeyIDs
			defer func() { s.bundleRevokedKeyIDs = revoked }()
			switch scenario {
			case "forged_permit":
				report.Plan.Permits[0].Signature = "forged"
			case "forged_attestation":
				report.Outcomes[1].Attestation.Signature = "forged"
			case "revoked_key":
				s.bundleRevokedKeyIDs = []string{report.Plan.Permits[0].KeyID}
			case "foreign_observer":
				report.Plan.Permits[0].ObserverID = "unrelated"
			case "wrong_class":
				report.Plan.Permits[0].TrafficClass = "dynamic_api"
			case "held_slow_headers":
				report.Outcomes[0].CompletedAt = issued.Add(8 * time.Second)
				report.Outcomes[0].BodySeconds = 0.001
			case "reference_slow_headers":
				report.Outcomes[1].CompletedAt = issued.Add(8 * time.Second)
				report.Outcomes[1].BodySeconds = 0.001
			case "partial_body":
				report.Outcomes[0].BytesReceived--
			case "foreign_front":
				code.FrontPodUID = "another-front"
			case "foreign_service":
				code.ServiceUID = "another-service"
			case "wrong_socket_slot":
				code.Slot = "b"
			case "future":
				report.Outcomes[0].CompletedAt = time.Now().Add(time.Second)
			case "duplicate_edge":
				report.Plan.Permits[1] = report.Plan.Permits[0]
			case "missing_reference":
				report.Outcomes = report.Outcomes[:1]
			case "reference_failed":
				report.Outcomes[1].Failure = "body"
			}
			e, reason := s.evaluateCapacityRound(h, "edge-b", report, code, time.Now().UTC())
			if (e != nil && reason == "capacity_verified") != (scenario == "valid") {
				t.Fatal(scenario, e, reason)
			}
			if e != nil && e.ElapsedMilliseconds != 2000 {
				t.Fatal("used body-only time for capacity", e)
			}
		})
	}
}

func TestCapacityRecoveryPersistsSignedRoundBeforeAdmittingAndRejectsCodeChange(t *testing.T) {
	for _, scenario := range []string{"valid", "code_changed", "reference_failed", "report_replaced", "evidence_corrupt", "deadline", "code_missing"} {
		t.Run(scenario, func(t *testing.T) {
			s, p, h, _ := capacityRecoveryFixture(t)
			code := testCapacityCode()
			now := time.Now().UTC()
			previous := platformproducer.OfflineRecoveryObservation{EdgeID: h.EdgeID, DeclarationDigest: platformproducer.OfflineRecoveryDigest(h), LastAttempt: now.Add(-6 * time.Minute), LastSuccess: now.Add(-6 * time.Minute), FirstSuccess: now.Add(-12 * time.Minute), ProofValidUntil: now.Add(time.Hour), Successes: 2, Capacity: &platformproducer.CapacityRecoveryEvidence{CheckedAt: now.Add(-6 * time.Minute), ReportDigest: "sha256:" + strings.Repeat("b", 64), ReferenceEdgeID: "edge-b", Bytes: 1 << 20, ElapsedMilliseconds: 2000, BytesPerSecond: 524288, Code: code, RouteDigest: "sha256:" + strings.Repeat("a", 64)}}
			if _, err := s.store.UpdateObservationCheckpoint(context.Background(), platformproducer.OfflineRecoveryKey(h), func([]byte) ([]byte, error) { return json.Marshal(previous) }); err != nil {
				t.Fatal(err)
			}
			calls, measureCalls := 0, 0
			s.offlineCapacityCode = func(context.Context, model.EdgeNode) (platformproducer.CapacityCodeIdentity, error) {
				calls++
				if scenario == "code_missing" {
					return code, errors.New("no current authority")
				}
				if scenario == "code_changed" && calls == 2 {
					code.WorkerPodUID = "replacement"
				}
				return code, nil
			}
			if scenario == "evidence_corrupt" {
				s.store.UpdateObservationCheckpoint(context.Background(), platformproducer.OfflineRecoveryKey(h)+"/capacity-rounds", func([]byte) ([]byte, error) { return []byte("invalid"), nil })
			}
			s.offlineCapacityMeasure = func(ctx context.Context, plan model.EdgeClientProbePlan) (model.EdgeClientProbeReport, error) {
				measureCalls++
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 5*time.Second {
					t.Error("missing bounded transfer deadline")
				}
				if scenario == "deadline" {
					return model.EdgeClientProbeReport{}, context.DeadlineExceeded
				}
				started := time.Now().UTC()
				time.Sleep(time.Millisecond)
				report := capacityReport(t, s, plan, started, time.Now().UTC())
				if scenario == "reference_failed" {
					report.Outcomes[1].Failure = "body"
				}
				if scenario == "report_replaced" {
					report.Plan.RoundID = "another-round"
				}
				return report, nil
			}
			if err := s.reconcileOfflineEdgeRecovery(context.Background(), p); err != nil {
				t.Fatal(err)
			}
			observed, err := s.readOfflineRecovery(context.Background(), h)
			if err != nil {
				t.Fatal(err)
			}
			if observed.Ready(h, time.Now().UTC()) != (scenario == "valid") {
				t.Fatal("incorrect readmission", scenario, observed)
			}
			if scenario == "code_missing" && measureCalls != 0 {
				t.Fatal("spent network budget without current code")
			}
			if scenario == "valid" {
				raw, err := s.store.ReadObservationCheckpoint(context.Background(), platformproducer.OfflineRecoveryKey(h)+"/capacity-rounds")
				var rounds []retainedCapacityRound
				if err != nil || json.Unmarshal(raw, &rounds) != nil || len(rounds) != 1 {
					t.Fatal("missing durable evidence", err)
				}
				encoded, _ := json.Marshal(rounds[0].Report)
				if clientmeasurement.Digest(encoded) != observed.Capacity.ReportDigest {
					t.Fatal("observation not bound to retained signed round")
				}
			} else if observed.Admitted || observed.Successes != 0 {
				t.Fatal("failure retained old progress", observed)
			}
			// A second replica/restart cannot immediately reuse the round.
			before := measureCalls
			if err := s.reconcileOfflineEdgeRecovery(context.Background(), p); err != nil || before != measureCalls {
				t.Fatal("probe interval replay", err)
			}
		})
	}
}

func TestOfflineRecoveryCorruptFactKeepsOnlySubjectHeld(t *testing.T) {
	s, _, h, input := offlineRecoveryFixture(t)
	s.store.UpdateObservationCheckpoint(context.Background(), platformproducer.OfflineRecoveryKey(h), func([]byte) ([]byte, error) { return []byte("broken"), nil })
	nodes := []model.EdgeNode{{ID: h.EdgeID, EdgeGroupID: h.EdgeGroupID, PublicIPv4: h.PublicIPv4}, {ID: "edge-b", EdgeGroupID: h.EdgeGroupID, PublicIPv4: "9.9.9.9"}}
	projection := platformIntentProjectionResponse{Policy: platformconfig.PolicySnapshot{Generation: "existing", Scope: "global"}}
	if err := s.projectDNSReadinessWithRecovery(context.Background(), &projection, nodes, time.Now().UTC(), &input); err != nil || len(projection.RuntimeSnapshot.DNSEdgeEndpoints) != 1 {
		t.Fatal("corrupt recovery fact blocked healthy member publication", err)
	}
}

func TestOfflineRecoveryWorkerDoesNotOwnConfigurationProducerLock(t *testing.T) {
	s, p, h, _ := offlineRecoveryFixture(t)
	p.SchemaVersion, p.Generation, p.Mode, p.InputSource, p.IntervalSeconds, p.RefreshSeconds = platformproducer.Schema, "independent-recovery", "shadow", "business-static-intent", 30, 300
	raw, _ := json.Marshal(p)
	var content map[string]any
	json.Unmarshal(raw, &content)
	a, err := s.store.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: platformproducer.Scope}, Generation: p.Generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	a, err = s.store.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err = s.store.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal()); err != nil {
		t.Fatal(err)
	}
	entered, done := make(chan struct{}), make(chan struct{})
	var probes atomic.Int32
	s.offlineEdgeRecoveryProbe = func(ctx context.Context, _, _, _, _ string, _ time.Duration) (routeprobe.Proof, error) {
		probes.Add(1)
		close(entered)
		<-ctx.Done()
		return routeprobe.Proof{}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { defer close(done); s.runOfflineEdgeRecovery(ctx) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("recovery worker did not start")
	}
	// The normal producer can enter its own election and reach capture while the
	// held endpoint is still hung. A capture sentinel avoids unrelated fixtures.
	sentinel := errors.New("reached independent publication capture")
	publicationCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	acquired, err := s.store.WithAdvisoryLock(publicationCtx, platformproducer.Actor, func() error {
		_, err := s.reconcilePlatformConfigurationWithCapture(publicationCtx, func(context.Context, model.Principal) (platformIntentProjectionResponse, error) {
			return platformIntentProjectionResponse{}, sentinel
		})
		return err
	})
	if !acquired || !errors.Is(err, sentinel) || probes.Load() != 1 {
		t.Fatal("offline member blocked configuration producer", acquired, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("recovery worker ignored cancellation")
	}
	o, err := s.readOfflineRecovery(context.Background(), h)
	if err != nil || o.Admitted {
		t.Fatal("canceled recovery admitted subject", o, err)
	}
}

func TestCapacityCodeFailuresCannotInventOrRetainACatchupPlan(t *testing.T) {
	for _, scenario := range []string{"timeout", "code-old", "conflict", "recovered-observation"} {
		t.Run(scenario, func(t *testing.T) {
			s, policy, hold, _ := capacityRecoveryFixture(t)
			current := model.EdgeCodeRecoveryTarget{Slot: "a", FrontGeneration: 3, BundleGeneration: "bundle-c", WorkerSourceSHA: strings.Repeat("c", 40), WorkerImageDigest: "sha256:" + strings.Repeat("c", 64)}
			previous := model.EdgeCodeRecoveryTarget{Slot: "b", FrontGeneration: 2, BundleGeneration: "bundle-b", WorkerSourceSHA: strings.Repeat("b", 40), WorkerImageDigest: "sha256:" + strings.Repeat("b", 64)}
			observed := model.EdgeCodeRecoveryTarget{Slot: "a", FrontGeneration: 1, BundleGeneration: "bundle-a", WorkerSourceSHA: strings.Repeat("a", 40), WorkerImageDigest: "sha256:" + strings.Repeat("a", 64)}
			plan, err := model.PlanEdgeCodeRecovery("sha256:"+strings.Repeat("d", 64), observed, current, &previous)
			if err != nil {
				t.Fatal(err)
			}
			before := platformproducer.OfflineRecoveryObservation{EdgeID: hold.EdgeID, DeclarationDigest: platformproducer.OfflineRecoveryDigest(hold), LastAttempt: time.Now().Add(-6 * time.Minute), CodePlan: &plan, CodeDetail: "code_binding", LastResult: "code_not_current"}
			_, err = s.store.UpdateObservationCheckpoint(context.Background(), platformproducer.OfflineRecoveryKey(hold), func([]byte) ([]byte, error) { return json.Marshal(before) })
			if err != nil {
				t.Fatal(err)
			}
			s.offlineCapacityCode = func(context.Context, model.EdgeNode) (platformproducer.CapacityCodeIdentity, error) {
				if scenario == "code-old" {
					return testCapacityCode(), &capacityCodeError{reason: "code_not_current", detail: "code_binding", plan: &plan}
				}
				if scenario == "conflict" {
					return testCapacityCode(), codeObservationError("code_identity_conflict", "worker_target")
				}
				if scenario == "recovered-observation" {
					return testCapacityCode(), nil
				}
				return platformproducer.CapacityCodeIdentity{}, context.DeadlineExceeded
			}
			s.offlineCapacityMeasure = func(context.Context, model.EdgeClientProbePlan) (model.EdgeClientProbeReport, error) {
				if scenario != "recovered-observation" {
					t.Error("network transfer began without verified current code")
				}
				return model.EdgeClientProbeReport{}, context.DeadlineExceeded
			}
			if err = s.reconcileOfflineEdgeRecovery(context.Background(), policy); err != nil {
				t.Fatal(err)
			}
			result, err := s.readOfflineRecovery(context.Background(), hold)
			if err != nil || result.Admitted {
				t.Fatal(result, err)
			}
			if (result.CodePlan != nil) != (scenario == "code-old") {
				t.Fatal("stale or speculative code plan retained", result)
			}
			if scenario == "timeout" && result.LastResult != "code_observation_unavailable" {
				t.Fatal("timeout claimed obsolete code", result)
			}
			if scenario == "recovered-observation" && (result.CodeDetail != "" || result.LastResult != "capacity_unavailable") {
				t.Fatal("code plan contaminated later capacity failure", result)
			}
		})
	}
}
