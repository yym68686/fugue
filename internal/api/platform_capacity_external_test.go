package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

func TestSingleCapacityRoundRestoresPublishedMembershipAndSurvivesRestart(t *testing.T) {
	s, _, hold, input := capacityRecoveryFixture(t)
	hold.Recovery.SuccessThreshold, hold.Recovery.FailureThreshold = 1, 1
	hold.Recovery.Capacity.Version, hold.Recovery.Capacity.MinimumStableSeconds = platformproducer.CapacitySingleVersion, 0
	input.OfflineEdges = []platformproducer.OfflineEdge{hold}
	ctx := t.Context()
	plan, err := s.claimExternalCapacity(ctx, hold, input.OfflineEdges, "observer-a")
	if err != nil || plan == nil {
		t.Fatal("single-round policy could not claim a signed measurement", err)
	}
	started := time.Now().UTC()
	time.Sleep(time.Millisecond)
	report := capacityReport(t, s, plan.Plan, started, time.Now().UTC())
	for i := 0; i < 2; i++ {
		if result, err := s.recordExternalCapacity(ctx, input.OfflineEdges, "observer-a", report); err != nil || result != "capacity_verified" {
			t.Fatal("one complete round or its idempotent completion failed", result, err)
		}
	}
	observation, err := s.readOfflineRecovery(ctx, hold)
	later := time.Now().UTC().Add(365 * 24 * time.Hour)
	if err != nil || !observation.Ready(hold, later) || observation.Successes != 1 {
		t.Fatal("completed recovery was lost on checkpoint reload or by age", observation, err)
	}
	nodes := []model.EdgeNode{{ID: hold.EdgeID, EdgeGroupID: hold.EdgeGroupID, PublicIPv4: hold.PublicIPv4}, {ID: "edge-b", EdgeGroupID: hold.EdgeGroupID, PublicIPv4: "9.9.9.9"}}
	projection := platformIntentProjectionResponse{Policy: platformconfig.PolicySnapshot{Generation: "old", Scope: "global"}}
	if err := s.projectDNSReadinessWithRecovery(ctx, &projection, nodes, later, &input); err != nil || len(projection.RuntimeSnapshot.DNSEdgeEndpoints) != 2 {
		t.Fatal("single success did not restore candidate membership", err)
	}
	parent := model.PlatformArtifact{Metadata: map[string]string{}, Provenance: model.PlatformArtifactProvenance{SignedAt: later}}
	if err := pinOfflineRecoveryEvidence(&parent, projection.RuntimeSnapshot); err != nil {
		t.Fatal(err)
	}
	if frozen, err := platformproducer.PublicationOfflineHolds(parent, input.OfflineEdges); err != nil || len(frozen) != 0 {
		t.Fatal("signed publication did not preserve recovered membership", err)
	}
}

func TestCapacityExternalClaimsAreBoundedAndReportsCannotReplayOrChangeIdentity(t *testing.T) {
	for _, scenario := range []string{"valid", "replay", "resume_completion", "foreign_observer", "forged", "code_changed", "superseded", "reference_failed", "changed_policy"} {
		t.Run(scenario, func(t *testing.T) {
			s, _, hold, input := capacityRecoveryFixture(t)
			ctx := context.Background()
			now := time.Now().UTC()
			previous := platformproducer.OfflineRecoveryObservation{EdgeID: hold.EdgeID, DeclarationDigest: platformproducer.OfflineRecoveryDigest(hold), LastAttempt: now.Add(-6 * time.Minute), LastSuccess: now.Add(-6 * time.Minute), FirstSuccess: now.Add(-12 * time.Minute), ProofValidUntil: now.Add(time.Hour), Successes: 2, Capacity: &platformproducer.CapacityRecoveryEvidence{CheckedAt: now.Add(-6 * time.Minute), ReportDigest: "sha256:" + strings.Repeat("b", 64), ReferenceEdgeID: "edge-b", Bytes: 1 << 20, ElapsedMilliseconds: 2000, BytesPerSecond: 524288, Code: testCapacityCode(), RouteDigest: "sha256:" + strings.Repeat("a", 64)}}
			key := platformproducer.OfflineRecoveryKey(hold)
			s.store.UpdateObservationCheckpoint(ctx, key, func([]byte) ([]byte, error) { return json.Marshal(previous) })
			plan, err := s.claimExternalCapacity(ctx, hold, input.OfflineEdges, "observer-a")
			if err != nil || plan == nil {
				t.Fatal("failed to issue bounded plan", err)
			}
			second, err := s.claimExternalCapacity(ctx, hold, input.OfflineEdges, "observer-b")
			if err != nil || second != nil {
				t.Fatal("duplicate observer spent interval budget", err)
			}
			started := time.Now().UTC()
			time.Sleep(time.Millisecond)
			report := capacityReport(t, s, plan.Plan, started, time.Now().UTC())
			observer := "observer-a"
			switch scenario {
			case "foreign_observer":
				observer = "observer-b"
			case "forged":
				report.Plan.Permits[0].Signature = "forged"
			case "code_changed":
				s.offlineCapacityCode = func(context.Context, model.EdgeNode) (platformproducer.CapacityCodeIdentity, error) {
					code := testCapacityCode()
					code.WorkerPodUID = "replacement"
					return code, nil
				}
			case "superseded":
				s.store.UpdateObservationCheckpoint(ctx, key, func(raw []byte) ([]byte, error) {
					var o platformproducer.OfflineRecoveryObservation
					json.Unmarshal(raw, &o)
					o.LastAttempt = time.Now().UTC()
					return json.Marshal(o)
				})
			case "reference_failed":
				report.Outcomes[1].Failure = "body"
			case "changed_policy":
				input.OfflineEdges[0].Recovery.Capacity.MinimumBytesPerSecond++
			}
			result, err := s.recordExternalCapacity(ctx, input.OfflineEdges, observer, report)
			rejected := scenario == "foreign_observer" || scenario == "forged" || scenario == "superseded" || scenario == "changed_policy"
			if (err != nil) != rejected {
				t.Fatal("unexpected report disposition", result, err)
			}
			if scenario == "replay" || scenario == "resume_completion" {
				if scenario == "resume_completion" {
					// Simulate a crash after persisting the consumed report but
					// before completing its admission checkpoint.
					raw, _ := s.store.ReadObservationCheckpoint(ctx, key+"/external")
					var pending externalCapacityRound
					json.Unmarshal(raw, &pending)
					claimed := previous
					claimed.LastAttempt = pending.ClaimedAt
					s.store.UpdateObservationCheckpoint(ctx, key, func([]byte) ([]byte, error) { return json.Marshal(claimed) })
				}
				if result, err = s.recordExternalCapacity(ctx, input.OfflineEdges, observer, report); err != nil || result != "capacity_verified" {
					t.Fatal("idempotent completion failed", result, err)
				}
				report.Outcomes[0].BytesReceived--
				if _, err = s.recordExternalCapacity(ctx, input.OfflineEdges, observer, report); err == nil {
					t.Fatal("consumed claim accepted a different report")
				}
			}
			observed, err := s.readOfflineRecovery(ctx, hold)
			valid := scenario == "valid" || scenario == "replay" || scenario == "resume_completion"
			if err != nil || observed.Ready(hold, time.Now().UTC()) != valid {
				t.Fatal("incorrect readmission", observed, err)
			}
			if valid && observed.Successes != 3 {
				t.Fatal("report replay manufactured consecutive successes", observed)
			}
		})
	}
}

func TestCapacityExternalObserverOwnsOnlyCapacityIntervals(t *testing.T) {
	t.Setenv("FUGUE_EDGE_CAPACITY_OBSERVER", "external")
	s, policy, hold, _ := capacityRecoveryFixture(t)
	s.offlineCapacityMeasure = func(context.Context, model.EdgeClientProbePlan) (model.EdgeClientProbeReport, error) {
		t.Fatal("cluster-local probe reset external observation")
		return model.EdgeClientProbeReport{}, nil
	}
	if err := s.reconcileOfflineEdgeRecovery(context.Background(), policy); err != nil {
		t.Fatal(err)
	}
	o, err := s.readOfflineRecovery(context.Background(), hold)
	if err != nil || !o.LastAttempt.IsZero() {
		t.Fatal("internal worker claimed external interval", o, err)
	}
}
