package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/routeprobe"
)

func offlineRecoveryFixture(t *testing.T) (*Server, platformproducer.Policy, platformproducer.OfflineEdge, platformproducer.ProjectionPolicyInput) {
	t.Helper()
	state, s, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	create := func(kind, generation string, value any) model.PlatformArtifact {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var content map[string]any
		if err = json.Unmarshal(raw, &content); err != nil {
			t.Fatal(err)
		}
		a, err := state.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: kind, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "global"}, Generation: generation, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		a, err = state.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	input, consumers, _ := pinnedDNSFixture()
	_, _, strategy, _ := directQueryFixture()
	input.DNSQueryPolicy = &strategy
	input.DNSPlacementMode = platformconfig.DNSPlacementConsumerReadiness
	hold := platformproducer.OfflineEdge{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "1.1.1.1", Reason: "quota_exhausted", Recovery: &platformproducer.OfflineEdgeRecoveryPolicy{Hostname: "api.example.test", Path: "/", ProbeIntervalSeconds: 300, ProbeTimeoutSeconds: 5, SuccessThreshold: 3, FailureThreshold: 1, FreshnessSeconds: 1800}}
	input.OfflineEdges = []platformproducer.OfflineEdge{hold}
	static := create(model.PlatformArtifactKindPlatformIntent, "static", platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: "static", DNSConsumers: consumers, Routes: []platformconfig.RouteIntent{{Hostname: hold.Recovery.Hostname, Kind: model.EdgeRouteKindPlatform, UpstreamURL: "http://127.0.0.1:8080", RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true}}})
	source := create(model.PlatformArtifactKindPolicySnapshot, input.Generation, input)
	if _, _, err := state.UpdateEdgeHeartbeat(model.EdgeNode{ID: hold.EdgeID, EdgeGroupID: hold.EdgeGroupID, PublicIPv4: hold.PublicIPv4, Healthy: true, Status: model.EdgeHealthHealthy}); err != nil {
		t.Fatal(err)
	}
	return s, platformproducer.Policy{TargetScope: "global", StaticIntentArtifactID: static.ID, StaticIntentDigest: static.ContentHash, DNSPolicyArtifactID: source.ID, DNSPolicyDigest: source.ContentHash}, hold, input
}

func TestOfflineRecoveryProbeBudgetAndConcurrentReplicas(t *testing.T) {
	s, policy, hold, _ := offlineRecoveryFixture(t)
	var calls atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	s.offlineEdgeRecoveryProbe = func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
		calls.Add(1)
		close(entered)
		<-release
		if host != hold.Recovery.Hostname || path != "/" || address != hold.PublicIPv4 || state != "" || timeout != 5*time.Second {
			t.Error("probe lost pinned target")
		}
		now := time.Now().UTC()
		return routeprobe.Proof{EdgeID: hold.EdgeID, GroupID: hold.EdgeGroupID, Digest: "sha256:" + strings.Repeat("a", 64), CheckedAt: now, ValidUntil: now.Add(time.Hour)}, nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.reconcileOfflineEdgeRecovery(context.Background(), policy); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("recovery never reached probe")
	}
	if err := s.reconcileOfflineEdgeRecovery(context.Background(), policy); err != nil {
		t.Fatal(err)
	}
	close(release)
	wg.Wait()
	if err := s.reconcileOfflineEdgeRecovery(context.Background(), policy); err != nil {
		t.Fatal(err)
	}
	o, err := s.readOfflineRecovery(context.Background(), hold)
	if err != nil || calls.Load() != 1 || o.Successes != 1 || o.Admitted {
		t.Fatal("replicas or retries inflated recovery", calls.Load(), o, err)
	}
}

func TestOfflineRecoveryRestoresCandidatesAndRetractsOnFailedProof(t *testing.T) {
	s, policy, hold, input := offlineRecoveryFixture(t)
	ctx := context.Background()
	now := time.Now().UTC()
	o := platformproducer.OfflineRecoveryObservation{EdgeID: hold.EdgeID, DeclarationDigest: platformproducer.OfflineRecoveryDigest(hold), LastAttempt: now.Add(-6 * time.Minute), LastSuccess: now.Add(-6 * time.Minute), ProofValidUntil: now.Add(time.Hour), Successes: 2}
	write := func(o platformproducer.OfflineRecoveryObservation) {
		t.Helper()
		if _, err := s.store.UpdateObservationCheckpoint(ctx, platformproducer.OfflineRecoveryKey(hold), func([]byte) ([]byte, error) { return json.Marshal(o) }); err != nil {
			t.Fatal(err)
		}
	}
	write(o)
	s.offlineEdgeRecoveryProbe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		now := time.Now().UTC()
		return routeprobe.Proof{EdgeID: hold.EdgeID, GroupID: hold.EdgeGroupID, CheckedAt: now, ValidUntil: now.Add(time.Hour)}, nil
	}
	if err := s.reconcileOfflineEdgeRecovery(ctx, policy); err != nil {
		t.Fatal(err)
	}
	o, err := s.readOfflineRecovery(ctx, hold)
	if err != nil || !o.Ready(hold, time.Now().UTC()) {
		t.Fatal("third success did not recover", o, err)
	}
	nodes := []model.EdgeNode{{ID: hold.EdgeID, EdgeGroupID: hold.EdgeGroupID, PublicIPv4: hold.PublicIPv4}, {ID: "edge-b", EdgeGroupID: hold.EdgeGroupID, PublicIPv4: "9.9.9.9"}}
	projection := platformIntentProjectionResponse{Policy: platformconfig.PolicySnapshot{Generation: "old", Scope: "global"}}
	if err := s.projectDNSReadinessWithRecovery(ctx, &projection, nodes, time.Now().UTC(), &input); err != nil || len(projection.RuntimeSnapshot.DNSEdgeEndpoints) != 2 {
		t.Fatal("candidate not restored", projection, err)
	}
	before, err := platformProducerSourceDigest(projection, "authority")
	if err != nil {
		t.Fatal(err)
	}
	parent := model.PlatformArtifact{Metadata: map[string]string{}, Provenance: model.PlatformArtifactProvenance{SignedAt: time.Now().UTC()}}
	if err := pinOfflineRecoveryEvidence(&parent, projection.RuntimeSnapshot); err != nil {
		t.Fatal(err)
	}
	frozen, err := platformproducer.PublicationOfflineHolds(parent, []platformproducer.OfflineEdge{hold})
	if err != nil || len(frozen) != 0 {
		t.Fatal("recovery not frozen in parent", err)
	}
	o.LastAttempt = now.Add(-6 * time.Minute)
	o.LastSuccess = o.LastAttempt
	write(o)
	s.offlineEdgeRecoveryProbe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		return routeprobe.Proof{}, errors.New("transport down")
	}
	if err := s.reconcileOfflineEdgeRecovery(ctx, policy); err != nil {
		t.Fatal(err)
	}
	projection = platformIntentProjectionResponse{Policy: platformconfig.PolicySnapshot{Generation: "old", Scope: "global"}}
	if err := s.projectDNSReadinessWithRecovery(ctx, &projection, nodes, time.Now().UTC(), &input); err != nil || len(projection.RuntimeSnapshot.DNSEdgeEndpoints) != 1 {
		t.Fatal("failed proof retained candidate", projection, err)
	}
	after, err := platformProducerSourceDigest(projection, "authority")
	if err != nil || before == after {
		t.Fatal("recovery state change did not trigger compilation", err)
	}
	frozen, err = platformproducer.PublicationOfflineHolds(parent, []platformproducer.OfflineEdge{hold})
	if err != nil || len(frozen) != 0 {
		t.Fatal("live failure rewrote historical membership", err)
	}
}

func TestOfflineRecoveryRejectsForeignAndExpiredProofs(t *testing.T) {
	for _, scenario := range []string{"foreign_edge", "foreign_group", "old_observation", "future_observation", "expired", "excluded"} {
		t.Run(scenario, func(t *testing.T) {
			s, policy, hold, _ := offlineRecoveryFixture(t)
			s.offlineEdgeRecoveryProbe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
				now := time.Now().UTC()
				proof := routeprobe.Proof{EdgeID: hold.EdgeID, GroupID: hold.EdgeGroupID, CheckedAt: now, ValidUntil: now.Add(time.Hour)}
				switch scenario {
				case "foreign_edge":
					proof.EdgeID = "edge-other"
				case "foreign_group":
					proof.GroupID = "edge-group-other"
				case "old_observation":
					proof.CheckedAt = now.Add(-time.Minute)
				case "future_observation":
					proof.CheckedAt = now.Add(time.Minute)
				case "expired":
					proof.ValidUntil = now.Add(-time.Second)
				case "excluded":
					proof.State = "excluded"
				}
				return proof, nil
			}
			if err := s.reconcileOfflineEdgeRecovery(context.Background(), policy); err != nil {
				t.Fatal(err)
			}
			o, err := s.readOfflineRecovery(context.Background(), hold)
			if err != nil || o.Successes != 0 || o.Failures != 1 || o.Admitted {
				t.Fatal("invalid proof contributed recovery success", o, err)
			}
		})
	}
}
