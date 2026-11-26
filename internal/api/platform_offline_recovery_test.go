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
	"fugue/internal/store"
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

func TestEnrolledRouteRecoveryRequiresCurrentExactCode(t *testing.T) {
	s, _, h, _ := offlineRecoveryFixture(t)
	ctx := context.Background()
	now := time.Now().UTC()
	node, _, err := s.store.GetEdgeNode(h.EdgeID)
	if err != nil {
		t.Fatal(err)
	}
	code := testCapacityCode()
	h.WorkerEnrollment = &platformproducer.OfflineWorkerEnrollment{NodeUID: code.NodeUID}
	o := platformproducer.OfflineRecoveryObservation{
		EdgeID: h.EdgeID, DeclarationDigest: platformproducer.OfflineRecoveryDigest(h), Admitted: true,
		LastAttempt: now, LastSuccess: now, ProofValidUntil: now.Add(time.Minute), Successes: h.Recovery.SuccessThreshold,
	}
	reads := 0
	live := code
	s.offlineCapacityCode = func(_ context.Context, seen model.EdgeNode) (platformproducer.CapacityCodeIdentity, error) {
		if seen.ID != h.EdgeID {
			t.Fatal("verified a different node")
		}
		reads++
		return live, nil
	}
	if !o.Ready(h, now) || s.offlineRecoveryReady(ctx, h, o, node, now) || reads != 0 {
		t.Fatal("a legacy route-only sample readmitted the enrolled Worker")
	}
	o.RouteCode, o.LastResult = &code, "route_code_verified"
	if !s.offlineRecoveryReady(ctx, h, o, node, now) || reads != 1 {
		t.Fatal("matching fresh executable and route evidence did not allow recovery", reads)
	}
	live.WorkerPodUID = "new-worker"
	if s.offlineRecoveryReady(ctx, h, o, node, now) {
		t.Fatal("a replaced Worker inherited a previous Pod's route evidence")
	}
	live = code
	live.NodeUID = "different-node"
	if s.offlineRecoveryReady(ctx, h, o, node, now) {
		t.Fatal("a replacement Node inherited the enrolled Node's route evidence")
	}
}

func TestPinnedWorkerRecoveryRejectsOldCodeAndDoesNotObserveOfflineNode(t *testing.T) {
	s, _, h, _ := offlineRecoveryFixture(t)
	now := time.Now().UTC()
	node, _, err := s.store.GetEdgeNode(h.EdgeID)
	if err != nil {
		t.Fatal(err)
	}
	code := testCapacityCode()
	code.NodeUID = "11111111-1111-1111-1111-111111111111"
	code.ServiceUID = "22222222-2222-2222-2222-222222222222"
	code.WorkerReleaseDigest = "sha256:" + strings.Repeat("a", 64)
	h.Recovery.WorkerTarget = &platformproducer.OfflineWorkerTarget{NodeUID: code.NodeUID, ServiceUID: code.ServiceUID,
		ReleaseDigest: code.WorkerReleaseDigest, WorkerSourceSHA: code.WorkerSourceSHA, WorkerImage: code.WorkerImageDigest}
	o := platformproducer.OfflineRecoveryObservation{EdgeID: h.EdgeID, DeclarationDigest: platformproducer.OfflineRecoveryDigest(h),
		Admitted: true, LastAttempt: now, LastSuccess: now, ProofValidUntil: now.Add(time.Minute), Successes: h.Recovery.SuccessThreshold,
		LastResult: "route_code_verified", RouteCode: &code}
	live, reads := code, 0
	s.offlineCapacityCode = func(context.Context, model.EdgeNode) (platformproducer.CapacityCodeIdentity, error) {
		reads++
		return live, nil
	}
	stale := node
	old := now.Add(-time.Hour)
	stale.LastSeenAt, stale.LastHeartbeatAt = &old, &old
	if s.offlineRecoveryReady(t.Context(), h, o, stale, now) || reads != 0 {
		t.Fatal("offline node was probed or admitted")
	}
	live.WorkerReleaseDigest = "sha256:" + strings.Repeat("b", 64)
	if s.offlineRecoveryReady(t.Context(), h, o, node, now) || reads != 1 {
		t.Fatal("historical executable receipt admitted the node")
	}
	live = code
	if !s.offlineRecoveryReady(t.Context(), h, o, node, now) || reads != 2 {
		t.Fatal("exact current Worker and prior verification did not authorize readiness")
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

func TestOfflineRecoveryRetainsSignedHoldForRetiredRouteEnrollment(t *testing.T) {
	s, _, retiredHold, input := offlineRecoveryFixture(t)
	at := time.Now().UTC()
	otherHold := retiredHold
	otherHold.EdgeID, otherHold.PublicIPv4 = "edge-b", "9.9.9.9"
	input.OfflineEdges = []platformproducer.OfflineEdge{retiredHold, otherHold}
	nodes := []model.EdgeNode{
		{ID: retiredHold.EdgeID, EdgeGroupID: retiredHold.EdgeGroupID, PublicIPv4: retiredHold.PublicIPv4},
		{ID: otherHold.EdgeID, EdgeGroupID: otherHold.EdgeGroupID, PublicIPv4: otherHold.PublicIPv4},
		{ID: "edge-c", EdgeGroupID: retiredHold.EdgeGroupID, PublicIPv4: "8.8.4.4"},
	}
	for _, hold := range input.OfflineEdges {
		o := platformproducer.OfflineRecoveryObservation{EdgeID: hold.EdgeID, DeclarationDigest: platformproducer.OfflineRecoveryDigest(hold), LastAttempt: at, LastSuccess: at, ProofValidUntil: at.Add(time.Hour), Successes: 3, Admitted: true}
		if _, err := s.store.UpdateObservationCheckpoint(t.Context(), platformproducer.OfflineRecoveryKey(hold), func([]byte) ([]byte, error) { return json.Marshal(o) }); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig} {
		t.Run(kind, func(t *testing.T) {
			key := store.PlatformConsumerRetirementTarget{ConsumerID: "edge-worker:" + retiredHold.EdgeID, ArtifactKind: kind, ScopeKey: "global"}
			calls := 0
			lookup := func(_ context.Context, targets []store.PlatformConsumerRetirementTarget) (map[store.PlatformConsumerRetirementTarget]bool, error) {
				calls++
				matches := map[store.PlatformConsumerRetirementTarget]bool{}
				for _, target := range targets {
					if target == key {
						matches[target] = true
					}
				}
				return matches, nil
			}
			intent := model.EdgeDNSAdmissionIntent{State: "suspended", Reason: retiredHold.Reason}
			s.observeOfflineRecoveryChecked(nodes[0], &intent, input.OfflineEdges, at, lookup)
			if intent.State != "suspended" || intent.RecoveryState != "enrollment_retired" || calls != 1 {
				t.Fatal("inventory reported a retired enrollment as eligible", intent)
			}
			calls = 0
			batchLookup := func(ctx context.Context, targets []store.PlatformConsumerRetirementTarget) (map[store.PlatformConsumerRetirementTarget]bool, error) {
				matches, err := lookup(ctx, targets)
				if len(targets) != 4 || len(matches) != 1 {
					t.Fatal("expected one batch with both exact route enrollments per recovered edge", targets)
				}
				return matches, err
			}
			projection := platformIntentProjectionResponse{Policy: platformconfig.PolicySnapshot{Generation: "old", Scope: "global"}}
			if err := s.projectDNSReadinessWithRecoveryChecked(t.Context(), &projection, nodes, at, &input, batchLookup); err != nil {
				t.Fatal(err)
			}
			if calls != 1 || len(projection.RuntimeSnapshot.DNSEdgeEndpoints) != 2 {
				t.Fatal("retired enrollment blocked independent recovery or serving membership", calls, projection.RuntimeSnapshot.DNSEdgeEndpoints)
			}
			for _, e := range projection.RuntimeSnapshot.DNSEdgeEndpoints {
				if e.EdgeID == retiredHold.EdgeID {
					t.Fatal("retired enrollment returned to DNS candidates")
				}
			}
			parent := model.PlatformArtifact{Metadata: map[string]string{}, Provenance: model.PlatformArtifactProvenance{SignedAt: at}}
			if err := pinOfflineRecoveryEvidence(&parent, projection.RuntimeSnapshot); err != nil {
				t.Fatal(err)
			}
			holds, err := platformproducer.PublicationOfflineHolds(parent, input.OfflineEdges)
			if err != nil || len(holds) != 1 || holds[0].EdgeID != retiredHold.EdgeID {
				t.Fatal("publication replay did not retain exactly the retired enrollment's hold", holds, err)
			}
		})
	}
	unavailable := errors.New("retirement lookup unavailable")
	projection := platformIntentProjectionResponse{Policy: platformconfig.PolicySnapshot{Generation: "old", Scope: "global"}}
	err := s.projectDNSReadinessWithRecoveryChecked(t.Context(), &projection, nodes, at, &input, func(context.Context, []store.PlatformConsumerRetirementTarget) (map[store.PlatformConsumerRetirementTarget]bool, error) {
		return nil, unavailable
	})
	if !errors.Is(err, unavailable) || len(projection.RuntimeSnapshot.DNSEdgeEndpoints) != 0 || len(projection.RuntimeSnapshot.Facts) != 0 {
		t.Fatal("unavailable ledger did not stop candidate publication", err)
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

func TestOfflineRecoveryDoesNotProbeExplicitlyDisabledStaticRoute(t *testing.T) {
	s, policy, _, _ := offlineRecoveryFixture(t)
	static, err := s.store.GetPlatformArtifact(policy.StaticIntentArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	var intent platformconfig.PlatformIntent
	raw, err := json.Marshal(static.Content)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &intent); err != nil {
		t.Fatal(err)
	}
	intent.Generation = "disabled-probe-route"
	intent.Routes[0].Enabled = false
	raw, err = json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]any
	if err = json.Unmarshal(raw, &content); err != nil {
		t.Fatal(err)
	}
	static, err = s.store.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPlatformIntent, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "global"}, Generation: intent.Generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	static, err = s.store.ValidatePlatformArtifact(static.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	policy.StaticIntentArtifactID, policy.StaticIntentDigest = static.ID, static.ContentHash
	s.offlineEdgeRecoveryProbe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		t.Error("disabled route was probed")
		return routeprobe.Proof{}, nil
	}
	if err = s.reconcileOfflineEdgeRecovery(context.Background(), policy); err == nil {
		t.Fatal("disabled route authorized recovery")
	}
}
