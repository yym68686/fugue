package store

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEdgeInstanceFencingIncidentSequence(t *testing.T) {
	t.Parallel()
	s := newEdgeInstanceTestStore(t)
	createEdgeInstanceControl(t, s, "edge-de-1", "edge-group-country-de")

	putEdgeTestEpoch(t, s, "edge-group-country-de", model.EdgeSlotA, "release-a", 1, 1)
	heartbeatEdgeInstanceTwice(t, s, healthyEdgeTestInstance("edge-de-1", "edge-group-country-de", model.EdgeSlotA, "pod-a", "release-a"))
	assertOnlyActiveEdge(t, s, "edge-group-country-de", "release-a", true)

	// B may become healthy before the control plane switches the central fence.
	// It must remain ineligible while A is the exact active epoch.
	heartbeatEdgeInstanceTwice(t, s, healthyEdgeTestInstance("edge-de-1", "edge-group-country-de", model.EdgeSlotB, "pod-b", "release-b"))
	assertOnlyActiveEdge(t, s, "edge-group-country-de", "release-a", true)

	putEdgeTestEpoch(t, s, "edge-group-country-de", model.EdgeSlotB, "release-b", 2, 1)
	assertOnlyActiveEdge(t, s, "edge-group-country-de", "release-b", true)

	// The incident's late inactive-A unhealthy heartbeats cannot overwrite B.
	lateA := healthyEdgeTestInstance("edge-de-1", "edge-group-country-de", model.EdgeSlotA, "pod-a", "release-a")
	lateA.Node.Healthy = false
	lateA.Node.Status = model.EdgeHealthUnhealthy
	for i := 0; i < 8; i++ {
		if _, err := s.UpdateEdgeInstanceHeartbeat(lateA); err != nil {
			t.Fatalf("late A heartbeat %d: %v", i, err)
		}
	}
	assertOnlyActiveEdge(t, s, "edge-group-country-de", "release-b", true)

	// A stale B release cannot move the central fence, even with a higher
	// heartbeat timestamp or a caller claiming healthy.
	staleB := healthyEdgeTestInstance("edge-de-1", "edge-group-country-de", model.EdgeSlotB, "pod-b-old", "release-b-old")
	heartbeatEdgeInstanceTwice(t, s, staleB)
	assertOnlyActiveEdge(t, s, "edge-group-country-de", "release-b", true)

	currentB := healthyEdgeTestInstance("edge-de-1", "edge-group-country-de", model.EdgeSlotB, "pod-b", "release-b")
	currentB.Node.Healthy = false
	currentB.Node.Status = model.EdgeHealthUnhealthy
	if _, err := s.UpdateEdgeInstanceHeartbeat(currentB); err != nil {
		t.Fatalf("first transient B failure: %v", err)
	}
	assertOnlyActiveEdge(t, s, "edge-group-country-de", "release-b", true)
	if _, err := s.UpdateEdgeInstanceHeartbeat(currentB); err != nil {
		t.Fatalf("second B failure: %v", err)
	}
	assertOnlyActiveEdge(t, s, "edge-group-country-de", "release-b", false)
}

func TestEdgeInstanceHardFailuresBypassHysteresis(t *testing.T) {
	t.Parallel()
	for _, failureClass := range []string{
		model.EdgeInstanceFailureSignatureInvalid,
		model.EdgeInstanceFailureMaxStaleExceeded,
		model.EdgeInstanceFailureIdentityDrift,
	} {
		failureClass := failureClass
		t.Run(failureClass, func(t *testing.T) {
			t.Parallel()
			s := newEdgeInstanceTestStore(t)
			createEdgeInstanceControl(t, s, "edge-us-1", "edge-group-country-us")
			putEdgeTestEpoch(t, s, "edge-group-country-us", model.EdgeSlotB, "release-b", 1, 1)
			instance := healthyEdgeTestInstance("edge-us-1", "edge-group-country-us", model.EdgeSlotB, "pod-b", "release-b")
			heartbeatEdgeInstanceTwice(t, s, instance)
			instance.FailureClass = failureClass
			if _, err := s.UpdateEdgeInstanceHeartbeat(instance); err != nil {
				t.Fatalf("hard-failure heartbeat: %v", err)
			}
			assertOnlyActiveEdge(t, s, "edge-group-country-us", "release-b", false)
		})
	}
}

func TestEdgeInstanceConcurrentReplicasCannotOverwriteActiveEpoch(t *testing.T) {
	t.Parallel()
	s := newEdgeInstanceTestStore(t)
	createEdgeInstanceControl(t, s, "edge-shared-1", "edge-group-country-us")
	putEdgeTestEpoch(t, s, "edge-group-country-us", model.EdgeSlotB, "release-b", 2, 1)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			instance := healthyEdgeTestInstance("edge-shared-1", "edge-group-country-us", model.EdgeSlotB, "pod-b", "release-b")
			for attempt := 0; attempt < 2; attempt++ {
				if _, err := s.UpdateEdgeInstanceHeartbeat(instance); err != nil {
					t.Errorf("B instance %d heartbeat: %v", index, err)
				}
			}
		}(i)
	}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			instance := healthyEdgeTestInstance("edge-shared-1", "edge-group-country-us", model.EdgeSlotA, "pod-a", "release-a")
			instance.Node.Healthy = false
			instance.Node.Status = model.EdgeHealthUnhealthy
			if _, err := s.UpdateEdgeInstanceHeartbeat(instance); err != nil {
				t.Errorf("A instance %d heartbeat: %v", index, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	assertOnlyActiveEdge(t, s, "edge-group-country-us", "release-b", true)
	instances, _, err := s.ListEdgeNodeInstances("edge-group-country-us")
	if err != nil {
		t.Fatalf("list concurrent instance: %v", err)
	}
	for _, instance := range instances {
		if instance.Slot == model.EdgeSlotB && instance.InstanceUID == "pod-b" && instance.ConsecutiveHealthy != 64 {
			t.Fatalf("concurrent replica writes lost observations: %+v", instance)
		}
	}
}

func TestEdgeInstanceCrashReopenAndFenceRollover(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "store.json")
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	createEdgeInstanceControl(t, s, "edge-us-1", "edge-group-country-us")
	putEdgeTestEpoch(t, s, "edge-group-country-us", model.EdgeSlotA, "release-a", 7, 1)
	a := healthyEdgeTestInstance("edge-us-1", "edge-group-country-us", model.EdgeSlotA, "pod-a", "release-a")
	heartbeatEdgeInstanceTwice(t, s, a)
	a.Node.Healthy = false
	a.Node.Status = model.EdgeHealthUnhealthy
	if _, err := s.UpdateEdgeInstanceHeartbeat(a); err != nil {
		t.Fatalf("first transient failure before restart: %v", err)
	}

	reopened := New(path)
	if err := reopened.Init(); err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	assertOnlyActiveEdge(t, reopened, "edge-group-country-us", "release-a", true)
	if _, err := reopened.UpdateEdgeInstanceHeartbeat(a); err != nil {
		t.Fatalf("second transient failure after restart: %v", err)
	}
	assertOnlyActiveEdge(t, reopened, "edge-group-country-us", "release-a", false)
	a.Node.Healthy = true
	a.Node.Status = model.EdgeHealthHealthy
	heartbeatEdgeInstanceTwice(t, reopened, a)
	if _, err := reopened.PutEdgeActiveEpoch(model.EdgeActiveEpoch{EdgeGroupID: "edge-group-country-us", Slot: model.EdgeSlotB, ReleaseEpoch: "release-b", FenceSequence: 6}); !errors.Is(err, ErrConflict) {
		t.Fatalf("older fence sequence must conflict, got %v", err)
	}
	// A higher fence sequence permits an explicit centrally fenced rollback.
	heartbeatEdgeInstanceTwice(t, reopened, healthyEdgeTestInstance("edge-us-1", "edge-group-country-us", model.EdgeSlotB, "pod-b", "release-b"))
	putEdgeTestEpoch(t, reopened, "edge-group-country-us", model.EdgeSlotB, "release-b", 8, 1)
	assertOnlyActiveEdge(t, reopened, "edge-group-country-us", "release-b", true)
}

func TestEdgeInstanceMinimumHealthyIsExactActiveEpochNOfM(t *testing.T) {
	t.Parallel()
	s := newEdgeInstanceTestStore(t)
	for _, edgeID := range []string{"edge-us-1", "edge-us-2"} {
		createEdgeInstanceControl(t, s, edgeID, "edge-group-country-us")
	}
	putEdgeTestEpoch(t, s, "edge-group-country-us", model.EdgeSlotB, "release-b", 1, 2)
	one := healthyEdgeTestInstance("edge-us-1", "edge-group-country-us", model.EdgeSlotB, "pod-b-1", "release-b")
	two := healthyEdgeTestInstance("edge-us-2", "edge-group-country-us", model.EdgeSlotB, "pod-b-2", "release-b")
	heartbeatEdgeInstanceTwice(t, s, one)
	two.Node.Healthy = false
	two.Node.Status = model.EdgeHealthUnhealthy
	heartbeatEdgeInstanceTwice(t, s, two)
	nodes, _, err := s.ListActiveEdgeNodes("edge-group-country-us")
	if err != nil {
		t.Fatalf("list active N-of-M inventory: %v", err)
	}
	for _, node := range nodes {
		if node.Healthy {
			t.Fatalf("1-of-2 must fail closed for every route candidate: %+v", nodes)
		}
	}
	two.Node.Healthy = true
	two.Node.Status = model.EdgeHealthHealthy
	heartbeatEdgeInstanceTwice(t, s, two)
	nodes, _, err = s.ListActiveEdgeNodes("edge-group-country-us")
	if err != nil {
		t.Fatalf("list recovered N-of-M inventory: %v", err)
	}
	if len(nodes) != 2 || !nodes[0].Healthy || !nodes[1].Healthy {
		t.Fatalf("2-of-2 stable active instances must recover: %+v", nodes)
	}
}

func TestLegacyEdgeMigrationRemainsAuthoritativeUntilCutover(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "store.json")
	s := New(path)
	if _, _, err := s.CreateEdgeNodeToken(model.EdgeNode{ID: "edge-old", EdgeGroupID: "edge-group-old", Status: model.EdgeHealthUnknown}); err != nil {
		t.Fatalf("create legacy edge token: %v", err)
	}
	if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{ID: "edge-old", EdgeGroupID: "edge-group-old", Status: model.EdgeHealthHealthy, Healthy: true}); err != nil {
		t.Fatalf("write legacy heartbeat: %v", err)
	}
	if err := s.Init(); err != nil {
		t.Fatalf("migrate legacy store: %v", err)
	}
	instances, epochs, err := s.ListEdgeNodeInstances("")
	if err != nil {
		t.Fatalf("list migrated instances: %v", err)
	}
	if len(instances) != 1 || instances[0].Slot != edgeLegacyMigrationSlot || instances[0].EffectiveHealthy || instances[0].Node.TokenHash != "" || len(epochs) != 0 {
		t.Fatalf("legacy row must be inert migration input: instances=%+v epochs=%+v", instances, epochs)
	}
	nodes, _, err := s.ListActiveEdgeNodes("")
	if err != nil || len(nodes) != 1 || !nodes[0].Healthy {
		t.Fatalf("phase0 must preserve legacy route authority: nodes=%+v err=%v", nodes, err)
	}
}

func TestEdgeInstanceStateTamperFailsReadiness(t *testing.T) {
	t.Parallel()
	s := newEdgeInstanceTestStore(t)
	createEdgeInstanceControl(t, s, "edge-us-1", "edge-group-country-us")
	putEdgeTestEpoch(t, s, "edge-group-country-us", model.EdgeSlotB, "release-b", 1, 1)
	heartbeatEdgeInstanceTwice(t, s, healthyEdgeTestInstance("edge-us-1", "edge-group-country-us", model.EdgeSlotB, "pod-b", "release-b"))
	if err := s.withLockedState(true, func(state *model.State) error {
		state.EdgeNodeInstances[0].ReleaseEpoch = "tampered"
		return nil
	}); err != nil {
		t.Fatalf("tamper state: %v", err)
	}
	if err := s.CheckReadiness(nil); !errors.Is(err, ErrEdgeInstanceFencingNotReady) {
		t.Fatalf("tampered instance must make store not ready, got %v", err)
	}
}

func TestEdgeInstanceWithoutCentralEpochFailsReadiness(t *testing.T) {
	t.Parallel()
	s := newEdgeInstanceTestStore(t)
	createEdgeInstanceControl(t, s, "edge-us-1", "edge-group-country-us")
	instance := healthyEdgeTestInstance("edge-us-1", "edge-group-country-us", model.EdgeSlotB, "pod-b", "release-b")
	if _, err := s.UpdateEdgeInstanceHeartbeat(instance); err != nil {
		t.Fatalf("write unfenced instance: %v", err)
	}
	if err := s.CheckReadiness(nil); err != nil {
		t.Fatalf("shadow observation without a fence must remain ready: %v", err)
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		state.EdgeActivation.Phase = model.EdgeActivationPhaseActive
		state.EdgeActivation.RouteAuthority = model.EdgeRouteAuthorityActiveEpoch
		state.EdgeActivation.Generation++
		state.EdgeActivation.UpdatedAt = time.Now().UTC()
		return nil
	}); err != nil {
		t.Fatalf("force incomplete active mode: %v", err)
	}
	if err := s.CheckReadiness(nil); !errors.Is(err, ErrEdgeInstanceFencingNotReady) {
		t.Fatalf("active mode without central epoch must fail readiness, got %v", err)
	}
	putEdgeTestEpoch(t, s, "edge-group-country-us", model.EdgeSlotB, "release-b", 1, 1)
	if err := s.CheckReadiness(nil); err != nil {
		t.Fatalf("exact central epoch must restore readiness: %v", err)
	}
}

func newEdgeInstanceTestStore(t *testing.T) *Store {
	t.Helper()
	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	return s
}

func createEdgeInstanceControl(t *testing.T, s *Store, edgeID, groupID string) {
	t.Helper()
	if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{ID: edgeID, EdgeGroupID: groupID, Status: model.EdgeHealthUnknown}); err != nil {
		t.Fatalf("create edge control identity: %v", err)
	}
}

func healthyEdgeTestInstance(edgeID, groupID, slot, uid, epoch string) model.EdgeNodeInstance {
	return model.EdgeNodeInstance{
		EdgeID: edgeID, EdgeGroupID: groupID, Slot: slot, InstanceUID: uid, ReleaseEpoch: epoch,
		Node: model.EdgeNode{
			ID: edgeID, EdgeGroupID: groupID, Status: model.EdgeHealthHealthy, Healthy: true,
			RouteBundleVersion: "routegen-current", ServingGeneration: "routegen-current", CaddyRouteCount: 1,
			TLSStatus: model.EdgeTLSStatusReady,
		},
	}
}

func heartbeatEdgeInstanceTwice(t *testing.T, s *Store, instance model.EdgeNodeInstance) {
	t.Helper()
	for i := 0; i < 2; i++ {
		if _, err := s.UpdateEdgeInstanceHeartbeat(instance); err != nil {
			t.Fatalf("instance heartbeat %d: %v", i, err)
		}
	}
}

func putEdgeTestEpoch(t *testing.T, s *Store, groupID, slot, epoch string, sequence uint64, minHealthy int) {
	t.Helper()
	err := s.withLockedState(true, func(state *model.State) error {
		active, err := normalizeEdgeActiveEpoch(model.EdgeActiveEpoch{
			EdgeGroupID: groupID, Slot: slot, ReleaseEpoch: epoch, FenceSequence: sequence, MinHealthyInstances: minHealthy,
		})
		if err != nil {
			return err
		}
		index := findEdgeActiveEpoch(state.EdgeActiveEpochs, groupID)
		if index >= 0 {
			if sequence <= state.EdgeActiveEpochs[index].FenceSequence {
				return ErrConflict
			}
			state.EdgeActiveEpochs[index] = active
		} else {
			state.EdgeActiveEpochs = append(state.EdgeActiveEpochs, active)
		}
		state.EdgeActivation.Phase = model.EdgeActivationPhaseActive
		state.EdgeActivation.RouteAuthority = model.EdgeRouteAuthorityActiveEpoch
		state.EdgeActivation.Generation++
		state.EdgeActivation.UpdatedAt = time.Now().UTC()
		return nil
	})
	if err != nil {
		t.Fatalf("put active epoch: %v", err)
	}
}

func assertOnlyActiveEdge(t *testing.T, s *Store, groupID, expectedEpoch string, healthy bool) {
	t.Helper()
	nodes, _, err := s.ListActiveEdgeNodes(groupID)
	if err != nil {
		t.Fatalf("list active edge nodes: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Healthy != healthy {
		t.Fatalf("expected one active edge healthy=%v, got %+v", healthy, nodes)
	}
	instances, epochs, err := s.ListEdgeNodeInstances(groupID)
	if err != nil {
		t.Fatalf("list edge instance material: %v", err)
	}
	if len(epochs) != 1 || epochs[0].ReleaseEpoch != expectedEpoch {
		t.Fatalf("active epoch drifted: epochs=%+v instances=%+v", epochs, instances)
	}
}

func TestEdgeInstanceServerTimeIgnoresCallerClockInFileHarness(t *testing.T) {
	t.Parallel()
	s := newEdgeInstanceTestStore(t)
	createEdgeInstanceControl(t, s, "edge-us-1", "edge-group-country-us")
	instance := healthyEdgeTestInstance("edge-us-1", "edge-group-country-us", model.EdgeSlotB, "pod-b", "release-b")
	serverNow := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	stored, err := s.updateEdgeInstanceHeartbeatAt(instance, serverNow)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !stored.LastHeartbeatAt.Equal(serverNow) || stored.Node.LastHeartbeatAt == nil || !stored.Node.LastHeartbeatAt.Equal(serverNow) {
		t.Fatalf("heartbeat must bind the store clock: %+v", stored)
	}
}

func TestEdgeInstanceSameEpochPodReplacementFencesLateOldUID(t *testing.T) {
	t.Parallel()
	s := newEdgeInstanceTestStore(t)
	createEdgeInstanceControl(t, s, "edge-de-1", "edge-group-country-de")
	base := time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC)
	old := healthyEdgeTestInstance("edge-de-1", "edge-group-country-de", model.EdgeSlotB, "pod-old", "release-b")
	old.Node.RouteBundleVersion = "route-old"
	if _, err := s.updateEdgeInstanceHeartbeatAt(old, base); err != nil {
		t.Fatal(err)
	}
	if _, err := s.updateEdgeInstanceHeartbeatAt(old, base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	putEdgeTestEpoch(t, s, old.EdgeGroupID, old.Slot, old.ReleaseEpoch, 1, 1)

	replacement := healthyEdgeTestInstance(old.EdgeID, old.EdgeGroupID, old.Slot, "pod-new", old.ReleaseEpoch)
	replacement.Node.RouteBundleVersion = "route-new"
	if _, err := s.updateEdgeInstanceHeartbeatAt(replacement, base.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	nodes, _, err := s.ListActiveEdgeNodes(old.EdgeGroupID)
	if err != nil || len(nodes) != 1 || nodes[0].RouteBundleVersion != "route-old" || !nodes[0].Healthy {
		t.Fatalf("one replacement observation must preserve the mature old UID: nodes=%+v err=%v", nodes, err)
	}
	if _, err := s.updateEdgeInstanceHeartbeatAt(replacement, base.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	nodes, _, err = s.ListActiveEdgeNodes(old.EdgeGroupID)
	if err != nil || len(nodes) != 1 || nodes[0].RouteBundleVersion != "route-new" || !nodes[0].Healthy {
		t.Fatalf("stable same-epoch replacement did not take authority: nodes=%+v err=%v", nodes, err)
	}
	if _, err := s.updateEdgeInstanceHeartbeatAt(old, base.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	nodes, _, err = s.ListActiveEdgeNodes(old.EdgeGroupID)
	if err != nil || len(nodes) != 1 || nodes[0].RouteBundleVersion != "route-new" {
		t.Fatalf("late old UID retook authority: nodes=%+v err=%v", nodes, err)
	}

	replacement.Node.Healthy = false
	replacement.Node.Status = model.EdgeHealthUnhealthy
	if _, err := s.updateEdgeInstanceHeartbeatAt(replacement, base.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.updateEdgeInstanceHeartbeatAt(replacement, base.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	wrong := healthyEdgeTestInstance(old.EdgeID, old.EdgeGroupID, model.EdgeSlotA, "pod-wrong", "release-wrong")
	wrong.Node.RouteBundleVersion = "route-wrong"
	if _, err := s.updateEdgeInstanceHeartbeatAt(wrong, base.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.updateEdgeInstanceHeartbeatAt(wrong, base.Add(8*time.Second)); err != nil {
		t.Fatal(err)
	}
	nodes, _, err = s.ListActiveEdgeNodes(old.EdgeGroupID)
	if err != nil || len(nodes) != 1 || nodes[0].RouteBundleVersion != "route-new" || nodes[0].Healthy {
		t.Fatalf("wrong slot/release replacement bypassed active epoch or old UID was revived: nodes=%+v err=%v", nodes, err)
	}
}
