package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEdgeInstancePostgresSameEpochPodReplacementFencesLateOldUID(t *testing.T) {
	databaseURL := requireEdgePostgresTestURL(t)
	s := New("", databaseURL)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	suffix := strings.ToLower(model.NewID("edgepguid"))
	edgeID, groupID := "edge-"+suffix, "edge-group-"+suffix
	if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{ID: edgeID, EdgeGroupID: groupID, Status: model.EdgeHealthUnknown}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resetPostgresEdgeActivationTestState(t, s, groupID) })
	old := healthyEdgeTestInstance(edgeID, groupID, model.EdgeSlotB, "pod-old", "release-b")
	old.Node.RouteBundleVersion = "route-old"
	state, _ := s.GetEdgeActivationState()
	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseShadow, nil, nil, "")
	heartbeatEdgeInstanceTwice(t, s, old)
	expected := []model.EdgeExpectedInstance{{EdgeID: edgeID, EdgeGroupID: groupID, Slot: old.Slot, InstanceUID: old.InstanceUID, ReleaseEpoch: old.ReleaseEpoch}}
	epochs := []model.EdgeActiveEpoch{{EdgeGroupID: groupID, Slot: old.Slot, ReleaseEpoch: old.ReleaseEpoch, FenceSequence: 1, MinHealthyInstances: 1}}
	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseFenced, expected, epochs, "")
	_ = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseActive, expected, nil, "api-generation-pg-replacement")
	replacement := healthyEdgeTestInstance(edgeID, groupID, old.Slot, "pod-new", old.ReleaseEpoch)
	replacement.Node.RouteBundleVersion = "route-new"
	if _, err := s.UpdateEdgeInstanceHeartbeat(replacement); err != nil {
		t.Fatal(err)
	}
	assertActiveRouteVersion(t, s, groupID, "route-old", true)
	if _, err := s.UpdateEdgeInstanceHeartbeat(replacement); err != nil {
		t.Fatal(err)
	}
	assertActiveRouteVersion(t, s, groupID, "route-new", true)
	if _, err := s.UpdateEdgeInstanceHeartbeat(old); err != nil {
		t.Fatal(err)
	}
	assertActiveRouteVersion(t, s, groupID, "route-new", true)
	wrong := healthyEdgeTestInstance(edgeID, groupID, model.EdgeSlotA, "pod-wrong", "release-wrong")
	wrong.Node.RouteBundleVersion = "route-wrong"
	heartbeatEdgeInstanceTwice(t, s, wrong)
	assertActiveRouteVersion(t, s, groupID, "route-new", true)
}

func TestEdgeActivationFilePostgresSemanticParity(t *testing.T) {
	databaseURL := requireEdgePostgresTestURL(t)
	pg := New("", databaseURL)
	if err := pg.Init(); err != nil {
		t.Fatal(err)
	}
	file := New(filepath.Join(t.TempDir(), "store.json"))
	if err := file.Init(); err != nil {
		t.Fatal(err)
	}
	suffix := strings.ToLower(model.NewID("edgeparity"))
	edgeID, groupID := "edge-"+suffix, "edge-group-"+suffix
	t.Cleanup(func() { resetPostgresEdgeActivationTestState(t, pg, groupID) })
	type paritySnapshot struct {
		Activation model.EdgeActivationState `json:"activation"`
		Epochs     []model.EdgeActiveEpoch   `json:"epochs"`
		Instances  []model.EdgeNodeInstance  `json:"instances"`
	}
	run := func(s *Store) paritySnapshot {
		if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{ID: edgeID, EdgeGroupID: groupID, Status: model.EdgeHealthUnknown}); err != nil {
			t.Fatal(err)
		}
		state, _ := s.GetEdgeActivationState()
		state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseShadow, nil, nil, "")
		active := healthyEdgeTestInstance(edgeID, groupID, model.EdgeSlotB, "pod-b", "release-b")
		heartbeatEdgeInstanceTwice(t, s, active)
		expected := []model.EdgeExpectedInstance{{EdgeID: edgeID, EdgeGroupID: groupID, Slot: active.Slot, InstanceUID: active.InstanceUID, ReleaseEpoch: active.ReleaseEpoch}}
		epochs := []model.EdgeActiveEpoch{{EdgeGroupID: groupID, Slot: active.Slot, ReleaseEpoch: active.ReleaseEpoch, FenceSequence: 3, MinHealthyInstances: 1}}
		state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseFenced, expected, epochs, "")
		state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseActive, expected, nil, "api-generation-parity")
		inactive := healthyEdgeTestInstance(edgeID, groupID, model.EdgeSlotA, "pod-a", "release-old")
		inactive.Node.Healthy = false
		inactive.Node.Status = model.EdgeHealthUnhealthy
		inactive.FailureClass = model.EdgeInstanceFailureSignatureInvalid
		if _, err := s.UpdateEdgeInstanceHeartbeat(inactive); err != nil {
			t.Fatal(err)
		}
		target := model.EdgeRemediationTarget{EdgeID: edgeID, EdgeGroupID: groupID, Slot: inactive.Slot, InstanceUID: inactive.InstanceUID, ReleaseEpoch: inactive.ReleaseEpoch, DaemonSetName: "fugue-worker-a", DaemonSetUID: "daemonset-a", DaemonSetVersion: "9", FailureClass: inactive.FailureClass}
		prepare := edgeRemediationTestAdvance(state, 0, model.EdgeRemediationPhasePrepared, target)
		state, _ = s.AdvanceEdgeRemediation(prepare)
		commit := edgeRemediationTestAdvance(state, state.Remediation.Sequence, model.EdgeRemediationPhaseCommitted, target)
		commit.ReleaseFence, commit.Nonce = state.Remediation.ReleaseFence, state.Remediation.Nonce
		state, _ = s.AdvanceEdgeRemediation(commit)
		verified := edgeRemediationTestAdvance(state, state.Remediation.Sequence, model.EdgeRemediationPhaseVerified, target)
		verified.ReleaseFence, verified.Nonce = state.Remediation.ReleaseFence, state.Remediation.Nonce
		state, _ = s.AdvanceEdgeRemediation(verified)
		instances, activeEpochs, err := s.ListEdgeNodeInstances(groupID)
		if err != nil {
			t.Fatal(err)
		}
		return paritySnapshot{Activation: state, Epochs: activeEpochs, Instances: instances}
	}
	fileState, pgState := run(file), run(pg)
	if left, right := canonicalEdgeActivationSemantics(t, fileState), canonicalEdgeActivationSemantics(t, pgState); !bytes.Equal(left, right) {
		t.Fatalf("file/PG activation semantics differ:\nfile=%s\npg=%s", left, right)
	}
}

func canonicalEdgeActivationSemantics(t *testing.T, state any) []byte {
	t.Helper()
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	timeFields := map[string]struct{}{"created_at": {}, "updated_at": {}, "recorded_at": {}, "activated_at": {}, "last_heartbeat_at": {}, "health_state_since": {}, "soak_started_at": {}, "last_seen_at": {}, "tls_ready_at": {}, "public_probe_last_at": {}}
	var strip func(any)
	strip = func(v any) {
		switch typed := v.(type) {
		case map[string]any:
			for key, child := range typed {
				if _, ok := timeFields[key]; ok {
					if child != nil {
						typed[key] = "<canonical-server-time>"
					}
					continue
				}
				strip(child)
			}
		case []any:
			for _, child := range typed {
				strip(child)
			}
		}
	}
	strip(value)
	out, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func assertActiveRouteVersion(t *testing.T, s *Store, groupID, version string, healthy bool) {
	t.Helper()
	nodes, _, err := s.ListActiveEdgeNodes(groupID)
	if err != nil || len(nodes) != 1 || nodes[0].RouteBundleVersion != version || nodes[0].Healthy != healthy {
		t.Fatalf("active route version=%s healthy=%v nodes=%+v err=%v", version, healthy, nodes, err)
	}
}

func requireEdgePostgresTestURL(t *testing.T) string {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL")
	}
	lower := strings.ToLower(databaseURL)
	if !strings.Contains(lower, "test") || (!strings.Contains(lower, "127.0.0.1") && !strings.Contains(lower, "localhost")) {
		t.Fatalf("refusing non-loopback/non-test database %q", databaseURL)
	}
	return databaseURL
}

func TestEdgeInstanceFencingPostgresParity(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run edge instance fencing Postgres integration test")
	}
	if !strings.Contains(strings.ToLower(databaseURL), "test") {
		t.Fatalf("refusing to run edge instance integration test against non-test database URL %q", databaseURL)
	}
	s := New("", databaseURL)
	if err := s.Init(); err != nil {
		t.Fatalf("init postgres store: %v", err)
	}
	suffix := strings.ToLower(model.NewID("edgepg"))
	edgeID := "edge-" + suffix
	groupID := "edge-group-" + suffix
	if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{ID: edgeID, EdgeGroupID: groupID, Status: model.EdgeHealthUnknown}); err != nil {
		t.Fatalf("create postgres edge control identity: %v", err)
	}
	if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{ID: edgeID, EdgeGroupID: groupID, Status: model.EdgeHealthHealthy, Healthy: true}); err != nil {
		t.Fatalf("phase0 legacy heartbeat must remain compatible: %v", err)
	}
	t.Cleanup(func() {
		resetPostgresEdgeActivationTestState(t, s, groupID)
	})
	activation, _ := s.GetEdgeActivationState()
	activation = advanceEdgeActivationTest(t, s, activation, model.EdgeActivationPhaseShadow, nil, nil, "")
	instance := healthyEdgeTestInstance(edgeID, groupID, model.EdgeSlotB, "pod-b", "release-b")
	fakeCallerTime := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	first, err := s.updateEdgeInstanceHeartbeatAt(instance, fakeCallerTime)
	if err != nil {
		t.Fatalf("first postgres heartbeat: %v", err)
	}
	if first.LastHeartbeatAt.Equal(fakeCallerTime) || time.Since(first.LastHeartbeatAt) > time.Minute {
		t.Fatalf("postgres heartbeat must use canonical server time, got %s", first.LastHeartbeatAt)
	}
	if _, err := s.updateEdgeInstanceHeartbeatAt(instance, fakeCallerTime); err != nil {
		t.Fatalf("second postgres heartbeat: %v", err)
	}
	expected := []model.EdgeExpectedInstance{{EdgeID: edgeID, EdgeGroupID: groupID, Slot: model.EdgeSlotB, InstanceUID: "pod-b", ReleaseEpoch: "release-b"}}
	epochs := []model.EdgeActiveEpoch{{EdgeGroupID: groupID, Slot: model.EdgeSlotB, ReleaseEpoch: "release-b", FenceSequence: 1, MinHealthyInstances: 1}}
	activation = advanceEdgeActivationTest(t, s, activation, model.EdgeActivationPhaseFenced, expected, epochs, "")
	activation = advanceEdgeActivationTest(t, s, activation, model.EdgeActivationPhaseActive, expected, nil, "api-generation-pg")
	assertOnlyActiveEdge(t, s, groupID, "release-b", true)
	activation = advanceEdgeActivationTest(t, s, activation, model.EdgeActivationPhaseEnforced, expected, nil, "api-generation-pg")
	if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{ID: edgeID, EdgeGroupID: groupID, Status: model.EdgeHealthHealthy, Healthy: true}); err == nil {
		t.Fatal("phase4 legacy flat heartbeat write must fail closed")
	}

	reopened := New("", databaseURL)
	if err := reopened.Init(); err != nil {
		t.Fatalf("reopen postgres store: %v", err)
	}
	assertOnlyActiveEdge(t, reopened, groupID, "release-b", true)
}

func TestEdgeInstanceFencingPostgresConcurrentReplicaWrites(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run edge instance concurrency Postgres integration test")
	}
	if !strings.Contains(strings.ToLower(databaseURL), "test") {
		t.Fatalf("refusing to run edge instance integration test against non-test database URL %q", databaseURL)
	}
	s := New("", databaseURL)
	if err := s.Init(); err != nil {
		t.Fatalf("init postgres store: %v", err)
	}
	suffix := strings.ToLower(model.NewID("edgepg"))
	edgeID := "edge-" + suffix
	groupID := "edge-group-" + suffix
	if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{ID: edgeID, EdgeGroupID: groupID, Status: model.EdgeHealthUnknown}); err != nil {
		t.Fatalf("create postgres edge control identity: %v", err)
	}
	t.Cleanup(func() {
		resetPostgresEdgeActivationTestState(t, s, groupID)
	})
	activation, _ := s.GetEdgeActivationState()
	activation = advanceEdgeActivationTest(t, s, activation, model.EdgeActivationPhaseShadow, nil, nil, "")
	var wg sync.WaitGroup
	for index := 0; index < 16; index++ {
		index := index
		wg.Add(1)
		go func() {
			defer wg.Done()
			instance := healthyEdgeTestInstance(edgeID, groupID, model.EdgeSlotB, "pod-b", "release-b")
			for observation := 0; observation < 2; observation++ {
				if _, err := s.UpdateEdgeInstanceHeartbeat(instance); err != nil {
					t.Errorf("postgres concurrent heartbeat %d: %v", index, err)
				}
			}
		}()
	}
	wg.Wait()
	expected := []model.EdgeExpectedInstance{{EdgeID: edgeID, EdgeGroupID: groupID, Slot: model.EdgeSlotB, InstanceUID: "pod-b", ReleaseEpoch: "release-b"}}
	epochs := []model.EdgeActiveEpoch{{EdgeGroupID: groupID, Slot: model.EdgeSlotB, ReleaseEpoch: "release-b", FenceSequence: 1}}
	activation = advanceEdgeActivationTest(t, s, activation, model.EdgeActivationPhaseFenced, expected, epochs, "")
	_ = advanceEdgeActivationTest(t, s, activation, model.EdgeActivationPhaseActive, expected, nil, "api-generation-pg")
	assertOnlyActiveEdge(t, s, groupID, "release-b", true)
	instances, _, err := s.ListEdgeNodeInstances(groupID)
	if err != nil {
		t.Fatalf("list postgres concurrent instance: %v", err)
	}
	if len(instances) != 1 || instances[0].ConsecutiveHealthy != 32 {
		t.Fatalf("postgres concurrent replica writes lost observations: %+v", instances)
	}
}

func TestEdgeRemediationPostgresCrashCASAndTerminalPhases(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run edge remediation Postgres integration test")
	}
	if !strings.Contains(strings.ToLower(databaseURL), "test") {
		t.Fatalf("refusing to run edge remediation integration test against non-test database URL %q", databaseURL)
	}
	s := New("", databaseURL)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	suffix := strings.ToLower(model.NewID("edgeremediationpg"))
	edgeID, groupID := "edge-"+suffix, "edge-group-"+suffix
	if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{ID: edgeID, EdgeGroupID: groupID, Status: model.EdgeHealthUnknown}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resetPostgresEdgeActivationTestState(t, s, groupID) })
	state, _ := s.GetEdgeActivationState()
	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseShadow, nil, nil, "")
	active := healthyEdgeTestInstance(edgeID, groupID, model.EdgeSlotB, "pod-b", "release-b")
	for index := 0; index < 2; index++ {
		if _, err := s.UpdateEdgeInstanceHeartbeat(active); err != nil {
			t.Fatal(err)
		}
	}
	expected := []model.EdgeExpectedInstance{{EdgeID: edgeID, EdgeGroupID: groupID, Slot: active.Slot, InstanceUID: active.InstanceUID, ReleaseEpoch: active.ReleaseEpoch}}
	epochs := []model.EdgeActiveEpoch{{EdgeGroupID: groupID, Slot: active.Slot, ReleaseEpoch: active.ReleaseEpoch, FenceSequence: 9, MinHealthyInstances: 1}}
	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseFenced, expected, epochs, "")
	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseActive, expected, nil, "api-generation-pg")

	// A crash after Phase3 can restore the durable legacy LKG before any Edge
	// mutation. This uses the same signed CAS transition as the release resume.
	rolledBack, err := s.AdvanceEdgeActivation(edgeActivationTestAdvance(state, model.EdgeActivationActionRollback, nil, nil, ""))
	if err != nil || rolledBack.RouteAuthority != model.EdgeRouteAuthorityLegacy {
		t.Fatalf("incomplete Phase3 rollback: %+v err=%v", rolledBack, err)
	}
	state = advanceEdgeActivationTest(t, s, rolledBack, model.EdgeActivationPhaseShadow, nil, nil, "")
	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseFenced, expected, epochs, "")
	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseActive, expected, nil, "api-generation-pg")

	inactive := healthyEdgeTestInstance(edgeID, groupID, model.EdgeSlotA, "pod-a", "release-old")
	inactive.Node.Healthy = false
	inactive.Node.Status = model.EdgeHealthUnhealthy
	inactive.FailureClass = model.EdgeInstanceFailureSignatureInvalid
	if _, err := s.UpdateEdgeInstanceHeartbeat(inactive); err != nil {
		t.Fatal(err)
	}
	target := model.EdgeRemediationTarget{EdgeID: edgeID, EdgeGroupID: groupID, Slot: inactive.Slot, InstanceUID: inactive.InstanceUID, ReleaseEpoch: inactive.ReleaseEpoch, DaemonSetName: "fugue-worker-a", DaemonSetUID: "daemonset-a", DaemonSetVersion: "51", FailureClass: inactive.FailureClass}
	prepare := edgeRemediationTestAdvance(state, 0, model.EdgeRemediationPhasePrepared, target)
	prepared, err := s.AdvanceEdgeRemediation(prepare)
	if err != nil {
		t.Fatal(err)
	}
	reopened := New("", databaseURL)
	if err := reopened.Init(); err != nil {
		t.Fatal(err)
	}
	crashState, _ := reopened.GetEdgeActivationState()
	if crashState.Remediation == nil || crashState.Remediation.Phase != model.EdgeRemediationPhasePrepared || crashState.Remediation.Nonce != prepare.Nonce {
		t.Fatalf("prepared action did not survive reopen: %+v", crashState.Remediation)
	}

	commit := edgeRemediationTestAdvance(prepared, prepared.Remediation.Sequence, model.EdgeRemediationPhaseCommitted, target)
	commit.ReleaseFence = prepared.Remediation.ReleaseFence
	commit.Nonce = prepared.Remediation.Nonce
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, replica := range []*Store{s, reopened} {
		wg.Add(1)
		go func(replica *Store) {
			defer wg.Done()
			_, err := replica.AdvanceEdgeRemediation(commit)
			results <- err
		}(replica)
	}
	wg.Wait()
	close(results)
	success, conflicts := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, ErrConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected replica CAS error: %v", err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatalf("two-replica action CAS result success=%d conflicts=%d", success, conflicts)
	}
	state, _ = s.GetEdgeActivationState()
	verified := edgeRemediationTestAdvance(state, state.Remediation.Sequence, model.EdgeRemediationPhaseVerified, target)
	verified.ReleaseFence, verified.Nonce = state.Remediation.ReleaseFence, state.Remediation.Nonce
	state, err = s.AdvanceEdgeRemediation(verified)
	if err != nil || state.Remediation.Phase != model.EdgeRemediationPhaseVerified {
		t.Fatalf("verify action: %+v err=%v", state.Remediation, err)
	}
	replay := edgeRemediationTestAdvance(state, state.Remediation.Sequence, model.EdgeRemediationPhasePrepared, target)
	replay.Nonce = prepare.Nonce
	if _, err := s.AdvanceEdgeRemediation(replay); !errors.Is(err, ErrConflict) {
		t.Fatalf("action nonce replay did not fail closed: %v", err)
	}

	second := edgeRemediationTestAdvance(state, state.Remediation.Sequence, model.EdgeRemediationPhasePrepared, target)
	state, err = s.AdvanceEdgeRemediation(second)
	if err != nil {
		t.Fatal(err)
	}
	committed := edgeRemediationTestAdvance(state, state.Remediation.Sequence, model.EdgeRemediationPhaseCommitted, target)
	committed.ReleaseFence, committed.Nonce = state.Remediation.ReleaseFence, state.Remediation.Nonce
	state, err = s.AdvanceEdgeRemediation(committed)
	if err != nil {
		t.Fatal(err)
	}
	pending := edgeRemediationTestAdvance(state, state.Remediation.Sequence, model.EdgeRemediationPhaseRollbackPending, target)
	pending.ReleaseFence, pending.Nonce = state.Remediation.ReleaseFence, state.Remediation.Nonce
	state, err = s.AdvanceEdgeRemediation(pending)
	if err != nil || state.Remediation.Phase != model.EdgeRemediationPhaseRollbackPending {
		t.Fatalf("rollback-pending action: %+v err=%v", state.Remediation, err)
	}
	finalReopen := New("", databaseURL)
	if err := finalReopen.Init(); err != nil {
		t.Fatal(err)
	}
	final, _ := finalReopen.GetEdgeActivationState()
	if final.Remediation == nil || final.Remediation.Phase != model.EdgeRemediationPhaseRollbackPending || final.Remediation.Sequence != 2 {
		t.Fatalf("rollback-pending action did not survive reopen: %+v", final.Remediation)
	}
}

func resetPostgresEdgeActivationTestState(t *testing.T, s *Store, groupID string) {
	t.Helper()
	now := time.Now().UTC()
	activation := defaultEdgeActivationState(now)
	payload, err := json.Marshal(activation)
	if err != nil {
		t.Errorf("encode activation cleanup: %v", err)
		return
	}
	if _, err := s.db.Exec(`DELETE FROM fugue_meta WHERE key=$1`, edgeFlatWriteFenceMetaKey); err != nil {
		t.Errorf("clear flat fence: %v", err)
	}
	if _, err := s.db.Exec(`DELETE FROM fugue_edge_groups WHERE id=$1`, groupID); err != nil {
		t.Errorf("clear test edge group: %v", err)
	}
	if _, err := s.db.Exec(`DELETE FROM fugue_edge_active_epochs`); err != nil {
		t.Errorf("clear active epochs: %v", err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_edge_activation SET phase=$1,generation=$2,state_json=$3,created_at=$4,updated_at=$4 WHERE singleton=true`, activation.Phase, activation.Generation, payload, now); err != nil {
		t.Errorf("reset activation: %v", err)
	}
}
