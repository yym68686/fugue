package store

import (
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEdgeRouteSyncActivityRequiresSuccessfulFetch(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	node, secret, err := s.CreateEdgeNodeToken(model.EdgeNode{ID: "edge-route-sync", EdgeGroupID: "edge-group-country-us", Status: model.EdgeHealthHealthy, Healthy: true})
	if err != nil {
		t.Fatalf("create edge token: %v", err)
	}
	old := time.Now().UTC().Add(-10 * time.Minute)
	if err := s.withLockedState(true, func(state *model.State) error {
		index := findEdgeNode(state, node.ID)
		state.EdgeNodes[index].LastSeenAt = &old
		state.EdgeNodes[index].LastHeartbeatAt = &old
		return nil
	}); err != nil {
		t.Fatalf("seed stale edge activity: %v", err)
	}
	if _, err := s.AuthenticateEdgeNode(secret); err != nil {
		t.Fatalf("authenticate edge token: %v", err)
	}
	afterAuth, _, err := s.GetEdgeNode(node.ID)
	if err != nil || afterAuth.LastSeenAt == nil || !afterAuth.LastSeenAt.Equal(old) {
		t.Fatalf("authentication changed freshness evidence: node=%+v err=%v", afterAuth, err)
	}
	if err := s.RecordEdgeRouteSync(node.ID); err != nil {
		t.Fatalf("record successful route sync: %v", err)
	}
	afterSync, _, err := s.GetEdgeNode(node.ID)
	if err != nil || afterSync.LastSeenAt == nil || !afterSync.LastSeenAt.After(old) || afterSync.LastHeartbeatAt == nil || !afterSync.LastHeartbeatAt.Equal(old) {
		t.Fatalf("successful route sync did not update only last_seen_at: node=%+v err=%v", afterSync, err)
	}
}

func TestEdgeGroupExistsDoesNotRequireEdgeNodeInventory(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{ID: "edge-group-exists", EdgeGroupID: "edge-group-country-us", Status: model.EdgeHealthUnknown}); err != nil {
		t.Fatalf("create edge group: %v", err)
	}
	exists, err := s.EdgeGroupExists("edge-group-country-us")
	if err != nil || !exists {
		t.Fatalf("expected existing edge group, exists=%v err=%v", exists, err)
	}
	exists, err = s.EdgeGroupExists("edge-group-missing")
	if err != nil {
		t.Fatalf("check missing edge group: %v", err)
	}
	if exists {
		t.Fatal("missing edge group reported as existing")
	}
}

func TestEdgeNodeControlStateSurvivesHeartbeat(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:              "edge-jp-1",
		EdgeGroupID:     "edge-group-country-jp",
		WorkloadMode:    model.EdgeWorkloadModeDynamic,
		CanaryState:     model.EdgeCanaryStateJoined,
		PublicIPv4:      "203.0.113.44",
		Status:          model.EdgeHealthHealthy,
		Healthy:         true,
		CaddyRouteCount: 1,
		TLSStatus:       model.EdgeTLSStatusReady,
	}); err != nil {
		t.Fatalf("create dynamic edge heartbeat: %v", err)
	}

	if _, _, err := s.UpdateEdgeNodeControlState("edge-jp-1", model.EdgeNode{
		Draining:    true,
		CanaryState: model.EdgeCanaryStateDrained,
	}); err != nil {
		t.Fatalf("drain dynamic edge: %v", err)
	}

	heartbeat, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:              "edge-jp-1",
		EdgeGroupID:     "edge-group-country-jp",
		WorkloadMode:    model.EdgeWorkloadModeDynamic,
		PublicIPv4:      "203.0.113.44",
		Status:          model.EdgeHealthHealthy,
		Healthy:         true,
		Draining:        false,
		CaddyRouteCount: 1,
		TLSStatus:       model.EdgeTLSStatusReady,
	})
	if err != nil {
		t.Fatalf("heartbeat after drain: %v", err)
	}
	if !heartbeat.Draining || heartbeat.CanaryState != model.EdgeCanaryStateDrained {
		t.Fatalf("expected control-plane drain to survive heartbeat, got %+v", heartbeat)
	}

	undrained, _, err := s.UpdateEdgeNodeControlState("edge-jp-1", model.EdgeNode{
		Draining:     false,
		CanaryState:  model.EdgeCanaryStateCanary,
		CanaryWeight: 1,
	})
	if err != nil {
		t.Fatalf("undrain dynamic edge: %v", err)
	}
	if undrained.Draining || undrained.CanaryState != model.EdgeCanaryStateCanary || undrained.CanaryWeight != 1 {
		t.Fatalf("expected explicit undrain to restore canary, got %+v", undrained)
	}
}

func TestEdgeNodePassingProbeClearsPreviousError(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:                   "edge-jp-1",
		EdgeGroupID:          "edge-group-country-jp",
		WorkloadMode:         model.EdgeWorkloadModeDynamic,
		Status:               model.EdgeHealthHealthy,
		Healthy:              true,
		CaddyRouteCount:      1,
		TLSStatus:            model.EdgeTLSStatusReady,
		PublicProbeStatus:    model.EdgePublicProbeStatusFailing,
		PublicProbeLastError: "tls443=remote error",
	}); err != nil {
		t.Fatalf("create dynamic edge heartbeat: %v", err)
	}

	updated, _, err := s.UpdateEdgeNodeControlState("edge-jp-1", model.EdgeNode{
		PublicProbeStatus: model.EdgePublicProbeStatusPassing,
	})
	if err != nil {
		t.Fatalf("mark probe passing: %v", err)
	}
	if updated.PublicProbeStatus != model.EdgePublicProbeStatusPassing || updated.PublicProbeLastError != "" {
		t.Fatalf("expected passing probe to clear stale error, got %+v", updated)
	}
}
