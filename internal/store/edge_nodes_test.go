package store

import (
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

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
