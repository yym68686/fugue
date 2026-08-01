package store

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

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
	if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{ID: edgeID, EdgeGroupID: groupID, Status: model.EdgeHealthHealthy, Healthy: true}); err == nil {
		t.Fatal("legacy flat heartbeat write must fail closed after instance schema activation")
	}
	t.Cleanup(func() {
		_, _ = s.db.Exec(`DELETE FROM fugue_edge_groups WHERE id=$1`, groupID)
	})
	if _, err := s.PutEdgeActiveEpoch(model.EdgeActiveEpoch{
		EdgeGroupID: groupID, Slot: model.EdgeSlotB, ReleaseEpoch: "release-b", FenceSequence: 1, MinHealthyInstances: 1,
	}); err != nil {
		t.Fatalf("put postgres active epoch: %v", err)
	}
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
	assertOnlyActiveEdge(t, s, groupID, "release-b", true)

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
		_, _ = s.db.Exec(`DELETE FROM fugue_edge_groups WHERE id=$1`, groupID)
	})
	if _, err := s.PutEdgeActiveEpoch(model.EdgeActiveEpoch{EdgeGroupID: groupID, Slot: model.EdgeSlotB, ReleaseEpoch: "release-b", FenceSequence: 1}); err != nil {
		t.Fatalf("put active epoch: %v", err)
	}
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
	assertOnlyActiveEdge(t, s, groupID, "release-b", true)
	instances, _, err := s.ListEdgeNodeInstances(groupID)
	if err != nil {
		t.Fatalf("list postgres concurrent instance: %v", err)
	}
	if len(instances) != 1 || instances[0].ConsecutiveHealthy != 32 {
		t.Fatalf("postgres concurrent replica writes lost observations: %+v", instances)
	}
}
