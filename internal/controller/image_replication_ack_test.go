package controller

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestCompleteSatisfiedImageReplicationTasksRequiresTargetCanonicalReplica(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	image, err := s.UpsertImage(model.Image{
		TenantID:        "tenant",
		AppID:           "app",
		ImageRef:        "registry.example/app:current",
		CanonicalDigest: digest,
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("create image: %v", err)
	}
	_, err = s.UpsertImageReplicationTask(model.ImageReplicationTask{
		ID:                    "replication-1",
		ImageID:               image.ID,
		TenantID:              image.TenantID,
		TargetNodeID:          "target-node",
		TargetRuntimeID:       "target-runtime",
		TargetClusterNodeName: "target-cluster",
		Priority:              model.ImageReplicationPriorityDeployBlocking,
		Status:                model.ImageReplicationTaskStatusPending,
	})
	if err != nil {
		t.Fatalf("create replication task: %v", err)
	}
	now := time.Now().UTC()
	if _, err := s.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		Digest:          digest,
		NodeID:          "other-node",
		RuntimeID:       "other-runtime",
		ClusterNodeName: "other-cluster",
		Status:          model.ImageReplicaStatusPresent,
		LastVerifiedAt:  &now,
	}); err != nil {
		t.Fatalf("create wrong-target replica: %v", err)
	}
	svc := &Service{Store: s}
	if err := svc.completeSatisfiedImageReplicationTasks(context.Background()); err != nil {
		t.Fatalf("check unsatisfied task: %v", err)
	}
	refreshed, err := s.ListImageReplicationTasks(model.ImageReplicationTaskFilter{ImageID: image.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list replication task: %v", err)
	}
	if len(refreshed) != 1 || refreshed[0].Status != model.ImageReplicationTaskStatusPending {
		t.Fatalf("wrong-target replica must not complete task: %+v", refreshed)
	}
	if _, err := s.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		Digest:          digest,
		NodeID:          "target-node",
		RuntimeID:       "target-runtime",
		ClusterNodeName: "target-cluster",
		Status:          model.ImageReplicaStatusPresent,
		LastVerifiedAt:  &now,
	}); err != nil {
		t.Fatalf("create target replica: %v", err)
	}
	if err := svc.completeSatisfiedImageReplicationTasks(context.Background()); err != nil {
		t.Fatalf("complete satisfied task: %v", err)
	}
	refreshed, err = s.ListImageReplicationTasks(model.ImageReplicationTaskFilter{ImageID: image.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list completed replication task: %v", err)
	}
	if len(refreshed) != 1 || refreshed[0].Status != model.ImageReplicationTaskStatusCompleted {
		t.Fatalf("canonical target replica should complete task: %+v", refreshed)
	}
}
