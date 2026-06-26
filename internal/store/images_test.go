package store

import (
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestImageStoreReplicaPinAndTaskSemantics(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	image, err := s.UpsertImage(model.Image{
		TenantID:                 "tenant_1",
		AppID:                    "app_1",
		ImageRef:                 "registry.fugue.internal:5000/fugue-apps/demo:git-abc",
		CanonicalDigest:          "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		LifecycleState:           model.ImageLifecycleAvailable,
		RequiredReplicaCount:     2,
		MinAvailableReplicaCount: 2,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	now := time.Now().UTC()
	first, err := s.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        image.TenantID,
		AppID:           image.AppID,
		RuntimeID:       "runtime_1",
		ClusterNodeName: "worker-1",
		CacheEndpoint:   "http://worker-1.example.com:5000/",
		Status:          model.ImageReplicaStatusPresent,
		LastVerifiedAt:  &now,
	})
	if err != nil {
		t.Fatalf("upsert replica: %v", err)
	}
	second, err := s.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        image.TenantID,
		AppID:           image.AppID,
		RuntimeID:       "runtime_1",
		ClusterNodeName: "worker-1",
		CacheEndpoint:   "http://worker-1.example.com:5000",
		Status:          model.ImageReplicaStatusPresent,
		LastVerifiedAt:  &now,
	})
	if err != nil {
		t.Fatalf("upsert duplicate replica: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("expected duplicate target to update same replica, got %s and %s", first.ID, second.ID)
	}
	staleCutoff := now.Add(time.Minute)
	stale, err := s.MarkStaleImageReplicas(staleCutoff)
	if err != nil {
		t.Fatalf("mark stale replicas: %v", err)
	}
	if stale != 1 {
		t.Fatalf("expected one stale replica, got %d", stale)
	}
	replicas, err := s.ListImageReplicas(model.ImageReplicaFilter{ImageID: image.ID, TenantID: image.TenantID, Status: model.ImageReplicaStatusStale})
	if err != nil {
		t.Fatalf("list stale replicas: %v", err)
	}
	if len(replicas) != 1 || replicas[0].ID != first.ID {
		t.Fatalf("expected first replica stale, got %+v", replicas)
	}

	pin, err := s.UpsertImagePin(model.ImagePin{
		ImageID:     image.ID,
		TenantID:    image.TenantID,
		AppID:       image.AppID,
		OperationID: "op_1",
		Reason:      model.ImagePinReasonCurrentDeploy,
		MinReplicas: 2,
	})
	if err != nil {
		t.Fatalf("upsert pin: %v", err)
	}
	if err := s.DeleteImagePin(pin.ID, image.TenantID, false); err != nil {
		t.Fatalf("delete tenant pin: %v", err)
	}
	pins, err := s.ListImagePins(model.ImagePinFilter{ImageID: image.ID, TenantID: image.TenantID})
	if err != nil {
		t.Fatalf("list pins: %v", err)
	}
	if len(pins) != 0 {
		t.Fatalf("expected pin deleted, got %+v", pins)
	}

	task, err := s.UpsertImageReplicationTask(model.ImageReplicationTask{
		ImageID:               image.ID,
		TenantID:              image.TenantID,
		AppID:                 image.AppID,
		TargetRuntimeID:       "runtime_2",
		TargetClusterNodeName: "worker-2",
		Priority:              model.ImageReplicationPriorityRepair,
		Status:                model.ImageReplicationTaskStatusPending,
	})
	if err != nil {
		t.Fatalf("upsert replication task: %v", err)
	}
	duplicate, err := s.UpsertImageReplicationTask(model.ImageReplicationTask{
		ImageID:               image.ID,
		TenantID:              image.TenantID,
		AppID:                 image.AppID,
		TargetRuntimeID:       "runtime_2",
		TargetClusterNodeName: "worker-2",
		Priority:              model.ImageReplicationPriorityRepair,
		Status:                model.ImageReplicationTaskStatusPending,
	})
	if err != nil {
		t.Fatalf("upsert duplicate replication task: %v", err)
	}
	if duplicate.ID != task.ID {
		t.Fatalf("expected task dedupe, got %s and %s", task.ID, duplicate.ID)
	}
	deployBlocking, err := s.UpsertImageReplicationTask(model.ImageReplicationTask{
		ImageID:               image.ID,
		TenantID:              image.TenantID,
		AppID:                 image.AppID,
		TargetRuntimeID:       "runtime_3",
		TargetClusterNodeName: "worker-3",
		Priority:              model.ImageReplicationPriorityDeployBlocking,
		Status:                model.ImageReplicationTaskStatusPending,
	})
	if err != nil {
		t.Fatalf("upsert deploy-blocking replication task: %v", err)
	}
	pendingTasks, err := s.ListImageReplicationTasks(model.ImageReplicationTaskFilter{
		ImageID:  image.ID,
		TenantID: image.TenantID,
		Status:   model.ImageReplicationTaskStatusPending,
	})
	if err != nil {
		t.Fatalf("list pending replication tasks: %v", err)
	}
	if len(pendingTasks) != 2 {
		t.Fatalf("expected repair and deploy-blocking pending tasks, got %+v", pendingTasks)
	}
	foundDeployBlocking := false
	for _, pending := range pendingTasks {
		if pending.ID == deployBlocking.ID {
			foundDeployBlocking = true
		}
	}
	if !foundDeployBlocking {
		t.Fatalf("expected status-only task filter to include deploy-blocking task, got %+v", pendingTasks)
	}
}
