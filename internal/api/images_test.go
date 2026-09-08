package api

import (
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestNodeUpdaterCanReportDistributedImageReplica(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Image Replica Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	updater, updaterToken, err := s.EnrollNodeUpdater(
		nodeSecret,
		"worker-image",
		"https://worker-image.example.com",
		nil,
		"worker-image",
		"machine-image",
		"v2",
		"join-v2",
		[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypeReplicateAppImage},
	)
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}
	image, err := s.UpsertImage(model.Image{
		TenantID:             tenant.ID,
		AppID:                "app_1",
		ImageRef:             "registry.fugue.internal:5000/fugue-apps/demo:git-abc",
		CanonicalDigest:      "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		LifecycleState:       model.ImageLifecycleAvailable,
		RequiredReplicaCount: 2,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{RegistryPullBase: "registry.fugue.internal:5000"})
	form := url.Values{}
	form.Set("image_id", image.ID)
	form.Set("app_id", image.AppID)
	form.Set("digest", image.CanonicalDigest)
	form.Set("status", model.ImageReplicaStatusPresent)
	form.Set("cache_endpoint", "http://worker-image.example.com:5000")
	recorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/image-replicas/report", updaterToken, form)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	replicas, err := s.ListImageReplicas(model.ImageReplicaFilter{ImageID: image.ID, TenantID: tenant.ID, Status: model.ImageReplicaStatusPresent})
	if err != nil {
		t.Fatalf("list image replicas: %v", err)
	}
	if len(replicas) != 1 {
		t.Fatalf("expected one reported replica, got %+v", replicas)
	}
	if replicas[0].RuntimeID != updater.RuntimeID || replicas[0].ClusterNodeName != updater.ClusterNodeName {
		t.Fatalf("expected updater target metadata, got %+v updater=%+v", replicas[0], updater)
	}
	locations, err := s.ListImageLocations(model.ImageLocationFilter{ImageRef: image.ImageRef, TenantID: tenant.ID, Status: model.ImageLocationStatusPresent})
	if err != nil {
		t.Fatalf("list compatibility image locations: %v", err)
	}
	if len(locations) != 1 || locations[0].CacheEndpoint != "http://worker-image.example.com:5000" {
		t.Fatalf("expected compatibility image location, got %+v", locations)
	}
}

func TestCreateImageReplicationTaskDispatchesExecutableNodeTask(t *testing.T) {
	t.Parallel()
	stateStore, server, readKey, app := setupSearchTestServer(t)
	_, deployKey, err := stateStore.CreateAPIKey(app.TenantID, "replication-owner", []string{"app.read", "app.deploy"})
	if err != nil {
		t.Fatal(err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(app.TenantID, "replication-node")
	if err != nil {
		t.Fatal(err)
	}
	updater, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "replication-node", "https://replication-node.example", nil, "replication-node", "machine-replication", "v1", "v1", []string{"tasks", model.NodeUpdateTaskTypeReplicateAppImage})
	if err != nil {
		t.Fatal(err)
	}
	image, err := stateStore.UpsertImage(model.Image{TenantID: app.TenantID, AppID: app.ID, ImageRef: "registry.example/app:build", CanonicalDigest: "sha256:" + strings.Repeat("a", 64), LifecycleState: model.ImageLifecycleAvailable})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := stateStore.UpsertImageReplica(model.ImageReplica{ImageID: image.ID, TenantID: app.TenantID, AppID: app.ID, NodeID: "source-machine", RuntimeID: "source-runtime", ClusterNodeName: "source-node", CacheEndpoint: "http://source.example:5000", Digest: image.CanonicalDigest, Status: model.ImageReplicaStatusPresent, LastVerifiedAt: &now, LeaseExpiresAt: ptrTime(now.Add(time.Hour))}); err != nil {
		t.Fatal(err)
	}
	var task model.ImageReplicationTask
	for _, selector := range []map[string]string{
		{"target_cluster_node_name": updater.ClusterNodeName},
		{"target_runtime_id": updater.RuntimeID},
		{"target_node_id": updater.MachineID},
	} {
		selector["image_id"] = image.ID
		response := performJSONRequest(t, server, http.MethodPost, "/v1/image-replication-tasks", deployKey, selector)
		if response.Code != http.StatusOK {
			t.Fatalf("replicate response: %d %s", response.Code, response.Body.String())
		}
		var body struct {
			Task model.ImageReplicationTask `json:"task"`
		}
		mustDecodeJSON(t, response, &body)
		if task.ID != "" && task.ID != body.Task.ID {
			t.Fatal("retry created a different replication task")
		}
		task = body.Task
	}
	if task.SourceReplicaID == "" || task.TargetNodeID != updater.MachineID || task.TargetRuntimeID != updater.RuntimeID || task.TargetClusterNodeName != updater.ClusterNodeName {
		t.Fatalf("task identities were not resolved: %+v", task)
	}
	queued, err := stateStore.ListNodeUpdateTasks(app.TenantID, false, updater.ID, model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 1 || queued[0].Type != model.NodeUpdateTaskTypeReplicateAppImage {
		t.Fatalf("expected executable node task, got %+v", queued)
	}
	if queued[0].Payload["replication_task_id"] != task.ID || queued[0].Payload["digest"] != image.CanonicalDigest {
		t.Fatal("node task is not bound to this transfer and immutable digest")
	}
	request := map[string]string{"image_id": image.ID, "target_cluster_node_name": updater.ClusterNodeName}
	if response := performJSONRequest(t, server, http.MethodPost, "/v1/image-replication-tasks", readKey, request); response.Code != http.StatusForbidden {
		t.Fatalf("read-only key can dispatch node work: %d", response.Code)
	}
	for _, invalid := range []map[string]string{
		{"image_id": image.ID},
		{"image_id": image.ID, "target_cluster_node_name": updater.ClusterNodeName, "target_runtime_id": "different-runtime"},
		{"image_id": image.ID, "target_cluster_node_name": updater.ClusterNodeName, "app_id": "different-app"},
		{"image_id": image.ID, "target_cluster_node_name": updater.ClusterNodeName, "source_cache_endpoint": "http://unreported-source.example:5000"},
	} {
		if response := performJSONRequest(t, server, http.MethodPost, "/v1/image-replication-tasks", deployKey, invalid); response.Code < 400 {
			t.Fatalf("invalid transfer accepted: %+v", invalid)
		}
	}
}

func ptrTime(value time.Time) *time.Time { return &value }
