package api

import (
	"net/http"
	"net/url"
	"path/filepath"
	"testing"

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
