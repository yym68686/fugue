package api

import (
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestImageCacheInventoryAndDryRunPrunePlanAPI(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Image Cache Admin Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, adminSecret, err := stateStore.CreateAPIKey(tenant.ID, "platform-admin", []string{"platform.admin"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	updaterToken := enrollImageCacheTestUpdater(t, server, nodeSecret)

	old := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339)
	report := performJSONRequest(t, server, http.MethodPost, "/v1/node-updater/image-cache/inventory", updaterToken, map[string]any{
		"endpoint":             "http://worker-1:5000",
		"cluster_node":         "worker-1",
		"manifest_total_count": 1,
		"disk": map[string]any{
			"total_bytes": 1000,
			"free_bytes":  200,
			"cache_bytes": 300,
		},
		"manifests": []map[string]any{{
			"repo":                       "fugue-apps/demo",
			"target":                     "old",
			"digest":                     "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"manifest_size_bytes":        50,
			"total_blob_bytes":           500,
			"referenced_blobs":           []string{"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
			"created_at_observed":        old,
			"present":                    true,
			"unique_blob_bytes_observed": 500,
		}},
	})
	if report.Code != http.StatusOK {
		t.Fatalf("report inventory status=%d body=%s", report.Code, report.Body.String())
	}

	forbidden := performFormRequest(t, server, http.MethodGet, "/v1/admin/image-cache/inventory", updaterToken, nil)
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("expected updater token forbidden from admin inventory, got %d body=%s", forbidden.Code, forbidden.Body.String())
	}

	inventory := performFormRequest(t, server, http.MethodGet, "/v1/admin/image-cache/inventory?cluster_node_name=worker-1", adminSecret, nil)
	if inventory.Code != http.StatusOK {
		t.Fatalf("admin inventory status=%d body=%s", inventory.Code, inventory.Body.String())
	}
	var inventoryResponse struct {
		Nodes     []model.ImageCacheNodeInventory `json:"nodes"`
		Manifests []model.ImageCacheManifest      `json:"manifests"`
	}
	mustDecodeJSON(t, inventory, &inventoryResponse)
	if len(inventoryResponse.Nodes) != 1 || inventoryResponse.Nodes[0].ManifestCount != 1 || len(inventoryResponse.Manifests) != 1 {
		t.Fatalf("unexpected inventory response: %+v", inventoryResponse)
	}

	planRequest := performJSONRequest(t, server, http.MethodPost, "/v1/admin/image-cache/prune-plan", adminSecret, map[string]any{
		"cluster_node_name": "worker-1",
		"mode":              "dry-run",
	})
	if planRequest.Code != http.StatusCreated {
		t.Fatalf("create prune plan status=%d body=%s", planRequest.Code, planRequest.Body.String())
	}
	var planResponse struct {
		Plan model.ImageCachePrunePlan `json:"plan"`
		Task model.NodeUpdateTask      `json:"task"`
	}
	mustDecodeJSON(t, planRequest, &planResponse)
	if planResponse.Plan.CandidateManifestCount != 1 {
		t.Fatalf("candidate count = %d, want 1: %+v", planResponse.Plan.CandidateManifestCount, planResponse.Plan)
	}
	if planResponse.Task.Type != model.NodeUpdateTaskTypePruneImageCache || planResponse.Task.Payload["allow_delete"] != "false" || planResponse.Task.Payload["dry_run"] != "true" {
		t.Fatalf("unexpected prune task: %+v", planResponse.Task)
	}
}

func TestImageCacheInventoryAndLocalPVAdminListsReturnEmptyArrays(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Empty Storage Admin Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, adminSecret, err := stateStore.CreateAPIKey(tenant.ID, "platform-admin", []string{"platform.admin"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})

	imageInventory := performFormRequest(t, server, http.MethodGet, "/v1/admin/image-cache/inventory?cluster_node_name=missing-node", adminSecret, nil)
	if imageInventory.Code != http.StatusOK {
		t.Fatalf("image inventory status=%d body=%s", imageInventory.Code, imageInventory.Body.String())
	}
	var imageResponse struct {
		Nodes     []model.ImageCacheNodeInventory `json:"nodes"`
		Manifests []model.ImageCacheManifest      `json:"manifests"`
	}
	mustDecodeJSON(t, imageInventory, &imageResponse)
	if imageResponse.Nodes == nil || imageResponse.Manifests == nil || len(imageResponse.Nodes) != 0 || len(imageResponse.Manifests) != 0 {
		t.Fatalf("expected empty arrays, got %+v", imageResponse)
	}

	localPVInventory := performFormRequest(t, server, http.MethodGet, "/v1/admin/localpv/inventory?cluster_node_name=missing-node", adminSecret, nil)
	if localPVInventory.Code != http.StatusOK {
		t.Fatalf("localpv inventory status=%d body=%s", localPVInventory.Code, localPVInventory.Body.String())
	}
	var localPVResponse struct {
		Inventories []model.LocalPVInventory `json:"inventories"`
	}
	mustDecodeJSON(t, localPVInventory, &localPVResponse)
	if localPVResponse.Inventories == nil || len(localPVResponse.Inventories) != 0 {
		t.Fatalf("expected empty inventories array, got %+v", localPVResponse)
	}
}

func TestLocalPVInventoryAPIRecomputesEligibility(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("LocalPV Admin Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, adminSecret, err := stateStore.CreateAPIKey(tenant.ID, "platform-admin", []string{"platform.admin"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	updaterToken := enrollImageCacheTestUpdater(t, server, nodeSecret)

	report := performJSONRequest(t, server, http.MethodPost, "/v1/node-updater/localpv/inventory", updaterToken, map[string]any{
		"inventory": map[string]any{
			"cluster_node_name":    "worker-1",
			"vg_name":              "fugue-vg",
			"image_path":           "/var/lib/fugue/lvm-localpv/fugue-vg.img",
			"image_size_bytes":     1024,
			"loop_device":          "/dev/loop9",
			"loop_backing_file":    "/var/lib/fugue/lvm-localpv/fugue-vg.img",
			"lv_count":             1,
			"active_lv_count":      1,
			"bound_pv_count":       1,
			"bound_pvc_refs":       []string{"tenant/demo-db"},
			"safe_to_decommission": true,
			"observed_at":          time.Now().UTC().Format(time.RFC3339),
		},
	})
	if report.Code != http.StatusOK {
		t.Fatalf("report localpv status=%d body=%s", report.Code, report.Body.String())
	}

	list := performFormRequest(t, server, http.MethodGet, "/v1/admin/localpv/inventory?cluster_node_name=worker-1", adminSecret, nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list localpv status=%d body=%s", list.Code, list.Body.String())
	}
	var response struct {
		Inventories []model.LocalPVInventory `json:"inventories"`
	}
	mustDecodeJSON(t, list, &response)
	if len(response.Inventories) != 1 || response.Inventories[0].SafeToDecommission || response.Inventories[0].BoundPVCount != 1 {
		t.Fatalf("expected unsafe LocalPV inventory, got %+v", response.Inventories)
	}
}

func TestImageCachePrunePlanClassifiesStaleReplicaAPI(t *testing.T) {
	t.Parallel()

	stateStore, adminSecret, updaterToken, server := newImageCacheAdminAPITest(t, "Stale Replica API Tenant")
	digest := "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	reportImageCacheTestManifest(t, server, updaterToken, digest)
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:        "tenant_1",
		AppID:           "app_1",
		ImageRef:        "registry.fugue.internal:5000/fugue-apps/demo:old",
		CanonicalDigest: digest,
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        image.TenantID,
		AppID:           image.AppID,
		Digest:          digest,
		NodeID:          "machine-1",
		RuntimeID:       "runtime-1",
		ClusterNodeName: "worker-1",
		Status:          model.ImageReplicaStatusStale,
	}); err != nil {
		t.Fatalf("upsert stale replica: %v", err)
	}

	planRequest := performFormRequest(t, server, http.MethodGet, "/v1/admin/image-cache/prune-plan?cluster_node_name=worker-1", adminSecret, nil)
	if planRequest.Code != http.StatusOK {
		t.Fatalf("get prune plan status=%d body=%s", planRequest.Code, planRequest.Body.String())
	}
	var response struct {
		Plan model.ImageCachePrunePlan `json:"plan"`
	}
	mustDecodeJSON(t, planRequest, &response)
	if len(response.Plan.Candidates) != 1 || response.Plan.Candidates[0].Reason != "stale_replica" {
		t.Fatalf("expected stale replica candidate, got %+v", response.Plan)
	}
}

func TestImageCachePrunePlanClassifiesDeletedGenerationAPI(t *testing.T) {
	t.Parallel()

	stateStore, adminSecret, updaterToken, server := newImageCacheAdminAPITest(t, "Deleted Generation API Tenant")
	digest := "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	reportImageCacheTestManifest(t, server, updaterToken, digest)
	if _, err := stateStore.UpsertImage(model.Image{
		TenantID:        "tenant_1",
		AppID:           "app_1",
		ImageRef:        "registry.fugue.internal:5000/fugue-apps/demo:old",
		CanonicalDigest: digest,
		LifecycleState:  model.ImageLifecycleDeleted,
	}); err != nil {
		t.Fatalf("upsert deleted image: %v", err)
	}

	planRequest := performFormRequest(t, server, http.MethodGet, "/v1/admin/image-cache/prune-plan?cluster_node_name=worker-1", adminSecret, nil)
	if planRequest.Code != http.StatusOK {
		t.Fatalf("get prune plan status=%d body=%s", planRequest.Code, planRequest.Body.String())
	}
	var response struct {
		Plan model.ImageCachePrunePlan `json:"plan"`
	}
	mustDecodeJSON(t, planRequest, &response)
	if len(response.Plan.Candidates) != 1 || response.Plan.Candidates[0].Reason != "deleted_image_generation" {
		t.Fatalf("expected deleted generation candidate, got %+v", response.Plan)
	}
}

func TestEvaluateLocalPVDecommissionSafetyGates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   model.LocalPVInventory
		safe bool
	}{
		{
			name: "empty vg is eligible",
			in: model.LocalPVInventory{
				ImagePath:       "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				ImageSizeBytes:  1024,
				LoopDevice:      "/dev/loop9",
				LoopBackingFile: "/var/lib/fugue/lvm-localpv/fugue-vg.img",
			},
			safe: true,
		},
		{
			name: "active lv refused",
			in: model.LocalPVInventory{
				ImagePath:       "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				ImageSizeBytes:  1024,
				LoopDevice:      "/dev/loop9",
				LoopBackingFile: "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				LVCount:         1,
				ActiveLVCount:   1,
			},
		},
		{
			name: "missing loop refused",
			in: model.LocalPVInventory{
				ImagePath:      "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				ImageSizeBytes: 1024,
			},
		},
		{
			name: "path mismatch refused",
			in: model.LocalPVInventory{
				ImagePath:       "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				ImageSizeBytes:  1024,
				LoopDevice:      "/dev/loop9",
				LoopBackingFile: "/tmp/other.img",
			},
		},
		{
			name: "bound pv refused",
			in: model.LocalPVInventory{
				ImagePath:       "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				ImageSizeBytes:  1024,
				LoopDevice:      "/dev/loop9",
				LoopBackingFile: "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				BoundPVCount:    1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			safe, reasons := evaluateLocalPVDecommissionSafety(tt.in)
			if safe != tt.safe {
				t.Fatalf("safe=%t want %t reasons=%v", safe, tt.safe, reasons)
			}
			if !tt.safe && len(reasons) == 0 {
				t.Fatal("expected unsafe reasons")
			}
		})
	}
}

func newImageCacheAdminAPITest(t *testing.T, tenantName string) (*store.Store, string, string, *Server) {
	t.Helper()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant(tenantName)
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, adminSecret, err := stateStore.CreateAPIKey(tenant.ID, "platform-admin", []string{"platform.admin"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	updaterToken := enrollImageCacheTestUpdater(t, server, nodeSecret)
	return stateStore, adminSecret, updaterToken, server
}

func reportImageCacheTestManifest(t *testing.T, server *Server, updaterToken, digest string) {
	t.Helper()
	old := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339)
	report := performJSONRequest(t, server, http.MethodPost, "/v1/node-updater/image-cache/inventory", updaterToken, map[string]any{
		"endpoint":             "http://worker-1:5000",
		"cluster_node":         "worker-1",
		"manifest_total_count": 1,
		"disk": map[string]any{
			"total_bytes": 1000,
			"free_bytes":  200,
			"cache_bytes": 300,
		},
		"manifests": []map[string]any{{
			"repo":                       "fugue-apps/demo",
			"target":                     "old",
			"digest":                     digest,
			"manifest_size_bytes":        50,
			"total_blob_bytes":           500,
			"referenced_blobs":           []string{"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
			"created_at_observed":        old,
			"present":                    true,
			"unique_blob_bytes_observed": 500,
		}},
	})
	if report.Code != http.StatusOK {
		t.Fatalf("report inventory status=%d body=%s", report.Code, report.Body.String())
	}
}

func enrollImageCacheTestUpdater(t *testing.T, server *Server, nodeSecret string) string {
	t.Helper()
	form := url.Values{}
	form.Set("node_key", nodeSecret)
	form.Set("node_name", "worker-1")
	form.Set("machine_name", "machine-1")
	form.Set("machine_fingerprint", "machine-1")
	form.Set("endpoint", "https://worker-1.example.com")
	form.Set("updater_version", "v10")
	form.Set("join_script_version", "join-v10")
	form.Set("capabilities", "heartbeat,tasks,report-image-cache-inventory,prune-image-cache,report-lvm-localpv-inventory,decommission-lvm-localpv")
	recorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/enroll", "", form)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("enroll updater status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	token := parseEnvResponse(recorder.Body.String())["FUGUE_NODE_UPDATER_TOKEN"]
	if token == "" {
		t.Fatalf("missing updater token in %q", recorder.Body.String())
	}
	return token
}
