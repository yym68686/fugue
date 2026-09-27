package api

import (
	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
	"net/http"
	"path/filepath"
	"testing"
)

func TestStorageGrowthAPIPreservesHAAndNodePin(t *testing.T) {
	for _, pin := range []string{"", "worker"} {
		t.Run("pin="+pin, func(t *testing.T) {
			s := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := s.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := s.CreateTenant("Resize test")
			if err != nil {
				t.Fatal(err)
			}
			project, err := s.CreateProject(tenant.ID, "sample", "")
			if err != nil {
				t.Fatal(err)
			}
			primary, _, err := s.CreateRuntime(tenant.ID, "primary", model.RuntimeTypeManagedOwned, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			standby, _, err := s.CreateRuntime(tenant.ID, "standby", model.RuntimeTypeManagedOwned, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			_, key, err := s.CreateAPIKey(tenant.ID, "operator", []string{"app.write"})
			if err != nil {
				t.Fatal(err)
			}
			app, err := s.CreateApp(tenant.ID, project.ID, "sample", "", model.AppSpec{RuntimeID: primary.ID, Image: "example/app:v1", Replicas: 1, Postgres: &model.AppPostgresSpec{RuntimeID: primary.ID, FailoverTargetRuntimeID: standby.ID, Instances: 2, SynchronousReplicas: 1, PrimaryNodeName: "worker", StorageSize: "5Gi", StorageClassName: "expandable"}})
			if err != nil {
				t.Fatal(err)
			}
			server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
			response := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/database/localize", key, map[string]any{"target_runtime_id": primary.ID, "target_node_name": pin, "storage_size": "10Gi"})
			if response.Code != http.StatusAccepted {
				t.Fatalf("%d %s", response.Code, response.Body.String())
			}
			var body struct {
				Operation model.Operation `json:"operation"`
			}
			mustDecodeJSON(t, response, &body)
			op, err := s.GetOperation(body.Operation.ID)
			if err != nil {
				t.Fatal(err)
			}
			pg := op.DesiredSpec.Postgres
			if pg == nil || pg.StorageSize != "10Gi" || pg.Instances != 2 || pg.SynchronousReplicas != 1 || pg.PrimaryNodeName != "worker" || pg.FailoverTargetRuntimeID != standby.ID {
				t.Fatalf("capacity operation altered topology: %+v", pg)
			}
		})
	}
}

func TestImageCacheDeletePlanExcludesUnsafeCandidatesAndSummaryKeepsTotals(t *testing.T) {
	_, key, token, server := newImageCacheAdminAPITest(t, "Prune safety test")
	reportImageCacheTestManifest(t, server, token, "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	for _, mode := range []string{"observe", "delete"} {
		r := performFormRequest(t, server, http.MethodGet, "/v1/admin/image-cache/prune-plan?cluster_node_name=worker-1&mode="+mode, key, nil)
		if r.Code != 200 {
			t.Fatalf("%d %s", r.Code, r.Body.String())
		}
		var body struct {
			Plan model.ImageCachePrunePlan `json:"plan"`
		}
		mustDecodeJSON(t, r, &body)
		if mode == "observe" && body.Plan.CandidateManifestCount != 1 {
			t.Fatalf("missing explain candidate: %+v", body.Plan)
		}
		if mode == "delete" && (body.Plan.CandidateManifestCount != 0 || body.Plan.ProtectedManifestCount != 1) {
			t.Fatalf("unsafe delete candidate: %+v", body.Plan)
		}
	}
	r := performJSONRequest(t, server, http.MethodPost, "/v1/admin/image-cache/prune-plan", key, map[string]any{"cluster_node_name": "worker-1", "mode": "dry-run", "summary": true})
	if r.Code != http.StatusCreated {
		t.Fatalf("%d %s", r.Code, r.Body.String())
	}
	var receipt struct {
		Plan model.ImageCachePrunePlan `json:"plan"`
		Task model.NodeUpdateTask      `json:"task"`
	}
	mustDecodeJSON(t, r, &receipt)
	if receipt.Plan.CandidateManifestCount != 1 || len(receipt.Plan.Candidates) != 0 || receipt.Task.ID == "" {
		t.Fatalf("invalid compact receipt: %+v", receipt)
	}
	get := performFormRequest(t, server, http.MethodGet, "/v1/admin/image-cache/inventory?summary=true&cluster_node_name=worker-1", key, nil)
	var inventory struct {
		Nodes     []model.ImageCacheNodeInventory `json:"nodes"`
		Manifests []model.ImageCacheManifest      `json:"manifests"`
	}
	mustDecodeJSON(t, get, &inventory)
	if get.Code != 200 || len(inventory.Nodes) != 1 || len(inventory.Manifests) != 0 {
		t.Fatalf("invalid summary: %s", get.Body.String())
	}
}
