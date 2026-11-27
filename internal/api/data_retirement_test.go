package api

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func retirementAPIFixture(t *testing.T) (*Server, *store.Store, model.DataWorkspace, string) {
	t.Helper()
	clearDefaultDataBackendEnv(t)
	s := newStoreTestDatabase(t, "")
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("retirement-fixture")
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.CreateDataWorkspace(model.DataWorkspace{TenantID: tenant.ID, Name: "dataset"})
	if err != nil {
		t.Fatal(err)
	}
	_, key, err := s.CreateAPIKey(tenant.ID, "owner", []string{"data.read", "data.write", "data.delete"})
	if err != nil {
		t.Fatal(err)
	}
	return NewServer(s, auth.New(s, ""), nil, ServerConfig{}), s, w, key
}

func TestRetirementAPIIsPreviewByDefaultAndTenantAuthorized(t *testing.T) {
	s, st, w, key := retirementAPIFixture(t)
	resp := performJSONRequest(t, s, http.MethodPost, "/v1/data/workspaces/"+w.ID+"/retirement", key, map[string]any{})
	if resp.Code != 200 || !strings.Contains(resp.Body.String(), `"allowed":true`) {
		t.Fatal(resp.Code, resp.Body.String())
	}
	if _, err := st.DataWorkspaceRetirement(w.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("preview wrote a task", err)
	}
	resp = performJSONRequest(t, s, http.MethodPost, "/v1/data/workspaces/"+w.ID+"/retirement", key, map[string]any{"execute": true})
	if resp.Code != 202 {
		t.Fatal(resp.Code, resp.Body.String())
	}
	var j model.StorageMaintenanceJob
	if err := json.Unmarshal(resp.Body.Bytes(), &j); err != nil {
		t.Fatal(err)
	}
	tenant, err := st.CreateTenant("other-tenant")
	if err != nil {
		t.Fatal(err)
	}
	_, other, err := st.CreateAPIKey(tenant.ID, "other", []string{"data.read", "data.delete"})
	if err != nil {
		t.Fatal(err)
	}
	resp = performJSONRequest(t, s, http.MethodGet, "/v1/data/workspace-retirements/"+j.ID, other, nil)
	if resp.Code != 404 {
		t.Fatal("task crossed tenant boundary", resp.Code, resp.Body.String())
	}
	_, reader, err := st.CreateAPIKey(w.TenantID, "reader", []string{"data.read"})
	if err != nil {
		t.Fatal(err)
	}
	resp = performJSONRequest(t, s, http.MethodPost, "/v1/data/workspaces/"+w.ID+"/retirement", reader, map[string]any{"execute": true})
	if resp.Code != 403 {
		t.Fatal("missing delete scope accepted", resp.Code)
	}
}

func TestRetirementStatusRetainsProjectBoundaryAfterDeletion(t *testing.T) {
	ret := &model.DataWorkspaceRetirement{TenantID: "owner-tenant", ProjectID: "original-project", ActorType: "api-key", ActorID: "initiator"}
	for _, tc := range []struct {
		name    string
		p       model.Principal
		allowed bool
	}{
		{"owner", model.Principal{TenantID: "owner-tenant"}, true},
		{"same-project", model.Principal{TenantID: "owner-tenant", ProjectID: "original-project"}, true},
		{"different-project", model.Principal{TenantID: "owner-tenant", ProjectID: "other-project"}, false},
		{"rebound-initiator", model.Principal{TenantID: "owner-tenant", ProjectID: "other-project", ActorType: "api-key", ActorID: "initiator"}, false},
		{"authorized-external-initiator", model.Principal{TenantID: "external", ActorType: "api-key", ActorID: "initiator"}, true},
		{"other-tenant", model.Principal{TenantID: "external", ActorType: "api-key", ActorID: "other"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := canInspectRetiredDataWorkspace(tc.p, ret); got != tc.allowed {
				t.Fatalf("allowed=%t want=%t", got, tc.allowed)
			}
		})
	}
}

func TestRetirementReclaimsOnlyExclusiveObjectsAndSurvivesMetadataDeletion(t *testing.T) {
	s, st, w, key := retirementAPIFixture(t)
	shared, unique := testDataDigest("shared"), testDataDigest("unique")
	for _, v := range []struct{ digest, value string }{{shared, "shared"}, {unique, "unique"}} {
		if _, err := st.WriteDataBlob(v.digest, strings.NewReader(v.value)); err != nil {
			t.Fatal(err)
		}
	}
	manifest := func(digests ...string) model.DataManifest {
		m := model.DataManifest{}
		for _, d := range digests {
			m.Entries = append(m.Entries, model.DataManifestEntry{Kind: "file", SHA256: d, RelativePath: d, AssetName: "data", Size: 6})
		}
		return model.NormalizeDataManifest(m)
	}
	if _, err := st.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: w.ID, Version: "one", Manifest: manifest(shared, unique)}); err != nil {
		t.Fatal(err)
	}
	other, err := st.CreateDataWorkspace(model.DataWorkspace{TenantID: w.TenantID, Name: "other"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: other.ID, Version: "one", Manifest: manifest(shared)}); err != nil {
		t.Fatal(err)
	}
	j, err := st.BeginDataWorkspaceRetirement(w, model.DataWorkspaceRetirementOptions{}, "api-key", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.runWorkspaceRetirement(context.Background(), &j); !errors.Is(err, errWorkspaceRetirementPending) {
		t.Fatal(err)
	}
	if !st.DataBlobExists(unique) {
		t.Fatal("grace skipped")
	}
	// Unit-test time travel only; production exposes no grace-shortening option.
	past := time.Now().Add(-time.Hour)
	j.WorkspaceRetirement.ReclaimAfter = &past
	if _, err = s.runWorkspaceRetirement(context.Background(), &j); err != nil {
		t.Fatal(err)
	}
	if !st.DataBlobExists(shared) || st.DataBlobExists(unique) {
		t.Fatal("exclusive/shared reclamation incorrect")
	}
	if j.WorkspaceRetirement.DeletedObjects != 1 || j.WorkspaceRetirement.DeletedBytes != 6 || !j.WorkspaceRetirement.MetadataDeleted {
		t.Fatal(j.WorkspaceRetirement)
	}
	if _, err = st.GetDataWorkspace(w.ID, "", true); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("metadata survived", err)
	}
	j.Status = "succeeded"
	if err = st.SaveStorageMaintenanceJob(j, j.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	resp := performJSONRequest(t, s, http.MethodGet, "/v1/data/workspace-retirements/"+j.ID, key, nil)
	if resp.Code != 200 || !strings.Contains(resp.Body.String(), `"metadata_deleted":true`) {
		t.Fatal(resp.Code, resp.Body.String())
	}
}

func TestRetirementWorkerDoesNotFailOrdinaryGraceWaiting(t *testing.T) {
	s, st, w, _ := retirementAPIFixture(t)
	j, err := st.BeginDataWorkspaceRetirement(w, model.DataWorkspaceRetirementOptions{}, "api-key", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	s.sweepStorageMaintenance(context.Background())
	j, err = st.GetStorageMaintenanceJob(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != "retrying" || j.Error != "" || j.Attempts != 0 || j.WorkspaceRetirement.Phase != "grace" || j.RetryAt == nil {
		t.Fatal(j)
	}
}

func TestRetirementCheckpointSurvivesRestartAndLastWorkspaceIsCollected(t *testing.T) {
	s, st, w, _ := retirementAPIFixture(t)
	m := model.DataManifest{}
	for i := 0; i < 12; i++ {
		value := strings.Repeat("x", i+1)
		digest := testDataDigest(value)
		if _, err := st.WriteDataBlob(digest, strings.NewReader(value)); err != nil {
			t.Fatal(err)
		}
		m.Entries = append(m.Entries, model.DataManifestEntry{Kind: "file", SHA256: digest, RelativePath: digest, AssetName: "data", Size: int64(len(value))})
	}
	if _, err := st.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: w.ID, Version: "one", Manifest: m}); err != nil {
		t.Fatal(err)
	}
	j, err := st.BeginDataWorkspaceRetirement(w, model.DataWorkspaceRetirementOptions{}, "api-key", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.runWorkspaceRetirement(context.Background(), &j); !errors.Is(err, errWorkspaceRetirementPending) {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Hour)
	j.WorkspaceRetirement.ReclaimAfter = &past
	if _, err = s.runWorkspaceRetirement(context.Background(), &j); !errors.Is(err, errWorkspaceRetirementPending) {
		t.Fatal(err)
	}
	if j.WorkspaceRetirement.Cursor != 10 || j.WorkspaceRetirement.DeletedObjects != 10 {
		t.Fatal(j.WorkspaceRetirement)
	}
	if err = st.SaveStorageMaintenanceJob(j, j.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	j, err = st.GetStorageMaintenanceJob(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewServer(st, auth.New(st, ""), nil, ServerConfig{})
	if _, err = restarted.runWorkspaceRetirement(context.Background(), &j); err != nil {
		t.Fatal(err)
	}
	if j.WorkspaceRetirement.DeletedObjects != 12 || !j.WorkspaceRetirement.MetadataDeleted {
		t.Fatal(j.WorkspaceRetirement)
	}
	for _, e := range m.Entries {
		if st.DataBlobExists(e.SHA256) {
			t.Fatal("last-workspace blob stranded")
		}
	}
	j.Status = "succeeded"
	if err = st.SaveStorageMaintenanceJob(j, j.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	// Background namespace ownership must outlive the last workspace so a
	// delayed object or a former shared object remains discoverable by GC.
	s.sweepStorageMaintenance(context.Background())
	jobs, err := st.ListStorageMaintenanceJobs()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, job := range jobs {
		if job.Kind == "data-gc" && strings.HasPrefix(job.ScopeID, "retired-backend:") {
			found = true
		}
	}
	if !found {
		t.Fatal("last workspace removed the namespace's automatic GC owner")
	}
}

func TestRetirementRemoteDeleteReceiptsAndFreshObjectGrace(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		t.Run(map[bool]string{false: "old", true: "fresh"}[fresh], func(t *testing.T) {
			s, st, w, _ := retirementAPIFixture(t)
			digest := testDataDigest("remote")
			key := model.DataObjectKey(digest)
			var mu sync.Mutex
			exists := true
			deletes := 0
			stamp := time.Now().Add(-10 * 24 * time.Hour)
			if fresh {
				stamp = time.Now()
			}
			remote := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.Method == http.MethodHead {
					if !exists {
						out.WriteHeader(404)
						return
					}
					out.Header().Set("Content-Length", "6")
					out.Header().Set("Last-Modified", stamp.UTC().Format(http.TimeFormat))
					out.Header().Set("ETag", `"fixture-etag"`)
					return
				}
				if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
					var req struct {
						Objects []struct {
							Key string `xml:"Key"`
						} `xml:"Object"`
					}
					if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					if len(req.Objects) != 1 || req.Objects[0].Key != "archive/"+key {
						t.Error("unexpected deletion target", req.Objects)
					}
					deletes++
					exists = false
					out.Header().Set("Content-Type", "application/xml")
					out.Write([]byte(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`))
					return
				}
				t.Errorf("unexpected object request %s %s", r.Method, r.URL.Path)
				out.WriteHeader(500)
			}))
			defer remote.Close()
			b, err := st.CreateDataBackend(model.DataBackend{TenantID: w.TenantID, Name: "archive", Provider: model.DataBackendProviderS3, Bucket: "bucket", Prefix: "archive", Endpoint: remote.URL, Credentials: model.DataBackendCredentials{AccessKeyID: "fixture", SecretAccessKey: "fixture"}})
			if err != nil {
				t.Fatal(err)
			}
			w, err = st.UpdateDataWorkspace(w.ID, w.TenantID, false, store.DataWorkspaceUpdate{StorageBackendID: &b.ID})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = st.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: w.ID, Version: "one", Manifest: model.DataManifest{Entries: []model.DataManifestEntry{{Kind: "file", SHA256: digest, Size: 6, AssetName: "data", RelativePath: "sample"}}}}); err != nil {
				t.Fatal(err)
			}
			j, err := st.BeginDataWorkspaceRetirement(w, model.DataWorkspaceRetirementOptions{}, "api-key", "fixture")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.runWorkspaceRetirement(context.Background(), &j); !errors.Is(err, errWorkspaceRetirementPending) {
				t.Fatal(err)
			}
			past := time.Now().Add(-time.Hour)
			j.WorkspaceRetirement.ReclaimAfter = &past
			_, err = s.runWorkspaceRetirement(context.Background(), &j)
			if fresh {
				if !errors.Is(err, errWorkspaceRetirementPending) || deletes != 0 || !j.WorkspaceRetirement.ReclaimAfter.After(time.Now()) {
					t.Fatal(err, deletes, j.WorkspaceRetirement)
				}
			} else if err != nil || deletes != 1 || j.WorkspaceRetirement.DeletedBytes != 6 || !j.WorkspaceRetirement.MetadataDeleted {
				t.Fatal(err, deletes, j.WorkspaceRetirement)
			}
		})
	}
}
