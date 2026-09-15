package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/objectstorage"
	"fugue/internal/store"
)

func TestObjectStorageBusyIsRetryableWithoutExecutingMutation(t *testing.T) {
	db := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := db.Init(); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(db, auth.New(db, ""), nil, ServerConfig{})
	acquired, err := db.WithAdvisoryLock(context.Background(), objectStorageLock, func() error {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/object-stores/demo/usage", nil)
		called := false
		srv.objectStorageRun(rec, req, func() error { called = true; return nil })
		var result httpx.ErrorResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if called || rec.Code != http.StatusConflict || result.Code != "object_storage_busy" || !result.Retryable || rec.Header().Get("Retry-After") != "1" {
			t.Fatalf("unexpected busy response: called=%t status=%d body=%s headers=%v", called, rec.Code, rec.Body.String(), rec.Header())
		}
		return nil
	})
	if err != nil || !acquired {
		t.Fatalf("hold management lock: acquired=%t err=%v", acquired, err)
	}
	called := false
	srv.objectStorageRun(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/object-stores/demo/usage", nil), func() error { called = true; return nil })
	if !called {
		t.Fatal("released lock did not allow the next operation")
	}
}

func TestNormalizeObjectStorageUsageTimeout(t *testing.T) {
	if got := normalizeObjectStorageUsageTimeout(0); got != defaultObjectStorageUsageTimeout {
		t.Fatalf("zero timeout = %s, want %s", got, defaultObjectStorageUsageTimeout)
	}
	if got := normalizeObjectStorageUsageTimeout(45 * time.Second); got != 45*time.Second {
		t.Fatalf("explicit timeout = %s, want 45s", got)
	}
}

func TestObjectStorageIsolationAndRetryableCredentialLifecycle(t *testing.T) {
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "synthetic-object-storage-test-encryption-key")
	statePath := filepath.Join(t.TempDir(), "state.json")
	db := store.New(statePath)
	if err := db.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := db.CreateTenant("Storage owner")
	other, _ := db.CreateTenant("Other owner")
	project, _ := db.CreateProject(tenant.ID, "analysis", "")
	otherProject, _ := db.CreateProject(other.ID, "other", "")
	runtime, _, _ := db.CreateRuntime(tenant.ID, "runtime", model.RuntimeTypeManagedOwned, "", nil)
	app, err := db.CreateApp(tenant.ID, project.ID, "worker", "", model.AppSpec{Image: "example/worker:latest", RuntimeID: runtime.ID, Replicas: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, admin, _ := db.CreateAPIKey(tenant.ID, "admin", []string{"platform.admin"})
	_, owner, _ := db.CreateAPIKey(tenant.ID, "storage", []string{"storage.admin"})
	_, reader, _ := db.CreateAPIKey(tenant.ID, "reader", []string{"storage.read"})
	_, attacker, _ := db.CreateAPIKey(other.ID, "storage", []string{"storage.admin"})
	_, ordinary, _ := db.CreateAPIKey(tenant.ID, "ordinary", []string{"app.read"})
	buckets := map[string]bool{}
	tokens := []objectstorage.Token{}
	creates := 0
	failDelete := false
	failCreateResponse := true
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/accounts/"+strings.Repeat("a", 32))
		result := any(map[string]any{})
		status := 200
		switch {
		case path == "/tokens/verify":
			result = map[string]string{"id": strings.Repeat("c", 32), "status": "active"}
		case path == "/tokens/permission_groups":
			result = []any{}
		case path == "/r2/buckets" && r.Method == "GET":
			result = map[string]any{"buckets": []any{}}
		case path == "/r2/buckets" && r.Method == "POST":
			var req struct {
				Name string `json:"name"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			if strings.Contains(req.Name, "backup") {
				t.Error("backup namespace used")
			}
			buckets[req.Name] = true
		case strings.HasPrefix(path, "/r2/buckets/"):
			name := strings.TrimPrefix(path, "/r2/buckets/")
			if !buckets[name] {
				status = 404
			} else {
				result = map[string]string{"name": name}
			}
		case path == "/tokens" && r.Method == "GET":
			result = tokens
		case path == "/tokens" && r.Method == "POST":
			var req struct {
				Name     string `json:"name"`
				Value    string `json:"value"`
				Policies []struct {
					Resources   map[string]string `json:"resources"`
					Permissions []struct {
						ID string `json:"id"`
					} `json:"permission_groups"`
				} `json:"policies"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			if len(req.Policies) != 1 || len(req.Policies[0].Resources) != 1 || req.Policies[0].Permissions[0].ID != objectstorage.ReadPermission {
				t.Error("credential not scoped to a single read-only bucket")
			}
			for k := range req.Policies[0].Resources {
				if !strings.HasPrefix(k, "com.cloudflare.edge.r2.bucket.") {
					t.Error("account-wide grant")
				}
			}
			creates++
			tok := objectstorage.Token{ID: strings.Repeat("d", 32), Name: req.Name, Value: strings.Repeat("v", 40), Status: "active"}
			tokens = append(tokens, tok)
			result = tok
			if failCreateResponse {
				failCreateResponse = false
				status = 503
			}
		case strings.HasPrefix(path, "/tokens/") && r.Method == "DELETE":
			if failDelete {
				status = 503
			} else {
				tokens = nil
			}
		default:
			t.Errorf("unexpected provider operation %s %s", r.Method, path)
			status = 400
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": status == 200, "result": result, "errors": []any{map[string]int{"code": 1000}}})
	}))
	defer fake.Close()
	srv := NewServer(db, auth.New(db, ""), nil, ServerConfig{})
	srv.newObjectStorageClient = func(a, b string) *objectstorage.Client { c := objectstorage.New(a, b); c.BaseURL = fake.URL; return c }
	request := func(method, path, key string, body any, want int) *httptest.ResponseRecorder {
		t.Helper()
		rec := performJSONRequest(t, srv, method, path, key, body)
		if rec.Code != want {
			t.Fatalf("%s %s: want %d got %d %s", method, path, want, rec.Code, rec.Body.String())
		}
		return rec
	}
	cfg := map[string]string{"account_id": strings.Repeat("a", 32), "api_token": strings.Repeat("b", 40)}
	request("PUT", "/v1/admin/object-storage", owner, cfg, 403)
	request("PUT", "/v1/admin/object-storage", admin, cfg, 200)
	request("GET", "/v1/object-stores", ordinary, nil, 403)
	request("POST", "/v1/object-stores", attacker, map[string]any{"project_id": project.ID, "name": "events"}, 404)
	request("POST", "/v1/object-stores", owner, map[string]any{"project_id": otherProject.ID, "name": "events"}, 404)
	rec := request("POST", "/v1/object-stores", owner, map[string]any{"project_id": project.ID, "name": "events"}, 200)
	var env objectStoreTestEnvelope
	mustDecodeJSON(t, rec, &env)
	id := env.Store.ID
	request("GET", "/v1/object-stores/"+id, attacker, nil, 404)
	request("GET", "/v1/object-stores?tenant_id="+tenant.ID, attacker, nil, 403)
	credsPath := "/v1/object-stores/" + id + "/credentials"
	body := map[string]string{"app_id": app.ID, "name": "reader", "permission": "read-only"}
	request("POST", credsPath, reader, body, 403)
	request("POST", credsPath, owner, body, 502)
	rec = request("POST", credsPath, owner, body, 200)
	var connection struct {
		Credential model.ObjectStorageCredential `json:"credential"`
		Secret     string                        `json:"secret_access_key"`
	}
	mustDecodeJSON(t, rec, &connection)
	if creates != 2 || len(connection.Secret) != 64 {
		t.Fatal("retry did not replace ambiguous credential or lost secret")
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("secret response cacheable")
	}
	listing := request("GET", credsPath, owner, nil, 200).Body.String()
	if strings.Contains(listing, connection.Secret) || strings.Contains(listing, "ciphertext") {
		t.Fatal("credential list leaked secret")
	}
	failDelete = true
	request("PATCH", "/v1/object-stores/"+id, owner, map[string]bool{"enabled": false}, 502)
	current, _ := db.GetObjectStore(id)
	if current.Status != "disabling" {
		t.Fatal("disable failure reported success")
	}
	request("POST", credsPath, owner, body, 409)
	failDelete = false
	request("PATCH", "/v1/object-stores/"+id, owner, map[string]bool{"enabled": false}, 200)
	current, _ = db.GetObjectStore(id)
	if current.Status != "disabled" || !buckets[current.Bucket] {
		t.Fatal("disable did not retain bucket")
	}
	request("PATCH", "/v1/object-stores/"+id, owner, map[string]bool{"enabled": true}, 200)
	request("POST", credsPath, owner, body, 409)
	// A new server over the same state keeps resources and revoked grants.
	db2 := store.New(statePath)
	if err = db2.Init(); err != nil {
		t.Fatal(err)
	}
	reloaded, err := db2.GetObjectStore(id)
	if err != nil || reloaded.Status != "active" {
		t.Fatal("resource lost after restart")
	}
	grants, err := db2.ListObjectStorageCredentials(id)
	if err != nil || len(grants) != 1 || grants[0].Credential.Status != "revoked" {
		t.Fatal("revocation lost after restart")
	}
}

type objectStoreTestEnvelope struct {
	Store model.ObjectStore `json:"store"`
}
