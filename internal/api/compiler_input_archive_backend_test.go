package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/objectstorage"
	"fugue/internal/store"
)

func TestManagedArchiveProvisioningIsolationResumeAndSecretRedaction(t *testing.T) {
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "synthetic-platform-archive-encryption-key")
	db := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := db.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := db.CreateTenant("owner")
	_, admin, _ := db.CreateAPIKey(tenant.ID, "admin", []string{"platform.admin"})
	_, owner, _ := db.CreateAPIKey(tenant.ID, "owner", []string{"storage.admin"})
	secret, err := db.SealObjectStorageSecret(model.DataBackendCredentials{Token: "management-secret"})
	if err != nil {
		t.Fatal(err)
	}
	account := strings.Repeat("a", 32)
	if err = db.SaveObjectStorageConfig(model.ObjectStorageConfig{AccountID: account, Secret: secret}); err != nil {
		t.Fatal(err)
	}
	bucket := ""
	locked := false
	missing := false
	calls := 0
	grants := []string{}
	failWriter := true
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		path := strings.TrimPrefix(r.URL.Path, "/accounts/"+account)
		result := any(map[string]any{})
		status := 200
		switch {
		case path == "/r2/buckets" && r.Method == "POST":
			var body struct {
				Name string `json:"name"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			bucket = body.Name
			if !strings.HasPrefix(bucket, "fugue-platform-archive-") {
				t.Error("archive bucket not isolated")
			}
		case strings.HasSuffix(path, "/lifecycle"):
			result = map[string]any{"rules": []any{}}
		case strings.HasSuffix(path, "/lock"):
			if r.Method == "PUT" {
				locked = true
			}
			rules := []any{}
			if locked {
				rules = append(rules, map[string]any{"id": archiveTestLockID, "enabled": true, "prefix": "", "condition": map[string]string{"type": "Indefinite"}})
			}
			result = map[string]any{"rules": rules}
		case strings.HasPrefix(path, "/r2/buckets/"):
			if bucket == "" || missing {
				status = 404
			} else {
				result = map[string]string{"name": bucket}
			}
		case path == "/tokens" && r.Method == "GET":
			result = []any{}
		case path == "/tokens" && r.Method == "POST":
			var body struct {
				Policies []struct {
					Resources   map[string]string `json:"resources"`
					Permissions []struct {
						ID string `json:"id"`
					} `json:"permission_groups"`
				} `json:"policies"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if len(body.Policies) != 1 || len(body.Policies[0].Resources) != 1 {
				t.Error("account-wide grant")
				http.Error(w, "invalid grant", http.StatusBadRequest)
				return
			}
			if _, ok := body.Policies[0].Resources["com.cloudflare.edge.r2.bucket."+account+"_default_"+bucket]; !ok {
				t.Error("incorrect bucket grant")
			}
			permission := body.Policies[0].Permissions[0].ID
			if permission == objectstorage.WritePermission && failWriter {
				failWriter = false
				status = 503
			} else {
				grants = append(grants, permission)
			}
			result = map[string]string{"id": strings.Repeat("c", 32), "value": strings.Repeat("secret", 8)}
		default:
			t.Errorf("unexpected provider request %s %s", r.Method, path)
			status = 400
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": status == 200, "result": result})
	}))
	defer fake.Close()
	srv := NewServer(db, auth.New(db, ""), nil, ServerConfig{})
	srv.newObjectStorageClient = func(account, token string) *objectstorage.Client {
		c := objectstorage.New(account, token)
		c.BaseURL = fake.URL
		c.HTTP = fake.Client()
		return c
	}
	path := "/v1/admin/storage/compiler-input-archive"
	for _, method := range []string{"GET", "POST"} {
		rec := performJSONRequest(t, srv, method, path, owner, map[string]bool{"execute": true})
		if rec.Code != 403 {
			t.Fatal("tenant administrator got platform access", rec.Code)
		}
	}
	rec := performJSONRequest(t, srv, "POST", path, admin, map[string]bool{"execute": false})
	if rec.Code != 200 || calls != 0 {
		t.Fatal("preview mutated provider", rec.Code, calls)
	}
	rec = performJSONRequest(t, srv, "POST", path, admin, map[string]bool{"execute": true})
	if rec.Code != 502 {
		t.Fatal("writer failure reported success", rec.Code, rec.Body.String())
	}
	cfg, _ := db.GetObjectStorageConfig()
	if cfg.CompilerInputArchive == nil || !cfg.CompilerInputArchive.ReaderConfigured || cfg.CompilerInputArchive.WriterConfigured || cfg.CompilerInputArchive.State != "provisioning" {
		t.Fatal("retry state lost")
	}
	rec = performJSONRequest(t, srv, "POST", path, admin, map[string]bool{"execute": true})
	if rec.Code != 200 || len(grants) != 2 || grants[0] != objectstorage.ReadPermission || grants[1] != objectstorage.WritePermission {
		t.Fatal("retry did not preserve reader", rec.Code, grants)
	}
	rec = performJSONRequest(t, srv, "GET", path, admin, nil)
	if strings.Contains(rec.Body.String(), "ciphertext") || strings.Contains(rec.Body.String(), "secretsecret") || !strings.Contains(rec.Body.String(), `"configured":true`) {
		t.Fatal("secret leak or missing active status", rec.Body.String())
	}
	missing = true
	rec = performJSONRequest(t, srv, "POST", path, admin, map[string]bool{"execute": true})
	if rec.Code != 409 {
		t.Fatal("active bucket silently recreated", rec.Code)
	}
}

const archiveTestLockID = "fugue-compiler-input-archive"
