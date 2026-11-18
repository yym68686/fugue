package api

import (
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/objectstorage"
	"fugue/internal/store"
)

type b2APITestTransport func(*http.Request) (*http.Response, error)

func (f b2APITestTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestObjectStoreBackblazeConfigurationAndProvisioning(t *testing.T) {
	const (
		keyID     = "0000123456789abcdef000001"
		key       = "syntheticApplicationKey"
		accountID = "012345abcdef"
		endpoint  = "https://s3.us-west-004.backblazeb2.com"
		path      = "/v1/admin/object-storage/backblaze-b2"
	)
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "synthetic-b2-encryption-key")
	db := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := db.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := db.CreateTenant("owner")
	project, _ := db.CreateProject(tenant.ID, "test", "")
	_, tenantKey, _ := db.CreateAPIKey(tenant.ID, "tenant", []string{"storage.admin"})
	_, adminKey, _ := db.CreateAPIKey(tenant.ID, "admin", []string{"platform.admin"})
	srv := NewServer(db, auth.New(db, ""), nil, ServerConfig{})
	r2 := model.ObjectStorageConfig{AccountID: strings.Repeat("a", 32)}
	if err := db.SaveObjectStorageConfig(r2); err != nil {
		t.Fatal(err)
	}
	var calls int
	var upstreamAccount = accountID
	srv.objectStorageB2Transport = b2APITestTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		var body any
		code := 200
		switch {
		case r.URL.Host == "api.backblazeb2.com" && r.Method == "GET":
			id, secret, ok := r.BasicAuth()
			if !ok || id != keyID {
				t.Error("wrong B2 authorization")
			}
			if secret != key {
				code, body = 401, map[string]string{"message": secret}
				break
			}
			body = map[string]any{"accountId": upstreamAccount, "authorizationToken": "syntheticToken", "apiInfo": map[string]any{"storageApi": map[string]any{
				"apiUrl": "https://api004.backblazeb2.com", "s3ApiUrl": endpoint, "allowed": map[string]any{"buckets": nil, "namePrefix": nil,
					"capabilities": []string{"listBuckets", "writeBuckets", "deleteBuckets", "listKeys", "writeKeys", "deleteKeys", "listFiles", "readFiles", "writeFiles", "deleteFiles"}},
			}}}
		case strings.HasSuffix(r.URL.Path, "b2_list_buckets"):
			body = map[string]any{"buckets": []any{}}
		case strings.HasSuffix(r.URL.Path, "b2_create_bucket"):
			var params map[string]any
			if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
				t.Fatal(err)
			}
			params["bucketId"] = "bucket001"
			body = params
		default:
			t.Errorf("unexpected call: %s", r.URL)
			code, body = 500, map[string]any{}
		}
		raw, _ := json.Marshal(body)
		return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
	})
	req := map[string]string{"key_id": keyID, "application_key": key}
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		if rec := performJSONRequest(t, srv, method, path, tenantKey, req); rec.Code != 403 {
			t.Fatalf("tenant reached admin endpoint: %d", rec.Code)
		}
	}
	if calls != 0 {
		t.Fatal("unauthorized request contacted provider")
	}
	for _, method := range []string{http.MethodPut, http.MethodGet} {
		rec := performJSONRequest(t, srv, method, path, adminKey, req)
		if rec.Code != 200 || strings.Contains(rec.Body.String(), key) || strings.Contains(rec.Body.String(), keyID) || !strings.Contains(rec.Body.String(), "us-west-004") || !strings.Contains(rec.Header().Get("Cache-Control"), "no-store") {
			t.Fatalf("unsafe configuration response: status=%d", rec.Code)
		}
	}
	cfg, err := db.GetObjectStorageConfig()
	b2, b2Err := db.GetBackblazeB2Config()
	if err != nil || b2Err != nil || cfg.AccountID != r2.AccountID || b2 == nil || strings.Contains(b2.Secret.Ciphertext, key) {
		t.Fatal("B2 management settings did not coexist with R2 or were not sealed")
	}
	req["application_key"] = "anotherSyntheticKey123"
	if rec := performJSONRequest(t, srv, http.MethodPut, path, adminKey, req); rec.Code != 502 || strings.Contains(rec.Body.String(), req["application_key"]) {
		t.Fatal("bad credentials were accepted or exposed")
	}
	create := map[string]string{"project_id": project.ID, "name": "example", "provider": model.DataBackendProviderBackblazeB2}
	rec := performJSONRequest(t, srv, http.MethodPost, "/v1/object-stores", tenantKey, create)
	if rec.Code != 200 {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	var out struct{ Store model.ObjectStore }
	mustDecodeJSON(t, rec, &out)
	if out.Store.Status != "active" || out.Store.BucketID != "bucket001" || out.Store.Endpoint != endpoint || out.Store.Region != "us-west-004" {
		t.Fatalf("wrong B2 store: %+v", out.Store)
	}
	create["provider"] = model.DataBackendProviderCloudflareR2
	if rec := performJSONRequest(t, srv, http.MethodPost, "/v1/object-stores", tenantKey, create); rec.Code != 409 {
		t.Fatal("existing store may not change providers")
	}
	req["application_key"] = key
	upstreamAccount = "112345abcdef"
	if rec := performJSONRequest(t, srv, http.MethodPut, path, adminKey, req); rec.Code != 409 {
		t.Fatal("live bucket allowed account switch")
	}
	if objectStoreS3Secret(model.DataBackendProviderBackblazeB2, key) != key || objectStoreS3Secret(model.DataBackendProviderCloudflareR2, key) != objectstorage.SecretAccessKey(key) {
		t.Fatal("incorrect provider credential conversion")
	}
}
