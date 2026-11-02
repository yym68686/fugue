package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/objectstorage"
	"fugue/internal/store"
)

type objectDeleteFixture struct {
	db                                                  *store.Store
	statePath                                           string
	srv                                                 *Server
	v                                                   model.ObjectStore
	key, reader, attacker                               string
	exists, failRevoke, failTemporaryRevoke, failBucket bool
	providerCalls, batches, revokes, bucketDeletes      int
	temporaryCredentialRevokes                          int
	temporaryCredentialPolicy                           map[string]any
	temporaryCredentialID                               string
	temporaryCredentialActive                           bool
}

func newObjectDeleteFixture(t *testing.T) *objectDeleteFixture {
	t.Helper()
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "synthetic-object-storage-delete-key")
	statePath := filepath.Join(t.TempDir(), "state.json")
	db := store.New(statePath)
	if err := db.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := db.CreateTenant("owner")
	other, _ := db.CreateTenant("other")
	project, _ := db.CreateProject(tenant.ID, "events", "")
	_, key, _ := db.CreateAPIKey(tenant.ID, "storage", []string{"storage.admin"})
	_, reader, _ := db.CreateAPIKey(tenant.ID, "reader", []string{"storage.read"})
	_, attacker, _ := db.CreateAPIKey(other.ID, "storage", []string{"storage.admin"})
	account := strings.Repeat("a", 32)
	secret, err := db.SealObjectStorageSecret(model.DataBackendCredentials{Token: strings.Repeat("b", 40), AccessKeyID: "management-id", SecretAccessKey: "synthetic-management-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.SaveObjectStorageConfig(model.ObjectStorageConfig{AccountID: account, Secret: secret}); err != nil {
		t.Fatal(err)
	}
	v := model.ObjectStore{ID: "object_store_sample", TenantID: tenant.ID, ProjectID: project.ID, Name: "events", Provider: "cloudflare-r2", Bucket: "fugue-objects-object-store-sample", Endpoint: "https://" + account + ".r2.cloudflarestorage.com", Region: "auto", Status: "active", ObjectCount: 100, UsedBytes: 2048, CreatedAt: time.Now().UTC()}
	if err = db.SaveObjectStore(v); err != nil {
		t.Fatal(err)
	}
	if err = db.SaveObjectStorageCredential(model.ObjectStorageCredentialRecord{Credential: model.ObjectStorageCredential{ID: "grant_sample", StoreID: v.ID, Name: "writer", Status: "active", AccessKeyID: "app-token"}}); err != nil {
		t.Fatal(err)
	}
	f := &objectDeleteFixture{db: db, statePath: statePath, v: v, key: key, reader: reader, attacker: attacker, exists: true}
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.providerCalls++
		path := strings.TrimPrefix(r.URL.Path, "/accounts/"+account)
		status := 200
		var result any = map[string]any{}
		switch {
		case path == "/tokens/app-token" && r.Method == http.MethodDelete:
			f.revokes++
			if f.failRevoke {
				status = 503
			}
		case path == "/tokens" && r.Method == http.MethodGet:
			if f.temporaryCredentialActive {
				result = []map[string]string{{"id": f.temporaryCredentialID, "name": objectStoreDeletionCredentialName(v.ID)}}
			} else {
				result = []any{}
			}
		case path == "/tokens" && r.Method == http.MethodPost:
			var request struct {
				Name     string           `json:"name"`
				Policies []map[string]any `json:"policies"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode temporary credential request: %v", err)
				status = 400
			} else {
				f.temporaryCredentialID = "temporary-bucket-token"
				f.temporaryCredentialActive = true
				if len(request.Policies) > 0 {
					f.temporaryCredentialPolicy = request.Policies[0]
				}
				result = map[string]string{"id": f.temporaryCredentialID, "value": strings.Repeat("c", 40), "name": request.Name, "status": "active"}
			}
		case path == "/tokens/temporary-bucket-token" && r.Method == http.MethodDelete:
			f.temporaryCredentialRevokes++
			if f.failTemporaryRevoke {
				status = 503
			} else {
				f.temporaryCredentialActive = false
			}
		case path == "/r2/buckets/"+v.Bucket && r.Method == http.MethodGet:
			if f.exists {
				result = map[string]string{"name": v.Bucket}
			} else {
				status = 404
			}
		case path == "/r2/buckets/"+v.Bucket && r.Method == http.MethodDelete:
			f.bucketDeletes++
			if f.failBucket {
				status = 503
			} else {
				f.exists = false
			}
		default:
			t.Errorf("unexpected provider operation: %s %s", r.Method, path)
			status = 400
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": status == 200, "result": result})
	}))
	t.Cleanup(fake.Close)
	f.srv = NewServer(db, auth.New(db, ""), nil, ServerConfig{})
	f.srv.newObjectStorageClient = func(a, b string) *objectstorage.Client { c := objectstorage.New(a, b); c.BaseURL = fake.URL; return c }
	f.srv.cleanupObjectStoreBatch = func(ctx context.Context, b model.DataBackend) (objectstorage.CleanupBatch, error) {
		f.batches++
		if b.Bucket != v.Bucket || b.Credentials.Token != "" || b.Credentials.AccessKeyID != "temporary-bucket-token" || b.Credentials.SecretAccessKey != objectstorage.SecretAccessKey(strings.Repeat("c", 40)) {
			t.Fatalf("unsafe cleanup connection: bucket=%s", b.Bucket)
		}
		records, _ := db.ListObjectStorageCredentials(v.ID)
		if records[0].Credential.Status != "revoked" {
			t.Fatal("objects deleted before credentials revoked")
		}
		resource := "com.cloudflare.edge.r2.bucket." + account + "_default_" + v.Bucket
		if f.temporaryCredentialPolicy["effect"] != "allow" || f.temporaryCredentialPolicy["resources"].(map[string]any)[resource] != "*" {
			t.Fatalf("temporary deletion credential is not scoped to its bucket: %#v", f.temporaryCredentialPolicy)
		}
		return objectstorage.CleanupBatch{DeletedObjects: 8, Empty: f.batches > 1}, nil
	}
	return f
}

func (f *objectDeleteFixture) request(t *testing.T, method, suffix, key string, body any, want int) *httptest.ResponseRecorder {
	t.Helper()
	r := performJSONRequest(t, f.srv, method, "/v1/object-stores/"+f.v.ID+suffix, key, body)
	if r.Code != want {
		t.Fatalf("%s %s: want=%d got=%d %s", method, suffix, want, r.Code, r.Body.String())
	}
	return r
}

func (f *objectDeleteFixture) plan(t *testing.T) (model.ObjectStoreDeleteResponse, map[string]any) {
	t.Helper()
	var out model.ObjectStoreDeleteResponse
	mustDecodeJSON(t, f.request(t, "POST", "/deletion-plan", f.key, nil, 200), &out)
	return out, map[string]any{"confirm_bucket": out.Store.Bucket, "plan_sha256": out.Inventory.PlanSHA256, "ack_no_external_writers": true}
}

func TestObjectStoreDeletePreviewConfirmationIsolationAndResume(t *testing.T) {
	f := newObjectDeleteFixture(t)
	out, req := f.plan(t)
	if out.Deleted || out.Inventory.Complete || out.Inventory.Source != "cached" || out.Inventory.Bytes != 2048 || f.providerCalls != 0 {
		t.Fatalf("unsafe preview: %+v calls=%d", out, f.providerCalls)
	}
	f.request(t, "POST", "/deletion-plan", f.reader, nil, 403)
	f.request(t, "DELETE", "", f.attacker, req, 404)
	f.request(t, "DELETE", "", f.reader, req, 403)
	delete(req, "ack_no_external_writers")
	f.request(t, "DELETE", "", f.key, req, 400)
	req["ack_no_external_writers"] = true
	req["confirm_bucket"] = "wrong"
	f.request(t, "DELETE", "", f.key, req, 409)
	req["confirm_bucket"] = f.v.Bucket
	req["plan_sha256"] = strings.Repeat("0", 64)
	f.request(t, "DELETE", "", f.key, req, 409)
	if f.providerCalls != 0 {
		t.Fatal("invalid confirmation contacted provider")
	}
	req["plan_sha256"] = out.Inventory.PlanSHA256
	f.failRevoke = true
	f.request(t, "DELETE", "", f.key, req, 502)
	if f.batches != 0 {
		t.Fatal("revoke failure deleted objects")
	}
	current, _ := f.db.GetObjectStore(f.v.ID)
	if current.Status != "deleting" {
		t.Fatal("failed delete did not persist fence")
	}
	f.request(t, "PATCH", "", f.key, map[string]bool{"enabled": true}, 409)
	f.request(t, "PATCH", "", f.key, map[string]bool{"enabled": false}, 409)
	f.request(t, "PATCH", "", f.key, map[string]int{"quota_bytes": 1}, 409)
	create := performJSONRequest(t, f.srv, "POST", "/v1/object-stores", f.key, map[string]string{"project_id": f.v.ProjectID, "name": f.v.Name})
	if create.Code != 409 {
		t.Fatal("create resurrected a deleting store")
	}
	f.failRevoke = false
	mustDecodeJSON(t, f.request(t, "DELETE", "", f.key, req, 202), &out)
	if out.Deleted || out.Store.Deletion.DeletedObjects != 8 {
		t.Fatal("incomplete batch reported completed")
	}
	// Reload durable state to model an API process restart between batches.
	f.srv.store = store.New(f.statePath)
	if err := f.srv.store.Init(); err != nil {
		t.Fatal(err)
	}
	f.failBucket = true
	f.request(t, "DELETE", "", f.key, req, 502)
	if _, err := f.db.GetObjectStore(f.v.ID); err != nil {
		t.Fatal("provider delete failure removed metadata")
	}
	f.failBucket = false
	mustDecodeJSON(t, f.request(t, "DELETE", "", f.key, req, 200), &out)
	if !out.Deleted || f.exists {
		t.Fatal("did not finish")
	}
	if _, err := f.db.GetObjectStore(f.v.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("metadata retained after completed deletion")
	}
	creds, err := f.db.ListObjectStorageCredentials(f.v.ID)
	if err != nil || len(creds) != 0 {
		t.Fatal("credential metadata retained")
	}
}

func TestObjectStoreDeleteAbsentBucketAndTargetGuard(t *testing.T) {
	f := newObjectDeleteFixture(t)
	_, req := f.plan(t)
	f.exists = false
	f.request(t, "DELETE", "", f.key, req, 200)
	if f.revokes != 1 || f.batches != 0 || f.bucketDeletes != 0 || f.temporaryCredentialRevokes != 0 {
		t.Fatal("absent-bucket recovery skipped revocation or contacted S3")
	}
	f = newObjectDeleteFixture(t)
	f.v.Bucket = "shared-backups"
	if err := f.db.SaveObjectStore(f.v); err != nil {
		t.Fatal(err)
	}
	f.request(t, "POST", "/deletion-plan", f.key, nil, 409)
	if f.providerCalls != 0 {
		t.Fatal("unsafe bucket contacted provider")
	}
}

func TestObjectStoreDeleteReconcilesTemporaryCredentialAfterRevocationFailure(t *testing.T) {
	f := newObjectDeleteFixture(t)
	out, req := f.plan(t)
	f.failTemporaryRevoke = true
	f.request(t, "DELETE", "", f.key, req, 502)
	if f.batches != 1 || f.bucketDeletes != 0 || !f.temporaryCredentialActive {
		t.Fatal("failed temporary-token revocation was not fenced")
	}
	f.failTemporaryRevoke = false
	mustDecodeJSON(t, f.request(t, "DELETE", "", f.key, req, 200), &out)
	if f.temporaryCredentialActive || f.temporaryCredentialRevokes != 3 {
		t.Fatal("retry did not revoke the prior token and the newly issued token")
	}
	if !out.Deleted || out.Store.Deletion.DeletedObjects != 16 {
		t.Fatalf("retry progress not persisted: %+v", out)
	}
}

func TestObjectStoreDeleteRevokesTemporaryCredentialAfterClientCancellation(t *testing.T) {
	f := newObjectDeleteFixture(t)
	_, body := f.plan(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cleanup := f.srv.cleanupObjectStoreBatch
	f.srv.cleanupObjectStoreBatch = func(ctx context.Context, b model.DataBackend) (objectstorage.CleanupBatch, error) {
		batch, _ := cleanup(ctx, b)
		cancel()
		return batch, ctx.Err()
	}
	payload, _ := json.Marshal(body)
	r := httptest.NewRequest(http.MethodDelete, "/v1/object-stores/"+f.v.ID, strings.NewReader(string(payload))).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+f.key)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.srv.Handler().ServeHTTP(w, r)
	if w.Code < 400 || f.temporaryCredentialRevokes != 1 || f.temporaryCredentialActive || f.bucketDeletes != 0 {
		t.Fatalf("cancelled request leaked a temporary token or removed bucket: status=%d revokes=%d", w.Code, f.temporaryCredentialRevokes)
	}
	current, err := f.db.GetObjectStore(f.v.ID)
	if err != nil || current.Status != "deleting" || current.Deletion.DeletedObjects != 8 {
		t.Fatalf("cancellation lost durable progress: %+v, %v", current, err)
	}
}
