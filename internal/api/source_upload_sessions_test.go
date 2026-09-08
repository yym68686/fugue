package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
	"net/http/httptest"
	"testing"
)

func TestSourceUploadSessionResumeAndExactOnceSubmission(t *testing.T) {
	st := store.New(t.TempDir() + "/state.json")
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := st.CreateTenant("Upload session tenant")
	_, key, _ := st.CreateAPIKey(tenant.ID, "uploader", []string{"app.write", "app.deploy"})
	_, otherKey, _ := st.CreateAPIKey(tenant.ID, "other key", []string{"app.write", "app.deploy"})
	server := NewServer(st, auth.New(st, ""), nil, ServerConfig{AppBaseDomain: "apps.example.test"})
	call := func(method, path, token string, body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		r := httptest.NewRequest(method, path, bytes.NewReader(raw))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, r)
		return w
	}
	decode := func(w *httptest.ResponseRecorder) model.SourceUploadSession {
		t.Helper()
		if w.Code != 200 {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		var out struct {
			Session model.SourceUploadSession `json:"session"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out.Session
	}
	archive := mustTarGz(t, map[string]string{"index.html": "<h1>example</h1>"})
	digest := fmt.Sprintf("%x", sha256.Sum256(archive))
	create := map[string]any{"request_id": "request-one", "filename": "source.tgz", "sha256": digest, "size_bytes": len(archive)}
	v := decode(call("POST", "/v1/source-upload-sessions", key, create))
	base := "/v1/source-upload-sessions/" + v.ID
	if same := decode(call("POST", "/v1/source-upload-sessions", key, create)); same.ID != v.ID {
		t.Fatal("duplicate session")
	}
	if w := call("GET", base, otherKey, nil); w.Code != 404 {
		t.Fatalf("another actor could read receipt: %d", w.Code)
	}
	if w := call("POST", base+"/complete", key, nil); w.Code != 409 {
		t.Fatal("completed missing chunks")
	}
	chunk := map[string]any{"sha256": digest, "data": archive}
	v = decode(call("PUT", base+"/chunks/0", key, chunk))
	v = decode(call("PUT", base+"/chunks/0", key, chunk))
	if len(v.Chunks) != 1 {
		t.Fatal(v)
	}
	v = decode(call("POST", base+"/complete", key, nil))
	if v.State != "ready" || v.UploadID == "" {
		t.Fatal(v)
	}
	oldUpload := v.UploadID
	v = decode(call("POST", base+"/complete", key, nil))
	if v.UploadID != oldUpload {
		t.Fatal("complete duplicated archive")
	}
	req := map[string]any{"name": "session-demo", "build_strategy": model.AppBuildStrategyStaticSite}
	first := call("POST", base+"/submit", key, req)
	v = decode(first)
	if v.State != "submitted" || len(v.OperationIDs) != 1 || len(v.AppIDs) != 1 {
		t.Fatalf("first result: %s", first.Body.String())
	}
	opID := v.OperationIDs[0]
	second := call("POST", base+"/submit", key, req)
	v = decode(second)
	if len(v.OperationIDs) != 1 || v.OperationIDs[0] != opID {
		t.Fatal("duplicate operation")
	}
	req["name"] = "changed-intent"
	if w := call("POST", base+"/submit", key, req); w.Code != 409 {
		t.Fatal("changed intent accepted")
	}
	recovered := decode(call("GET", "/v1/source-upload-requests/request-one", key, nil))
	if recovered.OperationIDs[0] != opID {
		t.Fatal("missing durable association")
	}
	ops, err := st.ListOperations(tenant.ID, false)
	if err != nil || len(ops) != 1 {
		t.Fatalf("operations=%d err=%v", len(ops), err)
	}
	if bytes.Contains(first.Body.Bytes(), []byte("download_token")) {
		t.Fatal("session exposed download capability")
	}
}
