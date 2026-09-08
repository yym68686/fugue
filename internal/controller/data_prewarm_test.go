package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"fugue/internal/dataprewarm"
	"fugue/internal/model"
	"fugue/internal/store"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPrewarmCompletionRequiresOwnedSuccessfulPodAndExactReceipt(t *testing.T) {
	st := store.New(t.TempDir() + "/state.json")
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := st.CreateTenant("Prewarm tenant")
	ws, err := st.CreateDataWorkspace(model.DataWorkspace{TenantID: tenant.ID, Name: "dataset"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	digest := strings.Repeat("a", 64)
	expires := now.Add(time.Hour)
	tr, err := st.CreateDataTransfer(model.DataTransfer{TenantID: tenant.ID, WorkspaceID: ws.ID, Direction: "prewarm", Status: "running", Target: "runtime_test", BytesTotal: 4, FilesTotal: 1, Manifest: model.DataManifest{Digest: digest}, ExpiresAt: &expires, Cache: &model.DataPrewarmCache{Namespace: "system", Job: "prewarm-job", Claim: "prewarm-claim", Node: "node-test", State: "downloading", ManifestDigest: digest}})
	if err != nil {
		t.Fatal(err)
	}
	correctNode := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Fatalf("unexpected mutation before completion: %s", r.Method)
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/jobs/prewarm-job"):
			json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": "job-uid", "labels": map[string]string{prewarmLabel: tr.ID}}, "status": map[string]int{"succeeded": 1}})
		case strings.HasSuffix(r.URL.Path, "/pods"):
			node := "different-node"
			if correctNode {
				node = "node-test"
			}
			fmt.Fprintf(w, `{"items":[{"metadata":{"name":"worker","ownerReferences":[{"kind":"Job","uid":"job-uid"}]},"spec":{"nodeName":%q},"status":{"phase":"Succeeded"}}]}`, node)
		case strings.HasSuffix(r.URL.Path, "/log"):
			json.NewEncoder(w).Encode(dataprewarm.Progress{TransferID: tr.ID, ManifestDigest: digest, State: "ready", BytesDone: 4, FilesDone: 1, ObservedAt: time.Now().UTC()})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	service := &Service{Store: st}
	client := &kubeClient{baseURL: server.URL, client: server.Client()}
	if err = service.reconcileDataPrewarm(context.Background(), client, tr); err != nil {
		t.Fatal(err)
	}
	tr, _ = st.GetDataTransfer(tr.ID)
	if tr.Status == "completed" {
		t.Fatal("different runtime pod counted as success")
	}
	correctNode = true
	if err = service.reconcileDataPrewarm(context.Background(), client, tr); err != nil {
		t.Fatal(err)
	}
	tr, _ = st.GetDataTransfer(tr.ID)
	if tr.Status != "completed" || tr.Cache.State != "ready" || tr.Cache.JobUID != "job-uid" {
		t.Fatal(tr)
	}
}
func TestPrewarmCleanupRefusesUnrelatedResources(t *testing.T) {
	mutations := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			mutations++
		}
		fmt.Fprint(w, `{"metadata":{"labels":{"fugue.pro/data-prewarm":"different-transfer"}}}`)
	}))
	defer server.Close()
	_, err := cleanupPrewarm(context.Background(), &kubeClient{baseURL: server.URL, client: server.Client()}, model.DataPrewarmCache{Namespace: "system", Job: "example", Claim: "example"}, "transfer_expected", true)
	if err == nil || mutations != 0 {
		t.Fatalf("unsafe cleanup: %d %v", mutations, err)
	}
}

func TestCanceledPrewarmCanCleanUpAfterRuntimeDisappears(t *testing.T) {
	st := store.New(t.TempDir() + "/state.json")
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := st.CreateTenant("Canceled cache tenant")
	ws, err := st.CreateDataWorkspace(model.DataWorkspace{TenantID: tenant.ID, Name: "dataset"})
	if err != nil {
		t.Fatal(err)
	}
	tr, err := st.CreateDataTransfer(model.DataTransfer{TenantID: tenant.ID, WorkspaceID: ws.ID, Direction: "prewarm", Status: "canceled", Target: "runtime_no_longer_exists", Cache: &model.DataPrewarmCache{Node: "former-node", State: "planned"}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("unexpected mutation %s", r.Method)
		}
		w.WriteHeader(404)
	}))
	defer server.Close()
	service := &Service{Store: st}
	client := &kubeClient{baseURL: server.URL, client: server.Client(), namespace: "system"}
	if err = service.reconcileDataPrewarm(context.Background(), client, tr); err != nil {
		t.Fatal(err)
	}
	tr, _ = st.GetDataTransfer(tr.ID)
	if tr.Status != "canceled" || tr.Cache.State != "removed" {
		t.Fatal(tr)
	}
}
