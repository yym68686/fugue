package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

func TestCompletedBuilderRegistersSharedRuntimeImageOnActualNode(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"complete", "failed-pod", "old-job-pod", "wrong-owner", "ambiguous-node", "remote-push", "read-failed", "incomplete-graph"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			state := store.New(filepath.Join(t.TempDir(), "store.json"))
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := state.CreateTenant("Tenant")
			if err != nil {
				t.Fatal(err)
			}
			_, secret, err := state.CreateNodeKey(tenant.ID, "default")
			if err != nil {
				t.Fatal(err)
			}
			updater, _, err := state.EnrollNodeUpdater(secret, "worker-1", "https://203.0.113.30:9443", nil, "worker-1", "machine-1", "v2", "join-v2", []string{"heartbeat", "tasks"})
			if err != nil {
				t.Fatal(err)
			}
			app := model.App{ID: "app-1", TenantID: tenant.ID}
			op := model.Operation{ID: "op-1", AppID: app.ID, TenantID: tenant.ID}
			labels := map[string]string{"fugue.pro/app-id": app.ID, "fugue.pro/tenant-id": tenant.ID, "fugue.pro/operation-id": op.ID}
			if scenario == "wrong-owner" {
				labels["fugue.pro/operation-id"] = "op-other"
			}
			pod := func(node, uid, phase string) map[string]any {
				return map[string]any{"metadata": map[string]any{"ownerReferences": []any{map[string]any{"kind": "Job", "uid": uid, "controller": true}}}, "spec": map[string]any{"nodeName": node}, "status": map[string]any{"phase": phase}}
			}
			pods := []any{pod("worker-1", "job-current", "Succeeded"), pod("worker-2", "job-current", "Failed")}
			switch scenario {
			case "failed-pod":
				pods = []any{pod("worker-1", "job-current", "Failed")}
			case "old-job-pod":
				pods = []any{pod("worker-1", "job-old", "Succeeded")}
			case "ambiguous-node":
				pods = append(pods, pod("worker-2", "job-current", "Succeeded"))
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if scenario == "read-failed" {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				switch r.URL.Path {
				case "/apis/batch/v1/namespaces/platform/jobs/build-1":
					_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": "job-current", "labels": labels}, "status": map[string]int{"succeeded": 1}})
				case "/api/v1/namespaces/platform/pods":
					if r.URL.Query().Get("labelSelector") != "job-name=build-1" {
						t.Error("unscoped pod query")
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"items": pods})
				default:
					t.Errorf("unexpected request: %s", r.URL)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			const managedRef = "registry.fugue.internal:5000/fugue-apps/demo:build-op-1"
			const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			svc := &Service{Store: state, Config: config.ControllerConfig{ImageStoreMode: "distributed", KubectlNamespace: "platform"}, builderRegistryPushBase: "127.0.0.1:5000", registryPushBase: "registry.fugue.internal:5000"}
			svc.newKubeClient = func(string) (*kubeClient, error) {
				return &kubeClient{client: server.Client(), baseURL: server.URL, namespace: "platform"}, nil
			}
			verified := false
			svc.verifyDestinationImageCache = func(_ context.Context, endpoint, ref string) (destinationImageCacheVerification, error) {
				verified = true
				if endpoint != "http://203.0.113.30:5000" || ref != managedRef {
					t.Fatalf("incorrect verification target: %s %s", endpoint, ref)
				}
				if scenario == "incomplete-graph" {
					return destinationImageCacheVerification{}, fmt.Errorf("blob unavailable")
				}
				return destinationImageCacheVerification{Repo: "fugue-apps/demo", Target: "build-op-1", Available: true, CanonicalDigest: digest, ReferencedBlobs: []string{"sha256:" + strings.Repeat("b", 64)}, ReferencedBlobBytes: 128}, nil
			}
			result := sourceimport.GitHubImportResult{BuildJobName: "build-1", ImageRef: managedRef, DestinationImageRef: "127.0.0.1:5000/fugue-apps/demo:build-op-1"}
			if scenario == "remote-push" {
				result.DestinationImageRef = "remote.example/fugue-apps/demo:build-op-1"
			}
			destination, err := svc.completedBuilderImageDestination(context.Background(), app, op, result)
			if err == nil {
				err = svc.recordImportedDistributedImage(context.Background(), app, op, managedRef, managedRef, destination)
			}
			images, listErr := state.ListImages(model.ImageFilter{AppID: app.ID, PlatformAdmin: true})
			if listErr != nil {
				t.Fatal(listErr)
			}
			if scenario != "complete" {
				if err == nil || len(images) != 0 {
					t.Fatalf("unverified evidence created authority: err=%v images=%+v", err, images)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !verified || len(images) != 1 || images[0].CanonicalDigest != digest {
				t.Fatalf("missing verified image identity: %+v", images)
			}
			replicas, err := state.ListImageReplicas(model.ImageReplicaFilter{ImageID: images[0].ID, PlatformAdmin: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(replicas) != 1 || replicas[0].RuntimeID != updater.RuntimeID || replicas[0].ClusterNodeName != "worker-1" || replicas[0].Status != model.ImageReplicaStatusPresent {
				t.Fatalf("replica not attributed to actual successful builder: %+v", replicas)
			}
		})
	}
}
