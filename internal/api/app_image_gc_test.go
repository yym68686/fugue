package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRequestAppImageRegistryGarbageCollectUpdatesLease(t *testing.T) {
	t.Parallel()

	var updated coordinationv1.Lease
	kubeAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/coordination.k8s.io/v1/namespaces/fugue-system/leases/fugue-fugue-registry-gc" {
			t.Fatalf("unexpected Kubernetes API path %s", r.URL.Path)
		}
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "fugue-fugue-registry-gc",
					Namespace:       "fugue-system",
					ResourceVersion: "7",
				},
			})
		case http.MethodPut:
			if err := json.NewDecoder(r.Body).Decode(&updated); err != nil {
				t.Fatalf("decode updated lease: %v", err)
			}
			_ = json.NewEncoder(w).Encode(updated)
		default:
			t.Fatalf("unexpected Kubernetes API method %s", r.Method)
		}
	}))
	defer kubeAPI.Close()

	server := &Server{
		controlPlaneNamespace: "fugue-system",
		registryGCLeaseName:   "fugue-fugue-registry-gc",
		newClusterNodeClient: func() (*clusterNodeClient, error) {
			return &clusterNodeClient{
				client:  kubeAPI.Client(),
				baseURL: kubeAPI.URL,
			}, nil
		},
	}
	if err := server.requestAppImageRegistryGarbageCollect(context.Background(), "stale app image deleted"); err != nil {
		t.Fatalf("request registry GC: %v", err)
	}
	if updated.Annotations[appImageRegistryGCRequestReasonAnnotation] != "stale app image deleted" {
		t.Fatalf("unexpected registry GC request annotations: %+v", updated.Annotations)
	}
	if updated.Annotations[appImageRegistryGCRequestedAtAnnotation] == "" {
		t.Fatalf("expected registry GC request timestamp: %+v", updated.Annotations)
	}
}
