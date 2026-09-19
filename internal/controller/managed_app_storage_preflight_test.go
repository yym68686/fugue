package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestValidatePersistentStorageClassMutationRejectsBoundClassChange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/persistentvolumeclaims/app-data") {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"metadata": map[string]any{"name": "app-data"},
			"spec":     map[string]any{"storageClassName": "longhorn", "volumeName": "pvc-bound"},
		})
	}))
	defer server.Close()
	client := &kubeClient{client: server.Client(), baseURL: server.URL, namespace: "tenant-a"}
	service := &Service{}
	objects := []map[string]any{{"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": map[string]any{"name": "app-data"}, "spec": map[string]any{"storageClassName": "fugue-workspace-rwo"}}}
	err := service.validatePersistentStorageClassMutation(context.Background(), client, "tenant-a", objects)
	if err == nil || !strings.Contains(err.Error(), "cannot be changed") {
		t.Fatalf("expected immutable storage class error, got %v", err)
	}
}

func TestPersistentStoragePreflightPreservesAllocation(t *testing.T) {
	for _, tc := range []struct {
		name, request, capacity, desired, want, volume string
	}{
		{"pending-claim-shrink", "2Gi", "", "1Gi", "2Gi", ""},
		{"bound-claim-shrink", "2Gi", "2Gi", "1Gi", "2Gi", "pv"},
		{"expansion-in-progress", "4Gi", "2Gi", "3Gi", "4Gi", "pv"},
		{"rounded-capacity", "1Gi", "2Gi", "1Gi", "2Gi", "pv"},
		{"increase", "1Gi", "1Gi", "3Gi", "3Gi", "pv"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("storage preflight wrote %s", r.Method)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"metadata": map[string]any{"name": "app-data"},
					"spec":     map[string]any{"volumeName": tc.volume, "resources": map[string]any{"requests": map[string]any{"storage": tc.request}}},
					"status":   map[string]any{"capacity": map[string]any{"storage": tc.capacity}},
				})
			}))
			defer server.Close()
			client := &kubeClient{client: server.Client(), baseURL: server.URL, namespace: "tenant-a"}
			objects := []map[string]any{{"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": map[string]any{"name": "app-data"}, "spec": map[string]any{"resources": map[string]any{"requests": map[string]any{"storage": tc.desired}}}}}
			if err := (&Service{}).validatePersistentStorageClassMutation(context.Background(), client, "tenant-a", objects); err != nil {
				t.Fatal(err)
			}
			if got := nestedObjectValue(objects[0], "spec", "resources", "requests", "storage"); got != tc.want {
				t.Fatalf("rendered storage request=%v, want %s", got, tc.want)
			}
		})
	}
}
