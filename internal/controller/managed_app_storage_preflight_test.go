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
