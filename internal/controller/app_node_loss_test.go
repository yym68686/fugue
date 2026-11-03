package controller

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestNodeLossUnknownExecutionOverridesStaleReadyDeployment(t *testing.T) {
	for _, signal := range []string{"phase", "reason", "condition"} {
		t.Run(signal, func(t *testing.T) {
			app := model.App{ID: "app_example", TenantID: "tenant_example", Spec: model.AppSpec{Replicas: 1}}
			deployment := kubeDeployment{}
			deployment.Metadata.Generation = 1
			deployment.Status.ObservedGeneration = 1
			deployment.Status.Replicas = 1
			deployment.Status.UpdatedReplicas = 1
			deployment.Status.ReadyReplicas = 1
			deployment.Status.AvailableReplicas = 1
			pod := kubePod{}
			pod.Status.Phase = "Running"
			switch signal {
			case "phase":
				pod.Status.Phase = "Unknown"
			case "reason":
				pod.Status.Reason = "NodeLost"
			case "condition":
				pod.Status.Conditions = []kubePodCondition{{Type: "Ready", Status: "Unknown"}}
			}
			status := buildManagedAppStatus(runtime.ManagedAppObject{}, app, deployment, true, []kubePod{pod}, nil)
			if status.Phase != runtime.ManagedAppPhaseProgressing || status.ReadyReplicas != 0 {
				t.Fatalf("unknown execution was reported ready: %#v", status)
			}
			for _, condition := range status.Conditions {
				if condition.Type == "NodeExecutionUnknown" && condition.Status == "True" {
					return
				}
			}
			t.Fatal("missing unknown execution condition")
		})
	}
}

func TestDisconnectedPodCleanupRequiresTerminalStateAndUID(t *testing.T) {
	for _, phase := range []string{"Running", "Unknown", "Pending", "Failed", "Succeeded"} {
		t.Run(phase, func(t *testing.T) {
			deletes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/pods"):
					json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"metadata": map[string]any{"name": "service-old", "uid": "old-uid", "deletionTimestamp": "2026-01-01T00:00:00Z"}, "spec": map[string]any{"nodeName": "worker-old"}, "status": map[string]any{"phase": phase}}}})
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/nodes/"):
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "worker-old", "uid": "node-old-uid"}, "status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "Unknown"}}}})
				case r.Method == http.MethodDelete:
					deletes++
					var body struct {
						Preconditions struct {
							UID string `json:"uid"`
						} `json:"preconditions"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Preconditions.UID != "old-uid" {
						t.Error("terminal deletion omitted immutable UID", err)
					}
					json.NewEncoder(w).Encode(map[string]any{})
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			err := (&Service{}).cleanupStrandedPodsBySelector(t.Context(), &kubeClient{baseURL: server.URL, client: server.Client()}, "tenant-example", "app=example", "app")
			if err != nil {
				t.Fatal(err)
			}
			want := 0 // API terminal phase alone also cannot fence an unavailable node.
			if deletes != want {
				t.Fatalf("%s deletion count %d, want %d", phase, deletes, want)
			}
		})
	}
}

func TestFailoverCannotUseMissingPodOrNewNodeIdentityAsTerminationProof(t *testing.T) {
	for _, scenario := range []string{"partition", "node-replaced", "terminating-pod", "confirmed-stop"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("fencing check mutated cluster")
					http.Error(w, "unexpected", 500)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if strings.Contains(r.URL.Path, "/nodes/") {
					uid, status := "old-node-uid", "True"
					if scenario == "partition" {
						status = "Unknown"
					}
					if scenario == "node-replaced" {
						uid = "new-node-uid"
					}
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "worker-old", "uid": uid}, "status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": status}}}})
					return
				}
				items := []any{}
				if scenario == "terminating-pod" {
					items = append(items, map[string]any{"metadata": map[string]any{"name": "service-old", "deletionTimestamp": "2026-01-01T00:00:00Z"}, "status": map[string]any{"phase": "Running"}})
				}
				json.NewEncoder(w).Encode(map[string]any{"items": items})
			}))
			defer server.Close()
			proof := appFailoverSource{RuntimeID: "runtime_source", Nodes: map[string]string{"worker-old": "old-node-uid"}}
			err := verifyAppFailoverSourceStopped(t.Context(), &kubeClient{baseURL: server.URL, client: server.Client()}, model.App{ID: "app_example", TenantID: "tenant_example"}, proof)
			if (err == nil) != (scenario == "confirmed-stop") {
				t.Fatalf("%s: %v", scenario, err)
			}
		})
	}
}
