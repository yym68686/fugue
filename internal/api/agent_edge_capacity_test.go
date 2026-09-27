package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/agentedge"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func agentCapacityFixture(id, address string, now time.Time) (corev1.Node, kubeNodeSummary) {
	n := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: id, UID: types.UID(id + "-uid"), CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))}, Status: corev1.NodeStatus{
		Addresses:   []corev1.NodeAddress{{Type: corev1.NodeExternalIP, Address: address}},
		Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("1Gi")},
		Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}, {Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse}, {Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse}, {Type: corev1.NodePIDPressure, Status: corev1.ConditionFalse}},
	}}
	var summary kubeNodeSummary
	summary.Node.NodeName = id
	summary.Node.CPU.Time = now.Add(-2 * time.Second).Format(time.RFC3339Nano)
	summary.Node.Memory.Time = now.Add(-5 * time.Second).Format(time.RFC3339Nano)
	cpu, memory := uint64(250_000_000), uint64(128<<20)
	summary.Node.CPU.UsageNanoCores = &cpu
	summary.Node.Memory.WorkingSetBytes = &memory
	return n, summary
}

func TestAgentCapacityUsesOriginalAuthenticatedFactsAndRejectsUnknowns(t *testing.T) {
	now := time.Now().UTC()
	for _, scenario := range []string{"fresh", "missing cpu", "missing memory", "old cpu", "future memory", "previous node metrics", "cpu saturated", "memory saturated", "foreign address", "draining", "pressure", "unknown readiness", "duplicate condition", "changed uid", "changed allocatable", "drained during observation", "foreign summary", "unauthenticated"} {
		t.Run(scenario, func(t *testing.T) {
			node, summary := agentCapacityFixture("edge-a", "8.8.8.8", now)
			switch scenario {
			case "missing cpu":
				summary.Node.CPU.UsageNanoCores = nil
			case "missing memory":
				summary.Node.Memory.WorkingSetBytes = nil
			case "old cpu":
				summary.Node.CPU.Time = now.Add(-121 * time.Second).Format(time.RFC3339Nano)
			case "future memory":
				summary.Node.Memory.Time = now.Add(time.Second).Format(time.RFC3339Nano)
			case "previous node metrics":
				node.CreationTimestamp = metav1.NewTime(now.Add(-time.Second))
			case "cpu saturated":
				*summary.Node.CPU.UsageNanoCores = 1_900_000_000
			case "memory saturated":
				*summary.Node.Memory.WorkingSetBytes = 1 << 30
			case "foreign address":
				node.Status.Addresses[0].Address = "9.9.9.9"
			case "draining":
				node.Spec.Unschedulable = true
			case "pressure":
				node.Status.Conditions[1].Status = corev1.ConditionTrue
			case "unknown readiness":
				node.Status.Conditions[0].Status = corev1.ConditionUnknown
			case "duplicate condition":
				node.Status.Conditions = append(node.Status.Conditions, node.Status.Conditions[0])
			case "foreign summary":
				summary.Node.NodeName = "foreign"
			}
			reads := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer synthetic-capacity-token" {
					t.Error("capacity request lost Kubernetes authentication")
				}
				if scenario == "unauthenticated" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				switch r.URL.Path {
				case "/api/v1/nodes/edge-a":
					reads++
					if reads == 2 {
						switch scenario {
						case "changed uid":
							node.UID = "replacement-node"
						case "changed allocatable":
							node.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("1")
						case "drained during observation":
							node.Spec.Unschedulable = true
						}
					}
					json.NewEncoder(w).Encode(node)
				case "/api/v1/nodes/edge-a/proxy/stats/summary":
					json.NewEncoder(w).Encode(summary)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := &clusterNodeClient{baseURL: server.URL, client: server.Client(), bearerToken: "synthetic-capacity-token"}
			e, err := readAgentCapacity(context.Background(), client, "edge-a", "8.8.8.8", agentedge.CapacityPolicy{MaxNodeCPUPercent: 85, MaxNodeMemoryPercent: 85, FactMaxAgeSeconds: 120}, now)
			if scenario != "fresh" {
				if err == nil {
					t.Fatal("invalid capacity authorized an Edge")
				}
				return
			}
			if err != nil || !e.ObservedAt.Equal(now.Add(-5*time.Second)) || !e.ValidUntil.Equal(now.Add(115*time.Second)) || e.NodeUID != "edge-a-uid" || reads != 2 {
				t.Fatalf("original capacity identity/lifetime was lost: %+v %v", e, err)
			}
		})
	}
}
