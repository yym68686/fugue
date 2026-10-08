package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/model"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNetworkNodeCapacityPreservesSaturationAndOriginalLimits(t *testing.T) {
	now := time.Now().UTC()
	for _, scenario := range []string{"fresh", "saturated", "pressure", "zero_observed", "missing_cpu", "missing_memory", "zero_limit", "missing_condition", "unknown_condition", "duplicate_condition", "foreign_address", "foreign_summary", "old_sample", "future_sample", "wrong_uid", "changed_limits", "changed_pressure", "auth_failure", "expired_during_recheck", "collected_during_read"} {
		t.Run(scenario, func(t *testing.T) {
			node, summary := agentCapacityFixture("edge-a", "203.0.113.5", now)
			switch scenario {
			case "saturated":
				*summary.Node.CPU.UsageNanoCores = 2_100_000_000
				*summary.Node.Memory.WorkingSetBytes = 2 << 30
			case "pressure":
				node.Status.Conditions[1].Status = corev1.ConditionTrue
			case "zero_observed":
				*summary.Node.CPU.UsageNanoCores = 0
				*summary.Node.Memory.WorkingSetBytes = 0
			case "missing_cpu":
				summary.Node.CPU.UsageNanoCores = nil
			case "missing_memory":
				summary.Node.Memory.WorkingSetBytes = nil
			case "zero_limit":
				delete(node.Status.Allocatable, corev1.ResourceCPU)
			case "missing_condition":
				node.Status.Conditions = node.Status.Conditions[:2]
			case "unknown_condition":
				node.Status.Conditions[2].Status = corev1.ConditionUnknown
			case "duplicate_condition":
				node.Status.Conditions = append(node.Status.Conditions, node.Status.Conditions[1])
			case "foreign_address":
				node.Status.Addresses[0].Address = "203.0.113.6"
			case "foreign_summary":
				summary.Node.NodeName = "edge-b"
			case "old_sample":
				summary.Node.CPU.Time = now.Add(-3 * time.Minute).Format(time.RFC3339Nano)
			case "future_sample", "collected_during_read":
				summary.Node.CPU.Time = now.Add(time.Second).Format(time.RFC3339Nano)
			}
			reads := 0
			clockAt := now
			server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Header.Get("Authorization") != "Bearer capacity-test" {
					t.Error("lost Kubernetes authentication")
				}
				if scenario == "auth_failure" {
					writer.WriteHeader(http.StatusUnauthorized)
					return
				}
				switch request.URL.Path {
				case "/api/v1/nodes/edge-a":
					reads++
					if reads == 2 {
						switch scenario {
						case "wrong_uid":
							node.UID = "replacement"
						case "changed_limits":
							node.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("1")
						case "changed_pressure":
							node.Status.Conditions[1].Status = corev1.ConditionTrue
						case "expired_during_recheck":
							clockAt = now.Add(3 * time.Minute)
						}
					}
					json.NewEncoder(writer).Encode(node)
				case "/api/v1/nodes/edge-a/proxy/stats/summary":
					if scenario == "collected_during_read" {
						clockAt = now.Add(2 * time.Second)
					}
					json.NewEncoder(writer).Encode(summary)
				default:
					http.NotFound(writer, request)
				}
			}))
			defer server.Close()
			client := &clusterNodeClient{baseURL: server.URL, client: server.Client(), bearerToken: "capacity-test"}
			capacity, err := readNetworkNodeCapacity(context.Background(), client, "edge-a", "203.0.113.5", func() time.Time { return clockAt })
			valid := scenario == "fresh" || scenario == "saturated" || scenario == "pressure" || scenario == "zero_observed" || scenario == "collected_during_read"
			if !valid {
				if err == nil || capacity != nil {
					t.Fatal("invalid capacity admitted", capacity, err)
				}
				return
			}
			if err != nil || model.ValidateEdgeNetworkNodeCapacity(capacity) != nil || reads != 2 || capacity.NodeUID != string(node.UID) ||
				capacity.CPUUsageNanoCores != *summary.Node.CPU.UsageNanoCores || capacity.MemoryWorkingSetBytes != *summary.Node.Memory.WorkingSetBytes ||
				capacity.CPUAllocatableMilliCores != 2000 || capacity.MemoryAllocatableBytes != 1<<30 || !capacity.ObservedAt.Equal(now.Add(-5*time.Second)) {
				t.Fatal("capacity identity, timestamps or denominators changed", capacity, err)
			}
			if scenario == "pressure" && (len(capacity.Pressure) != 1 || capacity.Pressure[0] != "MemoryPressure") {
				t.Fatal("observed pressure concealed", capacity)
			}
		})
	}
}
