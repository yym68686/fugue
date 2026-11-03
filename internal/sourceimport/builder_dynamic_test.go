package sourceimport

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/runtime"
)

func TestHeavyPlacementReservesLiveHeadroomBelowLegacySixGiFloor(t *testing.T) {
	for _, mode := range []string{"normal", "capacity changes under lock", "lock contention", "equal OOM limit", "lower than OOM limit", "greater than OOM limit"} {
		t.Run(mode, func(t *testing.T) {
			policy := defaultBuilderPodPolicy()
			demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
			if err != nil {
				t.Fatal(err)
			}
			leases := map[string]builderKubeLease{}
			locked := false
			const leasePath = "/apis/coordination.k8s.io/v1/namespaces/fugue-system/leases"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes":
					writeBuilderTestJSON(t, w, builderKubeNodeList{Items: []builderKubeNode{
						builderTestKubeNode("worker-a", "worker-a", policy), builderTestKubeNode("worker-b", "worker-b", policy),
					}})
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/pods":
					name := strings.TrimPrefix(r.URL.Query().Get("fieldSelector"), "spec.nodeName=")
					memory := "4Gi"
					if name == "worker-a" {
						memory = "3Gi"
					}
					if name == "worker-a" && mode == "capacity changes under lock" && locked {
						memory = "7936Mi"
					}
					writeBuilderTestJSON(t, w, builderKubePodList{Items: []builderKubePod{builderTestPod(name, "Running", "250m", memory, "0")}})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/proxy/stats/summary"):
					writeBuilderTestJSON(t, w, builderTestNodeSummary("20Gi"))
				case r.Method == http.MethodGet && r.URL.Path == leasePath:
					var items []builderKubeLease
					for _, lease := range leases {
						if lease.Metadata.Labels["app.kubernetes.io/component"] == builderReservationComponentValue {
							items = append(items, lease)
						}
					}
					writeBuilderTestJSON(t, w, builderKubeLeaseList{Items: items})
				case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, leasePath+"/"):
					name := strings.TrimPrefix(r.URL.Path, leasePath+"/")
					if mode == "lock contention" && name == builderNodeLockLeaseName("worker-a") {
						writeBuilderTestJSON(t, w, builderTestLease(name, builderNodeLockComponentValue, "another-build", "worker-a", formatKubeTimestamp(time.Now()), 20, nil))
					} else if lease, ok := leases[name]; ok {
						writeBuilderTestJSON(t, w, lease)
					} else {
						w.WriteHeader(http.StatusNotFound)
					}
				case r.Method == http.MethodPost && r.URL.Path == leasePath:
					var lease builderKubeLease
					if err := json.NewDecoder(r.Body).Decode(&lease); err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
					leases[lease.Metadata.Name] = lease
					if lease.Metadata.Name == builderNodeLockLeaseName("worker-a") {
						locked = true
					}
					writeBuilderTestJSON(t, w, lease)
				case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, leasePath+"/"):
					delete(leases, strings.TrimPrefix(r.URL.Path, leasePath+"/"))
					w.WriteHeader(http.StatusOK)
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(500)
				}
			}))
			defer server.Close()
			s := &builderScheduler{client: &builderKubeClient{client: server.Client(), baseURL: server.URL, namespace: "fugue-system"},
				policy: policy, profile: builderWorkloadProfileHeavy, demand: demand, candidateCount: 3, reservationLeaseDuration: 35 * time.Minute}
			ctx := context.Background()
			if strings.Contains(mode, "OOM limit") {
				limit := int64(5 << 30)
				if mode == "lower than OOM limit" {
					limit = 6 << 30
				}
				if mode == "greater than OOM limit" {
					limit = 4 << 30
				}
				ctx = WithBuilderOOMGuard(ctx, func(context.Context) (int64, error) { return limit, nil }, nil)
			}
			placement, reservation, err := s.tryReservePlacement(ctx, "build-first")
			if mode == "equal OOM limit" || mode == "lower than OOM limit" {
				var blocked *BuilderMemoryAdmissionError
				if !errors.As(err, &blocked) || reservation != "" {
					t.Fatalf("unsafe reservation: %+v %s %v", placement, reservation, err)
				}
				for _, lease := range leases {
					if lease.Metadata.Labels["app.kubernetes.io/component"] == builderReservationComponentValue {
						t.Fatal("blocked build reserved capacity")
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wantNode, wantMemory := "worker-a", "5Gi"
			if mode == "capacity changes under lock" || mode == "lock contention" {
				wantNode, wantMemory = "worker-b", "4Gi"
			}
			if placement.PreferredHostname != wantNode || placement.AllocatedDemand.MemoryBytes != parseBuilderBytes(wantMemory) {
				t.Fatalf("expected %s with %s, got %+v", wantNode, wantMemory, placement)
			}
			if got := parseBuilderInt64(leases[reservation].Metadata.Annotations[builderAnnotationMemoryBytes]); got != placement.AllocatedDemand.MemoryBytes {
				t.Fatalf("reservation %d does not match Pod budget %d", got, placement.AllocatedDemand.MemoryBytes)
			}
			podPolicy := builderPodPolicyWithAllocatedDemand(policy, builderWorkloadProfileHeavy, placement.AllocatedDemand)
			if parseBuilderBytes(podPolicy.Heavy.Resources.Requests["memory"]) != placement.AllocatedDemand.MemoryBytes ||
				parseBuilderBytes(podPolicy.Heavy.Resources.Limits["memory"]) != placement.AllocatedDemand.MemoryBytes {
				t.Fatal("Pod allocation differs from reservation")
			}
			if mode == "normal" {
				second, _, err := s.tryReservePlacement(context.Background(), "build-second")
				if err != nil || second.PreferredHostname != "worker-b" {
					t.Fatalf("second build reused reserved memory: %+v, %v", second, err)
				}
			}
		})
	}
}

func TestHeavyMinimumAndAllocationStayWithinPolicy(t *testing.T) {
	policy := defaultBuilderPodPolicy()
	demand, _ := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	for _, tc := range []struct {
		available string
		fits      bool
		allocated string
	}{
		{"511Mi", false, "511Mi"}, {"512Mi", true, "512Mi"}, {"4Gi", true, "4Gi"}, {"5Gi", true, "5Gi"}, {"24Gi", true, "16Gi"},
	} {
		t.Run(tc.available, func(t *testing.T) {
			available := builderResourceDemand{CPUMilli: 1000, MemoryBytes: parseBuilderBytes(tc.available), EphemeralBytes: parseBuilderBytes("20Gi")}
			if got := builderDemandFits(available, demand); got != tc.fits {
				t.Fatalf("fits=%t want=%t", got, tc.fits)
			}
			allocated := builderAllocatedDemand(policy, builderWorkloadProfileHeavy, builderCandidate{Available: available})
			if allocated.MemoryBytes != parseBuilderBytes(tc.allocated) || allocated.MemoryBytes > available.MemoryBytes {
				t.Fatal("allocation exceeds available memory", allocated)
			}
		})
	}
	policy.HeavyMemoryMinimum = "2Gi"
	demand, _ = builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if demand.MemoryBytes != parseBuilderBytes("2Gi") {
		t.Fatal("configured admission floor ignored", demand)
	}
}

func TestBuilderInspectionExplainsDisabledAndInsufficientNodes(t *testing.T) {
	policy := defaultBuilderPodPolicy()
	demand, _ := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	disabled := builderTestNode("worker", "worker", policy, "8", "32Gi", "100Gi", "0", "1Gi", "0")
	disabled.Labels[runtime.SharedPoolLabelKey] = runtime.SharedPoolLabelValue
	disabled.Labels[runtime.BuildNodeLabelKey] = "false"
	if builderNodeEligibleForBuilders(policy, disabled) {
		t.Fatal("disabled shared worker became a builder")
	}
	reasons := builderNodeInspectionReasons(policy, disabled, nil, "")
	if !strings.Contains(strings.Join(reasons, " "), "builds explicitly disabled") {
		t.Fatal("disabled node has no explanation", reasons)
	}
	reasons = builderInsufficientResourceReasons(builderResourceDemand{}, demand)
	for _, resource := range []string{"cpu_milli", "memory_bytes", "ephemeral_bytes"} {
		if !strings.Contains(strings.Join(reasons, " "), "insufficient "+resource) {
			t.Fatal("missing capacity reason", resource, reasons)
		}
	}
}

func TestDynamicOOMRetryUsesActualAllocationAndStopsAtCeiling(t *testing.T) {
	base := defaultBuilderPodPolicy()
	ctx := withBuilderEffectivePolicy(context.Background(), base, builderWorkloadProfileHeavy)
	calls := 0
	err := runBuilderJobWithRetry(ctx, "dockerfile", "dynamic-build", "registry.example/build:test", nil, func(_ context.Context, attempt builderJobAttempt) error {
		calls++
		if calls == 2 {
			policy, err := builderPodPolicyForJobAttempt(base, builderWorkloadProfileHeavy, attempt)
			if err != nil {
				t.Fatal(err)
			}
			demand, _ := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
			if demand.MemoryBytes != parseBuilderBytes("12289Mi") {
				t.Fatal("retry regressed below previous 12Gi allocation", demand)
			}
			return nil
		}
		actual := builderPodPolicyWithAllocatedDemand(base, builderWorkloadProfileHeavy, builderResourceDemand{MemoryBytes: parseBuilderBytes("12Gi")})
		return withBuilderAttemptFailure(errors.New("container OOMKilled"), actual, builderWorkloadProfileHeavy)
	})
	if err != nil || calls != 2 {
		t.Fatal("dynamic retry failed", calls, err)
	}
	actual := builderPodPolicyWithAllocatedDemand(base, builderWorkloadProfileHeavy, builderResourceDemand{MemoryBytes: parseBuilderBytes("16Gi")})
	err = classifyExhaustedBuilderPolicy(ctx, builderJobAttempt{Number: 1}, withBuilderAttemptFailure(errors.New("container OOMKilled"), actual, builderWorkloadProfileHeavy))
	var exhausted *BuilderResourceFailure
	if !errors.As(err, &exhausted) || exhausted.Preflight || exhausted.LimitBytes != parseBuilderBytes("16Gi") {
		t.Fatal("actual ceiling failure was not terminal", err)
	}
}
