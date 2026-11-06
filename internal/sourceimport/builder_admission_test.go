package sourceimport

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
)

// The fake API supports the real lock, refresh and reservation path, so these
// tests distinguish a pre-Pod capacity rejection from a safe later admission.
func admissionTestScheduler(t *testing.T, resource string, recoverAfter int32) (*builderScheduler, *atomic.Int32) {
	t.Helper()
	policy := builderPodPolicyForRetryCounts(defaultBuilderPodPolicy(), builderWorkloadProfileHeavy, 0, 1)
	var reads atomic.Int32
	leases := map[string]builderKubeLease{}
	const path = "/apis/coordination.k8s.io/v1/namespaces/fugue-system/leases"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/v1/nodes":
			read := reads.Add(1)
			node := builderTestKubeNode("worker-a", "host-a", policy)
			node.Status.Allocatable["ephemeral-storage"] = "40Gi"
			if recoverAfter == 0 || read <= recoverAfter {
				switch resource {
				case "cpu":
					node.Status.Allocatable["cpu"] = "500m"
				case "memory":
					node.Status.Allocatable["memory"] = "2Gi"
				case "storage":
					node.Status.Allocatable["ephemeral-storage"] = "10Gi"
				case "disabled":
					node.Metadata.Labels[policy.BuildNodeLabelKey] = "false"
				}
			}
			writeBuilderTestJSON(t, w, builderKubeNodeList{Items: []builderKubeNode{node}})
		case r.Method == "GET" && r.URL.Path == "/api/v1/pods":
			writeBuilderTestJSON(t, w, builderKubePodList{})
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/proxy/stats/summary"):
			if resource == "summary" {
				http.Error(w, "unavailable", 503)
				return
			}
			writeBuilderTestJSON(t, w, builderTestNodeSummary("40Gi"))
		case r.Method == "GET" && r.URL.Path == path:
			var list builderKubeLeaseList
			for _, lease := range leases {
				if lease.Metadata.Labels["app.kubernetes.io/component"] == builderReservationComponentValue {
					list.Items = append(list.Items, lease)
				}
			}
			writeBuilderTestJSON(t, w, list)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, path+"/"):
			if lease, ok := leases[strings.TrimPrefix(r.URL.Path, path+"/")]; ok {
				writeBuilderTestJSON(t, w, lease)
			} else {
				w.WriteHeader(404)
			}
		case r.Method == "POST" && r.URL.Path == path:
			var lease builderKubeLease
			if err := json.NewDecoder(r.Body).Decode(&lease); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			leases[lease.Metadata.Name] = lease
			writeBuilderTestJSON(t, w, lease)
		case r.Method == "DELETE" && strings.HasPrefix(r.URL.Path, path+"/"):
			delete(leases, strings.TrimPrefix(r.URL.Path, path+"/"))
		default:
			t.Errorf("unexpected API call: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	t.Cleanup(server.Close)
	s := &builderScheduler{client: &builderKubeClient{client: server.Client(), baseURL: server.URL, namespace: "fugue-system"},
		policy: policy, profile: builderWorkloadProfileHeavy, candidateCount: 3,
		selectionTimeout: time.Second, retryInterval: time.Millisecond, reservationLeaseDuration: 35 * time.Minute}
	return s, &reads
}

func admissionTestContext() context.Context {
	return WithBuilderOOMGuard(context.Background(), func(context.Context, string) (int64, error) { return 2 << 30, nil }, nil)
}

func TestPlacementAfterOOMReportsActualConstraintAndRetryInputs(t *testing.T) {
	for _, test := range []struct{ resource, cause string }{
		{"cpu", "builder_cpu_unavailable"}, {"memory", "builder_memory_unavailable"},
		{"storage", "builder_storage_unavailable"}, {"disabled", "placement_constraints_unsatisfied"},
		{"summary", "evidence_unavailable"},
	} {
		t.Run(test.resource, func(t *testing.T) {
			s, _ := admissionTestScheduler(t, test.resource, 0)
			ctx := admissionTestContext()
			_, reservation, err := s.tryReservePlacement(ctx, "build-a")
			var placement *builderPlacementError
			var memory *BuilderMemoryAdmissionError
			if !errors.As(err, &placement) || errors.As(err, &memory) || reservation != "" {
				t.Fatalf("wrong rejection: %s %v", reservation, err)
			}
			if placement.Inspection.Profile != "heavy" || placement.Inspection.Demand.MemoryBytes != (2<<30)+(1<<20) || placement.Inspection.Demand.EphemeralBytes != 12<<30 {
				t.Fatalf("lost effective retry inputs: %+v", placement.Inspection)
			}
			var fact model.BuilderAttemptEvidence
			var diagnostics map[string]any
			ctx = WithBuilderEvidenceRecorder(ctx, func(f model.BuilderAttemptEvidence, d map[string]any) { fact, diagnostics = f, d })
			recordBuilderAttempt(ctx, "build-a", builderJobAttempt{Number: 3, OOMRetryCount: 1, EphemeralRetryCount: 1}, time.Now(), err)
			if fact.Outcome != "rejected" || !hasBuilderCause(fact, test.cause) || hasBuilderCause(fact, "builder_memory_not_increased") || fact.MemoryLimitBytes != 0 {
				t.Fatalf("incorrect execution evidence: %+v", fact)
			}
			if diagnostics["builder_placement"] == nil || diagnostics["effective_policy"] == nil || diagnostics["execution_started"] != false {
				t.Fatalf("missing decision evidence: %+v", diagnostics)
			}
			if test.resource == "summary" && len(fact.MissingEvidence) == 0 {
				t.Fatal("missing statistics presented as complete evidence")
			}
		})
	}
}

func TestPlacementAfterOOMWaitsForCapacityAndReservesEscalatedStorage(t *testing.T) {
	s, reads := admissionTestScheduler(t, "cpu", 2)
	placement, reservation, err := s.reservePlacement(admissionTestContext(), "build-a")
	if err != nil || reservation == "" || reads.Load() < 4 {
		t.Fatalf("did not wait and revalidate: %+v %s %v reads=%d", placement, reservation, err, reads.Load())
	}
	if placement.AllocatedDemand.MemoryBytes <= 2<<30 || placement.AllocatedDemand.EphemeralBytes != 12<<30 {
		t.Fatalf("unsafe retry allocation: %+v", placement)
	}
}

func TestPlacementWaitIsBoundedAndDoesNotRestartJobRetry(t *testing.T) {
	s, reads := admissionTestScheduler(t, "cpu", 0)
	s.selectionTimeout = 200 * time.Millisecond
	s.retryInterval = 10 * time.Millisecond
	started := time.Now()
	_, _, err := s.reservePlacement(admissionTestContext(), "build-a")
	var placement *builderPlacementError
	if !errors.As(err, &placement) || errors.Is(err, context.DeadlineExceeded) || reads.Load() < 2 || time.Since(started) < s.selectionTimeout || time.Since(started) > time.Second {
		t.Fatalf("unbounded or premature wait: %v reads=%d", err, reads.Load())
	}
	if retry, _ := shouldRetryBuilderJobFailure(err); retry {
		t.Fatal("exhausted selection window must not restart a job attempt")
	}
}

func TestPlacementPolicyCeilingRefusalDoesNotWaitOrReadNodes(t *testing.T) {
	s, reads := admissionTestScheduler(t, "cpu", 0)
	ctx := WithBuilderOOMGuard(context.Background(), func(context.Context, string) (int64, error) { return 16 << 30, nil }, nil)
	_, _, err := s.reservePlacement(ctx, "build-a")
	var admission *BuilderMemoryAdmissionError
	if !errors.As(err, &admission) || reads.Load() != 0 {
		t.Fatalf("policy refusal must remain terminal: %v reads=%d", err, reads.Load())
	}
}

func TestPlacementRechecksOOMHistoryBeforeReservation(t *testing.T) {
	s, _ := admissionTestScheduler(t, "", 0)
	s.selectionTimeout = 200 * time.Millisecond
	s.retryInterval = 10 * time.Millisecond
	var historyReads atomic.Int32
	ctx := WithBuilderOOMGuard(context.Background(), func(context.Context, string) (int64, error) {
		if historyReads.Add(1) == 1 {
			return 2 << 30, nil
		}
		return 6 << 30, nil
	}, nil)
	_, reservation, err := s.reservePlacement(ctx, "build-a")
	var placement *builderPlacementError
	if reservation != "" || !errors.As(err, &placement) || historyReads.Load() < 3 || placement.Inspection.Demand.MemoryBytes <= 6<<30 {
		t.Fatalf("admitted stale OOM floor: reservation=%s reads=%d err=%v", reservation, historyReads.Load(), err)
	}
}
