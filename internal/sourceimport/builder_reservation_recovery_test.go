package sourceimport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type reservationRecoveryFixture struct {
	mu             sync.Mutex
	lease          *builderKubeLease
	jobStatus      int
	jobTerminal    bool
	podStatus      int
	podPhase       string
	nodeReady      bool
	nodeStatus     int
	deleteFailures int
	deleteAttempts int
	renewOnDelete  bool
	workloadReads  int
}

func newReservationRecoveryFixture(t *testing.T) (*reservationRecoveryFixture, *builderScheduler) {
	t.Helper()
	lease := newBuilderLease("build-example", builderReservationComponentValue, "attempt-current", 35*time.Minute,
		map[string]string{builderAnnotationNodeName: "worker-a", builderAnnotationMemoryBytes: "2147483648"}, time.Now().Add(-5*time.Minute))
	lease.Metadata.UID = "lease-uid"
	lease.Metadata.ResourceVersion = "7"
	f := &reservationRecoveryFixture{lease: &lease, jobStatus: 404, nodeReady: true}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/leases") && r.Method == http.MethodGet:
			items := []builderKubeLease{}
			if f.lease != nil {
				items = append(items, *f.lease)
			}
			writeBuilderTestJSON(t, w, builderKubeLeaseList{Items: items})
		case strings.HasSuffix(r.URL.Path, "/leases/build-example") && r.Method == http.MethodGet:
			if f.lease == nil {
				w.WriteHeader(404)
			} else {
				writeBuilderTestJSON(t, w, f.lease)
			}
		case strings.HasSuffix(r.URL.Path, "/leases/build-example") && r.Method == http.MethodPut:
			var updated builderKubeLease
			if err := json.NewDecoder(r.Body).Decode(&updated); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			if f.lease == nil || updated.Metadata.ResourceVersion != f.lease.Metadata.ResourceVersion {
				w.WriteHeader(409)
				return
			}
			updated.Metadata.ResourceVersion = "8"
			f.lease = &updated
			writeBuilderTestJSON(t, w, updated)
		case strings.HasSuffix(r.URL.Path, "/leases/build-example") && r.Method == http.MethodDelete:
			f.deleteAttempts++
			if f.deleteFailures > 0 {
				f.deleteFailures--
				w.WriteHeader(503)
				return
			}
			var options struct {
				Preconditions struct{ UID, ResourceVersion string } `json:"preconditions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&options); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			if f.renewOnDelete {
				f.lease.Metadata.ResourceVersion = "9"
				f.lease.Spec.RenewTime = formatKubeTimestamp(time.Now())
			}
			if options.Preconditions.UID != f.lease.Metadata.UID || options.Preconditions.ResourceVersion != f.lease.Metadata.ResourceVersion {
				w.WriteHeader(409)
				return
			}
			f.lease = nil
			w.WriteHeader(200)
		case strings.Contains(r.URL.Path, "/jobs/"):
			f.workloadReads++
			if f.jobStatus != 200 {
				w.WriteHeader(f.jobStatus)
				return
			}
			conditions := []map[string]string{}
			if f.jobTerminal {
				conditions = append(conditions, map[string]string{"type": "Complete", "status": "True"})
			}
			writeBuilderTestJSON(t, w, map[string]any{"status": map[string]any{"conditions": conditions}})
		case strings.HasSuffix(r.URL.Path, "/pods"):
			if r.URL.Query().Get("labelSelector") != "job-name=build-example" {
				t.Errorf("unexpected pod selector: %s", r.URL.RawQuery)
			}
			if f.podStatus != 0 {
				w.WriteHeader(f.podStatus)
				return
			}
			pods := []builderKubePod{}
			if f.podPhase != "" {
				pods = append(pods, builderTestPod("worker-a", f.podPhase, "250m", "2Gi", "3Gi"))
			}
			writeBuilderTestJSON(t, w, builderKubePodList{Items: pods})
		case strings.HasSuffix(r.URL.Path, "/nodes/worker-a"):
			if f.nodeStatus != 0 {
				w.WriteHeader(f.nodeStatus)
				return
			}
			ready := "False"
			if f.nodeReady {
				ready = "True"
			}
			writeBuilderTestJSON(t, w, map[string]any{"status": map[string]any{"conditions": []map[string]string{{"type": "Ready", "status": ready}}}})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(500)
		}
	}))
	t.Cleanup(server.Close)
	return f, &builderScheduler{client: &builderKubeClient{client: server.Client(), baseURL: server.URL, namespace: "fugue-system"}, reservationHolder: "attempt-current"}
}

func TestBuilderReservationRecoveryAfterInterruptedCleanup(t *testing.T) {
	for _, tc := range []struct {
		name          string
		setup         func(*reservationRecoveryFixture)
		keep, wantErr bool
	}{
		{"deleted job with unexpired legacy reservation", func(f *reservationRecoveryFixture) { f.lease.Spec.HolderIdentity = f.lease.Metadata.Name }, false, false},
		{"fresh admission before job creation", func(f *reservationRecoveryFixture) { f.lease.Spec.RenewTime = formatKubeTimestamp(time.Now()) }, true, false},
		{"running job", func(f *reservationRecoveryFixture) { f.jobStatus = 200 }, true, false},
		{"completed job and terminal pod", func(f *reservationRecoveryFixture) { f.jobStatus = 200; f.jobTerminal = true; f.podPhase = "Succeeded" }, false, false},
		{"deleted job with running pod", func(f *reservationRecoveryFixture) { f.podPhase = "Running" }, true, false},
		{"deleted job with pending pod", func(f *reservationRecoveryFixture) { f.podPhase = "Pending" }, true, false},
		{"deleted job with unknown pod", func(f *reservationRecoveryFixture) { f.podPhase = "Unknown" }, true, false},
		{"unreachable node", func(f *reservationRecoveryFixture) { f.nodeReady = false }, true, false},
		{"job observation failed", func(f *reservationRecoveryFixture) { f.jobStatus = 503 }, true, true},
		{"pod observation failed", func(f *reservationRecoveryFixture) { f.podStatus = 503 }, true, true},
		{"node observation failed", func(f *reservationRecoveryFixture) { f.nodeStatus = 503 }, true, true},
		{"delete failed", func(f *reservationRecoveryFixture) { f.deleteFailures = 1 }, true, true},
		{"renewed during recovery", func(f *reservationRecoveryFixture) { f.renewOnDelete = true }, true, false},
		{"missing version", func(f *reservationRecoveryFixture) { f.lease.Metadata.ResourceVersion = "" }, true, true},
		{"unknown renewal", func(f *reservationRecoveryFixture) { f.lease.Spec.RenewTime = "invalid" }, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, scheduler := newReservationRecoveryFixture(t)
			tc.setup(f)
			err := scheduler.reconcileReservations(context.Background(), time.Now())
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr=%t", err, tc.wantErr)
			}
			if (f.lease != nil) != tc.keep {
				t.Fatalf("reservation retained=%t, want=%t", f.lease != nil, tc.keep)
			}
			if strings.HasPrefix(tc.name, "fresh") && f.workloadReads != 0 {
				t.Fatal("recovery inspected an in-progress admission")
			}
		})
	}
}

func TestBuilderReservationReleaseRetriesAndPeriodicRecovery(t *testing.T) {
	f, scheduler := newReservationRecoveryFixture(t)
	f.deleteFailures = 2
	if err := scheduler.releaseReservationWithRetry(context.Background(), "build-example", 0); err != nil {
		t.Fatal(err)
	}
	if f.lease != nil || f.deleteAttempts != 3 {
		t.Fatalf("lease=%+v attempts=%d", f.lease, f.deleteAttempts)
	}
	// A persistent outage survives the local retry budget, then is recovered by
	// the next sweep without waiting for the 35-minute reservation expiry.
	f, scheduler = newReservationRecoveryFixture(t)
	f.deleteFailures = 3
	if err := scheduler.releaseReservationWithRetry(context.Background(), "build-example", 0); err == nil || f.lease == nil {
		t.Fatal("failed deletion was not retained for recovery")
	}
	if err := scheduler.reconcileReservations(context.Background(), time.Now()); err != nil || f.lease != nil {
		t.Fatalf("periodic recovery failed: %v lease=%+v", err, f.lease)
	}
}

func TestBuilderReservationOldAttemptCannotReleaseRenewOrReplaceSuccessor(t *testing.T) {
	f, scheduler := newReservationRecoveryFixture(t)
	scheduler.reservationHolder = "attempt-old"
	if err := scheduler.releaseReservation(context.Background(), "build-example"); err != nil || f.lease == nil || f.deleteAttempts != 0 {
		t.Fatalf("old owner affected successor: %v", err)
	}
	if err := scheduler.renewReservation(context.Background(), "build-example"); err == nil {
		t.Fatal("old owner renewed successor")
	}
	if err := scheduler.upsertReservation(context.Background(), "build-example", "worker-b", builderResourceDemand{}); err == nil {
		t.Fatal("old owner replaced successor")
	}
	if f.lease.Spec.HolderIdentity != "attempt-current" || f.lease.Metadata.ResourceVersion != "7" {
		t.Fatal("successor was modified")
	}
}

func TestBuilderReservationSubmissionRechecksOwnership(t *testing.T) {
	f, scheduler := newReservationRecoveryFixture(t)
	if err := scheduler.renewReservation(context.Background(), "build-example"); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.reconcileReservations(context.Background(), time.Now()); err != nil || f.lease == nil {
		t.Fatalf("fresh submission was reclaimed: %v", err)
	}
	f.lease = nil
	if err := scheduler.renewReservation(context.Background(), "build-example"); err == nil {
		t.Fatal("submission without a reservation was allowed")
	}
}
