package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"

	coordinationv1 "k8s.io/api/coordination/v1"
)

func TestKubernetesControlPlaneBackupCoordinationLeaseLifecycle(t *testing.T) {
	t.Parallel()

	const (
		namespace = "fugue-system"
		leaseName = "fugue-fugue-control-plane-db-backup"
		runID     = "backup_run_123"
	)
	apiPath := "/apis/coordination.k8s.io/v1/namespaces/" + namespace + "/leases/" + leaseName
	collectionPath := "/apis/coordination.k8s.io/v1/namespaces/" + namespace + "/leases"
	var (
		mu       sync.Mutex
		lease    coordinationv1.Lease
		present  bool
		revision int
	)
	kubeAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-service-account-token" {
			http.Error(w, "missing service account token", http.StatusUnauthorized)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodGet && r.URL.Path == apiPath:
			if !present {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(lease)
		case r.Method == http.MethodPost && r.URL.Path == collectionPath:
			if present {
				http.Error(w, "conflict", http.StatusConflict)
				return
			}
			if err := json.NewDecoder(r.Body).Decode(&lease); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			revision++
			lease.ResourceVersion = "1"
			present = true
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPut && r.URL.Path == apiPath:
			var update coordinationv1.Lease
			if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if !present {
				http.NotFound(w, r)
				return
			}
			if update.ResourceVersion != lease.ResourceVersion {
				http.Error(w, "conflict", http.StatusConflict)
				return
			}
			revision++
			update.ResourceVersion = strconv.Itoa(revision)
			lease = update
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected Kubernetes API request", http.StatusNotFound)
		}
	}))
	t.Cleanup(kubeAPI.Close)

	server := &Server{
		backupCoordination: BackupCoordinationConfig{
			LeaseName:      leaseName,
			LeaseNamespace: namespace,
			LeaseDuration:  120 * time.Second,
			RenewPeriod:    30 * time.Second,
		},
		newClusterNodeClient: func() (*clusterNodeClient, error) {
			return &clusterNodeClient{
				client:      kubeAPI.Client(),
				baseURL:     kubeAPI.URL,
				bearerToken: "test-service-account-token",
			}, nil
		},
	}
	coordinationLease, err := server.newKubernetesControlPlaneBackupCoordinationLease(runID)
	if err != nil {
		t.Fatalf("create coordination Lease client: %v", err)
	}
	defer coordinationLease.Close()
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	held, err := coordinationLease.TryAcquireOrRenew(context.Background(), now)
	if err != nil || !held {
		t.Fatalf("acquire coordination Lease: held=%v err=%v", held, err)
	}

	mu.Lock()
	created := *lease.DeepCopy()
	mu.Unlock()
	if created.Name != leaseName || created.Namespace != namespace {
		t.Fatalf("unexpected Lease identity: %+v", created.ObjectMeta)
	}
	if created.Spec.HolderIdentity == nil || *created.Spec.HolderIdentity != "backup/"+runID {
		t.Fatalf("unexpected backup holder: %+v", created.Spec.HolderIdentity)
	}
	if created.Spec.LeaseDurationSeconds == nil || *created.Spec.LeaseDurationSeconds != 120 {
		t.Fatalf("unexpected Lease duration: %+v", created.Spec.LeaseDurationSeconds)
	}
	token := created.Annotations[controlPlaneBackupCoordinationTokenAnnotation]
	if len(token) != 32 || strings.Trim(token, "0123456789abcdef") != "" {
		t.Fatalf("expected a 128-bit lowercase hex fencing token, got %q", token)
	}

	held, err = coordinationLease.TryAcquireOrRenew(context.Background(), now.Add(30*time.Second))
	if err != nil || !held {
		t.Fatalf("renew coordination Lease: held=%v err=%v", held, err)
	}
	released, err := coordinationLease.Release(context.Background(), now.Add(31*time.Second))
	if err != nil || !released {
		t.Fatalf("release coordination Lease: released=%v err=%v", released, err)
	}
	mu.Lock()
	releasedLease := *lease.DeepCopy()
	mu.Unlock()
	if releasedLease.Spec.HolderIdentity == nil || *releasedLease.Spec.HolderIdentity != "" {
		t.Fatalf("expected empty holder after release: %+v", releasedLease.Spec.HolderIdentity)
	}
	if releasedLease.Spec.LeaseDurationSeconds == nil || *releasedLease.Spec.LeaseDurationSeconds != 0 {
		t.Fatalf("expected zero duration after release: %+v", releasedLease.Spec.LeaseDurationSeconds)
	}
	if got := releasedLease.Annotations[controlPlaneBackupCoordinationTokenAnnotation]; got != "" {
		t.Fatalf("expected fencing token to be cleared, got %q", got)
	}
}

func TestExpiredBackupCoordinationHolderRequiresExactStoreProof(t *testing.T) {
	t.Parallel()

	t.Run("same pending run allowed", func(t *testing.T) {
		stateStore, backendID := newBackupCoordinationProofStore(t)
		pending := createBackupCoordinationProofRun(t, stateStore, backendID, model.BackupRunStatusPending)
		server := &Server{store: stateStore}
		if !server.canTakeOverExpiredBackupCoordinationHolder(pending.ID, "backup/"+pending.ID) {
			t.Fatal("same pending run should recover its expired execution token")
		}
	})

	t.Run("different terminal run allowed", func(t *testing.T) {
		stateStore, backendID := newBackupCoordinationProofStore(t)
		terminal := createBackupCoordinationProofRun(t, stateStore, backendID, model.BackupRunStatusSucceeded)
		requested := createBackupCoordinationProofRun(t, stateStore, backendID, model.BackupRunStatusPending)
		server := &Server{store: stateStore}
		if !server.canTakeOverExpiredBackupCoordinationHolder(requested.ID, "backup/"+terminal.ID) {
			t.Fatal("durably terminal old run should permit expired-holder takeover")
		}
	})

	t.Run("different pending run denied", func(t *testing.T) {
		stateStore, backendID := newBackupCoordinationProofStore(t)
		pending := createBackupCoordinationProofRun(t, stateStore, backendID, model.BackupRunStatusPending)
		server := &Server{store: stateStore}
		if server.canTakeOverExpiredBackupCoordinationHolder("backup_run_new", "backup/"+pending.ID) {
			t.Fatal("a different pending run is not proof that the old execution is finished")
		}
	})

	t.Run("running run denied", func(t *testing.T) {
		stateStore, backendID := newBackupCoordinationProofStore(t)
		running := createBackupCoordinationProofRun(t, stateStore, backendID, model.BackupRunStatusRunning)
		server := &Server{store: stateStore}
		if server.canTakeOverExpiredBackupCoordinationHolder("backup_run_new", "backup/"+running.ID) {
			t.Fatal("running old run must never be taken over from store status alone")
		}
	})

	t.Run("unknown run denied", func(t *testing.T) {
		stateStore, _ := newBackupCoordinationProofStore(t)
		server := &Server{store: stateStore}
		if server.canTakeOverExpiredBackupCoordinationHolder("backup_run_new", "backup/missing") {
			t.Fatal("missing run must fail closed")
		}
	})

	t.Run("release holder denied", func(t *testing.T) {
		stateStore, _ := newBackupCoordinationProofStore(t)
		server := &Server{store: stateStore}
		if server.canTakeOverExpiredBackupCoordinationHolder("backup_run_new", "release/123-1") {
			t.Fatal("backup worker must never take over a release holder")
		}
	})
}

func newBackupCoordinationProofStore(t *testing.T) (*store.Store, string) {
	t.Helper()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "coordination-proof-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "coordination-proof-bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backup backend: %v", err)
	}
	return stateStore, backend.ID
}

func createBackupCoordinationProofRun(t *testing.T, stateStore *store.Store, backendID, status string) model.BackupRun {
	t.Helper()
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: backendID,
		Trigger:   model.BackupRunTriggerManual,
		Status:    model.BackupRunStatusPending,
	})
	if err != nil {
		t.Fatalf("create backup run: %v", err)
	}
	if status == model.BackupRunStatusPending {
		return run
	}
	now := time.Now().UTC()
	claimed, err := stateStore.ClaimBackupRun(run.ID, "test-worker", now, 2*time.Minute)
	if err != nil {
		t.Fatalf("claim backup run: %v", err)
	}
	if status == model.BackupRunStatusRunning {
		return claimed
	}
	finished, err := stateStore.FinishBackupRun(run.ID, "test-worker", store.BackupRunFinish{
		Status:     status,
		FinishedAt: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("finish backup run: %v", err)
	}
	return finished
}
