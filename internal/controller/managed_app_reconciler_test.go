package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
	"fugue/internal/workloadidentity"
)

func TestManagedAppExpectedObjectNamesOmitsVolSyncByDefault(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Workspace: &model.AppWorkspaceSpec{
				MountPath: "/workspace",
			},
		},
	}

	names := managedAppExpectedObjectNamesByKind(app)
	if len(names[runtime.VolSyncReplicationDestinationKind]) != 0 {
		t.Fatalf("expected replication destination to be opt-in, got %+v", names[runtime.VolSyncReplicationDestinationKind])
	}
	if len(names[runtime.VolSyncReplicationSourceKind]) != 0 {
		t.Fatalf("expected replication source to be opt-in, got %+v", names[runtime.VolSyncReplicationSourceKind])
	}
}

func TestManagedAppExpectedObjectNamesIncludesVolSyncWhenReplicationEnabled(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Workspace: &model.AppWorkspaceSpec{
				MountPath: "/workspace",
			},
			VolumeReplication: &model.AppVolumeReplicationSpec{
				Mode: model.AppVolumeReplicationModeScheduled,
			},
		},
	}

	names := managedAppExpectedObjectNamesByKind(app)
	if _, ok := names[runtime.VolSyncReplicationDestinationKind][runtime.WorkspaceReplicationDestinationName(app)]; !ok {
		t.Fatalf("expected replication destination name, got %+v", names[runtime.VolSyncReplicationDestinationKind])
	}
	if _, ok := names[runtime.VolSyncReplicationSourceKind][runtime.WorkspaceReplicationSourceName(app)]; !ok {
		t.Fatalf("expected replication source name, got %+v", names[runtime.VolSyncReplicationSourceKind])
	}
}

func TestEnsureManagedPostgresDataSafetyRejectsImplicitInitDBForExistingService(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Status: model.AppStatus{
			Phase:           "deployed",
			CurrentReplicas: 1,
		},
		BackingServices: []model.BackingService{
			{
				ID:         "svc_pg",
				OwnerAppID: "app_demo",
				Type:       model.BackingServiceTypePostgres,
				Status:     model.BackingServiceStatusActive,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						ServiceName: "demo-postgres",
						Database:    "app",
						User:        "app",
					},
				},
				CreatedAt: time.Date(2026, time.May, 14, 10, 0, 0, 0, time.UTC),
			},
		},
	}
	desiredObjects := []map[string]any{
		{
			"apiVersion": runtime.CloudNativePGAPIVersion,
			"kind":       runtime.CloudNativePGClusterKind,
			"metadata": map[string]any{
				"name": "demo-postgres",
				"labels": map[string]any{
					runtime.FugueLabelBackingServiceType: model.BackingServiceTypePostgres,
					runtime.FugueLabelBackingServiceID:   "svc_pg",
				},
			},
		},
	}
	client := &kubeClient{
		baseURL: "https://kubernetes.example",
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodGet && req.URL.Path == "/apis/postgresql.cnpg.io/v1/namespaces/tenant-demo/clusters/demo-postgres" {
				return notFoundJSONResponse(), nil
			}
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.Path)
			return nil, nil
		})},
	}
	svc := &Service{now: func() time.Time {
		return time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC)
	}}

	err := svc.ensureManagedPostgresDataSafety(context.Background(), client, "tenant-demo", runtime.ManagedAppObject{}, app, desiredObjects)
	if err == nil {
		t.Fatal("expected implicit initdb to be rejected")
	}
	if !strings.Contains(err.Error(), "refusing to initialize managed postgres cluster") {
		t.Fatalf("expected managed postgres safety error, got %v", err)
	}
}

func TestStabilizeManagedPostgresStorageSpecsPreservesNonExpandablePVCSize(t *testing.T) {
	t.Parallel()

	const namespace = "tenant-demo"
	const clusterName = "demo-postgres"
	const pvcName = "demo-postgres-1"

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected request method %s for %s", r.Method, r.URL.String())
		}
		switch r.URL.Path {
		case "/api/v1/namespaces/" + namespace + "/persistentvolumeclaims":
			if got := r.URL.Query().Get("labelSelector"); got != "cnpg.io/cluster="+clusterName+",cnpg.io/pvcRole=PG_DATA" {
				t.Fatalf("unexpected pvc label selector %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name": pvcName,
						},
					},
				},
			})
		case "/api/v1/namespaces/" + namespace + "/persistentvolumeclaims/" + pvcName:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": pvcName,
				},
				"spec": map[string]any{
					"storageClassName": "local-path",
					"resources": map[string]any{
						"requests": map[string]string{
							"storage": "1Gi",
						},
					},
				},
				"status": map[string]any{
					"capacity": map[string]string{
						"storage": "1Gi",
					},
				},
			})
		case "/apis/storage.k8s.io/v1/storageclasses/local-path":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": "local-path",
				},
				"allowVolumeExpansion": false,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
		namespace:   namespace,
	}
	objects := []map[string]any{
		{
			"apiVersion": runtime.CloudNativePGAPIVersion,
			"kind":       runtime.CloudNativePGClusterKind,
			"metadata": map[string]any{
				"name":      clusterName,
				"namespace": namespace,
			},
			"spec": map[string]any{
				"instances": 1,
				"storage": map[string]any{
					"size": "5Gi",
				},
			},
		},
	}

	svc := &Service{}
	stabilized, err := svc.stabilizeManagedPostgresStorageSpecs(context.Background(), client, namespace, objects)
	if err != nil {
		t.Fatalf("stabilize managed postgres storage specs: %v", err)
	}
	if !stabilized {
		t.Fatal("expected managed postgres storage spec to be stabilized")
	}

	spec := objects[0]["spec"].(map[string]any)
	storage := spec["storage"].(map[string]any)
	if got := storage["size"]; got != "1Gi" {
		t.Fatalf("expected desired storage size to preserve live PVC capacity, got %#v", got)
	}
}

func TestStabilizeManagedPostgresStorageSpecsDoesNotShrinkRecordedClusterSpec(t *testing.T) {
	t.Parallel()

	const namespace = "tenant-demo"
	const clusterName = "demo-postgres"
	const pvcName = "demo-postgres-1"

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected request method %s for %s", r.Method, r.URL.String())
		}
		switch r.URL.Path {
		case "/apis/postgresql.cnpg.io/v1/namespaces/" + namespace + "/clusters/" + clusterName:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name":      clusterName,
					"namespace": namespace,
				},
				"spec": map[string]any{
					"storage": map[string]string{
						"size": "5Gi",
					},
				},
			})
		case "/api/v1/namespaces/" + namespace + "/persistentvolumeclaims":
			if got := r.URL.Query().Get("labelSelector"); got != "cnpg.io/cluster="+clusterName+",cnpg.io/pvcRole=PG_DATA" {
				t.Fatalf("unexpected pvc label selector %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name": pvcName,
						},
					},
				},
			})
		case "/api/v1/namespaces/" + namespace + "/persistentvolumeclaims/" + pvcName:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": pvcName,
				},
				"spec": map[string]any{
					"storageClassName": "local-path",
					"resources": map[string]any{
						"requests": map[string]string{
							"storage": "1Gi",
						},
					},
				},
				"status": map[string]any{
					"capacity": map[string]string{
						"storage": "1Gi",
					},
				},
			})
		case "/apis/storage.k8s.io/v1/storageclasses/local-path":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": "local-path",
				},
				"allowVolumeExpansion": false,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
		namespace:   namespace,
	}
	objects := []map[string]any{
		{
			"apiVersion": runtime.CloudNativePGAPIVersion,
			"kind":       runtime.CloudNativePGClusterKind,
			"metadata": map[string]any{
				"name":      clusterName,
				"namespace": namespace,
			},
			"spec": map[string]any{
				"instances": 1,
				"storage": map[string]any{
					"size": "5Gi",
				},
			},
		},
	}

	svc := &Service{}
	stabilized, err := svc.stabilizeManagedPostgresStorageSpecs(context.Background(), client, namespace, objects)
	if err != nil {
		t.Fatalf("stabilize managed postgres storage specs: %v", err)
	}
	if stabilized {
		t.Fatal("expected recorded cluster storage spec to remain unchanged")
	}

	spec := objects[0]["spec"].(map[string]any)
	storage := spec["storage"].(map[string]any)
	if got := storage["size"]; got != "5Gi" {
		t.Fatalf("expected desired storage size to avoid CNPG shrink, got %#v", got)
	}
}

func TestPrepareManagedPostgresInPlaceStorageExpansionForDesiredObjectsPatchesPVC(t *testing.T) {
	t.Parallel()

	const namespace = "tenant-demo"
	const clusterName = "demo-postgres"
	const pvcName = "demo-postgres-1"

	var patchedStorage string
	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/storage.k8s.io/v1/storageclasses/fugue-postgres-rwo":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata":             map[string]any{"name": "fugue-postgres-rwo"},
				"allowVolumeExpansion": true,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims":
			if got := r.URL.Query().Get("labelSelector"); got != "cnpg.io/cluster="+clusterName+",cnpg.io/pvcRole=PG_DATA" {
				t.Fatalf("unexpected pvc label selector %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{{
					"metadata": map[string]any{"name": pvcName},
				}},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims/"+pvcName:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": pvcName},
				"spec": map[string]any{
					"storageClassName": "fugue-postgres-rwo",
					"resources": map[string]any{
						"requests": map[string]any{"storage": "10Gi"},
					},
				},
				"status": map[string]any{
					"capacity": map[string]any{"storage": "10Gi"},
				},
			})
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims/"+pvcName:
			var body struct {
				Spec struct {
					Resources struct {
						Requests map[string]string `json:"requests"`
					} `json:"resources"`
				} `json:"spec"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode pvc patch: %v", err)
			}
			patchedStorage = body.Spec.Resources.Requests["storage"]
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": pvcName},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
		namespace:   namespace,
	}
	objects := []map[string]any{
		{
			"apiVersion": runtime.CloudNativePGAPIVersion,
			"kind":       runtime.CloudNativePGClusterKind,
			"metadata": map[string]any{
				"name":      clusterName,
				"namespace": namespace,
			},
			"spec": map[string]any{
				"storage": map[string]any{
					"size":         "20Gi",
					"storageClass": "fugue-postgres-rwo",
				},
			},
		},
	}

	svc := &Service{Logger: log.New(io.Discard, "", 0)}
	if err := svc.prepareManagedPostgresInPlaceStorageExpansionForDesiredObjects(context.Background(), client, namespace, objects); err != nil {
		t.Fatalf("prepare storage expansion for desired objects: %v", err)
	}
	if patchedStorage != "20Gi" {
		t.Fatalf("expected pvc storage patch to 20Gi, got %q", patchedStorage)
	}
}

func TestLogManagedPostgresStorageMigrationRequiredDedupe(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	svc := &Service{
		Logger: log.New(&out, "", 0),
	}

	svc.logManagedPostgresStorageMigrationRequired("tenant-demo", "demo-postgres", "demo-postgres-1", "5Gi", "1Gi")
	svc.logManagedPostgresStorageMigrationRequired("tenant-demo", "demo-postgres", "demo-postgres-1", "5Gi", "1Gi")

	if got := strings.Count(out.String(), "explicit data migration is required"); got != 1 {
		t.Fatalf("expected one migration-required log, got %d: %s", got, out.String())
	}
}

func TestManagedPostgresMissingClusterShouldBlockOnlyWhenServiceHasRuntimeHistory(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC)
	app := model.App{ID: "app_demo"}
	service := model.BackingService{
		ID:         "svc_pg",
		OwnerAppID: "app_demo",
		Type:       model.BackingServiceTypePostgres,
		Status:     model.BackingServiceStatusActive,
		Spec: model.BackingServiceSpec{
			Postgres: &model.AppPostgresSpec{Database: "app", User: "app"},
		},
		CreatedAt: now.Add(-time.Hour),
	}

	if managedPostgresMissingClusterShouldBlock(app, service, runtime.ManagedBackingServiceStatus{}, now) {
		t.Fatal("expected old undeployed service without runtime evidence to remain eligible for first initdb")
	}

	app.Status.Phase = "deployed"
	if !managedPostgresMissingClusterShouldBlock(app, service, runtime.ManagedBackingServiceStatus{}, now) {
		t.Fatal("expected deployed app with old postgres service to block missing cluster initdb")
	}

	freshService := service
	freshService.CreatedAt = now.Add(-time.Minute)
	if managedPostgresMissingClusterShouldBlock(app, freshService, runtime.ManagedBackingServiceStatus{}, now) {
		t.Fatal("expected fresh service inside init grace period not to block")
	}

	if !managedPostgresMissingClusterShouldBlock(model.App{ID: "app_demo"}, freshService, runtime.ManagedBackingServiceStatus{CurrentRuntimeReadyAt: now.Format(time.RFC3339)}, now) {
		t.Fatal("expected managed status runtime evidence to block even inside grace period")
	}

	foreignService := service
	foreignService.OwnerAppID = "app_other"
	if managedPostgresMissingClusterShouldBlock(app, foreignService, runtime.ManagedBackingServiceStatus{}, now) {
		t.Fatal("expected non-owned service not to block this app")
	}
}

func TestManagedPostgresStatefulObjectDetectors(t *testing.T) {
	t.Parallel()

	cluster := kubeCloudNativePGCluster{}
	cluster.Metadata.Labels = map[string]string{
		runtime.FugueLabelBackingServiceID: "svc_pg",
	}
	if !managedPostgresClusterLooksStateful(cluster) {
		t.Fatal("expected backing-service labeled CNPG cluster to look stateful")
	}

	pvc := kubePersistentVolumeClaim{}
	pvc.Metadata.Labels = map[string]string{
		"cnpg.io/cluster": "demo-postgres",
	}
	if !persistentVolumeClaimLooksLikeManagedPostgresData(pvc) {
		t.Fatal("expected CNPG PVC to look like managed postgres data")
	}
}

func TestBuildManagedAppStatusKeepsCurrentReleaseDuringRollout(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	currentStartedAt := time.Date(2026, time.March, 26, 9, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	currentReadyAt := time.Date(2026, time.March, 26, 9, 2, 0, 0, time.UTC).Format(time.RFC3339Nano)
	nextReleaseKey := runtime.ManagedAppReleaseKey(app, runtime.SchedulingConstraints{})
	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 2,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
		Status: runtime.ManagedAppStatus{
			CurrentReleaseKey:       "release_previous",
			CurrentReleaseStartedAt: currentStartedAt,
			CurrentReleaseReadyAt:   currentReadyAt,
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1

	status := buildManagedAppStatus(managed, app, deployment, true, nil, nil)

	if status.CurrentReleaseKey != "release_previous" {
		t.Fatalf("expected current release key to stay on previous release, got %q", status.CurrentReleaseKey)
	}
	if status.CurrentReleaseStartedAt != currentStartedAt {
		t.Fatalf("expected current release started at to stay %q, got %q", currentStartedAt, status.CurrentReleaseStartedAt)
	}
	if status.CurrentReleaseReadyAt != currentReadyAt {
		t.Fatalf("expected current release ready at to stay %q, got %q", currentReadyAt, status.CurrentReleaseReadyAt)
	}
	if status.PendingReleaseKey != nextReleaseKey {
		t.Fatalf("expected pending release key %q, got %q", nextReleaseKey, status.PendingReleaseKey)
	}
	if status.PendingReleaseStartedAt == "" {
		t.Fatal("expected pending release started at to be set")
	}
}

func TestBuildManagedAppStatusPromotesPendingReleaseWhenReady(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	pendingStartedAt := time.Date(2026, time.March, 26, 10, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	nextReleaseKey := runtime.ManagedAppReleaseKey(app, runtime.SchedulingConstraints{})
	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 2,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
		Status: runtime.ManagedAppStatus{
			CurrentReleaseKey:       "release_previous",
			CurrentReleaseStartedAt: time.Date(2026, time.March, 26, 9, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
			CurrentReleaseReadyAt:   time.Date(2026, time.March, 26, 9, 2, 0, 0, time.UTC).Format(time.RFC3339Nano),
			PendingReleaseKey:       nextReleaseKey,
			PendingReleaseStartedAt: pendingStartedAt,
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1

	status := buildManagedAppStatus(managed, app, deployment, true, nil, nil)

	if status.CurrentReleaseKey != nextReleaseKey {
		t.Fatalf("expected current release key %q, got %q", nextReleaseKey, status.CurrentReleaseKey)
	}
	if status.CurrentReleaseStartedAt != pendingStartedAt {
		t.Fatalf("expected promoted release started at %q, got %q", pendingStartedAt, status.CurrentReleaseStartedAt)
	}
	if status.CurrentReleaseReadyAt == "" {
		t.Fatal("expected promoted release ready at to be set")
	}
	if status.PendingReleaseKey != "" || status.PendingReleaseStartedAt != "" {
		t.Fatalf("expected pending release to be cleared, got key=%q started_at=%q", status.PendingReleaseKey, status.PendingReleaseStartedAt)
	}
}

func TestBuildManagedAppStatusWaitsForOldReplicasToTerminate(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 3,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 3
	deployment.Status.ObservedGeneration = 3
	deployment.Status.Replicas = 2
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1

	status := buildManagedAppStatus(managed, app, deployment, true, nil, nil)

	if status.Phase != runtime.ManagedAppPhaseProgressing {
		t.Fatalf("expected phase progressing while old replicas exist, got %q", status.Phase)
	}
	if !strings.Contains(status.Message, "old replicas to terminate") {
		t.Fatalf("expected old replica wait message, got %q", status.Message)
	}
}

func TestBuildManagedAppStatusMarksCrashLoopingPodsAsError(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 2,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1

	pods := []kubePod{
		{
			Metadata: struct {
				Name              string            `json:"name"`
				CreationTimestamp time.Time         `json:"creationTimestamp"`
				DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
				Labels            map[string]string `json:"labels,omitempty"`
				Annotations       map[string]string `json:"annotations,omitempty"`
			}{
				Name:              "demo-abc123",
				CreationTimestamp: time.Date(2026, time.March, 26, 10, 0, 0, 0, time.UTC),
			},
			Spec: kubePodSpec{
				NodeName: "gcp1",
			},
			Status: struct {
				Phase                 string                `json:"phase"`
				Reason                string                `json:"reason,omitempty"`
				Message               string                `json:"message,omitempty"`
				Conditions            []kubePodCondition    `json:"conditions,omitempty"`
				InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
				ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
			}{
				Phase: "Running",
				ContainerStatuses: []kubeContainerStatus{
					{
						Name: "demo",
						State: kubeRuntimeState{
							Waiting: &kubeStateDetail{
								Reason:  "CrashLoopBackOff",
								Message: "back-off restarting failed container",
							},
						},
						LastState: kubeRuntimeState{
							Terminated: &kubeStateDetail{
								Reason:   "OOMKilled",
								ExitCode: 137,
							},
						},
					},
				},
			},
		},
	}

	status := buildManagedAppStatus(managed, app, deployment, true, pods, nil)

	if status.Phase != runtime.ManagedAppPhaseError {
		t.Fatalf("expected phase error, got %q", status.Phase)
	}
	if !strings.Contains(status.Message, "OOMKilled") {
		t.Fatalf("expected OOMKilled in message, got %q", status.Message)
	}
	if !strings.Contains(status.Message, "demo-abc123") {
		t.Fatalf("expected pod name in message, got %q", status.Message)
	}
}

func TestBuildManagedAppStatusIgnoresRecoveredContainerLastFailure(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 2,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1

	pods := []kubePod{
		{
			Metadata: struct {
				Name              string            `json:"name"`
				CreationTimestamp time.Time         `json:"creationTimestamp"`
				DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
				Labels            map[string]string `json:"labels,omitempty"`
				Annotations       map[string]string `json:"annotations,omitempty"`
			}{
				Name:              "demo-abc123",
				CreationTimestamp: time.Date(2026, time.March, 26, 10, 0, 0, 0, time.UTC),
			},
			Spec: kubePodSpec{
				NodeName: "gcp1",
			},
			Status: struct {
				Phase                 string                `json:"phase"`
				Reason                string                `json:"reason,omitempty"`
				Message               string                `json:"message,omitempty"`
				Conditions            []kubePodCondition    `json:"conditions,omitempty"`
				InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
				ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
			}{
				Phase: "Running",
				ContainerStatuses: []kubeContainerStatus{
					{
						Name:  "demo",
						Ready: true,
						LastState: kubeRuntimeState{
							Terminated: &kubeStateDetail{
								Reason:   "Error",
								ExitCode: 3,
							},
						},
					},
				},
			},
		},
	}

	status := buildManagedAppStatus(managed, app, deployment, true, pods, nil)

	if status.Phase != runtime.ManagedAppPhaseReady {
		t.Fatalf("expected phase ready, got %q: %s", status.Phase, status.Message)
	}
	if strings.Contains(status.Message, "exit_code=3") {
		t.Fatalf("expected recovered last failure to be ignored, got %q", status.Message)
	}
}

func TestBuildManagedAppStatusPrefersPodFailureOverDeploymentCondition(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 2,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.Conditions = []runtime.ManagedAppCondition{
		{
			Type:    "Progressing",
			Status:  "False",
			Reason:  "ProgressDeadlineExceeded",
			Message: "ReplicaSet \"demo-abc123\" has timed out progressing.",
		},
	}

	pods := []kubePod{
		{
			Metadata: struct {
				Name              string            `json:"name"`
				CreationTimestamp time.Time         `json:"creationTimestamp"`
				DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
				Labels            map[string]string `json:"labels,omitempty"`
				Annotations       map[string]string `json:"annotations,omitempty"`
			}{
				Name:              "demo-abc123",
				CreationTimestamp: time.Date(2026, time.March, 26, 10, 0, 0, 0, time.UTC),
			},
			Spec: kubePodSpec{
				NodeName: "gcp1",
			},
			Status: struct {
				Phase                 string                `json:"phase"`
				Reason                string                `json:"reason,omitempty"`
				Message               string                `json:"message,omitempty"`
				Conditions            []kubePodCondition    `json:"conditions,omitempty"`
				InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
				ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
			}{
				Phase: "Running",
				ContainerStatuses: []kubeContainerStatus{
					{
						Name: "demo",
						State: kubeRuntimeState{
							Waiting: &kubeStateDetail{
								Reason:  "CrashLoopBackOff",
								Message: "back-off restarting failed container",
							},
						},
						LastState: kubeRuntimeState{
							Terminated: &kubeStateDetail{
								Reason:   "Error",
								ExitCode: 1,
							},
						},
					},
				},
			},
		},
	}

	status := buildManagedAppStatus(managed, app, deployment, true, pods, nil)

	if status.Phase != runtime.ManagedAppPhaseError {
		t.Fatalf("expected phase error, got %q", status.Phase)
	}
	if !strings.Contains(status.Message, "demo-abc123") {
		t.Fatalf("expected pod failure in message, got %q", status.Message)
	}
	if strings.Contains(status.Message, "ProgressDeadlineExceeded") {
		t.Fatalf("expected pod failure to override deployment condition, got %q", status.Message)
	}
}

func TestBuildManagedAppStatusIgnoresSIGTERMAndTerminatingPods(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 2,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 2
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1

	pods := []kubePod{
		{
			Metadata: struct {
				Name              string            `json:"name"`
				CreationTimestamp time.Time         `json:"creationTimestamp"`
				DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
				Labels            map[string]string `json:"labels,omitempty"`
				Annotations       map[string]string `json:"annotations,omitempty"`
			}{
				Name:              "demo-old",
				CreationTimestamp: time.Date(2026, time.March, 26, 9, 0, 0, 0, time.UTC),
				DeletionTimestamp: time.Date(2026, time.March, 26, 9, 10, 0, 0, time.UTC).Format(time.RFC3339Nano),
			},
			Status: struct {
				Phase                 string                `json:"phase"`
				Reason                string                `json:"reason,omitempty"`
				Message               string                `json:"message,omitempty"`
				Conditions            []kubePodCondition    `json:"conditions,omitempty"`
				InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
				ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
			}{
				Phase: "Failed",
				ContainerStatuses: []kubeContainerStatus{
					{
						Name: "demo",
						State: kubeRuntimeState{
							Terminated: &kubeStateDetail{
								Reason:   "Error",
								ExitCode: 143,
							},
						},
					},
				},
			},
		},
		{
			Metadata: struct {
				Name              string            `json:"name"`
				CreationTimestamp time.Time         `json:"creationTimestamp"`
				DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
				Labels            map[string]string `json:"labels,omitempty"`
				Annotations       map[string]string `json:"annotations,omitempty"`
			}{
				Name:              "demo-new",
				CreationTimestamp: time.Date(2026, time.March, 26, 9, 11, 0, 0, time.UTC),
			},
			Status: struct {
				Phase                 string                `json:"phase"`
				Reason                string                `json:"reason,omitempty"`
				Message               string                `json:"message,omitempty"`
				Conditions            []kubePodCondition    `json:"conditions,omitempty"`
				InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
				ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
			}{
				Phase: "Running",
				ContainerStatuses: []kubeContainerStatus{
					{
						Name: "demo",
						LastState: kubeRuntimeState{
							Terminated: &kubeStateDetail{
								Reason:   "Error",
								ExitCode: 143,
							},
						},
					},
				},
			},
		},
	}

	status := buildManagedAppStatus(managed, app, deployment, true, pods, nil)

	if status.Phase != runtime.ManagedAppPhaseProgressing {
		t.Fatalf("expected phase progressing, got %q message=%q", status.Phase, status.Message)
	}
	if strings.Contains(status.Message, "exit_code=143") {
		t.Fatalf("expected SIGTERM to stay out of rollout message, got %q", status.Message)
	}
}

func TestBuildManagedAppStatusIgnoresPodFailuresFromPreviousRelease(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 2,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
		Status: runtime.ManagedAppStatus{
			CurrentReleaseKey:       "release_previous",
			CurrentReleaseStartedAt: time.Date(2026, time.March, 26, 9, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1

	pods := []kubePod{
		{
			Metadata: struct {
				Name              string            `json:"name"`
				CreationTimestamp time.Time         `json:"creationTimestamp"`
				DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
				Labels            map[string]string `json:"labels,omitempty"`
				Annotations       map[string]string `json:"annotations,omitempty"`
			}{
				Name:              "demo-old",
				CreationTimestamp: time.Date(2026, time.March, 26, 9, 1, 0, 0, time.UTC),
			},
			Status: struct {
				Phase                 string                `json:"phase"`
				Reason                string                `json:"reason,omitempty"`
				Message               string                `json:"message,omitempty"`
				Conditions            []kubePodCondition    `json:"conditions,omitempty"`
				InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
				ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
			}{
				Phase: "Running",
				ContainerStatuses: []kubeContainerStatus{
					{
						Name: "demo",
						State: kubeRuntimeState{
							Waiting: &kubeStateDetail{
								Reason: "CrashLoopBackOff",
							},
						},
					},
				},
			},
		},
	}

	status := buildManagedAppStatus(managed, app, deployment, true, pods, nil)

	if status.Phase != runtime.ManagedAppPhaseProgressing {
		t.Fatalf("expected phase progressing, got %q", status.Phase)
	}
	if status.PendingReleaseKey != runtime.ManagedAppReleaseKey(app, runtime.SchedulingConstraints{}) {
		t.Fatalf("expected pending release key to be set for the new rollout, got %q", status.PendingReleaseKey)
	}
}

func TestBuildManagedAppStatusOnlyConsidersPodFailuresAfterPendingReleaseStart(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	releaseKey := runtime.ManagedAppReleaseKey(app, runtime.SchedulingConstraints{})
	pendingStartedAt := time.Date(2026, time.March, 26, 10, 0, 0, 0, time.UTC)
	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 2,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
		Status: runtime.ManagedAppStatus{
			PendingReleaseKey:       releaseKey,
			PendingReleaseStartedAt: pendingStartedAt.Format(time.RFC3339Nano),
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1

	pods := []kubePod{
		{
			Metadata: struct {
				Name              string            `json:"name"`
				CreationTimestamp time.Time         `json:"creationTimestamp"`
				DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
				Labels            map[string]string `json:"labels,omitempty"`
				Annotations       map[string]string `json:"annotations,omitempty"`
			}{
				Name:              "demo-old",
				CreationTimestamp: pendingStartedAt.Add(-time.Minute),
			},
			Status: struct {
				Phase                 string                `json:"phase"`
				Reason                string                `json:"reason,omitempty"`
				Message               string                `json:"message,omitempty"`
				Conditions            []kubePodCondition    `json:"conditions,omitempty"`
				InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
				ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
			}{
				Phase: "Running",
				ContainerStatuses: []kubeContainerStatus{
					{
						Name: "demo",
						State: kubeRuntimeState{
							Waiting: &kubeStateDetail{
								Reason: "CrashLoopBackOff",
							},
						},
					},
				},
			},
		},
		{
			Metadata: struct {
				Name              string            `json:"name"`
				CreationTimestamp time.Time         `json:"creationTimestamp"`
				DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
				Labels            map[string]string `json:"labels,omitempty"`
				Annotations       map[string]string `json:"annotations,omitempty"`
			}{
				Name:              "demo-new",
				CreationTimestamp: pendingStartedAt.Add(time.Minute),
			},
			Status: struct {
				Phase                 string                `json:"phase"`
				Reason                string                `json:"reason,omitempty"`
				Message               string                `json:"message,omitempty"`
				Conditions            []kubePodCondition    `json:"conditions,omitempty"`
				InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
				ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
			}{
				Phase: "Running",
				ContainerStatuses: []kubeContainerStatus{
					{
						Name: "demo",
						State: kubeRuntimeState{
							Waiting: &kubeStateDetail{
								Reason: "CrashLoopBackOff",
							},
						},
					},
				},
			},
		},
	}

	status := buildManagedAppStatus(managed, app, deployment, true, pods, nil)

	if status.Phase != runtime.ManagedAppPhaseError {
		t.Fatalf("expected phase error, got %q", status.Phase)
	}
	if !strings.Contains(status.Message, "demo-new") {
		t.Fatalf("expected only the new pod failure to be reported, got %q", status.Message)
	}
	if strings.Contains(status.Message, "demo-old") {
		t.Fatalf("expected old pod failure to be ignored, got %q", status.Message)
	}
}

func TestBuildManagedAppStatusIgnoresLegacyPodFailuresBeforeLastAppliedTime(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	lastAppliedAt := time.Date(2026, time.March, 26, 10, 0, 0, 0, time.UTC)
	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 2,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
		Status: runtime.ManagedAppStatus{
			LastAppliedTime: lastAppliedAt.Format(time.RFC3339Nano),
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1

	pods := []kubePod{
		{
			Metadata: struct {
				Name              string            `json:"name"`
				CreationTimestamp time.Time         `json:"creationTimestamp"`
				DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
				Labels            map[string]string `json:"labels,omitempty"`
				Annotations       map[string]string `json:"annotations,omitempty"`
			}{
				Name:              "demo-old-failed",
				CreationTimestamp: lastAppliedAt.Add(-time.Minute),
			},
			Status: struct {
				Phase                 string                `json:"phase"`
				Reason                string                `json:"reason,omitempty"`
				Message               string                `json:"message,omitempty"`
				Conditions            []kubePodCondition    `json:"conditions,omitempty"`
				InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
				ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
			}{
				Phase: "Running",
				ContainerStatuses: []kubeContainerStatus{
					{
						Name: "demo",
						State: kubeRuntimeState{
							Waiting: &kubeStateDetail{
								Reason: "CrashLoopBackOff",
							},
						},
					},
				},
			},
		},
	}

	status := buildManagedAppStatus(managed, app, deployment, true, pods, nil)

	if status.Phase != runtime.ManagedAppPhaseProgressing {
		t.Fatalf("expected phase progressing for legacy status, got %q message=%q", status.Phase, status.Message)
	}
	if strings.Contains(status.Message, "demo-old-failed") {
		t.Fatalf("expected old failed pod to be ignored for legacy status, got %q", status.Message)
	}
	if status.PendingReleaseKey != runtime.ManagedAppReleaseKey(app, runtime.SchedulingConstraints{}) {
		t.Fatalf("expected pending release key to be set, got %q", status.PendingReleaseKey)
	}
}

func TestBuildManagedAppStatusRefreshesPendingCutoffForSameReleaseRetry(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	releaseKey := runtime.ManagedAppReleaseKey(app, runtime.SchedulingConstraints{})
	oldPendingStartedAt := time.Date(2026, time.March, 26, 10, 0, 0, 0, time.UTC)
	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 3,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
		Status: runtime.ManagedAppStatus{
			ObservedGeneration:      2,
			PendingReleaseKey:       releaseKey,
			PendingReleaseStartedAt: oldPendingStartedAt.Format(time.RFC3339Nano),
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 3
	deployment.Status.ObservedGeneration = 3
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1

	pods := []kubePod{
		{
			Metadata: struct {
				Name              string            `json:"name"`
				CreationTimestamp time.Time         `json:"creationTimestamp"`
				DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
				Labels            map[string]string `json:"labels,omitempty"`
				Annotations       map[string]string `json:"annotations,omitempty"`
			}{
				Name:              "demo-old-failed",
				CreationTimestamp: oldPendingStartedAt.Add(time.Minute),
			},
			Status: struct {
				Phase                 string                `json:"phase"`
				Reason                string                `json:"reason,omitempty"`
				Message               string                `json:"message,omitempty"`
				Conditions            []kubePodCondition    `json:"conditions,omitempty"`
				InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
				ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
			}{
				Phase: "Running",
				ContainerStatuses: []kubeContainerStatus{
					{
						Name: "demo",
						State: kubeRuntimeState{
							Waiting: &kubeStateDetail{
								Reason: "CrashLoopBackOff",
							},
						},
					},
				},
			},
		},
	}

	status := buildManagedAppStatus(managed, app, deployment, true, pods, nil)

	if status.Phase != runtime.ManagedAppPhaseProgressing {
		t.Fatalf("expected phase progressing for a fresh retry, got %q message=%q", status.Phase, status.Message)
	}
	if strings.Contains(status.Message, "demo-old-failed") {
		t.Fatalf("expected old failed pod to be ignored for the fresh retry, got %q", status.Message)
	}
	if status.PendingReleaseKey != releaseKey {
		t.Fatalf("expected pending release key %q, got %q", releaseKey, status.PendingReleaseKey)
	}
	if status.PendingReleaseStartedAt == "" || status.PendingReleaseStartedAt == oldPendingStartedAt.Format(time.RFC3339Nano) {
		t.Fatalf("expected pending release start to refresh, got %q", status.PendingReleaseStartedAt)
	}
}

func TestBuildManagedAppStatusKeepsContainerCreatingPodsAsProgressing(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 2,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1

	pods := []kubePod{
		{
			Status: struct {
				Phase                 string                `json:"phase"`
				Reason                string                `json:"reason,omitempty"`
				Message               string                `json:"message,omitempty"`
				Conditions            []kubePodCondition    `json:"conditions,omitempty"`
				InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
				ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
			}{
				Phase: "Pending",
				ContainerStatuses: []kubeContainerStatus{
					{
						Name: "demo",
						State: kubeRuntimeState{
							Waiting: &kubeStateDetail{
								Reason: "ContainerCreating",
							},
						},
					},
				},
			},
		},
	}

	status := buildManagedAppStatus(managed, app, deployment, true, pods, nil)

	if status.Phase != runtime.ManagedAppPhaseProgressing {
		t.Fatalf("expected phase progressing, got %q", status.Phase)
	}
	if !strings.Contains(status.Message, "ready replicas 0/1") {
		t.Fatalf("expected rollout progress message, got %q", status.Message)
	}
}

func TestBuildManagedAppStatusKeepsUnschedulablePodsProgressing(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 2,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1

	pods := []kubePod{
		{
			Metadata: struct {
				Name              string            `json:"name"`
				CreationTimestamp time.Time         `json:"creationTimestamp"`
				DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
				Labels            map[string]string `json:"labels,omitempty"`
				Annotations       map[string]string `json:"annotations,omitempty"`
			}{
				Name:              "demo-abc123",
				CreationTimestamp: time.Date(2026, time.March, 26, 10, 0, 0, 0, time.UTC),
			},
			Status: struct {
				Phase                 string                `json:"phase"`
				Reason                string                `json:"reason,omitempty"`
				Message               string                `json:"message,omitempty"`
				Conditions            []kubePodCondition    `json:"conditions,omitempty"`
				InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
				ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
			}{
				Phase: "Pending",
				Conditions: []kubePodCondition{
					{
						Type:    "PodScheduled",
						Status:  "False",
						Reason:  "Unschedulable",
						Message: "0/4 nodes are available: 1 node(s) had volume node affinity conflict, 1 node(s) had untolerated taint {node.kubernetes.io/disk-pressure: }. preemption: 0/4 nodes are available: 4 Preemption is not helpful for scheduling.",
					},
				},
			},
		},
	}

	status := buildManagedAppStatus(managed, app, deployment, true, pods, nil)

	if status.Phase != runtime.ManagedAppPhaseProgressing {
		t.Fatalf("expected phase progressing, got %q message=%q", status.Phase, status.Message)
	}
	if !strings.Contains(status.Message, "Unschedulable") || !strings.Contains(status.Message, "volume node affinity conflict") {
		t.Fatalf("expected scheduling block to stay visible in progressing status message, got %q", status.Message)
	}
}

func TestBuildManagedBackingServiceStatusTracksCurrentRuntime(t *testing.T) {
	startedAt := time.Date(2026, time.March, 26, 11, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	readyAt := time.Date(2026, time.March, 26, 11, 1, 0, 0, time.UTC).Format(time.RFC3339Nano)
	previous := runtime.ManagedAppStatus{
		BackingServices: []runtime.ManagedBackingServiceStatus{
			{
				ServiceID:               "service_demo",
				RuntimeKey:              "runtime_same",
				CurrentRuntimeStartedAt: startedAt,
				CurrentRuntimeReadyAt:   readyAt,
			},
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 1
	deployment.Status.ObservedGeneration = 1
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1

	status := buildManagedBackingServiceStatus(previous, runtime.ManagedBackingServiceDeployment{
		ServiceID:    "service_demo",
		ResourceName: "demo-postgres",
		RuntimeKey:   "runtime_same",
	}, deployment, true)

	if status.CurrentRuntimeStartedAt != startedAt {
		t.Fatalf("expected service runtime started at %q, got %q", startedAt, status.CurrentRuntimeStartedAt)
	}
	if status.CurrentRuntimeReadyAt != readyAt {
		t.Fatalf("expected service runtime ready at %q, got %q", readyAt, status.CurrentRuntimeReadyAt)
	}
}

func TestDeleteManagedAppResourcesDeletesExpectedNamesWhenLabelsAreMissing(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "uni-api-web-api",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/uni-api:v1",
			Ports:     []int{8000},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Postgres: &model.AppPostgresSpec{
				Database:    "uniapi",
				User:        "uniapi",
				Password:    "secret",
				ServiceName: "uni-api-web-api-db-postgres",
			},
		},
	}

	var deleted []string
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet:
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"items":[]}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodDelete:
			deleted = append(deleted, req.Method+" "+req.URL.Path)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
			return nil, nil
		}
	})

	client := &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
		namespace:   "fugue-system",
	}

	svc := &Service{}
	if err := svc.deleteManagedAppResources(context.Background(), client, runtime.NamespaceForTenant(app.TenantID), app); err != nil {
		t.Fatalf("delete managed app resources: %v", err)
	}

	want := []string{
		"DELETE /api/v1/namespaces/fg-tenant-demo/services/app-demo",
		"DELETE /api/v1/namespaces/fg-tenant-demo/services/uni-api-web-api-db-postgres",
		"DELETE /api/v1/namespaces/fg-tenant-demo/secrets/uni-api-web-api-pgsec",
		"DELETE /apis/apps/v1/namespaces/fg-tenant-demo/deployments/app-demo",
		"DELETE /apis/postgresql.cnpg.io/v1/namespaces/fg-tenant-demo/clusters/uni-api-web-api-db-postgres",
	}
	sort.Strings(deleted)
	sort.Strings(want)
	if len(deleted) != len(want) {
		t.Fatalf("expected delete requests %v, got %v", want, deleted)
	}
	for i := range want {
		if deleted[i] != want[i] {
			t.Fatalf("expected delete request %q, got %q", want[i], deleted[i])
		}
	}
}

func TestBackfillManagedAppSourceUsesStoreSourceForLegacyManagedApps(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID: "app_demo",
	}
	stored := model.App{
		Source: &model.AppSource{
			Type:             model.AppSourceTypeDockerImage,
			ImageRef:         "mongo:7.0",
			ComposeService:   "mongodb",
			ComposeDependsOn: []string{"api"},
		},
	}

	backfillManagedAppSource(&app, stored)

	if app.Source == nil {
		t.Fatal("expected store source to backfill legacy managed app")
	}
	if app.Source.ComposeService != "mongodb" {
		t.Fatalf("expected compose service mongodb, got %q", app.Source.ComposeService)
	}

	stored.Source.ComposeDependsOn[0] = "changed"
	if got := app.Source.ComposeDependsOn[0]; got != "api" {
		t.Fatalf("expected copied compose dependencies to stay unchanged, got %q", got)
	}
}

func TestSelectManagedAppDesiredAppPrefersManagedSnapshotWhenStoredBaselineNeedsRecovery(t *testing.T) {
	t.Parallel()

	managedSnapshot := model.App{
		Spec: model.AppSpec{
			Image: "registry.pull.example/fugue-apps/argus-runtime:upload-abcdef123456",
		},
	}
	stored := model.App{
		Spec: model.AppSpec{},
		Source: &model.AppSource{
			Type:             model.AppSourceTypeUpload,
			UploadID:         "upload_demo",
			ResolvedImageRef: "registry.push.example/fugue-apps/argus-runtime:upload-abcdef123456",
		},
	}

	got, usedStoredBaseline := selectManagedAppDesiredApp(managedSnapshot, stored, false)
	if usedStoredBaseline {
		t.Fatal("expected managed snapshot to win when stored app image is missing")
	}
	if got.Spec.Image != managedSnapshot.Spec.Image {
		t.Fatalf("expected managed snapshot image %q, got %q", managedSnapshot.Spec.Image, got.Spec.Image)
	}
	if got.Source == nil || got.Source.ResolvedImageRef != stored.Source.ResolvedImageRef {
		t.Fatalf("expected store source to backfill managed snapshot, got %+v", got.Source)
	}
}

func TestSelectManagedAppDesiredAppRefreshesStoredBackingServicesDuringBaselineRecovery(t *testing.T) {
	t.Parallel()

	managedSnapshot := model.App{
		Spec: model.AppSpec{
			Image: "registry.pull.example/fugue-apps/demo:live",
		},
		BackingServices: []model.BackingService{
			{
				ID:   "service_demo",
				Name: "demo-postgres",
				Type: model.BackingServiceTypePostgres,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						RuntimeID:               "runtime_source",
						FailoverTargetRuntimeID: "runtime_target",
						Instances:               2,
						SynchronousReplicas:     1,
					},
				},
			},
		},
	}
	stored := model.App{
		Spec: model.AppSpec{},
		Source: &model.AppSource{
			Type:             model.AppSourceTypeUpload,
			UploadID:         "upload_demo",
			ResolvedImageRef: "registry.push.example/fugue-apps/demo:live",
		},
		BackingServices: []model.BackingService{
			{
				ID:   "service_demo",
				Name: "demo-postgres",
				Type: model.BackingServiceTypePostgres,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						RuntimeID:       "runtime_target",
						PrimaryNodeName: "node-target",
						Instances:       1,
					},
				},
			},
		},
		Bindings: []model.ServiceBinding{
			{
				ID:        "binding_demo",
				AppID:     "app_demo",
				ServiceID: "service_demo",
				Env: map[string]string{
					"DB_HOST": "demo-postgres-rw",
				},
			},
		},
	}

	got, usedStoredBaseline := selectManagedAppDesiredApp(managedSnapshot, stored, false)
	if usedStoredBaseline {
		t.Fatal("expected managed snapshot runtime baseline to win when stored app image is missing")
	}
	if got.Spec.Image != managedSnapshot.Spec.Image {
		t.Fatalf("expected managed snapshot image %q, got %q", managedSnapshot.Spec.Image, got.Spec.Image)
	}
	if len(got.BackingServices) != 1 || got.BackingServices[0].Spec.Postgres == nil {
		t.Fatalf("expected stored backing service to be copied, got %+v", got.BackingServices)
	}
	postgres := got.BackingServices[0].Spec.Postgres
	if postgres.RuntimeID != "runtime_target" || postgres.FailoverTargetRuntimeID != "" || postgres.Instances != 1 || postgres.PrimaryNodeName != "node-target" {
		t.Fatalf("expected stored finalized postgres spec, got %+v", postgres)
	}
	if len(got.Bindings) != 1 || got.Bindings[0].Env["DB_HOST"] != "demo-postgres-rw" {
		t.Fatalf("expected stored service bindings to be copied, got %+v", got.Bindings)
	}

	stored.BackingServices[0].Spec.Postgres.RuntimeID = "changed"
	stored.Bindings[0].Env["DB_HOST"] = "changed"
	if got.BackingServices[0].Spec.Postgres.RuntimeID != "runtime_target" {
		t.Fatalf("expected backing service copy to be isolated, got %+v", got.BackingServices[0].Spec.Postgres)
	}
	if got.Bindings[0].Env["DB_HOST"] != "demo-postgres-rw" {
		t.Fatalf("expected binding copy to be isolated, got %+v", got.Bindings[0].Env)
	}
}

func TestSelectManagedAppDesiredAppKeepsManagedSnapshotBackingServicesDuringActiveOperation(t *testing.T) {
	t.Parallel()

	managedSnapshot := model.App{
		Spec: model.AppSpec{
			Image: "registry.pull.example/fugue-apps/demo:live",
		},
		BackingServices: []model.BackingService{
			{
				ID:   "service_demo",
				Name: "demo-postgres",
				Type: model.BackingServiceTypePostgres,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						RuntimeID:               "runtime_source",
						FailoverTargetRuntimeID: "runtime_target",
						Instances:               2,
						SynchronousReplicas:     1,
					},
				},
			},
		},
	}
	stored := model.App{
		Spec: model.AppSpec{},
		Source: &model.AppSource{
			Type:             model.AppSourceTypeUpload,
			UploadID:         "upload_demo",
			ResolvedImageRef: "registry.push.example/fugue-apps/demo:live",
		},
		BackingServices: []model.BackingService{
			{
				ID:   "service_demo",
				Name: "demo-postgres",
				Type: model.BackingServiceTypePostgres,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						RuntimeID:       "runtime_target",
						PrimaryNodeName: "node-target",
						Instances:       1,
					},
				},
			},
		},
	}

	got, usedStoredBaseline := selectManagedAppDesiredApp(managedSnapshot, stored, true)
	if usedStoredBaseline {
		t.Fatal("expected active operation to keep managed snapshot")
	}
	if len(got.BackingServices) != 1 || got.BackingServices[0].Spec.Postgres == nil {
		t.Fatalf("expected managed snapshot backing service, got %+v", got.BackingServices)
	}
	postgres := got.BackingServices[0].Spec.Postgres
	if postgres.RuntimeID != "runtime_source" || postgres.FailoverTargetRuntimeID != "runtime_target" || postgres.Instances != 2 {
		t.Fatalf("expected active operation postgres spec to be preserved, got %+v", postgres)
	}
}

func TestSelectManagedAppDesiredAppUsesStoredBaselineWhenRecoveryIsNotNeeded(t *testing.T) {
	t.Parallel()

	managedSnapshot := model.App{
		Spec: model.AppSpec{
			Image: "registry.pull.example/fugue-apps/demo:old",
		},
	}
	stored := model.App{
		Spec: model.AppSpec{
			Image: "registry.pull.example/fugue-apps/demo:new",
		},
		Source: &model.AppSource{
			Type:             model.AppSourceTypeUpload,
			UploadID:         "upload_demo",
			ArchiveSHA256:    "archive_demo",
			ResolvedImageRef: "registry.push.example/fugue-apps/demo:new",
		},
	}

	got, usedStoredBaseline := selectManagedAppDesiredApp(managedSnapshot, stored, false)
	if !usedStoredBaseline {
		t.Fatal("expected stored app baseline to win when it is complete")
	}
	if got.Spec.Image != stored.Spec.Image {
		t.Fatalf("expected stored image %q, got %q", stored.Spec.Image, got.Spec.Image)
	}
}

func TestReconcileManagedAppObjectSkipsApplyWhileOperationIsActive(t *testing.T) {
	t.Parallel()

	stateStore := store.New(t.TempDir() + "/store.json")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Active Operation")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Project A", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.pull.example/fugue-apps/demo:git-old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         "upload_demo",
		ArchiveSHA256:    "sha256-old",
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		CommitSHA:        "sha256-old",
		ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-old",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	desiredSpec := app.Spec
	desiredSpec.Image = "registry.pull.example/fugue-apps/demo:git-new"
	if _, err := stateStore.CreateOperation(model.Operation{
		TenantID:    app.TenantID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &desiredSpec,
	}); err != nil {
		t.Fatalf("create active deploy operation: %v", err)
	}

	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("build managed app: %v", err)
	}

	requests := 0
	client := &kubeClient{
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests++
			t.Fatalf("expected active operation reconcile to skip kubernetes writes, got %s %s", req.Method, req.URL.String())
			return nil, nil
		})},
		baseURL: "http://kube.test",
	}
	svc := &Service{
		Store: stateStore,
	}
	if err := svc.reconcileManagedAppObject(context.Background(), client, managed); err != nil {
		t.Fatalf("reconcile managed app: %v", err)
	}
	if requests != 0 {
		t.Fatalf("expected no kubernetes requests while active operation owns apply, got %d", requests)
	}
}

func TestManagedAppBaselineUsesNormalizedBuildSource(t *testing.T) {
	t.Parallel()

	app := model.App{
		Spec: model.AppSpec{
			Image:    "registry.pull.example/fugue-apps/demo:git-current",
			Replicas: 1,
		},
		BuildSource: &model.AppSource{
			Type:             model.AppSourceTypeUpload,
			UploadID:         "upload_current",
			ArchiveSHA256:    "sha256-current",
			ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-current",
		},
	}

	if managedAppBaselineNeedsRecovery(app) {
		t.Fatal("expected normalized build source to satisfy managed app baseline")
	}
}

func TestSelectManagedAppDesiredAppPreservesCurrentOnlineRolloutSnapshot(t *testing.T) {
	t.Parallel()

	stored := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Source: &model.AppSource{
			Type:     model.AppSourceTypeDockerImage,
			ImageRef: "ghcr.io/example/demo:latest",
		},
		Spec: model.AppSpec{
			Image:                         "registry.fugue.internal:5000/fugue-apps/demo@sha256:abc",
			Ports:                         []int{8080},
			Replicas:                      1,
			RuntimeID:                     "runtime_demo",
			TerminationGracePeriodSeconds: 2100,
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
				},
			},
		},
	}
	managedSnapshot := stored
	managedSnapshot.Spec.RolloutIntent = model.AppRolloutIntentOnlineLifecycleUpdate

	selected, useStored := selectManagedAppDesiredApp(managedSnapshot, stored, false)
	if useStored {
		t.Fatal("expected current online rollout snapshot to keep driving reconcile")
	}
	if got := selected.Spec.RolloutIntent; got != model.AppRolloutIntentOnlineLifecycleUpdate {
		t.Fatalf("expected rollout intent to be preserved, got %q", got)
	}

	changedStored := stored
	changedStored.Spec.Image = "registry.fugue.internal:5000/fugue-apps/demo@sha256:def"
	selected, useStored = selectManagedAppDesiredApp(managedSnapshot, changedStored, false)
	if !useStored {
		t.Fatal("expected stored app to win after a real desired-state change")
	}
	if got := selected.Spec.Image; got != changedStored.Spec.Image {
		t.Fatalf("expected changed stored image %q, got %q", changedStored.Spec.Image, got)
	}
}

func TestSelectManagedAppDesiredAppPreservesCurrentOnlineResourceRolloutSnapshot(t *testing.T) {
	t.Parallel()

	stored := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Source: &model.AppSource{
			Type:     model.AppSourceTypeDockerImage,
			ImageRef: "ghcr.io/example/demo:latest",
		},
		Spec: model.AppSpec{
			Image:     "registry.fugue.internal:5000/fugue-apps/demo@sha256:abc",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Resources: &model.ResourceSpec{
				CPUMilliCores:   670,
				MemoryMebibytes: 512,
			},
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
				},
			},
		},
	}
	managedSnapshot := stored
	managedSnapshot.Spec.RolloutIntent = model.AppRolloutIntentOnlineResourceUpdate

	selected, useStored := selectManagedAppDesiredApp(managedSnapshot, stored, false)
	if useStored {
		t.Fatal("expected current online resource rollout snapshot to keep driving reconcile")
	}
	if got := selected.Spec.RolloutIntent; got != model.AppRolloutIntentOnlineResourceUpdate {
		t.Fatalf("expected resource rollout intent to be preserved, got %q", got)
	}

	changedStored := stored
	changedStored.Spec.Image = "registry.fugue.internal:5000/fugue-apps/demo@sha256:def"
	selected, useStored = selectManagedAppDesiredApp(managedSnapshot, changedStored, false)
	if !useStored {
		t.Fatal("expected stored app to win after a non-resource desired-state change")
	}
	if got := selected.Spec.Image; got != changedStored.Spec.Image {
		t.Fatalf("expected changed stored image %q, got %q", changedStored.Spec.Image, got)
	}
}

func TestSelectManagedAppDesiredAppPreservesCurrentOnlineImageRolloutSnapshot(t *testing.T) {
	t.Parallel()

	stored := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Source: &model.AppSource{
			Type:             model.AppSourceTypeDockerImage,
			ImageRef:         "ghcr.io/example/demo:latest",
			ResolvedImageRef: "registry.push.example/demo:image-new",
		},
		Spec: model.AppSpec{
			Image:        "registry.fugue.internal:5000/fugue-apps/demo@sha256:new",
			Ports:        []int{8080},
			Replicas:     1,
			RuntimeID:    "runtime_demo",
			RestartToken: "restart_new",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindFile, Path: "/home/api.yaml", SeedContent: "providers: []\n"},
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/home/data"},
				},
			},
		},
	}
	managedSnapshot := stored
	managedSnapshot.Spec.RolloutIntent = model.AppRolloutIntentOnlineImageUpdate

	selected, useStored := selectManagedAppDesiredApp(managedSnapshot, stored, false)
	if useStored {
		t.Fatal("expected current online image rollout snapshot to keep driving reconcile")
	}
	if got := selected.Spec.RolloutIntent; got != model.AppRolloutIntentOnlineImageUpdate {
		t.Fatalf("expected image rollout intent to be preserved, got %q", got)
	}

	changedStored := stored
	changedStored.Spec.Image = "registry.fugue.internal:5000/fugue-apps/demo@sha256:other"
	selected, useStored = selectManagedAppDesiredApp(managedSnapshot, changedStored, false)
	if !useStored {
		t.Fatal("expected stored app to win after a newer image")
	}
	if got := selected.Spec.Image; got != changedStored.Spec.Image {
		t.Fatalf("expected changed stored image %q, got %q", changedStored.Spec.Image, got)
	}
}

func TestSelectManagedAppDesiredAppPreservesStatelessOnlineImageRolloutSnapshot(t *testing.T) {
	t.Parallel()

	stored := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Source: &model.AppSource{
			Type:             model.AppSourceTypeDockerImage,
			ImageRef:         "ghcr.io/example/demo:latest",
			ResolvedImageRef: "registry.push.example/demo:image-new",
		},
		Spec: model.AppSpec{
			Image:     "registry.fugue.internal:5000/fugue-apps/demo@sha256:new",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}
	managedSnapshot := stored
	managedSnapshot.Spec.RolloutIntent = model.AppRolloutIntentOnlineImageUpdate

	selected, useStored := selectManagedAppDesiredApp(managedSnapshot, stored, false)
	if useStored {
		t.Fatal("expected stateless online image rollout snapshot to keep driving reconcile")
	}
	if got := selected.Spec.RolloutIntent; got != model.AppRolloutIntentOnlineImageUpdate {
		t.Fatalf("expected image rollout intent to be preserved, got %q", got)
	}

	changedStored := stored
	changedStored.Spec.Image = "registry.fugue.internal:5000/fugue-apps/demo@sha256:other"
	selected, useStored = selectManagedAppDesiredApp(managedSnapshot, changedStored, false)
	if !useStored {
		t.Fatal("expected stored app to win after a newer stateless image")
	}
	if got := selected.Spec.Image; got != changedStored.Spec.Image {
		t.Fatalf("expected changed stored image %q, got %q", changedStored.Spec.Image, got)
	}
}

func TestRefreshStoredManagedAppDesiredBeforeApplyUsesLatestOnlineRolloutSnapshot(t *testing.T) {
	t.Parallel()

	stateStore := store.New(t.TempDir() + "/store.json")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Online Resource Rollout")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Project A", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.fugue.internal:5000/fugue-apps/demo@sha256:abc",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
		Resources: &model.ResourceSpec{
			CPUMilliCores:   680,
			MemoryMebibytes: 768,
		},
		PersistentStorage: &model.AppPersistentStorageSpec{
			Mode:             model.AppPersistentStorageModeMovableRWO,
			StorageClassName: "fugue-local-rwo",
			Mounts: []model.AppPersistentStorageMount{
				{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
			},
		},
	}, model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         "ghcr.io/example/demo:latest",
		ResolvedImageRef: "registry.push.example/fugue-apps/demo@sha256:abc",
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	staleManaged, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{
		NodeSelector: map[string]string{
			runtime.SharedPoolLabelKey: runtime.SharedPoolLabelValue,
		},
	}))
	if err != nil {
		t.Fatalf("build stale managed app: %v", err)
	}
	latestApp := app
	latestApp.Spec.RolloutIntent = model.AppRolloutIntentOnlineResourceUpdate
	latestManaged, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(latestApp, runtime.SchedulingConstraints{
		NodeSelector: map[string]string{
			runtime.SharedPoolLabelKey: runtime.SharedPoolLabelValue,
			kubeHostnameLabelKey:       "node-a",
		},
	}))
	if err != nil {
		t.Fatalf("build latest managed app: %v", err)
	}
	latestManagedJSON, err := json.Marshal(latestManaged)
	if err != nil {
		t.Fatalf("marshal latest managed app: %v", err)
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	managedName := runtime.ManagedAppResourceName(app)
	gotLatestManaged := false
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet && req.URL.Path == managedAppAPIPath(namespace, managedName) {
			gotLatestManaged = true
			return okJSONResponse(string(latestManagedJSON)), nil
		}
		t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
		return nil, nil
	})

	svc := &Service{Store: stateStore}
	client := &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
		namespace:   namespace,
	}
	refreshedApp, refreshedManaged, skipApply, err := svc.refreshStoredManagedAppDesiredBeforeApply(
		context.Background(),
		client,
		namespace,
		staleManaged,
		runtime.AppFromManagedApp(staleManaged),
	)
	if err != nil {
		t.Fatalf("refresh stored desired before apply: %v", err)
	}
	if skipApply {
		t.Fatal("expected reconcile to continue after active operation completed")
	}
	if !gotLatestManaged {
		t.Fatal("expected latest managed app snapshot to be fetched")
	}
	if got := refreshedApp.Spec.RolloutIntent; got != model.AppRolloutIntentOnlineResourceUpdate {
		t.Fatalf("expected online resource rollout intent to be preserved, got %q", got)
	}
	if got := refreshedManaged.Spec.RolloutIntent; got != model.AppRolloutIntentOnlineResourceUpdate {
		t.Fatalf("expected managed rollout intent to be refreshed, got %q", got)
	}
	if got := refreshedManaged.Spec.Scheduling.NodeSelector[kubeHostnameLabelKey]; got != "node-a" {
		t.Fatalf("expected latest hostname pin to be preserved, got %q", got)
	}
}

func TestSelectManagedAppDesiredAppPreservesCurrentOnlineRestartRolloutSnapshot(t *testing.T) {
	t.Parallel()

	stored := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Source: &model.AppSource{
			Type:     model.AppSourceTypeDockerImage,
			ImageRef: "ghcr.io/example/demo:latest",
		},
		Spec: model.AppSpec{
			Image:        "registry.fugue.internal:5000/fugue-apps/demo@sha256:abc",
			Ports:        []int{8080},
			Replicas:     1,
			RuntimeID:    "runtime_demo",
			RestartToken: "restart_new",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindFile, Path: "/home/api.yaml", SeedContent: "providers: []\n"},
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/home/data"},
				},
			},
		},
	}
	managedSnapshot := stored
	managedSnapshot.Spec.RolloutIntent = model.AppRolloutIntentOnlineRestart

	selected, useStored := selectManagedAppDesiredApp(managedSnapshot, stored, false)
	if useStored {
		t.Fatal("expected current online restart rollout snapshot to keep driving reconcile")
	}
	if got := selected.Spec.RolloutIntent; got != model.AppRolloutIntentOnlineRestart {
		t.Fatalf("expected restart rollout intent to be preserved, got %q", got)
	}

	changedStored := stored
	changedStored.Spec.RestartToken = "restart_other"
	selected, useStored = selectManagedAppDesiredApp(managedSnapshot, changedStored, false)
	if !useStored {
		t.Fatal("expected stored app to win after a newer restart token")
	}
	if got := selected.Spec.RestartToken; got != changedStored.Spec.RestartToken {
		t.Fatalf("expected changed stored restart token %q, got %q", changedStored.Spec.RestartToken, got)
	}
}

func TestStoredManagedAppDesiredWithRolloutIntentInfersConfigUpdate(t *testing.T) {
	t.Parallel()

	managedSnapshot := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Image:        "registry.fugue.internal:5000/fugue-apps/demo@sha256:abc",
			Ports:        []int{8080},
			Replicas:     1,
			RuntimeID:    "runtime_demo",
			RestartToken: "restart_old",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindFile, Path: "/home/api.yaml", SeedContent: "providers: []\n", Mode: 0o644},
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/home/data"},
				},
			},
		},
	}
	storedDesired := managedSnapshot
	persistent := *managedSnapshot.Spec.PersistentStorage
	persistent.Mounts = append([]model.AppPersistentStorageMount(nil), managedSnapshot.Spec.PersistentStorage.Mounts...)
	storedDesired.Spec.PersistentStorage = &persistent
	storedDesired.Spec.RestartToken = "restart_new"
	storedDesired.Spec.PersistentStorage.Mounts[0].SeedContent = "providers:\n- openai\n"

	got := storedManagedAppDesiredWithRolloutIntent(managedSnapshot, storedDesired)
	if got.Spec.RolloutIntent != model.AppRolloutIntentOnlineConfigUpdate {
		t.Fatalf("expected online config rollout intent, got %q", got.Spec.RolloutIntent)
	}
	objects := runtime.BuildManagedAppChildObjects(got, runtime.SchedulingConstraints{}, runtime.ManagedAppOwnerReference(runtime.ManagedAppObject{}))
	deployment := controllerTestFirstObjectByKind(t, objects, "Deployment")
	spec, _ := deployment["spec"].(map[string]any)
	strategy, _ := spec["strategy"].(map[string]any)
	if gotStrategy := strategy["type"]; gotStrategy != "RollingUpdate" {
		t.Fatalf("expected config update to use RollingUpdate, got %#v", gotStrategy)
	}
	metadata, _ := deployment["metadata"].(map[string]any)
	annotations, _ := metadata["annotations"].(map[string]string)
	if gotClass := annotations["fugue.io/downtime-class"]; gotClass != "online-required" {
		t.Fatalf("expected online downtime class, got %q", gotClass)
	}
	if gotReason := annotations["fugue.io/rollout-reason"]; gotReason != "config-file-only" {
		t.Fatalf("expected config-file-only rollout reason, got %q", gotReason)
	}
}

func TestStoredManagedAppDesiredWithRolloutIntentKeepsStorageStructureChangeRecreate(t *testing.T) {
	t.Parallel()

	managedSnapshot := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Image:     "registry.fugue.internal:5000/fugue-apps/demo@sha256:abc",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindFile, Path: "/home/api.yaml", SeedContent: "providers: []\n", Mode: 0o644},
				},
			},
		},
	}
	storedDesired := managedSnapshot
	persistent := *managedSnapshot.Spec.PersistentStorage
	persistent.Mounts = append([]model.AppPersistentStorageMount(nil), managedSnapshot.Spec.PersistentStorage.Mounts...)
	storedDesired.Spec.PersistentStorage = &persistent
	storedDesired.Spec.PersistentStorage.Mounts[0].Path = "/home/config/api.yaml"
	storedDesired.Spec.PersistentStorage.Mounts[0].SeedContent = "providers:\n- openai\n"

	got := storedManagedAppDesiredWithRolloutIntent(managedSnapshot, storedDesired)
	if got.Spec.RolloutIntent != "" {
		t.Fatalf("expected no inferred rollout intent for storage structure change, got %q", got.Spec.RolloutIntent)
	}
	objects := runtime.BuildManagedAppChildObjects(got, runtime.SchedulingConstraints{}, runtime.ManagedAppOwnerReference(runtime.ManagedAppObject{}))
	deployment := controllerTestFirstObjectByKind(t, objects, "Deployment")
	spec, _ := deployment["spec"].(map[string]any)
	strategy, _ := spec["strategy"].(map[string]any)
	if gotStrategy := strategy["type"]; gotStrategy != "Recreate" {
		t.Fatalf("expected storage structure change to use Recreate, got %#v", gotStrategy)
	}
}

func TestSelectManagedAppDesiredAppPreservesCurrentOnlineRolloutSnapshotDespiteSourceDrift(t *testing.T) {
	t.Parallel()

	stored := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Source: &model.AppSource{
			Type:             model.AppSourceTypeDockerImage,
			ImageRef:         "ghcr.io/example/demo:latest",
			ResolvedImageRef: "ghcr.io/example/demo@sha256:abc",
		},
		Spec: model.AppSpec{
			Image:     "registry.fugue.internal:5000/fugue-apps/demo@sha256:abc",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Env: map[string]string{
				"CONFIG_URL":       "http://file_url/api.yaml",
				"FUGUE_PROJECT_ID": "project_demo",
				"FUGUE_TOKEN":      "runtime-token",
			},
			TerminationGracePeriodSeconds: 2101,
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
				},
			},
		},
	}
	managedSnapshot := stored
	managedSnapshot.Spec.RolloutIntent = model.AppRolloutIntentOnlineLifecycleUpdate
	managedSnapshot.Source = &model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: "ghcr.io/example/demo:latest",
	}
	managedSnapshot.Spec.Env = map[string]string{
		"CONFIG_URL": "http://file_url/api.yaml",
	}

	selected, useStored := selectManagedAppDesiredApp(managedSnapshot, stored, false)
	if useStored {
		t.Fatal("expected source and injected env drift to keep the current online rollout snapshot")
	}
	if got := selected.Spec.RolloutIntent; got != model.AppRolloutIntentOnlineLifecycleUpdate {
		t.Fatalf("expected rollout intent to be preserved, got %q", got)
	}
}

func TestReconcileManagedAppObjectRefreshesStoredDesiredStateBeforeApply(t *testing.T) {
	t.Parallel()

	stateStore := store.New(t.TempDir() + "/store.json")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Refresh Desired")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Project A", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	latestSource := model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         "upload_latest",
		ArchiveSHA256:    "sha256-latest",
		ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-latest",
	}
	latestApp, err := stateStore.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.pull.example/fugue-apps/demo:git-latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, latestSource)
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	staleApp := latestApp
	staleApp.Spec.Image = "registry.pull.example/fugue-apps/demo:git-stale"
	model.SetAppSourceState(&staleApp, &model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         "upload_stale",
		ArchiveSHA256:    "sha256-stale",
		ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-stale",
	}, &model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         "upload_stale",
		ArchiveSHA256:    "sha256-stale",
		ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-stale",
	})
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(staleApp, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("build stale managed app: %v", err)
	}

	namespace := runtime.NamespaceForTenant(latestApp.TenantID)
	managedName := runtime.ManagedAppResourceName(latestApp)
	deploymentName := runtime.RuntimeAppResourceName(latestApp)
	var recordedManagedApp map[string]any
	var recordedDeployment map[string]any

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPatch && strings.HasSuffix(req.URL.Path, "/status"):
			return okJSONResponse(`{}`), nil
		case req.Method == http.MethodPatch &&
			req.URL.Path == managedAppAPIPath(namespace, managedName) &&
			strings.HasPrefix(req.Header.Get("Content-Type"), "application/json-patch+json"):
			return okJSONResponse(`{}`), nil
		case req.Method == http.MethodPatch &&
			req.URL.Path == "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+deploymentName &&
			strings.HasPrefix(req.Header.Get("Content-Type"), "application/json-patch+json"):
			return okJSONResponse(`{}`), nil
		case req.Method == http.MethodPatch:
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatalf("decode apply object %s: %v", req.URL.Path, err)
			}
			switch strings.TrimSpace(body["kind"].(string)) {
			case runtime.ManagedAppKind:
				recordedManagedApp = body
			case "Deployment":
				recordedDeployment = body
			}
			return okJSONResponse(`{}`), nil
		case req.Method == http.MethodGet && req.URL.Path == managedAppAPIPath(namespace, managedName):
			if recordedManagedApp == nil {
				data, err := json.Marshal(managed)
				if err != nil {
					t.Fatalf("marshal stale managed app: %v", err)
				}
				return okJSONResponse(string(data)), nil
			}
			data, err := json.Marshal(recordedManagedApp)
			if err != nil {
				t.Fatalf("marshal recorded managed app: %v", err)
			}
			return okJSONResponse(string(data)), nil
		case req.Method == http.MethodGet && req.URL.Path == "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+deploymentName:
			return notFoundJSONResponse(), nil
		case req.Method == http.MethodGet && req.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/"+namespace+"/leases/"+managedName+"-fence":
			return notFoundJSONResponse(), nil
		case req.Method == http.MethodGet && req.URL.RawQuery != "":
			return okJSONResponse(`{"items":[]}`), nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
			return nil, nil
		}
	})

	svc := &Service{
		Store:    stateStore,
		Renderer: runtime.Renderer{},
	}
	client := &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
		namespace:   namespace,
	}

	if err := svc.reconcileManagedAppObject(context.Background(), client, managed); err != nil {
		t.Fatalf("reconcile managed app: %v", err)
	}
	if got := managedAppSpecImage(recordedManagedApp); got != latestApp.Spec.Image {
		t.Fatalf("expected managed app image %q, got %q", latestApp.Spec.Image, got)
	}
	if got := deploymentContainerImage(recordedDeployment); got != latestApp.Spec.Image {
		t.Fatalf("expected deployment image %q, got %q", latestApp.Spec.Image, got)
	}
}

func TestReconcileManagedAppObjectRepairsIncompleteStoredGitHubSourceFromReadyManagedSnapshot(t *testing.T) {
	t.Parallel()

	stateStore := store.New(t.TempDir() + "/store.json")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Runtime Recovery")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Project A", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.pull.example/fugue-apps/demo:git-old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyDockerfile,
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 8080,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Name:       runtime.ManagedAppResourceName(app),
			Namespace:  namespace,
			Generation: 1,
		},
		Spec: runtime.ManagedAppSpec{
			AppID:     app.ID,
			TenantID:  app.TenantID,
			ProjectID: app.ProjectID,
			Name:      app.Name,
			Source: &model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				RepoURL:          "https://github.com/example/demo",
				RepoBranch:       "main",
				BuildStrategy:    model.AppBuildStrategyDockerfile,
				CommitSHA:        "newcommit",
				ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-newcommit",
			},
			AppSpec: model.AppSpec{
				Image:     "registry.pull.example/fugue-apps/demo:git-newcommit",
				Ports:     []int{8080},
				Replicas:  1,
				RuntimeID: "runtime_managed_shared",
			},
			Scheduling: runtime.SchedulingConstraints{},
		},
	}

	deployment := kubeDeployment{}
	deployment.Metadata.Name = runtime.RuntimeAppResourceName(app)
	deployment.Metadata.Generation = 1
	deployment.Status.ObservedGeneration = 1
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPatch && strings.HasSuffix(req.URL.Path, "/status"):
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodPatch:
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet && req.URL.Path == "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+deployment.Metadata.Name:
			data, err := json.Marshal(deployment)
			if err != nil {
				t.Fatalf("marshal deployment: %v", err)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(string(data))),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/leases/"):
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"kind":"Status","status":"Failure","message":"not found","reason":"NotFound","code":404}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet && req.URL.RawQuery != "":
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"items":[]}`)),
				Header:     make(http.Header),
			}, nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
			return nil, nil
		}
	})

	client := &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
	}
	svc := &Service{
		Store: stateStore,
	}

	if err := svc.reconcileManagedAppObject(context.Background(), client, managed); err != nil {
		t.Fatalf("reconcile managed app: %v", err)
	}

	updated, err := stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get updated app: %v", err)
	}
	if updated.Source == nil {
		t.Fatal("expected source to be preserved after reconcile")
	}
	if got := updated.Source.CommitSHA; got != "newcommit" {
		t.Fatalf("expected recovered commit newcommit, got %q", got)
	}
	if got := updated.Source.ResolvedImageRef; got != "registry.push.example/fugue-apps/demo:git-newcommit" {
		t.Fatalf("expected recovered resolved image, got %q", got)
	}
	if got := updated.Spec.Image; got != "registry.pull.example/fugue-apps/demo:git-newcommit" {
		t.Fatalf("expected recovered runtime image, got %q", got)
	}
}

func TestReconcileManagedAppObjectScalesDownUnrecoverableFailedSnapshot(t *testing.T) {
	t.Parallel()

	stateStore := store.New(t.TempDir() + "/store.json")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Failed Snapshot")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Project A", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeUpload,
		UploadID:      "upload_demo",
		ArchiveSHA256: "sha256-demo",
		BuildStrategy: model.AppBuildStrategyDockerfile,
		CommitSHA:     "sha256-demo",
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	managedName := runtime.ManagedAppResourceName(app)
	deploymentName := runtime.RuntimeAppResourceName(app)
	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Name:       managedName,
			Namespace:  namespace,
			Generation: 1,
		},
		Spec: runtime.ManagedAppSpec{
			AppID:     app.ID,
			TenantID:  app.TenantID,
			ProjectID: app.ProjectID,
			Name:      app.Name,
			Source: &model.AppSource{
				Type:             model.AppSourceTypeUpload,
				UploadID:         "upload_demo",
				ArchiveSHA256:    "sha256-demo",
				BuildStrategy:    model.AppBuildStrategyDockerfile,
				CommitSHA:        "sha256-demo",
				ResolvedImageRef: "registry.push.example/fugue-apps/demo:upload-sha256",
			},
			AppSpec: model.AppSpec{
				Image:     "registry.pull.example/fugue-apps/demo:upload-sha256",
				Ports:     []int{8080},
				Replicas:  1,
				RuntimeID: "runtime_managed_shared",
			},
			Scheduling: runtime.SchedulingConstraints{},
		},
		Status: runtime.ManagedAppStatus{
			Phase:           runtime.ManagedAppPhaseError,
			Message:         "pod demo container demo failed: Error: exit_code=1",
			DesiredReplicas: 1,
			ReadyReplicas:   0,
		},
	}

	deployment := kubeDeployment{}
	deployment.Metadata.Name = deploymentName
	deployment.Metadata.Generation = 1
	deployment.Status.ObservedGeneration = 1
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.UnavailableReplicas = 1

	var recordedDisabledManagedApp map[string]any
	var scaledDeploymentReplicas *int
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == deploymentAPIPath(namespace, deploymentName):
			data, err := json.Marshal(deployment)
			if err != nil {
				t.Fatalf("marshal deployment: %v", err)
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(data))), Header: make(http.Header)}, nil
		case req.Method == http.MethodPatch && strings.HasSuffix(req.URL.Path, "/status"):
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
		case req.Method == http.MethodPatch && req.URL.Path == deploymentAPIPath(namespace, deploymentName):
			var body struct {
				Spec struct {
					Replicas int `json:"replicas"`
				} `json:"spec"`
			}
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatalf("decode deployment scale patch: %v", err)
			}
			scaledDeploymentReplicas = &body.Spec.Replicas
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
		case req.Method == http.MethodPatch:
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatalf("decode apply object %s: %v", req.URL.Path, err)
			}
			if kind, _ := body["kind"].(string); kind == runtime.ManagedAppKind {
				recordedDisabledManagedApp = body
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
			return nil, nil
		}
	})

	client := &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
	}
	svc := &Service{
		Store: stateStore,
	}

	if err := svc.reconcileManagedAppObject(context.Background(), client, managed); err != nil {
		t.Fatalf("reconcile managed app: %v", err)
	}
	if recordedDisabledManagedApp == nil {
		t.Fatal("expected unrecoverable managed app snapshot to be disabled")
	}
	spec, _ := recordedDisabledManagedApp["spec"].(map[string]any)
	appSpec, _ := spec["appSpec"].(map[string]any)
	if got := appSpec["replicas"]; got != float64(0) {
		t.Fatalf("expected managed app desired replicas to be 0, got %#v", got)
	}
	if scaledDeploymentReplicas == nil || *scaledDeploymentReplicas != 0 {
		t.Fatalf("expected deployment to be scaled to 0, got %#v", scaledDeploymentReplicas)
	}
}

func TestValidateManagedAppDeployableImageRejectsPositiveReplicasWithoutImage(t *testing.T) {
	t.Parallel()

	err := validateManagedAppDeployableImage(model.App{
		ID: "app_empty",
		Spec: model.AppSpec{
			Replicas: 1,
		},
	})
	if err == nil {
		t.Fatal("expected missing image with positive replicas to be rejected")
	}

	if err := validateManagedAppDeployableImage(model.App{
		ID: "app_disabled",
		Spec: model.AppSpec{
			Replicas: 0,
		},
	}); err != nil {
		t.Fatalf("expected disabled app without image to remain valid, got %v", err)
	}
}

func TestApplyManagedAppDesiredStateInjectsWorkloadIdentityOnlyIntoRuntimeObjects(t *testing.T) {
	t.Parallel()

	stateStore := store.New(t.TempDir() + "/store.json")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Workload Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Project A", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "gateway", "", model.AppSpec{
		Image:     "ghcr.io/example/gateway:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Env: map[string]string{
			"APP_ENV":    "prod",
			"FUGUE_ONLY": "user-defined",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	app.Route = &model.AppRoute{
		Hostname:  "gateway.example.com",
		PublicURL: "https://gateway.example.com",
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	managedName := runtime.ManagedAppResourceName(app)
	deploymentName := runtime.RuntimeAppResourceName(app)

	var (
		recordedManagedApp map[string]any
		recordedDeployment map[string]any
	)

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPatch && strings.HasSuffix(req.URL.Path, "/status"):
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodPatch &&
			req.URL.Path == managedAppAPIPath(namespace, managedName) &&
			strings.HasPrefix(req.Header.Get("Content-Type"), "application/json-patch+json"):
			var patch []map[string]any
			if err := json.NewDecoder(req.Body).Decode(&patch); err != nil {
				t.Fatalf("decode managed app spec patch: %v", err)
			}
			if len(patch) != 1 || patch[0]["op"] != "replace" || patch[0]["path"] != "/spec" {
				t.Fatalf("expected managed app spec replacement patch, got %#v", patch)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodPatch &&
			req.URL.Path == "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+deploymentName &&
			strings.HasPrefix(req.Header.Get("Content-Type"), "application/json-patch+json"):
			var patch []map[string]any
			if err := json.NewDecoder(req.Body).Decode(&patch); err != nil {
				t.Fatalf("decode deployment spec patch: %v", err)
			}
			if len(patch) != 1 || patch[0]["op"] != "replace" || patch[0]["path"] != "/spec" {
				t.Fatalf("expected deployment spec replacement patch, got %#v", patch)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodPatch:
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatalf("decode apply object %s: %v", req.URL.Path, err)
			}
			switch strings.TrimSpace(body["kind"].(string)) {
			case runtime.ManagedAppKind:
				recordedManagedApp = body
			case "Deployment":
				recordedDeployment = body
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet && req.URL.Path == managedAppAPIPath(namespace, managedName):
			if recordedManagedApp == nil {
				t.Fatalf("managed app was requested before apply")
			}
			data, err := json.Marshal(recordedManagedApp)
			if err != nil {
				t.Fatalf("marshal recorded managed app: %v", err)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(string(data))),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet && req.URL.Path == "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+deploymentName:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"kind":"Status","status":"Failure","message":"not found","reason":"NotFound","code":404}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet && req.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/"+namespace+"/leases/"+managedName+"-fence":
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"kind":"Status","status":"Failure","message":"not found","reason":"NotFound","code":404}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet && req.URL.RawQuery != "":
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"items":[]}`)),
				Header:     make(http.Header),
			}, nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
			return nil, nil
		}
	})

	svc := &Service{
		Store: stateStore,
		Renderer: runtime.Renderer{
			WorkloadIdentity: runtime.WorkloadIdentityConfig{
				APIBaseURL: "api.example.com",
				SigningKey: "signing-secret",
			},
			AppObservability: runtime.AppObservabilityConfig{
				Endpoint: "telemetry-agent.fugue-system.svc.cluster.local:7834",
			},
		},
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      &http.Client{Transport: transport},
				baseURL:     "http://kube.test",
				bearerToken: "token",
				namespace:   namespace,
			}, nil
		},
	}

	if err := svc.applyManagedAppDesiredState(context.Background(), app, runtime.SchedulingConstraints{}); err != nil {
		t.Fatalf("apply managed app desired state: %v", err)
	}
	if recordedManagedApp == nil {
		t.Fatal("expected managed app object to be applied")
	}
	if recordedDeployment == nil {
		t.Fatal("expected runtime deployment object to be applied")
	}

	managedEnv := managedAppSpecEnv(recordedManagedApp)
	if got := managedEnv["APP_ENV"]; got != "prod" {
		t.Fatalf("expected managed app user env APP_ENV=prod, got %q", got)
	}
	if got := managedEnv["FUGUE_ONLY"]; got != "user-defined" {
		t.Fatalf("expected managed app user env FUGUE_ONLY=user-defined, got %q", got)
	}
	for _, key := range []string{
		"FUGUE_PROJECT_ID",
		"FUGUE_RUNTIME_ID",
		"FUGUE_API_URL",
		"FUGUE_APP_URL",
		"FUGUE_TOKEN",
		"FUGUE_OBSERVABILITY_ENDPOINT",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"FUGUE_OBSERVABILITY_PROJECT_ID",
		"FUGUE_OBSERVABILITY_APP_ID",
		"FUGUE_OBSERVABILITY_RUNTIME_ID",
		"FUGUE_OBSERVABILITY_SERVICE_NAME",
	} {
		if got := managedEnv[key]; got != "" {
			t.Fatalf("expected managed app snapshot to omit injected %s, got %q", key, got)
		}
	}

	deploymentEnv := deploymentContainerEnv(recordedDeployment)
	if got := deploymentEnv["FUGUE_PROJECT_ID"]; got != project.ID {
		t.Fatalf("expected deployment FUGUE_PROJECT_ID %q, got %q", project.ID, got)
	}
	if got := deploymentEnv["FUGUE_RUNTIME_ID"]; got != app.Spec.RuntimeID {
		t.Fatalf("expected deployment FUGUE_RUNTIME_ID %q, got %q", app.Spec.RuntimeID, got)
	}
	if got := deploymentEnv["FUGUE_APP_URL"]; got != "https://gateway.example.com" {
		t.Fatalf("expected deployment FUGUE_APP_URL to be injected, got %q", got)
	}
	if got := deploymentEnv["FUGUE_OBSERVABILITY_ENDPOINT"]; got != "http://telemetry-agent.fugue-system.svc.cluster.local:7834" {
		t.Fatalf("expected deployment FUGUE_OBSERVABILITY_ENDPOINT to be injected, got %q", got)
	}
	if got := deploymentEnv["OTEL_EXPORTER_OTLP_ENDPOINT"]; got != "http://telemetry-agent.fugue-system.svc.cluster.local:7834" {
		t.Fatalf("expected deployment OTEL_EXPORTER_OTLP_ENDPOINT to be injected, got %q", got)
	}
	if got := deploymentEnv["FUGUE_OBSERVABILITY_PROJECT_ID"]; got != project.ID {
		t.Fatalf("expected deployment FUGUE_OBSERVABILITY_PROJECT_ID %q, got %q", project.ID, got)
	}
	if got := deploymentEnv["FUGUE_OBSERVABILITY_APP_ID"]; got != app.ID {
		t.Fatalf("expected deployment FUGUE_OBSERVABILITY_APP_ID %q, got %q", app.ID, got)
	}
	if got := deploymentEnv["FUGUE_OBSERVABILITY_RUNTIME_ID"]; got != app.Spec.RuntimeID {
		t.Fatalf("expected deployment FUGUE_OBSERVABILITY_RUNTIME_ID %q, got %q", app.Spec.RuntimeID, got)
	}
	if got := deploymentEnv["FUGUE_OBSERVABILITY_SERVICE_NAME"]; got != app.Name {
		t.Fatalf("expected deployment FUGUE_OBSERVABILITY_SERVICE_NAME %q, got %q", app.Name, got)
	}
	deploymentClaims, err := workloadidentity.Parse("signing-secret", deploymentEnv["FUGUE_TOKEN"])
	if err != nil {
		t.Fatalf("parse deployment workload token: %v", err)
	}
	if deploymentClaims.ProjectID != project.ID {
		t.Fatalf("expected deployment token project scope %q, got %q", project.ID, deploymentClaims.ProjectID)
	}
}

func TestApplyManagedAppDesiredStateOmitsDeploymentForDisabledAppWithoutImage(t *testing.T) {
	t.Parallel()

	stateStore := store.New(t.TempDir() + "/store.json")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Disabled Source Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Project A", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeUpload,
		UploadID:      "upload_demo",
		ArchiveSHA256: "sha256-demo",
		BuildStrategy: model.AppBuildStrategyDockerfile,
		CommitSHA:     "sha256-demo",
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}
	app.Spec.Replicas = 0

	namespace := runtime.NamespaceForTenant(app.TenantID)
	managedName := runtime.ManagedAppResourceName(app)
	deploymentName := runtime.RuntimeAppResourceName(app)

	var (
		recordedManagedApp map[string]any
		recordedStatus     map[string]any
		appliedDeployment  bool
		appliedService     bool
		deletedDeployment  bool
	)

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPatch && strings.HasSuffix(req.URL.Path, "/status"):
			if err := json.NewDecoder(req.Body).Decode(&recordedStatus); err != nil {
				t.Fatalf("decode managed app status: %v", err)
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
		case req.Method == http.MethodPatch &&
			req.URL.Path == managedAppAPIPath(namespace, managedName) &&
			strings.HasPrefix(req.Header.Get("Content-Type"), "application/json-patch+json"):
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
		case req.Method == http.MethodPatch:
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatalf("decode apply object %s: %v", req.URL.Path, err)
			}
			switch strings.TrimSpace(body["kind"].(string)) {
			case runtime.ManagedAppKind:
				recordedManagedApp = body
			case "Deployment":
				appliedDeployment = true
			case "Service":
				appliedService = true
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
		case req.Method == http.MethodGet && req.URL.Path == managedAppAPIPath(namespace, managedName):
			if recordedManagedApp == nil {
				t.Fatalf("managed app was requested before apply")
			}
			data, err := json.Marshal(recordedManagedApp)
			if err != nil {
				t.Fatalf("marshal recorded managed app: %v", err)
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(data))), Header: make(http.Header)}, nil
		case req.Method == http.MethodGet && req.URL.Path == deploymentAPIPath(namespace, deploymentName):
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"kind":"Status","status":"Failure","message":"not found","reason":"NotFound","code":404}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet && req.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/"+namespace+"/leases/"+managedName+"-fence":
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"kind":"Status","status":"Failure","message":"not found","reason":"NotFound","code":404}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet && req.URL.Path == "/apis/apps/v1/namespaces/"+namespace+"/deployments" && req.URL.RawQuery != "":
			items := `[]`
			if !deletedDeployment {
				items = `[{"metadata":{"name":"` + deploymentName + `"}}]`
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"items":` + items + `}`)), Header: make(http.Header)}, nil
		case req.Method == http.MethodDelete && req.URL.Path == deploymentAPIPath(namespace, deploymentName):
			deletedDeployment = true
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
		case req.Method == http.MethodGet && req.URL.RawQuery != "":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"items":[]}`)), Header: make(http.Header)}, nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
			return nil, nil
		}
	})

	svc := &Service{
		Store:    stateStore,
		Renderer: runtime.Renderer{},
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      &http.Client{Transport: transport},
				baseURL:     "http://kube.test",
				bearerToken: "token",
				namespace:   namespace,
			}, nil
		},
	}

	if err := svc.applyManagedAppDesiredState(context.Background(), app, runtime.SchedulingConstraints{}); err != nil {
		t.Fatalf("apply managed app desired state: %v", err)
	}
	if recordedManagedApp == nil {
		t.Fatal("expected managed app desired state to be applied")
	}
	if appliedDeployment {
		t.Fatal("disabled app without image must not apply a deployment")
	}
	if !appliedService {
		t.Fatal("expected service object to remain applied")
	}
	if !deletedDeployment {
		t.Fatal("expected stale deployment to be deleted")
	}
	statusSpec, _ := recordedStatus["status"].(map[string]any)
	if got := statusSpec["phase"]; got != runtime.ManagedAppPhaseDisabled {
		t.Fatalf("expected disabled status, got %#v", got)
	}
}

func TestApplyManagedAppDesiredStateUsesRequestedSchedulingWhenManagedAppReadIsStale(t *testing.T) {
	t.Parallel()

	stateStore := store.New(t.TempDir() + "/store.json")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Runtime Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Project A", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	targetRuntime, _, err := stateStore.CreateRuntime(tenant.ID, "shared-us", model.RuntimeTypeManagedShared, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	imageRef := "registry.fugue.internal:5000/fugue-apps/demo:git-current"
	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     imageRef,
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: targetRuntime.ID,
	}, model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         imageRef,
		ResolvedImageRef: imageRef,
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:        tenant.ID,
		AppID:           app.ID,
		ImageRef:        imageRef,
		CanonicalDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		LifecycleState:  model.ImageLifecycleLost,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	now := time.Now().UTC()
	if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        tenant.ID,
		AppID:           app.ID,
		NodeID:          "machine-target",
		RuntimeID:       "runtime_target_physical",
		ClusterNodeName: "worker-target",
		CacheEndpoint:   "http://worker-target.example.com:5000",
		Status:          model.ImageReplicaStatusStale,
		LastVerifiedAt:  &now,
	}); err != nil {
		t.Fatalf("upsert stale target replica: %v", err)
	}
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        tenant.ID,
		AppID:           app.ID,
		ImageRef:        imageRef,
		Digest:          image.CanonicalDigest,
		NodeID:          "machine-target",
		RuntimeID:       "runtime_target_physical",
		ClusterNodeName: "worker-target",
		CacheEndpoint:   "http://worker-target.example.com:5000",
		Status:          model.ImageLocationStatusPresent,
		LastSeenAt:      &now,
	}); err != nil {
		t.Fatalf("upsert target image location: %v", err)
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	managedName := runtime.ManagedAppResourceName(app)
	deploymentName := runtime.RuntimeAppResourceName(app)
	targetScheduling := runtime.SchedulingConstraints{
		NodeSelector: map[string]string{
			runtime.SharedPoolLabelKey: runtime.SharedPoolLabelValue,
			kubeHostnameLabelKey:       "worker-target",
		},
	}
	staleManagedApp := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{
		NodeSelector: map[string]string{
			runtime.SharedPoolLabelKey: runtime.SharedPoolLabelValue,
		},
	})

	var recordedDeployment map[string]any
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPatch && strings.HasSuffix(req.URL.Path, "/status"):
			return okJSONResponse(`{}`), nil
		case req.Method == http.MethodPatch &&
			req.URL.Path == managedAppAPIPath(namespace, managedName) &&
			strings.HasPrefix(req.Header.Get("Content-Type"), "application/json-patch+json"):
			return okJSONResponse(`{}`), nil
		case req.Method == http.MethodPatch &&
			req.URL.Path == "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+deploymentName &&
			strings.HasPrefix(req.Header.Get("Content-Type"), "application/json-patch+json"):
			return okJSONResponse(`{}`), nil
		case req.Method == http.MethodPatch:
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatalf("decode apply object %s: %v", req.URL.Path, err)
			}
			if strings.TrimSpace(body["kind"].(string)) == "Deployment" {
				recordedDeployment = body
			}
			return okJSONResponse(`{}`), nil
		case req.Method == http.MethodGet && req.URL.Path == managedAppAPIPath(namespace, managedName):
			data, err := json.Marshal(staleManagedApp)
			if err != nil {
				t.Fatalf("marshal stale managed app: %v", err)
			}
			return okJSONResponse(string(data)), nil
		case req.Method == http.MethodGet && req.URL.Path == "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+deploymentName:
			return notFoundJSONResponse(), nil
		case req.Method == http.MethodGet && req.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/"+namespace+"/leases/"+managedName+"-fence":
			return notFoundJSONResponse(), nil
		case req.Method == http.MethodGet && req.URL.RawQuery != "":
			return okJSONResponse(`{"items":[]}`), nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
			return nil, nil
		}
	})

	svc := &Service{
		Store:    stateStore,
		Config:   config.ControllerConfig{ImageStoreMode: "distributed", ImageStoreMinReplicas: 2, ImageStoreTargetReplicas: 2},
		Renderer: runtime.Renderer{},
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      &http.Client{Transport: transport},
				baseURL:     "http://kube.test",
				bearerToken: "token",
				namespace:   namespace,
			}, nil
		},
	}

	if err := svc.applyManagedAppDesiredState(context.Background(), app, targetScheduling); err != nil {
		t.Fatalf("apply managed app desired state: %v", err)
	}
	if recordedDeployment == nil {
		t.Fatal("expected runtime deployment object to be applied")
	}
	if got := deploymentTemplateNodeSelector(recordedDeployment); !stringMapsEqual(got, targetScheduling.NodeSelector) {
		t.Fatalf("expected deployment to use target runtime scheduling, got %#v", got)
	}
	refreshedImage, err := stateStore.GetImage(image.ID, tenant.ID, false)
	if err != nil {
		t.Fatalf("get refreshed image: %v", err)
	}
	if refreshedImage.LifecycleState != model.ImageLifecycleAvailable {
		t.Fatalf("expected target image location evidence to restore lost image, got %q", refreshedImage.LifecycleState)
	}
}

func TestBackfillManagedAppSourceDoesNotOverrideManagedSnapshot(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID: "app_demo",
		Source: &model.AppSource{
			Type:           model.AppSourceTypeDockerImage,
			ComposeService: "managed",
		},
	}
	stored := model.App{
		Source: &model.AppSource{
			Type:           model.AppSourceTypeDockerImage,
			ComposeService: "store",
		},
	}

	backfillManagedAppSource(&app, stored)

	if got := app.Source.ComposeService; got != "managed" {
		t.Fatalf("expected managed snapshot source to win, got %q", got)
	}
}

func TestSummarizeManagedAppPodFailureReportsServiceProcessExit(t *testing.T) {
	t.Parallel()

	pod := kubePod{}
	pod.Metadata.Name = "demo-exited"
	pod.Spec.NodeName = "worker-1"
	pod.Status.Phase = "Succeeded"
	pod.Status.InitContainerStatuses = []kubeContainerStatus{{
		Name: "init-persistent-storage",
		State: kubeRuntimeState{
			Terminated: &kubeStateDetail{Reason: "Completed", ExitCode: 0},
		},
	}}
	pod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name:  "demo",
		Ready: false,
		State: kubeRuntimeState{
			Terminated: &kubeStateDetail{Reason: "Completed", ExitCode: 0},
		},
	}}

	summary := summarizeManagedAppPodFailure(pod)
	if !strings.Contains(summary, "process exited successfully instead of staying online") {
		t.Fatalf("expected service process exit summary, got %q", summary)
	}
}

func TestManagedAppCloudNativePGApplyContextSkipsExistingForOnlineIntent(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:         "ghcr.io/example/demo:latest",
			Ports:         []int{8080},
			Replicas:      1,
			RolloutIntent: model.AppRolloutIntentOnlineResourceUpdate,
		},
	}

	ctx := managedAppCloudNativePGApplyContext(context.Background(), app, false)
	if !skipExistingCloudNativePGWrites(ctx) {
		t.Fatal("expected online rollout to skip existing CloudNativePG writes")
	}
	forcedCtx := managedAppCloudNativePGApplyContext(context.Background(), app, true)
	if skipExistingCloudNativePGWrites(forcedCtx) {
		t.Fatal("expected forced CloudNativePG write to override online rollout skip")
	}
	app.Spec.RolloutIntent = ""
	plainCtx := managedAppCloudNativePGApplyContext(context.Background(), app, false)
	if skipExistingCloudNativePGWrites(plainCtx) {
		t.Fatal("expected non-online rollout to allow CloudNativePG writes")
	}
}

func managedAppSpecEnv(obj map[string]any) map[string]string {
	spec, _ := obj["spec"].(map[string]any)
	appSpec, _ := spec["appSpec"].(map[string]any)
	return stringMapFromAnyMap(appSpec["env"])
}

func managedAppSpecImage(obj map[string]any) string {
	spec, _ := obj["spec"].(map[string]any)
	appSpec, _ := spec["appSpec"].(map[string]any)
	image, _ := appSpec["image"].(string)
	return strings.TrimSpace(image)
}

func deploymentContainerImage(obj map[string]any) string {
	spec, _ := obj["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	templateSpec, _ := template["spec"].(map[string]any)
	containers, _ := templateSpec["containers"].([]any)
	if len(containers) == 0 {
		return ""
	}
	container, _ := containers[0].(map[string]any)
	image, _ := container["image"].(string)
	return strings.TrimSpace(image)
}

func deploymentContainerEnv(obj map[string]any) map[string]string {
	spec, _ := obj["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	templateSpec, _ := template["spec"].(map[string]any)
	containers, _ := templateSpec["containers"].([]any)
	if len(containers) == 0 {
		return map[string]string{}
	}
	container, _ := containers[0].(map[string]any)
	envList, _ := container["env"].([]any)
	env := make(map[string]string, len(envList))
	for _, raw := range envList {
		item, _ := raw.(map[string]any)
		name, _ := item["name"].(string)
		value, _ := item["value"].(string)
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		env[name] = value
	}
	return env
}

func controllerTestFirstObjectByKind(t *testing.T, objects []map[string]any, kind string) map[string]any {
	t.Helper()
	for _, object := range objects {
		if got, _ := object["kind"].(string); got == kind {
			return object
		}
	}
	t.Fatalf("expected object kind %s in %#v", kind, objects)
	return nil
}

func deploymentTemplateNodeSelector(obj map[string]any) map[string]string {
	spec, _ := obj["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	templateSpec, _ := template["spec"].(map[string]any)
	return stringMapFromAnyMap(templateSpec["nodeSelector"])
}

func okJSONResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func notFoundJSONResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusNotFound,
		Body:       io.NopCloser(strings.NewReader(`{"kind":"Status","status":"Failure","message":"not found","reason":"NotFound","code":404}`)),
		Header:     make(http.Header),
	}
}

func stringMapFromAnyMap(raw any) map[string]string {
	items, _ := raw.(map[string]any)
	if len(items) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(items))
	for key, value := range items {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		switch typed := value.(type) {
		case string:
			out[key] = typed
		}
	}
	return out
}

func TestDeleteManagedAppResourcesIgnoresMissingCustomResourceAPIsForStatelessApps(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "uni-api-web-api",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/uni-api:v1",
			Ports:     []int{8000},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	var deleted []string
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet && (strings.HasPrefix(req.URL.Path, "/apis/postgresql.cnpg.io/") || strings.HasPrefix(req.URL.Path, "/apis/volsync.backube/")):
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"kind":"Status","status":"Failure","message":"the server could not find the requested resource","reason":"NotFound","code":404}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet:
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"items":[]}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodDelete:
			deleted = append(deleted, req.Method+" "+req.URL.Path)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
			return nil, nil
		}
	})

	client := &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
		namespace:   "fugue-system",
	}

	svc := &Service{}
	if err := svc.deleteManagedAppResources(context.Background(), client, runtime.NamespaceForTenant(app.TenantID), app); err != nil {
		t.Fatalf("delete managed app resources: %v", err)
	}

	want := []string{
		"DELETE /api/v1/namespaces/fg-tenant-demo/services/app-demo",
		"DELETE /apis/apps/v1/namespaces/fg-tenant-demo/deployments/app-demo",
	}
	sort.Strings(deleted)
	sort.Strings(want)
	if len(deleted) != len(want) {
		t.Fatalf("expected delete requests %v, got %v", want, deleted)
	}
	for i := range want {
		if deleted[i] != want[i] {
			t.Fatalf("expected delete request %q, got %q", want[i], deleted[i])
		}
	}
}

func TestReconcileManagedAppObjectDeletesOrphanedManagedApp(t *testing.T) {
	t.Parallel()

	stateStore := store.New(t.TempDir() + "/store.json")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	namespace := runtime.NamespaceForTenant("tenant_demo")
	managedName := "app-demo"
	var patchedStatus runtime.ManagedAppStatus
	patches := 0
	var deleted []string
	var scaled []string

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPatch && req.URL.Path == managedAppAPIPath(namespace, managedName)+"/status":
			patches++
			var body struct {
				Status runtime.ManagedAppStatus `json:"status"`
			}
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatalf("decode managed app status patch: %v", err)
			}
			patchedStatus = body.Status
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodPatch && req.URL.Path == deploymentAPIPath(namespace, managedName):
			var body struct {
				Spec struct {
					Replicas int `json:"replicas"`
				} `json:"spec"`
			}
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatalf("decode deployment scale patch: %v", err)
			}
			scaled = append(scaled, fmt.Sprintf("%s=%d", req.URL.Path, body.Spec.Replicas))
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet && (strings.HasPrefix(req.URL.Path, "/apis/postgresql.cnpg.io/") || strings.HasPrefix(req.URL.Path, "/apis/volsync.backube/")):
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"kind":"Status","status":"Failure","message":"the server could not find the requested resource","reason":"NotFound","code":404}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet:
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"items":[]}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodDelete:
			deleted = append(deleted, req.Method+" "+req.URL.Path)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
			return nil, nil
		}
	})

	client := &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
		namespace:   "fugue-system",
	}
	var logs bytes.Buffer
	svc := &Service{
		Store:  stateStore,
		Logger: log.New(&logs, "", 0),
	}

	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Name:       managedName,
			Namespace:  namespace,
			Generation: 1,
		},
		Spec: runtime.ManagedAppSpec{
			AppID:    "app_demo",
			TenantID: "tenant_demo",
			Name:     "demo",
			AppSpec: model.AppSpec{
				Image:     "ghcr.io/example/demo:latest",
				Ports:     []int{8080},
				Replicas:  1,
				RuntimeID: "runtime_demo",
			},
		},
	}

	if err := svc.reconcileManagedAppObject(context.Background(), client, managed); err != nil {
		t.Fatalf("reconcile orphaned managed app: %v", err)
	}
	if !strings.Contains(logs.String(), "disabled orphan managed app") {
		t.Fatalf("expected initial orphan disable to be logged, got %q", logs.String())
	}
	logs.Reset()

	if patchedStatus.Phase != runtime.ManagedAppPhaseDisabled {
		t.Fatalf("expected disabled orphan status phase %q, got %q", runtime.ManagedAppPhaseDisabled, patchedStatus.Phase)
	}
	if !strings.Contains(patchedStatus.Message, "app not found in store") || !strings.Contains(patchedStatus.Message, "retained storage") {
		t.Fatalf("expected orphan status message to mention missing store app, got %q", patchedStatus.Message)
	}
	if len(scaled) != 1 || scaled[0] != deploymentAPIPath(namespace, managedName)+"=0" {
		t.Fatalf("expected orphan reconcile to scale deployment to zero, got %v", scaled)
	}
	if len(deleted) != 1 || deleted[0] != "DELETE /api/v1/namespaces/fg-tenant-demo/services/app-demo" {
		t.Fatalf("expected orphan reconcile to delete only app service, got deletes %v", deleted)
	}
	if patches != 1 {
		t.Fatalf("expected one disabled status patch, got %d", patches)
	}

	managed.Status = patchedStatus
	if err := svc.reconcileManagedAppObject(context.Background(), client, managed); err != nil {
		t.Fatalf("reconcile unchanged disabled orphan managed app: %v", err)
	}
	if patches != 1 {
		t.Fatalf("expected unchanged disabled orphan status to skip patch, got %d patches", patches)
	}
	if strings.Contains(logs.String(), "disabled orphan managed app") {
		t.Fatalf("expected unchanged disabled orphan status to skip repeated log, got %q", logs.String())
	}
}
