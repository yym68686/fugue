package controller

import (
	"context"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

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
				Name              string    `json:"name"`
				CreationTimestamp time.Time `json:"creationTimestamp"`
			}{
				Name:              "demo-abc123",
				CreationTimestamp: time.Date(2026, time.March, 26, 10, 0, 0, 0, time.UTC),
			},
			Spec: struct {
				NodeName       string `json:"nodeName,omitempty"`
				InitContainers []struct {
					Name string `json:"name"`
				} `json:"initContainers"`
				Containers []struct {
					Name string `json:"name"`
				} `json:"containers"`
			}{
				NodeName: "gcp1",
			},
			Status: struct {
				Phase                 string                `json:"phase"`
				Reason                string                `json:"reason,omitempty"`
				Message               string                `json:"message,omitempty"`
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
	if !strings.Contains(status.Message, "deployment progressing") {
		t.Fatalf("expected rollout progress message, got %q", status.Message)
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
		"DELETE /api/v1/namespaces/fg-tenant-demo/services/uni-api-web-api",
		"DELETE /api/v1/namespaces/fg-tenant-demo/services/uni-api-web-api-db-postgres",
		"DELETE /api/v1/namespaces/fg-tenant-demo/secrets/uni-api-web-api-pgsec",
		"DELETE /apis/apps/v1/namespaces/fg-tenant-demo/deployments/uni-api-web-api",
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

func TestDeleteManagedAppResourcesKeepsLegacyManagedPostgresObjectsWhenStoragePathPresent(t *testing.T) {
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
				Image:       "postgres:16-alpine",
				Database:    "uniapi",
				User:        "uniapi",
				Password:    "secret",
				ServiceName: "uni-api-web-api-db-postgres",
				StoragePath: "/var/lib/fugue/tenant-data/fg-tenant-demo/uni-api-web-api-db/postgres",
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
		"DELETE /api/v1/namespaces/fg-tenant-demo/persistentvolumeclaims/uni-api-web-api-db-postgres-data",
		"DELETE /api/v1/namespaces/fg-tenant-demo/secrets/uni-api-web-api-pgsec",
		"DELETE /api/v1/namespaces/fg-tenant-demo/services/uni-api-web-api",
		"DELETE /api/v1/namespaces/fg-tenant-demo/services/uni-api-web-api-db-postgres",
		"DELETE /api/v1/namespaces/fg-tenant-demo/services/uni-api-web-api-db-postgres-rw",
		"DELETE /apis/apps/v1/namespaces/fg-tenant-demo/deployments/uni-api-web-api",
		"DELETE /apis/apps/v1/namespaces/fg-tenant-demo/deployments/uni-api-web-api-db-postgres",
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
		"DELETE /api/v1/namespaces/fg-tenant-demo/services/uni-api-web-api",
		"DELETE /apis/apps/v1/namespaces/fg-tenant-demo/deployments/uni-api-web-api",
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
