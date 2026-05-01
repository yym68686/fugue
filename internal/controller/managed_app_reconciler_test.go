package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

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
				Name              string    `json:"name"`
				CreationTimestamp time.Time `json:"creationTimestamp"`
				DeletionTimestamp string    `json:"deletionTimestamp,omitempty"`
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
				Name              string    `json:"name"`
				CreationTimestamp time.Time `json:"creationTimestamp"`
				DeletionTimestamp string    `json:"deletionTimestamp,omitempty"`
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
				Name              string    `json:"name"`
				CreationTimestamp time.Time `json:"creationTimestamp"`
				DeletionTimestamp string    `json:"deletionTimestamp,omitempty"`
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
				Name              string    `json:"name"`
				CreationTimestamp time.Time `json:"creationTimestamp"`
				DeletionTimestamp string    `json:"deletionTimestamp,omitempty"`
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
				Name              string    `json:"name"`
				CreationTimestamp time.Time `json:"creationTimestamp"`
				DeletionTimestamp string    `json:"deletionTimestamp,omitempty"`
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
				Name              string    `json:"name"`
				CreationTimestamp time.Time `json:"creationTimestamp"`
				DeletionTimestamp string    `json:"deletionTimestamp,omitempty"`
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
				Name              string    `json:"name"`
				CreationTimestamp time.Time `json:"creationTimestamp"`
				DeletionTimestamp string    `json:"deletionTimestamp,omitempty"`
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

func TestBuildManagedAppStatusMarksUnschedulablePodsAsError(t *testing.T) {
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
				DeletionTimestamp string    `json:"deletionTimestamp,omitempty"`
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

	if status.Phase != runtime.ManagedAppPhaseError {
		t.Fatalf("expected phase error, got %q", status.Phase)
	}
	if !strings.Contains(status.Message, "volume node affinity conflict") {
		t.Fatalf("expected node affinity conflict in message, got %q", status.Message)
	}
	if !strings.Contains(status.Message, "disk-pressure") {
		t.Fatalf("expected disk-pressure in message, got %q", status.Message)
	}
	if !strings.Contains(status.Message, "demo-abc123") {
		t.Fatalf("expected pod name in message, got %q", status.Message)
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

func TestApplyManagedAppDesiredStateInjectsWorkloadIdentityIntoManagedAndRuntimeObjects(t *testing.T) {
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
	if got := managedEnv["FUGUE_PROJECT_ID"]; got != project.ID {
		t.Fatalf("expected managed app FUGUE_PROJECT_ID %q, got %q", project.ID, got)
	}
	if got := managedEnv["FUGUE_RUNTIME_ID"]; got != app.Spec.RuntimeID {
		t.Fatalf("expected managed app FUGUE_RUNTIME_ID %q, got %q", app.Spec.RuntimeID, got)
	}
	if got := managedEnv["FUGUE_API_URL"]; got != "https://api.example.com" {
		t.Fatalf("expected managed app FUGUE_API_URL to be normalized, got %q", got)
	}
	if got := managedEnv["FUGUE_APP_URL"]; got != "https://gateway.example.com" {
		t.Fatalf("expected managed app FUGUE_APP_URL to be injected, got %q", got)
	}
	managedClaims, err := workloadidentity.Parse("signing-secret", managedEnv["FUGUE_TOKEN"])
	if err != nil {
		t.Fatalf("parse managed app workload token: %v", err)
	}
	if managedClaims.ProjectID != project.ID {
		t.Fatalf("expected managed token project scope %q, got %q", project.ID, managedClaims.ProjectID)
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
	deploymentClaims, err := workloadidentity.Parse("signing-secret", deploymentEnv["FUGUE_TOKEN"])
	if err != nil {
		t.Fatalf("parse deployment workload token: %v", err)
	}
	if deploymentClaims.ProjectID != project.ID {
		t.Fatalf("expected deployment token project scope %q, got %q", project.ID, deploymentClaims.ProjectID)
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

func managedAppSpecEnv(obj map[string]any) map[string]string {
	spec, _ := obj["spec"].(map[string]any)
	appSpec, _ := spec["appSpec"].(map[string]any)
	return stringMapFromAnyMap(appSpec["env"])
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
	var deleted []string

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPatch && req.URL.Path == managedAppAPIPath(namespace, managedName)+"/status":
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
	svc := &Service{Store: stateStore}

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

	if patchedStatus.Phase != runtime.ManagedAppPhaseDeleting {
		t.Fatalf("expected orphan status phase %q, got %q", runtime.ManagedAppPhaseDeleting, patchedStatus.Phase)
	}
	if !strings.Contains(patchedStatus.Message, "app not found in store") {
		t.Fatalf("expected orphan status message to mention missing store app, got %q", patchedStatus.Message)
	}

	wantDeleted := []string{
		"DELETE " + managedAppAPIPath(namespace, managedName),
		"DELETE /api/v1/namespaces/" + namespace + "/services/app-demo",
		"DELETE /apis/apps/v1/namespaces/" + namespace + "/deployments/app-demo",
	}
	sort.Strings(deleted)
	sort.Strings(wantDeleted)
	if len(deleted) != len(wantDeleted) {
		t.Fatalf("expected delete requests %v, got %v", wantDeleted, deleted)
	}
	for i := range wantDeleted {
		if deleted[i] != wantDeleted[i] {
			t.Fatalf("expected delete request %q, got %q", wantDeleted[i], deleted[i])
		}
	}
}
