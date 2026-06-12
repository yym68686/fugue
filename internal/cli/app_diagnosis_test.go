package cli

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestSummarizeLatestPodStateIgnoresHistoricalFailuresAndInitImageMismatches(t *testing.T) {
	t.Parallel()

	inventory := &model.AppRuntimePodInventory{
		Groups: []model.AppRuntimePodGroup{
			{
				OwnerKind: "ReplicaSet",
				OwnerName: "demo-abc123",
				Pods: []model.ClusterPod{
					{
						Name:  "demo-ready",
						Phase: "Running",
						Ready: true,
						Containers: []model.ClusterPodContainer{
							{Name: "wait-postgres", Image: "docker.io/library/busybox:1.36", Ready: true, State: "running"},
							{Name: "demo", Image: "registry.pull.example/fugue-apps/demo:git-newcommit", Ready: true, State: "running"},
						},
					},
					{
						Name:  "demo-evicted",
						Phase: "Failed",
						Ready: false,
						Containers: []model.ClusterPodContainer{
							{Name: "demo", Image: "registry.pull.example/fugue-apps/demo:git-old", Ready: false, State: "terminated", Reason: "Evicted"},
						},
					},
				},
			},
		},
	}

	state := summarizeLatestPodState(inventory, "registry.pull.example/fugue-apps/demo:git-newcommit")
	if state.LivePods != 1 {
		t.Fatalf("expected only active pods to count as live, got %d", state.LivePods)
	}
	if state.ReadyPods != 1 {
		t.Fatalf("expected one ready active pod, got %d", state.ReadyPods)
	}
	if len(state.Issues) != 0 {
		t.Fatalf("expected no runtime issues for ready pod with helper init container, got %+v", state.Issues)
	}
}

func TestDescribePodIssueReportsExpectedImageMismatchWhenNoContainerMatches(t *testing.T) {
	t.Parallel()

	pod := model.ClusterPod{
		Name:  "demo-pending",
		Phase: "Running",
		Ready: false,
		Containers: []model.ClusterPodContainer{
			{Name: "demo", Image: "registry.pull.example/fugue-apps/demo:git-old", Ready: false, State: "running"},
		},
	}

	issue := describePodIssue(pod, "registry.pull.example/fugue-apps/demo:git-newcommit")
	if issue == "" {
		t.Fatal("expected image mismatch issue")
	}
}

func TestDescribePodIssueDoesNotCompareRuntimeImageIDToExpectedSpecImage(t *testing.T) {
	t.Parallel()

	pod := model.ClusterPod{
		Name:  "demo-ready",
		Phase: "Running",
		Ready: true,
		Containers: []model.ClusterPodContainer{
			{
				Name:    "demo",
				Image:   "registry.pull.example/fugue-apps/demo@sha256:expected",
				ImageID: "sha256:runtime-id",
				Ready:   true,
				State:   "running",
			},
		},
	}

	issue := describePodIssue(pod, "registry.pull.example/fugue-apps/demo@sha256:expected")
	if issue != "" {
		t.Fatalf("expected no issue when spec image matches and image_id differs, got %q", issue)
	}
}

func TestDescribePodIssueIgnoresNonComparableRuntimeImageIDs(t *testing.T) {
	t.Parallel()

	for _, image := range []string{
		"sha256:runtime-id",
		"docker-pullable://registry.pull.example/fugue-apps/demo@sha256:expected",
		"containerd://runtime-id",
		"docker://runtime-id",
	} {
		pod := model.ClusterPod{
			Name:  "demo-ready",
			Phase: "Running",
			Ready: true,
			Containers: []model.ClusterPodContainer{
				{
					Name:  "demo",
					Image: image,
					Ready: true,
					State: "running",
				},
			},
		}

		issue := describePodIssue(pod, "registry.pull.example/fugue-apps/demo@sha256:expected")
		if issue != "" {
			t.Fatalf("expected no issue for non-comparable image %q, got %q", image, issue)
		}
	}
}

func TestDescribePodIssueIgnoresCompletedInitLikeContainers(t *testing.T) {
	t.Parallel()

	pod := model.ClusterPod{
		Name:  "demo-ready",
		Phase: "Running",
		Ready: true,
		Containers: []model.ClusterPodContainer{
			{
				Name:   "wait-postgres",
				Image:  "busybox:1.36",
				State:  "terminated",
				Reason: "Completed",
			},
			{
				Name:  "demo",
				Image: "registry.pull.example/fugue-apps/demo:git-abc123",
				Ready: true,
				State: "running",
			},
		},
	}

	issue := describePodIssue(pod, "")
	if issue != "" {
		t.Fatalf("expected no issue for completed helper container in ready pod, got %q", issue)
	}
}

func TestSummarizeAppBuildArtifactFallsBackToCurrentAppImageRefs(t *testing.T) {
	t.Parallel()

	managedRef := "registry.push.example/fugue-apps/demo:git-abc123"
	runtimeRef := "registry.pull.example/fugue-apps/demo:git-abc123"
	app := model.App{
		ID:   "app_123",
		Name: "demo",
		Source: &model.AppSource{
			Type:             model.AppSourceTypeGitHubPublic,
			ResolvedImageRef: managedRef,
		},
		Spec: model.AppSpec{Image: runtimeRef},
	}
	createdAt := time.Date(2026, 6, 12, 9, 45, 0, 0, time.UTC)
	operations := []model.Operation{
		{
			ID:        "op_deploy",
			AppID:     app.ID,
			Type:      model.OperationTypeDeploy,
			Status:    model.OperationStatusCompleted,
			CreatedAt: createdAt,
		},
	}
	images := &appImageInventoryResponse{
		Versions: []appImageVersion{
			{ImageRef: managedRef, RuntimeImageRef: runtimeRef, Status: "available", Current: true},
		},
	}
	pods := &model.AppRuntimePodInventory{
		Groups: []model.AppRuntimePodGroup{
			{
				OwnerKind: "ReplicaSet",
				OwnerName: "demo-abc123",
				Pods: []model.ClusterPod{
					{
						Name:  "demo-ready",
						Phase: "Running",
						Ready: true,
						Containers: []model.ClusterPodContainer{
							{Name: "demo", Image: runtimeRef, Ready: true, State: "running"},
						},
					},
				},
			},
		},
	}

	report, _, latestDeploy := summarizeAppBuildArtifact(app, operations, images, pods)
	if report == nil {
		t.Fatal("expected artifact report")
	}
	if report.ManagedImageRef != managedRef {
		t.Fatalf("expected managed image ref fallback %q, got %q", managedRef, report.ManagedImageRef)
	}
	if report.RuntimeImageRef != runtimeRef {
		t.Fatalf("expected runtime image ref fallback %q, got %q", runtimeRef, report.RuntimeImageRef)
	}
	if report.RegistryImageStatus != "available" || !report.RegistryImageCurrent {
		t.Fatalf("expected current available registry image, got status=%q current=%v", report.RegistryImageStatus, report.RegistryImageCurrent)
	}
	if len(report.PodIssues) != 0 || report.ReadyPods != 1 {
		t.Fatalf("expected one ready pod with no issues, got ready=%d issues=%+v", report.ReadyPods, report.PodIssues)
	}

	diagnosis := summarizeAppOverviewDiagnosis(app, nil, latestDeploy, nil, report)
	if diagnosis != nil {
		t.Fatalf("expected healthy artifact summary to suppress overview diagnosis, got %+v", diagnosis)
	}
}
