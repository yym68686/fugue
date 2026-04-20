package cli

import (
	"testing"

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
