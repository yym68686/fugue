package api

import (
	"testing"

	"fugue/internal/model"
)

func TestSummarizeKubePodFailureEvicted(t *testing.T) {
	var pod kubePodInfo
	pod.Metadata.Name = "fugue-build-1"
	pod.Spec.NodeName = "node-b"
	pod.Status.Phase = "Failed"
	pod.Status.Reason = "Evicted"
	pod.Status.Message = "The node was low on resource: ephemeral-storage."

	got := summarizeKubePodFailure(pod)
	want := "pod fugue-build-1 on node node-b failed: Evicted: The node was low on resource: ephemeral-storage."
	if got != want {
		t.Fatalf("unexpected summary:\nwant: %s\ngot:  %s", want, got)
	}
}

func TestSummarizeKubePodFailureIgnoresCompletedInitContainers(t *testing.T) {
	var pod kubePodInfo
	pod.Metadata.Name = "fugue-build-2"
	pod.Spec.NodeName = "node-b"
	pod.Status.Phase = "Failed"
	pod.Status.InitContainerStatuses = []kubeContainerStatus{
		{
			Name: "git-clone",
			State: kubeRuntimeState{
				Terminated: &kubeStateDetail{Reason: "Completed", ExitCode: 0},
			},
		},
		{
			Name: "git-checkout",
			State: kubeRuntimeState{
				Terminated: &kubeStateDetail{Reason: "Completed", ExitCode: 0},
			},
		},
	}
	pod.Status.ContainerStatuses = []kubeContainerStatus{
		{
			Name: "buildpacks",
			State: kubeRuntimeState{
				Terminated: &kubeStateDetail{Reason: "Error", ExitCode: 1},
			},
		},
	}

	got := summarizeKubePodFailure(pod)
	want := "pod fugue-build-2 on node node-b container buildpacks failed: Error: exit_code=1"
	if got != want {
		t.Fatalf("unexpected summary:\nwant: %s\ngot:  %s", want, got)
	}
}

func TestKubeJobFailedWithFailedCondition(t *testing.T) {
	status := kubeJobStatus{
		Conditions: []kubeJobCondition{
			{Type: "Failed", Status: "True", Reason: "BackoffLimitExceeded"},
		},
	}
	if !kubeJobFailed(status) {
		t.Fatal("expected job to be failed")
	}
}

func TestRuntimeLogTargetUsesAppIDSelectorForAppComponent(t *testing.T) {
	selector, containerName, err := runtimeLogTarget(model.App{
		ID:   "app_demo",
		Name: "demo",
	}, "app")
	if err != nil {
		t.Fatalf("runtime log target: %v", err)
	}

	wantSelector := "app.kubernetes.io/managed-by=fugue,app.kubernetes.io/name=demo,fugue.pro/app-id=app_demo"
	if selector != wantSelector {
		t.Fatalf("expected selector %q, got %q", wantSelector, selector)
	}
	if containerName != "demo" {
		t.Fatalf("expected container name %q, got %q", "demo", containerName)
	}
}
