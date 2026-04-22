package sourceimport

import (
	"errors"
	"testing"
)

func TestBuilderJobFailedWithFailedCondition(t *testing.T) {
	status := builderJobStatus{
		Conditions: []builderJobCondition{
			{Type: "Failed", Status: "True", Reason: "BackoffLimitExceeded"},
		},
	}
	if !builderJobFailed(status) {
		t.Fatal("expected job to be failed")
	}
	if builderJobCompleted(status) {
		t.Fatal("did not expect job to be completed")
	}
}

func TestBuilderJobFailedWithFailedCountAndNoActivePods(t *testing.T) {
	status := builderJobStatus{
		Failed:    1,
		Active:    0,
		Succeeded: 0,
	}
	if !builderJobFailed(status) {
		t.Fatal("expected job to be failed from failed count")
	}
}

func TestSummarizeBuilderPodFailureEvicted(t *testing.T) {
	var pod builderPod
	pod.Metadata.Name = "build-pod-1"
	pod.Spec.NodeName = "node-a"
	pod.Status.Phase = "Failed"
	pod.Status.Reason = "Evicted"
	pod.Status.Message = "The node was low on resource: ephemeral-storage."

	got := summarizeBuilderPodFailure(pod)
	want := "pod build-pod-1 on node node-a failed: Evicted: The node was low on resource: ephemeral-storage."
	if got != want {
		t.Fatalf("unexpected summary:\nwant: %s\ngot:  %s", want, got)
	}
}

func TestSummarizeBuilderPodFailureIgnoresCompletedInitContainers(t *testing.T) {
	var pod builderPod
	pod.Metadata.Name = "build-pod-2"
	pod.Spec.NodeName = "node-a"
	pod.Status.Phase = "Failed"
	pod.Status.InitContainerStatuses = []builderContainerStatus{
		{
			Name: "git-clone",
			State: builderRuntimeState{
				Terminated: &builderStateDetail{Reason: "Completed", ExitCode: 0},
			},
		},
		{
			Name: "git-checkout",
			State: builderRuntimeState{
				Terminated: &builderStateDetail{Reason: "Completed", ExitCode: 0},
			},
		},
	}
	pod.Status.ContainerStatuses = []builderContainerStatus{
		{
			Name: "buildpacks",
			State: builderRuntimeState{
				Terminated: &builderStateDetail{Reason: "Error", ExitCode: 1},
			},
		},
	}

	got := summarizeBuilderPodFailure(pod)
	want := "pod build-pod-2 on node node-a container buildpacks failed: Error: exit_code=1"
	if got != want {
		t.Fatalf("unexpected summary:\nwant: %s\ngot:  %s", want, got)
	}
}

func TestSummarizeBuilderLogTailUsesLastNonEmptyLines(t *testing.T) {
	got := summarizeBuilderLogTail("\nline one\n\nline two\nline three\nline four\n")
	want := "line two | line three | line four"
	if got != want {
		t.Fatalf("unexpected log tail summary:\nwant: %s\ngot:  %s", want, got)
	}
}

func TestFailingBuilderContainerNamePrefersTerminatedFailure(t *testing.T) {
	var pod builderPod
	pod.Status.InitContainerStatuses = []builderContainerStatus{
		{
			Name: "git-clone",
			State: builderRuntimeState{
				Terminated: &builderStateDetail{Reason: "Completed", ExitCode: 0},
			},
		},
	}
	pod.Status.ContainerStatuses = []builderContainerStatus{
		{
			Name: "kaniko",
			State: builderRuntimeState{
				Terminated: &builderStateDetail{Reason: "Error", ExitCode: 1},
			},
		},
	}

	if got := failingBuilderContainerName(pod); got != "kaniko" {
		t.Fatalf("unexpected failing container name: got %q want %q", got, "kaniko")
	}
}

func TestIsTransientBuilderObservationError(t *testing.T) {
	t.Parallel()

	if !isTransientBuilderObservationError(errors.New("kubectl -n fugue-system get job build-demo -o json: signal: killed")) {
		t.Fatal("expected signal: killed observation error to be treated as transient")
	}
	if isTransientBuilderObservationError(errors.New("kubectl -n fugue-system get job build-demo -o json: error: the server doesn't have a resource type")) {
		t.Fatal("did not expect permanent kubectl errors to be treated as transient")
	}
}
