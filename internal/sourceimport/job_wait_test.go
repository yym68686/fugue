package sourceimport

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
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

func TestSummarizeBuilderLogTailPreservesFailureSignals(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, cause, signal string
		oom                 bool
	}{
		{"heap exhaustion", "FATAL ERROR: Reached heap limit Allocation failed - JavaScript heap out of memory", "out of memory", true},
		{"transport failure", "failed to fetch layer: unexpected EOF", "unexpected eof", false},
		{"storage failure", "write failed: no space left on device", "no space left on device", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A runtime traceback and a long build wrapper used to discard the
			// causal line both at the three-line and the 320-byte boundaries.
			output := tc.cause + "\r\n" + strings.Repeat("native stack frame\n", 30) +
				"Aborted (core dumped)\nerror building image: " + strings.Repeat("wrapper detail ", 30) + "exit status 134\n"
			summary := summarizeBuilderLogTail(output)
			if !strings.Contains(summary, tc.cause) || len(summary) > 320 {
				t.Fatalf("causal line lost or unbounded summary: %q", summary)
			}
			err := errors.New(summary)
			retry, signal := shouldRetryBuilderJobFailure(err)
			if !retry || signal != tc.signal || isBuilderOOMFailure(err, signal) != tc.oom {
				t.Fatalf("incorrect retry decision: retry=%v signal=%q oom=%v", retry, signal, isBuilderOOMFailure(err, signal))
			}
		})
	}
}

func TestSummarizeBuilderLogTailPrioritizesMemoryAndBoundsUnicode(t *testing.T) {
	t.Parallel()
	for _, output := range []string{
		strings.Repeat("构建", 200),
		strings.Repeat("构建", 200) + " OUT OF MEMORY\n" + strings.Repeat("stack frame\n", 3),
		"out of memory\n" + strings.Repeat("stack frame\n", 3) + "unexpected EOF\n",
	} {
		summary := summarizeBuilderLogTail(output)
		if len(summary) > 320 || !utf8.ValidString(summary) {
			t.Fatalf("invalid bounded summary: %q", summary)
		}
		if strings.Contains(strings.ToLower(output), "out of memory") && !isBuilderOOMFailure(errors.New(summary), "") {
			t.Fatalf("memory failure lost: %q", summary)
		}
	}
}

func TestSummarizeBuilderLogTailDoesNotInferOOMFromExitCode(t *testing.T) {
	t.Parallel()
	for _, output := range []string{"", "Aborted (core dumped)\nexit status 134", "Killed\nexit status 137"} {
		summary := summarizeBuilderLogTail(output)
		retry, signal := shouldRetryBuilderJobFailure(errors.New(summary))
		if retry || isBuilderOOMFailure(errors.New(summary), signal) {
			t.Fatalf("unconfirmed memory failure inferred from %q", summary)
		}
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
	if !isTransientBuilderObservationError(errors.New("kubectl -n fugue-system get job build-demo -o json: exit status 1")) {
		t.Fatal("expected empty kubectl get job exit status 1 to be treated as transient")
	}
	if isTransientBuilderObservationError(errors.New("kubectl -n fugue-system get job build-demo -o json: error: the server doesn't have a resource type")) {
		t.Fatal("did not expect permanent kubectl errors to be treated as transient")
	}
}
