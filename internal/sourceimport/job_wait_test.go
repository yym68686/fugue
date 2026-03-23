package sourceimport

import "testing"

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
