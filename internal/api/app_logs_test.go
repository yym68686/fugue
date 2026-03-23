package api

import "testing"

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
