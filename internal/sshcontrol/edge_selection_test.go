package sshcontrol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
)

func TestEdgeSelectionDistinguishesTerminalEvidenceFromPossibleExecutors(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodFailed, corev1.PodSucceeded, corev1.PodRunning, corev1.PodPending, corev1.PodUnknown, ""} {
		t.Run(string(phase), func(t *testing.T) {
			_, edge, pod, _, _, _, _ := publicationFixture(t)
			pod.Labels = edge.PodLabels
			old := pod.DeepCopy()
			old.Name, old.UID, old.Status.Phase = "retained", "retained-uid", phase
			client := fake.NewClientset()
			client.PrependReactor("list", "pods", func(action kubetesting.Action) (bool, kuberuntime.Object, error) {
				fields := action.(kubetesting.ListAction).GetListRestrictions().Fields.String()
				if !strings.Contains(fields, "status.phase!=Failed") || !strings.Contains(fields, "status.phase!=Succeeded") || !strings.Contains(fields, "spec.nodeName="+edge.EdgeID) {
					t.Fatal("terminal history can fill the server-side limit", fields)
				}
				return true, &corev1.PodList{Items: []corev1.Pod{*old, pod}}, nil
			})
			runtime := &KubernetesRuntime{client: client}
			selected, err := runtime.edgePod(t.Context(), edge)
			terminal := phase == corev1.PodFailed || phase == corev1.PodSucceeded
			if (err == nil) != terminal || terminal && selected.UID != pod.UID {
				t.Fatal("possible executor was ignored or terminal history blocked replacement", selected.UID, err)
			}
		})
	}
}

func TestEdgeSelectionBackoffIsBoundedAndConfigurationChangeBypassesIt(t *testing.T) {
	_, edge, pod, _, _, _, _ := publicationFixture(t)
	pod.Labels = edge.PodLabels
	client := fake.NewClientset()
	complete, present := false, true
	requests := 0
	client.PrependReactor("list", "pods", func(action kubetesting.Action) (bool, kuberuntime.Object, error) {
		requests++
		result := &corev1.PodList{}
		if present {
			result.Items = []corev1.Pod{pod}
		}
		if !complete {
			result.ListMeta = metav1.ListMeta{Continue: "another-page"}
		}
		return true, result, nil
	})
	runtime := &KubernetesRuntime{client: client}
	if _, err := runtime.edgePod(t.Context(), edge); err == nil {
		t.Fatal("partial inventory authorized an executor")
	}
	for i := 0; i < 5; i++ {
		if _, err := runtime.edgePod(t.Context(), edge); err == nil {
			t.Fatal("negative cache authorized an executor")
		}
	}
	if requests != 1 {
		t.Fatal("stable failure retried without backoff", requests)
	}
	raw, _ := json.Marshal(edge)
	failure := runtime.selectionFailures[string(raw)]
	for i := 0; i < 8; i++ {
		failure.retryAt = time.Time{}
		_, _ = runtime.edgePod(t.Context(), edge)
		failure = runtime.selectionFailures[string(raw)]
		if failure.delay > time.Minute {
			t.Fatal("unbounded recovery delay", failure.delay)
		}
	}
	complete = true
	edge.ServiceUID = "new-reviewed-service"
	if selected, err := runtime.edgePod(t.Context(), edge); err != nil || selected.UID != pod.UID {
		t.Fatal("configuration change did not bypass old failure", err)
	}
	present = false
	if _, err := runtime.edgePod(t.Context(), edge); err == nil {
		t.Fatal("positive selection was cached after executor disappeared")
	}
}
