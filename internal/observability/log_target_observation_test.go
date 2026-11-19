package observability

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestObserveLogTargetClassifiesUnknownTerminationWithoutLeakingRuntimeDetails(t *testing.T) {
	target := kubernetesLogTarget{
		pod: corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{UID: "pod-uid"},
			Status: corev1.PodStatus{
				Phase: corev1.PodFailed,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:         "app",
					ContainerID:  "containerd://runtime-id",
					RestartCount: 0,
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						Reason: "ContainerStatusUnknown",
					}},
				}},
			},
		},
		container: "app",
	}

	got := observeLogTarget(target)
	if got.PodPhase != "Failed" || got.ContainerState != "terminated" || !got.StatusUnknown ||
		!got.ContainerIDPresent || got.FinishedAtPresent || got.RestartCount != 0 {
		t.Fatalf("unexpected lifecycle classification: %+v", got)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "runtime-id") || strings.Contains(string(raw), "ContainerStatusUnknown") {
		t.Fatalf("lifecycle projection leaked runtime identifier or arbitrary status reason: %s", raw)
	}
}

func TestObserveLogTargetUsesPreviousTerminationAndRetainsErrorLifecycle(t *testing.T) {
	finished := metav1.NewTime(time.Date(2026, time.November, 19, 10, 0, 0, 0, time.UTC))
	target := kubernetesLogTarget{
		pod: corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{UID: "pod-uid"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
				Name:         "app",
				ContainerID:  "containerd://current-id",
				RestartCount: 2,
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ContainerID: "containerd://previous-id",
					FinishedAt:  finished,
					Reason:      "OOMKilled",
				}},
			}}},
		},
		container: "app",
		previous:  true,
	}
	lifecycle := observeLogTarget(target)
	if lifecycle.PodPhase != "Running" || lifecycle.ContainerState != "terminated" ||
		lifecycle.StatusUnknown || !lifecycle.ContainerIDPresent || !lifecycle.FinishedAtPresent ||
		lifecycle.RestartCount != 2 {
		t.Fatalf("unexpected previous-container lifecycle: %+v", lifecycle)
	}

	p := NewPipeline(Config{}, nil)
	p.observeLogSource(logSourceObservation{
		Identity:   "source",
		Target:     lifecycle,
		Outcome:    "open_error",
		ErrorClass: "not_found",
		ObservedAt: time.Now().UTC(),
	})
	p.observeLogSource(logSourceObservation{
		Identity:   "source",
		Target:     logTargetObservation{PodPhase: "Running", ContainerState: "running"},
		Outcome:    "node_backoff",
		GapEvents:  1,
		GapReason:  "retention_window",
		ObservedAt: time.Now().UTC(),
	})
	value, err := p.DiagnosticSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := value.(map[string]any)
	row := snapshot["sources"].([]logSourceObservation)[0]
	if row.LastErrorTarget != lifecycle {
		t.Fatalf("error lifecycle was not retained: %+v", row.LastErrorTarget)
	}
	receipt := snapshot["gap_history"].(map[string]any)["receipts"].([]logGapObservation)[0]
	if receipt.Target.ContainerState != "running" || receipt.ReadExchange != nil {
		t.Fatalf("gap borrowed target state or transport evidence from the earlier error: %+v", receipt)
	}
}
