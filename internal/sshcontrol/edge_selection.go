package sshcontrol

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// Terminal Pods are retained evidence, not possible executors. Pending,
// unknown and terminating Pods must still participate in ambiguity checks:
// their absence from Ready endpoints does not prove that they have stopped.
const livePodFields = "status.phase!=Failed,status.phase!=Succeeded"

type edgeSelectionFailure struct {
	err     error
	retryAt time.Time
	delay   time.Duration
}

func (runtime *KubernetesRuntime) edgePod(ctx context.Context, edge EdgeConsumer) (corev1.Pod, error) {
	// Only negative instance selection is cached. Authorization renewal,
	// revocation, runtime observation and exact-instance ACKs are never cached.
	runtime.selectionMu.Lock()
	defer runtime.selectionMu.Unlock()
	raw, _ := json.Marshal(edge)
	key := string(raw)
	now := time.Now()
	previous := runtime.selectionFailures[key]
	if previous != nil && now.Before(previous.retryAt) {
		return corev1.Pod{}, previous.err
	}
	selector := labels.SelectorFromSet(edge.PodLabels).String()
	pods, err := runtime.client.CoreV1().Pods(edge.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector, FieldSelector: "spec.nodeName=" + edge.EdgeID + "," + livePodFields, Limit: 3,
	})
	if err != nil {
		return corev1.Pod{}, fmt.Errorf("SSH edge %s instance inventory: %w", edge.EdgeID, err)
	}
	active := make([]corev1.Pod, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded {
			active = append(active, pod)
		}
	}
	if pods.Continue == "" && len(active) == 1 {
		delete(runtime.selectionFailures, key)
		return active[0], nil
	}
	instances := make([]string, 0, len(active))
	for _, pod := range active {
		instances = append(instances, fmt.Sprintf("%s/%s:%s", pod.Name, pod.UID, pod.Status.Phase))
	}
	sort.Strings(instances)
	err = fmt.Errorf("SSH edge %s candidate is missing or ambiguous: namespace=%s selector=%s candidates=[%s] incomplete=%t", edge.EdgeID, edge.Namespace, selector, strings.Join(instances, ","), pods.Continue != "")
	delay := 10 * time.Second
	if previous != nil {
		delay = min(previous.delay*2, time.Minute)
	}
	if runtime.selectionFailures == nil {
		runtime.selectionFailures = make(map[string]*edgeSelectionFailure)
	}
	// Configuration changes must bypass old failures without growing this
	// process-local negative cache indefinitely.
	if len(runtime.selectionFailures) >= 64 && previous == nil {
		clear(runtime.selectionFailures)
	}
	runtime.selectionFailures[key] = &edgeSelectionFailure{err: err, retryAt: time.Now().Add(delay), delay: delay}
	return corev1.Pod{}, err
}
