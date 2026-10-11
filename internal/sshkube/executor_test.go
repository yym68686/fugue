package sshkube

import (
	"testing"
	"time"

	"fugue/internal/managedssh"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestKubernetesExecutorRequiresExactWorkloadIdentity(test *testing.T) {
	target := managedssh.Target{TenantID: "tenant", AppID: "app", Namespace: "workloads", PodName: "workspace", PodUID: "pod-uid", Container: "workspace", ContainerID: "containerd://container-id"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "workloads", Name: "workspace", UID: "pod-uid", Labels: map[string]string{"fugue.pro/tenant-id": "tenant", "fugue.pro/app-id": "app"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "workspace"}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "workspace", ContainerID: "containerd://container-id", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	if err := validatePodTarget(pod, target); err != nil {
		test.Fatal(err)
	}
	for name, mutate := range map[string]func(*corev1.Pod){
		"tenant":            func(candidate *corev1.Pod) { candidate.Labels["fugue.pro/tenant-id"] = "other" },
		"app":               func(candidate *corev1.Pod) { candidate.Labels["fugue.pro/app-id"] = "other" },
		"pod reuse":         func(candidate *corev1.Pod) { candidate.UID = "new-uid" },
		"container restart": func(candidate *corev1.Pod) { candidate.Status.ContainerStatuses[0].ContainerID = "new-container" },
		"sidecar":           func(candidate *corev1.Pod) { candidate.Spec.Containers[0].Name = "sidecar" },
		"stopped":           func(candidate *corev1.Pod) { candidate.Status.ContainerStatuses[0].State.Running = nil },
		"deleting":          func(candidate *corev1.Pod) { value := metav1.NewTime(time.Now()); candidate.DeletionTimestamp = &value },
		"host network":      func(candidate *corev1.Pod) { candidate.Spec.HostNetwork = true },
		"host pid":          func(candidate *corev1.Pod) { candidate.Spec.HostPID = true },
		"privileged": func(candidate *corev1.Pod) {
			value := true
			candidate.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: &value}
		},
	} {
		test.Run(name, func(test *testing.T) {
			candidate := pod.DeepCopy()
			mutate(candidate)
			if err := validatePodTarget(candidate, target); err == nil {
				test.Fatal("unsafe target accepted")
			}
		})
	}
}
