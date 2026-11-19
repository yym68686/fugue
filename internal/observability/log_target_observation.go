package observability

import corev1 "k8s.io/api/core/v1"

// This is the Kubernetes list snapshot used by the collector, not proof of
// the runtime's state at the instant of the read or of log delivery.
type logTargetObservation struct {
	PodPhase           string `json:"pod_phase"`
	ContainerState     string `json:"container_state"`
	StatusUnknown      bool   `json:"container_status_unknown,omitempty"`
	ContainerIDPresent bool   `json:"container_id_present"`
	FinishedAtPresent  bool   `json:"finished_at_present"`
	RestartCount       int32  `json:"restart_count"`
}

func observeLogTarget(target kubernetesLogTarget) logTargetObservation {
	out := logTargetObservation{PodPhase: "unknown", ContainerState: "unknown"}
	switch phase := target.pod.Status.Phase; phase {
	case corev1.PodPending, corev1.PodRunning, corev1.PodSucceeded, corev1.PodFailed:
		out.PodPhase = string(phase)
	}
	for _, statuses := range [...][]corev1.ContainerStatus{
		target.pod.Status.InitContainerStatuses,
		target.pod.Status.ContainerStatuses,
		target.pod.Status.EphemeralContainerStatuses,
	} {
		for _, status := range statuses {
			if status.Name != target.container {
				continue
			}
			out.RestartCount = status.RestartCount
			state, id := status.State, status.ContainerID
			if target.previous {
				state, id = status.LastTerminationState, ""
			}
			switch {
			case state.Terminated != nil:
				out.ContainerState = "terminated"
				out.StatusUnknown = state.Terminated.Reason == "ContainerStatusUnknown"
				out.FinishedAtPresent = !state.Terminated.FinishedAt.IsZero()
				if state.Terminated.ContainerID != "" {
					id = state.Terminated.ContainerID
				}
			case state.Running != nil:
				out.ContainerState = "running"
			case state.Waiting != nil:
				out.ContainerState = "waiting"
			}
			out.ContainerIDPresent = id != ""
			return out
		}
	}
	return out
}
