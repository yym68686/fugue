package runtime

import "fugue/internal/model"

// Only application Deployment Pods receive this policy, never independent
// database instances or node-wide/system workloads. NoSchedule is untouched.
func AppNodeLossScheduling(policy *model.AppNodeLossPolicy, in SchedulingConstraints) SchedulingConstraints {
	if policy == nil || policy.Mode == model.AppNodeLossLegacy {
		return in
	}
	out := in
	out.Tolerations = make([]Toleration, 0, len(in.Tolerations)+2)
	for _, item := range in.Tolerations {
		if item.Effect == "NoExecute" && (item.Key == "node.kubernetes.io/not-ready" || item.Key == "node.kubernetes.io/unreachable") {
			continue
		}
		out.Tolerations = append(out.Tolerations, item)
	}
	for _, key := range []string{"node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"} {
		toleration := Toleration{Key: key, Operator: "Exists", Effect: "NoExecute"}
		if policy.Mode == model.AppNodeLossReplace {
			seconds := int64(300)
			if policy.TolerationSeconds != nil {
				seconds = *policy.TolerationSeconds
			}
			toleration.TolerationSeconds = &seconds
		}
		out.Tolerations = append(out.Tolerations, toleration)
	}
	return out
}
