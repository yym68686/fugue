package model

import "fmt"

const (
	AppNodeLossWait    = "wait"
	AppNodeLossReplace = "replace"
	AppNodeLossLegacy  = "legacy"
)

// Nil is deliberately legacy: reading an old snapshot must not change its
// workload template. Only resource creation supplies the new default.
type AppNodeLossPolicy struct {
	Mode              string `json:"mode"`
	TolerationSeconds *int64 `json:"toleration_seconds,omitempty"`
}

func CloneAppNodeLossPolicy(in *AppNodeLossPolicy) *AppNodeLossPolicy {
	if in == nil {
		return nil
	}
	out := *in
	if in.TolerationSeconds != nil {
		seconds := *in.TolerationSeconds
		out.TolerationSeconds = &seconds
	}
	return &out
}

// Observation recovery cannot roll back a durable user policy from an older
// ManagedApp snapshot. Only an explicit configuration operation changes it.
func PreserveAppNodeLossPolicy(observed *AppSpec, stored AppSpec) {
	if stored.NodeLoss != nil {
		observed.NodeLoss = CloneAppNodeLossPolicy(stored.NodeLoss)
	}
}

func ValidateAppNodeLossPolicy(policy *AppNodeLossPolicy) error {
	if policy == nil {
		return nil
	}
	switch policy.Mode {
	case AppNodeLossWait, AppNodeLossLegacy:
		if policy.TolerationSeconds != nil {
			return fmt.Errorf("node_loss.toleration_seconds is only valid with replace")
		}
	case AppNodeLossReplace:
		if policy.TolerationSeconds != nil && (*policy.TolerationSeconds < 0 || *policy.TolerationSeconds > 86400) {
			return fmt.Errorf("node_loss.toleration_seconds must be between 0 and 86400")
		}
	default:
		return fmt.Errorf("node_loss.mode must be wait, replace, or legacy")
	}
	return nil
}
