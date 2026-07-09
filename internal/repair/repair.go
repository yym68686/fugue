package repair

import (
	"fmt"
	"strings"
	"time"

	"fugue/internal/localwal"
	"fugue/internal/model"
)

const (
	ActionEdgeWorkerRestart       = "edge_worker_restart"
	ActionDNSComponentRestart     = "dns_component_restart"
	ActionNodeGuardianSubtask     = "node_guardian_subtask_restart"
	ActionRouteBundleReload       = "edge_route_bundle_reload"
	ActionGenerationCacheRefresh  = "local_generation_cache_refresh"
	ActionK3SAgentGuardedRestart  = "k3s_agent_guarded_restart"
	ActionCNIKubeProxyHumanRepair = "cni_kube_proxy_human_repair"
)

type State struct {
	Attempts map[string][]model.EdgeRepairAttempt
}

type Decision struct {
	Allowed           bool
	Reason            string
	SafetyClass       string
	NextAttempt       int
	CooldownRemaining time.Duration
	SelfQuarantine    bool
}

func DefaultPolicy(action string) model.EdgeRepairPolicy {
	switch strings.TrimSpace(action) {
	case ActionEdgeWorkerRestart:
		return model.EdgeRepairPolicy{Action: action, SafetyClass: model.EdgeRepairSafetyL3StatelessRestart, FeatureFlag: "FUGUE_AUTONOMY_REPAIR_EDGE_WORKER_RESTART_ENABLED", Cooldown: 2 * time.Minute, MaxAttempts: 3, QuarantineOnFail: true}
	case ActionDNSComponentRestart:
		return model.EdgeRepairPolicy{Action: action, SafetyClass: model.EdgeRepairSafetyL3StatelessRestart, FeatureFlag: "FUGUE_AUTONOMY_REPAIR_DNS_RESTART_ENABLED", Cooldown: 2 * time.Minute, MaxAttempts: 3, QuarantineOnFail: true}
	case ActionNodeGuardianSubtask:
		return model.EdgeRepairPolicy{Action: action, SafetyClass: model.EdgeRepairSafetyL2LocalReload, FeatureFlag: "FUGUE_AUTONOMY_REPAIR_NODE_GUARDIAN_SUBTASK_ENABLED", Cooldown: time.Minute, MaxAttempts: 5}
	case ActionRouteBundleReload, ActionGenerationCacheRefresh:
		return model.EdgeRepairPolicy{Action: action, SafetyClass: model.EdgeRepairSafetyL2LocalReload, FeatureFlag: "FUGUE_AUTONOMY_REPAIR_LKG_RELOAD_ENABLED", Cooldown: 30 * time.Second, MaxAttempts: 5}
	case ActionK3SAgentGuardedRestart:
		return model.EdgeRepairPolicy{Action: action, SafetyClass: model.EdgeRepairSafetyL4GuardedNodeRepair, FeatureFlag: "FUGUE_AUTONOMY_REPAIR_K3S_AGENT_RESTART_ENABLED", Cooldown: 15 * time.Minute, MaxAttempts: 1, HumanBoundary: "requires local API unhealthy, no stateful workload migration, and preflight evidence", QuarantineOnFail: true}
	case ActionCNIKubeProxyHumanRepair:
		return model.EdgeRepairPolicy{Action: action, SafetyClass: model.EdgeRepairSafetyL5HumanOnly, FeatureFlag: "FUGUE_AUTONOMY_REPAIR_CNI_KUBE_PROXY_ENABLED", Cooldown: time.Hour, MaxAttempts: 0, HumanBoundary: "CNI/kube-proxy changes require human approval unless future fence evidence proves safe", QuarantineOnFail: true}
	default:
		return model.EdgeRepairPolicy{Action: strings.TrimSpace(action), SafetyClass: model.EdgeRepairSafetyL0ObserveOnly, Cooldown: time.Minute, MaxAttempts: 0, HumanBoundary: "unknown repair action is observe-only"}
	}
}

func Evaluate(policy model.EdgeRepairPolicy, state State, subject string, now time.Time, flagEnabled bool) Decision {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	action := strings.TrimSpace(policy.Action)
	if action == "" {
		return Decision{Allowed: false, Reason: "repair_action_required", SafetyClass: policy.SafetyClass}
	}
	if policy.MaxAttempts <= 0 || strings.TrimSpace(policy.SafetyClass) == model.EdgeRepairSafetyL5HumanOnly {
		return Decision{Allowed: false, Reason: "human_approval_required", SafetyClass: policy.SafetyClass, SelfQuarantine: policy.QuarantineOnFail}
	}
	if !flagEnabled {
		return Decision{Allowed: false, Reason: "feature_flag_disabled", SafetyClass: policy.SafetyClass}
	}
	key := historyKey(action, subject)
	history := state.Attempts[key]
	failures := 0
	for i := len(history) - 1; i >= 0; i-- {
		attempt := history[i]
		if attempt.Status == "success" {
			break
		}
		failures++
	}
	if failures >= policy.MaxAttempts {
		return Decision{Allowed: false, Reason: "repair_max_attempts_exceeded", SafetyClass: policy.SafetyClass, NextAttempt: failures + 1, SelfQuarantine: policy.QuarantineOnFail}
	}
	for i := len(history) - 1; i >= 0; i-- {
		attempt := history[i]
		if attempt.Status == "success" {
			break
		}
		if policy.Cooldown > 0 && !attempt.FinishedAt.IsZero() {
			nextAllowed := attempt.FinishedAt.Add(policy.Cooldown)
			if now.Before(nextAllowed) {
				return Decision{Allowed: false, Reason: "repair_cooldown_active", SafetyClass: policy.SafetyClass, NextAttempt: failures + 1, CooldownRemaining: nextAllowed.Sub(now), SelfQuarantine: policy.QuarantineOnFail && failures >= policy.MaxAttempts}
			}
		}
	}
	return Decision{Allowed: true, Reason: "repair_allowed", SafetyClass: policy.SafetyClass, NextAttempt: failures + 1}
}

func AppendAttempt(state *State, subject string, attempt model.EdgeRepairAttempt) {
	if state.Attempts == nil {
		state.Attempts = map[string][]model.EdgeRepairAttempt{}
	}
	key := historyKey(attempt.Action, subject)
	state.Attempts[key] = append(state.Attempts[key], attempt)
}

func RecordWAL(path, nodeID, subject string, attempt model.EdgeRepairAttempt, expiresAt *time.Time) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	evidence := map[string]string{
		"action":       strings.TrimSpace(attempt.Action),
		"status":       strings.TrimSpace(attempt.Status),
		"attempt":      fmt.Sprintf("%d", attempt.Attempt),
		"message":      strings.TrimSpace(attempt.Message),
		"safety_class": strings.TrimSpace(attempt.SafetyClass),
	}
	for key, value := range attempt.Evidence {
		if strings.TrimSpace(key) != "" {
			evidence[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	record, err := localwal.NewRecord("node-guardian", nodeID, "repair_action", evidence, evidence["generation"], expiresAt, attempt.StartedAt)
	if err != nil {
		return err
	}
	record.Subject = strings.TrimSpace(subject)
	record.SafetyClass = strings.TrimSpace(attempt.SafetyClass)
	return localwal.Append(path, record)
}

func historyKey(action, subject string) string {
	return strings.TrimSpace(action) + "|" + strings.TrimSpace(subject)
}
