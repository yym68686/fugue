package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

const releaseSignalPolicyScopeKey = "global"

func (s *Server) activeReleaseSignalPolicy() (model.ReleaseSignalPolicy, error) {
	policy := model.ReleaseSignalPolicy{Version: "v1"}
	if s == nil || s.store == nil {
		return policy, nil
	}
	artifact, _, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseGuardPolicy, releaseSignalPolicyScopeKey, model.PlatformArtifactReleaseChannelFull)
	if err != nil {
		return policy, err
	}
	if !found {
		return policy, nil
	}
	return releaseSignalPolicyFromArtifact(artifact)
}

func releaseSignalPolicyFromArtifact(artifact model.PlatformArtifact) (model.ReleaseSignalPolicy, error) {
	policy := model.ReleaseSignalPolicy{Version: "v1"}
	if len(artifact.Content) == 0 {
		return policy, nil
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return policy, err
	}
	if err := json.Unmarshal(raw, &policy); err != nil {
		return policy, err
	}
	policy, warnings := normalizeReleaseSignalPolicy(policy)
	if len(warnings) > 0 {
		return policy, errors.New(strings.Join(warnings, "; "))
	}
	return policy, nil
}

func normalizeReleaseSignalPolicy(policy model.ReleaseSignalPolicy) (model.ReleaseSignalPolicy, []string) {
	if strings.TrimSpace(policy.Version) == "" {
		policy.Version = "v1"
	}
	warnings := []string{}
	seen := map[string]struct{}{}
	out := make([]model.ReleaseSignal, 0, len(policy.Signals))
	for index, signal := range policy.Signals {
		normalized, signalWarnings := normalizeReleaseSignal(signal)
		for _, warning := range signalWarnings {
			warnings = append(warnings, fmt.Sprintf("signals[%d]: %s", index, warning))
		}
		if _, ok := seen[normalized.ID]; normalized.ID != "" && ok {
			warnings = append(warnings, fmt.Sprintf("signals[%d]: duplicate id %q", index, normalized.ID))
		}
		if normalized.ID != "" {
			seen[normalized.ID] = struct{}{}
		}
		out = append(out, normalized)
	}
	policy.Signals = out
	return policy, warnings
}

func normalizeReleaseSignal(signal model.ReleaseSignal) (model.ReleaseSignal, []string) {
	warnings := []string{}
	signal.ID = strings.TrimSpace(signal.ID)
	signal.Name = strings.TrimSpace(signal.Name)
	signal.OwnerScope = model.NormalizeReleaseSignalOwnerScope(signal.OwnerScope)
	signal.GateScope = model.NormalizeReleaseSignalGateScope(signal.GateScope)
	signal.Mode = model.NormalizeReleaseSignalMode(signal.Mode)
	signal.Subject = normalizeReleaseSignalSubject(signal.Subject)
	signal.CheckName = strings.TrimSpace(signal.CheckName)
	signal.Reason = strings.TrimSpace(signal.Reason)
	signal.CreatedAt = strings.TrimSpace(signal.CreatedAt)
	signal.UpdatedAt = strings.TrimSpace(signal.UpdatedAt)
	if signal.ID == "" {
		warnings = append(warnings, "id is required")
	}
	if signal.Subject == "" {
		warnings = append(warnings, "subject is required")
	}
	if signal.OwnerScope == "" {
		warnings = append(warnings, "owner_scope must be platform, first_party_service, or tenant_workload")
	}
	if signal.GateScope == "" {
		warnings = append(warnings, "gate_scope must be control_plane, edge_rollout, runtime_rollout, tenant_traffic, or report_only")
	}
	if signal.Mode == "" {
		warnings = append(warnings, "mode must be report_only, soft_gate, canary_gate, rollback_gate, or hard_gate")
	}
	if model.ReleaseSignalBlocksControlPlane(signal) && signal.Reason == "" {
		warnings = append(warnings, "control_plane hard_gate signals require reason")
	}
	return signal, warnings
}

func normalizeReleaseSignalSubject(subject string) string {
	subject = strings.TrimSpace(subject)
	if strings.HasPrefix(strings.ToLower(subject), "app:") {
		return "app:" + strings.TrimSpace(subject[len("app:"):])
	}
	return subject
}

func releaseSignalPolicyValidationResult(artifact model.PlatformArtifact) model.PlatformArtifactValidationResult {
	policy, err := releaseSignalPolicyFromArtifact(artifact)
	if err != nil {
		return model.PlatformArtifactValidationResult{
			Name:     "invariant.release_guard_policy",
			Pass:     false,
			Severity: model.RobustnessSeverityBlockPublish,
			Message:  "release guard policy is invalid: " + err.Error(),
		}
	}
	return model.PlatformArtifactValidationResult{
		Name:     "invariant.release_guard_policy",
		Pass:     true,
		Severity: model.RobustnessSeverityInfo,
		Message:  "release guard policy signals are explicit and schema-valid",
		Evidence: map[string]string{
			"signals": fmt.Sprintf("%d", len(policy.Signals)),
		},
	}
}

func applyReleaseSignalsToRobustnessChecks(checks []model.RobustnessCheck, signals []model.ReleaseSignal) []model.RobustnessCheck {
	if len(signals) == 0 {
		return checks
	}
	enabled := make([]model.ReleaseSignal, 0, len(signals))
	for _, signal := range signals {
		normalized, warnings := normalizeReleaseSignal(signal)
		if len(warnings) > 0 || !normalized.Enabled {
			continue
		}
		enabled = append(enabled, normalized)
	}
	if len(enabled) == 0 {
		return checks
	}
	matched := map[string]bool{}
	for index := range checks {
		for _, signal := range enabled {
			if !releaseSignalMatchesCheck(signal, checks[index]) {
				continue
			}
			matched[signal.ID] = true
			tagCheckWithReleaseSignal(&checks[index], signal)
			if !checks[index].Pass && model.ReleaseSignalBlocksControlPlane(signal) {
				checks[index].Severity = model.RobustnessSeverityBlockPublish
				checks[index].RepairHint = firstNonEmpty(checks[index].RepairHint, "configured release signal is required for control-plane rollout success")
			}
		}
	}
	now := time.Now().UTC().Format(time.RFC3339)
	for _, signal := range enabled {
		if matched[signal.ID] {
			continue
		}
		severity := model.RobustnessSeverityDegraded
		if model.ReleaseSignalBlocksControlPlane(signal) {
			severity = model.RobustnessSeverityBlockPublish
		}
		checks = append(checks, model.RobustnessCheck{
			Name:       "release_signal_observed",
			Pass:       false,
			Severity:   severity,
			Subject:    signal.Subject,
			Expected:   "configured release signal has at least one matching robustness check sample",
			Observed:   "no matching check observed",
			Message:    fmt.Sprintf("release signal %s did not match any current robustness check", signal.ID),
			RepairHint: "remove or correct the release signal, or restore the configured workload/check before rollout",
			Evidence: map[string]string{
				"release_signal_id":          signal.ID,
				"release_signal_mode":        signal.Mode,
				"release_signal_owner_scope": signal.OwnerScope,
				"release_gate_scope":         signal.GateScope,
				"release_signal_check_name":  signal.CheckName,
				"report_only":                fmt.Sprintf("%t", !model.ReleaseSignalBlocksControlPlane(signal)),
				"checked_at":                 now,
			},
		})
	}
	return checks
}

func releaseSignalMatchesCheck(signal model.ReleaseSignal, check model.RobustnessCheck) bool {
	if signal.CheckName != "" && !strings.EqualFold(strings.TrimSpace(signal.CheckName), strings.TrimSpace(check.Name)) {
		return false
	}
	subject := normalizeReleaseSignalSubject(signal.Subject)
	checkSubject := normalizeReleaseSignalSubject(check.Subject)
	if strings.EqualFold(subject, checkSubject) {
		return true
	}
	evidence := check.Evidence
	if evidence == nil {
		return false
	}
	if strings.HasPrefix(strings.ToLower(subject), "app:") {
		appRef := strings.TrimSpace(subject[len("app:"):])
		return strings.EqualFold(appRef, strings.TrimSpace(evidence["app_id"])) ||
			strings.EqualFold(appRef, strings.TrimSpace(evidence["app_name"]))
	}
	return strings.EqualFold(subject, strings.TrimSpace(evidence["hostname"]))
}

func tagCheckWithReleaseSignal(check *model.RobustnessCheck, signal model.ReleaseSignal) {
	if check.Evidence == nil {
		check.Evidence = map[string]string{}
	}
	check.Evidence["release_signal_id"] = signal.ID
	check.Evidence["release_signal_mode"] = signal.Mode
	check.Evidence["release_signal_owner_scope"] = signal.OwnerScope
	check.Evidence["release_gate_scope"] = signal.GateScope
	check.Evidence["report_only"] = fmt.Sprintf("%t", !model.ReleaseSignalBlocksControlPlane(signal))
	if signal.CheckName != "" {
		check.Evidence["release_signal_check_name"] = signal.CheckName
	}
}
