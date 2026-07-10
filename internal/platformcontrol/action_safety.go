package platformcontrol

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

type ActionSafetyEvaluator struct {
	Contracts []model.AutomaticActionContract
	Policies  []model.GatePolicy
	Now       func() time.Time
	LookupEnv func(string) string
}

func NewActionSafetyEvaluator(contracts []model.AutomaticActionContract, policies []model.GatePolicy) ActionSafetyEvaluator {
	return ActionSafetyEvaluator{
		Contracts: append([]model.AutomaticActionContract(nil), contracts...),
		Policies:  append([]model.GatePolicy(nil), policies...),
	}
}

func (e ActionSafetyEvaluator) Evaluate(request model.ActionSafetyRequest) model.ActionSafetyDecision {
	now := time.Now().UTC()
	if e.Now != nil {
		now = e.Now().UTC()
	}
	decision := model.ActionSafetyDecision{
		ContractID:     strings.TrimSpace(request.ContractID),
		Subject:        strings.TrimSpace(request.Subject),
		EffectiveMode:  model.GatePolicyModeDisabled,
		EvidenceStates: map[string]string{},
		BlastRadius: model.BlastRadiusEvaluation{
			Pass:  true,
			Scope: strings.TrimSpace(request.Scope),
		},
		GeneratedAt: now,
	}
	violations := []model.ActionSafetyViolation{}
	addViolation := func(code, message string) {
		violations = append(violations, model.ActionSafetyViolation{Code: code, Message: message})
	}

	contract, ok := actionContractByID(e.Contracts, request.ContractID)
	if !ok {
		addViolation("contract.not_found", "automatic action contract is not registered")
		return finalizeActionSafetyDecision(decision, violations)
	}
	decision.ContractID = contract.ID
	decision.GatePolicyID = contract.GatePolicyID

	if actionType := strings.TrimSpace(request.ActionType); actionType == "" || actionType != strings.TrimSpace(contract.ActionType) {
		addViolation("contract.action_type_mismatch", fmt.Sprintf("action_type must be %q", contract.ActionType))
	}
	if trigger := strings.TrimSpace(request.TriggerInvariant); trigger == "" || trigger != strings.TrimSpace(contract.TriggerInvariant) {
		addViolation("contract.trigger_invariant_mismatch", fmt.Sprintf("trigger_invariant must be %q", contract.TriggerInvariant))
	}
	if scope := strings.TrimSpace(request.Scope); scope == "" || scope != strings.TrimSpace(contract.Scope) {
		addViolation("contract.scope_mismatch", fmt.Sprintf("scope must be %q", contract.Scope))
	}
	if strings.TrimSpace(request.Subject) == "" {
		addViolation("request.subject_required", "subject is required")
	}

	policy, ok := gatePolicyByID(e.Policies, contract.GatePolicyID)
	if !ok {
		addViolation("gate_policy.not_found", fmt.Sprintf("gate policy %q is not registered", contract.GatePolicyID))
	} else {
		decision.EffectiveMode = tighterActionMode(policy.Mode, request.CurrentMode)
		if !stringInSlice(decision.EffectiveMode, contract.AllowedModes) {
			addViolation("gate_policy.mode_not_allowed", fmt.Sprintf("effective gate mode %q is not allowed by the action contract", decision.EffectiveMode))
		}
		if decision.EffectiveMode == model.GatePolicyModeDisabled {
			addViolation("gate_policy.disabled", "automatic action is disabled by gate policy")
		}
		if decision.EffectiveMode == model.GatePolicyModeCanary && !request.CanaryScopeMatch {
			addViolation("gate_policy.outside_canary_scope", "automatic action target is outside the configured canary scope")
		}
	}

	if e.envBool("FUGUE_AUTONOMY_KILL_SWITCH", false) {
		decision.EffectiveMode = model.GatePolicyModeDisabled
		addViolation("kill_switch.global", "global autonomy kill switch is active")
	}
	if killSwitch := strings.TrimSpace(contract.KillSwitchEnv); killSwitch != "" && e.envBool(killSwitch, false) {
		decision.EffectiveMode = model.GatePolicyModeDisabled
		addViolation("kill_switch.action", fmt.Sprintf("automatic action kill switch %s is active", killSwitch))
	}
	if enableEnv := strings.TrimSpace(contract.EnableEnv); enableEnv != "" && !e.actionEnvEnabled(enableEnv) {
		decision.EffectiveMode = model.GatePolicyModeDisabled
		addViolation("enable_switch.action", fmt.Sprintf("automatic action is not enabled by %s", enableEnv))
	}

	requiredEvidence := uniqueStrings(contract.RequiredEvidence)
	evidenceByID := map[string]model.ActionSafetyEvidence{}
	failureDomains := uniqueStrings(request.FailureDomains)
	for _, evidence := range request.Evidence {
		id := strings.TrimSpace(evidence.ID)
		if id == "" {
			continue
		}
		state := normalizeEvidenceState(evidence.State)
		if state == "" {
			state = model.InvariantEvidenceStateUnknown
		}
		if evidence.ExpiresAt != nil && !evidence.ExpiresAt.After(now) {
			state = model.InvariantEvidenceStateStale
		}
		evidence.State = state
		evidenceByID[id] = evidence
		decision.EvidenceStates[id] = state
		if failureDomain := strings.TrimSpace(evidence.FailureDomain); failureDomain != "" {
			failureDomains = append(failureDomains, failureDomain)
		}
	}
	failureDomains = uniqueStrings(failureDomains)
	for _, evidenceID := range requiredEvidence {
		evidence, found := evidenceByID[evidenceID]
		if !found {
			decision.EvidenceStates[evidenceID] = model.InvariantEvidenceStateUnknown
			addViolation("evidence.missing", fmt.Sprintf("required evidence %q is missing", evidenceID))
			continue
		}
		switch evidence.State {
		case model.InvariantEvidenceStatePass:
		case model.InvariantEvidenceStateFail:
			addViolation("evidence.failed", fmt.Sprintf("required evidence %q failed", evidenceID))
		case model.InvariantEvidenceStateStale:
			addViolation("evidence.stale", fmt.Sprintf("required evidence %q is stale", evidenceID))
		default:
			addViolation("evidence.unknown", fmt.Sprintf("required evidence %q is unknown", evidenceID))
		}
		if strings.TrimSpace(evidence.Source) == "" {
			addViolation("evidence.source_missing", fmt.Sprintf("required evidence %q has no trusted source", evidenceID))
		}
	}

	minimumSamples := contract.MinimumSamples
	minimumFailureDomains := contract.MinimumFailureDomains
	soakMinDuration := contract.SoakMinDuration
	blastRadius := contract.BlastRadius
	if ok {
		minimumSamples = maxInt(minimumSamples, policy.MinimumSamples)
		minimumFailureDomains = maxInt(minimumFailureDomains, policy.MinimumFailureDomains)
		soakMinDuration = longerDuration(soakMinDuration, policy.SoakMinDuration)
		blastRadius = tighterBlastRadius(blastRadius, policy.BlastRadius)
	}
	if request.SampleCount < minimumSamples {
		addViolation("evidence.minimum_samples", fmt.Sprintf("sample_count=%d is below minimum=%d", request.SampleCount, minimumSamples))
	}
	if len(failureDomains) < minimumFailureDomains {
		addViolation("evidence.minimum_failure_domains", fmt.Sprintf("failure_domains=%d is below minimum=%d", len(failureDomains), minimumFailureDomains))
	}
	if soakDuration, err := parseOptionalDuration(soakMinDuration); err != nil {
		addViolation("gate_policy.invalid_soak", err.Error())
	} else if soakDuration > 0 {
		if request.SoakStartedAt == nil {
			addViolation("gate_policy.soak_not_started", "soak_started_at is required")
		} else if request.SoakStartedAt.Add(soakDuration).After(now) {
			addViolation("gate_policy.soak_incomplete", "minimum soak window has not completed")
		}
	}

	ttl, ttlErr := actionTTL(request.TTL, contract.TTL)
	if ttlErr != nil {
		addViolation("action.invalid_ttl", ttlErr.Error())
	} else if ttl > 0 {
		expiresAt := now.Add(ttl)
		decision.ExpiresAt = &expiresAt
	}

	if strings.TrimSpace(contract.RollbackAction) == "" {
		addViolation("contract.rollback_action_missing", "automatic action contract has no rollback or compensation action")
	}
	if strings.TrimSpace(contract.RecoveryCondition) == "" {
		addViolation("contract.recovery_condition_missing", "automatic action contract has no recovery condition")
	}
	if strings.TrimSpace(contract.RunbookRef) == "" {
		addViolation("contract.runbook_missing", "automatic action contract has no runbook")
	}
	if contract.RequiresRollbackTarget && strings.TrimSpace(request.RollbackTarget) == "" {
		addViolation("request.rollback_target_missing", "rollback_target is required")
	}
	if strings.TrimSpace(contract.HumanApprovalBoundary) != "" && !request.HumanApproved {
		addViolation("request.human_approval_required", contract.HumanApprovalBoundary)
	}
	if contract.RequiresAudit && !request.AuditReady {
		addViolation("request.audit_not_ready", "tamper-evident audit sink is not ready")
	}
	if contract.RequiresWAL && !request.WALReady {
		addViolation("request.wal_not_ready", "local WAL is not ready")
	}
	if contract.RequiresIdempotencyKey && strings.TrimSpace(request.IdempotencyKey) == "" {
		addViolation("request.idempotency_key_missing", "idempotency_key is required")
	}
	if contract.RequiresFencingToken && request.FencingToken <= 0 {
		addViolation("request.fencing_token_missing", "positive fencing_token is required")
	}

	radiusPolicy := model.BlastRadiusPolicy{
		PreserveMinHealthyEdgeGroups:    blastRadius.PreserveMinHealthyEdgeGroups,
		PreserveMinEligibleEdgesPerHost: blastRadius.PreserveMinEligibleEdgesPerHost,
		MaxRemovedEdgesPerHost:          blastRadius.MaxEdgesPerGroup,
	}
	if hasBlastRadiusLimit(blastRadius) {
		if len(request.CurrentCounts) == 0 || len(request.CandidateCounts) == 0 {
			decision.BlastRadius = model.BlastRadiusEvaluation{
				Pass:   false,
				Scope:  strings.TrimSpace(request.Scope),
				Reason: "blast radius evidence is missing",
			}
			addViolation("blast_radius.unknown", "current_counts and candidate_counts are required")
		} else {
			decision.BlastRadius = model.EvaluateBlastRadius(request.CurrentCounts, request.CandidateCounts, request.Scope, radiusPolicy)
			if !decision.BlastRadius.Pass {
				addViolation("blast_radius.exceeded", decision.BlastRadius.Reason)
			}
			if blastRadius.MaxNodes > 0 && removedCount(request.CurrentCounts, request.CandidateCounts) > blastRadius.MaxNodes {
				decision.BlastRadius.Pass = false
				decision.BlastRadius.Reason = "node blast radius cap exceeded"
				addViolation("blast_radius.max_nodes", fmt.Sprintf("removed targets exceed max_nodes=%d", blastRadius.MaxNodes))
			}
		}
	}

	sort.SliceStable(violations, func(i, j int) bool {
		if violations[i].Code != violations[j].Code {
			return violations[i].Code < violations[j].Code
		}
		return violations[i].Message < violations[j].Message
	})
	return finalizeActionSafetyDecision(decision, violations)
}

func finalizeActionSafetyDecision(decision model.ActionSafetyDecision, violations []model.ActionSafetyViolation) model.ActionSafetyDecision {
	decision.Violations = violations
	decision.Pass = len(violations) == 0
	switch {
	case !decision.Pass:
		decision.Allowed = false
		decision.WouldAction = false
		decision.ProductionMutationAllowed = false
	case decision.EffectiveMode == model.GatePolicyModeShadow:
		decision.Allowed = false
		decision.WouldAction = true
		decision.ProductionMutationAllowed = false
	case decision.EffectiveMode == model.GatePolicyModeCanary || decision.EffectiveMode == model.GatePolicyModeEnforced:
		decision.Allowed = true
		decision.WouldAction = false
		decision.ProductionMutationAllowed = true
	default:
		decision.Allowed = false
		decision.WouldAction = false
		decision.ProductionMutationAllowed = false
	}
	if len(decision.EvidenceStates) == 0 {
		decision.EvidenceStates = nil
	}
	return decision
}

func actionContractByID(contracts []model.AutomaticActionContract, id string) (model.AutomaticActionContract, bool) {
	id = strings.TrimSpace(id)
	for _, contract := range contracts {
		if strings.TrimSpace(contract.ID) == id {
			return contract, true
		}
	}
	return model.AutomaticActionContract{}, false
}

func gatePolicyByID(policies []model.GatePolicy, id string) (model.GatePolicy, bool) {
	id = strings.TrimSpace(id)
	for _, policy := range policies {
		if strings.TrimSpace(policy.ID) == id {
			return policy, true
		}
	}
	return model.GatePolicy{}, false
}

func normalizeEvidenceState(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case model.InvariantEvidenceStatePass:
		return model.InvariantEvidenceStatePass
	case model.InvariantEvidenceStateFail:
		return model.InvariantEvidenceStateFail
	case model.InvariantEvidenceStateUnknown:
		return model.InvariantEvidenceStateUnknown
	case model.InvariantEvidenceStateStale:
		return model.InvariantEvidenceStateStale
	default:
		return ""
	}
}

func normalizeActionMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case model.GatePolicyModeDisabled:
		return model.GatePolicyModeDisabled
	case model.GatePolicyModeShadow:
		return model.GatePolicyModeShadow
	case model.GatePolicyModeCanary:
		return model.GatePolicyModeCanary
	case model.GatePolicyModeEnforced:
		return model.GatePolicyModeEnforced
	default:
		return ""
	}
}

func tighterActionMode(policyMode, requestedMode string) string {
	policyMode = normalizeActionMode(policyMode)
	if policyMode == "" {
		policyMode = model.GatePolicyModeShadow
	}
	requestedMode = normalizeActionMode(requestedMode)
	if requestedMode == "" {
		return policyMode
	}
	if actionModeRank(requestedMode) < actionModeRank(policyMode) {
		return requestedMode
	}
	return policyMode
}

func actionModeRank(mode string) int {
	switch normalizeActionMode(mode) {
	case model.GatePolicyModeDisabled:
		return 0
	case model.GatePolicyModeShadow:
		return 1
	case model.GatePolicyModeCanary:
		return 2
	case model.GatePolicyModeEnforced:
		return 3
	default:
		return -1
	}
}

func (e ActionSafetyEvaluator) actionEnvEnabled(name string) bool {
	value := strings.TrimSpace(strings.ToLower(e.lookupEnv(name)))
	switch value {
	case "1", "true", "yes", "on", model.GatePolicyModeCanary, model.GatePolicyModeEnforced:
		return true
	default:
		return false
	}
}

func (e ActionSafetyEvaluator) envBool(name string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(e.lookupEnv(name)))
	switch value {
	case "":
		return fallback
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func (e ActionSafetyEvaluator) lookupEnv(name string) string {
	if e.LookupEnv != nil {
		return e.LookupEnv(name)
	}
	return os.Getenv(name)
}

func actionTTL(requestedRaw, maximumRaw string) (time.Duration, error) {
	maximum, err := parseOptionalDuration(maximumRaw)
	if err != nil {
		return 0, fmt.Errorf("invalid contract TTL %q: %w", maximumRaw, err)
	}
	requestedRaw = strings.TrimSpace(requestedRaw)
	if requestedRaw == "" {
		if maximum <= 0 {
			return 0, fmt.Errorf("positive action TTL is required")
		}
		return maximum, nil
	}
	requested, err := time.ParseDuration(requestedRaw)
	if err != nil || requested <= 0 {
		return 0, fmt.Errorf("action TTL must be a positive duration")
	}
	if maximum > 0 && requested > maximum {
		return 0, fmt.Errorf("action TTL %s exceeds contract maximum %s", requested, maximum)
	}
	return requested, nil
}

func parseOptionalDuration(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration < 0 {
		return 0, fmt.Errorf("invalid duration %q", raw)
	}
	return duration, nil
}

func longerDuration(left, right string) string {
	leftDuration, leftErr := parseOptionalDuration(left)
	rightDuration, rightErr := parseOptionalDuration(right)
	switch {
	case leftErr != nil:
		return right
	case rightErr != nil:
		return left
	case rightDuration > leftDuration:
		return right
	default:
		return left
	}
}

func tighterBlastRadius(left, right model.GateBlastRadiusPolicy) model.GateBlastRadiusPolicy {
	return model.GateBlastRadiusPolicy{
		MaxNodes:                        smallerPositive(left.MaxNodes, right.MaxNodes),
		MaxEdgesPerGroup:                smallerPositive(left.MaxEdgesPerGroup, right.MaxEdgesPerGroup),
		PreserveMinHealthyEdgeGroups:    maxInt(left.PreserveMinHealthyEdgeGroups, right.PreserveMinHealthyEdgeGroups),
		PreserveMinEligibleEdgesPerHost: maxInt(left.PreserveMinEligibleEdgesPerHost, right.PreserveMinEligibleEdgesPerHost),
	}
}

func smallerPositive(left, right int) int {
	switch {
	case left <= 0:
		return right
	case right <= 0:
		return left
	case left < right:
		return left
	default:
		return right
	}
}

func hasBlastRadiusLimit(policy model.GateBlastRadiusPolicy) bool {
	return policy.MaxNodes > 0 ||
		policy.MaxEdgesPerGroup > 0 ||
		policy.PreserveMinHealthyEdgeGroups > 0 ||
		policy.PreserveMinEligibleEdgesPerHost > 0
}

func removedCount(before, after map[string]int) int {
	removed := 0
	for key, beforeCount := range before {
		afterCount := after[key]
		if beforeCount > afterCount {
			removed += beforeCount - afterCount
		}
	}
	return removed
}

func uniqueStrings(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			set[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func stringInSlice(value string, values []string) bool {
	value = strings.TrimSpace(value)
	for _, candidate := range values {
		if strings.TrimSpace(candidate) == value {
			return true
		}
	}
	return false
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func FormatActionSafetyCountMap(values map[string]int) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+strconv.Itoa(values[key]))
	}
	return strings.Join(parts, ",")
}
