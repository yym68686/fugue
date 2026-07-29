package automation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	maxAutomationOutcomeAggregates = 256
	maxAutomationOutcomeCount      = int64(1_000_000_000)
	maxAutomationTotalOutcomes     = int64(1_000_000_000_000)
)

var ErrInvalidEvaluation = errors.New("invalid automation evaluation")

type EvaluationInput struct {
	Policy   model.AutomationPolicy
	RuleID   string
	Evidence model.AutomationEvaluationEvidence
	Now      time.Time
}

type EvaluationResult struct {
	Decision model.AutomationEvaluationDecision
	Rule     model.AutomationRule
	Evidence model.AutomationEvaluationEvidence
}

// EvaluatePolicy applies a single typed rule to an immutable evidence
// snapshot. It has no persistence or execution side effects.
func EvaluatePolicy(input EvaluationInput) (EvaluationResult, error) {
	now := input.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	policy := input.Policy
	policy.ID = strings.TrimSpace(policy.ID)
	policy.Kind = strings.TrimSpace(strings.ToLower(policy.Kind))
	policy.OwnerType = strings.TrimSpace(strings.ToLower(policy.OwnerType))
	policy.Scope.Type = strings.TrimSpace(strings.ToLower(policy.Scope.Type))
	policy.Scope.ID = strings.TrimSpace(policy.Scope.ID)
	policy.Mode = strings.TrimSpace(strings.ToLower(policy.Mode))
	ruleID := strings.TrimSpace(input.RuleID)
	if policy.ID == "" || policy.Generation <= 0 ||
		policy.Kind != model.AutomationPolicyKindAppRecovery ||
		policy.OwnerType != model.AutomationOwnerUser ||
		policy.Managed ||
		policy.Scope.Type != model.AutomationScopeApp ||
		policy.Scope.ID == "" ||
		ruleID == "" {
		return EvaluationResult{}, invalidEvaluation("policy identity, generation, app scope, and rule id are required")
	}
	switch policy.Mode {
	case model.GatePolicyModeDisabled, model.GatePolicyModeShadow:
	default:
		return EvaluationResult{}, invalidEvaluation("user policy mode must be disabled or shadow")
	}

	rule, found := automationRuleByID(policy.Rules, ruleID)
	if !found {
		return EvaluationResult{}, invalidEvaluation("automation rule %q was not found", ruleID)
	}
	selector, window, err := validateEvaluationRule(rule)
	if err != nil {
		return EvaluationResult{}, err
	}
	evidence, err := normalizeEvaluationEvidence(input.Evidence, now, window)
	if err != nil {
		return EvaluationResult{}, err
	}
	evidenceHash, err := EvidenceHash(evidence)
	if err != nil {
		return EvaluationResult{}, err
	}

	matchingStatusCodes := make(map[int]struct{}, len(selector.StatusCodes))
	for _, statusCode := range selector.StatusCodes {
		matchingStatusCodes[statusCode] = struct{}{}
	}
	var matchingSamples int64
	failureDomainSet := map[string]struct{}{}
	for _, outcome := range evidence.RequestOutcomes {
		if _, matches := matchingStatusCodes[outcome.StatusCode]; !matches {
			continue
		}
		matchingSamples += outcome.Count
		if outcome.FailureDomain != "" {
			failureDomainSet[outcome.FailureDomain] = struct{}{}
		}
	}
	failureDomains := make([]string, 0, len(failureDomainSet))
	for failureDomain := range failureDomainSet {
		failureDomains = append(failureDomains, failureDomain)
	}
	sort.Strings(failureDomains)

	reasonCodes := make([]string, 0, 3)
	if policy.Mode == model.GatePolicyModeDisabled {
		reasonCodes = append(reasonCodes, "policy.disabled")
	}
	if matchingSamples < int64(rule.Trigger.MinimumSamples) {
		reasonCodes = append(reasonCodes, "trigger.minimum_samples")
	}
	if len(failureDomains) < rule.Trigger.MinimumFailureDomains {
		reasonCodes = append(reasonCodes, "trigger.minimum_failure_domains")
	}
	sort.Strings(reasonCodes)
	matched := len(reasonCodes) == 0
	decision := model.AutomationEvaluationDecision{
		PolicyID:                  policy.ID,
		PolicyGeneration:          policy.Generation,
		RuleID:                    rule.ID,
		Scope:                     policy.Scope,
		Mode:                      policy.Mode,
		Matched:                   matched,
		WouldAction:               matched && policy.Mode == model.GatePolicyModeShadow,
		ProductionMutationAllowed: false,
		MatchingSamples:           matchingSamples,
		FailureDomains:            failureDomains,
		EvidenceHash:              evidenceHash,
		ReasonCodes:               reasonCodes,
		EvaluatedAt:               now,
	}
	return EvaluationResult{
		Decision: decision,
		Rule:     cloneAutomationRule(rule),
		Evidence: evidence,
	}, nil
}

// NewObservedActionIntent builds an append-only shadow record. This function
// deliberately has no branch that can set production mutation eligibility.
func NewObservedActionIntent(
	policy model.AutomationPolicy,
	result EvaluationResult,
) (model.AutomationActionIntent, error) {
	if !result.Decision.Matched ||
		!result.Decision.WouldAction ||
		result.Decision.ProductionMutationAllowed ||
		policy.Mode != model.GatePolicyModeShadow {
		return model.AutomationActionIntent{}, invalidEvaluation("only a matched shadow decision may create an observed intent")
	}
	ttl, err := time.ParseDuration(strings.TrimSpace(result.Rule.Safety.TTL))
	if err != nil || ttl < time.Second || ttl > 24*time.Hour {
		return model.AutomationActionIntent{}, invalidEvaluation("automation safety ttl must be between 1s and 24h")
	}
	intent := model.AutomationActionIntent{
		TenantID:                  strings.TrimSpace(policy.TenantID),
		ProjectID:                 strings.TrimSpace(policy.ProjectID),
		PolicyID:                  strings.TrimSpace(policy.ID),
		PolicyGeneration:          policy.Generation,
		RuleID:                    strings.TrimSpace(result.Rule.ID),
		Scope:                     policy.Scope,
		Mode:                      model.GatePolicyModeShadow,
		Source:                    result.Evidence.CollectedBy,
		Status:                    model.AutomationIntentStatusObserved,
		RuleSnapshot:              cloneAutomationRule(result.Rule),
		Evidence:                  cloneEvaluationEvidence(result.Evidence),
		Decision:                  cloneEvaluationDecision(result.Decision),
		EvidenceHash:              result.Decision.EvidenceHash,
		RollbackTarget:            "app-spec:" + strings.TrimSpace(result.Evidence.AppRevision),
		ProductionMutationAllowed: false,
		ExpiresAt:                 result.Decision.EvaluatedAt.Add(ttl).UTC(),
	}
	intent.IdempotencyKey = IntentIdempotencyKey(intent)
	return NormalizeObservedIntent(intent)
}

// NormalizeObservedIntent validates the non-executable intent contract and
// returns a defensive copy suitable for persistence.
func NormalizeObservedIntent(intent model.AutomationActionIntent) (model.AutomationActionIntent, error) {
	intent.ID = strings.TrimSpace(intent.ID)
	intent.TenantID = strings.TrimSpace(intent.TenantID)
	intent.ProjectID = strings.TrimSpace(intent.ProjectID)
	intent.PolicyID = strings.TrimSpace(intent.PolicyID)
	intent.RuleID = strings.TrimSpace(intent.RuleID)
	intent.Scope.Type = strings.TrimSpace(strings.ToLower(intent.Scope.Type))
	intent.Scope.ID = strings.TrimSpace(intent.Scope.ID)
	intent.Mode = strings.TrimSpace(strings.ToLower(intent.Mode))
	intent.Source = strings.TrimSpace(strings.ToLower(intent.Source))
	intent.Status = strings.TrimSpace(strings.ToLower(intent.Status))
	intent.EvidenceHash = strings.TrimSpace(strings.ToLower(intent.EvidenceHash))
	intent.IdempotencyKey = strings.TrimSpace(strings.ToLower(intent.IdempotencyKey))
	intent.RollbackTarget = strings.TrimSpace(intent.RollbackTarget)
	if intent.TenantID == "" ||
		intent.ProjectID == "" ||
		intent.PolicyID == "" ||
		intent.PolicyGeneration <= 0 ||
		intent.RuleID == "" ||
		intent.Scope.Type != model.AutomationScopeApp ||
		intent.Scope.ID == "" {
		return model.AutomationActionIntent{}, invalidEvaluation("intent tenant, project, policy, rule, generation, and app scope are required")
	}
	if intent.Mode != model.GatePolicyModeShadow ||
		intent.Status != model.AutomationIntentStatusObserved ||
		intent.ProductionMutationAllowed {
		return model.AutomationActionIntent{}, invalidEvaluation("initial automation intents must be observe-only shadow records")
	}
	switch intent.Source {
	case model.AutomationIntentSourceAdminReplay, model.AutomationIntentSourceControlLoop:
	default:
		return model.AutomationActionIntent{}, invalidEvaluation("unsupported automation intent source %q", intent.Source)
	}

	if intent.Evidence.CollectedBy != intent.Source {
		return model.AutomationActionIntent{}, invalidEvaluation("intent source must match evidence collector")
	}
	if (intent.Source == model.AutomationIntentSourceAdminReplay && intent.Evidence.Trusted) ||
		(intent.Source == model.AutomationIntentSourceControlLoop && !intent.Evidence.Trusted) {
		return model.AutomationActionIntent{}, invalidEvaluation("intent evidence trust does not match its source")
	}

	intent.RuleSnapshot = cloneAutomationRule(intent.RuleSnapshot)
	if intent.RuleSnapshot.ID != intent.RuleID {
		return model.AutomationActionIntent{}, invalidEvaluation("intent rule snapshot does not match rule id")
	}
	decision := cloneEvaluationDecision(intent.Decision)
	recomputed, err := EvaluatePolicy(EvaluationInput{
		Policy: model.AutomationPolicy{
			ID:         intent.PolicyID,
			TenantID:   intent.TenantID,
			ProjectID:  intent.ProjectID,
			Kind:       model.AutomationPolicyKindAppRecovery,
			OwnerType:  model.AutomationOwnerUser,
			Scope:      intent.Scope,
			Mode:       intent.Mode,
			Managed:    false,
			Rules:      []model.AutomationRule{intent.RuleSnapshot},
			Generation: intent.PolicyGeneration,
		},
		RuleID:   intent.RuleID,
		Evidence: intent.Evidence,
		Now:      decision.EvaluatedAt,
	})
	if err != nil {
		return model.AutomationActionIntent{}, err
	}
	if !recomputed.Decision.Matched ||
		!recomputed.Decision.WouldAction ||
		recomputed.Decision.ProductionMutationAllowed ||
		!reflect.DeepEqual(decision, recomputed.Decision) {
		return model.AutomationActionIntent{}, invalidEvaluation("intent decision does not match the immutable rule and evidence snapshot")
	}
	if intent.EvidenceHash != recomputed.Decision.EvidenceHash {
		return model.AutomationActionIntent{}, invalidEvaluation("intent evidence hash does not match its evidence")
	}
	ttl, err := time.ParseDuration(strings.TrimSpace(intent.RuleSnapshot.Safety.TTL))
	if err != nil || ttl < time.Second || ttl > 24*time.Hour {
		return model.AutomationActionIntent{}, invalidEvaluation("automation safety ttl must be between 1s and 24h")
	}
	expectedExpiry := decision.EvaluatedAt.Add(ttl).UTC()
	if intent.ExpiresAt.IsZero() || !intent.ExpiresAt.Equal(expectedExpiry) {
		return model.AutomationActionIntent{}, invalidEvaluation("intent expiry does not match the rule safety ttl")
	}
	if intent.RollbackTarget != "app-spec:"+recomputed.Evidence.AppRevision {
		return model.AutomationActionIntent{}, invalidEvaluation("intent rollback target does not match the app revision")
	}
	intent.Evidence = recomputed.Evidence
	intent.Decision = decision
	expectedIdempotencyKey := IntentIdempotencyKey(intent)
	if intent.IdempotencyKey == "" {
		intent.IdempotencyKey = expectedIdempotencyKey
	} else if intent.IdempotencyKey != expectedIdempotencyKey {
		return model.AutomationActionIntent{}, invalidEvaluation("intent idempotency key does not match its immutable identity")
	}
	intent.ExpiresAt = intent.ExpiresAt.UTC()
	if !intent.CreatedAt.IsZero() {
		intent.CreatedAt = intent.CreatedAt.UTC()
	}
	if !intent.UpdatedAt.IsZero() {
		intent.UpdatedAt = intent.UpdatedAt.UTC()
	}
	if intent.CreatedAt.IsZero() != intent.UpdatedAt.IsZero() ||
		(!intent.CreatedAt.IsZero() && intent.UpdatedAt.Before(intent.CreatedAt)) {
		return model.AutomationActionIntent{}, invalidEvaluation("intent persistence timestamps are inconsistent")
	}
	return intent, nil
}

func EvidenceHash(evidence model.AutomationEvaluationEvidence) (string, error) {
	normalized, err := normalizeEvaluationEvidence(evidence, evidence.WindowEndedAt, 24*time.Hour)
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(normalized)
	if err != nil {
		return "", invalidEvaluation("encode automation evidence: %v", err)
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func IntentIdempotencyKey(intent model.AutomationActionIntent) string {
	identity := strings.Join([]string{
		strings.TrimSpace(intent.PolicyID),
		strconv.FormatInt(intent.PolicyGeneration, 10),
		strings.TrimSpace(intent.RuleID),
		strings.TrimSpace(intent.Scope.Type),
		strings.TrimSpace(intent.Scope.ID),
		strings.TrimSpace(strings.ToLower(intent.EvidenceHash)),
	}, "\n")
	sum := sha256.Sum256([]byte(identity))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func AppRevisionHash(spec model.AppSpec) (string, error) {
	payload, err := json.Marshal(spec)
	if err != nil {
		return "", invalidEvaluation("encode app revision: %v", err)
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func validateEvaluationRule(rule model.AutomationRule) (model.AutomationRequestMetricSelector, time.Duration, error) {
	rule.ID = strings.TrimSpace(rule.ID)
	rule.Trigger.Type = strings.TrimSpace(strings.ToLower(rule.Trigger.Type))
	rule.Trigger.Source = strings.TrimSpace(rule.Trigger.Source)
	if rule.ID == "" ||
		rule.Trigger.Type != model.AutomationTriggerRequestMetric ||
		rule.Trigger.Source != "app_request_outcomes" ||
		rule.Trigger.RequestMetric == nil ||
		strings.TrimSpace(strings.ToLower(rule.Trigger.RequestMetric.Metric)) != "http_status" ||
		len(rule.Trigger.RequestMetric.ErrorClasses) != 0 ||
		rule.Action.Type != "restart_app" {
		return model.AutomationRequestMetricSelector{}, 0, invalidEvaluation("rule is outside the supported app request-outcome contract")
	}
	selector := *rule.Trigger.RequestMetric
	selector.Metric = strings.TrimSpace(strings.ToLower(selector.Metric))
	selector.Window = strings.TrimSpace(selector.Window)
	window, err := time.ParseDuration(selector.Window)
	if err != nil || window < time.Second || window > 15*time.Minute {
		return model.AutomationRequestMetricSelector{}, 0, invalidEvaluation("request outcome window must be between 1s and 15m")
	}
	if len(selector.StatusCodes) == 0 || len(selector.StatusCodes) > 100 {
		return model.AutomationRequestMetricSelector{}, 0, invalidEvaluation("request outcome selector requires between 1 and 100 status codes")
	}
	seen := map[int]struct{}{}
	selector.StatusCodes = append([]int(nil), selector.StatusCodes...)
	sort.Ints(selector.StatusCodes)
	for _, statusCode := range selector.StatusCodes {
		if statusCode < 500 || statusCode > 599 {
			return model.AutomationRequestMetricSelector{}, 0, invalidEvaluation("request outcome status codes must be server errors")
		}
		if _, exists := seen[statusCode]; exists {
			return model.AutomationRequestMetricSelector{}, 0, invalidEvaluation("request outcome selector contains duplicate status codes")
		}
		seen[statusCode] = struct{}{}
	}
	if rule.Trigger.MinimumSamples < 0 || rule.Trigger.MinimumSamples > 10_000 ||
		rule.Trigger.MinimumFailureDomains < 0 || rule.Trigger.MinimumFailureDomains > 1_000 {
		return model.AutomationRequestMetricSelector{}, 0, invalidEvaluation("request outcome evidence limits are invalid")
	}
	return selector, window, nil
}

func normalizeEvaluationEvidence(
	evidence model.AutomationEvaluationEvidence,
	now time.Time,
	maxWindow time.Duration,
) (model.AutomationEvaluationEvidence, error) {
	evidence.CollectedBy = strings.TrimSpace(strings.ToLower(evidence.CollectedBy))
	evidence.AppRevision = strings.TrimSpace(strings.ToLower(evidence.AppRevision))
	evidence.AppReadiness = strings.TrimSpace(strings.ToLower(evidence.AppReadiness))
	evidence.WindowStartedAt = evidence.WindowStartedAt.UTC()
	evidence.WindowEndedAt = evidence.WindowEndedAt.UTC()
	evidence.AppReadinessObservedAt = evidence.AppReadinessObservedAt.UTC()
	now = now.UTC()
	switch evidence.CollectedBy {
	case model.AutomationIntentSourceAdminReplay, model.AutomationIntentSourceControlLoop:
	default:
		return model.AutomationEvaluationEvidence{}, invalidEvaluation("unsupported evidence collector %q", evidence.CollectedBy)
	}
	if evidence.WindowStartedAt.IsZero() ||
		evidence.WindowEndedAt.IsZero() ||
		!evidence.WindowEndedAt.After(evidence.WindowStartedAt) ||
		evidence.WindowEndedAt.After(now.Add(5*time.Second)) {
		return model.AutomationEvaluationEvidence{}, invalidEvaluation("request outcome window is invalid or in the future")
	}
	if maxWindow <= 0 || evidence.WindowEndedAt.Sub(evidence.WindowStartedAt) > maxWindow {
		return model.AutomationEvaluationEvidence{}, invalidEvaluation("request outcome window exceeds the configured rule window")
	}
	if evidence.AppRevision == "" ||
		len(evidence.AppRevision) > 200 ||
		evidence.AppReadiness == "" ||
		len(evidence.AppReadiness) > 100 ||
		evidence.AppReadinessObservedAt.IsZero() {
		return model.AutomationEvaluationEvidence{}, invalidEvaluation("app revision and readiness evidence are required")
	}
	if len(evidence.RequestOutcomes) == 0 || len(evidence.RequestOutcomes) > maxAutomationOutcomeAggregates {
		return model.AutomationEvaluationEvidence{}, invalidEvaluation("request outcomes require between 1 and %d aggregates", maxAutomationOutcomeAggregates)
	}

	type outcomeKey struct {
		statusCode    int
		failureDomain string
	}
	aggregates := make(map[outcomeKey]int64, len(evidence.RequestOutcomes))
	var total int64
	for _, outcome := range evidence.RequestOutcomes {
		outcome.FailureDomain = strings.TrimSpace(outcome.FailureDomain)
		if outcome.StatusCode < 100 || outcome.StatusCode > 599 ||
			outcome.Count <= 0 ||
			outcome.Count > maxAutomationOutcomeCount ||
			len(outcome.FailureDomain) > 200 {
			return model.AutomationEvaluationEvidence{}, invalidEvaluation("request outcome aggregate is outside its bounded contract")
		}
		if total > maxAutomationTotalOutcomes-outcome.Count {
			return model.AutomationEvaluationEvidence{}, invalidEvaluation("request outcome aggregate total is too large")
		}
		total += outcome.Count
		key := outcomeKey{statusCode: outcome.StatusCode, failureDomain: outcome.FailureDomain}
		if aggregates[key] > maxAutomationOutcomeCount-outcome.Count {
			return model.AutomationEvaluationEvidence{}, invalidEvaluation("combined request outcome aggregate is too large")
		}
		aggregates[key] += outcome.Count
	}
	outcomes := make([]model.AutomationRequestOutcomeAggregate, 0, len(aggregates))
	for key, count := range aggregates {
		outcomes = append(outcomes, model.AutomationRequestOutcomeAggregate{
			StatusCode:    key.statusCode,
			Count:         count,
			FailureDomain: key.failureDomain,
		})
	}
	sort.Slice(outcomes, func(i, j int) bool {
		if outcomes[i].StatusCode != outcomes[j].StatusCode {
			return outcomes[i].StatusCode < outcomes[j].StatusCode
		}
		return outcomes[i].FailureDomain < outcomes[j].FailureDomain
	})
	evidence.RequestOutcomes = outcomes
	return evidence, nil
}

func automationRuleByID(rules []model.AutomationRule, id string) (model.AutomationRule, bool) {
	for _, rule := range rules {
		if strings.TrimSpace(rule.ID) == id {
			return cloneAutomationRule(rule), true
		}
	}
	return model.AutomationRule{}, false
}

func cloneAutomationRule(rule model.AutomationRule) model.AutomationRule {
	rule.Trigger.RequiredEvidence = append([]string(nil), rule.Trigger.RequiredEvidence...)
	if selector := rule.Trigger.RequestMetric; selector != nil {
		cloned := *selector
		cloned.StatusCodes = append([]int(nil), selector.StatusCodes...)
		cloned.ErrorClasses = append([]string(nil), selector.ErrorClasses...)
		rule.Trigger.RequestMetric = &cloned
	}
	if len(rule.Action.Parameters) > 0 {
		parameters := make(map[string]string, len(rule.Action.Parameters))
		for key, value := range rule.Action.Parameters {
			parameters[key] = value
		}
		rule.Action.Parameters = parameters
	}
	return rule
}

func cloneEvaluationEvidence(evidence model.AutomationEvaluationEvidence) model.AutomationEvaluationEvidence {
	evidence.RequestOutcomes = append([]model.AutomationRequestOutcomeAggregate(nil), evidence.RequestOutcomes...)
	return evidence
}

func cloneEvaluationDecision(decision model.AutomationEvaluationDecision) model.AutomationEvaluationDecision {
	decision.FailureDomains = append([]string{}, decision.FailureDomains...)
	decision.ReasonCodes = append([]string{}, decision.ReasonCodes...)
	return decision
}

func invalidEvaluation(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidEvaluation, fmt.Sprintf(format, args...))
}
