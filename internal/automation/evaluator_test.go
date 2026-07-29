package automation

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEvaluatePolicyCreatesDeterministicObserveOnlyIntent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 29, 1, 0, 0, 0, time.UTC)
	input := evaluationFixture(now)
	result, err := EvaluatePolicy(input)
	if err != nil {
		t.Fatalf("evaluate policy: %v", err)
	}
	if !result.Decision.Matched ||
		!result.Decision.WouldAction ||
		result.Decision.ProductionMutationAllowed ||
		result.Decision.MatchingSamples != 3 ||
		!reflect.DeepEqual(result.Decision.FailureDomains, []string{"edge-us"}) ||
		len(result.Decision.ReasonCodes) != 0 {
		t.Fatalf("unexpected shadow decision: %+v", result.Decision)
	}
	if len(result.Evidence.RequestOutcomes) != 3 ||
		result.Evidence.RequestOutcomes[0].StatusCode != 200 ||
		result.Decision.EvidenceHash == "" {
		t.Fatalf("evidence was not normalized and hashed: %+v", result)
	}

	intent, err := NewObservedActionIntent(input.Policy, result)
	if err != nil {
		t.Fatalf("build observed intent: %v", err)
	}
	if intent.Status != model.AutomationIntentStatusObserved ||
		intent.Source != model.AutomationIntentSourceAdminReplay ||
		intent.Mode != model.GatePolicyModeShadow ||
		intent.ProductionMutationAllowed ||
		intent.IdempotencyKey == "" ||
		intent.RollbackTarget != "app-spec:sha256:revision" ||
		!intent.ExpiresAt.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("unexpected observed intent: %+v", intent)
	}
	repeated, err := NewObservedActionIntent(input.Policy, result)
	if err != nil {
		t.Fatalf("rebuild observed intent: %v", err)
	}
	if repeated.IdempotencyKey != intent.IdempotencyKey {
		t.Fatalf("same immutable evaluation changed idempotency key: first=%s second=%s", intent.IdempotencyKey, repeated.IdempotencyKey)
	}
}

func TestEvaluatePolicyCanonicalizesIntentTimestampForPostgres(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 29, 1, 0, 0, 123456789, time.UTC)
	result, err := EvaluatePolicy(evaluationFixture(now))
	if err != nil {
		t.Fatalf("evaluate policy: %v", err)
	}
	expectedEvaluatedAt := now.Truncate(time.Microsecond)
	if !result.Decision.EvaluatedAt.Equal(expectedEvaluatedAt) {
		t.Fatalf(
			"evaluation timestamp was not canonicalized: got=%s want=%s",
			result.Decision.EvaluatedAt.Format(time.RFC3339Nano),
			expectedEvaluatedAt.Format(time.RFC3339Nano),
		)
	}

	intent, err := NewObservedActionIntent(evaluationFixture(now).Policy, result)
	if err != nil {
		t.Fatalf("build observed intent: %v", err)
	}
	persisted := intent
	// PostgreSQL TIMESTAMPTZ has microsecond precision. The decision remains
	// inside JSONB while expires_at is returned from a timestamp column.
	persisted.ExpiresAt = persisted.ExpiresAt.Round(time.Microsecond)
	if _, err := NormalizeObservedIntent(persisted); err != nil {
		t.Fatalf("normalize PostgreSQL timestamp round trip: %v", err)
	}
}

func TestEvaluatePolicyDisabledAndInsufficientEvidenceDoNotCreateIntent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 29, 1, 0, 0, 0, time.UTC)
	disabled := evaluationFixture(now)
	disabled.Policy.Mode = model.GatePolicyModeDisabled
	result, err := EvaluatePolicy(disabled)
	if err != nil {
		t.Fatalf("evaluate disabled policy: %v", err)
	}
	if result.Decision.Matched || result.Decision.WouldAction ||
		!reflect.DeepEqual(result.Decision.ReasonCodes, []string{"policy.disabled"}) {
		t.Fatalf("disabled policy unexpectedly matched: %+v", result.Decision)
	}
	if _, err := NewObservedActionIntent(disabled.Policy, result); !errors.Is(err, ErrInvalidEvaluation) {
		t.Fatalf("disabled policy created intent: %v", err)
	}

	insufficient := evaluationFixture(now)
	insufficient.Policy.Rules[0].Trigger.MinimumSamples = 4
	insufficient.Policy.Rules[0].Trigger.MinimumFailureDomains = 2
	result, err = EvaluatePolicy(insufficient)
	if err != nil {
		t.Fatalf("evaluate insufficient evidence: %v", err)
	}
	if result.Decision.Matched || result.Decision.WouldAction ||
		!reflect.DeepEqual(result.Decision.ReasonCodes, []string{
			"trigger.minimum_failure_domains",
			"trigger.minimum_samples",
		}) {
		t.Fatalf("insufficient evidence unexpectedly matched: %+v", result.Decision)
	}
}

func TestEvaluatePolicyAcceptsTrustedControlLoopNoDataAndRejectsTrustConfusion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 29, 1, 0, 0, 0, time.UTC)
	controlLoop := evaluationFixture(now)
	controlLoop.Evidence.CollectedBy = model.AutomationIntentSourceControlLoop
	controlLoop.Evidence.Trusted = true
	controlLoop.Evidence.RequestOutcomes = nil
	result, err := EvaluatePolicy(controlLoop)
	if err != nil {
		t.Fatalf("evaluate trusted empty control-loop evidence: %v", err)
	}
	if result.Decision.Matched ||
		result.Decision.WouldAction ||
		result.Decision.MatchingSamples != 0 ||
		!reflect.DeepEqual(result.Decision.ReasonCodes, []string{
			"trigger.minimum_failure_domains",
			"trigger.minimum_samples",
		}) {
		t.Fatalf("empty trusted evidence did not fail closed: %+v", result.Decision)
	}

	untrustedControlLoop := controlLoop
	untrustedControlLoop.Evidence.Trusted = false
	if _, err := EvaluatePolicy(untrustedControlLoop); !errors.Is(err, ErrInvalidEvaluation) {
		t.Fatalf("untrusted control-loop evidence was accepted: %v", err)
	}

	trustedReplay := evaluationFixture(now)
	trustedReplay.Evidence.Trusted = true
	if _, err := EvaluatePolicy(trustedReplay); !errors.Is(err, ErrInvalidEvaluation) {
		t.Fatalf("trusted admin replay evidence was accepted: %v", err)
	}

	emptyReplay := evaluationFixture(now)
	emptyReplay.Evidence.RequestOutcomes = nil
	if _, err := EvaluatePolicy(emptyReplay); !errors.Is(err, ErrInvalidEvaluation) {
		t.Fatalf("empty admin replay evidence was accepted: %v", err)
	}
}

func TestEvaluationEvidenceHashIsOrderAndAggregationStable(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 29, 1, 0, 0, 0, time.UTC)
	first := evaluationFixture(now)
	second := evaluationFixture(now)
	second.Evidence.RequestOutcomes = []model.AutomationRequestOutcomeAggregate{
		{StatusCode: 504, Count: 1, FailureDomain: "edge-us"},
		{StatusCode: 200, Count: 7, FailureDomain: "edge-us"},
		{StatusCode: 503, Count: 1, FailureDomain: "edge-us"},
		{StatusCode: 503, Count: 1, FailureDomain: "edge-us"},
	}
	firstResult, err := EvaluatePolicy(first)
	if err != nil {
		t.Fatalf("evaluate first evidence: %v", err)
	}
	secondResult, err := EvaluatePolicy(second)
	if err != nil {
		t.Fatalf("evaluate second evidence: %v", err)
	}
	if firstResult.Decision.EvidenceHash != secondResult.Decision.EvidenceHash ||
		firstResult.Decision.MatchingSamples != secondResult.Decision.MatchingSamples {
		t.Fatalf("equivalent evidence changed result: first=%+v second=%+v", firstResult.Decision, secondResult.Decision)
	}
}

func TestEvaluatePolicyRejectsMalformedOrFutureEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 29, 1, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		mutate func(*EvaluationInput)
	}{
		{
			name: "future window",
			mutate: func(input *EvaluationInput) {
				input.Evidence.WindowEndedAt = now.Add(time.Minute)
			},
		},
		{
			name: "window exceeds rule",
			mutate: func(input *EvaluationInput) {
				input.Evidence.WindowStartedAt = now.Add(-3 * time.Minute)
			},
		},
		{
			name: "negative count",
			mutate: func(input *EvaluationInput) {
				input.Evidence.RequestOutcomes[0].Count = -1
			},
		},
		{
			name: "unknown rule",
			mutate: func(input *EvaluationInput) {
				input.RuleID = "missing"
			},
		},
	}
	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			input := evaluationFixture(now)
			test.mutate(&input)
			if _, err := EvaluatePolicy(input); !errors.Is(err, ErrInvalidEvaluation) {
				t.Fatalf("expected invalid evaluation, got %v", err)
			}
		})
	}
}

func TestNormalizeObservedIntentRejectsTrustMutationAndIdentityTampering(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 29, 1, 0, 0, 0, time.UTC)
	input := evaluationFixture(now)
	result, err := EvaluatePolicy(input)
	if err != nil {
		t.Fatalf("evaluate policy: %v", err)
	}
	intent, err := NewObservedActionIntent(input.Policy, result)
	if err != nil {
		t.Fatalf("build intent: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*model.AutomationActionIntent)
	}{
		{
			name: "production mutation",
			mutate: func(intent *model.AutomationActionIntent) {
				intent.ProductionMutationAllowed = true
			},
		},
		{
			name: "replay marked trusted",
			mutate: func(intent *model.AutomationActionIntent) {
				intent.Evidence.Trusted = true
			},
		},
		{
			name: "decision sample tamper",
			mutate: func(intent *model.AutomationActionIntent) {
				intent.Decision.MatchingSamples++
			},
		},
		{
			name: "expiry tamper",
			mutate: func(intent *model.AutomationActionIntent) {
				intent.ExpiresAt = intent.ExpiresAt.Add(time.Minute)
			},
		},
		{
			name: "idempotency tamper",
			mutate: func(intent *model.AutomationActionIntent) {
				intent.IdempotencyKey = "sha256:tampered"
			},
		},
	}
	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := intent
			candidate.Evidence = cloneEvaluationEvidence(intent.Evidence)
			candidate.Decision = cloneEvaluationDecision(intent.Decision)
			candidate.RuleSnapshot = cloneAutomationRule(intent.RuleSnapshot)
			test.mutate(&candidate)
			if _, err := NormalizeObservedIntent(candidate); !errors.Is(err, ErrInvalidEvaluation) {
				t.Fatalf("expected tampered intent rejection, got %v", err)
			}
		})
	}
}

func evaluationFixture(now time.Time) EvaluationInput {
	rule := model.AutomationRule{
		ID: "restart-on-unavailable",
		Trigger: model.AutomationTrigger{
			Type:        model.AutomationTriggerRequestMetric,
			Source:      "app_request_outcomes",
			InvariantID: "app.request_unavailability",
			RequestMetric: &model.AutomationRequestMetricSelector{
				Metric:      "http_status",
				Window:      "2m",
				StatusCodes: []int{503, 504},
			},
			RequiredEvidence:      []string{"app_request_outcomes", "app_revision", "app_readiness"},
			MinimumSamples:        3,
			MinimumFailureDomains: 1,
		},
		Action: model.AutomationAction{Type: "restart_app"},
		Safety: model.AutomationSafetyPolicy{
			ActionContractID:       "app.restart",
			GatePolicyID:           "automation.app-restart",
			TTL:                    "5m",
			BlastRadius:            model.GateBlastRadiusPolicy{MaxApps: 1},
			RecoveryCondition:      "application requests and readiness recover",
			RollbackAction:         "restore desired app revision",
			RequiresRollbackTarget: true,
			RequiresAudit:          true,
			RequiresWAL:            true,
			RequiresIdempotencyKey: true,
			RequiresFencingToken:   true,
		},
	}
	return EvaluationInput{
		Policy: model.AutomationPolicy{
			ID:         "automation_policy_123",
			TenantID:   "tenant_123",
			ProjectID:  "project_123",
			Name:       "API recovery",
			Kind:       model.AutomationPolicyKindAppRecovery,
			OwnerType:  model.AutomationOwnerUser,
			Scope:      model.AutomationScope{Type: model.AutomationScopeApp, ID: "app_123"},
			Mode:       model.GatePolicyModeShadow,
			Managed:    false,
			Rules:      []model.AutomationRule{rule},
			Generation: 7,
		},
		RuleID: rule.ID,
		Evidence: model.AutomationEvaluationEvidence{
			CollectedBy:     model.AutomationIntentSourceAdminReplay,
			Trusted:         false,
			WindowStartedAt: now.Add(-2 * time.Minute),
			WindowEndedAt:   now,
			RequestOutcomes: []model.AutomationRequestOutcomeAggregate{
				{StatusCode: 503, Count: 2, FailureDomain: "edge-us"},
				{StatusCode: 504, Count: 1, FailureDomain: "edge-us"},
				{StatusCode: 200, Count: 7, FailureDomain: "edge-us"},
			},
			AppRevision:            "sha256:revision",
			AppReadiness:           "degraded",
			AppReadinessObservedAt: now.Add(-10 * time.Second),
		},
		Now: now,
	}
}
