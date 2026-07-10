package platformcontrol

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestActionSafetyEvaluatorShadowNeverMutatesProduction(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 6, 0, 0, 0, time.UTC)
	evaluator := actionEvaluatorFixture(now, model.GatePolicyModeShadow)
	request := passingActionRequest(now)
	decision := evaluator.Evaluate(request)
	if !decision.Pass || decision.Allowed || !decision.WouldAction || decision.ProductionMutationAllowed {
		t.Fatalf("shadow action must only produce would_action: %+v", decision)
	}
}

func TestActionSafetyEvaluatorEnforcedAllowsBoundedAction(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 6, 0, 0, 0, time.UTC)
	evaluator := actionEvaluatorFixture(now, model.GatePolicyModeEnforced)
	decision := evaluator.Evaluate(passingActionRequest(now))
	if !decision.Pass || !decision.Allowed || decision.WouldAction || !decision.ProductionMutationAllowed {
		t.Fatalf("enforced bounded action should pass: %+v", decision)
	}
	if decision.ExpiresAt == nil || !decision.ExpiresAt.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("expected bounded action expiry, got %+v", decision.ExpiresAt)
	}
}

func TestActionSafetyEvaluatorRejectsUnknownEvidenceAndBlastRadius(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 6, 0, 0, 0, time.UTC)
	evaluator := actionEvaluatorFixture(now, model.GatePolicyModeEnforced)
	request := passingActionRequest(now)
	request.Evidence[0].State = model.InvariantEvidenceStateUnknown
	request.CandidateCounts["hostname"] = 0
	decision := evaluator.Evaluate(request)
	if decision.Pass || decision.Allowed || decision.ProductionMutationAllowed {
		t.Fatalf("unknown evidence and zero eligible targets must fail closed: %+v", decision)
	}
	assertActionViolation(t, decision, "evidence.unknown")
	assertActionViolation(t, decision, "blast_radius.exceeded")
}

func TestActionSafetyEvaluatorRejectsGlobalKillSwitchAndCanaryEscape(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 6, 0, 0, 0, time.UTC)
	evaluator := actionEvaluatorFixture(now, model.GatePolicyModeCanary)
	evaluator.LookupEnv = func(name string) string {
		if name == "FUGUE_AUTONOMY_KILL_SWITCH" {
			return "true"
		}
		return "true"
	}
	request := passingActionRequest(now)
	request.CanaryScopeMatch = false
	decision := evaluator.Evaluate(request)
	if decision.Pass || decision.Allowed {
		t.Fatalf("kill switch and canary escape must be rejected: %+v", decision)
	}
	assertActionViolation(t, decision, "gate_policy.outside_canary_scope")
	assertActionViolation(t, decision, "kill_switch.global")
}

func TestActionSafetyEvaluatorSeparatesEnableAndKillSwitchSemantics(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 6, 0, 0, 0, time.UTC)

	disabled := actionEvaluatorFixture(now, model.GatePolicyModeEnforced)
	disabled.LookupEnv = func(name string) string {
		switch name {
		case "FUGUE_TEST_ACTION_ENABLED":
			return "false"
		case "FUGUE_AUTONOMY_KILL_SWITCH", "FUGUE_TEST_ACTION_KILL_SWITCH":
			return "false"
		default:
			return ""
		}
	}
	disabledDecision := disabled.Evaluate(passingActionRequest(now))
	assertActionViolation(t, disabledDecision, "enable_switch.action")
	if disabledDecision.ProductionMutationAllowed {
		t.Fatalf("disabled enable switch must block production mutation: %+v", disabledDecision)
	}

	killed := actionEvaluatorFixture(now, model.GatePolicyModeEnforced)
	killed.LookupEnv = func(name string) string {
		switch name {
		case "FUGUE_TEST_ACTION_ENABLED":
			return "true"
		case "FUGUE_TEST_ACTION_KILL_SWITCH":
			return "true"
		case "FUGUE_AUTONOMY_KILL_SWITCH":
			return "false"
		default:
			return ""
		}
	}
	killedDecision := killed.Evaluate(passingActionRequest(now))
	assertActionViolation(t, killedDecision, "kill_switch.action")
	if killedDecision.ProductionMutationAllowed {
		t.Fatalf("active action kill switch must block production mutation: %+v", killedDecision)
	}
}

func TestActionSafetyEvaluatorRequiresAuditWALIdempotencyAndFencing(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 6, 0, 0, 0, time.UTC)
	evaluator := actionEvaluatorFixture(now, model.GatePolicyModeEnforced)
	request := passingActionRequest(now)
	request.AuditReady = false
	request.WALReady = false
	request.IdempotencyKey = ""
	request.FencingToken = 0
	decision := evaluator.Evaluate(request)
	for _, code := range []string{
		"request.audit_not_ready",
		"request.wal_not_ready",
		"request.idempotency_key_missing",
		"request.fencing_token_missing",
	} {
		assertActionViolation(t, decision, code)
	}
}

func TestActionSafetyEvaluatorRequiresRollbackTargetAndHumanApproval(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 6, 0, 0, 0, time.UTC)
	evaluator := actionEvaluatorFixture(now, model.GatePolicyModeEnforced)
	evaluator.Contracts[0].RequiresRollbackTarget = true
	evaluator.Contracts[0].HumanApprovalBoundary = "operator approval is required for this action class"
	request := passingActionRequest(now)
	request.RollbackTarget = ""
	request.HumanApproved = false
	decision := evaluator.Evaluate(request)
	assertActionViolation(t, decision, "request.rollback_target_missing")
	assertActionViolation(t, decision, "request.human_approval_required")
}

func TestActionSafetyEvaluatorRejectsUnknownContractAndExcessiveTTL(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 6, 0, 0, 0, time.UTC)
	evaluator := actionEvaluatorFixture(now, model.GatePolicyModeEnforced)

	unknown := passingActionRequest(now)
	unknown.ContractID = "missing"
	unknownDecision := evaluator.Evaluate(unknown)
	assertActionViolation(t, unknownDecision, "contract.not_found")

	excessiveTTL := passingActionRequest(now)
	excessiveTTL.TTL = "30m"
	ttlDecision := evaluator.Evaluate(excessiveTTL)
	assertActionViolation(t, ttlDecision, "action.invalid_ttl")
}

func actionEvaluatorFixture(now time.Time, mode string) ActionSafetyEvaluator {
	contract := model.AutomaticActionContract{
		ID:                     "test.action",
		ActionType:             "test_action",
		Scope:                  model.GatePolicyScopeHostname,
		TriggerInvariant:       "test.invariant",
		RequiredEvidence:       []string{"test.invariant"},
		GatePolicyID:           "test.gate",
		BlastRadius:            model.GateBlastRadiusPolicy{PreserveMinEligibleEdgesPerHost: 1, MaxEdgesPerGroup: 1},
		TTL:                    "5m",
		MinimumSamples:         2,
		MinimumFailureDomains:  2,
		SoakMinDuration:        "1m",
		RecoveryCondition:      "healthy",
		RollbackAction:         "restore",
		EnableEnv:              "FUGUE_TEST_ACTION_ENABLED",
		KillSwitchEnv:          "FUGUE_TEST_ACTION_KILL_SWITCH",
		RunbookRef:             "docs/runbooks/automatic-repair-safety.md",
		AllowedModes:           []string{model.GatePolicyModeShadow, model.GatePolicyModeCanary, model.GatePolicyModeEnforced},
		RequiresAudit:          true,
		RequiresWAL:            true,
		RequiresIdempotencyKey: true,
		RequiresFencingToken:   true,
	}
	policy := model.GatePolicy{
		ID:                    "test.gate",
		Mode:                  mode,
		Scope:                 model.GatePolicyScopeHostname,
		MinimumSamples:        2,
		MinimumFailureDomains: 2,
		SoakMinDuration:       "1m",
		BlastRadius:           model.GateBlastRadiusPolicy{PreserveMinEligibleEdgesPerHost: 1, MaxEdgesPerGroup: 1},
	}
	return ActionSafetyEvaluator{
		Contracts: []model.AutomaticActionContract{contract},
		Policies:  []model.GatePolicy{policy},
		Now:       func() time.Time { return now },
		LookupEnv: func(name string) string {
			if name == "FUGUE_AUTONOMY_KILL_SWITCH" || name == "FUGUE_TEST_ACTION_KILL_SWITCH" {
				return "false"
			}
			return "true"
		},
	}
}

func passingActionRequest(now time.Time) model.ActionSafetyRequest {
	soakStartedAt := now.Add(-2 * time.Minute)
	return model.ActionSafetyRequest{
		ActionType:       "test_action",
		ContractID:       "test.action",
		TriggerInvariant: "test.invariant",
		Scope:            model.GatePolicyScopeHostname,
		Subject:          "api.example.test",
		Evidence: []model.ActionSafetyEvidence{{
			ID:            "test.invariant",
			State:         model.InvariantEvidenceStatePass,
			Source:        "probe-a",
			FailureDomain: "provider-a",
			ObservedAt:    now.Add(-10 * time.Second),
		}},
		FailureDomains:   []string{"provider-a", "provider-b"},
		SampleCount:      3,
		SoakStartedAt:    &soakStartedAt,
		TTL:              "5m",
		CurrentCounts:    map[string]int{"hostname": 2},
		CandidateCounts:  map[string]int{"hostname": 1},
		IdempotencyKey:   "idem-1",
		FencingToken:     1,
		AuditReady:       true,
		WALReady:         true,
		CanaryScopeMatch: true,
	}
}

func assertActionViolation(t *testing.T, decision model.ActionSafetyDecision, code string) {
	t.Helper()
	for _, violation := range decision.Violations {
		if violation.Code == code {
			return
		}
	}
	t.Fatalf("expected violation %s, got %+v", code, decision.Violations)
}
