package api

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

const automationDispatchLoopActorID = "automation-shadow-control-loop"

// prepareAutomationActionDispatch materializes the immutable evaluation result
// into the durable action WAL. It is deliberately side-effect free with
// respect to applications: the only write is the dispatch ledger row.
func (s *Server) prepareAutomationActionDispatch(
	intent model.AutomationActionIntent,
	observationLayer string,
) (model.AutomationActionDispatch, bool, error) {
	if s == nil || s.store == nil {
		return model.AutomationActionDispatch{}, false, errors.New("automation dispatch store is unavailable")
	}
	if intent.Source != model.AutomationIntentSourceControlLoop || !intent.Evidence.Trusted {
		return model.AutomationActionDispatch{}, false, fmt.Errorf(
			"automation intent %s is not trusted control-loop evidence",
			intent.ID,
		)
	}
	if existing, err := s.store.GetAutomationActionDispatchByIntent(intent.ID); err == nil {
		if err := store.ValidateAutomationActionDispatchIntentForReplay(intent, existing); err != nil {
			return model.AutomationActionDispatch{}, false, err
		}
		return existing, false, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return model.AutomationActionDispatch{}, false, err
	}

	rule := intent.RuleSnapshot
	contractID := strings.TrimSpace(rule.Safety.ActionContractID)
	contract, ok := platformcontrol.AutomaticActionContractByID(contractID)
	if !ok {
		return model.AutomationActionDispatch{}, false, fmt.Errorf(
			"automation intent %s references unknown action contract %q",
			intent.ID,
			contractID,
		)
	}
	if strings.TrimSpace(rule.Action.Type) != strings.TrimSpace(contract.ActionType) {
		return model.AutomationActionDispatch{}, false, fmt.Errorf(
			"automation intent %s action type %q does not match contract %q",
			intent.ID,
			rule.Action.Type,
			contract.ActionType,
		)
	}

	gatePolicy, ok := gatePolicyByID(s.gatePolicyRegistry(), contract.GatePolicyID)
	if !ok {
		return model.AutomationActionDispatch{}, false, fmt.Errorf(
			"automation intent %s references unknown gate policy %q",
			intent.ID,
			contract.GatePolicyID,
		)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	evidenceExpiry := intent.ExpiresAt.UTC()
	if windowExpiry := intent.Evidence.WindowEndedAt.UTC().Add(5 * time.Minute); windowExpiry.Before(evidenceExpiry) {
		evidenceExpiry = windowExpiry
	}
	if evidenceExpiry.IsZero() {
		evidenceExpiry = now
	}
	evidenceSource := strings.TrimSpace(intent.Evidence.CollectedBy)
	if evidenceSource == "" {
		evidenceSource = intent.Source
	}
	evidence := []model.ActionSafetyEvidence{
		{
			ID:         "app_request_outcomes",
			State:      automationDispatchEvidenceState(intent.Decision.MatchingSamples > 0 && intent.Evidence.Trusted),
			Source:     evidenceSource,
			ObservedAt: intent.Evidence.WindowEndedAt.UTC(),
			ExpiresAt:  automationDispatchTimePtr(evidenceExpiry),
			Hash:       intent.EvidenceHash,
			Metadata: map[string]string{
				"observation_layer": observationLayer,
				"window_started_at": intent.Evidence.WindowStartedAt.UTC().Format(time.RFC3339Nano),
				"window_ended_at":   intent.Evidence.WindowEndedAt.UTC().Format(time.RFC3339Nano),
			},
		},
		{
			ID:         "app_revision",
			State:      automationDispatchEvidenceState(intent.Evidence.AppRevision != ""),
			Source:     "fugue-app-store",
			ObservedAt: intent.Evidence.AppReadinessObservedAt.UTC(),
			ExpiresAt:  automationDispatchTimePtr(now.Add(5 * time.Minute)),
			Hash:       intent.Evidence.AppRevision,
		},
		{
			ID:         "app_readiness",
			State:      automationDispatchEvidenceState(intent.Evidence.AppReadiness != "" && intent.Evidence.AppReadiness != "unknown"),
			Source:     "fugue-app-store",
			ObservedAt: intent.Evidence.AppReadinessObservedAt.UTC(),
			ExpiresAt:  automationDispatchTimePtr(now.Add(2 * time.Minute)),
			Metadata: map[string]string{
				"phase": intent.Evidence.AppReadiness,
			},
		},
	}
	fencingToken := int64(1)
	safetyRequest := model.ActionSafetyRequest{
		ActionType:       contract.ActionType,
		ContractID:       contract.ID,
		TriggerInvariant: contract.TriggerInvariant,
		Scope:            contract.Scope,
		Subject:          intent.Scope.ID,
		Evidence:         evidence,
		CurrentMode:      intent.Mode,
		CurrentCounts:    map[string]int{intent.Scope.ID: 1},
		CandidateCounts:  map[string]int{intent.Scope.ID: 1},
		FailureDomains:   append([]string(nil), intent.Decision.FailureDomains...),
		SampleCount:      int(intent.Decision.MatchingSamples),
		SoakStartedAt:    gatePolicy.SoakStartedAt,
		TTL:              rule.Safety.TTL,
		RollbackTarget:   intent.RollbackTarget,
		RequestedBy:      "system:" + automationDispatchLoopActorID,
		IdempotencyKey:   intent.IdempotencyKey,
		FencingToken:     fencingToken,
		AuditReady:       true,
		WALReady:         true,
		CanaryScopeMatch: true,
	}
	decision := platformcontrol.NewActionSafetyEvaluator(
		platformcontrol.AutomaticActionContracts(),
		s.gatePolicyRegistry(),
	).Evaluate(safetyRequest)
	dispatchExpiresAt := intent.ExpiresAt.UTC()
	if decision.ExpiresAt != nil &&
		(dispatchExpiresAt.IsZero() || decision.ExpiresAt.Before(dispatchExpiresAt)) {
		dispatchExpiresAt = decision.ExpiresAt.UTC()
	}

	dispatch := model.AutomationActionDispatch{
		IntentID:         intent.ID,
		TenantID:         intent.TenantID,
		ProjectID:        intent.ProjectID,
		PolicyID:         intent.PolicyID,
		PolicyGeneration: intent.PolicyGeneration,
		RuleID:           intent.RuleID,
		Scope:            intent.Scope,
		ActionType:       contract.ActionType,
		ContractID:       contract.ID,
		TriggerInvariant: contract.TriggerInvariant,
		Subject:          intent.Scope.ID,
		SourceGeneration: intent.Evidence.AppRevision,
		RollbackTarget:   intent.RollbackTarget,
		IdempotencyKey:   intent.IdempotencyKey,
		SafetyDecision:   decision,
		ExpiresAt:        dispatchExpiresAt,
	}
	stored, created, err := s.store.CreateAutomationActionDispatch(dispatch)
	if err != nil {
		return model.AutomationActionDispatch{}, false, err
	}
	if created {
		s.appendAutomationDispatchAudit(intent, stored, observationLayer)
	}
	return stored, created, nil
}

func automationDispatchEvidenceState(pass bool) string {
	if pass {
		return model.InvariantEvidenceStatePass
	}
	return model.InvariantEvidenceStateUnknown
}

func (s *Server) appendAutomationDispatchAudit(
	intent model.AutomationActionIntent,
	dispatch model.AutomationActionDispatch,
	observationLayer string,
) {
	s.appendAudit(
		model.Principal{
			ActorType: model.ActorTypeSystem,
			ActorID:   automationDispatchLoopActorID,
			TenantID:  intent.TenantID,
			ProjectID: intent.ProjectID,
			AppID:     intent.Scope.ID,
		},
		"automation.action_dispatch.prepare",
		"automation_action_dispatch",
		dispatch.ID,
		intent.TenantID,
		map[string]string{
			"intent_id":                   intent.ID,
			"app_id":                      intent.Scope.ID,
			"action_type":                 dispatch.ActionType,
			"contract_id":                 dispatch.ContractID,
			"trigger_invariant":           dispatch.TriggerInvariant,
			"status":                      dispatch.Status,
			"effective_mode":              dispatch.SafetyDecision.EffectiveMode,
			"safety_pass":                 strconv.FormatBool(dispatch.SafetyDecision.Pass),
			"production_mutation_allowed": strconv.FormatBool(dispatch.SafetyDecision.ProductionMutationAllowed),
			"fencing_token":               strconv.FormatInt(dispatch.FencingToken, 10),
			"wal_hash":                    dispatch.WALHash,
			"observation_layer":           observationLayer,
			"failure_reasons":             strconv.Itoa(len(dispatch.SafetyDecision.Violations)),
		},
	)
}

func automationDispatchTimePtr(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}
