package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fugue/internal/automation"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handleListAutomationActionIntents(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canReadAutomations(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing automation.read or automation.write scope")
		return
	}
	limit, err := readIntQuery(r, "limit", defaultAutomationActionIntentListLimit)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if limit < 1 || limit > maxAutomationActionIntentListLimit {
		httpx.WriteError(w, http.StatusBadRequest, "limit must be between 1 and 1000")
		return
	}

	tenantID := principal.TenantID
	if principal.IsPlatformAdmin() {
		tenantID = strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	}
	projectID := projectIDForPrincipal(principal, r.URL.Query().Get("project_id"))
	if !principal.IsPlatformAdmin() &&
		strings.TrimSpace(principal.ProjectID) != "" &&
		projectID != principal.ProjectID {
		httpx.WriteError(w, http.StatusForbidden, "project is outside the credential boundary")
		return
	}
	intents, err := s.store.ListAutomationActionIntents(store.AutomationActionIntentFilter{
		TenantID:      tenantID,
		ProjectID:     projectID,
		PolicyID:      strings.TrimSpace(r.URL.Query().Get("policy_id")),
		AppID:         strings.TrimSpace(r.URL.Query().Get("app_id")),
		Source:        strings.TrimSpace(r.URL.Query().Get("source")),
		Status:        strings.TrimSpace(r.URL.Query().Get("status")),
		PlatformAdmin: principal.IsPlatformAdmin(),
		Limit:         limit,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.AutomationActionIntentListResponse{
		Intents:     intents,
		GeneratedAt: time.Now().UTC(),
	})
}

func (s *Server) handleGetAutomationActionIntent(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canReadAutomations(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing automation.read or automation.write scope")
		return
	}
	intent, err := s.store.GetAutomationActionIntent(
		strings.TrimSpace(r.PathValue("intent_id")),
		principal.TenantID,
		principal.IsPlatformAdmin(),
	)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && !principal.AllowsProject(intent.ProjectID) {
		httpx.WriteError(w, http.StatusForbidden, "automation intent is outside the credential boundary")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.AutomationActionIntentResponse{Intent: intent})
}

func (s *Server) handleEvaluateAutomationPolicyReplay(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var request model.EvaluateAutomationPolicyRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	request.PolicyID = strings.TrimSpace(request.PolicyID)
	request.RuleID = strings.TrimSpace(request.RuleID)
	if request.PolicyID == "" || request.RuleID == "" || request.ExpectedGeneration <= 0 {
		httpx.WriteError(w, http.StatusBadRequest, "policy_id, rule_id, and a positive expected_generation are required")
		return
	}
	policy, err := s.store.GetAutomationPolicy(request.PolicyID, "", true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if policy.Generation != request.ExpectedGeneration {
		s.writeStoreError(w, store.ErrConflict)
		return
	}
	if policy.Scope.Type != model.AutomationScopeApp || strings.TrimSpace(policy.Scope.ID) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "automation replay currently requires an app-scoped policy")
		return
	}
	app, err := s.store.GetApp(policy.Scope.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if app.TenantID != policy.TenantID || app.ProjectID != policy.ProjectID {
		s.writeStoreError(w, store.ErrConflict)
		return
	}
	appRevision, err := automation.AppRevisionHash(app.Spec)
	if err != nil {
		s.writeAutomationEvaluationError(w, err)
		return
	}
	readinessObservedAt := app.Status.UpdatedAt.UTC()
	if readinessObservedAt.IsZero() {
		readinessObservedAt = app.UpdatedAt.UTC()
	}
	if readinessObservedAt.IsZero() {
		httpx.WriteError(w, http.StatusConflict, "app readiness evidence is unavailable")
		return
	}
	readiness := strings.TrimSpace(strings.ToLower(app.Status.Phase))
	if readiness == "" {
		readiness = "unknown"
	}
	now := time.Now().UTC()
	result, err := automation.EvaluatePolicy(automation.EvaluationInput{
		Policy: policy,
		RuleID: request.RuleID,
		Evidence: model.AutomationEvaluationEvidence{
			CollectedBy:            model.AutomationIntentSourceAdminReplay,
			Trusted:                false,
			WindowStartedAt:        request.WindowStartedAt,
			WindowEndedAt:          request.WindowEndedAt,
			RequestOutcomes:        request.RequestOutcomes,
			AppRevision:            appRevision,
			AppReadiness:           readiness,
			AppReadinessObservedAt: readinessObservedAt,
		},
		Now: now,
	})
	if err != nil {
		s.writeAutomationEvaluationError(w, err)
		return
	}

	response := model.AutomationEvaluationResponse{Decision: result.Decision}
	if result.Decision.WouldAction {
		intent, err := automation.NewObservedActionIntent(policy, result)
		if err != nil {
			s.writeAutomationEvaluationError(w, err)
			return
		}
		stored, created, err := s.store.CreateAutomationActionIntent(intent)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		response.Intent = &stored
		response.IntentCreated = created
		response.Decision = stored.Decision
	}
	s.appendAutomationEvaluationAudit(principal, policy, response)
	httpx.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) appendAutomationEvaluationAudit(
	principal model.Principal,
	policy model.AutomationPolicy,
	response model.AutomationEvaluationResponse,
) {
	metadata := map[string]string{
		"app_id":                      policy.Scope.ID,
		"project_id":                  policy.ProjectID,
		"policy_generation":           strconv.FormatInt(policy.Generation, 10),
		"rule_id":                     response.Decision.RuleID,
		"mode":                        response.Decision.Mode,
		"matched":                     strconv.FormatBool(response.Decision.Matched),
		"would_action":                strconv.FormatBool(response.Decision.WouldAction),
		"production_mutation_allowed": "false",
		"matching_samples":            strconv.FormatInt(response.Decision.MatchingSamples, 10),
		"failure_domains":             strconv.Itoa(len(response.Decision.FailureDomains)),
		"evidence_hash":               response.Decision.EvidenceHash,
		"source":                      model.AutomationIntentSourceAdminReplay,
		"intent_created":              strconv.FormatBool(response.IntentCreated),
	}
	if response.Intent != nil {
		metadata["intent_id"] = response.Intent.ID
		metadata["idempotency_key"] = response.Intent.IdempotencyKey
	}
	s.appendAudit(
		principal,
		"automation.policy.evaluate_replay",
		"automation_policy",
		policy.ID,
		policy.TenantID,
		metadata,
	)
}

func (s *Server) writeAutomationEvaluationError(w http.ResponseWriter, err error) {
	if errors.Is(err, automation.ErrInvalidEvaluation) {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if s.log != nil {
		s.log.Printf("automation evaluation: %v", err)
	}
	httpx.WriteError(w, http.StatusInternalServerError, "automation evaluation failed")
}

const (
	defaultAutomationActionIntentListLimit = 200
	maxAutomationActionIntentListLimit     = 1000
)
