package api

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/automation"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

const (
	automationReadScope  = "automation.read"
	automationWriteScope = "automation.write"
)

func (s *Server) handleListAutomationPolicies(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}

	ownerType := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("owner_type")))
	if ownerType != "" && ownerType != model.AutomationOwnerSystem && ownerType != model.AutomationOwnerUser {
		httpx.WriteError(w, http.StatusBadRequest, "owner_type must be system or user")
		return
	}

	policies := make([]model.AutomationPolicy, 0)
	if ownerType != model.AutomationOwnerUser &&
		strings.TrimSpace(r.URL.Query().Get("tenant_id")) == "" &&
		strings.TrimSpace(r.URL.Query().Get("project_id")) == "" {
		managed, err := s.managedAutomationPolicies()
		if err != nil {
			s.writeAutomationRegistryError(w, err)
			return
		}
		policies = append(policies, managed...)
	}
	if ownerType != model.AutomationOwnerSystem {
		persisted, err := s.store.ListAutomationPolicies(store.AutomationPolicyFilter{
			TenantID:      strings.TrimSpace(r.URL.Query().Get("tenant_id")),
			ProjectID:     strings.TrimSpace(r.URL.Query().Get("project_id")),
			PlatformAdmin: true,
		})
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		policies = append(policies, persisted...)
	}
	sort.SliceStable(policies, func(i, j int) bool { return policies[i].ID < policies[j].ID })
	httpx.WriteJSON(w, http.StatusOK, model.AutomationPolicyListResponse{
		Policies:    policies,
		GeneratedAt: time.Now().UTC(),
	})
}

func (s *Server) handleGetAutomationPolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	policyID := strings.TrimSpace(r.PathValue("policy_id"))
	if strings.HasPrefix(policyID, "system.") {
		policies, err := s.managedAutomationPolicies()
		if err != nil {
			s.writeAutomationRegistryError(w, err)
			return
		}
		for _, policy := range policies {
			if policy.ID == policyID {
				httpx.WriteJSON(w, http.StatusOK, model.AutomationPolicyResponse{Policy: policy})
				return
			}
		}
		httpx.WriteError(w, http.StatusNotFound, "automation policy not found")
		return
	}

	policy, err := s.store.GetAutomationPolicy(policyID, "", true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.AutomationPolicyResponse{Policy: policy})
}

func (s *Server) handleListUserAutomationPolicies(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canReadAutomations(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing automation.read or automation.write scope")
		return
	}

	tenantID := principal.TenantID
	if principal.IsPlatformAdmin() {
		tenantID = strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	}
	projectID := projectIDForPrincipal(principal, r.URL.Query().Get("project_id"))
	if !principal.IsPlatformAdmin() && strings.TrimSpace(principal.ProjectID) != "" && projectID != principal.ProjectID {
		httpx.WriteError(w, http.StatusForbidden, "project is outside the credential boundary")
		return
	}

	policies, err := s.store.ListAutomationPolicies(store.AutomationPolicyFilter{
		TenantID:      tenantID,
		ProjectID:     projectID,
		PlatformAdmin: principal.IsPlatformAdmin(),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.AutomationPolicyListResponse{
		Policies:    policies,
		GeneratedAt: time.Now().UTC(),
	})
}

func (s *Server) handleCreateUserAutomationPolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canWriteAutomations(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing automation.write scope")
		return
	}

	var request model.CreateAutomationPolicyRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	policy, err := s.buildNewUserAutomationPolicy(principal, request)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	created, err := s.store.CreateAutomationPolicy(policy)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAutomationPolicyAudit(principal, "automation.policy.create", created)
	httpx.WriteJSON(w, http.StatusCreated, model.AutomationPolicyResponse{Policy: created})
}

func (s *Server) handleGetUserAutomationPolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canReadAutomations(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing automation.read or automation.write scope")
		return
	}
	policy, ok := s.loadUserAutomationPolicy(w, principal, r.PathValue("policy_id"))
	if !ok {
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.AutomationPolicyResponse{Policy: policy})
}

func (s *Server) handleUpdateUserAutomationPolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canWriteAutomations(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing automation.write scope")
		return
	}
	current, ok := s.loadUserAutomationPolicy(w, principal, r.PathValue("policy_id"))
	if !ok {
		return
	}

	var request model.UpdateAutomationPolicyRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	replacement, err := s.buildUpdatedUserAutomationPolicy(current, request)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	updated, err := s.store.UpdateAutomationPolicy(
		replacement,
		principal.TenantID,
		principal.IsPlatformAdmin(),
		request.ExpectedGeneration,
	)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAutomationPolicyAudit(principal, "automation.policy.update", updated)
	httpx.WriteJSON(w, http.StatusOK, model.AutomationPolicyResponse{Policy: updated})
}

func (s *Server) handleDeleteUserAutomationPolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canWriteAutomations(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing automation.write scope")
		return
	}
	current, ok := s.loadUserAutomationPolicy(w, principal, r.PathValue("policy_id"))
	if !ok {
		return
	}
	expectedGeneration, err := parseAutomationExpectedGeneration(r.URL.Query().Get("expected_generation"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	removed, err := s.store.DeleteAutomationPolicy(
		current.ID,
		principal.TenantID,
		principal.IsPlatformAdmin(),
		expectedGeneration,
	)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAutomationPolicyAudit(principal, "automation.policy.delete", removed)
	httpx.WriteJSON(w, http.StatusOK, model.DeleteAutomationPolicyResponse{
		Deleted: true,
		Policy:  removed,
	})
}

func (s *Server) loadUserAutomationPolicy(
	w http.ResponseWriter,
	principal model.Principal,
	policyID string,
) (model.AutomationPolicy, bool) {
	policy, err := s.store.GetAutomationPolicy(
		strings.TrimSpace(policyID),
		principal.TenantID,
		principal.IsPlatformAdmin(),
	)
	if err != nil {
		s.writeStoreError(w, err)
		return model.AutomationPolicy{}, false
	}
	if !principal.IsPlatformAdmin() && !principal.AllowsProject(policy.ProjectID) {
		httpx.WriteError(w, http.StatusForbidden, "automation policy is outside the credential boundary")
		return model.AutomationPolicy{}, false
	}
	return policy, true
}

func (s *Server) buildNewUserAutomationPolicy(
	principal model.Principal,
	request model.CreateAutomationPolicyRequest,
) (model.AutomationPolicy, error) {
	if strings.TrimSpace(strings.ToLower(request.Kind)) != model.AutomationPolicyKindAppRecovery {
		return model.AutomationPolicy{}, fmt.Errorf("%w: only app_recovery policies are supported", store.ErrInvalidInput)
	}
	request.Scope.Type = strings.TrimSpace(strings.ToLower(request.Scope.Type))
	request.Scope.ID = strings.TrimSpace(request.Scope.ID)
	if request.Scope.Type != model.AutomationScopeApp || request.Scope.ID == "" {
		return model.AutomationPolicy{}, fmt.Errorf("%w: app_recovery policies require an app scope", store.ErrInvalidInput)
	}
	app, err := s.store.GetApp(request.Scope.ID)
	if err != nil {
		return model.AutomationPolicy{}, err
	}
	if !principal.IsPlatformAdmin() && !principalAllowsApp(principal, app) {
		return model.AutomationPolicy{}, store.ErrNotFound
	}

	tenantID := strings.TrimSpace(request.TenantID)
	if tenantID == "" {
		tenantID = app.TenantID
	}
	if !principal.IsPlatformAdmin() && tenantID != principal.TenantID {
		return model.AutomationPolicy{}, fmt.Errorf("%w: tenant is outside the credential boundary", store.ErrInvalidInput)
	}
	if tenantID != app.TenantID {
		return model.AutomationPolicy{}, store.ErrNotFound
	}
	projectID := projectIDForPrincipal(principal, request.ProjectID)
	if projectID == "" {
		projectID = app.ProjectID
	}
	if projectID != app.ProjectID || (!principal.IsPlatformAdmin() && !principal.AllowsProject(projectID)) {
		return model.AutomationPolicy{}, store.ErrNotFound
	}

	rules, err := s.buildUserAutomationRules(request.Rules, request.Scope)
	if err != nil {
		return model.AutomationPolicy{}, err
	}
	return model.AutomationPolicy{
		TenantID:    tenantID,
		ProjectID:   projectID,
		Name:        request.Name,
		Description: request.Description,
		Kind:        model.AutomationPolicyKindAppRecovery,
		OwnerType:   model.AutomationOwnerUser,
		Scope:       request.Scope,
		Mode:        request.Mode,
		Priority:    request.Priority,
		Managed:     false,
		SourceRef:   request.SourceRef,
		Rules:       rules,
		Metadata:    request.Metadata,
	}, nil
}

func (s *Server) buildUpdatedUserAutomationPolicy(
	current model.AutomationPolicy,
	request model.UpdateAutomationPolicyRequest,
) (model.AutomationPolicy, error) {
	if request.ExpectedGeneration <= 0 {
		return model.AutomationPolicy{}, fmt.Errorf("%w: expected_generation must be positive", store.ErrInvalidInput)
	}
	rules, err := s.buildUserAutomationRules(request.Rules, current.Scope)
	if err != nil {
		return model.AutomationPolicy{}, err
	}
	current.Name = request.Name
	current.Description = request.Description
	current.Mode = request.Mode
	current.Priority = request.Priority
	current.SourceRef = request.SourceRef
	current.Rules = rules
	current.Metadata = request.Metadata
	return current, nil
}

func (s *Server) buildUserAutomationRules(
	inputs []model.AutomationRuleInput,
	scope model.AutomationScope,
) ([]model.AutomationRule, error) {
	if len(inputs) == 0 || len(inputs) > 16 {
		return nil, fmt.Errorf("%w: policies require between 1 and 16 rules", store.ErrInvalidInput)
	}
	gates := s.gatePolicyRegistry()
	rules := make([]model.AutomationRule, 0, len(inputs))
	for _, input := range inputs {
		contract, ok := platformcontrol.AutomaticActionContractByActionType(strings.TrimSpace(input.Action.Type))
		if !ok || contract.ID != platformcontrol.ActionContractAppRestart {
			return nil, fmt.Errorf("%w: unsupported automation action", store.ErrInvalidInput)
		}
		if contract.Scope != scope.Type {
			return nil, fmt.Errorf("%w: action contract scope does not match policy scope", store.ErrInvalidInput)
		}
		gate, ok := automationGatePolicyByID(gates, contract.GatePolicyID)
		if !ok || gate.Scope != contract.Scope {
			return nil, fmt.Errorf("%w: automation action safety gate is unavailable", store.ErrInvalidInput)
		}

		trigger, err := buildAppRestartTrigger(input.Trigger, contract, gate)
		if err != nil {
			return nil, err
		}
		parameters, err := validateAppRestartParameters(input.Action.Parameters)
		if err != nil {
			return nil, err
		}
		rules = append(rules, model.AutomationRule{
			ID:          input.ID,
			Description: input.Description,
			Trigger:     trigger,
			Action: model.AutomationAction{
				Type:       contract.ActionType,
				Parameters: parameters,
			},
			Safety: model.AutomationSafetyPolicy{
				ActionContractID:       contract.ID,
				GatePolicyID:           contract.GatePolicyID,
				TTL:                    contract.TTL,
				BlastRadius:            tighterGateBlastRadius(contract.BlastRadius, gate.BlastRadius),
				RecoveryCondition:      contract.RecoveryCondition,
				RollbackAction:         contract.RollbackAction,
				RequiresRollbackTarget: contract.RequiresRollbackTarget,
				RequiresAudit:          contract.RequiresAudit,
				RequiresWAL:            contract.RequiresWAL,
				RequiresIdempotencyKey: contract.RequiresIdempotencyKey,
				RequiresFencingToken:   contract.RequiresFencingToken,
			},
		})
	}
	return rules, nil
}

func buildAppRestartTrigger(
	input model.AutomationTriggerInput,
	contract model.AutomaticActionContract,
	gate model.GatePolicy,
) (model.AutomationTrigger, error) {
	input.Type = strings.TrimSpace(strings.ToLower(input.Type))
	input.Source = strings.TrimSpace(input.Source)
	if input.Type != model.AutomationTriggerRequestMetric ||
		input.Source != contract.EvidenceSource ||
		input.RequestMetric == nil {
		return model.AutomationTrigger{}, fmt.Errorf("%w: restart_app requires an app_request_outcomes request-metric trigger", store.ErrInvalidInput)
	}
	selector := *input.RequestMetric
	selector.Metric = strings.TrimSpace(strings.ToLower(selector.Metric))
	selector.Window = strings.TrimSpace(selector.Window)
	if selector.Metric != "http_status" {
		return model.AutomationTrigger{}, fmt.Errorf("%w: restart_app only supports the http_status metric", store.ErrInvalidInput)
	}
	window, err := time.ParseDuration(selector.Window)
	if err != nil || window < time.Second || window > 15*time.Minute {
		return model.AutomationTrigger{}, fmt.Errorf("%w: restart_app metric window must be between 1s and 15m", store.ErrInvalidInput)
	}
	if len(selector.StatusCodes) == 0 {
		return model.AutomationTrigger{}, fmt.Errorf("%w: restart_app requires at least one server-error status code", store.ErrInvalidInput)
	}
	if len(selector.StatusCodes) > 100 {
		return model.AutomationTrigger{}, fmt.Errorf("%w: restart_app accepts at most 100 status codes", store.ErrInvalidInput)
	}
	if len(selector.ErrorClasses) != 0 {
		return model.AutomationTrigger{}, fmt.Errorf("%w: restart_app http_status triggers do not accept error classes", store.ErrInvalidInput)
	}
	for _, statusCode := range selector.StatusCodes {
		if statusCode < 500 || statusCode > 599 {
			return model.AutomationTrigger{}, fmt.Errorf("%w: restart_app status codes must be between 500 and 599", store.ErrInvalidInput)
		}
	}
	if input.MinimumSamples < 0 ||
		input.MinimumSamples > 10_000 ||
		input.MinimumFailureDomains < 0 ||
		input.MinimumFailureDomains > 1_000 {
		return model.AutomationTrigger{}, fmt.Errorf("%w: automation evidence limits are too large", store.ErrInvalidInput)
	}
	selector.StatusCodes = uniqueSortedInts(selector.StatusCodes)
	selector.ErrorClasses = uniqueSortedStrings(selector.ErrorClasses)
	return model.AutomationTrigger{
		Type:                  model.AutomationTriggerRequestMetric,
		Source:                contract.EvidenceSource,
		InvariantID:           contract.TriggerInvariant,
		RequestMetric:         &selector,
		RequiredEvidence:      uniqueSortedStrings(append(append([]string(nil), contract.RequiredEvidence...), input.RequiredEvidence...)),
		MinimumSamples:        gateMaxInt(contract.MinimumSamples, gateMaxInt(gate.MinimumSamples, input.MinimumSamples)),
		MinimumFailureDomains: gateMaxInt(contract.MinimumFailureDomains, gateMaxInt(gate.MinimumFailureDomains, input.MinimumFailureDomains)),
	}, nil
}

func validateAppRestartParameters(parameters map[string]string) (map[string]string, error) {
	if len(parameters) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(parameters))
	for key, value := range parameters {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "reason" || value == "" || len(value) > 500 {
			return nil, fmt.Errorf("%w: restart_app only accepts a non-empty reason parameter of at most 500 bytes", store.ErrInvalidInput)
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("%w: restart_app parameters contain duplicate normalized keys", store.ErrInvalidInput)
		}
		out[key] = value
	}
	return out, nil
}

func automationGatePolicyByID(policies []model.GatePolicy, id string) (model.GatePolicy, bool) {
	for _, policy := range policies {
		if policy.ID == id {
			return policy, true
		}
	}
	return model.GatePolicy{}, false
}

func canReadAutomations(principal model.Principal) bool {
	return principal.IsPlatformAdmin() ||
		principal.HasScope(automationReadScope) ||
		principal.HasScope(automationWriteScope)
}

func canWriteAutomations(principal model.Principal) bool {
	return principal.IsPlatformAdmin() || principal.HasScope(automationWriteScope)
}

func parseAutomationExpectedGeneration(raw string) (int64, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || value <= 0 {
		return 0, errors.New("expected_generation must be a positive integer")
	}
	return value, nil
}

func (s *Server) appendAutomationPolicyAudit(
	principal model.Principal,
	action string,
	policy model.AutomationPolicy,
) {
	s.appendAudit(principal, action, "automation_policy", policy.ID, policy.TenantID, map[string]string{
		"app_id":     policy.Scope.ID,
		"generation": strconv.FormatInt(policy.Generation, 10),
		"mode":       policy.Mode,
		"project_id": policy.ProjectID,
	})
}

func uniqueSortedInts(values []int) []int {
	if len(values) == 0 {
		return nil
	}
	out := append([]int(nil), values...)
	sort.Ints(out)
	writeIndex := 0
	for _, value := range out {
		if writeIndex > 0 && out[writeIndex-1] == value {
			continue
		}
		out[writeIndex] = value
		writeIndex++
	}
	return out[:writeIndex]
}

func (s *Server) managedAutomationPolicies() ([]model.AutomationPolicy, error) {
	return automation.BuildManagedPolicies(
		platformcontrol.AutomaticActionContracts(),
		s.gatePolicyRegistry(),
	)
}

func (s *Server) writeAutomationRegistryError(w http.ResponseWriter, err error) {
	if s.log != nil {
		s.log.Printf("build automation registry: %v", err)
	}
	httpx.WriteError(w, http.StatusInternalServerError, "automation registry unavailable")
}
