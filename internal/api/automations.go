package api

import (
	"net/http"
	"strings"
	"time"

	"fugue/internal/automation"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func (s *Server) handleListAutomationPolicies(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	policies, err := s.managedAutomationPolicies()
	if err != nil {
		s.writeAutomationRegistryError(w, err)
		return
	}
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
	policies, err := s.managedAutomationPolicies()
	if err != nil {
		s.writeAutomationRegistryError(w, err)
		return
	}
	policyID := strings.TrimSpace(r.PathValue("policy_id"))
	for _, policy := range policies {
		if policy.ID == policyID {
			httpx.WriteJSON(w, http.StatusOK, model.AutomationPolicyResponse{Policy: policy})
			return
		}
	}
	httpx.WriteError(w, http.StatusNotFound, "automation policy not found")
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
