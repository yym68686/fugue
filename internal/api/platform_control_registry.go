package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func (s *Server) handleListInvariantDefinitions(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.InvariantDefinitionListResponse{
		Invariants:  platformcontrol.InvariantDefinitions(),
		GeneratedAt: time.Now().UTC(),
	})
}

func (s *Server) handleGetInvariantDefinition(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	definition, ok := platformcontrol.InvariantDefinitionByID(r.PathValue("invariant_id"))
	if !ok {
		httpx.WriteError(w, http.StatusNotFound, "invariant not found")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.InvariantDefinitionResponse{Invariant: definition})
}

func (s *Server) handleGetPlatformControlInventory(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	inventory, err := s.buildPlatformControlInventory()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.PlatformControlInventoryResponse{Inventory: inventory})
}

func (s *Server) handleListAutomaticActionContracts(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.AutomaticActionContractListResponse{
		Contracts:   platformcontrol.AutomaticActionContracts(),
		GeneratedAt: time.Now().UTC(),
	})
}

func (s *Server) handleGetAutomaticActionContract(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	contract, ok := platformcontrol.AutomaticActionContractByID(r.PathValue("contract_id"))
	if !ok {
		httpx.WriteError(w, http.StatusNotFound, "automatic action contract not found")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.AutomaticActionContractResponse{Contract: contract})
}

func (s *Server) handleEvaluateActionSafety(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var request model.ActionSafetyRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(request.RequestedBy) == "" {
		request.RequestedBy = principal.ActorType + ":" + principal.ActorID
	}
	evaluator := platformcontrol.NewActionSafetyEvaluator(
		platformcontrol.AutomaticActionContracts(),
		s.gatePolicyRegistry(),
	)
	decision := evaluator.Evaluate(request)
	s.appendAudit(principal, "platform.action_safety.evaluate", "automatic_action_contract", decision.ContractID, "", map[string]string{
		"subject":             decision.Subject,
		"mode":                decision.EffectiveMode,
		"pass":                fmt.Sprintf("%t", decision.Pass),
		"allowed":             fmt.Sprintf("%t", decision.Allowed),
		"would_action":        fmt.Sprintf("%t", decision.WouldAction),
		"production_mutation": fmt.Sprintf("%t", decision.ProductionMutationAllowed),
		"violations":          fmt.Sprintf("%d", len(decision.Violations)),
	})
	httpx.WriteJSON(w, http.StatusOK, model.ActionSafetyDecisionResponse{Decision: decision})
}

func (s *Server) buildPlatformControlInventory() (model.PlatformControlInventory, error) {
	policy, err := s.activeReleaseSignalPolicy()
	if err != nil {
		return model.PlatformControlInventory{}, err
	}
	return model.PlatformControlInventory{
		GeneratedAt:      time.Now().UTC(),
		ArtifactKinds:    platformArtifactKindList(),
		Consumers:        platformcontrol.ConsumerContracts(),
		GatePolicies:     s.gatePolicyRegistry(),
		AutomaticActions: platformcontrol.AutomaticActionContracts(),
		AutonomyControls: platformAutonomyControlsFromEnv(),
		ReleaseSignals:   policy.Signals,
		SyntheticProbes:  platformcontrol.SyntheticProbes(),
		LKGPolicies:      platformcontrol.LKGPolicies(),
		Mechanisms:       platformcontrol.DefaultMechanisms(),
	}, nil
}
