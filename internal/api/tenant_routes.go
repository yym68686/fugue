package api

import (
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

type deleteTenantCleanup struct {
	Namespace                string   `json:"namespace"`
	NamespaceDeleteRequested bool     `json:"namespace_delete_requested"`
	OwnedNodes               int      `json:"owned_nodes"`
	ManagedOwnedNodes        int      `json:"managed_owned_nodes"`
	Warnings                 []string `json:"warnings,omitempty"`
}

func (s *Server) handleDeleteTenant(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}

	tenantID := strings.TrimSpace(r.PathValue("id"))
	if tenantID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "tenant id is required")
		return
	}

	nodes, err := s.store.ListNodes(tenantID, false)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	cleanup := deleteTenantCleanup{
		Namespace:  runtime.NamespaceForTenant(tenantID),
		OwnedNodes: len(nodes),
	}
	for _, node := range nodes {
		if node.Type == model.RuntimeTypeManagedOwned {
			cleanup.ManagedOwnedNodes++
		}
	}

	tenant, err := s.store.DeleteTenant(tenantID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	if err := runtime.DeleteTenantNamespace(tenantID); err == nil {
		cleanup.NamespaceDeleteRequested = true
	} else {
		cleanup.Warnings = append(cleanup.Warnings, "namespace cleanup was not completed automatically: "+err.Error())
	}
	if cleanup.ManagedOwnedNodes > 0 {
		cleanup.Warnings = append(cleanup.Warnings, "managed-owned nodes may still remain joined to the center cluster until k3s agent is uninstalled on the tenant VPS")
	}

	s.appendAudit(principal, "tenant.delete", "tenant", tenant.ID, tenant.ID, map[string]string{
		"name": tenant.Name,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"tenant":  tenant,
		"cleanup": cleanup,
	})
}
