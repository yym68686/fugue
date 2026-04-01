package api

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handleGetBilling(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	tenantID, ok := s.resolveTenantID(principal, r.URL.Query().Get("tenant_id"))
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot view billing for another tenant")
		return
	}
	if strings.TrimSpace(tenantID) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "tenant_id is required")
		return
	}

	summary, err := s.store.GetTenantBillingSummary(tenantID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	summary.CurrentUsage = s.currentTenantManagedUsage(r.Context(), tenantID, principal.IsPlatformAdmin())
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"billing": summary,
	})
}

func (s *Server) handleUpdateBilling(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("billing.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing billing.write scope")
		return
	}

	var req struct {
		TenantID   string             `json:"tenant_id"`
		ManagedCap model.ResourceSpec `json:"managed_cap"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot update billing for another tenant")
		return
	}
	if strings.TrimSpace(tenantID) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "tenant_id is required")
		return
	}

	summary, err := s.store.UpdateTenantBilling(tenantID, req.ManagedCap)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	summary.CurrentUsage = s.currentTenantManagedUsage(r.Context(), tenantID, principal.IsPlatformAdmin())
	s.appendAudit(principal, "billing.update", "tenant", tenantID, tenantID, map[string]string{
		"cpu_millicores":   strings.TrimSpace(httpxValue(req.ManagedCap.CPUMilliCores)),
		"memory_mebibytes": strings.TrimSpace(httpxValue(req.ManagedCap.MemoryMebibytes)),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"billing": summary,
	})
}

func (s *Server) handleTopUpBilling(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("billing.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing billing.write scope")
		return
	}

	var req struct {
		TenantID    string `json:"tenant_id"`
		AmountCents int64  `json:"amount_cents"`
		Note        string `json:"note"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot top up billing for another tenant")
		return
	}
	if strings.TrimSpace(tenantID) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "tenant_id is required")
		return
	}

	summary, err := s.store.TopUpTenantBilling(tenantID, req.AmountCents, req.Note)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	summary.CurrentUsage = s.currentTenantManagedUsage(r.Context(), tenantID, principal.IsPlatformAdmin())
	s.appendAudit(principal, "billing.top_up", "tenant", tenantID, tenantID, map[string]string{
		"amount_cents": httpxValue(req.AmountCents),
		"note":         strings.TrimSpace(req.Note),
	})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{
		"billing": summary,
	})
}

func (s *Server) handleSetBillingBalance(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin access required")
		return
	}

	var req struct {
		TenantID     string `json:"tenant_id"`
		BalanceCents int64  `json:"balance_cents"`
		Note         string `json:"note"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot update billing for another tenant")
		return
	}
	if strings.TrimSpace(tenantID) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "tenant_id is required")
		return
	}

	metadata := map[string]string{
		"source":     "platform-admin",
		"actor_type": principal.ActorType,
		"actor_id":   principal.ActorID,
	}
	if note := strings.TrimSpace(req.Note); note != "" {
		metadata["note"] = note
	}

	summary, err := s.store.SetTenantBillingBalance(tenantID, req.BalanceCents, metadata)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	summary.CurrentUsage = s.currentTenantManagedUsage(r.Context(), tenantID, true)
	s.appendAudit(principal, "billing.balance.set", "tenant", tenantID, tenantID, map[string]string{
		"balance_cents": httpxValue(req.BalanceCents),
		"note":          strings.TrimSpace(req.Note),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"billing": summary,
	})
}

func (s *Server) currentTenantManagedUsage(ctx context.Context, tenantID string, platformAdmin bool) *model.ResourceUsage {
	apps, err := s.store.ListApps(tenantID, platformAdmin)
	if err != nil {
		return nil
	}
	if platformAdmin {
		filtered := make([]model.App, 0, len(apps))
		for _, app := range apps {
			if app.TenantID == tenantID {
				filtered = append(filtered, app)
			}
		}
		apps = filtered
	}

	runtimes, err := s.store.ListRuntimes(tenantID, platformAdmin)
	if err != nil {
		return nil
	}
	runtimeTypes := make(map[string]string, len(runtimes))
	for _, runtime := range runtimes {
		runtimeTypes[strings.TrimSpace(runtime.ID)] = runtime.Type
	}

	apps = s.overlayCurrentResourceUsageOnApps(ctx, apps)
	accumulator := resourceUsageAccumulator{}
	for _, app := range apps {
		if app.TenantID != tenantID || app.Status.CurrentReplicas <= 0 {
			continue
		}
		if !isManagedBillingRuntimeType(runtimeTypes[strings.TrimSpace(app.Status.CurrentRuntimeID)]) {
			continue
		}
		addCurrentResourceUsage(&accumulator, app.CurrentResourceUsage)
		seenServices := map[string]struct{}{}
		for _, service := range app.BackingServices {
			serviceID := strings.TrimSpace(service.ID)
			if serviceID == "" {
				continue
			}
			if _, exists := seenServices[serviceID]; exists {
				continue
			}
			seenServices[serviceID] = struct{}{}
			if !isManagedBillingBackingService(service) {
				continue
			}
			addCurrentResourceUsage(&accumulator, service.CurrentResourceUsage)
		}
	}
	usage, ok := accumulator.resourceUsage()
	if !ok {
		return nil
	}
	return &usage
}

func addCurrentResourceUsage(accumulator *resourceUsageAccumulator, usage *model.ResourceUsage) {
	if accumulator == nil || usage == nil {
		return
	}
	if usage.CPUMilliCores != nil {
		accumulator.cpuMilliCores += *usage.CPUMilliCores
		accumulator.hasCPU = true
	}
	if usage.MemoryBytes != nil {
		accumulator.memoryBytes += *usage.MemoryBytes
		accumulator.hasMemory = true
	}
	if usage.EphemeralStorageBytes != nil {
		accumulator.ephemeralStorageBytes += *usage.EphemeralStorageBytes
		accumulator.hasEphemeralStorage = true
	}
}

func isManagedBillingRuntimeType(runtimeType string) bool {
	switch strings.TrimSpace(runtimeType) {
	case model.RuntimeTypeManagedShared, model.RuntimeTypeManagedOwned:
		return true
	default:
		return false
	}
}

func isManagedBillingBackingService(service model.BackingService) bool {
	if !strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) {
		return false
	}
	provisioner := strings.TrimSpace(strings.ToLower(service.Provisioner))
	return provisioner == "" || provisioner == model.BackingServiceProvisionerManaged
}

func httpxValue[T ~int64 | ~int](value T) string {
	return strconv.FormatInt(int64(value), 10)
}
