package api

import (
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
)

func (s *Server) handleListBillingSummaries(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can list billing summaries")
		return
	}
	ids := make([]string, 0)
	seen := map[string]struct{}{}
	for _, raw := range strings.Split(r.URL.Query().Get("tenant_ids"), ",") {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 || len(ids) > 500 {
		httpx.WriteError(w, http.StatusBadRequest, "tenant_ids must contain between 1 and 500 tenant IDs")
		return
	}
	includeUsage, err := readBoolQuery(r, "include_current_usage", true)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	started := time.Now()
	snapshot, err := s.store.GetTenantBillingSnapshot(r.Context(), ids)
	serverTimingFromContext(r.Context()).Add("billing_batch_summary", time.Since(started))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	runtimeTypes := make(map[string]string)
	billings := snapshot.Billings
	if includeUsage {
		started = time.Now()
		for _, runtime := range snapshot.Runtimes {
			runtimeTypes[strings.TrimSpace(runtime.ID)] = runtime.Type
		}
		apps := s.overlayCurrentResourceUsageOnApps(r.Context(), snapshot.Apps)
		for index := range billings {
			billings[index].CurrentUsage = tenantManagedUsageFromSnapshot(billings[index].TenantID, apps, runtimeTypes)
		}
		serverTimingFromContext(r.Context()).Add("billing_batch_usage", time.Since(started))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"billings": billings, "missing_tenant_ids": snapshot.MissingTenantIDs})
}
