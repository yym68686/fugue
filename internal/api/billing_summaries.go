package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
	"golang.org/x/sync/errgroup"
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
	result := make([]model.TenantBillingSummary, len(ids))
	missing := make([]bool, len(ids))
	started := time.Now()
	var apps []model.App
	runtimeTypes := make(map[string]string)
	if includeUsage {
		var runtimes []model.Runtime
		usageGroup := new(errgroup.Group)
		usageGroup.Go(func() error { var err error; apps, err = s.store.ListApps("", true); return err })
		usageGroup.Go(func() error { var err error; runtimes, err = s.store.ListRuntimes("", true); return err })
		if err := usageGroup.Wait(); err != nil {
			s.writeStoreError(w, err)
			return
		}
		for _, runtime := range runtimes {
			runtimeTypes[strings.TrimSpace(runtime.ID)] = runtime.Type
		}
		apps = s.overlayCurrentResourceUsageOnApps(r.Context(), apps)
		serverTimingFromContext(r.Context()).Add("billing_batch_usage", time.Since(started))
	}
	started = time.Now()
	group := new(errgroup.Group)
	group.SetLimit(8)
	for index, tenantID := range ids {
		index, tenantID := index, tenantID
		group.Go(func() error {
			summary, err := s.store.GetTenantBillingSummary(tenantID)
			if errors.Is(err, store.ErrNotFound) {
				missing[index] = true
				return nil
			}
			if err != nil {
				return err
			}
			if includeUsage {
				summary.CurrentUsage = tenantManagedUsageFromSnapshot(tenantID, apps, runtimeTypes)
			}
			result[index] = summary
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		s.writeStoreError(w, err)
		return
	}
	serverTimingFromContext(r.Context()).Add("billing_batch_summary", time.Since(started))
	billings := make([]model.TenantBillingSummary, 0, len(ids))
	missingIDs := make([]string, 0)
	for index, summary := range result {
		if missing[index] {
			missingIDs = append(missingIDs, ids[index])
		} else {
			billings = append(billings, summary)
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"billings": billings, "missing_tenant_ids": missingIDs})
}
