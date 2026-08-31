package api

import (
	"fmt"
	"net/http"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

const nodeDeepHealthReportMaxAge = 15 * time.Minute

const nodeDeepHealthReportStaleReason = "report_stale"

func (s *Server) handleListNodeDeepHealth(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	results, err := s.listNodeDeepHealthResults(time.Now().UTC())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.NodeDeepHealthListResponse{
		Results:     results,
		GeneratedAt: time.Now().UTC(),
	})
}

func (s *Server) handleGetNodeDeepHealth(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	result, err := s.store.GetNodeDeepHealthResult(r.PathValue("node_updater_id"))
	if err != nil {
		if err == store.ErrNotFound {
			httpx.WriteError(w, http.StatusNotFound, "node deep health result not found")
			return
		}
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.NodeDeepHealthResponse{Result: projectNodeDeepHealthFreshness(result, time.Now().UTC())})
}

func (s *Server) listNodeDeepHealthResults(now time.Time) ([]model.NodeDeepHealthResult, error) {
	results, err := s.store.ListNodeDeepHealthResults()
	if err != nil {
		return nil, err
	}
	for idx := range results {
		results[idx] = projectNodeDeepHealthFreshness(results[idx], now)
	}
	return results, nil
}

func nodeDeepHealthReportFresh(result model.NodeDeepHealthResult, now time.Time) bool {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	reportedAt := result.ReportedAt.UTC()
	if reportedAt.IsZero() || reportedAt.After(now.Add(30*time.Second)) {
		return false
	}
	return now.Sub(reportedAt) <= nodeDeepHealthReportMaxAge
}

func projectNodeDeepHealthFreshness(result model.NodeDeepHealthResult, now time.Time) model.NodeDeepHealthResult {
	if nodeDeepHealthReportFresh(result, now) {
		return result
	}
	result.OverallStatus = model.NodeDeepHealthStatusWarning
	result.QuarantineState = model.NodeQuarantineStateDegraded
	result.QuarantineReason = nodeDeepHealthReportStaleReason
	result.RecoveryConditions = []string{"await a fresh node-updater deep-health heartbeat"}
	age := "missing"
	if !result.ReportedAt.IsZero() {
		age = fmt.Sprintf("last report is %s old", now.Sub(result.ReportedAt.UTC()).Round(time.Second))
	}
	result.Checks = append(result.Checks, model.NodeDeepHealthCheck{
		Name:      "report_freshness",
		Category:  "heartbeat",
		Status:    model.NodeDeepHealthStatusWarning,
		Expected:  fmt.Sprintf("report age <= %s", nodeDeepHealthReportMaxAge),
		Observed:  age,
		Message:   "stored health evidence is historical and must not be reported as a current pass",
		CheckedAt: now,
	})
	return result
}
