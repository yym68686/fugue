package api

import (
	"context"
	"net/http"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/pgretire"
	"fugue/internal/store"
)

func (s *Server) handleRetireBackingServicePostgresSource(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, 403, "platform administrator required")
		return
	}
	svc, ok := s.loadAuthorizedBackingService(w, r, principal)
	if !ok {
		return
	}
	if !isMigratableBackingService(svc) {
		httpx.WriteError(w, 409, "retirement requires managed PostgreSQL")
		return
	}
	var req struct {
		Apply     bool   `json:"apply"`
		SourceUID string `json:"expected_source_uid"`
		PlanHash  string `json:"expected_plan_hash"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, 400, err.Error())
		return
	}
	if req.Apply && (req.SourceUID == "" || req.PlanHash == "") {
		httpx.WriteError(w, 400, "apply requires expected_source_uid and expected_plan_hash from a preceding plan")
		return
	}
	app, err := s.backingServiceSwitchoverApp(svc)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	target, err := store.PostgresImageMigrationTargetForApp(app, svc.ID)
	if err != nil {
		httpx.WriteError(w, 409, "source retirement requires an active single-instance service and exactly one bound consumer in the same tenant/project")
		return
	}
	// Use the same normalized service snapshot as admission and completion.
	svc = *target.Service
	active, err := s.store.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	for _, op := range active {
		if op.Status != model.OperationStatusPending && op.Status != model.OperationStatusRunning && op.Status != model.OperationStatusWaitingAgent {
			continue
		}
		if req.Apply && op.Type == pgretire.OperationType && op.ServiceID == svc.ID && op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil && op.DesiredSpec.Postgres.SourceRetirement != nil {
			plan := *op.DesiredSpec.Postgres.SourceRetirement
			if plan.SourceUID == req.SourceUID && plan.PlanHash == req.PlanHash && plan.ConfigHash == pgretire.ConfigHash(app, svc) {
				httpx.WriteJSON(w, 202, map[string]any{"plan": plan, "dry_run": false, "operation": redactOperationForDebugBundle(op)})
				return
			}
		}
		httpx.WriteError(w, 409, "an app/database operation is active; source retirement was not queued")
		return
	}
	// A lost HTTP response may be retried after the exact operation completed.
	// Return its durable result rather than attempting to rediscover a deleted source.
	if req.Apply {
		for _, op := range active {
			if op.Status == model.OperationStatusCompleted && op.Type == pgretire.OperationType && op.ServiceID == svc.ID && op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil && op.DesiredSpec.Postgres.SourceRetirement != nil {
				p := *op.DesiredSpec.Postgres.SourceRetirement
				if p.SourceUID == req.SourceUID && p.PlanHash == req.PlanHash && p.ConfigHash == pgretire.ConfigHash(app, svc) {
					httpx.WriteJSON(w, 202, map[string]any{"plan": p, "dry_run": false, "operation": redactOperationForDebugBundle(op)})
					return
				}
			}
		}
	}
	factory := s.newManagedAppStatusClient
	if factory == nil {
		factory = newManagedAppStatusClient
	}
	client, err := factory()
	if err != nil {
		httpx.WriteError(w, 503, "fresh Kubernetes identity observation is unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	get := func(ctx context.Context, path string) (map[string]any, bool, error) {
		var out map[string]any
		err := client.doJSON(ctx, path, &out)
		if isKubeNotFound(err) {
			return nil, false, nil
		}
		return out, err == nil, err
	}
	plan, err := pgretire.Inspect(ctx, get, app, svc)
	if err != nil {
		httpx.WriteError(w, 409, "source retirement preflight blocked: "+err.Error())
		return
	}
	if !req.Apply {
		httpx.WriteJSON(w, 200, map[string]any{"plan": plan, "dry_run": true})
		return
	}
	if req.SourceUID != plan.SourceUID || req.PlanHash != plan.PlanHash {
		httpx.WriteError(w, 409, "retirement identity/configuration changed; inspect a new plan")
		return
	}
	op, _, err := s.store.CreateOperationWithResult(model.Operation{TenantID: svc.TenantID, AppID: app.ID, ServiceID: svc.ID, Type: pgretire.OperationType, RequestedByType: principal.ActorType, RequestedByID: principal.ActorID, DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{SourceRetirement: &plan}}})
	if err != nil {
		s.writeBackingServiceResizeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "backing_service.retire_source", "operation", op.ID, svc.TenantID, map[string]string{"service_id": svc.ID, "source_cluster": plan.SourceCluster, "source_uid": plan.SourceUID, "target_cluster": plan.TargetCluster, "plan_hash": plan.PlanHash, "preserve_storage": "true"})
	httpx.WriteJSON(w, 202, map[string]any{"plan": plan, "dry_run": false, "operation": redactOperationForDebugBundle(op)})
}
