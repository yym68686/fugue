package api

import (
	"net/http"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/pgimage"
	"fugue/internal/store"
)

func (s *Server) handleMigrateBackingServiceImage(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() && !p.HasScope("app.write") && !p.HasScope("project.write") {
		httpx.WriteError(w, 403, "missing app.write or project.write scope")
		return
	}
	svc, ok := s.loadAuthorizedBackingService(w, r, p)
	if !ok {
		return
	}
	if !isMigratableBackingService(svc) || svc.Spec.Postgres == nil || svc.OwnerAppID == "" || svc.Spec.Postgres.Instances != 1 || svc.Spec.Postgres.Suspended {
		httpx.WriteError(w, 409, "image migration requires an active app-owned single-instance PostgreSQL service")
		return
	}
	var req struct {
		Image       string `json:"image"`
		Apply       bool   `json:"apply"`
		Acknowledge bool   `json:"acknowledge_downtime"`
		Expected    string `json:"expected_config_hash"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, 400, err.Error())
		return
	}
	image, err := pgimage.Target(req.Image)
	if err != nil {
		httpx.WriteError(w, 400, err.Error())
		return
	}
	app, err := s.store.GetApp(svc.OwnerAppID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if app.TenantID != svc.TenantID || app.ProjectID != svc.ProjectID {
		httpx.WriteError(w, 409, "database owner identity differs")
		return
	}
	plan := pgimage.Plan{ServiceID: svc.ID, AppID: app.ID, TargetImage: image, ConfigHash: store.PostgresImageMigrationConfigHash(app, svc, image), RequiresDowntime: true, LivePreflightPending: true,
		Stages: []string{"verify current Cluster, primary, PVC, node, resource envelope and writable SQL", "persist immutable preflight witness and image-change intent", "compare-and-swap only Cluster.spec.imageName; let CNPG replace the primary", "verify unchanged PVC identities, system ID, placement, resources, SQL and no bootstrap container", "persist only the backing service image; never deploy the consumer app"}}
	if !req.Apply {
		httpx.WriteJSON(w, 200, map[string]any{"plan": plan, "dry_run": true})
		return
	}
	if !req.Acknowledge || req.Expected == "" {
		httpx.WriteError(w, 400, "apply requires acknowledge_downtime and expected_config_hash from a preceding plan")
		return
	}
	if req.Expected != plan.ConfigHash {
		httpx.WriteError(w, 409, "configuration changed since the migration plan; inspect a new plan")
		return
	}
	op, result, err := s.store.CreateOperationWithResult(model.Operation{TenantID: svc.TenantID, AppID: app.ID, ServiceID: svc.ID, Type: pgimage.OperationType, RequestedByType: p.ActorType, RequestedByID: p.ActorID,
		DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{Image: image, ImageMigrationConfigHash: req.Expected}}})
	if err != nil {
		s.writeBackingServiceResizeStoreError(w, err)
		return
	}
	decision := "reused_existing"
	if result.Created {
		decision = "accepted"
	}
	s.appendAudit(p, "backing_service.migrate_image", "operation", op.ID, svc.TenantID, map[string]string{"service_id": svc.ID, "target_image": image, "config_hash": plan.ConfigHash, "downtime_acknowledged": "true", "result": decision})
	httpx.WriteJSON(w, 202, map[string]any{"plan": plan, "dry_run": false, "operation": redactOperationForDebugBundle(op)})
}
