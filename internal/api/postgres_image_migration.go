package api

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/pgimage"
	"fugue/internal/runtime"
	"fugue/internal/store"

	corev1 "k8s.io/api/core/v1"
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
	if !isMigratableBackingService(svc) || svc.Spec.Postgres == nil || svc.Spec.Postgres.Instances != 1 || svc.Spec.Postgres.Suspended {
		httpx.WriteError(w, 409, "image migration requires an active, exclusively bound single-instance PostgreSQL service")
		return
	}
	var req struct {
		Image                  string `json:"image"`
		Apply                  bool   `json:"apply"`
		Acknowledge            bool   `json:"acknowledge_downtime"`
		Expected               string `json:"expected_config_hash"`
		DrainBudget            *int   `json:"drain_budget_seconds"`
		AcknowledgeTermination bool   `json:"acknowledge_connection_termination"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, 400, err.Error())
		return
	}
	budget := 0
	if req.DrainBudget != nil {
		budget = *req.DrainBudget
		if budget == 0 || !pgimage.ValidDrainBudget(budget) {
			httpx.WriteError(w, 400, "drain_budget_seconds must be between 10 and 15")
			return
		}
	}
	if (req.Apply && budget != 0 && !req.AcknowledgeTermination) || (budget == 0 && req.AcknowledgeTermination) {
		httpx.WriteError(w, 400, "fast drain apply requires both drain_budget_seconds and acknowledge_connection_termination")
		return
	}
	image, err := pgimage.Target(req.Image)
	if err != nil {
		httpx.WriteError(w, 400, err.Error())
		return
	}
	app, err := s.backingServiceSwitchoverApp(svc)
	if err != nil {
		if err == store.ErrInvalidInput {
			httpx.WriteError(w, 409, "image migration requires exactly one bound consumer app in the same tenant and project; unbound or shared independent services are not supported")
		} else {
			s.writeStoreError(w, err)
		}
		return
	}
	target, err := store.PostgresImageMigrationTargetForApp(app, svc.ID)
	if err != nil || app.TenantID != svc.TenantID || app.ProjectID != svc.ProjectID {
		httpx.WriteError(w, 409, "database ownership, consumer binding or tenant/project identity differs")
		return
	}
	ownership := "app_owned"
	if svc.OwnerAppID == "" {
		ownership = "independent"
	}
	plan := pgimage.Plan{ServiceID: svc.ID, AppID: app.ID, Ownership: ownership, TargetImage: image, ConfigHash: store.PostgresImageMigrationPlanHash(app, svc, image, budget), RequiresDowntime: true, LivePreflightPending: true, DrainBudgetSeconds: budget,
		Stages: []string{"verify current Cluster, primary, PVC, node, persisted resource envelope and writable SQL", "persist immutable preflight witness and image-change intent", "atomically compare-and-swap Cluster.spec.imageName and, if needed, align its restart template with the applied in-place resource target; let CNPG replace the primary", "verify unchanged PVC identities, system ID, placement, running resources, SQL and no bootstrap container", "persist the backing service image and verified restart template; never deploy the consumer app"}}
	if budget != 0 {
		standard := plan.Stages
		plan.Stages = []string{standard[0], standard[1],
			"sample current sessions; stage the operation-local budget and verify the source primary loaded it; recheck transactions",
			standard[2], "restore and verify original shutdown settings; record stable Service SQL probe observations",
			standard[3], standard[4]}
	}
	if !req.Apply {
		if budget != 0 {
			sample := s.previewPostgresImageDrain(r.Context(), runtime.NamespaceForTenant(svc.TenantID), target.Postgres.ServiceName, budget)
			plan.Drain = &sample
		}
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
		DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{Image: image, ImageMigrationConfigHash: req.Expected, ImageMigrationDrainBudgetSeconds: budget}}})
	if err != nil {
		s.writeBackingServiceResizeStoreError(w, err)
		return
	}
	decision := "reused_existing"
	if result.Created {
		decision = "accepted"
	}
	audit := map[string]string{"service_id": svc.ID, "target_image": image, "config_hash": plan.ConfigHash, "downtime_acknowledged": "true", "result": decision}
	if budget != 0 {
		audit["drain_budget_seconds"] = strconv.Itoa(budget)
		audit["connection_termination_acknowledged"] = "true"
	}
	s.appendAudit(p, "backing_service.migrate_image", "operation", op.ID, svc.TenantID, audit)
	httpx.WriteJSON(w, 202, map[string]any{"plan": plan, "dry_run": false, "operation": redactOperationForDebugBundle(op)})
}

// Only inspect the Cluster-owned primary in the authorized service namespace.
// This is a sample, never a substitute for the operator's live preflight.
func (s *Server) previewPostgresImageDrain(ctx context.Context, namespace, cluster string, budget int) pgimage.DrainObservation {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	unavailable := pgimage.UnavailableDrainObservation()
	client, err := s.managedAppStatusClient()
	if err != nil {
		return unavailable
	}
	var observed struct {
		Metadata struct {
			UID               string
			DeletionTimestamp *time.Time
		}
		Spec   struct{ Instances int }
		Status struct{ CurrentPrimary string }
	}
	if err := client.doJSON(ctx, "/apis/postgresql.cnpg.io/v1/namespaces/"+url.PathEscape(namespace)+"/clusters/"+url.PathEscape(cluster), &observed); err != nil ||
		observed.Metadata.UID == "" || observed.Metadata.DeletionTimestamp != nil || observed.Spec.Instances != 1 || observed.Status.CurrentPrimary == "" {
		return unavailable
	}
	var pod corev1.Pod
	podName := observed.Status.CurrentPrimary
	podPath := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods/" + url.PathEscape(podName)
	if err := client.doJSON(ctx, podPath, &pod); err != nil || pod.UID == "" || pod.DeletionTimestamp != nil || pod.Labels["cnpg.io/cluster"] != cluster || pod.Labels["cnpg.io/instanceRole"] != "primary" {
		return unavailable
	}
	owned := false
	for _, ref := range pod.OwnerReferences {
		owned = owned || (ref.Kind == "Cluster" && ref.APIVersion == "postgresql.cnpg.io/v1" && ref.Name == cluster && string(ref.UID) == observed.Metadata.UID && ref.Controller != nil && *ref.Controller)
	}
	if !owned {
		return unavailable
	}
	raw, err := (kubeFilesystemExecRunner{}).Run(ctx, namespace, podName, "postgres", nil, pgimage.DrainObservationCommand()...)
	if err != nil {
		return unavailable
	}
	var current corev1.Pod
	if err = client.doJSON(ctx, podPath, &current); err != nil || current.UID != pod.UID || current.DeletionTimestamp != nil {
		return unavailable
	}
	return pgimage.DecodeDrainObservation(raw, budget)
}
