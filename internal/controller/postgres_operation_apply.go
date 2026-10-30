package controller

import (
	"context"
	"fmt"
	"net/http"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/storagerecovery"
	"fugue/internal/store"
)

// A database operation must not roll its consumer Deployment, refresh unrelated
// ingress, or require the consumer's historical image to remain in a registry.
// The active operation lease prevents background app reconciliation until the
// database's final state is committed. Only its exact existing Cluster changes.
func (s *Service) applyManagedPostgresOperationObjects(ctx context.Context, operationID string, app model.App, placements map[string][]runtime.SchedulingConstraints) error {
	op, err := s.Store.GetOperation(operationID)
	if err != nil {
		return err
	}
	if op.AppID != app.ID || op.TenantID != app.TenantID {
		return fmt.Errorf("database operation ownership differs")
	}
	switch op.Type {
	case model.OperationTypeDatabaseSwitchover, model.OperationTypeDatabaseLocalize, storagerecovery.OperationType:
	default:
		return fmt.Errorf("operation is not a database placement transition")
	}
	target, err := store.ManagedPostgresOperationTargetForApp(app, op.ServiceID)
	if err != nil || target == nil {
		return fmt.Errorf("database operation target unavailable: %v", err)
	}
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	ns := runtime.NamespaceForTenant(app.TenantID)
	managed, found, err := client.getManagedApp(ctx, ns, runtime.ManagedAppResourceName(app))
	if err != nil || !found {
		return fmt.Errorf("database owner ManagedApp unavailable: %v", err)
	}
	objects := s.Renderer.BuildManagedAppChildObjectsWithPlacements(app, runtime.SchedulingConstraints{}, placements, runtime.ManagedAppOwnerReference(managed))
	var selected []map[string]any
	for _, obj := range objects {
		if obj["kind"] == runtime.CloudNativePGClusterKind && objectStringField(objectMapField(obj, "metadata"), "name") == target.Postgres.ServiceName {
			selected = append(selected, obj)
		}
	}
	if len(selected) != 1 {
		return fmt.Errorf("database operation must identify exactly one Cluster")
	}
	live, exists, err := client.getRawObject(ctx, cloudNativePGClusterAPIPath(ns, target.Postgres.ServiceName))
	if err != nil {
		return err
	}
	if !exists || objectStringField(objectMapField(live, "metadata"), "uid") == "" || !postgresCredentialOwnerMatches(live, selected[0]) {
		return fmt.Errorf("database placement requires the exact existing owned Cluster; refusing initialization or adoption")
	}
	if err := s.ensureManagedPostgresDataSafety(ctx, client, ns, managed, app, selected); err != nil {
		return err
	}
	if _, err := s.stabilizeManagedPostgresStorageSpecs(ctx, client, ns, selected); err != nil {
		return err
	}
	if err := s.prepareManagedPostgresInPlaceStorageExpansionForDesiredObjects(ctx, client, ns, selected); err != nil {
		return err
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	current, exists, err := client.getRawObject(ctx, cloudNativePGClusterAPIPath(ns, target.Postgres.ServiceName))
	if err != nil {
		return err
	}
	if !exists || objectStringField(objectMapField(current, "metadata"), "uid") != objectStringField(objectMapField(live, "metadata"), "uid") || !postgresCredentialOwnerMatches(current, selected[0]) || coldFencedSource(current) {
		return fmt.Errorf("database identity changed or was fenced during placement preparation")
	}
	patch, err := managedPostgresPlacementOperationPatch(current, selected[0])
	if err != nil {
		return err
	}
	_, err = client.doRequest(ctx, http.MethodPatch, cloudNativePGClusterAPIPath(ns, target.Postgres.ServiceName), "application/json-patch+json", patch, nil)
	return err
}

// A placement operation is not a code/configuration release. In particular it
// cannot rotate roles, change bootstrap identity, upgrade images, or replace
// webhook defaults. CAS tests also prevent a delete/recreate or concurrent
// operator change from being silently overwritten.
func managedPostgresPlacementOperationPatch(current, desired map[string]any) ([]map[string]any, error) {
	metadata := objectMapField(current, "metadata")
	uid, rv := objectStringField(metadata, "uid"), objectStringField(metadata, "resourceVersion")
	if uid == "" || rv == "" {
		return nil, fmt.Errorf("database placement requires UID/resourceVersion concurrency evidence")
	}
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": uid},
		{"op": "test", "path": "/metadata/resourceVersion", "value": rv},
	}
	spec := objectMapField(desired, "spec")
	for _, field := range []string{"instances", "minSyncReplicas", "maxSyncReplicas", "affinity", "storage"} {
		value, present := spec[field]
		if !present {
			continue
		}
		if field == "storage" {
			storage := cloneKubeMap(objectMapField(objectMapField(current, "spec"), "storage"))
			if storage == nil {
				storage = map[string]any{}
			}
			for key, v := range objectMapField(spec, "storage") {
				storage[key] = v
			}
			value = storage
		}
		patch = append(patch, map[string]any{"op": "add", "path": "/spec/" + field, "value": value})
	}
	annotations := cloneKubeMap(objectMapField(metadata, "annotations"))
	if annotations == nil {
		annotations = map[string]any{}
	}
	hold, present := normalizeKubeMap(objectMapField(desired, "metadata")["annotations"])[runtime.CloudNativePGReconcilePodSpecAnno]
	if present {
		annotations[runtime.CloudNativePGReconcilePodSpecAnno] = hold
	} else {
		delete(annotations, runtime.CloudNativePGReconcilePodSpecAnno)
	}
	patch = append(patch, map[string]any{"op": "add", "path": "/metadata/annotations", "value": annotations})
	return patch, nil
}
