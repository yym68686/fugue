package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"fugue/internal/model"
	"fugue/internal/pgimage"
)

func PostgresImageMigrationConfigHash(app model.App, service model.BackingService, image string) string {
	return PostgresImageMigrationPlanHash(app, service, image, 0)
}

func PostgresImageMigrationPlanHash(app model.App, service model.BackingService, image string, budget int) string {
	app.Spec = *cloneAppSpec(&app.Spec)
	model.ApplyAppSpecDefaults(&app.Spec)
	service = cloneBackingService(service)
	if service.Spec.Postgres != nil {
		normalized := normalizeManagedPostgresSpec(appNameForService(&service, &app), app.Spec.RuntimeID, *service.Spec.Postgres)
		service.Spec.Postgres = &normalized
	}
	configuration := []any{app.ID, app.TenantID, app.ProjectID, app.Spec, service.ID, service.OwnerAppID, service.Spec, image}
	if service.OwnerAppID == "" {
		// An independent service's consumer is an operation anchor, not an owner.
		// Pin the actual binding so an unbind/rebind invalidates an old plan.
		bindings := []model.ServiceBinding{}
		for _, binding := range app.Bindings {
			if binding.ServiceID == service.ID {
				bindings = append(bindings, cloneServiceBinding(binding))
			}
		}
		sortServiceBindings(bindings)
		configuration = append(configuration, bindings)
	}
	if budget != 0 {
		configuration = append(configuration, "fast-drain-v1", budget)
	}
	return pgimage.Hash(configuration)
}

// PostgresImageMigrationTargetForApp accepts an owner or an independent service's
// actual single consumer. The store transaction separately locks and validates ALL
// bindings to the service, including any not visible through this app snapshot.
func PostgresImageMigrationTargetForApp(app model.App, serviceID string) (*ManagedPostgresOperationTarget, error) {
	if strings.TrimSpace(serviceID) == "" {
		return nil, ErrConflict
	}
	target, err := ManagedPostgresOperationTargetForApp(app, serviceID)
	if err != nil || target == nil || target.Service == nil || target.Postgres.Suspended || target.Postgres.Instances != 1 {
		return nil, ErrConflict
	}
	service := target.Service
	if service.TenantID != app.TenantID || service.ProjectID != app.ProjectID || (service.OwnerAppID != "" && !target.AppOwned) {
		return nil, ErrConflict
	}
	bindings := 0
	for _, binding := range app.Bindings {
		if binding.ServiceID != service.ID {
			continue
		}
		if binding.AppID != app.ID || binding.TenantID != app.TenantID {
			return nil, ErrConflict
		}
		bindings++
	}
	if bindings != 1 {
		return nil, ErrConflict
	}
	return target, nil
}

func preparePostgresImageMigration(app model.App, op *model.Operation) error {
	if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil {
		return ErrInvalidInput
	}
	image, err := pgimage.Target(op.DesiredSpec.Postgres.Image)
	if err != nil {
		return ErrInvalidInput
	}
	target, err := PostgresImageMigrationTargetForApp(app, op.ServiceID)
	if err != nil {
		return ErrConflict
	}
	budget := op.DesiredSpec.Postgres.ImageMigrationDrainBudgetSeconds
	if !pgimage.ValidDrainBudget(budget) {
		return ErrInvalidInput
	}
	expected := PostgresImageMigrationPlanHash(app, *target.Service, image, budget)
	if op.DesiredSpec.Postgres.ImageMigrationConfigHash != expected {
		return ErrConflict
	}
	next := model.CloneAppPostgresSpec(&target.Postgres)
	next.Image = image
	next.ImageMigrationConfigHash = expected
	next.ImageMigrationDrainBudgetSeconds = budget
	op.DesiredSpec = cloneAppSpec(&app.Spec)
	op.DesiredSpec.Postgres = next
	op.DesiredSource = nil
	op.DesiredOriginSource = nil
	op.DesiredReplicas = nil
	op.SourceRuntimeID = next.RuntimeID
	op.TargetRuntimeID = next.RuntimeID
	if strings.TrimSpace(next.RuntimeID) == "" {
		return ErrInvalidInput
	}
	return nil
}

func postgresImageMigrationRetryMatches(a, b model.Operation) bool {
	return a.Type == pgimage.OperationType && b.Type == a.Type && a.AppID == b.AppID && a.ServiceID == b.ServiceID && a.TenantID == b.TenantID &&
		a.DesiredSpec != nil && b.DesiredSpec != nil && a.DesiredSpec.Postgres != nil && b.DesiredSpec.Postgres != nil &&
		a.DesiredSpec.Postgres.Image == b.DesiredSpec.Postgres.Image && a.DesiredSpec.Postgres.ImageMigrationConfigHash == b.DesiredSpec.Postgres.ImageMigrationConfigHash
}

func imageMigrationPostgres(service model.BackingService, app model.App, op model.Operation, intent map[string]any) (*model.AppPostgresSpec, error) {
	if err := preparePostgresImageMigration(app, &op); err != nil {
		return nil, err
	}
	p := model.CloneAppPostgresSpec(service.Spec.Postgres)
	p.Image = op.DesiredSpec.Postgres.Image
	if raw, present := intent["restart_resource_target"]; present {
		// Only this explicit maintenance operation may advance the restart
		// template. Ordinary in-place resize must never roll a database Pod.
		var target model.ResourceSpec
		encoded, err := json.Marshal(raw)
		if err != nil || json.Unmarshal(encoded, &target) != nil || p.RuntimeResources == nil || target != *p.RuntimeResources {
			return nil, ErrConflict
		}
		specHash, _ := intent["restart_spec_hash"].(string)
		resources, resourcesOK := intent["restart_resources"].(map[string]any)
		resourcesHash, _ := intent["resources_hash"].(string)
		if _, err := normalizeManagedPostgresResizeTarget(&target); err != nil || specHash == "" ||
			!resourcesOK || resourcesHash == "" || pgimage.Hash(resources) != resourcesHash || !imageMigrationResourceEvidenceMatches(resources, target) {
			return nil, ErrConflict
		}
		p.Resources = model.CloneResourceSpec(&target)
	}
	return p, nil
}

func imageMigrationResourceEvidenceMatches(resources map[string]any, target model.ResourceSpec) bool {
	quantityMatches := func(section, name, expected string) bool {
		values, ok := resources[section].(map[string]any)
		if !ok {
			return false
		}
		actualText, ok := values[name].(string)
		if !ok {
			return false
		}
		actual, actualErr := resource.ParseQuantity(actualText)
		wanted, wantedErr := resource.ParseQuantity(expected)
		return actualErr == nil && wantedErr == nil && actual.Cmp(wanted) == 0
	}
	if !quantityMatches("requests", "cpu", fmt.Sprintf("%dm", target.CPUMilliCores)) ||
		!quantityMatches("requests", "memory", fmt.Sprintf("%dMi", target.MemoryMebibytes)) ||
		!quantityMatches("limits", "memory", fmt.Sprintf("%dMi", target.MemoryLimitMebibytes)) {
		return false
	}
	limits, ok := resources["limits"].(map[string]any)
	if !ok {
		return false
	}
	if target.CPULimitMilliCores == 0 {
		_, found := limits["cpu"]
		return !found
	}
	return quantityMatches("limits", "cpu", fmt.Sprintf("%dm", target.CPULimitMilliCores))
}

func validatePostgresImageMigrationProof(op model.Operation, proofs map[string]map[string]any) error {
	baseline, intent, verified := proofs[pgimage.BaselineEvidence], proofs[pgimage.IntentEvidence], proofs[pgimage.VerifiedEvidence]
	if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil || len(baseline) == 0 || pgimage.Hash(baseline) != pgimage.Hash(intent) || len(verified) == 0 {
		return ErrConflict
	}
	uid, _ := verified["verified_pod_uid"].(string)
	sourceUID, _ := intent["pod_uid"].(string)
	clusterUID, _ := intent["cluster_uid"].(string)
	systemID, _ := intent["system_id"].(string)
	if uid == "" || sourceUID == "" || uid == sourceUID || intent["target_image"] != op.DesiredSpec.Postgres.Image || clusterUID == "" || systemID == "" {
		return ErrConflict
	}
	budget := op.DesiredSpec.Postgres.ImageMigrationDrainBudgetSeconds
	if budget != 0 {
		drain, ok := intent["drain"].(map[string]any)
		if !pgimage.ValidDrainBudget(budget) || !ok || drain["budget_seconds"] != float64(budget) || verified["drain_restored"] != true {
			return ErrConflict
		}
	} else if intent["drain"] != nil || verified["drain_restored"] != nil {
		return ErrConflict
	}
	copy := map[string]any{}
	for k, v := range verified {
		if k != "verified_pod_uid" && k != "drain_restored" {
			copy[k] = v
		}
	}
	if pgimage.Hash(copy) != pgimage.Hash(intent) {
		return ErrConflict
	}
	return nil
}

func applyPostgresImageMigration(state *model.State, app *model.App, op *model.Operation) error {
	proofs := map[string]map[string]any{}
	for _, e := range state.OperationEvidence {
		if e.OperationID == op.ID && e.TenantID == op.TenantID {
			proofs[e.Type] = e.Payload
		}
	}
	if err := validatePostgresImageMigrationProof(*op, proofs); err != nil {
		return err
	}
	if err := validateManagedPostgresResizeTargetState(state, *app, op.ServiceID); err != nil {
		return err
	}
	i := findBackingService(state, op.ServiceID)
	svc := cloneBackingService(state.BackingServices[i])
	owner := *app
	hydrateAppBackingServices(state, &owner)
	p, err := imageMigrationPostgres(svc, owner, *op, proofs[pgimage.IntentEvidence])
	if err != nil {
		return err
	}
	svc.Spec.Postgres = p
	svc.UpdatedAt = time.Now().UTC()
	state.BackingServices[i] = svc
	return nil
}

func (s *Store) pgApplyPostgresImageMigration(ctx context.Context, tx *sql.Tx, app *model.App, op *model.Operation) error {
	proofs := map[string]map[string]any{}
	for _, kind := range []string{pgimage.BaselineEvidence, pgimage.IntentEvidence, pgimage.VerifiedEvidence} {
		var raw []byte
		if err := tx.QueryRowContext(ctx, `SELECT payload_json FROM fugue_operation_evidence WHERE operation_id=$1 AND tenant_id=$2 AND evidence_type=$3 ORDER BY collected_at DESC, id DESC LIMIT 1`, op.ID, op.TenantID, kind).Scan(&raw); err != nil {
			return ErrConflict
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			return ErrConflict
		}
		proofs[kind] = payload
	}
	if err := validatePostgresImageMigrationProof(*op, proofs); err != nil {
		return err
	}
	svc, err := s.pgValidateManagedPostgresResizeTargetTx(ctx, tx, app, op.ServiceID)
	if err != nil {
		return err
	}
	// The completion transaction rehydrates the exact service before hashing.
	app.BackingServices = []model.BackingService{svc}
	p, err := imageMigrationPostgres(svc, *app, *op, proofs[pgimage.IntentEvidence])
	if err != nil {
		return err
	}
	svc.Spec.Postgres = p
	svc.UpdatedAt = time.Now().UTC()
	return s.pgUpdateBackingServiceTx(ctx, tx, svc)
}
