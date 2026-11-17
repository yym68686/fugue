package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgimage"
)

func PostgresImageMigrationConfigHash(app model.App, service model.BackingService, image string) string {
	app.Spec = *cloneAppSpec(&app.Spec)
	model.ApplyAppSpecDefaults(&app.Spec)
	service = cloneBackingService(service)
	if service.Spec.Postgres != nil {
		normalized := normalizeManagedPostgresSpec(appNameForService(&service, &app), app.Spec.RuntimeID, *service.Spec.Postgres)
		service.Spec.Postgres = &normalized
	}
	return pgimage.Hash([]any{app.ID, app.TenantID, app.ProjectID, app.Spec, service.ID, service.OwnerAppID, service.Spec, image})
}

func preparePostgresImageMigration(app model.App, op *model.Operation) error {
	if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil {
		return ErrInvalidInput
	}
	image, err := pgimage.Target(op.DesiredSpec.Postgres.Image)
	if err != nil {
		return ErrInvalidInput
	}
	target, err := ManagedPostgresOperationTargetForApp(app, op.ServiceID)
	if err != nil || target == nil || target.Service == nil || !target.AppOwned || target.Postgres.Suspended || target.Postgres.Instances != 1 {
		return ErrConflict
	}
	expected := PostgresImageMigrationConfigHash(app, *target.Service, image)
	if op.DesiredSpec.Postgres.ImageMigrationConfigHash != expected {
		return ErrConflict
	}
	next := model.CloneAppPostgresSpec(&target.Postgres)
	next.Image = image
	next.ImageMigrationConfigHash = expected
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

func imageMigrationPostgres(service model.BackingService, app model.App, op model.Operation) (*model.AppPostgresSpec, error) {
	if err := preparePostgresImageMigration(app, &op); err != nil {
		return nil, err
	}
	p := model.CloneAppPostgresSpec(service.Spec.Postgres)
	p.Image = op.DesiredSpec.Postgres.Image
	return p, nil
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
	copy := map[string]any{}
	for k, v := range verified {
		if k != "verified_pod_uid" {
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
	p, err := imageMigrationPostgres(svc, owner, *op)
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
	p, err := imageMigrationPostgres(svc, *app, *op)
	if err != nil {
		return err
	}
	svc.Spec.Postgres = p
	svc.UpdatedAt = time.Now().UTC()
	return s.pgUpdateBackingServiceTx(ctx, tx, svc)
}
