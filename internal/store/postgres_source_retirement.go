package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"fugue/internal/model"
	"fugue/internal/pgretire"
)

func preparePostgresSourceRetirement(app model.App, op *model.Operation) error {
	if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil || op.DesiredSpec.Postgres.SourceRetirement == nil {
		return ErrInvalidInput
	}
	p := *op.DesiredSpec.Postgres.SourceRetirement
	target, err := PostgresImageMigrationTargetForApp(app, op.ServiceID)
	if err != nil {
		return ErrConflict
	}
	if p.ServiceID != op.ServiceID || p.AppID != app.ID || p.Namespace != "fg-"+strings.ReplaceAll(app.TenantID, "_", "-") || p.SourceUID == "" || p.TargetUID == "" || p.SourceUID == p.TargetUID || p.TargetCluster != target.Postgres.ServiceName || p.SourceCluster == p.TargetCluster || !p.PreserveStorage || p.ConfigHash != pgretire.ConfigHash(app, *target.Service) || p.PlanHash != pgretire.PlanHash(p) || len(p.Volumes) == 0 {
		return ErrConflict
	}
	next := model.CloneAppPostgresSpec(&target.Postgres)
	next.SourceRetirement = &p
	op.DesiredSpec = cloneAppSpec(&app.Spec)
	op.DesiredSpec.Postgres = next
	op.DesiredSource = nil
	op.DesiredOriginSource = nil
	op.DesiredReplicas = nil
	op.SourceRuntimeID = next.RuntimeID
	op.TargetRuntimeID = next.RuntimeID
	return nil
}

func sourceRetirementRetryMatches(a, b model.Operation) bool {
	return a.Type == pgretire.OperationType && b.Type == a.Type && a.AppID == b.AppID && a.ServiceID == b.ServiceID && a.TenantID == b.TenantID && a.DesiredSpec != nil && b.DesiredSpec != nil && a.DesiredSpec.Postgres != nil && b.DesiredSpec.Postgres != nil && a.DesiredSpec.Postgres.SourceRetirement != nil && b.DesiredSpec.Postgres.SourceRetirement != nil && a.DesiredSpec.Postgres.SourceRetirement.PlanHash == b.DesiredSpec.Postgres.SourceRetirement.PlanHash
}

func validateSourceRetirementProof(op model.Operation, proofs map[string]map[string]any) error {
	if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil || op.DesiredSpec.Postgres.SourceRetirement == nil {
		return ErrConflict
	}
	p := op.DesiredSpec.Postgres.SourceRetirement
	if p.PlanHash != pgretire.PlanHash(*p) || pgretire.Hash(p) != pgretire.Hash(proofs[pgretire.IntentEvidence]) || pgretire.Hash(p) != pgretire.Hash(proofs[pgretire.VerifiedEvidence]) {
		return ErrConflict
	}
	return nil
}
func applyPostgresSourceRetirement(state *storeProjection, app *model.App, op *model.Operation) error {
	proofs := map[string]map[string]any{}
	for _, e := range state.OperationEvidence {
		if e.OperationID == op.ID && e.TenantID == op.TenantID {
			proofs[e.Type] = e.Payload
		}
	}
	if err := validateSourceRetirementProof(*op, proofs); err != nil {
		return err
	}
	if err := validateManagedPostgresResizeTargetState(state, *app, op.ServiceID); err != nil {
		return err
	}
	current := *app
	hydrateAppBackingServices(state, &current)
	return preparePostgresSourceRetirement(current, op)
}
func (s *Store) pgValidateSourceRetirementCompletion(ctx context.Context, tx *sql.Tx, app *model.App, op *model.Operation) error {
	proofs := map[string]map[string]any{}
	for _, kind := range []string{pgretire.IntentEvidence, pgretire.VerifiedEvidence} {
		var raw []byte
		if err := tx.QueryRowContext(ctx, `SELECT payload_json FROM fugue_operation_evidence WHERE operation_id=$1 AND tenant_id=$2 AND evidence_type=$3 ORDER BY collected_at DESC, id DESC LIMIT 1`, op.ID, op.TenantID, kind).Scan(&raw); err != nil {
			return ErrConflict
		}
		var p map[string]any
		if json.Unmarshal(raw, &p) != nil {
			return ErrConflict
		}
		proofs[kind] = p
	}
	if err := validateSourceRetirementProof(*op, proofs); err != nil {
		return err
	}
	svc, err := s.pgValidateManagedPostgresResizeTargetTx(ctx, tx, app, op.ServiceID)
	if err != nil {
		return err
	}
	app.BackingServices = []model.BackingService{svc}
	return preparePostgresSourceRetirement(*app, op)
}
