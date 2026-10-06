package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"fugue/internal/model"
	"fugue/internal/storagerecovery"
)

func PostgresSpecFingerprint(pg model.AppPostgresSpec) string {
	b, _ := json.Marshal(pg)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// CommitColdPostgresTarget changes only the exact service under the running
// recovery operation's exclusive lease. It never overwrites application config.
// An exact repeat after a lost response is idempotent.
func (s *Store) CommitColdPostgresTarget(operationID, expected string, next model.AppPostgresSpec) (model.BackingService, error) {
	return s.commitRecoveryPostgresTarget(operationID, expected, next, true)
}

func (s *Store) UpdateRecoveryPostgresSpec(operationID, expected string, next model.AppPostgresSpec) (model.BackingService, error) {
	return s.commitRecoveryPostgresTarget(operationID, expected, next, false)
}

func (s *Store) commitRecoveryPostgresTarget(operationID, expected string, next model.AppPostgresSpec, cold bool) (model.BackingService, error) {
	validate := func(op model.Operation, svc model.BackingService) error {
		if op.Type != storagerecovery.OperationType || op.Status != model.OperationStatusRunning || op.ServiceID != svc.ID || op.TenantID != svc.TenantID || next.ServiceName == "" {
			return ErrConflict
		}
		if cold && (op.TargetRuntimeID != next.RuntimeID || next.EndpointServiceName == "" || op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil || op.DesiredSpec.Postgres.StorageClassName != next.StorageClassName) {
			return ErrConflict
		}
		if svc.Spec.Postgres == nil {
			return ErrInvalidInput
		}
		if !cold && (next.ServiceName != svc.Spec.Postgres.ServiceName || next.EndpointServiceName != svc.Spec.Postgres.EndpointServiceName) {
			return ErrConflict
		}
		if PostgresSpecFingerprint(*svc.Spec.Postgres) == PostgresSpecFingerprint(next) {
			return nil
		}
		if PostgresSpecFingerprint(*svc.Spec.Postgres) != expected || model.PostgresEndpointName(*svc.Spec.Postgres) != model.PostgresEndpointName(next) || next.CredentialSecretName != svc.Spec.Postgres.CredentialSecretName || next.Database != svc.Spec.Postgres.Database || next.User != svc.Spec.Postgres.User || next.Password != svc.Spec.Postgres.Password {
			return ErrConflict
		}
		return nil
	}
	var saved model.BackingService
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return saved, err
		}
		defer tx.Rollback()
		op, err := s.pgGetOperationTx(ctx, tx, operationID, true)
		if err != nil {
			return saved, mapDBErr(err)
		}
		svc, err := s.pgGetBackingServiceTx(ctx, tx, op.ServiceID, true)
		if err != nil {
			return saved, mapDBErr(err)
		}
		if err := validate(op, svc); err != nil {
			return saved, err
		}
		svc.Spec.Postgres = model.CloneAppPostgresSpec(&next)
		svc.UpdatedAt = time.Now().UTC()
		if err := s.pgUpdateBackingServiceTx(ctx, tx, svc); err != nil {
			return saved, err
		}
		if err := tx.Commit(); err != nil {
			return saved, err
		}
		return svc, nil
	}
	err := s.withLockedState(true, func(state *model.State) error {
		oi := findOperation(state, operationID)
		if oi < 0 {
			return ErrNotFound
		}
		op := state.Operations[oi]
		si := findBackingService(state, op.ServiceID)
		if si < 0 {
			return ErrNotFound
		}
		svc := state.BackingServices[si]
		if err := validate(op, svc); err != nil {
			return err
		}
		svc.Spec.Postgres = model.CloneAppPostgresSpec(&next)
		svc.UpdatedAt = time.Now().UTC()
		state.BackingServices[si] = svc
		saved = cloneBackingService(svc)
		return nil
	})
	return saved, err
}
