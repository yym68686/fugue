package store

import (
	"context"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) ReserveIdempotencyRecord(scope, tenantID, key, requestHash string) (model.IdempotencyRecord, bool, error) {
	scope = strings.TrimSpace(scope)
	tenantID = strings.TrimSpace(tenantID)
	key = strings.TrimSpace(key)
	requestHash = strings.TrimSpace(requestHash)
	if scope == "" || tenantID == "" || key == "" || requestHash == "" {
		return model.IdempotencyRecord{}, false, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgReserveIdempotencyRecord(scope, tenantID, key, requestHash)
	}

	var record model.IdempotencyRecord
	var fresh bool
	err := s.withLockedState(true, func(state *model.State) error {
		index := findIdempotencyRecord(state, scope, tenantID, key)
		if index >= 0 {
			record = state.Idempotency[index]
			if record.RequestHash != requestHash {
				return ErrIdempotencyMismatch
			}
			return nil
		}

		now := time.Now().UTC()
		record = model.IdempotencyRecord{
			Scope:       scope,
			TenantID:    tenantID,
			Key:         key,
			RequestHash: requestHash,
			Status:      model.IdempotencyStatusPending,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		state.Idempotency = append(state.Idempotency, record)
		fresh = true
		return nil
	})
	return record, fresh, err
}

func (s *Store) CompleteIdempotencyRecord(scope, tenantID, key, appID, operationID string) (model.IdempotencyRecord, error) {
	scope = strings.TrimSpace(scope)
	tenantID = strings.TrimSpace(tenantID)
	key = strings.TrimSpace(key)
	appID = strings.TrimSpace(appID)
	operationID = strings.TrimSpace(operationID)
	if scope == "" || tenantID == "" || key == "" || appID == "" || operationID == "" {
		return model.IdempotencyRecord{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCompleteIdempotencyRecord(scope, tenantID, key, appID, operationID)
	}

	var record model.IdempotencyRecord
	err := s.withLockedState(true, func(state *model.State) error {
		index := findIdempotencyRecord(state, scope, tenantID, key)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		state.Idempotency[index].Status = model.IdempotencyStatusCompleted
		state.Idempotency[index].AppID = appID
		state.Idempotency[index].OperationID = operationID
		state.Idempotency[index].UpdatedAt = now
		record = state.Idempotency[index]
		return nil
	})
	return record, err
}

func (s *Store) ReleaseIdempotencyRecord(scope, tenantID, key string) error {
	scope = strings.TrimSpace(scope)
	tenantID = strings.TrimSpace(tenantID)
	key = strings.TrimSpace(key)
	if scope == "" || tenantID == "" || key == "" {
		return ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgReleaseIdempotencyRecord(scope, tenantID, key)
	}

	return s.withLockedState(true, func(state *model.State) error {
		index := findIdempotencyRecord(state, scope, tenantID, key)
		if index < 0 {
			return nil
		}
		if state.Idempotency[index].Status == model.IdempotencyStatusCompleted {
			return nil
		}
		state.Idempotency = append(state.Idempotency[:index], state.Idempotency[index+1:]...)
		return nil
	})
}

func (s *Store) GetIdempotencyRecord(scope, tenantID, key string) (model.IdempotencyRecord, error) {
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		record, err := scanIdempotencyRecord(s.db.QueryRowContext(ctx, `SELECT scope, tenant_id, key, request_hash, status, app_id, operation_id, created_at, updated_at FROM fugue_idempotency_keys WHERE scope=$1 AND tenant_id=$2 AND key=$3`, scope, tenantID, key))
		return record, mapDBErr(err)
	}
	var record model.IdempotencyRecord
	err := s.withLockedState(false, func(state *model.State) error {
		idx := findIdempotencyRecord(state, scope, tenantID, key)
		if idx < 0 {
			return ErrNotFound
		}
		record = state.Idempotency[idx]
		return nil
	})
	return record, err
}
