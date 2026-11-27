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
	return s.pgReserveIdempotencyRecord(scope, tenantID, key, requestHash)
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
	return s.pgCompleteIdempotencyRecord(scope, tenantID, key, appID, operationID)
}

func (s *Store) ReleaseIdempotencyRecord(scope, tenantID, key string) error {
	scope = strings.TrimSpace(scope)
	tenantID = strings.TrimSpace(tenantID)
	key = strings.TrimSpace(key)
	if scope == "" || tenantID == "" || key == "" {
		return ErrInvalidInput
	}
	return s.pgReleaseIdempotencyRecord(scope, tenantID, key)
}

func (s *Store) GetIdempotencyRecord(scope, tenantID, key string) (model.IdempotencyRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	record, err := scanIdempotencyRecord(s.db.QueryRowContext(ctx, `SELECT scope, tenant_id, key, request_hash, status, app_id, operation_id, created_at, updated_at FROM fugue_idempotency_keys WHERE scope=$1 AND tenant_id=$2 AND key=$3`, scope, tenantID, key))
	return record, mapDBErr(err)
}
