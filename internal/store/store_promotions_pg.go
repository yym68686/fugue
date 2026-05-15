package store

import (
	"context"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgAppendStorePromotion(promotion model.StorePromotion) (model.StorePromotion, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	metadataJSON, err := marshalNullableJSON(promotion.Metadata)
	if err != nil {
		return model.StorePromotion{}, err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO fugue_store_promotions (
	id, source_kind, source_fingerprint, target_store, generation,
	operator_type, operator_id, status, dry_run, backup_ref, rollback_ref,
	restore_manifest_ref, permission_verification_status, invariant_status,
	message, metadata_json, started_at, completed_at, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5,
	$6, $7, $8, $9, $10, $11,
	$12, $13, $14,
	$15, $16, $17, $18, $19, $20
)`, promotion.ID, promotion.SourceKind, promotion.SourceFingerprint, promotion.TargetStore, promotion.Generation,
		promotion.OperatorType, promotion.OperatorID, promotion.Status, promotion.DryRun, promotion.BackupRef, promotion.RollbackRef,
		promotion.RestoreManifestRef, promotion.PermissionVerificationStatus, promotion.InvariantStatus,
		promotion.Message, metadataJSON, promotion.StartedAt, promotion.CompletedAt, promotion.CreatedAt, promotion.UpdatedAt)
	if err != nil {
		return model.StorePromotion{}, mapDBErr(err)
	}
	return promotion, nil
}

func (s *Store) pgListStorePromotions(limit int) ([]model.StorePromotion, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, source_kind, source_fingerprint, target_store, generation,
	operator_type, operator_id, status, dry_run, backup_ref, rollback_ref,
	restore_manifest_ref, permission_verification_status, invariant_status,
	message, metadata_json, started_at, completed_at, created_at, updated_at
FROM fugue_store_promotions
ORDER BY updated_at DESC, id ASC
LIMIT $1`, limit)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	promotions := []model.StorePromotion{}
	for rows.Next() {
		promotion, err := scanStorePromotion(rows)
		if err != nil {
			return nil, err
		}
		promotions = append(promotions, promotion)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return promotions, nil
}

func scanStorePromotion(scanner sqlScanner) (model.StorePromotion, error) {
	var promotion model.StorePromotion
	var metadataRaw []byte
	if err := scanner.Scan(
		&promotion.ID,
		&promotion.SourceKind,
		&promotion.SourceFingerprint,
		&promotion.TargetStore,
		&promotion.Generation,
		&promotion.OperatorType,
		&promotion.OperatorID,
		&promotion.Status,
		&promotion.DryRun,
		&promotion.BackupRef,
		&promotion.RollbackRef,
		&promotion.RestoreManifestRef,
		&promotion.PermissionVerificationStatus,
		&promotion.InvariantStatus,
		&promotion.Message,
		&metadataRaw,
		&promotion.StartedAt,
		&promotion.CompletedAt,
		&promotion.CreatedAt,
		&promotion.UpdatedAt,
	); err != nil {
		return model.StorePromotion{}, mapDBErr(err)
	}
	metadata, err := decodeJSONValue[map[string]string](metadataRaw)
	if err != nil {
		return model.StorePromotion{}, err
	}
	promotion.Metadata = metadata
	return promotion, nil
}
