package store

import (
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) AppendStorePromotion(promotion model.StorePromotion) (model.StorePromotion, error) {
	promotion = normalizeStorePromotion(promotion)
	if promotion.SourceKind == "" || promotion.TargetStore == "" || promotion.Generation == "" {
		return model.StorePromotion{}, ErrInvalidInput
	}
	return s.pgAppendStorePromotion(promotion)
}

func (s *Store) ListStorePromotions(limit int) ([]model.StorePromotion, error) {
	return s.pgListStorePromotions(limit)
}

func normalizeStorePromotion(promotion model.StorePromotion) model.StorePromotion {
	now := time.Now().UTC()
	promotion.ID = strings.TrimSpace(promotion.ID)
	if promotion.ID == "" {
		promotion.ID = model.NewID("storepromotion")
	}
	promotion.SourceKind = strings.TrimSpace(promotion.SourceKind)
	promotion.SourceFingerprint = strings.TrimSpace(promotion.SourceFingerprint)
	promotion.TargetStore = strings.TrimSpace(promotion.TargetStore)
	promotion.Generation = strings.TrimSpace(promotion.Generation)
	promotion.OperatorType = strings.TrimSpace(promotion.OperatorType)
	promotion.OperatorID = strings.TrimSpace(promotion.OperatorID)
	promotion.Status = strings.TrimSpace(promotion.Status)
	if promotion.Status == "" {
		promotion.Status = "pending"
	}
	promotion.BackupRef = strings.TrimSpace(promotion.BackupRef)
	promotion.RollbackRef = strings.TrimSpace(promotion.RollbackRef)
	promotion.RestoreManifestRef = strings.TrimSpace(promotion.RestoreManifestRef)
	promotion.PermissionVerificationStatus = strings.TrimSpace(promotion.PermissionVerificationStatus)
	promotion.InvariantStatus = strings.TrimSpace(promotion.InvariantStatus)
	promotion.Message = strings.TrimSpace(promotion.Message)
	if promotion.StartedAt.IsZero() {
		promotion.StartedAt = now
	}
	if promotion.CreatedAt.IsZero() {
		promotion.CreatedAt = now
	}
	promotion.UpdatedAt = now
	if promotion.Metadata == nil {
		promotion.Metadata = map[string]string{}
	}
	return promotion
}
