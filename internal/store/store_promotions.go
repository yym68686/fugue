package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) AppendStorePromotion(promotion model.StorePromotion) (model.StorePromotion, error) {
	promotion = normalizeStorePromotion(promotion)
	if promotion.SourceKind == "" || promotion.TargetStore == "" || promotion.Generation == "" {
		return model.StorePromotion{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgAppendStorePromotion(promotion)
	}
	err := s.withLockedState(true, func(state *model.State) error {
		state.StorePromotions = append(state.StorePromotions, promotion)
		return nil
	})
	if err != nil {
		return model.StorePromotion{}, err
	}
	return promotion, nil
}

func (s *Store) ListStorePromotions(limit int) ([]model.StorePromotion, error) {
	if s.usingDatabase() {
		return s.pgListStorePromotions(limit)
	}
	promotions := []model.StorePromotion{}
	err := s.withLockedState(false, func(state *model.State) error {
		promotions = append(promotions, state.StorePromotions...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortStorePromotions(promotions)
	if limit > 0 && len(promotions) > limit {
		promotions = promotions[:limit]
	}
	return promotions, nil
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

func sortStorePromotions(promotions []model.StorePromotion) {
	sort.Slice(promotions, func(i, j int) bool {
		if !promotions[i].UpdatedAt.Equal(promotions[j].UpdatedAt) {
			return promotions[i].UpdatedAt.After(promotions[j].UpdatedAt)
		}
		return promotions[i].ID < promotions[j].ID
	})
}
