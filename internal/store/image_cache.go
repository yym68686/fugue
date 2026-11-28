package store

import (
	"fugue/internal/model"
	"fugue/internal/store/internal/imagecache"
)

func (s *Store) imageCache() imagecache.Repository {
	return imagecache.NewPostgres(s.db, ErrInvalidInput, ErrConflict)
}

func (s *Store) UpsertImageCacheInventory(node model.ImageCacheNodeInventory, manifests []model.ImageCacheManifest) (model.ImageCacheNodeInventory, error) {
	out, err := s.imageCache().UpsertImageCacheInventory(node, manifests)
	return out, mapDBErr(err)
}

func (s *Store) ListImageCacheNodeInventories(filter model.ImageCacheNodeInventoryFilter) ([]model.ImageCacheNodeInventory, error) {
	out, err := s.imageCache().ListImageCacheNodeInventories(filter)
	return out, mapDBErr(err)
}

func (s *Store) ListImageCacheManifests(filter model.ImageCacheManifestFilter) ([]model.ImageCacheManifest, error) {
	out, err := s.imageCache().ListImageCacheManifests(filter)
	return out, mapDBErr(err)
}

func (s *Store) UpsertImageCachePrunePlan(plan model.ImageCachePrunePlan) (model.ImageCachePrunePlan, error) {
	out, err := s.imageCache().UpsertImageCachePrunePlan(plan)
	return out, mapDBErr(err)
}

func (s *Store) UpsertImageCachePrunePlanSummary(plan model.ImageCachePrunePlan) (model.ImageCachePrunePlan, error) {
	out, err := s.imageCache().UpsertImageCachePrunePlanSummary(plan)
	return out, mapDBErr(err)
}

func (s *Store) ListImageCachePrunePlans(filter model.ImageCachePrunePlanFilter) ([]model.ImageCachePrunePlan, error) {
	out, err := s.imageCache().ListImageCachePrunePlans(filter)
	return out, mapDBErr(err)
}
