package store

import (
	"context"
	"encoding/json"
	"fugue/internal/model"
	"time"
)

// ListImageRetirements reads independently retained generation decisions. The
// archive has no foreign key to apps/images, so deleting metadata never erases
// the positive retirement authority. Live rows take precedence when revived.
func (s *Store) ListImageRetirements() ([]model.Image, error) {
	if !s.usingDatabase() {
		var out []model.Image
		err := s.withLockedState(false, func(state *model.State) error { out = append(out, state.ImageRetirements...); return nil })
		return out, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT image_json FROM fugue_image_retirements ORDER BY image_id,digest`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Image{}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var item model.Image
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
func (s *Store) ListImagesWithRetirementAuthority() ([]model.Image, error) {
	images, err := s.ListImages(model.ImageFilter{PlatformAdmin: true})
	if err != nil {
		return nil, err
	}
	retired, err := s.ListImageRetirements()
	if err != nil {
		return nil, err
	}
	live := map[string]bool{}
	for _, im := range images {
		live[im.ID] = true
	}
	for _, im := range retired {
		if !live[im.ID] {
			images = append(images, im)
		}
	}
	return images, nil
}

func retainImageRetirement(state *model.State, image model.Image) {
	if (image.LifecycleState != model.ImageLifecycleDeleting && image.LifecycleState != model.ImageLifecycleDeleted) || CanonicalImageDigest(image.CanonicalDigest) == "" {
		return
	}
	for _, old := range state.ImageRetirements {
		if old.ID == image.ID && old.CanonicalDigest == image.CanonicalDigest {
			return
		}
	}
	image.ManifestJSON = ""
	state.ImageRetirements = append(state.ImageRetirements, image)
}
