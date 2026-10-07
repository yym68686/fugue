package store

import (
	"context"
	"fugue/internal/model"
	"strings"
	"time"
)

// RestoreLostImageAvailability is used only after exact fresh physical evidence
// has been verified. A concurrent retirement/deletion must win over recovery;
// no other image fields are copied from the caller's possibly stale snapshot.
func (s *Store) RestoreLostImageAvailability(id, tenant, digest string) error {
	id, tenant, digest = strings.TrimSpace(id), strings.TrimSpace(tenant), CanonicalImageDigest(digest)
	if id == "" || tenant == "" || digest == "" {
		return ErrInvalidInput
	}
	now := time.Now().UTC()
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := s.db.ExecContext(ctx, `UPDATE fugue_images SET lifecycle_state=$4, updated_at=$5 WHERE id=$1 AND tenant_id=$2 AND canonical_digest=$3 AND lifecycle_state=$6`, id, tenant, digest, model.ImageLifecycleAvailable, now, model.ImageLifecycleLost)
		return mapDBErr(err)
	}
	return s.withLockedState(true, func(state *model.State) error {
		for i := range state.Images {
			im := &state.Images[i]
			if im.ID == id && im.TenantID == tenant && CanonicalImageDigest(im.CanonicalDigest) == digest && im.LifecycleState == model.ImageLifecycleLost {
				im.LifecycleState = model.ImageLifecycleAvailable
				im.UpdatedAt = now
			}
		}
		return nil
	})
}
