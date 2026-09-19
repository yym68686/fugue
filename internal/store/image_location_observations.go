package store

import (
	"context"
	"fugue/internal/model"
	"sort"
	"strings"
	"time"
)

// ImageLocationScope is an exact observation identity, never a cross-tenant
// image match. Both positive and negative records retain their original age.
type ImageLocationScope struct{ TenantID, AppID, RuntimeID, ImageRef string }

func (s *Store) ListImageLocationObservations(ctx context.Context, scopes []ImageLocationScope) ([]model.ImageLocation, error) {
	wanted := map[ImageLocationScope]bool{}
	var tenants, apps, runtimes, refs []string
	for _, scope := range scopes {
		scope = ImageLocationScope{strings.TrimSpace(scope.TenantID), strings.TrimSpace(scope.AppID), strings.TrimSpace(scope.RuntimeID), strings.TrimSpace(scope.ImageRef)}
		if scope.TenantID == "" || scope.AppID == "" || scope.RuntimeID == "" || scope.ImageRef == "" || wanted[scope] {
			continue
		}
		wanted[scope] = true
		tenants = append(tenants, scope.TenantID)
		apps = append(apps, scope.AppID)
		runtimes = append(runtimes, scope.RuntimeID)
		refs = append(refs, scope.ImageRef)
	}
	if len(wanted) == 0 {
		return []model.ImageLocation{}, nil
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		rows, err := s.db.QueryContext(ctx, `
SELECT l.id,l.tenant_id,l.app_id,l.image_ref,l.digest,l.source_operation_id,l.node_id,l.runtime_id,l.cluster_node_name,l.cache_endpoint,l.status,l.last_seen_at,l.size_bytes,l.last_error,l.created_at,l.updated_at
FROM fugue_image_locations l JOIN unnest($1::text[],$2::text[],$3::text[],$4::text[]) AS wanted(tenant_id,app_id,runtime_id,image_ref)
ON l.tenant_id=wanted.tenant_id AND l.app_id=wanted.app_id AND l.runtime_id=wanted.runtime_id AND l.image_ref=wanted.image_ref
WHERE l.status IN ('present','pulling','missing','failed')
ORDER BY COALESCE(l.last_seen_at,l.updated_at) DESC,l.updated_at DESC`, tenants, apps, runtimes, refs)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanImageLocationRows(rows)
	}
	var out []model.ImageLocation
	err := s.withLockedState(false, func(state *model.State) error {
		for _, l := range state.ImageLocations {
			if !wanted[ImageLocationScope{l.TenantID, l.AppID, l.RuntimeID, l.ImageRef}] {
				continue
			}
			switch l.Status {
			case model.ImageLocationStatusPresent, model.ImageLocationStatusPulling, model.ImageLocationStatusMissing, model.ImageLocationStatusFailed:
				out = append(out, l)
			}
		}
		return ctx.Err()
	})
	sort.SliceStable(out, func(i, j int) bool {
		a, b := imageLocationSeenAt(out[i]), imageLocationSeenAt(out[j])
		if !a.Equal(b) {
			return a.After(b)
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out, err
}
