package store

import (
	"context"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

const maxPlatformProducerScopes = 32

// Discovery schedules readers only. Each reader independently validates the
// signed current policy and publication transaction before producing output.
func (s *Store) ListPlatformProducerScopes() ([]string, error) {
	scopes := []string{}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rows, err := s.db.QueryContext(ctx, `SELECT scope_key FROM fugue_platform_release_lanes
WHERE artifact_kind = $1 AND release_channel = 'shadow' AND active_release_id <> ''
  AND (scope_key = $2 OR scope_key LIKE $3)
ORDER BY scope_key LIMIT $4`, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, platformproducer.Scope+":%", maxPlatformProducerScopes+1)
		if err != nil {
			return nil, mapDBErr(err)
		}
		defer rows.Close()
		for rows.Next() {
			var scope string
			if err := rows.Scan(&scope); err != nil {
				return nil, err
			}
			scopes = append(scopes, scope)
		}
		if err := rows.Err(); err != nil {
			return nil, mapDBErr(err)
		}
	} else {
		if err := s.withLockedState(false, func(state *model.State) error {
			for _, lane := range state.PlatformReleaseLanes {
				if lane.ArtifactKind == model.PlatformArtifactKindPolicySnapshot && lane.ReleaseChannel == model.PlatformArtifactReleaseChannelShadow && lane.ActiveReleaseID != "" && platformproducer.IsPolicyScope(lane.ScopeKey) {
					scopes = append(scopes, lane.ScopeKey)
					if len(scopes) > maxPlatformProducerScopes {
						return ErrConflict
					}
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if len(scopes) > maxPlatformProducerScopes {
		return nil, ErrConflict
	}
	sort.Strings(scopes)
	return scopes, nil
}
