package store

import (
	"context"
	"database/sql"
	"fmt"
	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

func validateTrafficCanary(parent model.PlatformArtifact, ref string, artifacts []model.PlatformArtifact, keyring bundleauth.Keyring) error {
	if _, err := platformconfig.ResolveTrafficCanary(parent, ref); err != nil {
		return fmt.Errorf("%w: %v", ErrConflict, err)
	}
	ids, ok := parent.Content["artifact_ids"].([]any)
	kinds, kindOK := parent.Content["artifact_kinds"].([]any)
	if !ok || !kindOK || len(ids) != 3 || len(kinds) != 3 {
		return fmt.Errorf("%w: traffic canary members missing", ErrConflict)
	}
	seen := map[string]bool{}
	for i, raw := range ids {
		id, ok := raw.(string)
		kind, kok := kinds[i].(string)
		if !ok || !kok || seen[kind] {
			return ErrConflict
		}
		seen[kind] = true
		var child *model.PlatformArtifact
		for j := range artifacts {
			if artifacts[j].ID == id {
				child = &artifacts[j]
				break
			}
		}
		if child == nil || child.ArtifactKind != kind || child.ScopeKey != parent.ScopeKey || child.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(*child, keyring).Pass {
			return ErrConflict
		}
		if err := platformconfig.ValidateTrafficCohortProjection(parent, *child); err != nil {
			return fmt.Errorf("%w: %v", ErrConflict, err)
		}
	}
	if !seen[model.PlatformArtifactKindEdgeRouteBundle] || !seen[model.PlatformArtifactKindDNSAnswerBundle] || !seen[model.PlatformArtifactKindCaddyRouteConfig] {
		return ErrConflict
	}
	return nil
}

func (s *Store) pgValidateTrafficCanary(ctx context.Context, tx *sql.Tx, parent model.PlatformArtifact, ref string) error {
	ids, ok := parent.Content["artifact_ids"].([]any)
	if !ok || len(ids) != 3 {
		return ErrConflict
	}
	children := make([]model.PlatformArtifact, 0, len(ids))
	for _, raw := range ids {
		id, ok := raw.(string)
		if !ok || id == parent.ID {
			return ErrConflict
		}
		child, err := pgGetPlatformArtifactForUpdate(ctx, tx, id, true)
		if err != nil {
			return err
		}
		children = append(children, child)
	}
	return validateTrafficCanary(parent, ref, children, s.platformArtifactSigningKeyring())
}
