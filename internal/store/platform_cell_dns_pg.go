package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func (s *Store) pgLoadCellDNSReferences(ctx context.Context, tx *sql.Tx, parent model.PlatformArtifact, state *model.State) error {
	if parent.Content["publication_role"] != platformconfig.PublicationRoleCellDNS {
		return nil
	}
	var dependencies []platformconfig.DNSRouteDependency
	for _, child := range state.PlatformArtifacts {
		if child.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || child.ScopeKey != parent.ScopeKey {
			continue
		}
		var err error
		dependencies, err = platformconfig.DNSRouteDependencies(child)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrConflict, err)
		}
	}
	if len(dependencies) == 0 {
		return ErrConflict
	}
	// The DNS transaction already owns its own scope. Never wait on a routing
	// scope while holding that lock: a concurrent cross-scope mutation causes a
	// bounded conflict and preserves the prior positive publication instead.
	lockedScopes := map[string]bool{}
	for _, p := range dependencies {
		if lockedScopes[p.Parent.ScopeKey] {
			continue
		}
		lockedScopes[p.Parent.ScopeKey] = true
		sum := sha256.Sum256([]byte("fugue.platform.release-set-promotion/v1\x00" + p.Parent.ScopeKey))
		var locked bool
		if err := tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock_shared($1)", int64(binary.BigEndian.Uint64(sum[:8]))).Scan(&locked); err != nil {
			return err
		}
		if !locked {
			return fmt.Errorf("%w: referenced Cell publication is changing", ErrConflict)
		}
	}
	loadedParents := map[string]bool{}
	for _, p := range dependencies {
		if loadedParents[p.Parent.ID] {
			continue
		}
		loadedParents[p.Parent.ID] = true
		stored, err := pgGetPlatformArtifactForUpdate(ctx, tx, p.Parent.ID, false)
		if err != nil {
			return err
		}
		if stored.Content["publication_role"] != p.Parent.Content["publication_role"] || stored.ScopeKey != p.Parent.ScopeKey || stored.ContentHash != p.Parent.ContentHash {
			return ErrConflict
		}
		view, err := s.pgFullReleaseSetSnapshot(ctx, tx, stored)
		if err != nil {
			return err
		}
		for _, r := range view.PlatformArtifactReleases {
			if platformArtifactIndex(view.PlatformArtifacts, r.ArtifactID) >= 0 {
				continue
			}
			a, err := pgGetPlatformArtifactForUpdate(ctx, tx, r.ArtifactID, false)
			if err != nil {
				return err
			}
			view.PlatformArtifacts = append(view.PlatformArtifacts, a)
		}
		state.PlatformArtifacts = append(state.PlatformArtifacts, view.PlatformArtifacts...)
		state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, view.PlatformArtifactReleases...)
		state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, view.PlatformReleaseLanes...)
	}
	return nil
}
