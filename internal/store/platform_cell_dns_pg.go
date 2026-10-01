package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

func (s *Store) pgLoadCellDNSReferences(ctx context.Context, tx *sql.Tx, parent model.PlatformArtifact, state *model.State) error {
	if parent.Content["publication_role"] != platformconfig.PublicationRoleCellDNS {
		return nil
	}
	var dependencies []platformconfig.DNSRouteDependency
	var producerArtifacts, producerReleases []string
	for _, child := range state.PlatformArtifacts {
		if child.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || child.ScopeKey != parent.ScopeKey {
			continue
		}
		var err error
		dependencies, err = platformconfig.DNSRouteDependencies(child)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrConflict, err)
		}
		sources, err := platformconfig.DNSRouteSourceAuthorizations(child)
		if err != nil {
			return err
		}
		for _, source := range sources {
			producerArtifacts = append(producerArtifacts, source.PolicyArtifactID)
		}
		if len(sources) > 0 {
			cells, err := platformconfig.DecodeCellRoutePublications(child)
			if err != nil {
				return err
			}
			for _, cell := range cells {
				if cell.ProducerPolicy == nil {
					return ErrConflict
				}
				producerReleases = append(producerReleases, cell.ProducerPolicy.ReleaseID)
			}
			previous, err := platformconfig.DecodePreviousTrafficPublication(child)
			if err != nil {
				return err
			}
			if previous != nil {
				if previous.ProducerPolicy == nil {
					return ErrConflict
				}
				producerReleases = append(producerReleases, previous.ProducerPolicy.ReleaseID)
			}
		}
	}
	if len(dependencies) == 0 {
		return ErrConflict
	}
	// The DNS transaction already owns its own scope. Never wait on a routing
	// scope while holding that lock: a concurrent cross-scope mutation causes a
	// bounded conflict and preserves the prior positive publication instead.
	lockedScopes := map[string]bool{}
	scopes := []string{}
	for _, dependency := range dependencies {
		scopes = append(scopes, dependency.Parent.ScopeKey)
		if len(producerArtifacts) > 0 {
			policyScope, err := platformproducer.PolicyScopeForTarget(dependency.Parent.ScopeKey)
			if err != nil {
				return err
			}
			scopes = append(scopes, policyScope)
		}
	}
	for _, scope := range scopes {
		if lockedScopes[scope] {
			continue
		}
		lockedScopes[scope] = true
		sum := sha256.Sum256([]byte("fugue.platform.release-set-promotion/v1\x00" + scope))
		var locked bool
		if err := tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock_shared($1)", int64(binary.BigEndian.Uint64(sum[:8]))).Scan(&locked); err != nil {
			return err
		}
		if !locked {
			return fmt.Errorf("%w: referenced Cell publication is changing", ErrConflict)
		}
	}
	for _, id := range producerArtifacts {
		if platformArtifactIndex(state.PlatformArtifacts, id) >= 0 {
			continue
		}
		artifact, err := pgGetPlatformArtifactForUpdate(ctx, tx, id, false)
		if err != nil {
			return err
		}
		state.PlatformArtifacts = append(state.PlatformArtifacts, artifact)
	}
	for _, id := range producerReleases {
		if platformArtifactReleaseIndex(state.PlatformArtifactReleases, id) >= 0 {
			continue
		}
		release, err := pgGetPlatformArtifactRelease(ctx, tx, id, false)
		if err != nil {
			return err
		}
		state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, release)
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
