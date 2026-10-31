package store

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

// The producer already holds the target scope, full lane and LKG locks. Repeat
// retention's source/fence/expiry checks at gray, full and verification, so a
// slow rollout cannot publish expired evidence or rebase it to a new baseline.
func validateRetainedTrafficBaseline(state *model.State, parent model.PlatformArtifact, baseline model.PlatformArtifactRelease, lkg model.PlatformLKGSnapshot, now time.Time, keys bundleauth.Keyring) error {
	ids, ok := parent.Content["artifact_ids"].([]any)
	if !ok {
		return ErrConflict
	}
	// A source route artifact can contain many failed applications. Decode and
	// index it once per admission, rather than rescanning every route per app.
	routesBySource := map[string]map[string][]platformconfig.CompiledRoute{}
	for _, rawID := range ids {
		id, ok := rawID.(string)
		if !ok {
			return ErrConflict
		}
		index := platformArtifactIndex(state.PlatformArtifacts, id)
		if index < 0 {
			return ErrConflict
		}
		child := state.PlatformArtifacts[index]
		if child.ArtifactKind != model.PlatformArtifactKindEdgeRouteBundle {
			continue
		}
		parts, err := platformconfig.RetainedTrafficPartitionsFromArtifact(child)
		if err != nil {
			return ErrConflict
		}
		for _, part := range parts {
			pin := part.Source
			if pin.ArtifactID != lkg.ArtifactID || pin.ContentHash != lkg.ContentHash || pin.ReleaseID != baseline.ID || pin.FencingToken != baseline.FencingToken || !part.ExpiresAt.After(now) || part.ExpiresAt.After(lkg.ExpiresAt) {
				return fmt.Errorf("%w: retained traffic partition baseline changed or evidence expired", ErrConflict)
			}
			parentIndex := platformArtifactIndex(state.PlatformArtifacts, pin.ArtifactID)
			if parentIndex < 0 {
				return fmt.Errorf("%w: retained traffic source parent unavailable", ErrConflict)
			}
			sourceParent := state.PlatformArtifacts[parentIndex]
			member := false
			memberIDs, _ := sourceParent.Content["artifact_ids"].([]any)
			for _, id := range memberIDs {
				member = member || id == part.RouteArtifactID
			}
			if !member || sourceParent.Status != model.PlatformArtifactStatusValidated || sourceParent.ContentHash != pin.ContentHash || !platformsafety.EvaluateArtifactIntegrity(sourceParent, keys).Pass {
				return fmt.Errorf("%w: retained traffic source membership revoked or changed", ErrConflict)
			}
			sourceIndex := platformArtifactIndex(state.PlatformArtifacts, part.RouteArtifactID)
			if sourceIndex < 0 {
				return fmt.Errorf("%w: retained traffic source route unavailable", ErrConflict)
			}
			source := state.PlatformArtifacts[sourceIndex]
			if source.ArtifactKind != model.PlatformArtifactKindEdgeRouteBundle || source.ScopeKey != parent.ScopeKey || source.Status != model.PlatformArtifactStatusValidated || source.ContentHash != part.RouteArtifactDigest || !platformsafety.EvaluateArtifactIntegrity(source, keys).Pass {
				return fmt.Errorf("%w: retained traffic source route revoked or changed", ErrConflict)
			}
			byOwner, indexed := routesBySource[source.ID]
			if !indexed {
				var routes []platformconfig.CompiledRoute
				raw, err := json.Marshal(source.Content["routes"])
				if err != nil || json.Unmarshal(raw, &routes) != nil {
					return ErrConflict
				}
				byOwner = map[string][]platformconfig.CompiledRoute{}
				for _, route := range routes {
					key := route.TenantID + "\x00" + route.AppID
					byOwner[key] = append(byOwner[key], route)
				}
				routesBySource[source.ID] = byOwner
			}
			exact := byOwner[part.TenantID+"\x00"+part.AppID]
			if !reflect.DeepEqual(platformconfig.NormalizeTrafficPartitionRoutes(exact), part.Routes) {
				return fmt.Errorf("%w: retained traffic source routes differ", ErrConflict)
			}
		}
	}
	return nil
}

func retainedTrafficSourceRouteIDs(child model.PlatformArtifact) ([]string, error) {
	parts, err := platformconfig.RetainedTrafficPartitionsFromArtifact(child)
	if err != nil {
		return nil, ErrConflict
	}
	ids := []string{}
	seen := map[string]bool{}
	for _, part := range parts {
		if part.RouteArtifactID == "" {
			return nil, ErrConflict
		}
		if !seen[part.RouteArtifactID] {
			ids = append(ids, part.RouteArtifactID)
			seen[part.RouteArtifactID] = true
		}
	}
	return ids, nil
}

// Apply expiration to every serving admission, including manual release and
// rollback paths, not only the automatic producer's phase guard.
func validateRetainedTrafficExpiry(child model.PlatformArtifact, now time.Time) error {
	parts, err := platformconfig.RetainedTrafficPartitionsFromArtifact(child)
	if err != nil {
		return fmt.Errorf("%w: retained traffic partition evidence invalid", ErrConflict)
	}
	for _, part := range parts {
		if !part.ExpiresAt.After(now) {
			return fmt.Errorf("%w: retained traffic partition evidence expired", ErrConflict)
		}
	}
	return nil
}
