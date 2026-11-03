package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func (s *Store) validateRoutePolicyMigration(state *model.State, hostname string, next *model.EdgeRoutePolicy) error {
	var constraint *platformconfig.RoutePolicyConstraint
	if next != nil {
		base := platformconfig.PolicySnapshot{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", MinimumHealthyEdges: 1, MaxStaleSeconds: 60}
		p, err := platformconfig.ProjectPolicySnapshot(base, []model.EdgeRoutePolicy{*next}, nil, "route-write-preflight")
		if err != nil {
			return fmt.Errorf("%w: route constraint projection invalid", ErrConflict)
		}
		constraint = &p.RouteConstraints[0]
	}
	for _, lane := range state.PlatformReleaseLanes {
		if lane.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || lane.ReleaseChannel != "shadow" || lane.ActiveReleaseID == "" || !platformproducer.IsPolicyScope(lane.ScopeKey) {
			continue
		}
		ri := platformArtifactReleaseIndex(state.PlatformArtifactReleases, lane.ActiveReleaseID)
		if ri < 0 {
			return ErrConflict
		}
		r := state.PlatformArtifactReleases[ri]
		ai := platformArtifactIndex(state.PlatformArtifacts, r.ArtifactID)
		if ai < 0 || r.FencingToken != lane.FencingToken || r.Status != model.PlatformArtifactReleaseStatusActive {
			return ErrConflict
		}
		a := state.PlatformArtifacts[ai]
		p, err := platformproducer.Decode(a)
		if err != nil || a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, s.platformArtifactSigningKeyring()).Pass {
			return ErrConflict
		}
		if err := platformproducer.ValidateRouteConstraintMutation(p, hostname, constraint); err != nil {
			return fmt.Errorf("%w: %s: %s", ErrConflict, lane.ScopeKey, err)
		}
	}
	return nil
}

// The guard runs in the write transaction. Shared authority locks serialize it
// with producer replacement; bounded discovery never reads publication history.
func (s *Store) pgGuardRoutePolicyMigration(ctx context.Context, tx *sql.Tx, hostname string, next *model.EdgeRoutePolicy) error {
	rows, err := tx.QueryContext(ctx, `SELECT lane_key, artifact_kind, scope_key, release_channel, fencing_token, version, active_release_id, frozen, freeze_reason, updated_at FROM fugue_platform_release_lanes WHERE artifact_kind=$1 AND release_channel='shadow' AND active_release_id<>'' AND (scope_key=$2 OR scope_key LIKE $3) ORDER BY scope_key LIMIT $4`, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, platformproducer.Scope+":%", maxPlatformProducerScopes+1)
	if err != nil {
		return err
	}
	state := &model.State{}
	for rows.Next() {
		lane, err := scanPlatformReleaseLane(rows)
		if err != nil {
			rows.Close()
			return err
		}
		state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, lane)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(state.PlatformReleaseLanes) > maxPlatformProducerScopes {
		return ErrConflict
	}
	read := pgDNSRouteSourceReader(ctx, tx)
	for i, lane := range state.PlatformReleaseLanes {
		if err := pgTrySharedPromotionScope(ctx, tx, lane.ScopeKey); err != nil {
			return err
		}
		// Discovery can race selection. Re-read under the authority lock.
		lane, err = scanPlatformReleaseLane(tx.QueryRowContext(ctx, `SELECT lane_key, artifact_kind, scope_key, release_channel, fencing_token, version, active_release_id, frozen, freeze_reason, updated_at FROM fugue_platform_release_lanes WHERE lane_key=$1 FOR SHARE`, lane.LaneKey))
		if err != nil {
			return err
		}
		state.PlatformReleaseLanes[i] = lane
		r, err := read.release(lane.ActiveReleaseID)
		if err != nil {
			return err
		}
		a, err := read.artifact(r.ArtifactID)
		if err != nil {
			return err
		}
		state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, r)
		state.PlatformArtifacts = append(state.PlatformArtifacts, a)
	}
	if next != nil {
		old, err := scanEdgeRoutePolicy(tx.QueryRowContext(ctx, `SELECT `+edgeRoutePolicySelectColumns+` FROM fugue_edge_route_policies WHERE hostname=$1`, hostname))
		if err == nil {
			next.ID = old.ID
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	return s.validateRoutePolicyMigration(state, hostname, next)
}
