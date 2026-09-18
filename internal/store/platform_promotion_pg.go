package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"

	"fugue/internal/model"
)

// This advisory lock is scoped to one configuration scope, not the database.
// Ordinary evidence/expectation writers share it; full publication excludes
// those writers until commit. Locks are acquired before their mutable reads.
func pgLockPromotionScope(ctx context.Context, tx *sql.Tx, scope string, exclusive bool) error {
	scope = strings.ToLower(strings.TrimSpace(scope))
	if scope == "" {
		return ErrInvalidInput
	}
	sum := sha256.Sum256([]byte("fugue.platform.release-set-promotion/v1\x00" + scope))
	key := int64(binary.BigEndian.Uint64(sum[:8]))
	query := "SELECT pg_advisory_xact_lock_shared($1)"
	if exclusive {
		query = "SELECT pg_advisory_xact_lock($1)"
	}
	_, err := tx.ExecContext(ctx, query, key)
	return err
}

func (s *Store) pgFullReleaseSetSnapshot(ctx context.Context, tx *sql.Tx, parent model.PlatformArtifact) (*model.State, error) {
	state := &model.State{PlatformArtifacts: []model.PlatformArtifact{parent}}
	ids, ok := parent.Content["artifact_ids"].([]any)
	if !ok || len(ids) == 0 || len(ids) > 64 {
		return nil, fmt.Errorf("%w: release set members invalid", ErrConflict)
	}
	for _, raw := range ids {
		id, ok := raw.(string)
		if !ok || id == parent.ID {
			return nil, ErrConflict
		}
		child, err := pgGetPlatformArtifactForUpdate(ctx, tx, id, true)
		if err != nil {
			return nil, err
		}
		state.PlatformArtifacts = append(state.PlatformArtifacts, child)
	}
	rows, err := tx.QueryContext(ctx, `SELECT lane_key, artifact_kind, scope_key, release_channel, fencing_token, version,
 active_release_id, frozen, freeze_reason, updated_at
 FROM fugue_platform_release_lanes WHERE artifact_kind=$1 AND scope_key=$2 ORDER BY lane_key FOR SHARE`, parent.ArtifactKind, parent.ScopeKey)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		lane, err := scanPlatformReleaseLane(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, lane)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for _, lane := range state.PlatformReleaseLanes {
		if lane.ActiveReleaseID == "" {
			continue
		}
		release, err := pgGetPlatformArtifactRelease(ctx, tx, lane.ActiveReleaseID, false)
		if err != nil {
			return nil, err
		}
		state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, release)
	}
	rows, err = tx.QueryContext(ctx, `SELECT id, release_set_id, artifact_release_id, artifact_kind, scope_key, scope_json,
 expected_generation, topology_revision, revision, requires_consumers,
 required_cardinality, optional_cardinality, heartbeat_deadline,
 convergence_deadline, consumers_json, created_at, updated_at
 FROM fugue_platform_expected_consumer_sets WHERE release_set_id=$1 AND scope_key=$2 ORDER BY artifact_kind, revision DESC`, parent.ID, parent.ScopeKey)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		set, err := scanPlatformExpectedConsumerSet(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		state.ExpectedConsumerSets = append(state.ExpectedConsumerSets, set)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	rows, err = tx.QueryContext(ctx, `SELECT id, consumer_id, credential_id, token_id, component, node_id, artifact_kind, scope_key,
 release_set_id, expected_consumer_set_id, fencing_token, supported_kinds_json,
 protocol_version, schema_version, compatibility_capabilities_json,
 sequence, issued_at, nonce, generation_sequence, evidence_hash, identity_verified,
 desired_generation, actual_generation, candidate_generation, lkg_generation, apply_status, probe_status,
 serving_lkg, lkg_expired, last_error, last_heartbeat_at, updated_at
 FROM fugue_platform_consumer_instances WHERE scope_key=$1 ORDER BY artifact_kind, consumer_id FOR SHARE`, parent.ScopeKey)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		consumer, err := scanPlatformConsumerInstance(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		state.PlatformConsumerInstances = append(state.PlatformConsumerInstances, consumer)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	return state, nil
}

// Resolve immutable identity without a row lock, acquire the scope lock first,
// then let the caller lock/reload the artifact. All publication paths use this
// ordering, including rollback between two different ReleaseSets.
func pgLockReleaseSetMutation(ctx context.Context, tx *sql.Tx, id string, exclusive bool) error {
	var kind, scope string
	if err := tx.QueryRowContext(ctx, `SELECT artifact_kind, scope_key FROM fugue_platform_artifacts WHERE id=$1 OR generation=$1 ORDER BY updated_at DESC, id ASC LIMIT 1`, id).Scan(&kind, &scope); err != nil {
		return mapDBErr(err)
	}
	if kind != model.PlatformArtifactKindReleaseSet {
		return nil
	}
	return pgLockPromotionScope(ctx, tx, scope, exclusive)
}
