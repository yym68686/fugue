package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"fugue/internal/model"
)

type platformStateDB interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *Store) pgCreatePlatformArtifact(artifact model.PlatformArtifact) (model.PlatformArtifact, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	defer tx.Rollback()
	if _, err := pgUpsertPlatformArtifactContent(ctx, tx, buildPlatformArtifactContent(artifact)); err != nil {
		return model.PlatformArtifact{}, err
	}
	out, err := pgInsertPlatformArtifact(ctx, tx, artifact)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.PlatformArtifact{}, err
	}
	return out, nil
}

func pgUpsertPlatformArtifactContent(ctx context.Context, db platformStateDB, content model.PlatformArtifactContent) (model.PlatformArtifactContent, error) {
	contentJSON, err := marshalJSON(content.Content)
	if err != nil {
		return model.PlatformArtifactContent{}, err
	}
	out, err := scanPlatformArtifactContent(db.QueryRowContext(ctx, `
INSERT INTO fugue_platform_artifact_contents (
	content_hash, content_json, size_bytes, created_at, updated_at
) VALUES (
	$1, $2::jsonb, $3, $4, $5
) ON CONFLICT (content_hash) DO UPDATE SET
	content_json = EXCLUDED.content_json,
	size_bytes = EXCLUDED.size_bytes,
	updated_at = EXCLUDED.updated_at
RETURNING content_hash, content_json, size_bytes, created_at, updated_at`,
		content.ContentHash, contentJSON, content.SizeBytes, content.CreatedAt, content.UpdatedAt))
	if err != nil {
		return model.PlatformArtifactContent{}, mapDBErr(err)
	}
	return out, nil
}

func pgInsertPlatformArtifact(ctx context.Context, db platformStateDB, artifact model.PlatformArtifact) (model.PlatformArtifact, error) {
	scopeJSON, err := marshalJSON(artifact.Scope)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	contentJSON, err := marshalJSON(artifact.Content)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	validationJSON, err := marshalJSON(artifact.ValidationResults)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	metadataJSON, err := marshalJSON(artifact.Metadata)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	out, err := scanPlatformArtifact(db.QueryRowContext(ctx, `
INSERT INTO fugue_platform_artifacts (
	id, artifact_kind, scope_key, scope_json, generation, status, content_hash,
	content_json, validation_results_json, compatibility_floor, metadata_json,
	created_by_type, created_by_id, created_at, updated_at
) VALUES (
	$1, $2, $3, $4::jsonb, $5, $6, $7,
	$8::jsonb, $9::jsonb, $10, $11::jsonb,
	$12, $13, $14, $15
) RETURNING id, artifact_kind, scope_key, scope_json, generation, status, content_hash,
	content_json, validation_results_json, compatibility_floor, metadata_json,
	created_by_type, created_by_id, created_at, updated_at`,
		artifact.ID, artifact.ArtifactKind, artifact.ScopeKey, scopeJSON, artifact.Generation, artifact.Status, artifact.ContentHash,
		contentJSON, validationJSON, artifact.CompatibilityFloor, metadataJSON,
		artifact.CreatedByType, artifact.CreatedByID, artifact.CreatedAt, artifact.UpdatedAt))
	if err != nil {
		return model.PlatformArtifact{}, mapDBErr(err)
	}
	return out, nil
}

func (s *Store) pgGetPlatformArtifactContent(contentHash string) (model.PlatformArtifactContent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	content, err := scanPlatformArtifactContent(s.db.QueryRowContext(ctx, `
SELECT content_hash, content_json, size_bytes, created_at, updated_at
FROM fugue_platform_artifact_contents
WHERE content_hash = $1`, contentHash))
	if err != nil {
		return model.PlatformArtifactContent{}, mapDBErr(err)
	}
	return content, nil
}

func (s *Store) pgListPlatformArtifacts(filter model.PlatformArtifactFilter) ([]model.PlatformArtifact, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	args := []any{}
	query := `SELECT id, artifact_kind, scope_key, scope_json, generation, status, content_hash,
	content_json, validation_results_json, compatibility_floor, metadata_json,
	created_by_type, created_by_id, created_at, updated_at
FROM fugue_platform_artifacts WHERE true`
	if filter.ArtifactKind != "" {
		args = append(args, filter.ArtifactKind)
		query += " AND artifact_kind = $" + formatStoreArgIndex(len(args))
	}
	if filter.ScopeKey != "" {
		args = append(args, filter.ScopeKey)
		query += " AND scope_key = $" + formatStoreArgIndex(len(args))
	}
	if filter.Status != "" {
		args = append(args, filter.Status)
		query += " AND status = $" + formatStoreArgIndex(len(args))
	}
	args = append(args, limit)
	query += " ORDER BY updated_at DESC, id ASC LIMIT $" + formatStoreArgIndex(len(args))
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	artifacts := []model.PlatformArtifact{}
	for rows.Next() {
		artifact, err := scanPlatformArtifact(rows)
		if err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return artifacts, nil
}

func (s *Store) pgGetPlatformArtifact(idOrGeneration string) (model.PlatformArtifact, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	artifact, err := pgGetPlatformArtifactForUpdate(ctx, s.db, idOrGeneration, false)
	if err != nil {
		return model.PlatformArtifact{}, mapDBErr(err)
	}
	return artifact, nil
}

func pgGetPlatformArtifactForUpdate(ctx context.Context, db platformStateDB, idOrGeneration string, forUpdate bool) (model.PlatformArtifact, error) {
	query := `SELECT id, artifact_kind, scope_key, scope_json, generation, status, content_hash,
	content_json, validation_results_json, compatibility_floor, metadata_json,
	created_by_type, created_by_id, created_at, updated_at
FROM fugue_platform_artifacts
WHERE id = $1 OR generation = $1
ORDER BY updated_at DESC, id ASC
LIMIT 1`
	if forUpdate {
		query += " FOR UPDATE"
	}
	return scanPlatformArtifact(db.QueryRowContext(ctx, query, idOrGeneration))
}

func (s *Store) pgValidatePlatformArtifact(id string, results []model.PlatformArtifactValidationResult) (model.PlatformArtifact, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	validationJSON, err := marshalJSON(results)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	status := platformArtifactStatusFromValidation(results)
	out, err := scanPlatformArtifact(s.db.QueryRowContext(ctx, `
UPDATE fugue_platform_artifacts
SET status = $2, validation_results_json = $3::jsonb, updated_at = $4
WHERE id = $1 OR generation = $1
RETURNING id, artifact_kind, scope_key, scope_json, generation, status, content_hash,
	content_json, validation_results_json, compatibility_floor, metadata_json,
	created_by_type, created_by_id, created_at, updated_at`, id, status, validationJSON, time.Now().UTC()))
	if err != nil {
		return model.PlatformArtifact{}, mapDBErr(err)
	}
	return out, nil
}

func (s *Store) pgReleasePlatformArtifact(id string, req model.PlatformArtifactReleaseRequest, principal model.Principal) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	defer tx.Rollback()
	artifact, err := pgGetPlatformArtifactForUpdate(ctx, tx, id, true)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, mapDBErr(err)
	}
	if artifact.Status != model.PlatformArtifactStatusValidated && !req.ForcePublish {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrConflict
	}
	now := time.Now().UTC()
	channel := NormalizePlatformReleaseChannel(req.ReleaseChannel)
	release := buildPlatformArtifactRelease(artifact, channel, "", req.CanaryRuleRef, req.Reason, principal, now)
	if err := pgSupersedePlatformReleases(ctx, tx, artifact.ArtifactKind, artifact.ScopeKey, channel, now); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	if _, err := pgInsertPlatformArtifactRelease(ctx, tx, release); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	message := buildPlatformReleaseMessage(artifact, release, model.PlatformReleaseMessageTypeRelease, now)
	if _, err := pgInsertPlatformReleaseMessage(ctx, tx, message); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	var lkg *model.PlatformLKGSnapshot
	if channel == model.PlatformArtifactReleaseChannelFull {
		snapshot := buildPlatformLKGSnapshot(artifact, now)
		if _, err := pgUpsertPlatformLKGSnapshot(ctx, tx, snapshot); err != nil {
			return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
		}
		lkg = &snapshot
	}
	if err := tx.Commit(); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	return artifact, release, message, lkg, nil
}

func (s *Store) pgRollbackPlatformArtifact(id string, req model.PlatformArtifactRollbackRequest, principal model.Principal) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	defer tx.Rollback()
	current, err := pgGetPlatformArtifactForUpdate(ctx, tx, id, true)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, mapDBErr(err)
	}
	target, err := pgGetPlatformArtifactByGenerationForUpdate(ctx, tx, current.ArtifactKind, current.ScopeKey, req.ToGeneration)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, mapDBErr(err)
	}
	if target.Status != model.PlatformArtifactStatusValidated && !req.ForcePublish {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrConflict
	}
	now := time.Now().UTC()
	channel := NormalizePlatformReleaseChannel(req.ReleaseChannel)
	release := buildPlatformArtifactRelease(target, channel, current.Generation, req.CanaryRuleRef, req.Reason, principal, now)
	if err := pgSupersedePlatformReleases(ctx, tx, target.ArtifactKind, target.ScopeKey, channel, now); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	if _, err := pgInsertPlatformArtifactRelease(ctx, tx, release); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	message := buildPlatformReleaseMessage(target, release, model.PlatformReleaseMessageTypeRollback, now)
	if _, err := pgInsertPlatformReleaseMessage(ctx, tx, message); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	var lkg *model.PlatformLKGSnapshot
	if channel == model.PlatformArtifactReleaseChannelFull {
		snapshot := buildPlatformLKGSnapshot(target, now)
		if _, err := pgUpsertPlatformLKGSnapshot(ctx, tx, snapshot); err != nil {
			return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
		}
		lkg = &snapshot
	}
	if err := tx.Commit(); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	return target, release, message, lkg, nil
}

func pgGetPlatformArtifactByGenerationForUpdate(ctx context.Context, db platformStateDB, kind, scopeKey, generation string) (model.PlatformArtifact, error) {
	return scanPlatformArtifact(db.QueryRowContext(ctx, `
SELECT id, artifact_kind, scope_key, scope_json, generation, status, content_hash,
	content_json, validation_results_json, compatibility_floor, metadata_json,
	created_by_type, created_by_id, created_at, updated_at
FROM fugue_platform_artifacts
WHERE artifact_kind = $1 AND scope_key = $2 AND generation = $3
LIMIT 1
FOR UPDATE`, kind, scopeKey, generation))
}

func pgSupersedePlatformReleases(ctx context.Context, db platformStateDB, kind, scopeKey, channel string, now time.Time) error {
	_, err := db.ExecContext(ctx, `
UPDATE fugue_platform_artifact_releases
SET status = $5, updated_at = $6
WHERE artifact_kind = $1 AND scope_key = $2 AND release_channel = $3 AND status = $4`,
		kind, scopeKey, channel, model.PlatformArtifactReleaseStatusActive, model.PlatformArtifactReleaseStatusSuperseded, now)
	return mapDBErr(err)
}

func pgInsertPlatformArtifactRelease(ctx context.Context, db platformStateDB, release model.PlatformArtifactRelease) (model.PlatformArtifactRelease, error) {
	scopeJSON, err := marshalJSON(release.Scope)
	if err != nil {
		return model.PlatformArtifactRelease{}, err
	}
	out, err := scanPlatformArtifactRelease(db.QueryRowContext(ctx, `
INSERT INTO fugue_platform_artifact_releases (
	id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	release_channel, status, rollback_target_generation, canary_rule_ref, reason,
	released_by_type, released_by_id, released_at, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5::jsonb, $6,
	$7, $8, $9, $10, $11,
	$12, $13, $14, $15, $16
) RETURNING id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	release_channel, status, rollback_target_generation, canary_rule_ref, reason,
	released_by_type, released_by_id, released_at, created_at, updated_at`,
		release.ID, release.ArtifactID, release.ArtifactKind, release.ScopeKey, scopeJSON, release.Generation,
		release.ReleaseChannel, release.Status, release.RollbackTargetGeneration, release.CanaryRuleRef, release.Reason,
		release.ReleasedByType, release.ReleasedByID, release.ReleasedAt, release.CreatedAt, release.UpdatedAt))
	if err != nil {
		return model.PlatformArtifactRelease{}, mapDBErr(err)
	}
	return out, nil
}

func pgInsertPlatformReleaseMessage(ctx context.Context, db platformStateDB, message model.PlatformReleaseMessage) (model.PlatformReleaseMessage, error) {
	scopeJSON, err := marshalJSON(message.Scope)
	if err != nil {
		return model.PlatformReleaseMessage{}, err
	}
	out, err := scanPlatformReleaseMessage(db.QueryRowContext(ctx, `
INSERT INTO fugue_platform_release_messages (
	id, release_id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	release_channel, message_type, created_at, expires_at, ack_count
) VALUES (
	$1, $2, $3, $4, $5, $6::jsonb, $7,
	$8, $9, $10, $11, $12
) RETURNING id, release_id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	release_channel, message_type, created_at, expires_at, ack_count`,
		message.ID, message.ReleaseID, message.ArtifactID, message.ArtifactKind, message.ScopeKey, scopeJSON, message.Generation,
		message.ReleaseChannel, message.MessageType, message.CreatedAt, message.ExpiresAt, message.AckCount))
	if err != nil {
		return model.PlatformReleaseMessage{}, mapDBErr(err)
	}
	return out, nil
}

func pgUpsertPlatformLKGSnapshot(ctx context.Context, db platformStateDB, snapshot model.PlatformLKGSnapshot) (model.PlatformLKGSnapshot, error) {
	scopeJSON, err := marshalJSON(snapshot.Scope)
	if err != nil {
		return model.PlatformLKGSnapshot{}, err
	}
	out, err := scanPlatformLKGSnapshot(db.QueryRowContext(ctx, `
INSERT INTO fugue_platform_lkg_snapshots (
	id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	content_hash, expires_at, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5::jsonb, $6,
	$7, $8, $9, $10
) ON CONFLICT (artifact_kind, scope_key) DO UPDATE SET
	id = EXCLUDED.id,
	artifact_id = EXCLUDED.artifact_id,
	scope_json = EXCLUDED.scope_json,
	generation = EXCLUDED.generation,
	content_hash = EXCLUDED.content_hash,
	expires_at = EXCLUDED.expires_at,
	updated_at = EXCLUDED.updated_at
RETURNING id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	content_hash, expires_at, created_at, updated_at`,
		snapshot.ID, snapshot.ArtifactID, snapshot.ArtifactKind, snapshot.ScopeKey, scopeJSON, snapshot.Generation,
		snapshot.ContentHash, snapshot.ExpiresAt, snapshot.CreatedAt, snapshot.UpdatedAt))
	if err != nil {
		return model.PlatformLKGSnapshot{}, mapDBErr(err)
	}
	return out, nil
}

func (s *Store) pgGetActivePlatformArtifact(kind, scopeKey, channel string) (model.PlatformArtifact, model.PlatformArtifactRelease, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	release, err := scanPlatformArtifactRelease(s.db.QueryRowContext(ctx, `
SELECT id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	release_channel, status, rollback_target_generation, canary_rule_ref, reason,
	released_by_type, released_by_id, released_at, created_at, updated_at
FROM fugue_platform_artifact_releases
WHERE artifact_kind = $1 AND scope_key = $2 AND release_channel = $3 AND status = $4
ORDER BY released_at DESC, id ASC
LIMIT 1`, kind, scopeKey, channel, model.PlatformArtifactReleaseStatusActive))
	if err != nil {
		if mapDBErr(err) == ErrNotFound {
			return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, false, nil
		}
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, false, mapDBErr(err)
	}
	artifact, err := pgGetPlatformArtifactForUpdate(ctx, s.db, release.ArtifactID, false)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, false, mapDBErr(err)
	}
	return artifact, release, true, nil
}

func (s *Store) pgListPlatformReleaseMessages(kind, scopeKey string, since time.Time, limit int) ([]model.PlatformReleaseMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if limit <= 0 {
		limit = 50
	}
	args := []any{kind, scopeKey}
	query := `SELECT id, release_id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	release_channel, message_type, created_at, expires_at, ack_count
FROM fugue_platform_release_messages
WHERE artifact_kind = $1 AND scope_key = $2`
	if !since.IsZero() {
		args = append(args, since)
		query += " AND created_at > $" + formatStoreArgIndex(len(args))
	}
	args = append(args, limit)
	query += " ORDER BY created_at DESC, id ASC LIMIT $" + formatStoreArgIndex(len(args))
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	messages := []model.PlatformReleaseMessage{}
	for rows.Next() {
		message, err := scanPlatformReleaseMessage(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return messages, nil
}

func (s *Store) pgUpsertPlatformConsumerHeartbeat(consumer model.PlatformConsumerInstance) (model.PlatformConsumerInstance, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	supportedKindsJSON, err := marshalJSON(consumer.SupportedKinds)
	if err != nil {
		return model.PlatformConsumerInstance{}, err
	}
	out, err := scanPlatformConsumerInstance(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_platform_consumer_instances (
	id, consumer_id, component, node_id, artifact_kind, scope_key, supported_kinds_json,
	desired_generation, actual_generation, lkg_generation, apply_status, probe_status,
	serving_lkg, lkg_expired, last_error, last_heartbeat_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7::jsonb,
	$8, $9, $10, $11, $12,
	$13, $14, $15, $16, $17
) ON CONFLICT (consumer_id, artifact_kind, scope_key) DO UPDATE SET
	component = EXCLUDED.component,
	node_id = EXCLUDED.node_id,
	supported_kinds_json = EXCLUDED.supported_kinds_json,
	desired_generation = EXCLUDED.desired_generation,
	actual_generation = EXCLUDED.actual_generation,
	lkg_generation = EXCLUDED.lkg_generation,
	apply_status = EXCLUDED.apply_status,
	probe_status = EXCLUDED.probe_status,
	serving_lkg = EXCLUDED.serving_lkg,
	lkg_expired = EXCLUDED.lkg_expired,
	last_error = EXCLUDED.last_error,
	last_heartbeat_at = EXCLUDED.last_heartbeat_at,
	updated_at = EXCLUDED.updated_at
RETURNING id, consumer_id, component, node_id, artifact_kind, scope_key, supported_kinds_json,
	desired_generation, actual_generation, lkg_generation, apply_status, probe_status,
	serving_lkg, lkg_expired, last_error, last_heartbeat_at, updated_at`,
		consumer.ID, consumer.ConsumerID, consumer.Component, consumer.NodeID, consumer.ArtifactKind, consumer.ScopeKey, supportedKindsJSON,
		consumer.DesiredGeneration, consumer.ActualGeneration, consumer.LKGGeneration, consumer.ApplyStatus, consumer.ProbeStatus,
		consumer.ServingLKG, consumer.LKGExpired, consumer.LastError, consumer.LastHeartbeatAt, consumer.UpdatedAt))
	if err != nil {
		return model.PlatformConsumerInstance{}, mapDBErr(err)
	}
	return out, nil
}

func (s *Store) pgListPlatformConsumers(kind, scopeKey string) ([]model.PlatformConsumerInstance, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `
SELECT id, consumer_id, component, node_id, artifact_kind, scope_key, supported_kinds_json,
	desired_generation, actual_generation, lkg_generation, apply_status, probe_status,
	serving_lkg, lkg_expired, last_error, last_heartbeat_at, updated_at
FROM fugue_platform_consumer_instances
WHERE artifact_kind = $1 AND scope_key = $2
ORDER BY updated_at DESC, consumer_id ASC`, kind, scopeKey)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	consumers := []model.PlatformConsumerInstance{}
	for rows.Next() {
		consumer, err := scanPlatformConsumerInstance(rows)
		if err != nil {
			return nil, err
		}
		consumers = append(consumers, consumer)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return consumers, nil
}

func (s *Store) pgGetPlatformLKG(kind, scopeKey string) (*model.PlatformLKGSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snapshot, err := scanPlatformLKGSnapshot(s.db.QueryRowContext(ctx, `
SELECT id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	content_hash, expires_at, created_at, updated_at
FROM fugue_platform_lkg_snapshots
WHERE artifact_kind = $1 AND scope_key = $2
LIMIT 1`, kind, scopeKey))
	if err != nil {
		if mapDBErr(err) == ErrNotFound {
			return nil, nil
		}
		return nil, mapDBErr(err)
	}
	return &snapshot, nil
}

func scanPlatformArtifact(scanner sqlScanner) (model.PlatformArtifact, error) {
	var artifact model.PlatformArtifact
	var scopeRaw, contentRaw, validationRaw, metadataRaw []byte
	if err := scanner.Scan(
		&artifact.ID,
		&artifact.ArtifactKind,
		&artifact.ScopeKey,
		&scopeRaw,
		&artifact.Generation,
		&artifact.Status,
		&artifact.ContentHash,
		&contentRaw,
		&validationRaw,
		&artifact.CompatibilityFloor,
		&metadataRaw,
		&artifact.CreatedByType,
		&artifact.CreatedByID,
		&artifact.CreatedAt,
		&artifact.UpdatedAt,
	); err != nil {
		return model.PlatformArtifact{}, mapDBErr(err)
	}
	scope, err := decodeJSONValue[model.PlatformArtifactScope](scopeRaw)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	content, err := decodeJSONValue[map[string]any](contentRaw)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	validation, err := decodeJSONValue[[]model.PlatformArtifactValidationResult](validationRaw)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	metadata, err := decodeJSONValue[map[string]string](metadataRaw)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	artifact.Scope = scope
	artifact.Content = content
	artifact.ValidationResults = validation
	artifact.Metadata = metadata
	return artifact, nil
}

func scanPlatformArtifactContent(scanner sqlScanner) (model.PlatformArtifactContent, error) {
	var content model.PlatformArtifactContent
	var contentRaw []byte
	if err := scanner.Scan(
		&content.ContentHash,
		&contentRaw,
		&content.SizeBytes,
		&content.CreatedAt,
		&content.UpdatedAt,
	); err != nil {
		return model.PlatformArtifactContent{}, mapDBErr(err)
	}
	decoded, err := decodeJSONValue[map[string]any](contentRaw)
	if err != nil {
		return model.PlatformArtifactContent{}, err
	}
	content.Content = decoded
	return content, nil
}

func scanPlatformArtifactRelease(scanner sqlScanner) (model.PlatformArtifactRelease, error) {
	var release model.PlatformArtifactRelease
	var scopeRaw []byte
	if err := scanner.Scan(
		&release.ID,
		&release.ArtifactID,
		&release.ArtifactKind,
		&release.ScopeKey,
		&scopeRaw,
		&release.Generation,
		&release.ReleaseChannel,
		&release.Status,
		&release.RollbackTargetGeneration,
		&release.CanaryRuleRef,
		&release.Reason,
		&release.ReleasedByType,
		&release.ReleasedByID,
		&release.ReleasedAt,
		&release.CreatedAt,
		&release.UpdatedAt,
	); err != nil {
		return model.PlatformArtifactRelease{}, mapDBErr(err)
	}
	scope, err := decodeJSONValue[model.PlatformArtifactScope](scopeRaw)
	if err != nil {
		return model.PlatformArtifactRelease{}, err
	}
	release.Scope = scope
	return release, nil
}

func scanPlatformReleaseMessage(scanner sqlScanner) (model.PlatformReleaseMessage, error) {
	var message model.PlatformReleaseMessage
	var scopeRaw []byte
	if err := scanner.Scan(
		&message.ID,
		&message.ReleaseID,
		&message.ArtifactID,
		&message.ArtifactKind,
		&message.ScopeKey,
		&scopeRaw,
		&message.Generation,
		&message.ReleaseChannel,
		&message.MessageType,
		&message.CreatedAt,
		&message.ExpiresAt,
		&message.AckCount,
	); err != nil {
		return model.PlatformReleaseMessage{}, mapDBErr(err)
	}
	scope, err := decodeJSONValue[model.PlatformArtifactScope](scopeRaw)
	if err != nil {
		return model.PlatformReleaseMessage{}, err
	}
	message.Scope = scope
	return message, nil
}

func scanPlatformConsumerInstance(scanner sqlScanner) (model.PlatformConsumerInstance, error) {
	var consumer model.PlatformConsumerInstance
	var supportedRaw []byte
	if err := scanner.Scan(
		&consumer.ID,
		&consumer.ConsumerID,
		&consumer.Component,
		&consumer.NodeID,
		&consumer.ArtifactKind,
		&consumer.ScopeKey,
		&supportedRaw,
		&consumer.DesiredGeneration,
		&consumer.ActualGeneration,
		&consumer.LKGGeneration,
		&consumer.ApplyStatus,
		&consumer.ProbeStatus,
		&consumer.ServingLKG,
		&consumer.LKGExpired,
		&consumer.LastError,
		&consumer.LastHeartbeatAt,
		&consumer.UpdatedAt,
	); err != nil {
		return model.PlatformConsumerInstance{}, mapDBErr(err)
	}
	supported, err := decodeJSONValue[[]string](supportedRaw)
	if err != nil {
		return model.PlatformConsumerInstance{}, err
	}
	consumer.SupportedKinds = supported
	return consumer, nil
}

func scanPlatformLKGSnapshot(scanner sqlScanner) (model.PlatformLKGSnapshot, error) {
	var snapshot model.PlatformLKGSnapshot
	var scopeRaw []byte
	if err := scanner.Scan(
		&snapshot.ID,
		&snapshot.ArtifactID,
		&snapshot.ArtifactKind,
		&snapshot.ScopeKey,
		&scopeRaw,
		&snapshot.Generation,
		&snapshot.ContentHash,
		&snapshot.ExpiresAt,
		&snapshot.CreatedAt,
		&snapshot.UpdatedAt,
	); err != nil {
		return model.PlatformLKGSnapshot{}, mapDBErr(err)
	}
	scope, err := decodeJSONValue[model.PlatformArtifactScope](scopeRaw)
	if err != nil {
		return model.PlatformLKGSnapshot{}, err
	}
	snapshot.Scope = scope
	return snapshot, nil
}

func formatStoreArgIndex(index int) string {
	return fmt.Sprintf("%d", index)
}
