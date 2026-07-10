package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformsafety"
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
	artifact.GenerationSequence, err = pgNextPlatformArtifactGenerationSequence(
		ctx,
		tx,
		artifact.ArtifactKind,
		artifact.ScopeKey,
		artifact.UpdatedAt,
	)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	artifact, err = platformsafety.SignPlatformArtifact(artifact, s.platformArtifactSigningKeyring())
	if err != nil {
		return model.PlatformArtifact{}, err
	}
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

func pgNextPlatformArtifactGenerationSequence(
	ctx context.Context,
	db platformStateDB,
	kind string,
	scopeKey string,
	now time.Time,
) (int64, error) {
	if _, err := db.ExecContext(ctx, `
INSERT INTO fugue_platform_artifact_generation_sequences (
	artifact_kind, scope_key, last_sequence, updated_at
) VALUES ($1, $2, 0, $3)
ON CONFLICT (artifact_kind, scope_key) DO NOTHING`,
		kind, scopeKey, now); err != nil {
		return 0, mapDBErr(err)
	}
	var sequence int64
	if err := db.QueryRowContext(ctx, `
UPDATE fugue_platform_artifact_generation_sequences
SET last_sequence = GREATEST(
		last_sequence,
		COALESCE((
			SELECT MAX(generation_sequence)
			FROM fugue_platform_artifacts
			WHERE artifact_kind = $1 AND scope_key = $2
		), 0)
	) + 1,
	updated_at = $3
WHERE artifact_kind = $1 AND scope_key = $2
RETURNING last_sequence`,
		kind, scopeKey, now).Scan(&sequence); err != nil {
		return 0, mapDBErr(err)
	}
	return sequence, nil
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
	provenanceJSON, err := marshalJSON(artifact.Provenance)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	out, err := scanPlatformArtifact(db.QueryRowContext(ctx, `
INSERT INTO fugue_platform_artifacts (
	id, artifact_kind, scope_key, scope_json, schema_version, generation, generation_sequence, status, content_hash,
	content_json, validation_results_json, compatibility_floor, metadata_json,
	created_by_type, created_by_id, provenance_json, created_at, updated_at
) VALUES (
	$1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9,
	$10::jsonb, $11::jsonb, $12, $13::jsonb,
	$14, $15, $16::jsonb, $17, $18
) RETURNING id, artifact_kind, scope_key, scope_json, schema_version, generation, generation_sequence, status, content_hash,
	content_json, validation_results_json, compatibility_floor, metadata_json,
	created_by_type, created_by_id, provenance_json, created_at, updated_at`,
		artifact.ID, artifact.ArtifactKind, artifact.ScopeKey, scopeJSON, artifact.SchemaVersion, artifact.Generation, artifact.GenerationSequence, artifact.Status, artifact.ContentHash,
		contentJSON, validationJSON, artifact.CompatibilityFloor, metadataJSON,
		artifact.CreatedByType, artifact.CreatedByID, provenanceJSON, artifact.CreatedAt, artifact.UpdatedAt))
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
	query := `SELECT id, artifact_kind, scope_key, scope_json, schema_version, generation, generation_sequence, status, content_hash,
	content_json, validation_results_json, compatibility_floor, metadata_json,
	created_by_type, created_by_id, provenance_json, created_at, updated_at
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
	query := `SELECT id, artifact_kind, scope_key, scope_json, schema_version, generation, generation_sequence, status, content_hash,
	content_json, validation_results_json, compatibility_floor, metadata_json,
	created_by_type, created_by_id, provenance_json, created_at, updated_at
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
RETURNING id, artifact_kind, scope_key, scope_json, schema_version, generation, generation_sequence, status, content_hash,
	content_json, validation_results_json, compatibility_floor, metadata_json,
	created_by_type, created_by_id, provenance_json, created_at, updated_at`, id, status, validationJSON, time.Now().UTC()))
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
	now := time.Now().UTC()
	channel := NormalizePlatformReleaseChannel(req.ReleaseChannel)
	laneKey := platformsafety.ReleaseLaneKey(artifact.ArtifactKind, artifact.ScopeKey, channel)
	if strings.TrimSpace(req.IdempotencyKey) != "" {
		if existingRelease, found, err := pgGetPlatformArtifactReleaseByIdempotency(ctx, tx, laneKey, req.IdempotencyKey); err != nil {
			return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
		} else if found {
			existingArtifact, err := pgGetPlatformArtifactForUpdate(ctx, tx, existingRelease.ArtifactID, false)
			if err != nil {
				return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, mapDBErr(err)
			}
			existingMessage, err := pgGetLatestPlatformReleaseMessage(ctx, tx, existingRelease.ID)
			if err != nil {
				return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
			}
			existingLKG, err := s.pgGetVerifiedPlatformLKGForUpdate(ctx, tx, artifact.ArtifactKind, artifact.ScopeKey, now)
			if err != nil {
				return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
			}
			return existingArtifact, existingRelease, existingMessage, existingLKG, nil
		}
	}
	if artifact.Status != model.PlatformArtifactStatusValidated {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrConflict
	}
	lkg, err := s.pgGetVerifiedPlatformLKGForUpdate(ctx, tx, artifact.ArtifactKind, artifact.ScopeKey, now)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	pinnedRollbackGeneration := ""
	if lkg != nil {
		pinnedRollbackGeneration = lkg.Generation
	}
	lane, err := pgNextPlatformReleaseLane(ctx, tx, artifact.ArtifactKind, artifact.ScopeKey, channel, now)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	if strings.TrimSpace(req.IdempotencyKey) != "" {
		if existingRelease, found, err := pgGetPlatformArtifactReleaseByIdempotency(ctx, tx, laneKey, req.IdempotencyKey); err != nil {
			return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
		} else if found {
			existingArtifact, err := pgGetPlatformArtifactForUpdate(ctx, tx, existingRelease.ArtifactID, false)
			if err != nil {
				return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, mapDBErr(err)
			}
			existingMessage, err := pgGetLatestPlatformReleaseMessage(ctx, tx, existingRelease.ID)
			if err != nil {
				return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
			}
			return existingArtifact, existingRelease, existingMessage, lkg, nil
		}
	}
	previousGenerationSequence, err := pgActivePlatformReleaseGenerationSequence(ctx, tx, lane)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	if decision := platformsafety.EvaluateArtifactRelease(
		artifact,
		channel,
		pinnedRollbackGeneration,
		req.CanaryRuleRef,
		previousGenerationSequence,
		s.platformArtifactSigningKeyring(),
	); !decision.Pass {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrConflict
	}
	entry := buildPlatformArtifactReleaseLedgerEntry(
		artifact,
		channel,
		pinnedRollbackGeneration,
		req.CanaryRuleRef,
		req.Reason,
		req.IdempotencyKey,
		model.PlatformReleaseMessageTypeRelease,
		principal,
		lane,
		now,
	)
	release := entry.Release
	if err := pgSupersedePlatformReleases(ctx, tx, artifact.ArtifactKind, artifact.ScopeKey, channel, now); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	if _, err := pgInsertPlatformArtifactRelease(ctx, tx, release); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	message := entry.Message
	if _, err := pgInsertPlatformReleaseMessage(ctx, tx, message); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	if err := pgSetPlatformReleaseLaneActive(ctx, tx, lane.LaneKey, release.ID, lane.FencingToken, now); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
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
	if target.Status != model.PlatformArtifactStatusValidated {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrConflict
	}
	now := time.Now().UTC()
	channel := NormalizePlatformReleaseChannel(req.ReleaseChannel)
	lkg, err := s.pgGetVerifiedPlatformLKGForUpdate(ctx, tx, target.ArtifactKind, target.ScopeKey, now)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	pinnedRollbackGeneration := ""
	if lkg != nil {
		pinnedRollbackGeneration = lkg.Generation
	}
	lane, err := pgNextPlatformReleaseLane(ctx, tx, target.ArtifactKind, target.ScopeKey, channel, now)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	canaryRuleRef := strings.TrimSpace(req.CanaryRuleRef)
	if channel == model.PlatformArtifactReleaseChannelGray && canaryRuleRef == "" && strings.TrimSpace(lane.ActiveReleaseID) != "" {
		activeRelease, releaseErr := pgGetPlatformArtifactRelease(ctx, tx, lane.ActiveReleaseID, true)
		if releaseErr != nil {
			return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, releaseErr
		}
		if activeRelease.Status != model.PlatformArtifactReleaseStatusActive ||
			activeRelease.LaneKey != lane.LaneKey ||
			activeRelease.ReleaseChannel != channel {
			return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrConflict
		}
		canaryRuleRef = activeRelease.CanaryRuleRef
	}
	if decision := platformsafety.EvaluateArtifactRollback(
		target,
		channel,
		pinnedRollbackGeneration,
		canaryRuleRef,
		s.platformArtifactSigningKeyring(),
	); !decision.Pass {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrConflict
	}
	entry := buildPlatformArtifactReleaseLedgerEntry(
		target,
		channel,
		pinnedRollbackGeneration,
		canaryRuleRef,
		req.Reason,
		"",
		model.PlatformReleaseMessageTypeRollback,
		principal,
		lane,
		now,
	)
	release := entry.Release
	if err := pgSupersedePlatformReleases(ctx, tx, target.ArtifactKind, target.ScopeKey, channel, now); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	if _, err := pgInsertPlatformArtifactRelease(ctx, tx, release); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	message := entry.Message
	if _, err := pgInsertPlatformReleaseMessage(ctx, tx, message); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	if err := pgSetPlatformReleaseLaneActive(ctx, tx, lane.LaneKey, release.ID, lane.FencingToken, now); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	if err := tx.Commit(); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	return target, release, message, lkg, nil
}

func (s *Store) pgVerifyPlatformArtifactReleaseLKG(releaseID string, req model.PlatformArtifactVerifyLKGRequest, principal model.Principal) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	_ = principal
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	defer tx.Rollback()
	releaseSnapshot, err := pgGetPlatformArtifactRelease(ctx, tx, releaseID, false)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, mapDBErr(err)
	}
	// Release/rollback transactions lock LKG -> lane -> active release. Keep the
	// same order here so verification cannot deadlock with a newer release.
	currentLKG, err := s.pgGetVerifiedPlatformLKGForUpdate(ctx, tx, releaseSnapshot.ArtifactKind, releaseSnapshot.ScopeKey, time.Now().UTC())
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	lane, err := pgGetPlatformReleaseLaneForUpdate(ctx, tx, releaseSnapshot.LaneKey)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	release, err := pgGetPlatformArtifactRelease(ctx, tx, releaseID, true)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, mapDBErr(err)
	}
	if release.Status != model.PlatformArtifactReleaseStatusActive ||
		release.LaneKey != releaseSnapshot.LaneKey ||
		release.ArtifactKind != releaseSnapshot.ArtifactKind ||
		release.ScopeKey != releaseSnapshot.ScopeKey {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrConflict
	}
	if lane.Frozen || lane.ActiveReleaseID != release.ID || lane.FencingToken != release.FencingToken {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrConflict
	}
	requestEvidenceHash := platformsafety.VerificationEvidenceHash(req)
	if release.VerificationState == model.PlatformArtifactVerificationStateVerified {
		if currentLKG != nil &&
			currentLKG.Generation == release.Generation &&
			currentLKG.VerifiedByReleaseID == release.ID &&
			currentLKG.VerificationEvidenceHash == requestEvidenceHash {
			artifact, err := pgGetPlatformArtifactForUpdate(ctx, tx, release.ArtifactID, false)
			if err != nil {
				return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, mapDBErr(err)
			}
			message, err := pgGetLatestPlatformReleaseMessage(ctx, tx, release.ID)
			if err != nil {
				return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
			}
			return artifact, release, message, currentLKG, nil
		}
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrConflict
	}
	artifact, err := pgGetPlatformArtifactForUpdate(ctx, tx, release.ArtifactID, false)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, mapDBErr(err)
	}
	if decision := platformsafety.EvaluateArtifactIntegrity(artifact, s.platformArtifactSigningKeyring()); !decision.Pass {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrConflict
	}
	if decision := platformsafety.EvaluateLKGPromotion(release, req, currentLKG != nil); !decision.Pass {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrConflict
	}
	now := time.Now().UTC()
	snapshot, err := buildPlatformLKGSnapshot(
		artifact,
		release.ID,
		requestEvidenceHash,
		now,
		s.platformArtifactSigningKeyring(),
	)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	lkg, err := pgUpsertPlatformLKGSnapshot(ctx, tx, snapshot)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	release.VerificationState = model.PlatformArtifactVerificationStateVerified
	release.VerificationEvidence = platformsafety.VerificationEvidenceMap(req)
	release.VerifiedLKGGeneration = artifact.Generation
	release.ServingUnverifiedGeneration = ""
	release.VerifiedAt = &now
	release.Version++
	release.UpdatedAt = now
	release, err = pgUpdatePlatformArtifactReleaseVerification(ctx, tx, release)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	message := buildPlatformReleaseMessage(artifact, release, model.PlatformReleaseMessageTypeVerifiedLKG, now)
	if _, err := pgInsertPlatformReleaseMessage(ctx, tx, message); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	if err := tx.Commit(); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	return artifact, release, message, &lkg, nil
}

func pgGetPlatformArtifactByGenerationForUpdate(ctx context.Context, db platformStateDB, kind, scopeKey, generation string) (model.PlatformArtifact, error) {
	return scanPlatformArtifact(db.QueryRowContext(ctx, `
SELECT id, artifact_kind, scope_key, scope_json, schema_version, generation, generation_sequence, status, content_hash,
	content_json, validation_results_json, compatibility_floor, metadata_json,
	created_by_type, created_by_id, provenance_json, created_at, updated_at
FROM fugue_platform_artifacts
WHERE artifact_kind = $1 AND scope_key = $2 AND generation = $3
LIMIT 1
FOR UPDATE`, kind, scopeKey, generation))
}

func pgNextPlatformReleaseLane(ctx context.Context, db platformStateDB, kind, scopeKey, channel string, now time.Time) (model.PlatformReleaseLane, error) {
	laneKey := platformsafety.ReleaseLaneKey(kind, scopeKey, channel)
	lane, err := scanPlatformReleaseLane(db.QueryRowContext(ctx, `
INSERT INTO fugue_platform_release_lanes (
	lane_key, artifact_kind, scope_key, release_channel, fencing_token, version,
	active_release_id, frozen, freeze_reason, updated_at
) VALUES ($1, $2, $3, $4, 1, 1, '', FALSE, '', $5)
ON CONFLICT (lane_key) DO UPDATE SET
	fencing_token = fugue_platform_release_lanes.fencing_token + 1,
	version = fugue_platform_release_lanes.version + 1,
	updated_at = EXCLUDED.updated_at
WHERE fugue_platform_release_lanes.frozen = FALSE
RETURNING lane_key, artifact_kind, scope_key, release_channel, fencing_token, version,
	active_release_id, frozen, freeze_reason, updated_at`,
		laneKey, kind, scopeKey, channel, now))
	if err != nil {
		if mapDBErr(err) == ErrNotFound {
			return model.PlatformReleaseLane{}, ErrConflict
		}
		return model.PlatformReleaseLane{}, mapDBErr(err)
	}
	return lane, nil
}

func pgActivePlatformReleaseGenerationSequence(
	ctx context.Context,
	db platformStateDB,
	lane model.PlatformReleaseLane,
) (int64, error) {
	if strings.TrimSpace(lane.ActiveReleaseID) == "" {
		return 0, nil
	}
	release, err := pgGetPlatformArtifactRelease(ctx, db, lane.ActiveReleaseID, true)
	if err != nil {
		return 0, err
	}
	if release.Status != model.PlatformArtifactReleaseStatusActive ||
		release.LaneKey != lane.LaneKey {
		return 0, ErrConflict
	}
	artifact, err := pgGetPlatformArtifactForUpdate(ctx, db, release.ArtifactID, false)
	if err != nil {
		return 0, mapDBErr(err)
	}
	return artifact.GenerationSequence, nil
}

func pgSetPlatformReleaseLaneActive(ctx context.Context, db platformStateDB, laneKey, releaseID string, fencingToken int64, now time.Time) error {
	result, err := db.ExecContext(ctx, `
UPDATE fugue_platform_release_lanes
SET active_release_id = $2, updated_at = $4
WHERE lane_key = $1 AND fencing_token = $3 AND frozen = FALSE`,
		laneKey, releaseID, fencingToken, now)
	if err != nil {
		return mapDBErr(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrConflict
	}
	return nil
}

func pgGetPlatformReleaseLaneForUpdate(ctx context.Context, db platformStateDB, laneKey string) (model.PlatformReleaseLane, error) {
	lane, err := scanPlatformReleaseLane(db.QueryRowContext(ctx, `
SELECT lane_key, artifact_kind, scope_key, release_channel, fencing_token, version,
	active_release_id, frozen, freeze_reason, updated_at
FROM fugue_platform_release_lanes
WHERE lane_key = $1
FOR UPDATE`, laneKey))
	if err != nil {
		return model.PlatformReleaseLane{}, mapDBErr(err)
	}
	return lane, nil
}

func pgGetPlatformArtifactRelease(ctx context.Context, db platformStateDB, releaseID string, forUpdate bool) (model.PlatformArtifactRelease, error) {
	query := `
SELECT id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	release_channel, status, lane_key, fencing_token, version, idempotency_key,
	candidate_generation, serving_unverified_generation, verified_lkg_generation,
	pinned_rollback_generation, verification_state, verification_evidence_json, verified_at,
	rollback_target_generation, canary_rule_ref, reason,
	released_by_type, released_by_id, released_at, created_at, updated_at
FROM fugue_platform_artifact_releases
WHERE id = $1`
	if forUpdate {
		query += " FOR UPDATE"
	}
	release, err := scanPlatformArtifactRelease(db.QueryRowContext(ctx, query, releaseID))
	if err != nil {
		return model.PlatformArtifactRelease{}, mapDBErr(err)
	}
	return release, nil
}

func pgGetPlatformArtifactReleaseByIdempotency(ctx context.Context, db platformStateDB, laneKey, idempotencyKey string) (model.PlatformArtifactRelease, bool, error) {
	release, err := scanPlatformArtifactRelease(db.QueryRowContext(ctx, `
SELECT id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	release_channel, status, lane_key, fencing_token, version, idempotency_key,
	candidate_generation, serving_unverified_generation, verified_lkg_generation,
	pinned_rollback_generation, verification_state, verification_evidence_json, verified_at,
	rollback_target_generation, canary_rule_ref, reason,
	released_by_type, released_by_id, released_at, created_at, updated_at
FROM fugue_platform_artifact_releases
WHERE lane_key = $1 AND idempotency_key = $2
LIMIT 1`, laneKey, strings.TrimSpace(idempotencyKey)))
	if err != nil {
		if mapDBErr(err) == ErrNotFound {
			return model.PlatformArtifactRelease{}, false, nil
		}
		return model.PlatformArtifactRelease{}, false, mapDBErr(err)
	}
	return release, true, nil
}

func pgGetLatestPlatformReleaseMessage(ctx context.Context, db platformStateDB, releaseID string) (model.PlatformReleaseMessage, error) {
	message, err := scanPlatformReleaseMessage(db.QueryRowContext(ctx, `
SELECT id, release_id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	release_channel, message_type, created_at, expires_at, ack_count
FROM fugue_platform_release_messages
WHERE release_id = $1
ORDER BY created_at DESC, id ASC
LIMIT 1`, releaseID))
	if err != nil {
		return model.PlatformReleaseMessage{}, mapDBErr(err)
	}
	return message, nil
}

func pgGetPlatformLKGForUpdate(ctx context.Context, db platformStateDB, kind, scopeKey string) (*model.PlatformLKGSnapshot, error) {
	snapshot, err := scanPlatformLKGSnapshot(db.QueryRowContext(ctx, `
SELECT id, artifact_id, artifact_kind, scope_key, scope_json, schema_version,
	generation, generation_sequence, content_hash, artifact_provenance_json,
	verified_by_release_id, verification_evidence_hash, snapshot_provenance_json,
	expires_at, created_at, updated_at
FROM fugue_platform_lkg_snapshots
WHERE artifact_kind = $1 AND scope_key = $2
LIMIT 1
FOR UPDATE`, kind, scopeKey))
	if err != nil {
		if mapDBErr(err) == ErrNotFound {
			return nil, nil
		}
		return nil, mapDBErr(err)
	}
	return &snapshot, nil
}

func (s *Store) pgGetVerifiedPlatformLKGForUpdate(
	ctx context.Context,
	db platformStateDB,
	kind string,
	scopeKey string,
	now time.Time,
) (*model.PlatformLKGSnapshot, error) {
	snapshot, err := pgGetPlatformLKGForUpdate(ctx, db, kind, scopeKey)
	if err != nil || snapshot == nil {
		return snapshot, err
	}
	artifact, err := pgGetPlatformArtifactForUpdate(ctx, db, snapshot.ArtifactID, false)
	if err != nil {
		if mapDBErr(err) == ErrNotFound {
			return nil, nil
		}
		return nil, mapDBErr(err)
	}
	if !platformLKGSnapshotMatchesArtifact(*snapshot, artifact, now, s.platformArtifactSigningKeyring()) {
		return nil, nil
	}
	return snapshot, nil
}

func pgUpdatePlatformArtifactReleaseVerification(ctx context.Context, db platformStateDB, release model.PlatformArtifactRelease) (model.PlatformArtifactRelease, error) {
	evidenceJSON, err := marshalJSON(release.VerificationEvidence)
	if err != nil {
		return model.PlatformArtifactRelease{}, err
	}
	out, err := scanPlatformArtifactRelease(db.QueryRowContext(ctx, `
UPDATE fugue_platform_artifact_releases
SET serving_unverified_generation = $2,
	verified_lkg_generation = $3,
	verification_state = $4,
	verification_evidence_json = $5::jsonb,
	verified_at = $6,
	version = $7,
	updated_at = $8
WHERE id = $1 AND fencing_token = $9 AND status = $10
RETURNING id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	release_channel, status, lane_key, fencing_token, version, idempotency_key,
	candidate_generation, serving_unverified_generation, verified_lkg_generation,
	pinned_rollback_generation, verification_state, verification_evidence_json, verified_at,
	rollback_target_generation, canary_rule_ref, reason,
	released_by_type, released_by_id, released_at, created_at, updated_at`,
		release.ID,
		release.ServingUnverifiedGeneration,
		release.VerifiedLKGGeneration,
		release.VerificationState,
		evidenceJSON,
		release.VerifiedAt,
		release.Version,
		release.UpdatedAt,
		release.FencingToken,
		model.PlatformArtifactReleaseStatusActive))
	if err != nil {
		return model.PlatformArtifactRelease{}, mapDBErr(err)
	}
	return out, nil
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
	verificationEvidenceJSON, err := marshalJSON(release.VerificationEvidence)
	if err != nil {
		return model.PlatformArtifactRelease{}, err
	}
	out, err := scanPlatformArtifactRelease(db.QueryRowContext(ctx, `
INSERT INTO fugue_platform_artifact_releases (
	id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	release_channel, status, lane_key, fencing_token, version, idempotency_key,
	candidate_generation, serving_unverified_generation, verified_lkg_generation,
	pinned_rollback_generation, verification_state, verification_evidence_json, verified_at,
	rollback_target_generation, canary_rule_ref, reason,
	released_by_type, released_by_id, released_at, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5::jsonb, $6,
	$7, $8, $9, $10, $11, $12,
	$13, $14, $15,
	$16, $17, $18::jsonb, $19,
	$20, $21, $22,
	$23, $24, $25, $26, $27
) RETURNING id, artifact_id, artifact_kind, scope_key, scope_json, generation,
	release_channel, status, lane_key, fencing_token, version, idempotency_key,
	candidate_generation, serving_unverified_generation, verified_lkg_generation,
	pinned_rollback_generation, verification_state, verification_evidence_json, verified_at,
	rollback_target_generation, canary_rule_ref, reason,
	released_by_type, released_by_id, released_at, created_at, updated_at`,
		release.ID, release.ArtifactID, release.ArtifactKind, release.ScopeKey, scopeJSON, release.Generation,
		release.ReleaseChannel, release.Status, release.LaneKey, release.FencingToken, release.Version, release.IdempotencyKey,
		release.CandidateGeneration, release.ServingUnverifiedGeneration, release.VerifiedLKGGeneration,
		release.PinnedRollbackGeneration, release.VerificationState, verificationEvidenceJSON, release.VerifiedAt,
		release.RollbackTargetGeneration, release.CanaryRuleRef, release.Reason,
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
	artifactProvenanceJSON, err := marshalJSON(snapshot.ArtifactProvenance)
	if err != nil {
		return model.PlatformLKGSnapshot{}, err
	}
	snapshotProvenanceJSON, err := marshalJSON(snapshot.SnapshotProvenance)
	if err != nil {
		return model.PlatformLKGSnapshot{}, err
	}
	out, err := scanPlatformLKGSnapshot(db.QueryRowContext(ctx, `
INSERT INTO fugue_platform_lkg_snapshots (
	id, artifact_id, artifact_kind, scope_key, scope_json, schema_version,
	generation, generation_sequence, content_hash, artifact_provenance_json,
	verified_by_release_id, verification_evidence_hash, snapshot_provenance_json,
	expires_at, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5::jsonb, $6,
	$7, $8, $9, $10::jsonb,
	$11, $12, $13::jsonb,
	$14, $15, $16
) ON CONFLICT (artifact_kind, scope_key) DO UPDATE SET
	id = EXCLUDED.id,
	artifact_id = EXCLUDED.artifact_id,
	scope_json = EXCLUDED.scope_json,
	schema_version = EXCLUDED.schema_version,
	generation = EXCLUDED.generation,
	generation_sequence = EXCLUDED.generation_sequence,
	content_hash = EXCLUDED.content_hash,
	artifact_provenance_json = EXCLUDED.artifact_provenance_json,
	verified_by_release_id = EXCLUDED.verified_by_release_id,
	verification_evidence_hash = EXCLUDED.verification_evidence_hash,
	snapshot_provenance_json = EXCLUDED.snapshot_provenance_json,
	expires_at = EXCLUDED.expires_at,
	created_at = EXCLUDED.created_at,
	updated_at = EXCLUDED.updated_at
RETURNING id, artifact_id, artifact_kind, scope_key, scope_json, schema_version,
	generation, generation_sequence, content_hash, artifact_provenance_json,
	verified_by_release_id, verification_evidence_hash, snapshot_provenance_json,
	expires_at, created_at, updated_at`,
		snapshot.ID, snapshot.ArtifactID, snapshot.ArtifactKind, snapshot.ScopeKey, scopeJSON, snapshot.SchemaVersion,
		snapshot.Generation, snapshot.GenerationSequence, snapshot.ContentHash, artifactProvenanceJSON,
		snapshot.VerifiedByReleaseID, snapshot.VerificationEvidenceHash, snapshotProvenanceJSON,
		snapshot.ExpiresAt, snapshot.CreatedAt, snapshot.UpdatedAt))
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
	release_channel, status, lane_key, fencing_token, version, idempotency_key,
	candidate_generation, serving_unverified_generation, verified_lkg_generation,
	pinned_rollback_generation, verification_state, verification_evidence_json, verified_at,
	rollback_target_generation, canary_rule_ref, reason,
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
SELECT id, artifact_id, artifact_kind, scope_key, scope_json, schema_version,
	generation, generation_sequence, content_hash, artifact_provenance_json,
	verified_by_release_id, verification_evidence_hash, snapshot_provenance_json,
	expires_at, created_at, updated_at
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
	var scopeRaw, contentRaw, validationRaw, metadataRaw, provenanceRaw []byte
	if err := scanner.Scan(
		&artifact.ID,
		&artifact.ArtifactKind,
		&artifact.ScopeKey,
		&scopeRaw,
		&artifact.SchemaVersion,
		&artifact.Generation,
		&artifact.GenerationSequence,
		&artifact.Status,
		&artifact.ContentHash,
		&contentRaw,
		&validationRaw,
		&artifact.CompatibilityFloor,
		&metadataRaw,
		&artifact.CreatedByType,
		&artifact.CreatedByID,
		&provenanceRaw,
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
	provenance, err := decodeJSONValue[model.PlatformArtifactProvenance](provenanceRaw)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	artifact.Scope = scope
	artifact.Content = content
	artifact.ValidationResults = validation
	artifact.Metadata = metadata
	artifact.Provenance = provenance
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
	var scopeRaw, verificationEvidenceRaw []byte
	if err := scanner.Scan(
		&release.ID,
		&release.ArtifactID,
		&release.ArtifactKind,
		&release.ScopeKey,
		&scopeRaw,
		&release.Generation,
		&release.ReleaseChannel,
		&release.Status,
		&release.LaneKey,
		&release.FencingToken,
		&release.Version,
		&release.IdempotencyKey,
		&release.CandidateGeneration,
		&release.ServingUnverifiedGeneration,
		&release.VerifiedLKGGeneration,
		&release.PinnedRollbackGeneration,
		&release.VerificationState,
		&verificationEvidenceRaw,
		&release.VerifiedAt,
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
	verificationEvidence, err := decodeJSONValue[map[string]string](verificationEvidenceRaw)
	if err != nil {
		return model.PlatformArtifactRelease{}, err
	}
	release.Scope = scope
	release.VerificationEvidence = verificationEvidence
	return release, nil
}

func scanPlatformReleaseLane(scanner sqlScanner) (model.PlatformReleaseLane, error) {
	var lane model.PlatformReleaseLane
	if err := scanner.Scan(
		&lane.LaneKey,
		&lane.ArtifactKind,
		&lane.ScopeKey,
		&lane.ReleaseChannel,
		&lane.FencingToken,
		&lane.Version,
		&lane.ActiveReleaseID,
		&lane.Frozen,
		&lane.FreezeReason,
		&lane.UpdatedAt,
	); err != nil {
		return model.PlatformReleaseLane{}, mapDBErr(err)
	}
	return lane, nil
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
	var scopeRaw, artifactProvenanceRaw, snapshotProvenanceRaw []byte
	if err := scanner.Scan(
		&snapshot.ID,
		&snapshot.ArtifactID,
		&snapshot.ArtifactKind,
		&snapshot.ScopeKey,
		&scopeRaw,
		&snapshot.SchemaVersion,
		&snapshot.Generation,
		&snapshot.GenerationSequence,
		&snapshot.ContentHash,
		&artifactProvenanceRaw,
		&snapshot.VerifiedByReleaseID,
		&snapshot.VerificationEvidenceHash,
		&snapshotProvenanceRaw,
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
	artifactProvenance, err := decodeJSONValue[model.PlatformArtifactProvenance](artifactProvenanceRaw)
	if err != nil {
		return model.PlatformLKGSnapshot{}, err
	}
	snapshotProvenance, err := decodeJSONValue[model.PlatformArtifactProvenance](snapshotProvenanceRaw)
	if err != nil {
		return model.PlatformLKGSnapshot{}, err
	}
	snapshot.Scope = scope
	snapshot.ArtifactProvenance = artifactProvenance
	snapshot.SnapshotProvenance = snapshotProvenance
	return snapshot, nil
}

func formatStoreArgIndex(index int) string {
	return fmt.Sprintf("%d", index)
}
