package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

const QualityEvidenceRetentionBatchRows = 256 // Keep in sync with prune_quality_evidence_v1.

type QualityEvidenceRetentionPass struct {
	State       string
	Deleted     int64
	CompletedAt time.Time
}

type QualityEvidenceRetentionProgress struct {
	Owner           string
	CompletedAt     time.Time
	CompletedPasses int64
	TotalDeleted    int64
}

func (s *Store) ReadQualityEvidenceRetentionProgress(ctx context.Context) (QualityEvidenceRetentionProgress, error) {
	if s == nil || s.db == nil {
		return QualityEvidenceRetentionProgress{}, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return readQualityEvidenceRetentionProgress(ctx, s.db)
}

func readQualityEvidenceRetentionProgress(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (QualityEvidenceRetentionProgress, error) {
	var progress QualityEvidenceRetentionProgress
	var at sql.NullTime
	var granted bool
	err := db.QueryRowContext(ctx, `SELECT owner, completed_at, completed_passes, total_deleted,
has_function_privilege('fugue_observation_maintenance.prune_quality_evidence_v1()', 'EXECUTE')
FROM fugue_observation_maintenance.quality_evidence_worker_progress`).
		Scan(&progress.Owner, &at, &progress.CompletedPasses, &progress.TotalDeleted, &granted)
	if err != nil {
		return QualityEvidenceRetentionProgress{}, err
	}
	if !granted || (progress.Owner != "api" && progress.Owner != "worker") ||
		progress.CompletedPasses < 0 || progress.TotalDeleted < 0 ||
		at.Valid != (progress.CompletedPasses > 0) {
		return QualityEvidenceRetentionProgress{}, errors.New("invalid quality evidence retention progress or capability")
	}
	progress.CompletedAt = at.Time.UTC()
	return progress, nil
}

// RunQualityEvidenceRetentionPass requires only execute access to the bounded
// PostgreSQL operation. It does not accept a supplied timestamp, predicate or
// limit, so a worker cannot broaden expiry or read retained evidence.
func (s *Store) RunQualityEvidenceRetentionPass(ctx context.Context) (QualityEvidenceRetentionPass, error) {
	if s == nil || s.db == nil {
		return QualityEvidenceRetentionPass{}, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var result QualityEvidenceRetentionPass
	var at sql.NullTime
	err := s.db.QueryRowContext(ctx, `SELECT state, deleted, finished_at FROM fugue_observation_maintenance.prune_quality_evidence_v1()`).
		Scan(&result.State, &result.Deleted, &at)
	if err != nil {
		return QualityEvidenceRetentionPass{}, err
	}
	switch result.State {
	case "standby", "busy":
		if result.Deleted != 0 || at.Valid {
			return QualityEvidenceRetentionPass{}, errors.New("invalid passive quality evidence retention result")
		}
	case "completed":
		if result.Deleted < 0 || result.Deleted > QualityEvidenceRetentionBatchRows || !at.Valid {
			return QualityEvidenceRetentionPass{}, errors.New("invalid completed quality evidence retention result")
		}
		result.CompletedAt = at.Time.UTC()
	default:
		return QualityEvidenceRetentionPass{}, errors.New("unrecognized quality evidence retention result")
	}
	return result, nil
}
