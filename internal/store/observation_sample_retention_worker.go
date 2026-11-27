package store

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"time"
)

func ObservationSampleRetentionTargets() [3]string {
	return [3]string{networkSampleRetention, performanceSampleRetention, resourceUsageSampleRetention}
}

type ObservationSampleRetentionPass struct {
	State       string
	Deleted     int64
	CompletedAt time.Time
}

type ObservationSampleRetentionProgress struct {
	Target          string
	Owner           string
	CompletedAt     time.Time
	CompletedPasses int64
	TotalDeleted    int64
}

func (s *Store) RunObservationSampleRetentionPass(ctx context.Context, target string, policy ObservationRetentionPolicy) (ObservationSampleRetentionPass, error) {
	targets := ObservationSampleRetentionTargets()
	if s == nil || s.db == nil || !slices.Contains(targets[:], target) {
		return ObservationSampleRetentionPass{}, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var pass ObservationSampleRetentionPass
	var at sql.NullTime
	if err := s.db.QueryRowContext(ctx, `SELECT state, deleted, finished_at
FROM fugue_observation_maintenance.prune_samples_v1($1,$2,$3,$4,$5)`,
		target, policy.Enabled, policy.ProbeDays, policy.ProbeRowsPerTable, policy.BatchRows).Scan(&pass.State, &pass.Deleted, &at); err != nil {
		return ObservationSampleRetentionPass{}, err
	}
	switch pass.State {
	case "standby", "busy":
		if pass.Deleted != 0 || at.Valid {
			return ObservationSampleRetentionPass{}, errors.New("invalid passive sample retention result")
		}
	case "completed":
		if pass.Deleted < 0 || pass.Deleted > int64(policy.BatchRows) || !at.Valid {
			return ObservationSampleRetentionPass{}, errors.New("invalid completed sample retention result")
		}
		pass.CompletedAt = at.Time.UTC()
	default:
		return ObservationSampleRetentionPass{}, errors.New("unrecognized sample retention result")
	}
	return pass, nil
}

func (s *Store) ReadObservationSampleRetentionProgress(ctx context.Context) ([]ObservationSampleRetentionProgress, error) {
	if s == nil || s.db == nil {
		return nil, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT target,owner,completed_at,completed_passes,total_deleted,fence_enforced,
has_function_privilege('fugue_observation_maintenance.prune_samples_v1(text,boolean,integer,integer,integer)', 'EXECUTE')
FROM fugue_observation_maintenance.sample_worker_progress ORDER BY target`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	targets := ObservationSampleRetentionTargets()
	reports := make([]ObservationSampleRetentionProgress, 0, len(targets))
	var seen [3]bool
	for rows.Next() {
		var progress ObservationSampleRetentionProgress
		var at sql.NullTime
		var fenced, granted bool
		if err := rows.Scan(&progress.Target, &progress.Owner, &at, &progress.CompletedPasses, &progress.TotalDeleted, &fenced, &granted); err != nil {
			return nil, err
		}
		index := slices.Index(targets[:], progress.Target)
		if index < 0 || seen[index] || !granted ||
			(progress.Owner != "api" && progress.Owner != "worker") ||
			fenced != (progress.Owner == "worker") ||
			(len(reports) > 0 && reports[0].Owner != progress.Owner) ||
			progress.CompletedPasses < 0 || progress.TotalDeleted < 0 || at.Valid != (progress.CompletedPasses > 0) {
			return nil, errors.New("invalid sample retention progress or capability")
		}
		seen[index] = true
		progress.CompletedAt = at.Time.UTC()
		reports = append(reports, progress)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(reports) != len(targets) {
		return nil, errors.New("incomplete sample retention progress")
	}
	return reports, nil
}
