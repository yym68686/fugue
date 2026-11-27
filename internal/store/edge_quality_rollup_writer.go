package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/schemamigrate"
)

const (
	// Keep the same lock as the legacy session-scoped writer so mixed rollout
	// revisions still contend. New writers hold it on the actual write tx.
	EdgeQualityRollupWriterLock       = "edge-quality-rollup-builder"
	EdgeQualityRollupWriterCheckpoint = schemamigrate.EdgeQualityRollupWriterCheckpoint
)

type EdgeQualityRollupWriter string

const (
	EdgeQualityRollupAPIWriter    EdgeQualityRollupWriter = ""
	EdgeQualityRollupWorkerWriter EdgeQualityRollupWriter = "worker"
)

// OpenEdgeQualityRollupWorkerStore opens only a bounded PostgreSQL pool. In
// particular it does not bootstrap the schema, seed backup data or touch apps.
func OpenEdgeQualityRollupWorkerStore(ctx context.Context, databaseURL string) (*Store, func() error, error) {
	databaseURL = strings.TrimSpace(databaseURL)
	if databaseURL == "" {
		return nil, nil, errors.New("rollup worker requires PostgreSQL")
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("configure rollup database: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(time.Minute)
	s := &Store{databaseURL: databaseURL, db: db, dbReady: true}
	if err := s.CheckEdgeQualityRollupWorker(ctx); err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return s, db.Close, nil
}

// CheckEdgeQualityRollupWorker checks the schema, effective privileges and
// owner without acquiring the writer lock or running a work pass.
func (s *Store) CheckEdgeQualityRollupWorker(ctx context.Context) error {
	if s == nil || s.db == nil {
		return ErrInvalidInput
	}
	for _, query := range []string{
		`SELECT id FROM fugue_edge_performance_samples LIMIT 0`,
		`SELECT window_name FROM fugue_edge_quality_rollup_watermarks LIMIT 0`,
		`SELECT hostname FROM fugue_edge_quality_rollups LIMIT 0`,
	} {
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	var permitted bool
	if err := s.db.QueryRowContext(ctx, `
SELECT has_table_privilege('fugue_edge_quality_rollups', 'INSERT')
   AND has_table_privilege('fugue_edge_quality_rollups', 'UPDATE')
   AND has_table_privilege('fugue_edge_quality_rollups', 'DELETE')
   AND has_table_privilege('fugue_edge_quality_rollup_watermarks', 'INSERT')
   AND has_table_privilege('fugue_edge_quality_rollup_watermarks', 'UPDATE')
   AND NOT has_any_column_privilege('fugue_meta', 'SELECT')
   AND NOT has_any_column_privilege('fugue_meta', 'INSERT')
   AND NOT has_any_column_privilege('fugue_meta', 'UPDATE')
   AND NOT has_table_privilege('fugue_meta', 'DELETE')
   AND NOT has_any_column_privilege('fugue_edge_quality_rollup_owner', 'INSERT')
   AND NOT has_any_column_privilege('fugue_edge_quality_rollup_owner', 'UPDATE')
   AND NOT has_table_privilege('fugue_edge_quality_rollup_owner', 'DELETE')
`).Scan(&permitted); err != nil {
		return err
	}
	if !permitted {
		return errors.New("rollup worker database privileges do not match the writer contract")
	}
	_, err := readEdgeQualityRollupWriter(ctx, s.db, true)
	return err
}

func parseEdgeQualityRollupWriter(raw string) (EdgeQualityRollupWriter, error) {
	switch writer := EdgeQualityRollupWriter(raw); writer {
	case EdgeQualityRollupAPIWriter, EdgeQualityRollupWorkerWriter:
		return writer, nil
	default:
		return "", errors.New("unknown edge quality rollup writer")
	}
}

// AcquireWriter locks and checks ownership in the transaction that will commit
// rollups and watermarks. A dropped session cannot commit on a different
// connection. A handoff must take this same transaction lock.
func (b *EdgeQualityRollupBuildSession) AcquireWriter(writer EdgeQualityRollupWriter) (bool, error) {
	if b == nil || b.closed || b.writeTx == nil {
		return false, ErrInvalidInput
	}
	return acquireEdgeQualityRollupWriter(b.ctx, b.writeTx, writer, false)
}

// TransferEdgeQualityRollupsToWorker performs the one-way writer handoff.
// Callers must first verify the deployed API rollback floor and an independent
// ready worker. Restarting a process never transfers ownership.
func (s *Store) TransferEdgeQualityRollupsToWorker(ctx context.Context) (bool, error) {
	if s == nil || s.db == nil {
		return false, ErrInvalidInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	ok, err := acquireEdgeQualityRollupWriter(ctx, tx, EdgeQualityRollupWorkerWriter, true)
	if err != nil || !ok {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func acquireEdgeQualityRollupWriter(ctx context.Context, tx *sql.Tx, writer EdgeQualityRollupWriter, handoff bool) (bool, error) {
	if _, err := parseEdgeQualityRollupWriter(string(writer)); err != nil {
		return false, err
	}
	var locked bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))`, EdgeQualityRollupWriterLock).Scan(&locked); err != nil || !locked {
		return false, err
	}
	owner, err := readEdgeQualityRollupWriter(ctx, tx, writer == EdgeQualityRollupWorkerWriter && !handoff)
	if err != nil {
		return false, err
	}
	if owner == writer {
		return true, nil
	}
	if !handoff || owner != EdgeQualityRollupAPIWriter || writer != EdgeQualityRollupWorkerWriter {
		return false, nil
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO fugue_meta (key, value) VALUES ($1, $2)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`, observationCheckpointKey(EdgeQualityRollupWriterCheckpoint), string(writer))
	return err == nil, err
}

type edgeQualityRollupOwnerReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readEdgeQualityRollupWriter(ctx context.Context, reader edgeQualityRollupOwnerReader, useProjection bool) (EdgeQualityRollupWriter, error) {
	var row *sql.Row
	if useProjection {
		row = reader.QueryRowContext(ctx, `SELECT owner FROM fugue_edge_quality_rollup_owner`)
	} else {
		row = reader.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key = $1`, observationCheckpointKey(EdgeQualityRollupWriterCheckpoint))
	}
	var value string
	if err := row.Scan(&value); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	return parseEdgeQualityRollupWriter(value)
}
