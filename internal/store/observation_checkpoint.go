package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// Observation checkpoints contain only rebuildable runtime facts. They never
// change configuration, release authority, backup metadata, or serving LKG.
func (s *Store) ReadObservationCheckpoint(ctx context.Context, key string) ([]byte, error) {
	key = observationCheckpointKey(key)
	var b string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, key).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return []byte(b), err
}

// UpdateObservationCheckpoint serializes one bounded page and its cursor
// across replicas. A canceled/failed update cannot publish partial progress.
func (s *Store) UpdateObservationCheckpoint(ctx context.Context, key string, fn func([]byte) ([]byte, error)) (bool, error) {
	if strings.HasPrefix(key, backupInventoryCheckpointPrefix) {
		return false, fmt.Errorf("%w: backup inventory uses its dedicated checkpoint writer", ErrInvalidInput)
	}
	return s.updateObservationCheckpoint(ctx, key, fn)
}

func (s *Store) updateObservationCheckpoint(ctx context.Context, key string, fn func([]byte) ([]byte, error)) (bool, error) {
	key = observationCheckpointKey(key)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var locked bool
	if err = tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, key).Scan(&locked); err != nil || !locked {
		return false, err
	}
	var value string
	err = tx.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, key).Scan(&value)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return true, err
	}
	b, err := fn([]byte(value))
	if err != nil {
		return true, err
	}
	if len(b) > 16<<20 {
		return true, fmt.Errorf("observation checkpoint exceeds size limit")
	}
	if b != nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_meta(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=NOW()`, key, string(b))
		if err != nil {
			return true, err
		}
	}
	return true, tx.Commit()
}
func observationCheckpointKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return "observation/v1/" + hex.EncodeToString(h[:])
}
