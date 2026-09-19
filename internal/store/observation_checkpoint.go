package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// Observation checkpoints contain only rebuildable runtime facts. They never
// change configuration, release authority, backup metadata, or serving LKG.
func (s *Store) ReadObservationCheckpoint(ctx context.Context, key string) ([]byte, error) {
	key = observationCheckpointKey(key)
	if s.usingDatabase() {
		var b string
		err := s.db.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, key).Scan(&b)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return []byte(b), err
	}
	b, err := os.ReadFile(s.observationCheckpointPath(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

// UpdateObservationCheckpoint serializes one bounded page and its cursor
// across replicas. A canceled/failed update cannot publish partial progress.
func (s *Store) UpdateObservationCheckpoint(ctx context.Context, key string, fn func([]byte) ([]byte, error)) (bool, error) {
	key = observationCheckpointKey(key)
	if s.usingDatabase() {
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
	path := s.observationCheckpointPath(key)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return false, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return false, err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); errors.Is(err, syscall.EWOULDBLOCK) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	b, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return true, err
	}
	b, err = fn(b)
	if err != nil {
		return true, err
	}
	if b == nil {
		return true, nil
	}
	if len(b) > 16<<20 {
		return true, fmt.Errorf("observation checkpoint exceeds size limit")
	}
	if err = ctx.Err(); err != nil {
		return true, err
	}
	f, err := os.CreateTemp(filepath.Dir(path), "checkpoint-")
	if err != nil {
		return true, err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return true, err
	}
	if closeErr != nil {
		return true, closeErr
	}
	return true, os.Rename(f.Name(), path)
}
func observationCheckpointKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return "observation/v1/" + hex.EncodeToString(h[:])
}
func (s *Store) observationCheckpointPath(key string) string {
	return filepath.Join(s.path+".observations", filepath.Base(key)+".json")
}
