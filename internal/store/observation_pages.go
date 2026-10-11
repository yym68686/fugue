package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ObservationPages splits a rebuildable observation into bounded named pages.
// Metadata carries the cursor and generation references; both commit together.
type ObservationPages struct {
	Metadata []byte
	Pages    map[string][]byte
}
type ObservationPageUpdate func(ObservationPages, []byte) (ObservationPages, error)

func readObservationPages(ctx context.Context, db inputQueryer, key string) (ObservationPages, int64, error) {
	var out ObservationPages
	var revision int64
	err := db.QueryRowContext(ctx, `SELECT revision,metadata FROM fugue_observation_checkpoint_heads WHERE checkpoint_key=$1`, key).Scan(&revision, &out.Metadata)
	if err != nil {
		return out, 0, err
	}
	if revision < 1 || len(out.Metadata) > inputChunkBytes {
		return out, 0, ErrConflict
	}
	rows, err := db.QueryContext(ctx, `SELECT entry_key,value FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND bucket='pages' ORDER BY entry_key`, key)
	if err != nil {
		return out, 0, err
	}
	defer rows.Close()
	out.Pages = map[string][]byte{}
	size := len(out.Metadata)
	for rows.Next() {
		var k string
		var raw []byte
		if err = rows.Scan(&k, &raw); err != nil {
			return out, 0, err
		}
		size += len(raw)
		if len(raw) > 64<<10 || len(k) > 4096 || size > 16<<20 {
			return out, 0, fmt.Errorf("observation pages exceed bounded size")
		}
		out.Pages[k] = raw
	}
	return out, revision, rows.Err()
}

func (s *Store) ReadObservationPages(ctx context.Context, key string) (ObservationPages, []byte, error) {
	if s.usingDatabase() {
		// One transaction gives head and pages a consistent generation during
		// another replica's cursor update or completion swap.
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
		if err != nil {
			return ObservationPages{}, nil, err
		}
		defer tx.Rollback()
		p, _, err := readObservationPages(ctx, tx, observationCheckpointKey(key))
		if err == nil {
			return p, nil, tx.Commit()
		}
		if !absentInputManifest(err) {
			return p, nil, err
		}
		// Release the pinned read connection before the legacy lookup, including
		// when the configured pool contains only one connection.
		if err := tx.Rollback(); err != nil {
			return p, nil, err
		}
	}
	raw, err := s.ReadObservationCheckpoint(ctx, key)
	return ObservationPages{}, raw, err
}

func (s *Store) UpdateObservationPages(ctx context.Context, key string, update ObservationPageUpdate, legacy func([]byte) ([]byte, error)) (bool, error) {
	if !s.usingDatabase() {
		return s.UpdateObservationCheckpoint(ctx, key, legacy)
	}
	originalKey := key
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
	old, revision, err := readObservationPages(ctx, tx, key)
	// Deployment orders schema before activation. A missing table is supported
	// by the read path, but a writer never mutates an unverified schema.
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		if absentInputManifest(err) && !s.StoragePolicy().CheckpointPages {
			tx.Rollback()
			return s.UpdateObservationCheckpoint(ctx, originalKey, legacy)
		}
		return true, err
	}
	var raw []byte
	if errors.Is(err, sql.ErrNoRows) {
		if err = tx.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, key).Scan(&raw); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return true, err
		}
		if !s.StoragePolicy().CheckpointPages {
			b, err := legacy(raw)
			if err != nil {
				return true, err
			}
			if len(b) > 16<<20 {
				return true, fmt.Errorf("observation checkpoint exceeds size limit")
			}
			if b != nil {
				if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_meta(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=NOW()`, key, string(b)); err != nil {
					return true, err
				}
			}
			return true, tx.Commit()
		}
	}
	next, err := update(old, raw)
	if err != nil {
		return true, err
	}
	if next.Metadata == nil {
		return true, nil
	}
	total := len(next.Metadata)
	if total > inputChunkBytes {
		return true, fmt.Errorf("observation metadata exceeds byte budget")
	}
	for k, b := range next.Pages {
		total += len(b)
		if len(k) > 4096 || len(b) > 64<<10 || total > 16<<20 {
			return true, fmt.Errorf("observation pages exceed byte budget")
		}
	}
	for k, b := range next.Pages {
		if bytes.Equal(old.Pages[k], b) {
			continue
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_observation_checkpoint_entries(checkpoint_key,bucket,entry_key,value) VALUES($1,'pages',$2,$3) ON CONFLICT(checkpoint_key,bucket,entry_key) DO UPDATE SET value=EXCLUDED.value`, key, k, b); err != nil {
			return true, err
		}
	}
	for k := range old.Pages {
		if _, ok := next.Pages[k]; !ok {
			if _, err = tx.ExecContext(ctx, `DELETE FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND bucket='pages' AND entry_key=$2`, key, k); err != nil {
				return true, err
			}
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_observation_checkpoint_heads(checkpoint_key,revision,metadata,updated_at) VALUES($1,$2,$3,$4) ON CONFLICT(checkpoint_key) DO UPDATE SET revision=EXCLUDED.revision,metadata=EXCLUDED.metadata,updated_at=EXCLUDED.updated_at`, key, revision+1, next.Metadata, time.Now().UTC()); err != nil {
		return true, err
	}
	return true, tx.Commit()
}
