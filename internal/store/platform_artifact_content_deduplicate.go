package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// At most four bodies and 8MiB per tick; keyset cursor bounds metadata work
// even after all old bodies have been converted. The cursor is durable/shared
// across API/controller replicas. Reaching the end wraps on the next tick.
// This removes only a verified duplicate, never an artifact or its CAS body.
func (s *Store) deduplicatePlatformArtifactContent(parent context.Context) (int, error) {
	if s.db == nil || !s.artifactContentReferencesEnabled() {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, statement := range []string{`SET LOCAL lock_timeout='100ms'`, `SET LOCAL statement_timeout='2s'`, `SET LOCAL work_mem='1MB'`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return 0, err
		}
	}
	var locked bool
	if err = tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(315609238744298)`).Scan(&locked); err != nil || !locked {
		return 0, err
	}
	var cursor string
	err = tx.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key='artifact-content-deduplicate-cursor-v1'`).Scan(&cursor)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,content_json IS NOT NULL FROM fugue_platform_artifacts WHERE id>$1 ORDER BY id LIMIT 64`, cursor)
	if err != nil {
		return 0, err
	}
	type entry struct {
		id     string
		inline bool
	}
	entries := []entry{}
	for rows.Next() {
		var e entry
		if err = rows.Scan(&e.id, &e.inline); err != nil {
			break
		}
		entries = append(entries, e)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return 0, err
	}
	count, bytes := 0, 0
	refused := false
	if len(entries) == 0 {
		cursor = ""
	}
	for _, e := range entries {
		previousCursor := cursor
		cursor = e.id
		if !e.inline {
			continue
		}
		var size int
		if err = tx.QueryRowContext(ctx, `SELECT octet_length(content_json::text) FROM fugue_platform_artifacts WHERE id=$1 FOR UPDATE`, e.id).Scan(&size); err != nil {
			return 0, err
		}
		// Oversized/corrupt rows remain untouched; their inability to compact
		// must not authorize a repair or an unbounded historical body read.
		if size > 8<<20 {
			refused = true
			continue
		}
		if bytes+size > 8<<20 {
			cursor = previousCursor
			break
		}
		a, err := pgGetPlatformArtifactForUpdate(ctx, tx, e.id, false)
		if err != nil {
			return 0, err
		}
		if err = s.VerifyPlatformArtifactIntegrity(a); err != nil {
			refused = true
			continue
		}
		if _, err = pgUpsertPlatformArtifactContent(ctx, tx, buildPlatformArtifactContent(a)); err != nil {
			if errors.Is(err, ErrConflict) {
				refused = true
				continue
			}
			return 0, err
		}
		if err = pgAdmitPlatformArtifactContentReference(ctx, tx, a.ContentHash); err != nil {
			if errors.Is(err, ErrConflict) {
				refused = true
				continue
			}
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE fugue_platform_artifacts SET content_json=NULL,content_ref_hash=content_hash WHERE id=$1 AND content_json IS NOT NULL`, a.ID); err != nil {
			return 0, err
		}
		bytes += size
		count++
		if count == 4 {
			break
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_meta(key,value,updated_at) VALUES('artifact-content-deduplicate-cursor-v1',$1,now()) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, cursor); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	if refused {
		return count, ErrConflict
	}
	return count, nil
}

// Call ONLY after comparing the complete original content against CAS. Legacy
// untyped content can then be classified without touching body/timestamps.
func pgAdmitPlatformArtifactContentReference(ctx context.Context, db platformStateDB, digest string) error {
	var kind string
	if err := db.QueryRowContext(ctx, `SELECT content_kind FROM fugue_platform_artifact_contents WHERE content_hash=$1`, digest).Scan(&kind); err != nil {
		return err
	}
	if kind == "platform-artifact-v1" {
		return nil
	}
	if kind != "" {
		return ErrConflict
	}
	_, err := db.ExecContext(ctx, `UPDATE fugue_platform_artifact_contents SET content_kind='platform-artifact-v1' WHERE content_hash=$1 AND content_kind=''`, digest)
	if err != nil {
		return err
	}
	if err = db.QueryRowContext(ctx, `SELECT content_kind FROM fugue_platform_artifact_contents WHERE content_hash=$1`, digest).Scan(&kind); err != nil {
		return err
	}
	if kind != "platform-artifact-v1" {
		return ErrConflict
	}
	return nil
}
