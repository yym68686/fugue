package store

import (
	"context"
	"fmt"
	"time"
)

// RunCompilerInputArchiveBatch copies a bounded set with a single worker. It
// never changes read routes, fences writers, or deletes source rows. Verified
// inputs are skipped on the next run, so no unbounded in-memory cursor is kept.
func RunCompilerInputArchiveBatch(ctx context.Context, databaseURL string, limit int, apply bool, emit func(CompilerInputArchiveMigrationReport) error) error {
	if limit < 1 || limit > 1000 || emit == nil {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Minute)
	defer cancel()
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	s := &Store{db: db, databaseURL: databaseURL, dbReady: true}
	acquired, err := s.WithAdvisoryLock(ctx, "compiler-input-archive-batch", func() error {
		rows, err := db.QueryContext(ctx, `SELECT m.content_hash FROM fugue_platform_input_manifests m
		 LEFT JOIN fugue_platform_input_archives a ON a.content_hash=m.content_hash
		 WHERE m.format IN ($1,$2) AND COALESCE(to_jsonb(m)->>'storage_location','legacy')='legacy'
		 AND (a.state IS NULL OR a.state NOT IN ('verified','routed'))
		 ORDER BY m.size_bytes,m.content_hash LIMIT $3`, inputFormatV1, inputFormatCodecV2, limit)
		if err != nil {
			return err
		}
		digests := []string{}
		for rows.Next() {
			var digest string
			if err = rows.Scan(&digest); err != nil {
				rows.Close()
				return err
			}
			digests = append(digests, digest)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, digest := range digests {
			report, err := RunCompilerInputArchiveMigration(ctx, databaseURL, CompilerInputArchiveMigrationOptions{Digest: digest, Apply: apply, SegmentBytes: 16 << 20})
			if err != nil {
				return err
			}
			if err = emit(report); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !acquired {
		return fmt.Errorf("archive batch worker is already active: %w", ErrConflict)
	}
	return nil
}
