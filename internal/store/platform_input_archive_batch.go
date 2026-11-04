package store

import (
	"context"
	"fmt"
	"time"

	"fugue/internal/inputarchive"
)

// RunCompilerInputArchiveBatch copies a bounded set with a single worker. It
// never changes read routes, fences writers, or deletes source rows. Verified
// inputs are skipped on the next run, so no unbounded in-memory cursor is kept.
func RunCompilerInputArchiveBatch(ctx context.Context, databaseURL string, limit int, apply bool, emit func(CompilerInputArchiveMigrationReport) error) error {
	return runCompilerInputArchiveBatch(ctx, databaseURL, limit, false, emit, func(ctx context.Context, digest string) (CompilerInputArchiveMigrationReport, error) {
		return RunCompilerInputArchiveMigration(ctx, databaseURL, CompilerInputArchiveMigrationOptions{Digest: digest, Apply: apply, SegmentBytes: 16 << 20})
	})
}

// RunCompilerInputArchiveRouteBatch only selects fully verified, unrouted
// receipts. Each item independently rechecks the external bytes, source digest,
// epoch and CAS before committing its route. A failure stops the batch; already
// committed routes remain valid and are skipped on resume. It never retires or
// deletes the source relation.
func RunCompilerInputArchiveRouteBatch(ctx context.Context, databaseURL string, limit int, emit func(CompilerInputArchiveMigrationReport) error) error {
	enabled, err := inputarchive.RoutingEnabledFromEnv()
	if err != nil || !enabled {
		return fmt.Errorf("compiler input archive routing opt-in is required: %w", ErrConflict)
	}
	return runCompilerInputArchiveBatch(ctx, databaseURL, limit, true, emit, func(ctx context.Context, digest string) (CompilerInputArchiveMigrationReport, error) {
		return RunCompilerInputArchiveRoute(ctx, databaseURL, CompilerInputArchiveRouteOptions{Digest: digest, Apply: true})
	})
}

func runCompilerInputArchiveBatch(ctx context.Context, databaseURL string, limit int, routing bool, emit func(CompilerInputArchiveMigrationReport) error, process func(context.Context, string) (CompilerInputArchiveMigrationReport, error)) error {
	if limit < 1 || limit > 1000 || emit == nil || process == nil {
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
		query := `SELECT m.content_hash FROM fugue_platform_input_manifests m
		 LEFT JOIN fugue_platform_input_archives a ON a.content_hash=m.content_hash
		 WHERE m.format IN ($1,$2) AND COALESCE(to_jsonb(m)->>'storage_location','legacy')='legacy'
		 AND (a.state IS NULL OR a.state NOT IN ('verified','routed'))
		 ORDER BY m.size_bytes,m.content_hash LIMIT $3`
		if routing {
			query = `SELECT m.content_hash FROM fugue_platform_input_manifests m
			 JOIN fugue_platform_input_archives a ON a.content_hash=m.content_hash
			 WHERE m.format IN ($1,$2) AND COALESCE(to_jsonb(m)->>'storage_location','legacy')='legacy'
			 AND a.state='verified' AND a.verified_digest=m.content_hash AND a.verified_at IS NOT NULL
			 ORDER BY m.size_bytes,m.content_hash LIMIT $3`
		}
		rows, err := db.QueryContext(ctx, query, inputFormatV1, inputFormatCodecV2, limit)
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
			report, err := process(ctx, digest)
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
