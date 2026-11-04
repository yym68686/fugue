package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"fugue/internal/platformconfig"
)

func TestCompilerInputArchiveRouteBatchResumesAndSkipsIncompleteReceipts(t *testing.T) {
	s := archiveTestStore(t)
	snapshot, _ := chunkedInputFixture(t)
	digests := []string{}
	objects := &archiveTestObjects{}
	backend := "sha256:" + strings.Repeat("e", 64)
	t.Cleanup(func() {
		for _, digest := range digests {
			for _, table := range []string{"fugue_platform_input_archives", "fugue_platform_input_chunks", "fugue_platform_input_manifests"} {
				if _, err := s.db.Exec("DELETE FROM "+table+" WHERE content_hash=$1", digest); err != nil {
					t.Error(err)
				}
			}
		}
	})
	for i := 0; i < 3; i++ {
		snapshot.IntentGeneration += "-next"
		digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		digests = append(digests, digest)
		if err = s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
			t.Fatal(err)
		}
		options := CompilerInputArchiveMigrationOptions{Digest: digest, Apply: true, SegmentBytes: 65536}
		if i < 2 {
			if _, err = copyCompilerInputArchive(t.Context(), s.db, objects, backend, options); err != nil {
				t.Fatal(err)
			}
		} else if _, err = claimCompilerInputArchive(t.Context(), s.db, backend, options); err != nil {
			t.Fatal(err)
		}
	}
	processed := []string{}
	process := func(ctx context.Context, digest string) (CompilerInputArchiveMigrationReport, error) {
		processed = append(processed, digest)
		return routeCompilerInputArchive(ctx, s.db, objects, backend, digest)
	}
	emit := func(report CompilerInputArchiveMigrationReport) error {
		if !report.Applied || report.State != "routed" {
			t.Fatal("route receipt was not committed", report)
		}
		return nil
	}
	if err := runCompilerInputArchiveBatch(t.Context(), s.databaseURL, 1, true, emit, process); err != nil {
		t.Fatal(err)
	}
	if len(processed) != 1 || processed[0] != digests[0] {
		t.Fatal("batch did not honor the exact limit and size order", processed)
	}
	failure := errors.New("synthetic transient routing failure")
	if err := runCompilerInputArchiveBatch(t.Context(), s.databaseURL, 1000, true, emit, func(context.Context, string) (CompilerInputArchiveMigrationReport, error) {
		return CompilerInputArchiveMigrationReport{}, failure
	}); !errors.Is(err, failure) {
		t.Fatal("batch did not stop on the first failure", err)
	}
	if err := runCompilerInputArchiveBatch(t.Context(), s.databaseURL, 1000, true, emit, process); err != nil {
		t.Fatal("batch could not resume", err)
	}
	if len(processed) != 2 || processed[1] != digests[1] {
		t.Fatal("batch replayed routed inputs or included incomplete ones", processed)
	}
	if err := runCompilerInputArchiveBatch(t.Context(), s.databaseURL, 1000, true, emit, process); err != nil || len(processed) != 2 {
		t.Fatal("exhausted routing batch was not inert", err)
	}
	var routes, chunks int
	if err := s.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_input_archives WHERE state='routed'),(SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash=ANY($1::text[]))`, digests).Scan(&routes, &chunks); err != nil || routes != 2 || chunks == 0 {
		t.Fatal("routing batch did not retain old source bytes", routes, chunks, err)
	}
}

func TestCompilerInputArchiveRouteBatchRequiresExplicitOptIn(t *testing.T) {
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_ROUTING_ENABLED", "false")
	if err := RunCompilerInputArchiveRouteBatch(t.Context(), "", 1, func(CompilerInputArchiveMigrationReport) error { return nil }); !errors.Is(err, ErrConflict) {
		t.Fatal("routing batch accepted a missing routing opt-in", err)
	}
}
