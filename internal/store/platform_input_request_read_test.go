package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"fugue/internal/platformconfig"
)

func TestCompilerInputFullReadDefersDNSIndexPostgres(t *testing.T) {
	for _, format := range []string{"codec", "separated"} {
		t.Run(format, func(t *testing.T) {
			var s *Store
			var snapshot platformconfig.RuntimeSnapshot
			var digest string
			if format == "codec" {
				s = codecTestStore(t)
				snapshot, digest = compilerDNSFixture(t)
				insertCodecFixture(t, s, snapshot, digest)
			} else {
				s = separatedTestStore(t)
				snapshot = separatedFixture(t)
				digest, _ = platformconfig.RuntimeSnapshotDigest(snapshot)
				if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
					t.Fatal(err)
				}
			}
			reader := &Store{databaseURL: s.databaseURL, db: s.db, dbReady: true}
			got, err := reader.GetPlatformCompilerInputContext(t.Context(), digest)
			if err != nil {
				t.Fatal(err)
			}
			if actual, err := platformconfig.RuntimeSnapshotDigest(got); err != nil || actual != digest {
				t.Fatal("full read changed canonical bytes", err)
			}
			if reader.compilerDNSInputs.get(digest) != nil {
				t.Fatal("full read performed unrelated DNS index preparation")
			}
			selected, err := reader.GetPlatformCompilerInputDNSSelections(digest, func(CompilerDNSSelectionSummary) bool { return false })
			if err != nil || len(selected.DNSSelections) != 0 || len(selected.Omitted) != len(snapshot.DNSSelections) {
				t.Fatal("cold selective read differs", err)
			}
			if reader.compilerDNSInputs.get(digest) == nil {
				t.Fatal("selective reader did not prepare its verified index")
			}
			// A valid stored chunk hash is not a substitute for the full input
			// SHA. The unselected evidence must remain protected on full reads.
			query := `UPDATE fugue_platform_input_chunks SET data=set_byte(data,octet_length(data)-1,42),chunk_hash='sha256:'||encode(sha256(set_byte(data,octet_length(data)-1,42)),'hex') WHERE content_hash=$1 AND ordinal=(SELECT max(ordinal) FROM fugue_platform_input_chunks WHERE content_hash=$1)`
			if format == "separated" {
				query = `UPDATE fugue_platform_input_shared_packs SET data=set_byte(data,5,(get_byte(data,5)+1)%256),stored_hash=sha256(set_byte(data,5,(get_byte(data,5)+1)%256)) WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_component_refs WHERE content_hash IN(SELECT component_hash FROM fugue_platform_input_separated_refs WHERE content_hash=$1))`
			}
			tx, err := s.db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			result, err := tx.ExecContext(t.Context(), query, digest)
			if err != nil {
				t.Fatal(err)
			}
			if n, _ := result.RowsAffected(); n == 0 {
				t.Fatal("corruption fixture did not change physical bytes")
			}
			m, err := readInputManifest(t.Context(), tx, digest)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reader.readCompilerInputSnapshot(t.Context(), tx, m, false); err == nil {
				t.Fatal("omitting DNS index preparation accepted corrupt evidence")
			}
		})
	}
}

func TestCompilerInputRequestCancellationReleasesReadLeasePostgres(t *testing.T) {
	s := boundedContentTestStore(t)
	snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: "cancel-read", PolicyGeneration: "policy"}
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	s.compilerInputArchiveReadLeaseEnabled = true
	blocker, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.ExecContext(t.Context(), `LOCK TABLE fugue_platform_input_chunks IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := s.GetPlatformCompilerInputContext(ctx, digest); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked read ignored caller deadline", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("short request retained the default full-read work budget")
	}
	// The request had acquired a session read lease before its chunk SELECT
	// blocked. Cancellation must release it, not just abandon the HTTP reply.
	// Driver cancellation can close a connection asynchronously. Wait at most
	// one second for PostgreSQL to observe that close and release its locks.
	drain, stopDrain := context.WithTimeout(t.Context(), time.Second)
	defer stopDrain()
	if _, err := blocker.ExecContext(drain, `SELECT pg_advisory_xact_lock($1)`, compilerInputArchiveReadLeaseLockID); err != nil {
		t.Fatal("canceled request leaked its session read lease", err)
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetPlatformCompilerInputContext(t.Context(), digest); err != nil {
		t.Fatal("subsequent reader could not recover after cancellation", err)
	}
	canceled, stop := context.WithCancel(t.Context())
	stop()
	if _, err := s.GetPlatformCompilerInputContext(canceled, digest); !errors.Is(err, context.Canceled) {
		t.Fatal("already canceled request accessed storage", err)
	}
}

func TestCompilerInputLegacyContentReadHonorsRequestCancellationPostgres(t *testing.T) {
	s := boundedContentTestStore(t)
	s.storagePolicy.value.CompilerInputChunks = false
	snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: "cancel-legacy-read", PolicyGeneration: "policy"}
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	blocker, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.ExecContext(t.Context(), `LOCK TABLE fugue_platform_artifact_contents IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := s.GetPlatformCompilerInputContext(ctx, digest); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("legacy fallback ignored caller deadline", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("legacy fallback restarted an independent read timeout")
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetPlatformCompilerInputContext(t.Context(), digest)
	if err != nil {
		t.Fatal("legacy input unavailable after cancellation", err)
	}
	if actual, err := platformconfig.RuntimeSnapshotDigest(got); err != nil || actual != digest {
		t.Fatal("legacy full SHA changed", err)
	}
}
