package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestCompilerInputJSONBQueuedWriterSeesCommittedFence(t *testing.T) {
	s := archiveTestStore(t)
	resetArchiveFenceFixture(t, s)
	s.storagePolicy.value.CompilerInputChunks = false
	holder, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if _, err = holder.Exec(`SELECT pg_advisory_xact_lock(315609238744290); UPDATE fugue_platform_input_archive_control SET writes_fenced=true WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	snapshot, digest := chunkedInputFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.EnsurePlatformCompilerInputContext(ctx, snapshot, digest) }()
	const lockID uint64 = 315609238744290
	for {
		var waiting bool
		err = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid::bigint=$1 AND objid::bigint=$2 AND objsubid=1)`, lockID>>32, lockID&0xffffffff).Scan(&waiting)
		if err != nil {
			t.Fatal("queued writer never reached the common barrier", err)
		}
		if waiting {
			break
		}
		select {
		case err := <-done:
			t.Fatal("writer bypassed the uncommitted fence barrier", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err = holder.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal("queued writer did not continue in its original budget", err)
	}
	var jsonRows, hotRows int
	if err = s.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_artifact_contents WHERE content_hash=$1),(SELECT count(*) FROM fugue_platform_input_hot_chunks WHERE content_hash=$1)`, digest).Scan(&jsonRows, &hotRows); err != nil || jsonRows != 0 || hotRows == 0 {
		t.Fatal("writer used stale pre-barrier fence state", jsonRows, hotRows, err)
	}
}

func TestCompilerInputJSONBWriterUsesBarrierAndCallerBudget(t *testing.T) {
	s := archiveTestStore(t)
	resetArchiveFenceFixture(t, s)
	s.storagePolicy.value.CompilerInputChunks = false
	snapshot, digest := chunkedInputFixture(t)
	before := compilerInputWriterStats.snapshot()
	if err := s.EnsurePlatformCompilerInputContext(t.Context(), snapshot, digest); err != nil {
		t.Fatal("unfenced policy-disabled writer", err)
	}
	var created time.Time
	if err := s.db.QueryRow(`SELECT created_at FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsurePlatformCompilerInputContext(t.Context(), snapshot, digest); err != nil {
		t.Fatal("idempotent JSONB ensure", err)
	}
	var unchanged bool
	if err := s.db.QueryRow(`SELECT created_at=$2 FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest, created).Scan(&unchanged); err != nil || !unchanged {
		t.Fatal("idempotent ensure changed the canonical observation", err)
	}
	after := compilerInputWriterStats.snapshot()
	if after["attempts"].(uint64) != before["attempts"].(uint64)+2 || after["successful_returns"].(uint64) != before["successful_returns"].(uint64)+2 || after["in_flight"] != before["in_flight"] {
		t.Fatal("policy-disabled writer outcomes were not observed once", before, after)
	}

	holder, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if _, err = holder.Exec(`SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		t.Fatal(err)
	}
	changed := snapshot
	changed.IntentGeneration += "-blocked"
	blockedDigest, _ := platformconfig.RuntimeSnapshotDigest(changed)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err = s.EnsurePlatformCompilerInputContext(ctx, changed, blockedDigest); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("policy-disabled writer ignored the caller deadline/barrier", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("caller deadline was replaced with the writer's ten-second budget")
	}
	if err = holder.Commit(); err != nil {
		t.Fatal(err)
	}
	var rows int
	if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_artifact_contents WHERE content_hash=$1`, blockedDigest).Scan(&rows); err != nil || rows != 0 {
		t.Fatal("canceled queued writer persisted bytes", rows, err)
	}
	if _, err = s.db.Exec(`UPDATE fugue_platform_input_hot_chunks SET data=set_byte(data,0,33) WHERE content_hash=$1 AND ordinal=0`, digest); err != nil {
		t.Fatal(err)
	}
	if err = s.EnsurePlatformCompilerInputContext(t.Context(), snapshot, digest); !errors.Is(err, ErrConflict) {
		t.Fatal("corrupt canonical local bytes were replaced or accepted", err)
	}
}

func TestCompilerInputCurrentWriterPoliciesSurviveMissingOldTableAndRestart(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		t.Run(fmt.Sprintf("fenced=%t", fenced), func(t *testing.T) {
			testCompilerInputCurrentWriterPolicies(t, fenced)
		})
	}
}

func testCompilerInputCurrentWriterPolicies(t *testing.T, fenced bool) {
	for _, policy := range []string{"jsonb", "v1", "v2", "v3", "v4", "separated-budget-fallback"} {
		t.Run(policy, func(t *testing.T) {
			s := archiveTestStore(t)
			resetArchiveFenceFixture(t, s)
			s.storagePolicy.value.CompilerInputChunks = policy != "jsonb"
			s.storagePolicy.codec = compilerInputCodecPolicy{Version: 1, Enabled: policy == "v2" || policy == "separated-budget-fallback"}
			s.storagePolicy.shared = compilerInputCodecPolicy{Version: 1, Enabled: policy == "v3"}
			s.storagePolicy.separated = compilerInputCodecPolicy{Version: 1, Enabled: policy == "v4" || policy == "separated-budget-fallback"}
			if fenced {
				if _, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false); err != nil {
					t.Fatal("arm actual database fence", err)
				}
			}
			// Hide the old relation, retaining every fixture row for cleanup. New
			// writers/readers must work without its name or any process cache.
			if _, err := s.db.Exec(`ALTER TABLE fugue_platform_input_chunks RENAME TO fugue_platform_input_chunks_writer_hidden`); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := s.db.Exec(`ALTER TABLE fugue_platform_input_chunks_writer_hidden RENAME TO fugue_platform_input_chunks`); err != nil {
					t.Error("restore disposable old relation", err)
				}
			})
			snapshot := separatedFixture(t)
			if policy == "separated-budget-fallback" {
				snapshot.Facts["large_config"] = strings.Repeat("config", separatedFrameLimit/6+1)
			}
			digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
			before := compilerInputWriterStats.snapshot()
			if err := s.EnsurePlatformCompilerInputContext(t.Context(), snapshot, digest); err != nil {
				t.Fatal("fenced writer depended on old chunks", err)
			}
			after := compilerInputWriterStats.snapshot()
			if after["attempts"].(uint64) != before["attempts"].(uint64)+1 || after["successful_returns"].(uint64) != before["successful_returns"].(uint64)+1 {
				t.Fatal("redirected writer counted more than one outer outcome", before, after)
			}
			m, err := readInputManifest(t.Context(), s.db, digest)
			if err != nil {
				t.Fatal(err)
			}
			wantFormat := map[string]string{"jsonb": inputFormatV1, "v1": inputFormatV1, "v2": inputFormatCodecV2, "v3": inputFormatSharedV3, "v4": inputFormatSeparatedV4, "separated-budget-fallback": inputFormatCodecV2}[policy]
			if m.Format != wantFormat || ((m.Format == inputFormatV1 || m.Format == inputFormatCodecV2) && m.Location != "hot") {
				t.Fatal("fenced policy/fallback chose the wrong representation", m)
			}
			var jsonRows, oldRows int
			if err = s.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_artifact_contents WHERE content_hash=$1),(SELECT count(*) FROM fugue_platform_input_chunks_writer_hidden WHERE content_hash=$1)`, digest).Scan(&jsonRows, &oldRows); err != nil || jsonRows != 0 || oldRows != 0 {
				t.Fatal("fenced input leaked into legacy storage", jsonRows, oldRows, err)
			}
			db, err := openPostgresDatabase(s.databaseURL)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			restarted := &Store{db: db, databaseURL: s.databaseURL, dbReady: true, compilerInputArchiveReadLeaseEnabled: true, compilerInputArchiveObjects: &writerRejectArchiveObjects{}}
			// Restart with chunks policy off, independent of the writer's format.
			got, err := restarted.GetPlatformCompilerInput(digest)
			if err != nil {
				t.Fatal("fresh process could not read independent bytes", err)
			}
			if actual, err := platformconfig.RuntimeSnapshotDigest(got); err != nil || actual != digest {
				t.Fatal("restart changed original full SHA", actual, err)
			}
			if err = restarted.EnsurePlatformCompilerInputContext(t.Context(), snapshot, digest); err != nil {
				t.Fatal("policy-disabled restart could not verify existing format", err)
			}
			// The shared contents table also stores signed artifact bodies. Input
			// isolation must not indiscriminately prohibit their normal writes.
			now := time.Now().UTC()
			artifact := buildPlatformArtifactContent(model.PlatformArtifact{Content: map[string]any{"normal_artifact": policy}, CreatedAt: now, UpdatedAt: now})
			artifact.ContentHash, err = platformArtifactContentHash(artifact.Content)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = pgUpsertPlatformArtifactContent(t.Context(), db, artifact); err != nil {
				t.Fatal("input fence blocked normal artifact payloads", err)
			}
		})
	}
}

type writerRejectArchiveObjects struct{ gets int }

func (o *writerRejectArchiveObjects) Get(context.Context, string) (io.ReadCloser, error) {
	o.gets++
	return nil, errors.New("normal writer must not GET cold history")
}
func (*writerRejectArchiveObjects) Put(context.Context, string, []byte) error {
	return errors.New("normal writer must not PUT archives")
}

func TestCompilerInputColdWriterRequiresPreparedLocalBytesAcrossPolicies(t *testing.T) {
	s := archiveTestStore(t)
	resetArchiveFenceFixture(t, s)
	digest, objects, backend := prepareVerifiedRoutedArchive(t, s)
	// Capture this synthetic fixture's original local bytes before testing the
	// cold route. No archive environment/credentials are needed by a writer.
	s.compilerInputArchiveReadLeaseEnabled = false
	snapshot, err := s.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal("fixture's original full-SHA read", err)
	}
	s.compilerInputArchiveReadLeaseEnabled = true
	reject := &writerRejectArchiveObjects{}
	s.compilerInputArchiveObjects = reject
	for _, readerEnabled := range []bool{true, false} {
		s.compilerInputArchiveReadLeaseEnabled = readerEnabled
		for _, chunks := range []bool{true, false} {
			s.storagePolicy.value.CompilerInputChunks = chunks
			if err = s.EnsurePlatformCompilerInputContext(t.Context(), snapshot, digest); !errors.Is(err, ErrConflict) {
				t.Fatal("cold input was restored implicitly or overwritten", chunks, readerEnabled, err)
			}
		}
	}
	if reject.gets != 0 {
		t.Fatal("normal writer attempted remote recovery", reject.gets)
	}
	var rows int
	if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_artifact_contents WHERE content_hash=$1`, digest).Scan(&rows); err != nil || rows != 0 {
		t.Fatal("policy toggle replaced the cold canonical representation", rows, err)
	}
	if _, err = copyCompilerInputArchiveHot(t.Context(), s.db, objects, backend, CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: hotCopyTestBudget}); err != nil {
		t.Fatal("explicit full-SHA hot preparation", err)
	}
	for _, chunks := range []bool{true, false} {
		s.storagePolicy.value.CompilerInputChunks = chunks
		if err = s.EnsurePlatformCompilerInputContext(t.Context(), snapshot, digest); err != nil {
			t.Fatal("prepared input writer depended on archive", chunks, err)
		}
	}
	if _, err = s.db.Exec(`UPDATE fugue_platform_input_hot_chunks SET data=decode('00','hex') WHERE content_hash=$1 AND ordinal=0`, digest); err != nil {
		t.Fatal(err)
	}
	if err = s.EnsurePlatformCompilerInputContext(t.Context(), snapshot, digest); err == nil {
		t.Fatal("normal writer concealed corrupt prepared bytes")
	}
	if reject.gets != 0 {
		t.Fatal("corrupt local input was silently recovered", reject.gets)
	}
}
