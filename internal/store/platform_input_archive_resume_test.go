package store

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"fugue/internal/inputarchive"
)

func TestCompilerInputArchiveResumeIdentityGolden(t *testing.T) {
	// Shared with the offline Python proof verifier. Include HTML characters
	// and a Unicode line separator so different JSON encoders cannot silently
	// sign a different ordered receipt or source identity.
	source := inputManifest{Digest: "sha256:" + strings.Repeat("d", 64), Format: inputFormatCodecV2,
		Location: "legacy", Size: 65537, ChunkSize: 32769, Count: 2,
		Created: time.Date(2026, time.January, 2, 3, 4, 5, 123456000, time.UTC)}
	if got := inputArchiveSourceIdentityHash(source); got != "sha256:8f028c5cdb3a1560b5288aee315c6d483742f6a89383a2e14f9a84b0d7522e70" {
		t.Fatalf("source identity differs from cross-language golden: %s", got)
	}
	segments := []inputarchive.Segment{{Ordinal: 0, Key: "inputs/neutral/<shared>&\u2028/00000-" + strings.Repeat("1", 64) + ".gz",
		RawBytes: 65536, StoredBytes: 1234, RawHash: "sha256:" + strings.Repeat("2", 64), StoredHash: "sha256:" + strings.Repeat("1", 64)}}
	if got := inputArchiveIdentityHash(segments); got != "sha256:1fe01b3610813f3ed16cb645db4819b0104d0c6eebb3c94f38182dfb2ff3d331" {
		t.Fatalf("receipt identity differs from cross-language golden: %s", got)
	}
}

func readResumeTestExpectation(t *testing.T, s *Store, digest string) inputArchiveResumeExpectation {
	t.Helper()
	m, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil {
		t.Fatal(err)
	}
	e := inputArchiveResumeExpectation{SourceManifestSHA256: inputArchiveSourceIdentityHash(m)}
	if err = s.db.QueryRowContext(t.Context(), `SELECT object_prefix,source_epoch,fencing_token,backend_hash FROM fugue_platform_input_archives WHERE content_hash=$1`, digest).Scan(&e.Prefix, &e.Epoch, &e.Token, &e.BackendHash); err != nil {
		t.Fatal(err)
	}
	rows, err := s.db.QueryContext(t.Context(), `SELECT ordinal,object_key,raw_bytes,stored_bytes,segment_hash,stored_hash FROM fugue_platform_input_archive_segments WHERE content_hash=$1 AND uploaded_at IS NOT NULL AND verified_at IS NOT NULL ORDER BY ordinal`, digest)
	if err != nil {
		t.Fatal(err)
	}
	var segments []inputarchive.Segment
	for rows.Next() {
		var segment inputarchive.Segment
		if err = rows.Scan(&segment.Ordinal, &segment.Key, &segment.RawBytes, &segment.StoredBytes, &segment.RawHash, &segment.StoredHash); err != nil {
			break
		}
		segments = append(segments, segment)
		e.StoredBytes += int64(segment.StoredBytes)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	e.Segments, e.ReceiptsSHA256 = len(segments), inputArchiveIdentityHash(segments)
	return e
}

func TestCompilerInputArchiveExactResumeRejectsDriftThenReusesPrefix(t *testing.T) {
	s := archiveTestStore(t)
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	source, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil {
		t.Fatal(err)
	}
	objects := &archiveTestObjects{failSecond: true}
	backend := "sha256:" + strings.Repeat("d", 64)
	options := CompilerInputArchiveMigrationOptions{Digest: digest, Apply: true, SegmentBytes: 65536,
		expectedFormat: source.Format, expectedRawBytes: source.Size, requireNew: true, sourcePageChunks: 1}
	if _, err = copyCompilerInputArchive(t.Context(), s.db, objects, backend, options); err == nil {
		t.Fatal("fixture did not fail after its first verified segment")
	}
	e := readResumeTestExpectation(t, s, digest)
	if e.Segments != 1 || e.Token != 1 || e.validate() != nil {
		t.Fatal("unexpected failed prefix", e)
	}
	n := (source.Size + int64(options.SegmentBytes) - 1) / int64(options.SegmentBytes)
	contract := CompilerInputArchiveCopyContract{Version: 2, Digest: digest, SourceFormat: source.Format, RawBytes: source.Size,
		BackendHash: backend, SegmentBytes: options.SegmentBytes, MaxPutBytes: 2 * source.Size, MaxGetBytes: 4 * source.Size,
		MaxPutRequests: n - 1, MaxGetRequests: 2 * n, BytesPerSecond: 2 << 20, MaxSeconds: 540,
		Resume: &CompilerInputArchiveCopyResumeContract{SourceManifestSHA256: e.SourceManifestSHA256, ReceiptsSHA256: e.ReceiptsSHA256,
			Prefix: e.Prefix, Epoch: e.Epoch, Token: e.Token, VerifiedSegments: e.Segments, VerifiedStoredBytes: e.StoredBytes,
			PriorAttemptSHA256: "sha256:" + strings.Repeat("a", 64), PriorReceiptSHA256: "sha256:" + strings.Repeat("b", 64),
			PriorReceiptUID: "11111111-2222-4333-8444-555555555555"}}
	if contract.Validate() != nil || *contract.resumeExpectation() != e {
		t.Fatal("native v2 contract differs from the precise failed prefix")
	}
	options.requireNew, options.expectedResume = contract.Version == 1, contract.resumeExpectation()
	if _, err = copyCompilerInputArchive(t.Context(), s.db, objects, backend, options); !errors.Is(err, ErrConflict) {
		t.Fatal("exact expectation stole an active lease", err)
	}
	if _, err = s.db.ExecContext(t.Context(), `UPDATE fugue_platform_input_archive_leases SET expires_at=now()-interval '1 second' WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*inputArchiveResumeExpectation){
		"token":        func(x *inputArchiveResumeExpectation) { x.Token++ },
		"epoch":        func(x *inputArchiveResumeExpectation) { x.Epoch++ },
		"backend":      func(x *inputArchiveResumeExpectation) { x.BackendHash = "sha256:" + strings.Repeat("f", 64) },
		"source":       func(x *inputArchiveResumeExpectation) { x.SourceManifestSHA256 = backend },
		"receipt":      func(x *inputArchiveResumeExpectation) { x.ReceiptsSHA256 = backend },
		"prefix":       func(x *inputArchiveResumeExpectation) { x.Prefix += "-different" },
		"count":        func(x *inputArchiveResumeExpectation) { x.Segments++ },
		"stored bytes": func(x *inputArchiveResumeExpectation) { x.StoredBytes++ },
	} {
		t.Run(name, func(t *testing.T) {
			x := e
			mutate(&x)
			attempt := options
			attempt.expectedResume = &x
			if _, err := copyCompilerInputArchive(t.Context(), s.db, objects, backend, attempt); !errors.Is(err, ErrConflict) {
				t.Fatal("changed expectation was accepted", err)
			}
			if after := readResumeTestExpectation(t, s, digest); after != e {
				t.Fatal("refused claim changed its durable prefix", after)
			}
		})
	}
	if _, err = s.db.ExecContext(t.Context(), `UPDATE fugue_platform_input_archive_control SET writes_fenced=true WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	if _, err = copyCompilerInputArchive(t.Context(), s.db, objects, backend, options); !errors.Is(err, ErrConflict) {
		t.Fatal("write fence did not block exact resume", err)
	}
	if _, err = s.db.ExecContext(t.Context(), `UPDATE fugue_platform_input_archive_control SET writes_fenced=false WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	// A change to an actual receipt, rather than just the declaration, must
	// also be rejected before the lease/token or any object is touched.
	var originalHash string
	if err = s.db.QueryRowContext(t.Context(), `SELECT segment_hash FROM fugue_platform_input_archive_segments WHERE content_hash=$1 AND ordinal=0`, digest).Scan(&originalHash); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.ExecContext(t.Context(), `UPDATE fugue_platform_input_archive_segments SET segment_hash=$2 WHERE content_hash=$1 AND ordinal=0`, digest, backend); err != nil {
		t.Fatal(err)
	}
	if _, err = copyCompilerInputArchive(t.Context(), s.db, objects, backend, options); !errors.Is(err, ErrConflict) {
		t.Fatal("changed durable receipt was accepted", err)
	}
	if _, err = s.db.ExecContext(t.Context(), `UPDATE fugue_platform_input_archive_segments SET segment_hash=$2 WHERE content_hash=$1 AND ordinal=0`, digest, originalHash); err != nil {
		t.Fatal(err)
	}
	objects.failSecond = false
	budget, err := inputarchive.NewBudgetObjects(objects, objects, inputarchive.CopyBudget{
		MaxPutBytes: 2 * source.Size, MaxGetBytes: 4 * source.Size,
		MaxPutRequests: n - 1, MaxGetRequests: 2 * n, BytesPerSecond: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	options.wrapSource = func(r io.Reader) io.Reader { return budget.Source(t.Context(), r) }
	report, err := copyCompilerInputArchive(t.Context(), s.db, budget, backend, options)
	if err != nil || report.State != "verified" || report.FencingToken != e.Token+1 {
		t.Fatal("exact resume did not complete", report, err)
	}
	if usage := budget.Usage(); usage.SourceBytes != source.Size || usage.PutRequests != n-1 || usage.GetRequests != 2*n {
		t.Fatal("resume did not reuse its prefix and perform both complete passes", usage)
	}
	if after, err := readInputManifest(t.Context(), s.db, digest); err != nil || after != source {
		t.Fatal("resume changed source identity or route", after, err)
	}
	var rows, active int
	if err = s.db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash=$1),(SELECT count(*) FROM fugue_platform_input_archive_leases WHERE content_hash=$1 AND expires_at>now())`, digest).Scan(&rows, &active); err != nil || rows != source.Count || active != 0 {
		t.Fatal("source rows or terminal lease differ", rows, active, err)
	}
	if _, err = copyCompilerInputArchive(t.Context(), s.db, budget, backend, options); !errors.Is(err, ErrConflict) {
		t.Fatal("stale exact resume replay was accepted", err)
	}
}

func TestCompilerInputArchiveExactResumeOneClaimWins(t *testing.T) {
	s := archiveTestStore(t)
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	objects := &archiveTestObjects{failSecond: true}
	backend := "sha256:" + strings.Repeat("e", 64)
	options := CompilerInputArchiveMigrationOptions{Digest: digest, Apply: true, SegmentBytes: 65536}
	if _, err := copyCompilerInputArchive(t.Context(), s.db, objects, backend, options); err == nil {
		t.Fatal("fixture did not fail")
	}
	e := readResumeTestExpectation(t, s, digest)
	options.expectedResume = &e
	if _, err := s.db.ExecContext(t.Context(), `UPDATE fugue_platform_input_archive_leases SET expires_at=now()-interval '1 second' WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	start, results := make(chan struct{}), make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			_, err := claimCompilerInputArchive(context.Background(), s.db, backend, options)
			results <- err
		}()
	}
	close(start)
	winners := 0
	for range 2 {
		if err := <-results; err == nil {
			winners++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal("unexpected concurrent refusal", err)
		}
	}
	if winners != 1 {
		t.Fatal("concurrent exact resumes did not have a single winner", winners)
	}
	var token int64
	var active int
	if err := s.db.QueryRowContext(t.Context(), `SELECT (SELECT fencing_token FROM fugue_platform_input_archives WHERE content_hash=$1),(SELECT count(*) FROM fugue_platform_input_archive_leases WHERE content_hash=$1 AND expires_at>now())`, digest).Scan(&token, &active); err != nil || token != e.Token+1 || active != 1 {
		t.Fatal("concurrent resume token/lease differs", token, active, err)
	}
}
