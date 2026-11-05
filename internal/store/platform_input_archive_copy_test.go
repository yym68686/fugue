package store

import (
	"errors"
	"io"
	"strings"
	"testing"

	"fugue/internal/inputarchive"
)

func TestCompilerInputArchiveCopyExactDeclarationBeforeAnyClaim(t *testing.T) {
	s := archiveTestStore(t)
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	m, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil {
		t.Fatal(err)
	}
	o := &archiveTestObjects{}
	backend := "sha256:" + strings.Repeat("c", 64)
	options := CompilerInputArchiveMigrationOptions{Digest: digest, Apply: true, SegmentBytes: 65536, expectedFormat: m.Format, expectedRawBytes: m.Size + 1, requireNew: true, sourcePageChunks: 1}
	if _, err = copyCompilerInputArchive(t.Context(), s.db, o, backend, options); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	var count int
	if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_archives WHERE content_hash=$1`, digest).Scan(&count); err != nil || count != 0 {
		t.Fatal("mismatched source wrote ledger", count, err)
	}
	options.expectedRawBytes = m.Size
	n := (m.Size + 65535) / 65536
	b, err := inputarchive.NewBudgetObjects(o, o, inputarchive.CopyBudget{MaxPutBytes: 2 * m.Size, MaxGetBytes: 4 * m.Size, MaxPutRequests: n, MaxGetRequests: 2 * n, BytesPerSecond: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	options.wrapSource = func(r io.Reader) io.Reader { return b.Source(t.Context(), r) }
	r, err := copyCompilerInputArchive(t.Context(), s.db, b, backend, options)
	if err != nil || r.State != "verified" {
		t.Fatal(r, err)
	}
	after, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil || m != after {
		t.Fatal("source manifest changed", err)
	}
	if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest).Scan(&count); err != nil || count != m.Count {
		t.Fatal("source rows changed", count, err)
	}
	if _, err = copyCompilerInputArchive(t.Context(), s.db, b, backend, options); !errors.Is(err, ErrConflict) {
		t.Fatal("one-shot copy replay accepted", err)
	}
	if b.Usage().SourceBytes != m.Size || b.Usage().GetRequests != 2*n {
		t.Fatal(b.Usage())
	}
}

func TestCompilerInputArchiveCopyContractRejectsUnboundedOrWrongFormat(t *testing.T) {
	c := CompilerInputArchiveCopyContract{Version: 1, Digest: "sha256:" + strings.Repeat("a", 64), BackendHash: "sha256:" + strings.Repeat("b", 64), SourceFormat: inputFormatV1, RawBytes: 65537, SegmentBytes: 65536, MaxPutBytes: 131074, MaxGetBytes: 262148, MaxPutRequests: 2, MaxGetRequests: 4, BytesPerSecond: 1 << 20, MaxSeconds: 600}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*CompilerInputArchiveCopyContract){func(c *CompilerInputArchiveCopyContract) { c.RawBytes = 0 }, func(c *CompilerInputArchiveCopyContract) { c.MaxSeconds = 601 }, func(c *CompilerInputArchiveCopyContract) { c.MaxPutRequests = 3 }, func(c *CompilerInputArchiveCopyContract) { c.MaxGetRequests = 5 }, func(c *CompilerInputArchiveCopyContract) { c.BytesPerSecond = 0 }, func(c *CompilerInputArchiveCopyContract) { c.SourceFormat = inputFormatSeparatedV4 }, func(c *CompilerInputArchiveCopyContract) { c.SegmentBytes = 16 << 20 }} {
		x := c
		mutate(&x)
		if x.Validate() == nil {
			t.Fatal("unbounded copy contract accepted", x)
		}
	}
}
