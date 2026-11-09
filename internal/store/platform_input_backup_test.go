package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
)

func inputBackupFixture(t *testing.T) ([]inputBackupRecord, CompilerInputBackupContract, []byte) {
	return inputBackupFixtureWithEvidence(t, []byte(`"neutral evidence <中文>"`))
}

func inputBackupFixtureWithEvidence(t *testing.T, evidence []byte) ([]inputBackupRecord, CompilerInputBackupContract, []byte) {
	t.Helper()
	frame := []byte("[,]")
	full := append([]byte{'['}, evidence...)
	full = append(full, ',')
	full = append(full, evidence...)
	full = append(full, ']')
	frameSHA, evidenceSHA := sharedHash(sha256.Sum256(frame)), sharedHash(sha256.Sum256(evidence))
	layout, err := encodeSeparatedLayout([]separatedPart{{Size: 1}, {Evidence: true, Digest: evidenceSHA, Size: uint32(len(evidence))}, {Offset: 1, Size: 1}, {Evidence: true, Digest: evidenceSHA, Size: uint32(len(evidence))}, {Offset: 2, Size: 1}})
	if err != nil {
		t.Fatal(err)
	}
	rawPack := append(append([]byte(nil), frame...), evidence...)
	codec := inputChunkEncoder{}
	pack, err := codec.encode(rawPack)
	if err != nil {
		t.Fatal(err)
	}
	witness := inputBackupPackWitness{ID: 7, Hash: sharedHash(sha256.Sum256(rawPack)), StoredHash: sharedHash(sha256.Sum256(pack)), Size: len(rawPack), StoredSize: len(pack)}
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	component := func(id int64, raw []byte, offset int) *inputBackupComponent {
		recipe, err := encodeSharedRecipe([]sharedRecipeRef{{ID: id, Size: len(raw)}}, len(raw))
		if err != nil {
			t.Fatal(err)
		}
		return &inputBackupComponent{Digest: sharedHash(sha256.Sum256(raw)), Size: int64(len(raw)), CreatedAt: created, Recipes: [][]byte{recipe}, Objects: []inputBackupObject{{ID: id, Hash: sharedHash(sha256.Sum256(raw)), PackID: 7, Size: len(raw), Offset: offset}}, Packs: []inputBackupPackWitness{witness}}
	}
	c := CompilerInputBackupContract{Schema: CompilerInputBackupSchema, Digest: sharedHash(sha256.Sum256(full)), ExpectedLogicalBytes: int64(len(full)), MaxEncodedBytes: inputBackupStreamLimit}
	return []inputBackupRecord{{Kind: "header", Header: &inputBackupHeader{Schema: c.Schema, Digest: c.Digest, LogicalBytes: c.ExpectedLogicalBytes, CreatedAt: created, FrameDigest: frameSHA, LayoutHash: sharedHash(sha256.Sum256(layout)), Layout: layout}}, {Kind: "component", Component: component(1, frame, 0)}, {Kind: "component", Component: component(2, evidence, len(frame))}, {Kind: "pack", Pack: &inputBackupPack{ID: 7, Data: pack}}}, c, full
}

func TestCompilerInputBackupLargeLogicalInputStaysWithinEncodedBound(t *testing.T) {
	evidence := []byte(`"` + strings.Repeat("neutral fact ", 2400) + `"`)
	records, c, _ := inputBackupFixtureWithEvidence(t, evidence)
	parts := []separatedPart{{Size: 1}}
	h := sha256.New()
	_, _ = h.Write([]byte{'['})
	const occurrences = 4000
	for i := 0; i < occurrences; i++ {
		if i != 0 {
			parts = append(parts, separatedPart{Offset: 1, Size: 1})
			_, _ = h.Write([]byte{','})
		}
		parts = append(parts, separatedPart{Evidence: true, Digest: records[2].Component.Digest, Size: uint32(len(evidence))})
		_, _ = h.Write(evidence)
	}
	parts = append(parts, separatedPart{Offset: 2, Size: 1})
	_, _ = h.Write([]byte{']'})
	layout, err := encodeSeparatedLayout(parts)
	if err != nil {
		t.Fatal(err)
	}
	c.Digest, c.ExpectedLogicalBytes = sharedHashBytes(h.Sum(nil)), int64(occurrences*len(evidence)+occurrences+1)
	records[0].Header.Digest, records[0].Header.LogicalBytes = c.Digest, c.ExpectedLogicalBytes
	records[0].Header.Layout, records[0].Header.LayoutHash = layout, sharedHash(sha256.Sum256(layout))
	wire := inputBackupWire(t, records)
	if len(wire) >= 1<<20 || c.ExpectedLogicalBytes < 100<<20 {
		t.Fatal("fixture did not exercise large compressed reconstruction")
	}
	r, err := DecodeCompilerInputBackup(t.Context(), bytes.NewReader(wire), c, io.Discard)
	if err != nil || !r.Complete || !r.FullOriginalSHA256Verified || r.LogicalBytes != c.ExpectedLogicalBytes {
		t.Fatal("large bounded reconstruction failed", r, err)
	}
}

func inputBackupWire(t *testing.T, records []inputBackupRecord) []byte {
	t.Helper()
	var out bytes.Buffer
	e := inputBackupEncoder{ctx: t.Context(), out: &out, max: inputBackupStreamLimit, hash: sha256.New()}
	for _, r := range records {
		if err := e.record(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.finish(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestCompilerInputBackupExactReconstructionAndCorruption(t *testing.T) {
	records, c, full := inputBackupFixture(t)
	wire := inputBackupWire(t, records)
	var out bytes.Buffer
	r, err := DecodeCompilerInputBackup(t.Context(), bytes.NewReader(wire), c, &out)
	if err != nil || !r.Complete || !r.FullOriginalSHA256Verified || !r.ReadOnly || !bytes.Equal(full, out.Bytes()) || r.Packs != 1 || r.Components != 2 || r.EncodedBytes != int64(len(wire)) {
		t.Fatal("exact repeated evidence or shared pack changed", r, err)
	}
	for _, name := range []string{"layout hash", "object hash", "component hash", "decoded pack hash", "stored pack hash", "missing component", "missing pack", "duplicate component", "duplicate pack", "unexpected pack", "late component", "wrong root SHA", "wrong logical size"} {
		t.Run(name, func(t *testing.T) {
			records, c, _ := inputBackupFixture(t)
			bad := "sha256:" + strings.Repeat("a", 64)
			switch name {
			case "layout hash":
				records[0].Header.Layout[5] ^= 1
			case "object hash":
				records[2].Component.Objects[0].Hash = bad
			case "component hash":
				records[1].Component.Digest, records[0].Header.FrameDigest = bad, bad
			case "decoded pack hash":
				for _, r := range records[1:3] {
					r.Component.Packs[0].Hash = bad
				}
			case "stored pack hash":
				records[3].Pack.Data[6] ^= 1
			case "missing component":
				records = append(records[:2], records[3:]...)
			case "missing pack":
				records = records[:3]
			case "duplicate component":
				records = append(records[:3], records[2], records[3])
			case "duplicate pack":
				records = append(records, records[3])
			case "unexpected pack":
				records[3].Pack.ID++
			case "late component":
				records[2], records[3] = records[3], records[2]
			case "wrong root SHA":
				c.Digest, records[0].Header.Digest = bad, bad
			case "wrong logical size":
				c.ExpectedLogicalBytes++
				records[0].Header.LogicalBytes++
			}
			r, err := DecodeCompilerInputBackup(t.Context(), bytes.NewReader(inputBackupWire(t, records)), c, io.Discard)
			if err == nil || r.Complete || r.FullOriginalSHA256Verified {
				t.Fatal("corruption accepted", r)
			}
		})
	}
	for _, bad := range [][]byte{wire[:len(wire)-1], wire[:len(wire)/2], append(append([]byte(nil), wire...), '\n'), bytes.Replace(wire, []byte(`"kind":"header"`), []byte(`"kind":"header","kind":"header"`), 1), bytes.Replace(wire, []byte(`"kind":"header"`), []byte(`"unknown":0,"kind":"header"`), 1)} {
		if r, err := DecodeCompilerInputBackup(t.Context(), bytes.NewReader(bad), c, io.Discard); err == nil || r.Complete {
			t.Fatal("framing corruption accepted")
		}
	}
	c.MaxEncodedBytes = int64(len(wire) - 1)
	if _, err := DecodeCompilerInputBackup(t.Context(), bytes.NewReader(wire), c, io.Discard); err == nil {
		t.Fatal("stream budget exceeded")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := DecodeCompilerInputBackup(ctx, bytes.NewReader(wire), c, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
}

type backupShortWriter struct{}

func (backupShortWriter) Write(p []byte) (int, error) { return len(p) / 2, nil }

type backupBlockedWriter struct{ entered, release, exited chan struct{} }

func (w backupBlockedWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	close(w.exited)
	return len(p), nil
}

func TestCompilerInputBackupCancellationDoesNotWaitForBlockedReceiver(t *testing.T) {
	records, _, _ := inputBackupFixture(t)
	w := backupBlockedWriter{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	e := inputBackupEncoder{ctx: ctx, out: w, max: inputBackupStreamLimit, hash: sha256.New()}
	go func() { done <- e.record(records[0]) }()
	<-w.entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Error("blocked receiver cancellation lost", err)
		}
	case <-time.After(time.Second):
		t.Error("blocked receiver retained source execution")
	}
	close(w.release)
	<-w.exited
}

func TestCompilerInputBackupShortWriteAndInvalidContract(t *testing.T) {
	records, c, _ := inputBackupFixture(t)
	wire := inputBackupWire(t, records)
	if r, err := DecodeCompilerInputBackup(t.Context(), bytes.NewReader(wire), c, backupShortWriter{}); !errors.Is(err, io.ErrShortWrite) || r.Complete {
		t.Fatal("partial output accepted", r, err)
	}
	for _, mutate := range []func(*CompilerInputBackupContract){func(c *CompilerInputBackupContract) { c.Schema = "unknown" }, func(c *CompilerInputBackupContract) { c.Digest = "bad" }, func(c *CompilerInputBackupContract) { c.ExpectedLogicalBytes = 128<<20 + 1 }, func(c *CompilerInputBackupContract) { c.MaxEncodedBytes = inputBackupStreamLimit + 1 }} {
		bad := c
		mutate(&bad)
		if _, err := runCompilerInputBackup(t.Context(), nil, bad, io.Discard); !errors.Is(err, ErrInvalidInput) {
			t.Fatal("invalid contract reached database", err)
		}
	}
}

func TestCompilerInputBackupReadOnlyPostgres(t *testing.T) {
	s := separatedTestStore(t)
	if _, err := s.db.Exec(schemamigrate.CompilerInputZeroTempSQL); err != nil {
		t.Fatal(err)
	}
	snapshot := separatedFixture(t)
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	raw, err := platformconfig.EncodeRuntimeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	c := CompilerInputBackupContract{Schema: CompilerInputBackupSchema, Digest: digest, ExpectedLogicalBytes: int64(len(raw)), MaxEncodedBytes: inputBackupStreamLimit}
	roleName := model.NewID("input_backup_reader")
	role := quotePostgresIntegrationIdentifier(t, roleName)
	if _, err = s.db.Exec(`CREATE ROLE ` + role + ` LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE; GRANT USAGE ON SCHEMA public TO ` + role + `; GRANT SELECT ON ALL TABLES IN SCHEMA public TO ` + role + `; REVOKE SELECT ON fugue_platform_input_chunks FROM ` + role); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := s.db.Exec(`DROP OWNED BY ` + role + `; DROP ROLE ` + role); err != nil {
			t.Error(err)
		}
	})
	u, _ := url.Parse(s.databaseURL)
	u.User = url.User(roleName)
	var encoded, rebuilt bytes.Buffer
	r, err := RunCompilerInputBackup(t.Context(), u.String(), c, &encoded)
	if err != nil || !r.Complete || r.FullOriginalSHA256Verified || r.Snapshot == "" || r.ObservedAt.IsZero() {
		t.Fatal("read-only exact snapshot export failed", r, err)
	}
	d, err := DecodeCompilerInputBackup(t.Context(), bytes.NewReader(encoded.Bytes()), c, &rebuilt)
	if err != nil || !d.FullOriginalSHA256Verified || !bytes.Equal(raw, rebuilt.Bytes()) {
		t.Fatal("database encoded representation changed original bytes", d, err)
	}
	// Metadata and original timestamps are unchanged; this does not create a pin.
	m, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil {
		t.Fatal(err)
	}
	var first inputBackupRecord
	if err = json.Unmarshal(bytes.SplitN(encoded.Bytes(), []byte{'\n'}, 2)[0], &first); err != nil || !first.Header.CreatedAt.Equal(m.Created) {
		t.Fatal("original creation time changed", err)
	}
	for _, kind := range []string{"budget", "short output", "canceled"} {
		ctx := t.Context()
		contract := c
		var out io.Writer = io.Discard
		if kind == "budget" {
			contract.MaxEncodedBytes = 100
		} else if kind == "short output" {
			out = backupShortWriter{}
		} else {
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			cancel()
		}
		r, err := RunCompilerInputBackup(ctx, u.String(), contract, out)
		if err == nil || r.Complete {
			t.Fatal("failed export completed", kind, r)
		}
	}
	var acquired bool
	conn, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err = conn.QueryRowContext(t.Context(), `SELECT pg_try_advisory_lock($1)`, compilerInputArchiveReadLeaseLockID).Scan(&acquired); err != nil || !acquired {
		t.Fatal("export leaked reader barrier", err)
	}
	if _, err = conn.ExecContext(t.Context(), `SELECT pg_advisory_unlock($1)`, compilerInputArchiveReadLeaseLockID); err != nil {
		t.Fatal(err)
	}
}
