package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func TestPlatformReleaseBackupContractAndCorruption(t *testing.T) {
	c := PlatformReleaseBackupContract{Schema: PlatformReleaseBackupSchema, ReleaseIDs: []string{"release-neutral"}, MaxEncodedBytes: 32 << 20}
	for _, mutate := range []func(*PlatformReleaseBackupContract){
		func(c *PlatformReleaseBackupContract) { c.Schema = "unknown" },
		func(c *PlatformReleaseBackupContract) { c.ReleaseIDs = nil },
		func(c *PlatformReleaseBackupContract) { c.ReleaseIDs = []string{"same", "same"} },
		func(c *PlatformReleaseBackupContract) { c.ReleaseIDs = []string{" x"} },
		func(c *PlatformReleaseBackupContract) { c.ReleaseIDs = []string{strings.Repeat("x", 257)} },
		func(c *PlatformReleaseBackupContract) { c.ReleaseIDs = make([]string, 33) },
		func(c *PlatformReleaseBackupContract) { c.MaxEncodedBytes++ },
	} {
		bad := c
		mutate(&bad)
		if _, err := runPlatformReleaseBackup(t.Context(), nil, bad, io.Discard); !errors.Is(err, ErrInvalidInput) {
			t.Fatal("invalid selection reached DB", err)
		}
	}
	fields := map[string]any{}
	for _, key := range strings.Split(strings.ReplaceAll(releaseBackupColumns, "\n", ""), ",") {
		fields[key] = nil
	}
	fields["id"] = c.ReleaseIDs[0]
	fields["fencing_token"] = json.Number("9007199254740993")
	fields["scope_json"] = map[string]any{"future_field": "<中文>"}
	row, _ := json.Marshal(fields)
	rows, _ := json.Marshal([]json.RawMessage{row})
	b := PlatformReleaseBackup{Schema: c.Schema, ObservedAt: time.Now().UTC(), Snapshot: "12:12:", Rows: []json.RawMessage{row}, RowsSHA256: sharedHash(sha256.Sum256(rows))}
	wire, _ := json.Marshal(b)
	wire = append(wire, '\n')
	got, err := DecodePlatformReleaseBackup(bytes.NewReader(wire), c)
	if err != nil || !bytes.Equal(got.Rows[0], row) {
		t.Fatal("exact raw columns lost", err)
	}
	for _, bad := range [][]byte{
		wire[:len(wire)-1], wire[:len(wire)/2], append(append([]byte{}, wire...), '\n'),
		bytes.Replace(wire, []byte(`"rows_sha256":`), []byte(`"unknown":0,"rows_sha256":`), 1),
		bytes.Replace(wire, []byte(`"schema":`), []byte(`"schema":"duplicate","schema":`), 1),
		bytes.Replace(wire, []byte(`9007199254740993`), []byte(`9007199254740994`), 1),
	} {
		if _, err = DecodePlatformReleaseBackup(bytes.NewReader(bad), c); err == nil {
			t.Fatal("corrupt or truncated backup accepted")
		}
	}
	badSelection := c
	badSelection.ReleaseIDs = []string{"other"}
	if _, err = DecodePlatformReleaseBackup(bytes.NewReader(wire), badSelection); err == nil {
		t.Fatal("different selection accepted")
	}
	c.MaxEncodedBytes = int64(len(wire) - 1)
	if _, err = DecodePlatformReleaseBackup(bytes.NewReader(wire), c); err == nil {
		t.Fatal("byte limit exceeded")
	}
}

func TestPlatformReleaseBackupReadOnlyPostgres(t *testing.T) {
	s := boundedContentTestStore(t)
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = s.applyPostgresSchemaTx(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(schemamigrate.CompilerInputZeroTempSQL); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 1, 2, 3, 456789000, time.FixedZone("test", 3600))
	a, err := pgInsertPlatformArtifact(t.Context(), s.db, model.PlatformArtifact{ID: model.NewID("artifact"), ArtifactKind: "neutral", ScopeKey: "global", Generation: "generation-neutral", Content: map[string]any{}, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	verified := now.Add(time.Minute)
	release := model.PlatformArtifactRelease{ID: model.NewID("release"), ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, ScopeKey: a.ScopeKey, Scope: model.PlatformArtifactScope{Region: "region-neutral"}, Generation: a.Generation, ReleaseChannel: "shadow", Status: "superseded", LaneKey: "neutral|global|shadow", FencingToken: 9007199254740993, Version: 7, IdempotencyKey: model.NewID("operation"), CandidateGeneration: "candidate-neutral", ServingUnverifiedGeneration: "unverified-neutral", VerifiedLKGGeneration: "lkg-neutral", PinnedRollbackGeneration: "pinned-neutral", VerificationState: "verified", VerificationEvidence: map[string]string{"evidence": "<中文>"}, VerifiedAt: &verified, RollbackTargetGeneration: "rollback-neutral", CanaryRuleRef: "rule-neutral", OverrideMode: "neutral", OverrideExpiresAt: &verified, BypassedInvariants: []string{"invariant-neutral"}, Reason: "original reason", ReleasedByType: "system", ReleasedByID: "actor-neutral", ReleasedAt: now, CreatedAt: now.Add(-time.Minute), UpdatedAt: verified}
	want, err := pgInsertPlatformArtifactRelease(t.Context(), s.db, release)
	if err != nil {
		t.Fatal(err)
	}
	// Unknown nested scope fields must be retained by the raw-column backup.
	if _, err = s.db.Exec(`UPDATE fugue_platform_artifact_releases SET scope_json=scope_json || '{"future_field":"retain-me"}'::jsonb WHERE id=$1`, release.ID); err != nil {
		t.Fatal(err)
	}
	roleName := model.NewID("release_backup_reader")
	role := quotePostgresIntegrationIdentifier(t, roleName)
	if _, err = s.db.Exec(`CREATE ROLE ` + role + ` LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE; GRANT USAGE ON SCHEMA public TO ` + role + `; GRANT SELECT ON fugue_platform_artifact_releases TO ` + role); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := s.db.Exec(`DROP OWNED BY ` + role + `; DROP ROLE ` + role); err != nil {
			t.Error(err)
		}
	})
	u, _ := url.Parse(s.databaseURL)
	u.User = url.User(roleName)
	c := PlatformReleaseBackupContract{Schema: PlatformReleaseBackupSchema, ReleaseIDs: []string{release.ID}, MaxEncodedBytes: 32 << 20}
	var wire bytes.Buffer
	r, err := RunPlatformReleaseBackup(t.Context(), u.String(), c, &wire)
	if err != nil || !r.Complete || r.Records != 1 || r.Snapshot == "" || r.ObservedAt.IsZero() || r.SHA256 != sharedHash(sha256.Sum256(wire.Bytes())) {
		t.Fatal("ordinary SELECT export failed", r, err)
	}
	b, err := DecodePlatformReleaseBackup(bytes.NewReader(wire.Bytes()), c)
	if err != nil || !bytes.Contains(b.Rows[0], []byte(`"future_field":"retain-me"`)) {
		t.Fatal("raw scope not retained", err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(b.Rows[0], &fields); err != nil {
		t.Fatal(err)
	}
	// Convert column aliases solely for comparison to the ordinary model read.
	for _, pair := range [][2]string{{"scope_json", "scope"}, {"verification_evidence_json", "verification_evidence"}, {"bypassed_invariants_json", "bypassed_invariants"}} {
		fields[pair[1]] = fields[pair[0]]
		delete(fields, pair[0])
	}
	modelRaw, _ := json.Marshal(fields)
	var got model.PlatformArtifactRelease
	if err = json.Unmarshal(modelRaw, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) { // PG's timestamp zone representation may differ; normalize comparison copies.
		got.ReleasedAt, want.ReleasedAt = got.ReleasedAt.UTC(), want.ReleasedAt.UTC()
		got.CreatedAt, want.CreatedAt = got.CreatedAt.UTC(), want.CreatedAt.UTC()
		got.UpdatedAt, want.UpdatedAt = got.UpdatedAt.UTC(), want.UpdatedAt.UTC()
		g, w := got.VerifiedAt.UTC(), want.VerifiedAt.UTC()
		got.VerifiedAt, want.VerifiedAt = &g, &w
		g, w = got.OverrideExpiresAt.UTC(), want.OverrideExpiresAt.UTC()
		got.OverrideExpiresAt, want.OverrideExpiresAt = &g, &w
		if !reflect.DeepEqual(got, want) {
			t.Fatal("persisted release facts changed")
		}
	}
	for _, kind := range []string{"missing", "budget", "short", "canceled"} {
		ctx, contract, out := t.Context(), c, io.Writer(io.Discard)
		if kind == "missing" {
			contract.ReleaseIDs = []string{release.ID, "missing-neutral"}
		}
		if kind == "budget" {
			contract.MaxEncodedBytes = 100
		}
		if kind == "short" {
			out = backupShortWriter{}
		}
		if kind == "canceled" {
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			cancel()
		}
		if r, err = RunPlatformReleaseBackup(ctx, u.String(), contract, out); err == nil || r.Complete {
			t.Fatal("incomplete backup accepted", kind, r)
		}
	}
	blocked := backupBlockedWriter{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := RunPlatformReleaseBackup(ctx, u.String(), c, blocked); done <- err }()
	<-blocked.entered
	// The entire DB lease must already be gone while the receiver is blocked.
	conn, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var acquired bool
	if err = conn.QueryRowContext(t.Context(), `SELECT pg_try_advisory_lock($1)`, compilerInputArchiveReadLeaseLockID).Scan(&acquired); err != nil || !acquired {
		t.Fatal("blocked output retained DB reader barrier", err)
	}
	_, _ = conn.ExecContext(t.Context(), `SELECT pg_advisory_unlock($1)`, compilerInputArchiveReadLeaseLockID)
	conn.Close()
	cancel()
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("blocked output ignored cancellation")
	}
	close(blocked.release)
	<-blocked.exited
	if _, err = s.db.Exec(`UPDATE fugue_platform_artifact_releases SET reason=$2 WHERE id=$1`, release.ID, strings.Repeat("x", 1<<20)); err != nil {
		t.Fatal(err)
	}
	if r, err = RunPlatformReleaseBackup(t.Context(), u.String(), c, io.Discard); err == nil || r.Complete {
		t.Fatal("oversized source row accepted")
	}
	if _, err = s.db.Exec(`ALTER TABLE fugue_platform_artifact_releases ADD COLUMN backup_future_contract TEXT`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.db.Exec(`ALTER TABLE fugue_platform_artifact_releases DROP COLUMN backup_future_contract`)
	})
	if r, err = RunPlatformReleaseBackup(t.Context(), u.String(), c, io.Discard); !errors.Is(err, ErrConflict) || r.Complete {
		t.Fatal("unknown persisted column silently omitted", err)
	}
}
