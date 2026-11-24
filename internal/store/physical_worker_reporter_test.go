package store

import (
	"database/sql"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/schemamigrate"
)

func TestWorkerReporterPredecessorRejectsDelayedWriteAcrossRollback(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires disposable PostgreSQL")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/fugue_test") {
		t.Fatal("requires a disposable loopback database")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := t.Context()
	if _, err = db.ExecContext(ctx, schemamigrate.WorkerReporterHeadsSQL); err != nil {
		t.Fatal(err)
	}
	s := &Store{db: db}
	claims := platformcontrol.PlatformComponentIdentityClaims{NodeID: model.NewID("test-node"), Component: model.PlatformConsumerComponentEdgeWorker, ScopeKey: "global", CredentialID: "pod-a"}
	const hex = "0123456789abcdef"
	digest := func(n int) string { return "sha256:" + strings.Repeat(hex[n:n+1], 64) }
	read := func() WorkerReporterPredecessor {
		p, e := s.ReadWorkerReporterPredecessor(ctx, claims.NodeID)
		if e != nil {
			t.Fatal(e)
		}
		return p
	}
	write := func(p WorkerReporterPredecessor, d string, commit bool) error {
		tx, e := db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		defer tx.Rollback()
		if e = s.acceptPhysicalWorker(ctx, tx, claims, p.Bind(ctx, d, claims.CredentialID)); e != nil {
			return e
		}
		if commit {
			return tx.Commit()
		}
		return nil
	}
	original := read()
	unbound := func() error {
		tx, e := db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		defer tx.Rollback()
		return s.acceptPhysicalWorker(ctx, tx, claims)
	}
	if err = unbound(); err != nil {
		t.Fatal("unregistered node affected", err)
	}
	if err = write(original, digest(0), false); err != nil || read().revision != 0 {
		t.Fatal("aborted receipt advanced the node", err)
	}
	if err = write(original, digest(0), true); err != nil {
		t.Fatal(err)
	}
	first := read()
	if err = unbound(); !errors.Is(err, ErrConflict) {
		t.Fatal("missing pin reopened an enrolled node", err)
	}
	if err = write(first, digest(0), true); err != nil || read().revision != 1 {
		t.Fatal("stable binding rewrote revision", err)
	}
	claims.CredentialID = "pod-b"
	if err = write(first, digest(1), true); err != nil {
		t.Fatal(err)
	}
	second := read()
	claims.CredentialID = "pod-a"
	if err = write(first, digest(0), true); !errors.Is(err, ErrConflict) {
		t.Fatal("delayed first owner overwrote successor", err)
	}
	if err = write(second, digest(0), true); err != nil {
		t.Fatal("fresh node rollback", err)
	}
	if err = write(first, digest(0), true); !errors.Is(err, ErrConflict) {
		t.Fatal("stale pre-rollback owner accepted after A/B/A", err)
	}
	if p := read(); p.revision != 3 || p.binding != digest(0) {
		t.Fatal("wrong final reporter head", p.revision, p.binding)
	}
}
