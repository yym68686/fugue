package store

import (
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/routeartifact"
	"fugue/internal/schemamigrate"
)

// Shadows the catalog with session-local relations. Exercises PostgreSQL's
// actual conflict, statement snapshot and timestamptz semantics without
// mutating or deleting any project, artifact or offer.
func TestHostedPreparationOfferPostgresCAS(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable local PostgreSQL required")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/fugue_test") || u.Query().Get("host") != "" && !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("local test database required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, q := range []string{
		`CREATE TEMP TABLE fugue_platform_artifacts(id text,content_hash text,artifact_kind text,scope_key text,status text,updated_at timestamptz) ON COMMIT DROP`,
		`SET LOCAL search_path TO pg_temp`, schemamigrate.HostedPrepareOffersSQL, schemamigrate.HostedProgramImagesSQL,
	} {
		if _, err = tx.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	d := func(c string) string { return "sha256:" + strings.Repeat(c, 64) }
	at := time.Now().UTC().Round(time.Microsecond)
	end := at.Add(time.Hour)
	add := func(id, hash, scope string, when time.Time) {
		t.Helper()
		if _, e := tx.Exec(`INSERT INTO fugue_platform_artifacts VALUES ($1,$2,'hosted_path_proposal',$3,'draft',$4)`, id, hash, scope, when); e != nil {
			t.Fatal(e)
		}
	}
	add("first", d("a"), "scope", at)
	add("foreign", d("f"), "other", at.Add(time.Second))
	call := func(id, hash, owner string, expires time.Time, want int64) {
		t.Helper()
		var n int64
		e := tx.QueryRowContext(t.Context(), hostedPrepareOfferCAS, "scope", id, hash, "project", owner, d("c"), d("d"), d("e"), expires).Scan(&n)
		if want == 0 {
			if !errors.Is(e, sql.ErrNoRows) {
				t.Fatalf("invalid offer not rejected: %v %d", e, n)
			}
			return
		}
		if e != nil || n != want {
			t.Fatalf("expected sequence %d, got %d: %v", want, n, e)
		}
	}
	call("first", d("a"), d("b"), end, 1)
	wireDigest := func(b []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(b)) }
	program := routeartifact.CompiledHostedProgram{Prepared: d("d"), Forward: d("e"), Digest: wireDigest([]byte("fixture")), Wire: []byte("fixture")}
	save := func(key string, sequence int64, id, hash string, p routeartifact.CompiledHostedProgram) error {
		return putHostedProgramImage(t.Context(), tx, key, sequence, id, hash, p)
	}
	if err := save("scope", 1, "first", d("a"), program); err != nil {
		t.Fatal(err)
	}
	tuple := func() string {
		var revision string
		if e := tx.QueryRow("SELECT ctid::text FROM fugue_hosted_program_images WHERE scope_key='scope'").Scan(&revision); e != nil {
			t.Fatal(e)
		}
		return revision
	}
	version := tuple()
	if err := save("scope", 1, "first", d("a"), program); err != nil || tuple() != version {
		t.Fatal("exact retry rewrote body", err)
	}
	tampered := program
	tampered.Wire = []byte("changed")
	tampered.Digest = wireDigest(tampered.Wire)
	if e := save("scope", 1, "first", d("a"), tampered); !errors.Is(e, ErrConflict) || tuple() != version {
		t.Fatal("overwrote existing bytes", e)
	}
	// A second host selects the same path digest independently.
	var otherEpoch int64
	if e := tx.QueryRow(hostedPrepareOfferCAS, "other", "foreign", d("f"), "project", d("2"), d("c"), d("d"), d("e"), end).Scan(&otherEpoch); e != nil {
		t.Fatal(e)
	}
	if e := save("other", otherEpoch, "foreign", d("f"), tampered); e != nil {
		t.Fatal(e)
	}
	readPin := func() (HostedSuccessorClaim, error) {
		c, _, e := readHostedClaim(tx.QueryRowContext(t.Context(), hostedClaimRowSQL+hostedClaimLatest+" FOR SHARE OF o", "scope"))
		return c, e
	}
	first, e := readPin()
	if e != nil || first.Sequence != 1 || first.Draft != "first" || first.Prepared != d("d") || first.Forward != d("e") {
		t.Fatal("cannot bind existing offer", first, e)
	}
	var offered time.Time
	if err := tx.QueryRow("SELECT offered_at FROM fugue_hosted_path_prepare_offers WHERE scope_key='scope'").Scan(&offered); err != nil {
		t.Fatal(err)
	}
	call("first", d("a"), d("b"), end, 1)
	call("first", d("a"), d("b"), end.Add(time.Hour), 0)
	call("first", d("a"), d("b"), at.Add(-time.Minute), 0)
	call("foreign", d("f"), d("b"), end, 0)
	var again time.Time
	if err := tx.QueryRow("SELECT offered_at FROM fugue_hosted_path_prepare_offers WHERE scope_key='scope'").Scan(&again); err != nil || !again.Equal(offered) {
		t.Fatal("retry renewed an observation", err)
	}
	add("second", d("1"), "scope", at.Add(time.Second))
	if _, e := readPin(); !errors.Is(e, sql.ErrNoRows) {
		t.Fatal("superseded draft retained a claim", e)
	}
	call("first", d("a"), d("b"), end, 0)
	call("second", d("1"), d("2"), end, 0)
	call("second", d("a"), d("b"), end, 0)
	call("second", d("1"), d("b"), end, 2)
	if e := tx.QueryRow(readHostedProgramSQL, "scope", 1, "first", d("a"), program.Prepared, program.Forward).Scan(new(string), new([]byte)); !errors.Is(e, sql.ErrNoRows) {
		t.Fatal("stale image visible after old writer advanced offer", e)
	}
	if e := save("scope", 2, "second", d("1"), tampered); e != nil {
		t.Fatal(e)
	}
	if e := save("scope", 1, "first", d("a"), program); e == nil {
		t.Fatal("stale writer replaced newer image")
	}
	if e := save("other", otherEpoch, "foreign", d("f"), tampered); e != nil {
		t.Fatal("host B image no longer matches", e)
	}
	second, e := readPin()
	if e != nil || second.Sequence != 2 || second.Draft != "second" || second == first {
		t.Fatal("new offer did not fence previous sequence", second, e)
	}
}
