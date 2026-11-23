package store

import (
	"database/sql"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

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
		`SET LOCAL search_path TO pg_temp`, schemamigrate.HostedPrepareOffersSQL,
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
	readPin := func() (HostedSuccessorClaim, error) {
		c, _, e := readHostedClaim(tx.QueryRowContext(t.Context(), hostedClaimRowSQL+hostedClaimLatest+" FOR SHARE OF o", "scope"))
		return c, e
	}
	first, e := readPin()
	if e != nil || first.Sequence != 1 || first.Draft != "first" || first.Prepared != d("d") || first.Forward != d("e") {
		t.Fatal("cannot bind existing offer", first, e)
	}
	var offered time.Time
	if err := tx.QueryRow("SELECT offered_at FROM fugue_hosted_path_prepare_offers").Scan(&offered); err != nil {
		t.Fatal(err)
	}
	call("first", d("a"), d("b"), end, 1)
	call("first", d("a"), d("b"), end.Add(time.Hour), 0)
	call("first", d("a"), d("b"), at.Add(-time.Minute), 0)
	call("foreign", d("f"), d("b"), end, 0)
	var again time.Time
	if err := tx.QueryRow("SELECT offered_at FROM fugue_hosted_path_prepare_offers").Scan(&again); err != nil || !again.Equal(offered) {
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
	second, e := readPin()
	if e != nil || second.Sequence != 2 || second.Draft != "second" || second == first {
		t.Fatal("new offer did not fence previous sequence", second, e)
	}
}
