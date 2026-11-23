package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

// Uses distinct, committed PostgreSQL connections, not a simulated cursor.
// Leaving A's transaction open must not prevent selecting B on another one.
func TestHostedChoicesFenceOnlyOneHostAndCanWithdrawPartialForward(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires disposable local PostgreSQL")
	}
	u, e := url.Parse(dsn)
	if e != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/fugue_test") {
		t.Fatal("disposable local PostgreSQL required")
	}
	base, e := sql.Open("pgx", dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer base.Close()
	schema := fmt.Sprintf("scoped_choice_%d", time.Now().UnixNano())
	if _, e = base.Exec("CREATE SCHEMA " + schema); e != nil {
		t.Fatal(e)
	}
	defer base.Exec("DROP SCHEMA " + schema + " CASCADE")
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	db, e := sql.Open("pgx", u.String())
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	for _, ddl := range []string{`CREATE TABLE fugue_platform_artifacts (id text,content_hash text,artifact_kind text,scope_key text,status text,updated_at timestamptz)`, schemamigrate.HostedPrepareOffersSQL, schemamigrate.HostedProgramImagesSQL, schemamigrate.HostedSelectionsSQL, schemamigrate.HostedSelectionReceiptsSQL, schemamigrate.HostedMemberDirectorySQL} {
		if _, e = db.Exec(ddl); e != nil {
			t.Fatal(e)
		}
	}
	d := func(v string) string { return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(v))) }
	now := time.Now().UTC().Truncate(time.Microsecond)
	txcall := func(f func(*sql.Tx) error) error {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		tx, e := db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		defer tx.Rollback()
		if e = f(tx); e != nil {
			return e
		}
		return tx.Commit()
	}
	init := func(host string) hostedSelectionCommand {
		c := HostedSuccessorClaim{Scope: "host:" + host, Host: host, Project: "project", Owner: d(host + "owner"), Paths: d(host + "paths"), Draft: "draft-" + host, DraftDigest: d(host + "draft"), Prepared: d(host + "stage"), Forward: d(host + "forward"), Business: d(host + "business"), Phase: "prepare", Sequence: 1}
		cmd := hostedSelectionCommand{Claim: c, Program: d(host + "-program"), Authority: d("authority"), BaselinePaths: d("baseline-" + host), ValidUntil: now.Add(time.Hour), Parts: map[string]string{d("edge-member"): d("edge-content"), d("dns-zone-one"): d("zone-one"), d("dns-zone-two"): d("zone-two")}}
		if e := txcall(func(tx *sql.Tx) error {
			if _, e := tx.Exec("INSERT INTO fugue_platform_artifacts VALUES($1,$2,'hosted_path_proposal',$3,'draft',clock_timestamp())", c.Draft, c.DraftDigest, c.Scope); e != nil {
				return e
			}
			var sequence int64
			if e := tx.QueryRow(hostedPrepareOfferCAS, c.Scope, c.Draft, c.DraftDigest, c.Project, c.Owner, c.Paths, c.Prepared, c.Forward, cmd.ValidUntil).Scan(&sequence); e != nil {
				return e
			}
			_, e := tx.Exec(putHostedProgramSQL, c.Scope, sequence, c.Draft, c.DraftDigest, cmd.Program, []byte(host+"-program"), c.Prepared, c.Forward)
			return e
		}); e != nil {
			t.Fatal(e)
		}
		return cmd
	}
	selectPhase := func(c hostedSelectionCommand) (h hostedSelectionHead, err error) {
		err = txcall(func(tx *sql.Tx) error { var e error; h, e = pgSelectHostedPhase(t.Context(), tx, c); return e })
		return
	}
	mustSelect := func(c hostedSelectionCommand) hostedSelectionHead {
		t.Helper()
		h, e := selectPhase(c)
		if e != nil || h.Fence != c.Fence+1 || h.Phase != c.Claim.Phase {
			t.Fatalf("select %s: %v %+v", c.Claim.Phase, e, h)
		}
		return h
	}
	reject := func(c hostedSelectionCommand) {
		t.Helper()
		if _, e := selectPhase(c); !errors.Is(e, ErrConflict) {
			t.Fatal("invalid transition not rejected", c.Claim.Phase, e)
		}
	}
	receipt := func(c hostedSelectionCommand, h hostedSelectionHead, part string, pass bool) hostedPartReceipt {
		t.Helper()
		at := time.Now().UTC().Truncate(time.Microsecond)
		return hostedPartReceipt{Host: c.Claim.Host, Fence: h.Fence, Decision: h.Decision, Part: part, Content: c.Parts[part], Evidence: d(part + h.Decision + at.String() + fmt.Sprint(pass)), Passing: pass, ObservedAt: at, ValidUntil: c.ValidUntil.Add(-time.Second)}
	}
	record := func(r hostedPartReceipt) error {
		return txcall(func(tx *sql.Tx) error { return pgRecordHostedPart(t.Context(), tx, r) })
	}
	mustRecord := func(r hostedPartReceipt) {
		t.Helper()
		if e := record(r); e != nil {
			t.Fatal(e)
		}
	}
	next := func(c hostedSelectionCommand, h hostedSelectionHead, name string) hostedSelectionCommand {
		c.Fence, c.Previous, c.Claim.Phase = h.Fence, h.Decision, name
		return c
	}
	a, b := init("a.example.test"), init("b.example.test")
	status := func(c hostedSelectionCommand, h hostedSelectionHead, count int) {
		t.Helper()
		err := txcall(func(tx *sql.Tx) error {
			s, e := pgHostedSelectionStatus(t.Context(), tx, c.Claim)
			if e != nil {
				return e
			}
			if s.hostedSelectionHead != h || s.Passing != count || s.Required != len(c.Parts) || s.EvidenceUntil.After(time.Now()) != (count == len(c.Parts)) {
				return fmt.Errorf("unexpected scoped status: %+v", s)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	held, e := db.BeginTx(t.Context(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer held.Rollback()
	ah, e := pgSelectHostedPhase(t.Context(), held, a)
	if e != nil {
		t.Fatal(e)
	}
	bh := mustSelect(b)
	if e = held.Commit(); e != nil {
		t.Fatal(e)
	}
	if same := mustSelect(a); same != ah {
		t.Fatal("retry moved selection")
	}
	af, bf := next(a, ah, "forward"), next(b, bh, "forward")
	af.Parts, bf.Parts = maps.Clone(a.Parts), maps.Clone(b.Parts)
	af.Parts[d("edge-member")], bf.Parts[d("edge-member")] = d("four path input"), d("independent peer release")
	reject(af)
	for key := range b.Parts {
		mustRecord(receipt(b, bh, key, true))
	}
	bh = mustSelect(bf)
	for key := range bf.Parts {
		mustRecord(receipt(bf, bh, key, true))
	}
	status(bf, bh, 3)
	cross := receipt(a, ah, d("dns-zone-one"), true)
	cross.Host = b.Claim.Host
	if e := record(cross); !errors.Is(e, ErrConflict) {
		t.Fatal("foreign host proof counted", e)
	}
	edge := receipt(a, ah, d("edge-member"), true)
	mustRecord(edge)
	mustRecord(edge)
	renewed := edge
	renewed.ObservedAt = time.Now().UTC()
	if e := record(renewed); !errors.Is(e, ErrConflict) {
		t.Fatal("renewed identical evidence", e)
	}
	mustRecord(receipt(a, ah, d("dns-zone-one"), true))
	status(a, ah, 2)
	reject(af)
	mustRecord(receipt(a, ah, d("dns-zone-two"), true))
	negative := receipt(a, ah, d("dns-zone-one"), false)
	mustRecord(negative)
	status(a, ah, 2)
	reject(af)
	mustRecord(receipt(a, ah, d("dns-zone-one"), true))
	status(a, ah, 3)
	dropZone := af
	dropZone.Parts = maps.Clone(af.Parts)
	delete(dropZone.Parts, d("dns-zone-two"))
	reject(dropZone)
	ah = mustSelect(af)
	if e := record(edge); !errors.Is(e, ErrConflict) {
		t.Fatal("old global/host receipt credited to new choice", e)
	}
	mustRecord(receipt(af, ah, d("edge-member"), true)) // DNS has not applied forward.
	status(af, ah, 1)
	// A mixed-version newer writer supersedes the staged image. Withdrawing
	// the chosen older image must neither select the new draft nor write B.
	if _, e := db.Exec("UPDATE fugue_hosted_path_prepare_offers SET sequence=sequence+1 WHERE scope_key=$1", a.Claim.Scope); e != nil {
		t.Fatal(e)
	}
	if _, e := db.Exec("UPDATE fugue_hosted_program_images SET sequence=sequence+1,program_digest=$2,body=$3 WHERE scope_key=$1", a.Claim.Scope, d("new image"), []byte("new image")); e != nil {
		t.Fatal(e)
	}
	aw := next(a, ah, "withdraw")
	aw.Claim.Business = d("restored project intent")
	wrongImage := aw
	wrongImage.Parts = af.Parts
	reject(wrongImage)
	awh := mustSelect(aw)
	status(aw, awh, 0)
	for key := range aw.Parts {
		mustRecord(receipt(aw, awh, key, true))
	}
	status(aw, awh, 3)
	status(bf, bh, 3)
	if awh.Fence != 3 || awh.Decision == ah.Decision {
		t.Fatal("withdraw lacked own fence")
	}
	var skipped, prior, priorEdge, nowEdge string
	if e := db.QueryRow("SELECT skipped_forward_from,previous_decision,previous_members->>$2,members->>$2 FROM fugue_hosted_selections WHERE hostname=$1", a.Claim.Host, d("edge-member")).Scan(&skipped, &prior, &priorEdge, &nowEdge); e != nil || skipped != af.Previous || prior != ah.Decision || priorEdge != af.Parts[d("edge-member")] || nowEdge != a.Parts[d("edge-member")] {
		t.Fatal("did not retain both authorized withdrawal sources", e)
	}
	old := next(a, ah, "withdraw")
	old.Claim.Business = d("stale intent")
	reject(old)
	var program, phase string
	var fence int64
	if e := db.QueryRow("SELECT convert_from(program,'UTF8'),phase,fence FROM fugue_hosted_selections WHERE hostname=$1", a.Claim.Host).Scan(&program, &phase, &fence); e != nil || program != "a.example.test-program" || phase != "withdraw" || fence != 3 {
		t.Fatal("did not retain selected image", e, phase, fence)
	}
	if e := db.QueryRow("SELECT convert_from(program,'UTF8'),phase,fence FROM fugue_hosted_selections WHERE hostname=$1", b.Claim.Host).Scan(&program, &phase, &fence); e != nil || program != "b.example.test-program" || phase != "forward" || fence != bh.Fence {
		t.Fatal("B rolled back with A", e, phase, fence)
	}
	a1 := model.PlatformConsumerAssignment{ArtifactKind: "route", ExpectedConsumerSetID: "set", ArtifactID: "child"}
	directory, _ := json.Marshal([]hostedMemberDirectoryEntry{{"member", a1.ArtifactKind, a1.ExpectedConsumerSetID, a1.ArtifactID}})
	list := func(host string) {
		t.Helper()
		if _, e := db.Exec(hostedDirectoryInsertSQL, host, string(directory)); e != nil {
			t.Fatal(e)
		}
	}
	list(a.Claim.Host)
	list(b.Claim.Host)
	for i := 0; i < 31; i++ {
		c := init(fmt.Sprintf("p%02d.example.test", i))
		mustSelect(c)
		list(c.Claim.Host)
	}
	first, e := pgHostedHintPage(t.Context(), db, "member", a1, "")
	if e != nil || len(first.Selections) != 32 || first.Next != "p29.example.test" || first.Selections[0].Decision != awh.Decision || first.Selections[1].Decision != bh.Decision {
		t.Fatal("wrong recipient page or stale hint", e, first.Next)
	}
	tail, e := pgHostedHintPage(t.Context(), db, "member", a1, first.Next)
	if e != nil || tail.Next != "" || len(tail.Selections) != 1 || tail.Selections[0].Host != "p30.example.test" {
		t.Fatal("keyset lost terminal host", e)
	}
	for _, key := range []struct{ member, kind, set, child string }{{"other", "route", "set", "child"}, {"member", "tls", "set", "child"}, {"member", "route", "new-set", "child"}, {"member", "route", "set", "new-child"}} {
		q, e := pgHostedHintPage(t.Context(), db, key.member, model.PlatformConsumerAssignment{ArtifactKind: key.kind, ExpectedConsumerSetID: key.set, ArtifactID: key.child}, "")
		if e != nil || len(q.Selections) != 0 {
			t.Fatal("foreign membership disclosed", e)
		}
	}
	t.Run("real producer admission with legacy and enrolled members", func(t *testing.T) { qualifyHostedProducerAdmission(t, db) })
}
