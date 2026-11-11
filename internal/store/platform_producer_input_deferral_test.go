package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/schemamigrate"
)

func inputDeferralStoreFixture(t *testing.T) (*Store, platformproducer.InputDeferral) {
	t.Helper()
	s := New(t.TempDir() + "/state.json")
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "policy-alpha", Mode: "shadow", InputSource: "business-static-intent", TargetScope: "global", IntervalSeconds: 30, RefreshSeconds: 120, StaticIntentArtifactID: "static-alpha", StaticIntentDigest: "sha256:" + strings.Repeat("a", 64)}
	a := saveInputDeferralTestArtifact(t, s, platformproducer.Scope, p.Generation, p)
	_, r, _, _, err := s.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, captureDeferralTestPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Add(-time.Second)
	request := platformproducer.CaptureDeferralRequest{SchemaVersion: platformproducer.CaptureDeferralSchema, OperationID: "finite-alpha", RequestedAt: at, ExpiresAt: at.Add(20 * time.Minute), Authorities: []platformproducer.CaptureDeferralAuthority{{Scope: platformproducer.Scope, Policy: model.PlatformPublicationPrecondition{ArtifactID: a.ID, ContentHash: a.ContentHash, ReleaseID: r.ID, FencingToken: r.FencingToken}}}}
	digest, _ := request.Digest()
	return s, platformproducer.InputDeferral{SchemaVersion: platformproducer.InputDeferralSchema, Generation: request.OperationID, Action: "defer", Request: request, RequestDigest: digest}
}

// Exercise the entire ordinary PostgreSQL artifact/publication preparation and
// commit path as well as the focused lock fixture below. All schemas and WAL
// measurements belong to the private loopback cluster, never production.
func TestInputDeferralPrivatePostgresNormalPublicationCost(t *testing.T) {
	open, _ := producerCapturePrivatePostgres(t, os.Getenv("FUGUE_INPUT_DEFERRAL_POSTGRES_BIN"), 140000)
	s := open()
	for _, statement := range []string{postgresMetaSchemaStatement, postgresSchemaBatch(), schemamigrate.CompilerInputZeroTempSQL, schemamigrate.BoundedContentSQL, schemamigrate.CompilerInputArchiveSQL, schemamigrate.PlatformShadowConsumersSQL} {
		if _, err := s.db.ExecContext(t.Context(), statement); err != nil {
			t.Fatal("private full schema", err)
		}
	}
	p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "policy-normal", Mode: "shadow", InputSource: "business-static-intent", TargetScope: "global", IntervalSeconds: 30, RefreshSeconds: 120, StaticIntentArtifactID: "synthetic-static", StaticIntentDigest: "sha256:" + strings.Repeat("a", 64)}
	a := saveInputDeferralTestArtifact(t, s, platformproducer.Scope, p.Generation, p)
	principal := captureDeferralTestPrincipal()
	_, original, _, _, err := s.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, principal)
	if err != nil {
		t.Fatal("ordinary producer policy publication", err)
	}
	b := platformproducer.CaptureDeferralAuthority{Scope: a.ScopeKey, Policy: model.PlatformPublicationPrecondition{ArtifactID: a.ID, ContentHash: a.ContentHash, ReleaseID: original.ID, FencingToken: original.FencingToken}}
	at := time.Now().UTC().Add(-time.Second)
	request := platformproducer.CaptureDeferralRequest{SchemaVersion: platformproducer.CaptureDeferralSchema, OperationID: "normal-alpha", RequestedAt: at, ExpiresAt: at.Add(20 * time.Minute), Authorities: []platformproducer.CaptureDeferralAuthority{b}}
	digest, _ := request.Digest()
	control := platformproducer.InputDeferral{SchemaVersion: platformproducer.InputDeferralSchema, Generation: request.OperationID, Action: "defer", Request: request, RequestDigest: digest}
	lsn := func() string {
		t.Helper()
		var value string
		if err := s.db.QueryRowContext(t.Context(), `SELECT pg_current_wal_insert_lsn()::text`).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	delta := func(before string) int64 {
		t.Helper()
		var value int64
		if err := s.db.QueryRowContext(t.Context(), `SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::pg_lsn)::bigint`, before).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	publish := func() {
		t.Helper()
		a := saveInputDeferralTestArtifact(t, s, platformproducer.InputDeferralScope, control.Generation, control)
		if _, _, _, _, err := s.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: control.IdempotencyKey()}, principal); err != nil {
			t.Fatal("ordinary finite control publication", err)
		}
	}
	before, started := lsn(), time.Now()
	publish()
	publicationWAL, publicationTime := delta(before), time.Since(started)
	// Exercise admission under an ordinary read-only role. A privileged fixture
	// must not hide a direct superuser-only SET temp_file_limit in this path.
	if _, err := s.db.ExecContext(t.Context(), `CREATE ROLE input_deferral_reader NOLOGIN; GRANT USAGE ON SCHEMA public TO input_deferral_reader; GRANT SELECT ON ALL TABLES IN SCHEMA public TO input_deferral_reader`); err != nil {
		t.Fatal(err)
	}
	readerURL, err := url.Parse(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := readerURL.Query()
	query.Del("temp_file_limit")
	query.Set("options", "-c role=input_deferral_reader")
	readerURL.RawQuery = query.Encode()
	readerDB, err := sql.Open("pgx", readerURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer readerDB.Close()
	reader := &Store{db: readerDB, databaseURL: readerURL.String()}
	configureTestPlatformArtifactSigning(reader)
	var ordinary bool
	if err := readerDB.QueryRowContext(t.Context(), `SELECT current_user='input_deferral_reader' AND NOT rolsuper FROM pg_roles WHERE rolname=current_user`).Scan(&ordinary); err != nil || !ordinary {
		t.Fatal("ordinary admission identity", ordinary, err)
	}
	before, started = lsn(), time.Now()
	for range 32 {
		if deferred, err := reader.ProducerInputDeferred(t.Context(), b.Scope, b.Policy); err != nil || !deferred {
			t.Fatal("normal selected gate", deferred, err)
		}
	}
	pollWAL, pollTime := delta(before), time.Since(started)
	control.Action, control.Generation = "release", request.OperationID+":release"
	before, started = lsn(), time.Now()
	publish()
	releaseWAL, releaseTime := delta(before), time.Since(started)
	if deferred, err := s.ProducerInputDeferred(t.Context(), b.Scope, b.Policy); err != nil || deferred {
		t.Fatal("normal terminal release", deferred, err)
	}
	var optional *string
	if err := s.db.QueryRowContext(t.Context(), `SELECT to_regclass('fugue_producer_capture_work')::text`).Scan(&optional); err != nil || optional != nil {
		t.Fatal("normal publication installed ticket schema", err)
	}
	var releases int
	if err := s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM fugue_platform_artifact_releases WHERE scope_key=$1`, platformproducer.InputDeferralScope).Scan(&releases); err != nil || releases != 2 {
		t.Fatal("polling allocated releases", releases, err)
	}
	t.Logf("isolated PG sample only, one authority, no production upper bound: publication_wal_bytes=%d publication_ms=%d polls=32 poll_wal_bytes=%d poll_total_ms=%d release_wal_bytes=%d release_ms=%d", publicationWAL, publicationTime.Milliseconds(), pollWAL, pollTime.Milliseconds(), releaseWAL, releaseTime.Milliseconds())
}

// Uses only an isolated loopback cluster it creates itself. The release guard,
// selection barrier, signed reads and crash recovery are production functions;
// tiny row insertion below is fixture setup, never an alternate production API.
func TestInputDeferralPrivatePostgresAtomicSelectionAndCrash(t *testing.T) {
	open, crash := producerCapturePrivatePostgres(t, os.Getenv("FUGUE_INPUT_DEFERRAL_POSTGRES_BIN"), 140000)
	s := open()
	ctx := t.Context()
	if _, err := s.db.ExecContext(ctx, schemamigrate.CompilerInputZeroTempSQL); err != nil {
		t.Fatal(err)
	}
	for _, statement := range postgresSchemaStatements {
		for _, table := range []string{"fugue_platform_artifacts", "fugue_platform_artifact_releases", "fugue_platform_release_lanes"} {
			if strings.HasPrefix(statement, "CREATE TABLE IF NOT EXISTS "+table+" (") || strings.HasPrefix(statement, "ALTER TABLE "+table+" ") || strings.HasPrefix(statement, "CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_platform_releases_idempotency ") {
				if _, err := s.db.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	file, p := inputDeferralStoreFixture(t)
	bound, err := file.readState()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range bound.PlatformArtifacts {
		if _, err := pgInsertPlatformArtifact(ctx, s.db, a); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range bound.PlatformArtifactReleases {
		if _, err := pgInsertPlatformArtifactRelease(ctx, s.db, r); err != nil {
			t.Fatal(err)
		}
	}
	for _, l := range bound.PlatformReleaseLanes {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO fugue_platform_release_lanes(lane_key,artifact_kind,scope_key,release_channel,fencing_token,version,active_release_id,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, l.LaneKey, l.ArtifactKind, l.ScopeKey, l.ReleaseChannel, l.FencingToken, l.Version, l.ActiveReleaseID, l.UpdatedAt); err != nil {
			t.Fatal(err)
		}
	}
	a := saveInputDeferralTestArtifact(t, file, platformproducer.InputDeferralScope, p.Generation, p)
	if _, err := pgInsertPlatformArtifact(ctx, s.db, a); err != nil {
		t.Fatal(err)
	}
	principal := captureDeferralTestPrincipal()
	install := func(a model.PlatformArtifact, p platformproducer.InputDeferral) error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := setCompilerInputAdmissionTimeouts(ctx, tx); err != nil {
			return err
		}
		if err := pgLockPlatformReleaseMutation(ctx, tx, a.ID, false); err != nil {
			return err
		}
		if err := s.pgValidateInputDeferralPublication(ctx, tx, a, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: p.IdempotencyKey()}, principal); err != nil {
			return err
		}
		now := time.Now().UTC()
		key := platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, "shadow")
		var fence int64
		if err := tx.QueryRowContext(ctx, `INSERT INTO fugue_platform_release_lanes(lane_key,artifact_kind,scope_key,release_channel,fencing_token,version,active_release_id,updated_at) VALUES($1,$2,$3,'shadow',1,1,'',$4) ON CONFLICT(lane_key) DO UPDATE SET fencing_token=fugue_platform_release_lanes.fencing_token+1,version=fugue_platform_release_lanes.version+1 RETURNING fencing_token`, key, a.ArtifactKind, a.ScopeKey, now).Scan(&fence); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE fugue_platform_artifact_releases SET status='superseded' WHERE lane_key=$1 AND status='active'`, key); err != nil {
			return err
		}
		r := model.PlatformArtifactRelease{ID: model.NewID("control-release"), ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, ScopeKey: a.ScopeKey, Scope: a.Scope, Generation: a.Generation, ReleaseChannel: "shadow", Status: "active", LaneKey: key, FencingToken: fence, Version: 1, IdempotencyKey: p.IdempotencyKey(), ReleasedAt: now, CreatedAt: now, UpdatedAt: now}
		if _, err := pgInsertPlatformArtifactRelease(ctx, tx, r); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE fugue_platform_release_lanes SET active_release_id=$2 WHERE lane_key=$1`, key, r.ID); err != nil {
			return err
		}
		return tx.Commit()
	}
	b := p.Request.Authorities[0]
	check := func(want bool) {
		t.Helper()
		got, err := s.ProducerInputDeferred(ctx, b.Scope, b.Policy)
		if err != nil || got != want {
			t.Fatal("durable decision", got, want, err)
		}
	}
	check(false)
	// A publication cannot pass a concurrent admission's shared barrier. A
	// timeout is a refusal with no partial selection, not a grant of capture.
	read, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pgLockPromotionScope(ctx, read, platformproducer.InputDeferralScope, false); err != nil {
		t.Fatal(err)
	}
	if err := install(a, p); err == nil {
		t.Fatal("publication crossed admission barrier")
	}
	if err := read.Commit(); err != nil {
		t.Fatal(err)
	}
	check(false)
	if err := install(a, p); err != nil {
		t.Fatal(err)
	}
	check(true)
	write, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pgLockPlatformReleaseMutation(ctx, write, a.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProducerInputDeferred(ctx, b.Scope, b.Policy); err == nil {
		t.Fatal("uncertain concurrent selection granted admission")
	}
	if err := write.Rollback(); err != nil {
		t.Fatal(err)
	}
	check(true)
	crash()
	s = open()
	check(true)
	released := p
	released.Action, released.Generation = "release", p.Generation+":release"
	terminal := saveInputDeferralTestArtifact(t, file, platformproducer.InputDeferralScope, released.Generation, released)
	if _, err := pgInsertPlatformArtifact(ctx, s.db, terminal); err != nil {
		t.Fatal(err)
	}
	if err := install(terminal, released); err != nil {
		t.Fatal(err)
	}
	check(false)
	if err := install(a, p); err == nil {
		t.Fatal("terminal control was reactivated")
	}
	crash()
	s = open()
	check(false)
	// No optional schema or durable per-attempt work was allocated.
	var optional *string
	if err := s.db.QueryRowContext(ctx, `SELECT to_regclass('fugue_producer_capture_work')::text`).Scan(&optional); err != nil || optional != nil {
		t.Fatal("installed ticket storage", err)
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM fugue_platform_artifact_releases WHERE scope_key=$1`, platformproducer.InputDeferralScope).Scan(&count); err != nil || count != 2 {
		t.Fatal("polling/retry allocated releases", count, err)
	}
}

func saveInputDeferralTestArtifact(t *testing.T, s *Store, scope, generation string, value any) model.PlatformArtifact {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]any
	if err := json.Unmarshal(raw, &content); err != nil {
		t.Fatal(err)
	}
	a, err := s.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{Key: scope}, Generation: generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	a, err = s.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "signed synthetic policy", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestInputDeferralNormalPublicationRestartReleaseAndReplay(t *testing.T) {
	s, p := inputDeferralStoreFixture(t)
	b := p.Request.Authorities[0]
	check := func(want bool) {
		t.Helper()
		got, err := s.ProducerInputDeferred(t.Context(), b.Scope, b.Policy)
		if err != nil || got != want {
			t.Fatal("input admission", got, want, err)
		}
	}
	check(false)
	a := saveInputDeferralTestArtifact(t, s, platformproducer.InputDeferralScope, p.Generation, p)
	runtimeNode, err := compilerInputArtifactRuntimeNode(a)
	if err != nil || len(runtimeNode.References) != 0 {
		t.Fatal("finite control runtime acquired historical authority dependencies", runtimeNode, err)
	}
	node, err := compilerInputArtifactHotNode(a)
	if err != nil || len(node.References) != 2 {
		t.Fatal("finite control omitted its original signed authority dependencies", node, err)
	}
	for _, ref := range node.References {
		if ref.Kind == "artifact" && (ref.ID != b.Policy.ArtifactID || ref.ExpectedHash != b.Policy.ContentHash) || ref.Kind == "release" && ref.ID != b.Policy.ReleaseID {
			t.Fatal("finite control rebound a retention dependency", ref)
		}
	}
	check(false) // signed drafts do not activate a restriction
	req := model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: p.IdempotencyKey()}
	principal := captureDeferralTestPrincipal()
	for _, invalid := range []model.Principal{
		{ActorType: model.ActorTypeAPIKey, ActorID: "ordinary", Scopes: map[string]struct{}{"platform.admin": {}}},
		{ActorType: model.ActorTypeAPIKey, ActorID: "control-only", Scopes: map[string]struct{}{"producer_capture.control": {}}},
	} {
		if _, _, _, _, err := s.ReleasePlatformArtifact(a.ID, req, invalid); err == nil {
			t.Fatal("implicit control authority accepted")
		}
	}
	for _, change := range []func(*model.PlatformArtifactReleaseRequest){
		func(r *model.PlatformArtifactReleaseRequest) { r.IdempotencyKey = "unrelated" },
		func(r *model.PlatformArtifactReleaseRequest) { r.ReleaseChannel = "full" },
		func(r *model.PlatformArtifactReleaseRequest) { r.SoftOverride = true },
	} {
		bad := req
		change(&bad)
		if _, _, _, _, err := s.ReleasePlatformArtifact(a.ID, bad, principal); err == nil {
			t.Fatal("ambiguous/override control accepted")
		}
	}
	if _, _, _, _, err := s.ReleasePlatformArtifact(a.ID, req, principal); err != nil {
		t.Fatal(err)
	}
	check(true)
	// A fresh Store reads the persisted gate, with no registration or timer reset.
	restarted := New(s.path)
	configureTestPlatformArtifactSigning(restarted)
	s = restarted
	check(true)
	changed := b.Policy
	changed.FencingToken++
	if _, err := s.ProducerInputDeferred(t.Context(), b.Scope, changed); err == nil {
		t.Fatal("changed policy bypassed gate")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.ProducerInputDeferred(ctx, b.Scope, b.Policy); err == nil {
		t.Fatal("canceled read granted capture")
	}
	_, before, _, _ := s.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, b.Scope, "shadow")
	released := p
	released.Action, released.Generation = "release", p.Generation+":release"
	terminal := saveInputDeferralTestArtifact(t, s, platformproducer.InputDeferralScope, released.Generation, released)
	if _, _, _, _, err := s.ReleasePlatformArtifact(terminal.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: released.IdempotencyKey()}, principal); err != nil {
		t.Fatal(err)
	}
	check(false)
	if _, _, _, _, err := s.ReleasePlatformArtifact(a.ID, req, principal); err == nil {
		t.Fatal("released operation reactivated by replay")
	}
	if _, _, _, _, err := s.RollbackPlatformArtifact(terminal.ID, model.PlatformArtifactRollbackRequest{ReleaseChannel: "shadow", ToGeneration: a.Generation}, principal); err == nil {
		t.Fatal("control reactivated by rollback")
	}
	_, after, _, _ := s.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, b.Scope, "shadow")
	if before.ID != after.ID || before.FencingToken != after.FencingToken {
		t.Fatal("control changed original producer authority")
	}
	check(false)
}

func TestInputDeferralRejectsDriftAndTampering(t *testing.T) {
	for _, scenario := range []string{"policy drift", "gate tamper", "frozen"} {
		t.Run(scenario, func(t *testing.T) {
			s, p := inputDeferralStoreFixture(t)
			if scenario == "policy drift" {
				p.Request.Authorities[0].Policy.FencingToken++
				p.RequestDigest, _ = p.Request.Digest()
			}
			a := saveInputDeferralTestArtifact(t, s, platformproducer.InputDeferralScope, p.Generation, p)
			_, _, _, _, err := s.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: p.IdempotencyKey()}, captureDeferralTestPrincipal())
			if scenario == "policy drift" {
				if err == nil {
					t.Fatal("stale policy enabled")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := s.withLockedState(true, func(state *model.State) error {
				if scenario == "gate tamper" {
					i := platformArtifactIndex(state.PlatformArtifacts, a.ID)
					state.PlatformArtifacts[i].Content["action"] = "release"
				} else {
					for i := range state.PlatformReleaseLanes {
						if state.PlatformReleaseLanes[i].ScopeKey == platformproducer.InputDeferralScope {
							state.PlatformReleaseLanes[i].Frozen = true
						}
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			b := p.Request.Authorities[0]
			if _, err := s.ProducerInputDeferred(t.Context(), b.Scope, b.Policy); err == nil {
				t.Fatal("unknown durable control granted input")
			}
		})
	}
}
