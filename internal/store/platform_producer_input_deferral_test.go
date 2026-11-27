package store

import (
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/schemamigrate"
)

func inputDeferralStoreFixture(t *testing.T) (*Store, platformproducer.InputDeferral) {
	t.Helper()
	s := newStoreTestDatabase(t, "")
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
