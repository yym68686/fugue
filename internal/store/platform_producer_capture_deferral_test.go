package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func TestCaptureDeferralRejectsUnprivilegedOrUninstalledControl(t *testing.T) {
	s := &Store{}
	request := platformproducer.CaptureDeferralRequest{}
	if _, err := s.PrepareProducerCaptureDeferral(t.Context(), request, model.Principal{ActorID: "ordinary"}); err == nil {
		t.Fatal("unprivileged control was accepted")
	}
	control := platformproducer.CaptureDeferralControlRequest{SchemaVersion: platformproducer.CaptureDeferralControlSchema, OperationID: "control-alpha", RequestDigest: "sha256:" + strings.Repeat("a", 64), Action: "release"}
	if _, err := s.UpdateProducerCaptureDeferral(t.Context(), control, model.Principal{ActorID: "ordinary"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal("unprivileged update reached storage", err)
	}
	identityless := captureDeferralTestPrincipal()
	identityless.ActorType = ""
	if _, err := s.UpdateProducerCaptureDeferral(t.Context(), control, identityless); !errors.Is(err, ErrInvalidInput) {
		t.Fatal("control with an incomplete caller identity reached storage", err)
	}
	control.Action = "extend"
	if _, err := s.UpdateProducerCaptureDeferral(t.Context(), control, captureDeferralTestPrincipal()); !errors.Is(err, ErrInvalidInput) {
		t.Fatal("unsupported update reached storage", err)
	}
	principal := captureDeferralTestPrincipal()
	if _, _, err := s.BeginProducerCapture(t.Context(), "boot-alpha", platformproducer.Scope, model.PlatformPublicationPrecondition{}, principal); err == nil {
		t.Fatal("uninstalled admission silently bypassed")
	}
	principal.ActorID = "unrelated-bootstrap-worker"
	if _, _, err := s.BeginProducerCapture(t.Context(), "boot-alpha", platformproducer.Scope, model.PlatformPublicationPrecondition{}, principal); err == nil {
		t.Fatal("unrelated bootstrap worker admitted capture")
	}
}

func captureDeferralTestPrincipal() model.Principal {
	return model.Principal{ActorType: model.ActorTypeBootstrap, ActorID: platformproducer.Actor, Scopes: map[string]struct{}{"platform.admin": {}}}
}

// Initializes its own loopback-only fsync-enabled cluster. No external DSN,
// existing data directory, production snapshot or environment credential is
// accepted. The fixture contains only signed synthetic policies and work IDs.
func captureDeferralPrivatePostgres(t *testing.T) (func() *Store, func()) {
	t.Helper()
	bin := os.Getenv("FUGUE_PRODUCER_DEFERRAL_POSTGRES_BIN")
	if bin == "" {
		t.Skip("set FUGUE_PRODUCER_DEFERRAL_POSTGRES_BIN to disposable PostgreSQL 17+ tools")
	}
	if !filepath.IsAbs(bin) {
		t.Fatal("PostgreSQL tools directory must be absolute")
	}
	root := t.TempDir()
	data, log := filepath.Join(root, "data"), filepath.Join(root, "postgres.log")
	run := func(args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if out, err := exec.CommandContext(ctx, filepath.Join(bin, args[0]), args[1:]...).CombinedOutput(); err != nil {
			t.Fatalf("private PostgreSQL %s: %v\n%s", args[0], err, out)
		}
	}
	run("initdb", "-D", data, "-A", "trust", "--no-locale", "--encoding=UTF8", "-U", "deferral_test")
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	options := fmt.Sprintf("-h 127.0.0.1 -p %d -c unix_socket_directories='' -c fsync=on -c full_page_writes=on -c synchronous_commit=on", port)
	start := func() { run("pg_ctl", "-D", data, "-l", log, "-o", options, "-t", "15", "-w", "start") }
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		status := exec.CommandContext(ctx, filepath.Join(bin, "pg_ctl"), "-D", data, "status")
		if err := status.Run(); err != nil {
			if exit, ok := err.(*exec.ExitError); ok && exit.ExitCode() == 3 {
				return
			}
			t.Errorf("private cluster cleanup status: %v", err)
			return
		}
		run("pg_ctl", "-D", data, "-m", "smart", "-t", "15", "-w", "stop")
	})
	start()
	address := url.URL{Scheme: "postgres", User: url.User("deferral_test"), Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), Path: "/postgres"}
	address.RawQuery = url.Values{"sslmode": {"disable"}, "connect_timeout": {"5"}, "temp_file_limit": {"0"}, "work_mem": {"1MB"}}.Encode()
	open := func() *Store {
		t.Helper()
		db, err := sql.Open("pgx", address.String())
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(12)
		db.SetMaxIdleConns(0)
		t.Cleanup(func() { _ = db.Close() })
		var version, actualPort int
		var actualDirectory string
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		if err := db.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int,inet_server_port(),current_setting('data_directory')`).Scan(&version, &actualPort, &actualDirectory); err != nil || version < 170000 || actualPort != port || actualDirectory != data {
			t.Fatalf("private server identity: %d %d %q %v", version, actualPort, actualDirectory, err)
		}
		s := &Store{db: db, databaseURL: address.String()}
		configureTestPlatformArtifactSigning(s)
		return s
	}
	return open, func() { run("pg_ctl", "-D", data, "-m", "immediate", "-t", "15", "-w", "stop"); start() }
}

func TestCaptureDeferralPrivatePostgresAtomicDrainCrashAndExpiry(t *testing.T) {
	open, crash := captureDeferralPrivatePostgres(t)
	s := open()
	ctx := t.Context()
	principal := captureDeferralTestPrincipal()
	// Reuse the real three platform table definitions; never install this
	// optional schema as a side effect of Store.Init or a production request.
	for _, statement := range postgresSchemaStatements {
		for _, table := range []string{"fugue_platform_artifacts", "fugue_platform_artifact_releases", "fugue_platform_release_lanes"} {
			if strings.HasPrefix(statement, "CREATE TABLE IF NOT EXISTS "+table+" (") || strings.HasPrefix(statement, "ALTER TABLE "+table+" ") {
				if _, err := s.db.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	var before *string
	if err := s.db.QueryRowContext(ctx, `SELECT to_regclass('fugue_producer_capture_deferrals')::text`).Scan(&before); err != nil || before != nil {
		t.Fatal("optional schema already existed", err)
	}
	if _, _, err := s.BeginProducerCapture(ctx, "boot-alpha", platformproducer.Scope, model.PlatformPublicationPrecondition{}, principal); err == nil {
		t.Fatal("missing optional schema did not refuse")
	}
	if _, err := s.db.ExecContext(ctx, producerCaptureDeferralSchema); err != nil {
		t.Fatal(err)
	}
	participant := ProducerCaptureParticipant{ID: "boot-alpha", Scopes: []string{platformproducer.Scope}}
	if err := s.RegisterProducerCaptureParticipant(ctx, participant, model.Principal{ActorID: "ordinary"}); err == nil {
		t.Fatal("unprivileged replica registered")
	}
	if err := s.RegisterProducerCaptureParticipant(ctx, participant, principal); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterProducerCaptureParticipant(ctx, participant, principal); err == nil {
		t.Fatal("same boot registered twice")
	}
	file := New(filepath.Join(t.TempDir(), "fixture.json"))
	configureTestPlatformArtifactSigning(file)
	if err := file.Init(); err != nil {
		t.Fatal(err)
	}
	policy := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "policy-alpha", Mode: "shadow", InputSource: "business-static-intent", TargetScope: "global", IntervalSeconds: 30, RefreshSeconds: 120, StaticIntentArtifactID: "synthetic-static", StaticIntentDigest: "sha256:" + strings.Repeat("a", 64)}
	raw, _ := json.Marshal(policy)
	var content map[string]any
	if err := json.Unmarshal(raw, &content); err != nil {
		t.Fatal(err)
	}
	a, err := file.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{Key: platformproducer.Scope}, Generation: policy.Generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	a, err = file.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "synthetic policy", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgInsertPlatformArtifact(ctx, s.db, a); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	lane := platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, "shadow")
	release := model.PlatformArtifactRelease{ID: "policy-release-alpha", ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, ScopeKey: a.ScopeKey, Scope: a.Scope, Generation: a.Generation, ReleaseChannel: "shadow", Status: "active", LaneKey: lane, FencingToken: 1, Version: 1, ReleasedAt: now, CreatedAt: now, UpdatedAt: now}
	if _, err := pgInsertPlatformArtifactRelease(ctx, s.db, release); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO fugue_platform_release_lanes(lane_key,artifact_kind,scope_key,release_channel,fencing_token,version,active_release_id,updated_at) VALUES($1,$2,$3,'shadow',1,1,$4,$5)`, lane, a.ArtifactKind, a.ScopeKey, release.ID, now); err != nil {
		t.Fatal(err)
	}
	binding := model.PlatformPublicationPrecondition{ArtifactID: a.ID, ContentHash: a.ContentHash, ReleaseID: release.ID, FencingToken: release.FencingToken}
	request := func(id string, duration time.Duration) platformproducer.CaptureDeferralRequest {
		at := time.Now().UTC().Add(-time.Millisecond)
		return platformproducer.CaptureDeferralRequest{SchemaVersion: platformproducer.CaptureDeferralSchema, OperationID: id, RequestedAt: at, ExpiresAt: at.Add(duration), Authorities: []platformproducer.CaptureDeferralAuthority{{Scope: a.ScopeKey, Policy: binding}}}
	}
	control := func(request platformproducer.CaptureDeferralRequest, action string) platformproducer.CaptureDeferralControlRequest {
		t.Helper()
		digest, err := request.Digest()
		if err != nil {
			t.Fatal(err)
		}
		return platformproducer.CaptureDeferralControlRequest{SchemaVersion: platformproducer.CaptureDeferralControlSchema, OperationID: request.OperationID, RequestDigest: digest, Action: action}
	}
	update := func(db *Store, request platformproducer.CaptureDeferralRequest, action string) (platformproducer.CaptureDeferralRecord, error) {
		t.Helper()
		c := control(request, action)
		receipt, err := db.UpdateProducerCaptureDeferral(ctx, c, principal)
		if err == nil && (receipt.Control != c || receipt.Record.RequestDigest != c.RequestDigest || receipt.Record.Request.OperationID != c.OperationID || receipt.Version < 1 || receipt.ActorID != principal.ActorID || receipt.ActorType != principal.ActorType) {
			t.Fatal("committed control receipt lost its target or caller", receipt)
		}
		return receipt.Record, err
	}
	work, deferred, err := s.BeginProducerCapture(ctx, participant.ID, a.ScopeKey, binding, principal)
	if err != nil || deferred || work.ID == "" {
		t.Fatal("initial admission", work, deferred, err)
	}
	if _, _, err := s.BeginProducerCapture(ctx, "unregistered-boot", a.ScopeKey, binding, principal); err == nil {
		t.Fatal("unregistered process admitted work")
	}
	if _, _, err := s.BeginProducerCapture(ctx, participant.ID, platformproducer.Scope+":other-cell", binding, principal); err == nil {
		t.Fatal("process admitted an unenrolled scope")
	}
	if err := s.CloseProducerCaptureParticipant(ctx, participant, principal); err == nil {
		t.Fatal("participant with admitted work closed")
	}
	req := request("control-alpha", 90*time.Second)
	record, err := s.PrepareProducerCaptureDeferral(ctx, req, principal)
	if err != nil || record.Phase != "draining" {
		t.Fatal("in-flight work was not retained", record, err)
	}
	for _, action := range []string{"observe", "release"} {
		wrong := control(req, action)
		wrong.RequestDigest = "sha256:" + strings.Repeat("f", 64)
		if got, err := s.UpdateProducerCaptureDeferral(ctx, wrong, principal); !errors.Is(err, ErrConflict) || got.Version != 0 || got.Record.RequestDigest != "" {
			t.Fatal("mismatched target changed a control or returned a receipt", got, err)
		}
	}
	polled, err := s.UpdateProducerCaptureDeferral(ctx, control(req, "observe"), principal)
	if err != nil || polled.Version != 1 || polled.Changed || polled.Record.Phase != "draining" || !polled.Record.ObservedAt.Equal(record.ObservedAt) {
		t.Fatal("unchanged poll rewrote or renewed a control", polled, err)
	}
	if _, err := s.PrepareProducerCaptureDeferral(ctx, request("overlap", time.Minute), principal); err == nil {
		t.Fatal("overlapping scope claim was accepted")
	}
	// A killed process/connection does not erase its durable admission. Restart
	// a real fsync-enabled PG, then use a new Store and pool as another replica.
	crash()
	restarted := open()
	record, err = update(restarted, req, "observe")
	if err != nil || record.Phase != "draining" || !record.Request.ExpiresAt.Equal(req.ExpiresAt) {
		t.Fatal("crash lost the drain or reset its deadline", record, err)
	}
	wrong := work
	wrong.Policy.FencingToken++
	if err := restarted.FinishProducerCapture(ctx, wrong, principal); err == nil {
		t.Fatal("different work authority completed a ticket")
	}
	wrong = work
	wrong.ParticipantID = "boot-other"
	if err := restarted.FinishProducerCapture(ctx, wrong, principal); err == nil {
		t.Fatal("another participant completed a ticket")
	}
	if err := restarted.CloseProducerCaptureParticipant(ctx, participant, principal); err == nil {
		t.Fatal("crash erased an occupied participant")
	}
	// Real concurrent clients all observe the same barrier; none obtains an
	// admission while draining. This is not a fake single-process counter.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			w, held, err := restarted.BeginProducerCapture(ctx, participant.ID, a.ScopeKey, binding, principal)
			if err != nil || !held || w.ID != "" {
				t.Errorf("concurrent drain admission: %v %t %v", w, held, err)
			}
		})
	}
	wg.Wait()
	if err := restarted.FinishProducerCapture(ctx, work, principal); err != nil {
		t.Fatal(err)
	}
	record, err = update(restarted, req, "observe")
	if err != nil || record.Phase != "active" || !record.Request.ExpiresAt.Equal(req.ExpiresAt) {
		t.Fatal("zero-ticket activation", record, err)
	}
	released, err := restarted.UpdateProducerCaptureDeferral(ctx, control(req, "release"), principal)
	record = released.Record
	if err != nil || record.Phase != "released" {
		t.Fatal("release", record, err)
	}
	if !released.Changed || released.Version != 3 || released.ActorID != principal.ActorID || released.Control != control(req, "release") {
		t.Fatal("release receipt did not describe its committed CAS", released)
	}
	crash()
	restarted = open()
	// A lost release response can be retried using the original request. Read
	// the persisted terminal record without another write or deadline renewal.
	replayed, err := restarted.UpdateProducerCaptureDeferral(ctx, control(req, "release"), principal)
	if err != nil || replayed.Changed || replayed.Version != released.Version || replayed.Record.Phase != "released" || !replayed.Record.EndedAt.Equal(*released.Record.EndedAt) || !replayed.Record.Request.ExpiresAt.Equal(req.ExpiresAt) {
		t.Fatal("release replay changed the durable terminal receipt", replayed, err)
	}
	// A retained terminal control no longer needs its historical policy. Make
	// the policy relation unavailable, rather than supplying an empty fake
	// observation, and verify that observe still returns the committed receipt.
	policyOffline := func(offline bool) {
		t.Helper()
		statement := `ALTER TABLE fugue_platform_release_lanes RENAME TO capture_test_offline_lanes`
		if !offline {
			statement = `ALTER TABLE capture_test_offline_lanes RENAME TO fugue_platform_release_lanes`
		}
		if _, err := restarted.db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	policyOffline(true)
	terminal, err := restarted.UpdateProducerCaptureDeferral(ctx, control(req, "observe"), principal)
	if err != nil || terminal.Changed || terminal.Version != released.Version || !terminal.Record.EndedAt.Equal(*released.Record.EndedAt) {
		t.Fatal("terminal observation depended on historical policy availability", terminal, err)
	}
	policyOffline(false)
	if _, err := restarted.PrepareProducerCaptureDeferral(ctx, req, principal); err == nil {
		t.Fatal("terminal operation replayed after real PG restart")
	}
	var scopes, retained int
	if err := restarted.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM fugue_producer_capture_deferral_scopes),(SELECT count(*) FROM fugue_producer_capture_deferrals)`).Scan(&scopes, &retained); err != nil || scopes != 0 || retained != 1 {
		t.Fatal("overlap rolled back incompletely or tombstone lost", scopes, retained, err)
	}
	expiryWork, held, err := restarted.BeginProducerCapture(ctx, participant.ID, a.ScopeKey, binding, principal)
	if err != nil || held || expiryWork.ID == "" {
		t.Fatal("admission before finite drain", expiryWork, held, err)
	}
	short := request("control-expiry", 400*time.Millisecond)
	if record, err = restarted.PrepareProducerCaptureDeferral(ctx, short, principal); err != nil || record.Phase != "draining" {
		t.Fatal(record, err)
	}
	<-time.After(500 * time.Millisecond)
	policyOffline(true)
	expired, err := restarted.UpdateProducerCaptureDeferral(ctx, control(short, "observe"), principal)
	if err != nil || !expired.Changed || expired.Version != 2 || expired.Record.Phase != "expired" || expired.Record.EndedAt == nil || !expired.Record.Request.ExpiresAt.Equal(short.ExpiresAt) {
		t.Fatal("absolute expiry depended on historical policy availability", expired, err)
	}
	if _, _, err := restarted.BeginProducerCapture(ctx, participant.ID, a.ScopeKey, binding, principal); err == nil {
		t.Fatal("expiry bypassed ordinary policy storage validation")
	}
	terminal, err = restarted.UpdateProducerCaptureDeferral(ctx, control(short, "observe"), principal)
	if err != nil || terminal.Changed || terminal.Version != expired.Version || !terminal.Record.EndedAt.Equal(*expired.Record.EndedAt) {
		t.Fatal("terminal expiry observation rewrote its receipt", terminal, err)
	}
	policyOffline(false)
	if err := restarted.CloseProducerCaptureParticipant(ctx, participant, principal); err == nil {
		t.Fatal("expiry erased previously admitted work")
	}
	work, deferred, err = restarted.BeginProducerCapture(ctx, participant.ID, a.ScopeKey, binding, principal)
	if err != nil || deferred || work.ID == "" {
		t.Fatal("absolute expiry remained restrictive", work, deferred, err)
	}
	if err := restarted.FinishProducerCapture(ctx, work, principal); err != nil {
		t.Fatal(err)
	}
	if err := restarted.FinishProducerCapture(ctx, expiryWork, principal); err != nil {
		t.Fatal("pre-expiry ticket did not retain its exact completion", err)
	}
	record, err = update(restarted, short, "observe")
	if err != nil || record.Phase != "expired" || !record.Request.ExpiresAt.Equal(short.ExpiresAt) {
		t.Fatal("expiry tombstone", record, err)
	}
	authorityRequest := request("control-authority", time.Minute)
	if _, err := restarted.PrepareProducerCaptureDeferral(ctx, authorityRequest, principal); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.db.ExecContext(ctx, `UPDATE fugue_platform_release_lanes SET frozen=true WHERE lane_key=$1`, lane); err != nil {
		t.Fatal(err)
	}
	record, err = update(restarted, authorityRequest, "observe")
	if err != nil || record.Phase != "invalidated" {
		t.Fatal("authority drift was not terminal", record, err)
	}
	if _, _, err := restarted.BeginProducerCapture(ctx, participant.ID, a.ScopeKey, binding, principal); err == nil {
		t.Fatal("gate release bypassed ordinary authority validation")
	}
	if _, err := restarted.db.ExecContext(ctx, `UPDATE fugue_platform_release_lanes SET frozen=false WHERE lane_key=$1`, lane); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.db.ExecContext(ctx, `UPDATE fugue_platform_artifacts SET content_json=jsonb_set(content_json,'{refresh_seconds}','999') WHERE id=$1`, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.PrepareProducerCaptureDeferral(ctx, request("tampered-policy", time.Minute), principal); err == nil {
		t.Fatal("matching metadata hid an invalid signed policy body")
	}
	if err := restarted.CloseProducerCaptureParticipant(ctx, participant, principal); err != nil {
		t.Fatal("empty participant did not close", err)
	}
	crash()
	restarted = open()
	if err := restarted.RegisterProducerCaptureParticipant(ctx, participant, principal); err == nil {
		t.Fatal("closed boot identity was reused after PG restart")
	}
	if _, _, err := restarted.BeginProducerCapture(ctx, participant.ID, a.ScopeKey, binding, principal); err == nil {
		t.Fatal("closed participant admitted new capture")
	}
}
