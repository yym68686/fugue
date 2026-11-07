package store

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/schemamigrate"
)

func historyPhase(t *testing.T, s *Store) string {
	t.Helper()
	var phase string
	if err := s.db.QueryRow(`SELECT phase FROM fugue_platform_input_history_admission WHERE singleton`).Scan(&phase); err != nil {
		t.Fatal(err)
	}
	return phase
}

func releaseHistoryFixturePins(t *testing.T, s *Store, pins []compilerInputPinBinding) {
	t.Helper()
	for _, pin := range pins {
		if _, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{Key: pin.Key, Digest: pin.Digest, Apply: true, Release: true, ExpectedToken: pin.Token}); err != nil {
			t.Fatal(err)
		}
	}
}

func installHistoryPhaseRoot(t *testing.T, s *Store) {
	t.Helper()
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := installActivationRoot(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryPhasePostgresCapabilityPreservesRequestsButNeverPreparationProof(t *testing.T) {
	s, roots, pins := inputActivationFixturePhase(t, 1, false)
	releaseHistoryFixturePins(t, s, pins)
	a, err := s.GetPlatformArtifact("activation-root")
	if err != nil {
		t.Fatal(err)
	}
	reads := []PlatformInputReadSelection{{ArtifactID: a.ID, ContentHash: a.ContentHash}}
	if historyPhase(t, s) != "capability" || s.CheckPlatformInputRead(t.Context(), reads) != nil {
		t.Fatal("first capability deployment required pre-existing verified pins")
	}
	if r, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false); !errors.Is(err, ErrConflict) || r.Applied || historyPhase(t, s) != "capability" {
		t.Fatal("capability deployment armed writer fence", r, err)
	}
	p, err := s.prepareCompilerInputActivation(t.Context(), roots)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.prepareCompilerInputActivationPins(t.Context(), p); !errors.Is(err, ErrConflict) {
		t.Fatal("capability phase weakened explicit preparation confirmation", err)
	}
	// Exercise the real ordinary publication, including its existing business
	// signature, channel, message and retention checks, with no restored pin.
	publicationInputValidate(t, s, "activation-artifact-000")
	if _, _, _, _, err := s.ReleasePlatformArtifact("activation-artifact-000", model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal()); err != nil {
		t.Fatal("capability release rejected existing intact legacy dependencies", err)
	}
	prepared, err := s.prepareCompilerInputHistoryEnforcement(t.Context())
	if err == nil || prepared != nil || historyPhase(t, s) != "capability" {
		t.Fatal("missing retained pin enabled strict phase", prepared, err)
	}
	// Fence and retirement states can never select the compatibility path.
	for _, q := range []string{`UPDATE fugue_platform_input_archive_control SET writes_fenced=true`, `UPDATE fugue_platform_input_archive_control SET writes_fenced=false,retirement_started_at=now()`} {
		if _, err := s.db.Exec(q); err != nil {
			t.Fatal(err)
		}
		if err := s.CheckPlatformInputRead(t.Context(), reads); !errors.Is(err, ErrConflict) {
			t.Fatal("fenced/retired database used capability compatibility", err)
		}
	}
}

func TestHistoryPhasePostgresEnforcementSurvivesNewPoolMigrationAndCannotDowngrade(t *testing.T) {
	s, _, pins := inputActivationFixturePhase(t, 1, false)
	installHistoryPhaseRoot(t, s)
	prepared, err := s.prepareCompilerInputHistoryEnforcement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := s.enforcePreparedCompilerInputHistory(t.Context(), prepared)
	if err != nil || len(plan) != 64 || historyPhase(t, s) != "enforced" {
		t.Fatal(plan, err)
	}
	if _, err := s.enforcePreparedCompilerInputHistory(t.Context(), prepared); !errors.Is(err, ErrConflict) {
		t.Fatal("enforcement plan replayed", err)
	}
	if err := schemamigrate.MigrateCompilerInputArchive(t.Context(), s.databaseURL); err != nil {
		t.Fatal("additive schema rerun failed", err)
	}
	for _, q := range []string{
		`UPDATE fugue_platform_input_history_admission SET phase='capability',plan_sha256='',enforced_at=NULL`,
		`UPDATE fugue_platform_input_history_admission SET plan_sha256='` + strings.Repeat("b", 64) + `'`,
		`DELETE FROM fugue_platform_input_history_admission`,
		`TRUNCATE fugue_platform_input_history_admission`,
	} {
		if _, err := s.db.Exec(q); err == nil {
			t.Fatal("durable enforcement was downgraded or erased", q)
		}
	}
	db, err := openPostgresDatabase(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fresh := &Store{db: db, databaseURL: s.databaseURL}
	fresh.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
	a, err := fresh.GetPlatformArtifact("activation-root")
	if err != nil {
		t.Fatal(err)
	}
	read := []PlatformInputReadSelection{{ArtifactID: a.ID, ContentHash: a.ContentHash}}
	if err := fresh.CheckPlatformInputRead(t.Context(), read); err != nil {
		t.Fatal("prepared enforcement failed in fresh process pool", err)
	}
	releaseHistoryFixturePins(t, s, pins)
	if err := fresh.CheckPlatformInputRead(t.Context(), read); !errors.Is(err, ErrConflict) {
		t.Fatal("fresh process ignored enforced missing pin", err)
	}
}

func TestHistoryPhasePostgresEnforcementWaitsForUnregisteredSnapshot(t *testing.T) {
	s, _, _ := inputActivationFixturePhase(t, 1, false)
	installHistoryPhaseRoot(t, s)
	prepared, err := s.prepareCompilerInputHistoryEnforcement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := s.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	// No input table, row lock or application advisory lease is touched. This
	// snapshot is newer than preparation but still predates the switching CAS.
	var snapshot string
	if err := reader.QueryRow(`SELECT pg_current_snapshot()::text`).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	if plan, err := s.enforcePreparedCompilerInputHistory(t.Context(), prepared); !errors.Is(err, ErrConflict) || plan != "" || historyPhase(t, s) != "capability" {
		t.Fatal("unregistered snapshot did not abort enforcement atomically", plan, err)
	}
	var stillActive bool
	if err := reader.QueryRow(`SELECT true`).Scan(&stillActive); err != nil || !stillActive {
		t.Fatal("enforcement cancelled another session instead of waiting", err)
	}
	if err := reader.Rollback(); err != nil {
		t.Fatal(err)
	}
	if plan, err := s.enforcePreparedCompilerInputHistory(t.Context(), prepared); err != nil || len(plan) != 64 || historyPhase(t, s) != "enforced" {
		t.Fatal("naturally drained snapshot still blocked exact prepared transition", plan, err)
	}
}

func TestHistoryPhasePostgresEnforcementWaitsForPreparedTransaction(t *testing.T) {
	s, _, _ := inputActivationFixturePhase(t, 1, false)
	installHistoryPhaseRoot(t, s)
	prepared, err := s.prepareCompilerInputHistoryEnforcement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var limit int
	if err := s.db.QueryRow(`SHOW max_prepared_transactions`).Scan(&limit); err != nil {
		t.Fatal(err)
	}
	if limit == 0 {
		t.Skip("disposable PostgreSQL needs max_prepared_transactions>0")
	}
	conn, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	const prepare = `PREPARE TRANSACTION 'neutral_history_enforcement_drain'`
	const rollback = `ROLLBACK PREPARED 'neutral_history_enforcement_drain'`
	for _, query := range []string{`BEGIN`, `SELECT 1`, prepare} {
		if _, err := conn.ExecContext(t.Context(), query); err != nil {
			_, _ = conn.ExecContext(t.Context(), `ROLLBACK`)
			t.Fatal(err)
		}
	}
	active := true
	defer func() {
		if active {
			if _, err := conn.ExecContext(t.Context(), rollback); err != nil {
				t.Error("clean disposable prepared transaction", err)
			}
		}
	}()
	if plan, err := s.enforcePreparedCompilerInputHistory(t.Context(), prepared); !errors.Is(err, ErrConflict) || plan != "" || historyPhase(t, s) != "capability" {
		t.Fatal("prepared transaction did not abort enforcement", plan, err)
	}
	var remaining int
	if err := s.db.QueryRow(`SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database() AND gid='neutral_history_enforcement_drain'`).Scan(&remaining); err != nil || remaining != 1 {
		t.Fatal("enforcement disposed of another prepared transaction", remaining, err)
	}
	if _, err := conn.ExecContext(t.Context(), rollback); err != nil {
		t.Fatal(err)
	}
	active = false
	if plan, err := s.enforcePreparedCompilerInputHistory(t.Context(), prepared); err != nil || len(plan) != 64 || historyPhase(t, s) != "enforced" {
		t.Fatal("finished prepared transaction still blocked transition", plan, err)
	}
}

func TestHistoryPhasePostgresEnforcementRequiresFullStatsVisibility(t *testing.T) {
	s, _, _ := inputActivationFixturePhase(t, 1, false)
	installHistoryPhaseRoot(t, s)
	var schema string
	if err := s.db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	role := quotePostgresIntegrationIdentifier(t, model.NewID("history_enforcement_reader_test"))
	if _, err := s.db.Exec("CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, query := range []string{"DROP OWNED BY " + role, "DROP ROLE " + role} {
			if _, err := s.db.Exec(query); err != nil {
				t.Error("clean disposable enforcement role", err)
			}
		}
	})
	if _, err := s.db.Exec("GRANT USAGE ON SCHEMA " + quotePostgresIntegrationIdentifier(t, schema) + " TO " + role + "; GRANT SELECT,UPDATE ON ALL TABLES IN SCHEMA " + quotePostgresIntegrationIdentifier(t, schema) + " TO " + role); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.User(strings.Trim(role, `"`))
	db, err := openPostgresDatabase(u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	limited := &Store{db: db, databaseURL: u.String()}
	limited.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
	prepared, err := limited.prepareCompilerInputHistoryEnforcement(t.Context())
	if err != nil {
		t.Fatal("limited role could not prepare the real retained closure", err)
	}
	if plan, err := limited.enforcePreparedCompilerInputHistory(t.Context(), prepared); !errors.Is(err, ErrConflict) || plan != "" || historyPhase(t, s) != "capability" {
		t.Fatal("hidden transaction metadata was accepted as drained", plan, err)
	}
	// Only the disposable role gains statistics visibility; no application
	// credential or production grant is changed by this test or implementation.
	if _, err := s.db.Exec("GRANT pg_read_all_stats TO " + role); err != nil {
		t.Fatal(err)
	}
	if plan, err := limited.enforcePreparedCompilerInputHistory(t.Context(), prepared); err != nil || len(plan) != 64 || historyPhase(t, s) != "enforced" {
		t.Fatal("full effective visibility did not permit drained transition", plan, err)
	}
}

func TestHistoryPhasePostgresCapabilityConsumerAndTaskKeepOriginalAuthority(t *testing.T) {
	s, _, pins := inputActivationFixturePhase(t, 1, false)
	releaseHistoryFixturePins(t, s, pins)
	publicationInputValidate(t, s, "activation-artifact-000")
	_, release, _, _, err := s.ReleasePlatformArtifact("activation-artifact-000", model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	set, claims, heartbeat := consumerInputEnvelope(t, s, release)
	if _, err := s.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatal("capability consumer registration required migration pins", err)
	}
	audit := bundleauth.NewKeyring("neutral-audit", "neutral-audit", "", "", nil)
	if _, err := s.AcceptTrustedPlatformConsumerHeartbeatWithAudit(claims, set.ID, heartbeat, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}, audit); err != nil {
		t.Fatal("capability heartbeat required migration pins", err)
	}
	before := consumerInputCounts(t, s)
	bad := claims
	bad.NodeID = "unassigned-neutral-node"
	if _, err := s.AcceptTrustedPlatformConsumerHeartbeatWithAudit(bad, set.ID, heartbeat, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}, audit); err == nil || before != consumerInputCounts(t, s) {
		t.Fatal("capability phase bypassed consumer ownership/cursor/audit", err)
	}
	if _, err := s.db.Exec(`INSERT INTO fugue_node_updaters(id,status,token_prefix,token_hash,created_at,updated_at)
 VALUES('phase-updater','ready','neutral','neutral-hash',now(),now());
 INSERT INTO fugue_node_update_tasks(id,node_updater_id,task_type,payload_json,status,created_at,updated_at)
 VALUES('phase-task','phase-updater','neutral','{"artifact_id":"activation-root"}','pending',now(),now())`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNodeUpdateTask("phase-task", "unassigned-neutral-updater"); !errors.Is(err, ErrNotFound) {
		t.Fatal("capability phase bypassed task ownership", err)
	}
	if task, err := s.ClaimNodeUpdateTask("phase-task", "phase-updater"); err != nil || task.Status != model.NodeUpdateTaskStatusRunning {
		t.Fatal("capability task claim required migration pins", task, err)
	}
}

func TestHistoryPhasePostgresStaleCapabilityProofAndEnablingDriftRefuse(t *testing.T) {
	for _, change := range []string{"root", "tuple", "pin", "keyring", "expired"} {
		t.Run(change, func(t *testing.T) {
			s, _, pins := inputActivationFixturePhase(t, 1, false)
			installHistoryPhaseRoot(t, s)
			prepared, err := s.prepareCompilerInputHistoryEnforcement(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "root":
				_, err = s.db.Exec(`UPDATE fugue_platform_artifact_releases SET status='superseded' WHERE id='activation-release'`)
			case "tuple":
				_, err = s.db.Exec(`UPDATE fugue_platform_artifacts SET updated_at=updated_at WHERE id='activation-root'`)
			case "pin":
				releaseHistoryFixturePins(t, s, pins)
			case "keyring":
				s.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
			case "expired":
				prepared.proof.expires = time.Now().Add(-time.Second)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.enforcePreparedCompilerInputHistory(t.Context(), prepared); err == nil || historyPhase(t, s) != "capability" {
				t.Fatal("drifted preparation enabled enforcement", err)
			}
		})
	}
}

func TestHistoryPhasePostgresCapabilityTransactionBlocksSwitchAndMissingStateRefuses(t *testing.T) {
	s, roots, _ := inputActivationFixturePhase(t, 0, false)
	p, err := s.prepareOrdinaryCompilerInputProof(t.Context(), roots, nil, nil, nil, nil)
	if err != nil || !p.capabilityOnly || p.closure.MetadataComplete {
		t.Fatal(p, err)
	}
	prepared, err := s.prepareCompilerInputHistoryEnforcement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The callback is a real transaction holding shared barriers. A phase
	// switch must hit the original 250ms lock budget, never overtake it.
	err = s.withPreparedCompilerInputActivation(t.Context(), p, nil, func(context.Context, *sql.Tx) error {
		if _, err := s.enforcePreparedCompilerInputHistory(t.Context(), prepared); err == nil || historyPhase(t, s) != "capability" {
			t.Fatal("switch overtook in-flight compatibility admission", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.enforcePreparedCompilerInputHistory(t.Context(), prepared); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := s.withPreparedCompilerInputActivation(t.Context(), p, nil, func(context.Context, *sql.Tx) error { called = true; return nil }); !errors.Is(err, ErrConflict) || called {
		t.Fatal("stale capability proof crossed durable switch", err)
	}
	if _, err := s.db.Exec(`DROP TABLE fugue_platform_input_history_admission`); err != nil {
		t.Fatal(err)
	}
	if p, err := s.prepareOrdinaryCompilerInputProof(t.Context(), roots, nil, nil, nil, nil); !errors.Is(err, ErrConflict) || p != nil {
		t.Fatal("missing schema was interpreted as compatibility phase", p, err)
	}
}
