package store

import (
	"context"
	"database/sql"
	"errors"
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

func TestHistoryPhasePostgresCapabilityPreservesRequestsButNeverPreparationProof(t *testing.T) {
	s, roots, pins := inputActivationFixturePhase(t, 1, false)
	releaseHistoryFixturePins(t, s, pins)
	a, err := s.GetPlatformArtifact("activation-root")
	if err != nil {
		t.Fatal(err)
	}
	reads := []PlatformInputReadSelection{{ArtifactID: a.ID, ContentHash: a.ContentHash}}
	if historyPhase(t, s) != "capability" || s.CheckPlatformInputRead(t.Context(), reads) != nil {
		t.Fatal("capability compatibility required pre-existing verified pins")
	}
	if r, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false); !errors.Is(err, ErrConflict) || r.Applied || historyPhase(t, s) != "capability" {
		t.Fatal("capability compatibility armed writer fence", r, err)
	}
	p, err := s.prepareCompilerInputActivation(t.Context(), roots)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.prepareCompilerInputActivationPins(t.Context(), p); !errors.Is(err, ErrConflict) {
		t.Fatal("capability phase weakened explicit preparation confirmation", err)
	}
	publicationInputValidate(t, s, "activation-artifact-000")
	if _, _, _, _, err := s.ReleasePlatformArtifact("activation-artifact-000", model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal()); err != nil {
		t.Fatal("capability publication rejected intact dependencies", err)
	}
	for _, q := range []string{`UPDATE fugue_platform_input_archive_control SET writes_fenced=true`, `UPDATE fugue_platform_input_archive_control SET writes_fenced=false,retirement_started_at=now()`} {
		if _, err := s.db.Exec(q); err != nil {
			t.Fatal(err)
		}
		if err := s.CheckPlatformInputRead(t.Context(), reads); !errors.Is(err, ErrConflict) {
			t.Fatal("fenced/retired database used capability compatibility", err)
		}
	}
}

func TestHistoryPhasePostgresExistingEnforcementSurvivesNewPoolAndCannotDowngrade(t *testing.T) {
	// A historical persisted state, not a new production enabling workflow.
	s, _, pins := inputActivationFixture(t, 1)
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
			t.Fatal("existing enforcement was downgraded or erased", q)
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
		t.Fatal("existing enforcement failed in fresh process pool", err)
	}
	releaseHistoryFixturePins(t, s, pins)
	if err := fresh.CheckPlatformInputRead(t.Context(), read); !errors.Is(err, ErrConflict) {
		t.Fatal("fresh process ignored existing enforced missing pin", err)
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

func TestHistoryPhasePostgresCapabilityTransactionGuardsLegacyStateAndMissingStateRefuses(t *testing.T) {
	s, roots, _ := inputActivationFixturePhase(t, 0, false)
	p, err := s.prepareOrdinaryCompilerInputProof(t.Context(), roots, nil, nil, nil, nil)
	if err != nil || !p.capabilityOnly || p.closure.MetadataComplete {
		t.Fatal(p, err)
	}
	// Simulate an old binary/schema owner changing state. This deliberately
	// does not recreate the removed full-history enabling implementation.
	update := func() error {
		tx, err := s.db.BeginTx(t.Context(), nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := setCompilerInputAdmissionTimeouts(t.Context(), tx); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE fugue_platform_input_history_admission
 SET phase='enforced',plan_sha256=$1,enforced_at=now() WHERE singleton AND phase='capability'`, strings.Repeat("a", 64)); err != nil {
			return err
		}
		return tx.Commit()
	}
	err = s.withPreparedCompilerInputActivation(t.Context(), p, nil, func(context.Context, *sql.Tx) error {
		if err := update(); err == nil {
			t.Fatal("legacy update overtook in-flight compatibility admission")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := update(); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := s.withPreparedCompilerInputActivation(t.Context(), p, nil, func(context.Context, *sql.Tx) error { called = true; return nil }); !errors.Is(err, ErrConflict) || called {
		t.Fatal("stale capability proof crossed persisted state change", err)
	}
	if _, err := s.db.Exec(`DROP TABLE fugue_platform_input_history_admission`); err != nil {
		t.Fatal(err)
	}
	if p, err := s.prepareOrdinaryCompilerInputProof(t.Context(), roots, nil, nil, nil, nil); !errors.Is(err, ErrConflict) || p != nil {
		t.Fatal("missing schema was interpreted as capability compatibility", p, err)
	}
}
