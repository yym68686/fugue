package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"

	"github.com/jackc/pgx/v5/pgconn"
)

func consumerRetirementFixture(t *testing.T) (*sql.DB, string, PlatformConsumerRetirementContract) {
	t.Helper()
	db, address, _ := hotInventoryTestDatabase(t)
	for range 2 {
		if err := schemamigrate.MigratePlatformConsumerRetirements(t.Context(), address); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED", "true")
	c := PlatformConsumerRetirementContract{Version: 1, Actor: "test-operator", Reason: "Explicitly abandon the old identity's recovery contract", PermanentlyAbandonRecovery: true,
		Targets: []PlatformConsumerRetirementTarget{{"edge-worker:neutral-host", "edge_route_bundle", "global"}, {"edge-worker:neutral-host", "caddy_route_config", "global"}}}
	for _, target := range c.Targets {
		if _, err := db.Exec(`INSERT INTO fugue_platform_consumer_instances(id,consumer_id,artifact_kind,scope_key,actual_generation,lkg_generation,last_heartbeat_at,updated_at)
 VALUES($1,$2,$3,$4,'old-actual','old-lkg',now()-interval '2 days',now()-interval '2 days')`, "old-"+target.ArtifactKind, target.ConsumerID, target.ArtifactKind, target.ScopeKey); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO fugue_platform_consumer_instances(id,consumer_id,artifact_kind,scope_key,actual_generation,last_heartbeat_at,updated_at)
 VALUES('new-authority','edge-worker:neutral-host','edge_route_bundle','authority-cell:cell-neutral','current-generation',now(),now());
 INSERT INTO fugue_platform_shadow_consumers(consumer_id,artifact_kind,scope_key,body)
 SELECT consumer_id,artifact_kind,scope_key,to_jsonb(c) FROM fugue_platform_consumer_instances c WHERE id='old-edge_route_bundle'`); err != nil {
		t.Fatal(err)
	}
	return db, address, c
}

func retirementCounts(t *testing.T, db *sql.DB) [4]int64 {
	t.Helper()
	var counts [4]int64
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_consumer_instances),(SELECT count(*) FROM fugue_platform_shadow_consumers),(SELECT count(*) FROM fugue_platform_consumer_retirements),(SELECT epoch FROM fugue_platform_consumer_retirement_epoch WHERE singleton)`).Scan(&counts[0], &counts[1], &counts[2], &counts[3]); err != nil {
		t.Fatal(err)
	}
	return counts
}

func TestConsumerRetirementPostgresRecoveryEligibilityAndInsertRejection(t *testing.T) {
	db, address, c := consumerRetirementFixture(t)
	s := &Store{db: db, databaseURL: address}
	set := expectedConsumerSetFixture(t, 1)
	target := c.Targets[1]
	otherScope := target
	otherScope.ScopeKey = "authority-cell:cell-neutral"
	otherKind := target
	otherKind.ArtifactKind = model.PlatformArtifactKindDNSAnswerBundle
	keys := []PlatformConsumerRetirementTarget{target, c.Targets[0], otherScope, otherKind}
	if retired, err := s.RetiredPlatformConsumerTargets(t.Context(), keys); err != nil || len(retired) != 0 {
		t.Fatal("live slots counted as retired", retired, err)
	}
	p, err := RunPlatformConsumerRetirement(t.Context(), address, c, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunPlatformConsumerRetirement(t.Context(), address, c, true, p.PlanSHA256); err != nil {
		t.Fatal(err)
	}
	if retired, err := s.RetiredPlatformConsumerTargets(t.Context(), keys); err != nil || len(retired) != 2 || !retired[target] || !retired[c.Targets[0]] {
		t.Fatal("retirement lookup did not match only exact enrollment keys", retired, err)
	}
	survivor := set.Consumers[0]
	set.Consumers[0].ConsumerID = target.ConsumerID
	if _, err := pgInsertPlatformExpectedConsumerSet(t.Context(), db, set); !errors.Is(err, ErrPlatformConsumerRetired) || errors.Is(err, ErrConflict) {
		t.Fatal("database fence did not report a distinct permanent rejection", err)
	}
	set.Consumers[0] = survivor
	if _, err := pgInsertPlatformExpectedConsumerSet(t.Context(), db, set); err != nil {
		t.Fatal("database fence blocked surviving membership", err)
	}
	if _, err := pgInsertPlatformExpectedConsumerSet(t.Context(), db, set); !errors.Is(err, ErrConflict) || errors.Is(err, ErrPlatformConsumerRetired) {
		t.Fatal("real uniqueness conflict was misclassified", err)
	}
}

func TestConsumerRetirementPostgresExactAtomicAuditedAndIdempotent(t *testing.T) {
	db, address, c := consumerRetirementFixture(t)
	before := retirementCounts(t, db)
	preview, err := RunPlatformConsumerRetirement(t.Context(), address, c, false, "")
	if err != nil || preview.Applied || len(preview.Targets) != 2 || before != retirementCounts(t, db) {
		t.Fatal("preview changed state", preview, err)
	}
	r, err := RunPlatformConsumerRetirement(t.Context(), address, c, true, preview.PlanSHA256)
	if err != nil || !r.Applied || r.Retired != 2 || r.HistoryDeleted || r.NodeExecutionFenced || retirementCounts(t, db) != [4]int64{1, 0, 2, 2} {
		t.Fatal("exact retirement failed", r, err, retirementCounts(t, db))
	}
	var audited int
	if err = db.QueryRow(`SELECT count(*) FROM fugue_platform_consumer_retirements WHERE consumer_snapshot->>'actual_generation'='old-actual' AND consumer_snapshot->>'lkg_generation'='old-lkg' AND actor=$1 AND reason=$2 AND recovery_permanently_abandoned`, c.Actor, c.Reason).Scan(&audited); err != nil || audited != 2 {
		t.Fatal("old facts were lost or rewritten", audited, err)
	}
	for range 2 {
		if err = schemamigrate.MigratePlatformConsumerRetirements(t.Context(), address); err != nil {
			t.Fatal("migration retry", err)
		}
	}
	r, err = RunPlatformConsumerRetirement(t.Context(), address, c, true, preview.PlanSHA256)
	if err != nil || !r.Applied || r.PlanSHA256 != preview.PlanSHA256 || !r.Targets[0].AlreadyRetired || retirementCounts(t, db) != [4]int64{1, 0, 2, 2} {
		t.Fatal("exact retry not idempotent", r, err)
	}
	// A different scope on the same physical host is not retired.
	if _, err = db.Exec(`UPDATE fugue_platform_consumer_instances SET sequence=sequence+1 WHERE id='new-authority'`); err != nil {
		t.Fatal("unrelated scope was fenced", err)
	}
	for _, q := range []string{
		`INSERT INTO fugue_platform_consumer_instances SELECT (jsonb_populate_record(NULL::fugue_platform_consumer_instances,consumer_snapshot)).* FROM fugue_platform_consumer_retirements WHERE artifact_kind='edge_route_bundle'`,
		`INSERT INTO fugue_platform_shadow_consumers(consumer_id,artifact_kind,scope_key,body) SELECT consumer_id,artifact_kind,scope_key,consumer_snapshot FROM fugue_platform_consumer_retirements WHERE artifact_kind='edge_route_bundle'`,
		`UPDATE fugue_platform_consumer_instances SET scope_key='global' WHERE id='new-authority'`,
		`UPDATE fugue_platform_consumer_retirements SET reason='reversed'`,
		`DELETE FROM fugue_platform_consumer_retirements`,
		`TRUNCATE fugue_platform_consumer_retirements`,
		`UPDATE fugue_platform_consumer_retirement_epoch SET epoch=0`,
		`DELETE FROM fugue_platform_consumer_retirement_epoch`,
		`TRUNCATE fugue_platform_consumer_retirement_epoch`,
	} {
		_, err = db.Exec(q)
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "40001" {
			t.Fatal("permanent retirement bypassed", q, err)
		}
	}
	// ALWAYS triggers also fence old writers using replica session mode.
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`SET LOCAL session_replication_role='replica'`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO fugue_platform_consumer_instances SELECT (jsonb_populate_record(NULL::fugue_platform_consumer_instances,consumer_snapshot)).* FROM fugue_platform_consumer_retirements WHERE artifact_kind='edge_route_bundle'`); err == nil {
		t.Fatal("replica writer recreated retired identity")
	}
}

func TestConsumerRetirementPostgresStalePlanRollsBackWholeBatch(t *testing.T) {
	for _, change := range []string{"heartbeat", "shadow", "reason", "missing", "target-set", "disabled-guard"} {
		t.Run(change, func(t *testing.T) {
			db, address, c := consumerRetirementFixture(t)
			p, err := RunPlatformConsumerRetirement(t.Context(), address, c, false, "")
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "heartbeat":
				_, err = db.Exec(`UPDATE fugue_platform_consumer_instances SET sequence=sequence+1 WHERE id='old-edge_route_bundle'`)
			case "shadow":
				_, err = db.Exec(`UPDATE fugue_platform_shadow_consumers SET body=body||'{"sequence":42}'::jsonb`)
			case "reason":
				c.Reason += " changed"
			case "missing":
				_, err = db.Exec(`DELETE FROM fugue_platform_consumer_instances WHERE id='old-edge_route_bundle'`)
			case "target-set":
				c.Targets = c.Targets[:1]
			case "disabled-guard":
				_, err = db.Exec(`ALTER TABLE fugue_platform_shadow_consumers DISABLE TRIGGER fugue_retired_consumer_root`)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := retirementCounts(t, db)
			if r, err := RunPlatformConsumerRetirement(t.Context(), address, c, true, p.PlanSHA256); err == nil || r.Applied || before != retirementCounts(t, db) {
				t.Fatal("stale plan committed a prefix", r, err)
			}
		})
	}
}

func seedRetirementExpectation(t *testing.T, db *sql.DB, c PlatformConsumerRetirementContract) {
	t.Helper()
	members, _ := json.Marshal([]map[string]string{{"consumer_id": c.Targets[0].ConsumerID}})
	queries := []string{
		`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation,status,content_hash,created_at,updated_at) VALUES('neutral-artifact','edge_route_bundle','global','old','validated','neutral-hash',now(),now())`,
		`INSERT INTO fugue_platform_artifact_releases(id,artifact_id,artifact_kind,scope_key,generation,release_channel,status,released_at,created_at,updated_at) VALUES('neutral-release','neutral-artifact','edge_route_bundle','global','old','full','superseded',now(),now(),now())`,
	}
	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO fugue_platform_expected_consumer_sets(id,artifact_release_id,artifact_kind,scope_key,expected_generation,topology_revision,revision,heartbeat_deadline,convergence_deadline,consumers_json,created_at,updated_at)
 VALUES('neutral-expectation','neutral-release','edge_route_bundle','global','old','neutral-topology',1,now(),now(),$1::jsonb,now(),now())`, string(members)); err != nil {
		t.Fatal(err)
	}
}

func TestConsumerRetirementPostgresActiveTopologyAndReactivationAreFenced(t *testing.T) {
	db, address, c := consumerRetirementFixture(t)
	seedRetirementExpectation(t, db, c)
	p, err := RunPlatformConsumerRetirement(t.Context(), address, c, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE fugue_platform_artifact_releases SET status='active' WHERE id='neutral-release'`); err != nil {
		t.Fatal(err)
	}
	before := retirementCounts(t, db)
	if _, err = RunPlatformConsumerRetirement(t.Context(), address, c, true, p.PlanSHA256); !errors.Is(err, ErrConflict) || before != retirementCounts(t, db) {
		t.Fatal("retirement silently changed active topology", err)
	}
	if _, err = db.Exec(`UPDATE fugue_platform_artifact_releases SET status='superseded' WHERE id='neutral-release'`); err != nil {
		t.Fatal(err)
	}
	if _, err = RunPlatformConsumerRetirement(t.Context(), address, c, true, p.PlanSHA256); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`UPDATE fugue_platform_artifact_releases SET status='active' WHERE id='neutral-release'`,
		`INSERT INTO fugue_platform_release_lanes(lane_key,artifact_kind,scope_key,release_channel,active_release_id,updated_at) VALUES('neutral-lane','edge_route_bundle','global','full','neutral-release',now())`,
		`INSERT INTO fugue_platform_expected_consumer_sets SELECT 'new-expectation',release_set_id,artifact_release_id,artifact_kind,scope_key,scope_json,expected_generation,topology_revision,revision+1,requires_consumers,required_cardinality,optional_cardinality,heartbeat_deadline,convergence_deadline,consumers_json,created_at,updated_at FROM fugue_platform_expected_consumer_sets WHERE id='neutral-expectation'`,
	} {
		if _, err = db.Exec(q); err == nil {
			t.Fatal("old expectation reactivated", q)
		}
	}
}

func TestConsumerRetirementPostgresShadowIdentityDoesNotRetirePublication(t *testing.T) {
	db, address, c := consumerRetirementFixture(t)
	seedRetirementExpectation(t, db, c)
	if _, err := db.Exec(`UPDATE fugue_platform_artifact_releases SET release_channel='shadow',status='active' WHERE id='neutral-release';
 INSERT INTO fugue_platform_release_lanes(lane_key,artifact_kind,scope_key,release_channel,active_release_id,updated_at) VALUES('neutral-shadow','edge_route_bundle','global','shadow','neutral-release',now())`); err != nil {
		t.Fatal(err)
	}
	p, err := RunPlatformConsumerRetirement(t.Context(), address, c, false, "")
	if err != nil || len(p.Targets) != 2 || len(p.Targets[1].RetainedShadowExpectations) != 1 {
		t.Fatal("non-serving shadow ownership hidden", p, err)
	}
	r, err := RunPlatformConsumerRetirement(t.Context(), address, c, true, p.PlanSHA256)
	if err != nil || !r.Applied {
		t.Fatal(r, err)
	}
	var status, selected string
	if err = db.QueryRow(`SELECT r.status,l.active_release_id FROM fugue_platform_artifact_releases r JOIN fugue_platform_release_lanes l ON l.active_release_id=r.id WHERE r.id='neutral-release'`).Scan(&status, &selected); err != nil || status != "active" || selected != "neutral-release" {
		t.Fatal("identity retirement changed publication", status, selected, err)
	}
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	roots, err := compilerInputHotRoots(t.Context(), tx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, root := range roots {
		if root.Source == "active-expectation" && root.ID == "neutral-expectation" {
			found = true
		}
	}
	if !found {
		t.Fatal("independent shadow retention disappeared")
	}
}

func TestConsumerRetirementPostgresWaitsForAdmittedWriter(t *testing.T) {
	db, address, c := consumerRetirementFixture(t)
	p, err := RunPlatformConsumerRetirement(t.Context(), address, c, false, "")
	if err != nil {
		t.Fatal(err)
	}
	w, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Rollback()
	if _, err = w.Exec(`UPDATE fugue_platform_consumer_instances SET sequence=sequence+1 WHERE id='old-edge_route_bundle'`); err != nil {
		t.Fatal(err)
	}
	if r, err := RunPlatformConsumerRetirement(t.Context(), address, c, true, p.PlanSHA256); err == nil || r.Applied {
		t.Fatal("retirement overtook an admitted writer", r, err)
	}
	if err = w.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = RunPlatformConsumerRetirement(t.Context(), address, c, true, p.PlanSHA256); !errors.Is(err, ErrConflict) {
		t.Fatal("pre-writer snapshot reused", err)
	}
}

func TestConsumerRetirementPostgresOldSnapshotCannotRecreateIdentity(t *testing.T) {
	db, address, c := consumerRetirementFixture(t)
	p, err := RunPlatformConsumerRetirement(t.Context(), address, c, false, "")
	if err != nil {
		t.Fatal(err)
	}
	w, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Rollback()
	var epoch int64
	if err = w.QueryRow(`SELECT epoch FROM fugue_platform_consumer_retirement_epoch WHERE singleton`).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	if _, err = RunPlatformConsumerRetirement(t.Context(), address, c, true, p.PlanSHA256); err != nil {
		t.Fatal(err)
	}
	_, err = w.Exec(`INSERT INTO fugue_platform_consumer_instances(id,consumer_id,artifact_kind,scope_key,last_heartbeat_at,updated_at) VALUES('reconnect','edge-worker:neutral-host','edge_route_bundle','global',now(),now())`)
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "40001" {
		t.Fatal("old snapshot escaped persistent identity fence", err)
	}
}

func TestConsumerRetirementPostgresLateFailureCannotCommitPrefix(t *testing.T) {
	db, address, c := consumerRetirementFixture(t)
	p, err := RunPlatformConsumerRetirement(t.Context(), address, c, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE FUNCTION neutral_retirement_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.artifact_kind='edge_route_bundle' THEN RAISE EXCEPTION 'neutral late failure'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER neutral_failure AFTER INSERT ON fugue_platform_consumer_retirements FOR EACH ROW EXECUTE FUNCTION neutral_retirement_failure()`); err != nil {
		t.Fatal(err)
	}
	before := retirementCounts(t, db)
	if r, err := RunPlatformConsumerRetirement(t.Context(), address, c, true, p.PlanSHA256); err == nil || r.Applied || retirementCounts(t, db) != before {
		t.Fatal("late failure committed an identity/epoch prefix", r, err)
	}
}

func TestConsumerRetirementPostgresNativeRoots(t *testing.T) {
	db, address, c := consumerRetirementFixture(t)
	p, err := RunPlatformConsumerRetirement(t.Context(), address, c, false, "")
	if err != nil {
		t.Fatal("ordinary read-only preview", err)
	}
	if _, err = RunPlatformConsumerRetirement(t.Context(), address, c, true, p.PlanSHA256); err != nil {
		t.Fatal("identity retirement", err)
	}
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	roots, err := compilerInputHotRoots(t.Context(), tx)
	if err != nil {
		t.Fatal(err)
	}
	foundNew := false
	for _, root := range roots {
		if root.ID == "old-edge_route_bundle" || root.ID == "old-caddy_route_config" || strings.Contains(root.ID, "neutral-host") {
			t.Fatal("audit snapshot became a retention root", root)
		}
		if root.ID == "new-authority" {
			foundNew = true
		}
	}
	if !foundNew {
		t.Fatal("unrelated live root disappeared")
	}
}

func TestConsumerRetirementPostgresOrdinaryRole(t *testing.T) {
	db, address, c := consumerRetirementFixture(t)
	var schema string
	if err := db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	role := model.NewID("consumer_retire_operator_test")
	quotedRole := quotePostgresIntegrationIdentifier(t, role)
	quotedSchema := quotePostgresIntegrationIdentifier(t, schema)
	password := postgresFixturePassword()
	if _, err := db.Exec("CREATE ROLE " + quotedRole + " LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB PASSWORD '" + password + "'"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, q := range []string{"DROP OWNED BY " + quotedRole, "DROP ROLE " + quotedRole} {
			if _, err := db.Exec(q); err != nil {
				t.Error(err)
			}
		}
	})
	if _, err := db.Exec("GRANT USAGE ON SCHEMA " + quotedSchema + " TO " + quotedRole + "; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA " + quotedSchema + " TO " + quotedRole); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(address)
	u.User = url.UserPassword(role, password)
	p, err := RunPlatformConsumerRetirement(t.Context(), u.String(), c, false, "")
	if err != nil {
		t.Fatal("ordinary read-only preview", err)
	}
	if _, err = RunPlatformConsumerRetirement(t.Context(), u.String(), c, true, p.PlanSHA256); err != nil {
		t.Fatal("ordinary role retirement", err)
	}
}

func TestConsumerRetirementRequiresExplicitExactConsent(t *testing.T) {
	good := PlatformConsumerRetirementContract{1, "neutral-operator", "explicit recovery abandonment", true, []PlatformConsumerRetirementTarget{{"edge-worker:neutral", "edge_route_bundle", "global"}}}
	for _, change := range []string{"consent", "empty", "duplicate", "scope", "actor", "wildcard"} {
		c := good
		c.Targets = append([]PlatformConsumerRetirementTarget(nil), good.Targets...)
		switch change {
		case "consent":
			c.PermanentlyAbandonRecovery = false
		case "empty":
			c.Targets = nil
		case "duplicate":
			c.Targets = append(c.Targets, c.Targets[0])
		case "scope":
			c.Targets[0].ScopeKey = "GLOBAL"
		case "actor":
			c.Actor = " "
		case "wildcard":
			c.Targets[0].ConsumerID = ""
		}
		if c.Validate() == nil {
			t.Fatal("invalid contract accepted", change)
		}
	}
	if _, err := RunPlatformConsumerRetirement(t.Context(), "invalid", good, true, strings.Repeat("a", 64)); !errors.Is(err, ErrInvalidInput) {
		t.Fatal("apply without exact preview SHA accepted", err)
	}
}
