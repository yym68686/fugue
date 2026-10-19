package store

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func testRetentionPolicy() ObservationRetentionPolicy {
	return ObservationRetentionPolicy{Version: 1, Enabled: true, IntervalSeconds: 30, BatchRows: 2, HeartbeatDays: 7, HeartbeatRowsPerChain: 1000, ProbeDays: 7, ProbeRowsPerTable: 10000}
}

func TestObservationRetentionPolicyHasClosedScopeAndProtectedWindows(t *testing.T) {
	p := testRetentionPolicy()
	raw, _ := json.Marshal(p)
	if _, err := ParseObservationRetentionPolicy(raw); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*ObservationRetentionPolicy){func(p *ObservationRetentionPolicy) { p.BatchRows = 1001 }, func(p *ObservationRetentionPolicy) { p.HeartbeatDays = 0 }, func(p *ObservationRetentionPolicy) { p.ProbeDays = 6 }, func(p *ObservationRetentionPolicy) { p.IntervalSeconds = 0 }} {
		p := testRetentionPolicy()
		change(&p)
		raw, _ := json.Marshal(p)
		if _, err := ParseObservationRetentionPolicy(raw); err == nil {
			t.Fatal("unsafe retention accepted", p)
		}
	}
	if _, err := ParseObservationRetentionPolicy(append(raw[:len(raw)-1], []byte(`,"tables":["fugue_apps"]}`)...)); err == nil {
		t.Fatal("config selected business tables")
	}
}

func retentionPostgres(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("FUGUE_RETENTION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable retention database not configured")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("only disposable loopback test databases allowed")
	}
	db, err := openPostgresDatabase(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	schema := fmt.Sprintf("retention_%d", time.Now().UnixNano())
	if _, err = db.Exec("CREATE SCHEMA " + schema + "; SET search_path TO " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DROP SCHEMA " + schema + " CASCADE") })
	for _, statement := range postgresSchemaStatements {
		if strings.HasPrefix(statement, "CREATE TABLE IF NOT EXISTS fugue_audit_events (") || strings.HasPrefix(statement, "CREATE TABLE IF NOT EXISTS fugue_security_audit_chain_state (") || strings.HasPrefix(statement, "CREATE TABLE IF NOT EXISTS fugue_meta (") {
			if _, err = db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, table := range retainedObservationTables {
		if _, err = db.Exec(fmt.Sprintf("CREATE TABLE %s(id text PRIMARY KEY,%s timestamptz NOT NULL)", table.table, table.timestamp)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = db.Exec(`CREATE TABLE fugue_platform_artifacts(id text PRIMARY KEY);INSERT INTO fugue_platform_artifacts VALUES('protected')`); err != nil {
		t.Fatal(err)
	}
	return &Store{db: db, databaseURL: dsn, dbReady: true}
}

func TestObservationRetentionPostgresPreservesBusinessAndVerifiableHeartbeatSuffix(t *testing.T) {
	s := retentionPostgres(t)
	now := time.Now().UTC()
	p := testRetentionPolicy()
	keys := bundleauth.NewKeyring("synthetic-retention-key", "key-a", "", "", nil)
	chain := "platform-consumer-heartbeat:consumer-a"
	var events []model.AuditEvent
	previous := ""
	for i := 1; i <= 5; i++ {
		at := now.Add(-10 * 24 * time.Hour)
		if i == 5 {
			at = now
		}
		e, err := platformsafety.SignTamperEvidentAuditEvent(model.AuditEvent{ID: fmt.Sprintf("heartbeat-%d", i), ActorType: "platform_component", ActorID: "consumer", Action: "platform_consumer.heartbeat_accepted", TargetType: "platform_consumer", TargetID: "consumer-a", ChainID: chain, ChainSequence: int64(i), PreviousHash: previous, CreatedAt: at}, keys)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, e)
		previous = e.EventHash
		metadata, _ := json.Marshal(e.Metadata)
		provenance, _ := json.Marshal(e.Provenance)
		if _, err = s.db.Exec(`INSERT INTO fugue_audit_events(id,actor_type,actor_id,action,target_type,target_id,metadata_json,chain_id,chain_sequence,previous_hash,event_hash,provenance_json,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, e.ID, e.ActorType, e.ActorID, e.Action, e.TargetType, e.TargetID, metadata, e.ChainID, e.ChainSequence, e.PreviousHash, e.EventHash, provenance, e.CreatedAt); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec(`INSERT INTO fugue_security_audit_chain_state VALUES($1,5,$2,$3);`, chain, previous, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO fugue_audit_events(id,actor_type,actor_id,action,target_type,created_at) VALUES('business','user','user','app.delete','app',$1)`, now.Add(-365*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, table := range retainedObservationTables {
		for i := 0; i < 4; i++ {
			at := now.Add(-10 * 24 * time.Hour)
			if i == 3 {
				at = now
			}
			if _, err := s.db.Exec(fmt.Sprintf("INSERT INTO %s VALUES($1,$2)", table.table), fmt.Sprint(i), at); err != nil {
				t.Fatal(err)
			}
		}
	}
	counts, err := s.pruneObservationHistory(context.Background(), p, keys, now)
	if err != nil {
		t.Fatal(err)
	}
	if counts["heartbeat_history"] != 2 {
		t.Fatal("batch bound not enforced", counts)
	}
	var raw string
	if err = s.db.QueryRow(`SELECT value FROM fugue_meta WHERE key=$1`, heartbeatRetentionAnchorPrefix+chain).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var anchor model.AuditEvent
	if json.Unmarshal([]byte(raw), &anchor) != nil || anchor.EventHash != events[1].EventHash || platformsafety.VerifyTamperEvidentAuditEvent(anchor, keys) != nil {
		t.Fatal("signed boundary not retained")
	}
	for _, table := range retainedObservationTables {
		var n int
		if err = s.db.QueryRow("SELECT count(*) FROM " + table.table).Scan(&n); err != nil || n != 2 {
			t.Fatal("bounded observation cleanup failed", table, n, err)
		}
	}
	var preserved int
	if err = s.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_artifacts)+(SELECT count(*) FROM fugue_audit_events WHERE id='business')`).Scan(&preserved); err != nil || preserved != 2 {
		t.Fatal("business data changed", preserved, err)
	}
	// A corrupted retained suffix must roll back both anchor and deletion.
	if _, err = s.db.Exec(`UPDATE fugue_audit_events SET previous_hash='tampered' WHERE id='heartbeat-3'`); err != nil {
		t.Fatal(err)
	}
	conn, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pruneHeartbeatChain(t.Context(), conn, chain, p, keys, now); err == nil {
		t.Fatal("corrupt history pruned")
	}
	conn.Close()
	var after string
	s.db.QueryRow(`SELECT value FROM fugue_meta WHERE key=$1`, heartbeatRetentionAnchorPrefix+chain).Scan(&after)
	if after != raw {
		t.Fatal("failed cleanup advanced anchor")
	}
	if _, err = s.db.Exec(`UPDATE fugue_audit_events SET previous_hash=$1 WHERE id='heartbeat-3'`, events[2].PreviousHash); err != nil {
		t.Fatal(err)
	}
	conn, err = s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	n, err := pruneHeartbeatChain(t.Context(), conn, chain, p, keys, now)
	conn.Close()
	if err != nil || n != 2 {
		t.Fatal("retry failed", n, err)
	}
	var remaining int
	s.db.QueryRow(`SELECT count(*) FROM fugue_audit_events WHERE chain_id=$1`, chain).Scan(&remaining)
	if remaining != 1 {
		t.Fatal("head not preserved")
	}
	s.db.QueryRow(`SELECT value FROM fugue_meta WHERE key=$1`, heartbeatRetentionAnchorPrefix+chain).Scan(&after)
	json.Unmarshal([]byte(after), &anchor)
	if anchor.EventHash != events[4].PreviousHash || platformsafety.VerifyTamperEvidentAuditEvent(anchor, keys) != nil || platformsafety.VerifyTamperEvidentAuditEvent(events[4], keys) != nil {
		t.Fatal("remaining heartbeat suffix no longer verifiable")
	}
	var head int64
	s.db.QueryRow(`SELECT last_sequence FROM fugue_security_audit_chain_state WHERE chain_id=$1`, chain).Scan(&head)
	if head != 5 {
		t.Fatal("retention reset replay protection")
	}
}

func TestObservationRetentionCapacityNeverRemovesProtectedRecentSamples(t *testing.T) {
	s := retentionPostgres(t)
	p := testRetentionPolicy()
	p.ProbeDays = 30
	p.BatchRows = 1000
	now := time.Now().UTC()
	if _, err := s.db.Exec(`INSERT INTO fugue_edge_network_samples SELECT n::text,$1::timestamptz-interval '8 days'+n*interval '1 second' FROM generate_series(1,10010)n`, now); err != nil {
		t.Fatal(err)
	}
	conn, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	n, err := pruneObservationTable(t.Context(), conn, retainedObservationTables[0], p, now)
	conn.Close()
	if err != nil || n != 10 {
		t.Fatal("capacity boundary did not discard only the oldest eligible observations", n, err)
	}
	if _, err = s.db.Exec(`UPDATE fugue_edge_network_samples SET observed_at=$1`, now); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`INSERT INTO fugue_edge_network_samples VALUES('extra',$1)`, now); err != nil {
		t.Fatal(err)
	}
	conn, err = s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	n, err = pruneObservationTable(t.Context(), conn, retainedObservationTables[0], p, now)
	conn.Close()
	if err != nil || n != 0 {
		t.Fatal("row budget overrode protected recent evidence", n, err)
	}
}
