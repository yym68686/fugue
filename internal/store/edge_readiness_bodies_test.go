package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
	"github.com/DATA-DOG/go-sqlmock"
)

type edgeReadinessCountingReader struct {
	sqlQueryer
	bodyQueries int
}

func (r *edgeReadinessCountingReader) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if strings.Contains(query, ", node_json::text FROM") {
		r.bodyQueries++
	}
	return r.sqlQueryer.QueryContext(ctx, query, args...)
}

func TestEdgeReadinessBodiesRejectMissingOrDifferentContent(t *testing.T) {
	for _, scenario := range []string{"missing", "changed", "foreign", "duplicate", "invalid_json", "reference_failure"} {
		t.Run(scenario, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			body := []byte(`{"id":"edge-a","edge_group_id":"edge-group-a","status":"healthy"}`)
			if scenario == "invalid_json" {
				body = []byte(`{"healthy":"not-a-boolean"}`)
			}
			h := sha256.Sum256(body)
			now := time.Now().UTC()
			refs := sqlmock.NewRows([]string{"edge", "group", "slot", "uid", "epoch", "bytes", "hash", "failure", "healthy", "good", "bad", "since", "heartbeat", "created", "updated"}).AddRow("edge-a", "edge-group-a", "a", "pod-a", "release-a", len(body), hex.EncodeToString(h[:]), "", true, 2, 0, now, now, now, now)
			if scenario == "reference_failure" {
				refs.RowError(0, context.Canceled)
			}
			mock.ExpectQuery(regexp.QuoteMeta(edgeReadinessReferencesSQL)).WillReturnRows(refs).RowsWillBeClosed()
			if scenario != "reference_failure" {
				rows := sqlmock.NewRows([]string{"edge", "group", "slot", "uid", "epoch", "body"})
				if scenario != "missing" {
					id := "edge-a"
					if scenario == "foreign" {
						id = "edge-b"
					}
					raw := body
					if scenario == "changed" {
						raw = []byte(`{"healthy":false}`)
					}
					rows.AddRow(id, "edge-group-a", "a", "pod-a", "release-a", raw)
					if scenario == "duplicate" {
						rows.AddRow(id, "edge-group-a", "a", "pod-a", "release-a", raw)
					}
				}
				mock.ExpectQuery("SELECT edge_id, edge_group_id, slot, instance_uid, release_epoch, node_json::text").WithArgs("edge-a", "edge-group-a", "a", "pod-a", "release-a").WillReturnRows(rows).RowsWillBeClosed()
			}
			if _, err := pgReadEdgeReadinessInstances(t.Context(), db, newObservationPageBodyCache(1<<20, 10)); err == nil {
				t.Fatal("unsafe readiness body accepted")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEdgeReadinessBodiesBoundColdBatchAndSurviveEviction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	refs := sqlmock.NewRows([]string{"edge", "group", "slot", "uid", "epoch", "bytes", "hash", "failure", "healthy", "good", "bad", "since", "heartbeat", "created", "updated"})
	now := time.Now().UTC()
	bodies := make([][]byte, 129)
	for i := range bodies {
		bodies[i] = []byte(fmt.Sprintf(`{"id":"edge-a","edge_group_id":"edge-group-a","status":"healthy","tls_last_message":"%d"}`, i))
		h := sha256.Sum256(bodies[i])
		refs.AddRow("edge-a", "edge-group-a", "a", fmt.Sprintf("pod-%03d", i), "release-a", len(bodies[i]), hex.EncodeToString(h[:]), "", true, 2, 0, now, now, now, now)
	}
	mock.ExpectQuery(regexp.QuoteMeta(edgeReadinessReferencesSQL)).WillReturnRows(refs).RowsWillBeClosed()
	for first := 0; first < len(bodies); first += 128 {
		rows := sqlmock.NewRows([]string{"edge", "group", "slot", "uid", "epoch", "body"})
		for i := first; i < min(first+128, len(bodies)); i++ {
			rows.AddRow("edge-a", "edge-group-a", "a", fmt.Sprintf("pod-%03d", i), "release-a", bodies[i])
		}
		lastParameter := 5 * min(128, len(bodies)-first)
		mock.ExpectQuery(`SELECT edge_id, edge_group_id, slot, instance_uid, release_epoch, node_json::text.*\$` + fmt.Sprint(lastParameter) + `\)\)$`).WillReturnRows(rows).RowsWillBeClosed()
	}
	cache := newObservationPageBodyCache(1024, 1)
	got, err := pgReadEdgeReadinessInstances(t.Context(), db, cache)
	if err != nil || len(got) != 129 {
		t.Fatal("cold batching lost members", len(got), err)
	}
	for i, v := range got {
		if v.InstanceUID != fmt.Sprintf("pod-%03d", i) || v.Node.TLSLastMessage != fmt.Sprint(i) {
			t.Fatal("body eviction changed caller result")
		}
	}
	if len(cache.entries) != 1 || cache.bytes > 1024 {
		t.Fatal("cache exceeded budget")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(edgeReadinessReferencesSQL, "sha256(convert_to(node_json::text,'UTF8'))") || strings.Contains(edgeReadinessReferencesSQL, "node_json,") {
		t.Fatal("readiness cache key is not bound to actual current body bytes")
	}
}

func TestEdgeReadinessBodiesPostgresPreserveSnapshotAndValidation(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.ensureDatabaseReady(); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateEdgeInstanceFencing(t.Context(), s.databaseURL); err != nil {
		t.Fatal(err)
	}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	suffix := strings.ToLower(model.NewID("readiness"))
	id, group := "edge-"+suffix, "edge-group-"+suffix
	if _, _, err := s.UpdateEdgeHeartbeat(model.EdgeNode{ID: id, EdgeGroupID: group, Healthy: true, Status: model.EdgeHealthHealthy}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resetPostgresEdgeActivationTestState(t, s, group) })
	instance := healthyEdgeTestInstance(id, group, model.EdgeSlotA, "pod-a", "release-a")
	instance.Node.TLSLastMessage = strings.Repeat("x", 32<<10)
	if _, err := s.UpdateEdgeInstanceHeartbeat(instance); err != nil {
		t.Fatal(err)
	}
	cache := newObservationPageBodyCache(1<<20, 100)
	read := func() ([]model.EdgeNodeInstance, int) {
		t.Helper()
		tx, err := s.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		want, err := pgReadEdgeNodeInstances(t.Context(), tx, "")
		if err != nil {
			t.Fatal(err)
		}
		reader := &edgeReadinessCountingReader{sqlQueryer: tx}
		got, err := pgReadEdgeReadinessInstances(t.Context(), reader, cache)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("cached readiness changed full inventory")
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		return got, reader.bodyQueries
	}
	_, cold := read()
	got, warm := read()
	if cold != 1 || warm != 0 {
		t.Fatalf("cold=%d warm=%d", cold, warm)
	}
	for i := range got {
		got[i].Node.TLSLastMessage = "caller mutation"
	}
	if _, queries := read(); queries != 0 {
		t.Fatal("caller mutation broke cache")
	}
	// Keep version/timestamp columns unchanged: only the actual body digest may
	// authorize reuse. Mutable metadata must also be read on a warm cache hit.
	if _, err := s.db.Exec(`UPDATE fugue_edge_node_instances SET node_json=jsonb_set(node_json,'{tls_last_message}','"changed without timestamp"'::jsonb) WHERE edge_id=$1 AND slot='a'`, id); err != nil {
		t.Fatal(err)
	}
	if _, queries := read(); queries != 1 {
		t.Fatal("body mutation reused old cache")
	}
	if _, err := s.db.Exec(`UPDATE fugue_edge_node_instances SET effective_healthy=false,consecutive_healthy=0,failure_class=$2 WHERE edge_id=$1 AND slot='a'`, id, model.EdgeInstanceFailureSignatureInvalid); err != nil {
		t.Fatal(err)
	}
	got, queries := read()
	if queries != 0 {
		t.Fatal("unchanged body retransferred for fresh metadata")
	}
	for _, i := range got {
		if i.EdgeID == id && i.Slot == "a" && (i.EffectiveHealthy || i.FailureClass != model.EdgeInstanceFailureSignatureInvalid) {
			t.Fatal("stale health or fence metadata")
		}
	}
	// Freeze a database snapshot, then change the same body from a second
	// connection. A cold lookup must still return the earlier snapshot bytes.
	tx, err := s.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	before, err := pgReadEdgeNodeInstances(t.Context(), tx, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_edge_node_instances SET node_json=jsonb_set(node_json,'{tls_last_message}','"newer snapshot"'::jsonb) WHERE edge_id=$1 AND slot='a'`, id); err != nil {
		t.Fatal(err)
	}
	frozen, err := pgReadEdgeReadinessInstances(t.Context(), tx, newObservationPageBodyCache(1<<20, 1))
	if err != nil || !reflect.DeepEqual(before, frozen) {
		t.Fatal("body read mixed database snapshots", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	read()
	if _, err := s.db.Exec(`DELETE FROM fugue_edge_node_instances WHERE edge_id=$1 AND slot='a'`, id); err != nil {
		t.Fatal(err)
	}
	got, _ = read()
	for _, i := range got {
		if i.EdgeID == id && i.Slot == "a" {
			t.Fatal("deleted instance retained by cache")
		}
	}
	// Invalid nested types still fail even when identity/timestamps are valid.
	if _, err := s.UpdateEdgeInstanceHeartbeat(instance); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_edge_node_instances SET node_json='{"healthy":"invalid"}'::jsonb WHERE edge_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := pgReadEdgeNodeInstances(t.Context(), s.db, ""); err == nil {
		t.Fatal("baseline accepted invalid JSON field")
	}
	if _, err := pgReadEdgeReadinessInstances(t.Context(), s.db, cache); err == nil {
		t.Fatal("cached reader accepted invalid JSON field")
	}
	// The cache retains bytes only, never mutable parsed nodes.
	for _, entry := range cache.entries {
		var node model.EdgeNode
		if err := json.Unmarshal(entry.Value.(observationPageBodyEntry).body, &node); err == nil && node.TLSLastMessage == "caller mutation" {
			t.Fatal("caller modified cached bytes")
		}
	}
}
