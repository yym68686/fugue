package schemamigrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type jsonArgument struct {
	want map[string]any
}

func (argument jsonArgument) Match(value driver.Value) bool {
	bytes, ok := value.([]byte)
	if !ok {
		if stringValue, ok := value.(string); ok {
			bytes = []byte(stringValue)
		}
	}
	var got map[string]any
	return json.Unmarshal(bytes, &got) == nil && got["token_hash"] == argument.want["token_hash"] && got["id"] == argument.want["id"]
}

func TestLegacyInstanceUIDIsStableAndScoped(t *testing.T) {
	if got, want := legacyInstanceUID(" EDGE-US-1 "), "legacy-ea2a7ddce9bff1ccacb262b8"; got != want {
		t.Fatalf("legacy instance uid = %q, want %q", got, want)
	}
	if legacyInstanceUID("edge-us-1") != legacyInstanceUID(" EDGE-US-1 ") {
		t.Fatal("legacy instance uid must normalize edge identity")
	}
}

func TestCopyLegacyEdgeInstancesRedactsCredentialAndIsIdempotent(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT n.id, n.edge_group_id, to_jsonb(n), n.last_heartbeat_at
FROM fugue_edge_nodes AS n
LEFT JOIN fugue_edge_node_instances AS i
  ON i.edge_id=n.id AND i.edge_group_id=n.edge_group_id AND i.slot=$1 AND i.release_epoch=$2
WHERE i.edge_id IS NULL
ORDER BY n.id FOR UPDATE OF n`)).
		WithArgs(edgeLegacyMigrationSlot, edgeLegacyMigrationEpoch).
		WillReturnRows(sqlmock.NewRows([]string{"id", "edge_group_id", "node_json", "last_heartbeat_at"}).
			AddRow("edge-us-1", "group-us", []byte(`{"id":"edge-us-1","token_hash":"secret"}`), nil))
	mock.ExpectExec(`INSERT INTO fugue_edge_node_instances \(\s*edge_id, edge_group_id, slot, instance_uid, release_epoch`).
		WithArgs("edge-us-1", "group-us", edgeLegacyMigrationSlot, legacyInstanceUID("edge-us-1"), edgeLegacyMigrationEpoch,
			jsonArgument{want: map[string]any{"id": "edge-us-1", "token_hash": ""}}, nil).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	tx, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyLegacyEdgeInstances(context.Background(), tx); err != nil {
		t.Fatalf("copyLegacyEdgeInstances: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEdgeInstanceFencingReceiptRoundTripsWithoutSecrets(t *testing.T) {
	receipt := EdgeInstanceFencingReceipt{
		Schema: edgeInstanceFencingReceiptSchema, Marker: edgeInstanceFencingSchema,
		LegacyRowCount: 5, MigratedRowCount: 5, ActiveEpochCount: 0,
		ActivationPhase: edgeActivationLegacyPhase, ActivationGeneration: 19,
		InstanceUIDAlgorithm: "sha256(normalized-edge-id)-first-12-bytes", RecordedAt: time.Unix(1, 0).UTC(),
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) == "" {
		t.Fatal("receipt JSON is empty")
	}
	var decoded EdgeInstanceFencingReceipt
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != receipt {
		t.Fatalf("receipt round trip changed value: %#v != %#v", decoded, receipt)
	}
	if string(raw) == "secret" {
		t.Fatal("receipt unexpectedly contains credential material")
	}
}

func TestEdgeInstanceFencingMigrationLive(t *testing.T) {
	databaseURL := os.Getenv("FUGUE_EDGE_SCHEMA_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set FUGUE_EDGE_SCHEMA_TEST_DATABASE_URL to run the live edge-instance migration test")
	}
	if err := MigrateEdgeInstanceFencing(context.Background(), databaseURL); err != nil {
		t.Fatalf("first edge-instance schema migration: %v", err)
	}
	if err := MigrateEdgeInstanceFencing(context.Background(), databaseURL); err != nil {
		t.Fatalf("idempotent edge-instance schema migration: %v", err)
	}
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var marker, receipt string
	if err := database.QueryRow(`SELECT value FROM fugue_meta WHERE key=$1`, edgeInstanceFencingMetaKey).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRow(`SELECT value FROM fugue_meta WHERE key=$1`, edgeInstanceFencingReceiptKey).Scan(&receipt); err != nil {
		t.Fatal(err)
	}
	if marker != edgeInstanceFencingSchema {
		t.Fatalf("marker = %q, want %q", marker, edgeInstanceFencingSchema)
	}
	var decoded EdgeInstanceFencingReceipt
	if err := json.Unmarshal([]byte(receipt), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.LegacyRowCount != decoded.MigratedRowCount || decoded.LegacyRowCount == 0 {
		t.Fatalf("receipt counts = legacy %d migrated %d", decoded.LegacyRowCount, decoded.MigratedRowCount)
	}
	var tokenHash string
	if err := database.QueryRow(`SELECT node_json->>'token_hash' FROM fugue_edge_node_instances WHERE slot=$1`, edgeLegacyMigrationSlot).Scan(&tokenHash); err != nil {
		t.Fatal(err)
	}
	if tokenHash != "" {
		t.Fatalf("migrated token hash = %q, want empty", tokenHash)
	}
}
