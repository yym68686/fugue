package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresMigrationReceiptDistinguishesNewRegistrationFromLostLegacyMapping(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL for the isolated PostgreSQL receipt test")
	}
	db, err := openPostgresDatabase(address)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, statement := range []string{
		`CREATE TEMP TABLE fugue_meta (key text PRIMARY KEY, value text) ON COMMIT DROP`,
		`CREATE TEMP TABLE fugue_edge_nodes (id text, edge_group_id text, created_at timestamptz) ON COMMIT DROP`,
		`CREATE TEMP TABLE fugue_edge_node_instances (edge_id text, edge_group_id text, slot text, release_epoch text) ON COMMIT DROP`,
	} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_meta VALUES ($1,$2),($3,$4)`, edgeInstanceFencingMetaKey, model.EdgeInstanceFencingSchemaV1, edgeInstanceFencingReceiptKey, testEdgeInstanceMigrationReceipt()); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_edge_nodes VALUES ('old-node','edge-group-fixture',$1),('new-node','edge-group-fixture',$2)`, time.Unix(0, 0), time.Unix(2, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_edge_node_instances VALUES ('old-node','edge-group-fixture',$1,$2)`, edgeLegacyMigrationSlot, edgeLegacyMigrationEpoch); err != nil {
		t.Fatal(err)
	}
	if err = verifyEdgeInstanceMigrationReceipt(ctx, tx); err != nil {
		t.Fatalf("post-migration registration blocked startup: %v", err)
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM fugue_edge_node_instances WHERE edge_id='old-node'`); err != nil {
		t.Fatal(err)
	}
	if err = verifyEdgeInstanceMigrationReceipt(ctx, tx); !errors.Is(err, ErrEdgeInstanceFencingNotReady) {
		t.Fatalf("missing historical mapping was not rejected: %v", err)
	}
}

func testEdgeInstanceMigrationReceipt() string {
	raw, _ := json.Marshal(edgeInstanceMigrationReceipt{
		Schema: edgeInstanceFencingReceiptSchema, Marker: model.EdgeInstanceFencingSchemaV1,
		LegacyRowCount: 5, MigratedRowCount: 5, ActiveEpochCount: 0,
		ActivationPhase: model.EdgeActivationPhaseLegacyAuthoritative, ActivationGeneration: 19,
		InstanceUIDAlgorithm: edgeInstanceUIDAlgorithm, RecordedAt: time.Unix(1, 0).UTC(),
	})
	return string(raw)
}

func TestVerifyEdgeInstanceMigrationReceiptRequiresImmutableSchemaReceipt(t *testing.T) {
	for _, tc := range []struct {
		name    string
		marker  string
		receipt string
		missing int64
		wantErr error
	}{
		{name: "missing marker", wantErr: sql.ErrNoRows},
		{name: "missing receipt", marker: model.EdgeInstanceFencingSchemaV1, wantErr: sql.ErrNoRows},
		{name: "mapping missing", marker: model.EdgeInstanceFencingSchemaV1, receipt: testEdgeInstanceMigrationReceipt(), missing: 1, wantErr: ErrEdgeInstanceFencingNotReady},
		{name: "complete", marker: model.EdgeInstanceFencingSchemaV1, receipt: testEdgeInstanceMigrationReceipt(), missing: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			markerRows := sqlmock.NewRows([]string{"value"})
			if tc.marker != "" {
				markerRows.AddRow(tc.marker)
			}
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT value FROM fugue_meta WHERE key=$1`)).
				WithArgs(edgeInstanceFencingMetaKey).WillReturnRows(markerRows)
			if tc.marker != "" {
				receiptRows := sqlmock.NewRows([]string{"value"})
				if tc.receipt != "" {
					receiptRows.AddRow(tc.receipt)
				}
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT value FROM fugue_meta WHERE key=$1`)).
					WithArgs(edgeInstanceFencingReceiptKey).WillReturnRows(receiptRows)
				if tc.receipt != "" {
					mock.ExpectQuery(`SELECT count\(\*\) FROM fugue_edge_nodes AS n`).
						WithArgs(edgeLegacyMigrationSlot, edgeLegacyMigrationEpoch, time.Unix(1, 0).UTC()).
						WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tc.missing))
				}
			}
			err = verifyEdgeInstanceMigrationReceipt(context.Background(), database)
			switch {
			case tc.wantErr == nil && err != nil:
				t.Fatalf("verify receipt error = %v", err)
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("verify receipt error = %v, want %v", err, tc.wantErr)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPgReadEdgeControlNodesUsesMigratedInstanceIdentities(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT edge_id, edge_group_id
FROM fugue_edge_node_instances
GROUP BY edge_id, edge_group_id
ORDER BY edge_id`)).WillReturnRows(
		sqlmock.NewRows([]string{"edge_id", "edge_group_id"}).
			AddRow("edge-de-1", "edge-group-country-de").
			AddRow("edge-us-1", "edge-group-country-us"),
	)
	nodes, err := pgReadEdgeControlNodes(context.Background(), database)
	if err != nil {
		t.Fatalf("pgReadEdgeControlNodes: %v", err)
	}
	if len(nodes) != 2 || nodes[0].ID != "edge-de-1" || nodes[1].EdgeGroupID != "edge-group-country-us" {
		t.Fatalf("unexpected control identities: %+v", nodes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
