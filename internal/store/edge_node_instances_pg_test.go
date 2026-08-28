package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

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
						WithArgs(edgeLegacyMigrationSlot, edgeLegacyMigrationEpoch).
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
