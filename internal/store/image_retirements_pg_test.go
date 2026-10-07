package store

import (
	"context"
	"database/sql"
	"fugue/internal/model"
	"fugue/internal/schemamigrate"
	"os"
	"strings"
	"testing"
	"time"
)

func TestImageRetirementPostgresArchiveAndLocalPVUnknownRoundTrip(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL required")
	}
	if !strings.Contains(dsn, "fugue_test") {
		t.Fatal("test database required")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db, databaseURL: dsn}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.applyPostgresSchemaTx(context.Background(), tx); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := schemamigrate.MigrateImageRetirement(context.Background(), dsn); err != nil {
			t.Fatal(err)
		}
	}
	digest := "sha256:" + strings.Repeat("c", 64)
	im, err := s.UpsertImage(model.Image{ImageRef: "registry.example/archive@" + digest, CanonicalDigest: digest, LifecycleState: model.ImageLifecycleDeleting})
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec(`DELETE FROM fugue_image_retirements WHERE image_id=$1`, im.ID)
	if _, err = s.db.Exec(`DELETE FROM fugue_images WHERE id=$1`, im.ID); err != nil {
		t.Fatal(err)
	}
	rows, err := s.ListImagesWithRetirementAuthority()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range rows {
		if row.ID == im.ID && row.CanonicalDigest == digest {
			found = true
		}
	}
	if !found {
		t.Fatal("retirement authority disappeared with metadata")
	}
	inv, err := s.UpsertLocalPVInventory(model.LocalPVInventory{NodeID: "test-node-" + model.NewID("pv"), ClusterNodeName: "worker", BoundPVCount: -1, ObservedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec(`DELETE FROM fugue_localpv_inventories WHERE id=$1`, inv.ID)
	if inv.BoundPVCount != -1 || inv.BoundPVCountKnown {
		t.Fatalf("lost unknown in postgres: %+v", inv)
	}
}
