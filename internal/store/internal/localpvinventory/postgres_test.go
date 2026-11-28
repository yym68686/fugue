package localpvinventory

import (
	"database/sql"
	"errors"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/testpostgres"
)

func testRepository(t *testing.T) Repository {
	t.Helper()
	db, err := sql.Open("pgx", testpostgres.Database(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewPostgres(db)
}

func TestLocalPVInventoryUpsertReplacesNodeSnapshot(t *testing.T) {
	t.Parallel()

	repo := testRepository(t)
	first, err := repo.Upsert(model.LocalPVInventory{
		NodeID:             "machine-1",
		ClusterNodeName:    "worker-1",
		VGName:             "fugue-vg",
		ImagePath:          "/var/lib/fugue/lvm-localpv/fugue-vg.img",
		ImageSizeBytes:     10,
		LVCount:            1,
		ActiveLVCount:      1,
		BoundPVCount:       1,
		SafeToDecommission: false,
		UnsafeReasons:      []string{"active_lvs_present"},
		ObservedAt:         time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("upsert first inventory: %v", err)
	}
	second, err := repo.Upsert(model.LocalPVInventory{
		NodeID:             "machine-1",
		ClusterNodeName:    "worker-1",
		VGName:             "fugue-vg",
		ImagePath:          "/var/lib/fugue/lvm-localpv/fugue-vg.img",
		ImageSizeBytes:     10,
		LoopDevice:         "/dev/loop9",
		LoopBackingFile:    "/var/lib/fugue/lvm-localpv/fugue-vg.img",
		LVCount:            0,
		ActiveLVCount:      0,
		BoundPVCount:       0,
		SafeToDecommission: true,
		ObservedAt:         time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("upsert second inventory: %v", err)
	}
	if first.ID != second.ID || !first.CreatedAt.Equal(second.CreatedAt) {
		t.Fatalf("expected inventory row replacement, got first=%s second=%s", first.ID, second.ID)
	}
	inventories, err := repo.List(model.LocalPVInventoryFilter{ClusterNodeName: " worker-1 "})
	if err != nil {
		t.Fatalf("list inventories: %v", err)
	}
	if len(inventories) != 1 || !inventories[0].SafeToDecommission || inventories[0].LVCount != 0 {
		t.Fatalf("unexpected inventories: %+v", inventories)
	}
}

func TestLocalPVInventoryUnknownBindingsRoundTripAndFilters(t *testing.T) {
	repo := testRepository(t)
	at := time.Now().UTC().Truncate(time.Microsecond)
	older, err := repo.Upsert(model.LocalPVInventory{
		ClusterNodeName: "worker", RuntimeID: "old-runtime", ObservedAt: at.Add(-time.Hour),
		BoundPVCount: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	newer, err := repo.Upsert(model.LocalPVInventory{
		NodeID: " machine ", ClusterNodeName: "worker", RuntimeID: "runtime", ObservedAt: at,
		BoundPVCount: 0, NodeRoles: []string{" worker ", "control", "worker"},
		UnsafeReasons: []string{"kubectl_pv_unavailable"},
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := repo.List(model.LocalPVInventoryFilter{ClusterNodeName: "worker"})
	if err != nil || len(rows) != 2 || rows[0].ID != newer.ID || rows[1].ID != older.ID {
		t.Fatalf("latest observation should be first: %+v, %v", rows, err)
	}
	for _, row := range rows {
		if row.BoundPVCount != -1 || row.BoundPVCountKnown {
			t.Fatalf("unknown binding became a known zero: %+v", row)
		}
	}
	rows, err = repo.List(model.LocalPVInventoryFilter{NodeID: " machine ", RuntimeID: "runtime", StaleAfter: at})
	if err != nil || len(rows) != 1 || rows[0].ID != newer.ID || !slices.Equal(rows[0].NodeRoles, []string{"control", "worker"}) {
		t.Fatalf("filtered inventory or normalized roles changed: %+v, %v", rows, err)
	}
}

func TestLocalPVInventoryRejectsMissingIdentityWithoutDatabase(t *testing.T) {
	if _, err := (Repository{}).Upsert(model.LocalPVInventory{ClusterNodeName: " "}); !errors.Is(err, ErrMissingIdentity) {
		t.Fatalf("missing identity: %v", err)
	}
}
