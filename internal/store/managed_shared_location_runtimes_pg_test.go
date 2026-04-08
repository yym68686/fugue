package store

import (
	"context"
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGListManagedSharedLocationRuntimesTxScansPublicOfferColumn(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()

	s := &Store{
		databaseURL: "postgres://example",
		db:          db,
		dbReady:     true,
	}

	now := time.Date(2026, time.April, 8, 9, 30, 0, 0, time.UTC)
	labelsJSON := `{"managed":"true","fugue.io/internal-cluster-location-key":"region:us-west1","topology.kubernetes.io/region":"us-west1"}`
	publicOfferJSON := `{"reference_bundle":{"cpu_millicores":2000,"memory_mebibytes":4096,"storage_gibibytes":30},"reference_monthly_price_microcents":123456789,"free_storage":true,"price_book":{"currency":"USD","hours_per_month":730,"cpu_microcents_per_millicore_hour":77,"memory_microcents_per_mib_hour":12,"storage_microcents_per_gib_hour":0},"updated_at":"2026-04-08T09:30:00Z"}`

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT id, tenant_id, name, machine_name, type, access_mode, public_offer_json, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE type = $1
  AND id LIKE $2
ORDER BY created_at ASC
FOR UPDATE
`)).
		WithArgs(model.RuntimeTypeManagedShared, managedSharedLocationRuntimeIDPrefix+"%").
		WillReturnRows(sqlmock.NewRows([]string{
			"id",
			"tenant_id",
			"name",
			"machine_name",
			"type",
			"access_mode",
			"public_offer_json",
			"pool_mode",
			"connection_mode",
			"status",
			"endpoint",
			"labels_json",
			"node_key_id",
			"cluster_node_name",
			"fingerprint_prefix",
			"fingerprint_hash",
			"agent_key_prefix",
			"agent_key_hash",
			"last_seen_at",
			"last_heartbeat_at",
			"created_at",
			"updated_at",
		}).AddRow(
			managedSharedLocationRuntimeID("region:us-west1"),
			nil,
			managedSharedLocationRuntimeName("region:us-west1"),
			nil,
			model.RuntimeTypeManagedShared,
			model.RuntimeAccessModePlatformShared,
			[]byte(publicOfferJSON),
			model.RuntimePoolModeDedicated,
			"",
			model.RuntimeStatusActive,
			"in-cluster",
			[]byte(labelsJSON),
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			now,
			now,
		))

	runtimes, err := s.pgListManagedSharedLocationRuntimesTx(context.Background(), tx)
	if err != nil {
		t.Fatalf("pgListManagedSharedLocationRuntimesTx: %v", err)
	}
	if len(runtimes) != 1 {
		t.Fatalf("expected 1 runtime, got %d", len(runtimes))
	}

	runtimeObj := runtimes[0]
	if runtimeObj.ID != managedSharedLocationRuntimeID("region:us-west1") {
		t.Fatalf("expected runtime id %q, got %q", managedSharedLocationRuntimeID("region:us-west1"), runtimeObj.ID)
	}
	if runtimeObj.PublicOffer == nil {
		t.Fatal("expected managed shared location runtime to decode public offer")
	}
	if got := runtimeObj.PublicOffer.ReferenceMonthlyPriceMicroCents; got != 123456789 {
		t.Fatalf("expected reference monthly price 123456789, got %d", got)
	}
	if got := runtimeObj.PublicOffer.ReferenceBundle.CPUMilliCores; got != 2000 {
		t.Fatalf("expected reference cpu 2000, got %d", got)
	}
	if !runtimeObj.PublicOffer.FreeStorage {
		t.Fatal("expected free storage flag to decode")
	}
	if got := runtimeObj.PublicOffer.PriceBook.CPUMicroCentsPerMilliCoreHour; got != 77 {
		t.Fatalf("expected cpu price 77, got %d", got)
	}
	if got := runtimeObj.Labels[managedSharedLocationKeyLabelKey]; got != "region:us-west1" {
		t.Fatalf("expected managed shared location label %q, got %q", "region:us-west1", got)
	}

	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}
