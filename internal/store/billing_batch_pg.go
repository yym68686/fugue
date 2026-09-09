package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"fugue/internal/model"
	"github.com/jackc/pgx/v5/pgconn"
)

func (s *Store) pgGetTenantBillingSnapshot(ctx context.Context, ids []string) (TenantBillingSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for attempt := 0; ; attempt++ {
		snapshot, err := s.pgBillingSummariesAttempt(ctx, ids)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || (pgErr.Code != "40001" && pgErr.Code != "40P01" && pgErr.Code != "55P03") {
			return snapshot, err
		}
		started := time.Now()
		err = sleepContext(ctx, time.Duration(min(attempt+1, 10))*10*time.Millisecond)
		recordReadStage(ctx, "billing_retry_wait", started)
		if err != nil {
			return TenantBillingSnapshot{}, err
		}
	}
}

func (s *Store) pgBillingSummariesAttempt(ctx context.Context, ids []string) (TenantBillingSnapshot, error) {
	started := time.Now()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	recordReadStage(ctx, "billing_begin", started)
	if err != nil {
		return TenantBillingSnapshot{}, err
	}
	defer tx.Rollback()
	started = time.Now()
	state, err := s.pgLoadBillingSummaryInputsTx(withReadStageName(ctx, "billing_inputs"), tx, ids)
	recordReadStage(ctx, "billing_inputs", started)
	if err != nil {
		return TenantBillingSnapshot{}, err
	}
	started = time.Now()
	lockIDs := make([]string, 0, len(state.Tenants))
	seen := make(map[string]bool, len(state.Tenants))
	for _, tenant := range state.Tenants {
		lockIDs = append(lockIDs, tenant.ID)
		seen[tenant.ID] = true
	}
	index := newBillingStateIndex(&state)
	owners := make([]string, 0)
	for _, tenant := range state.Tenants {
		for _, component := range tenantPublicRuntimeChargeComponentsWithIndex(&state, index, tenant.ID) {
			if !seen[component.OwnerTenantID] {
				seen[component.OwnerTenantID] = true
				owners = append(owners, component.OwnerTenantID)
				lockIDs = append(lockIDs, component.OwnerTenantID)
			}
		}
	}
	if len(owners) > 0 {
		events, err := s.pgLoadBillingEventsTx(ctx, tx, owners)
		if err != nil {
			return TenantBillingSnapshot{}, err
		}
		state.BillingEvents = append(state.BillingEvents, events...)
	}
	sort.Strings(lockIDs)
	recordReadStage(ctx, "billing_owners", started)
	// Do not wait while holding a subset of ledgers: existing single-tenant
	// transactions may already hold the consumer and next acquire its owner.
	started = time.Now()
	rows, err := tx.QueryContext(withReadStageName(ctx, "billing_locks"), `
SELECT tenant_id, managed_cap_json, managed_image_storage_gibibytes, balance_microcents, price_book_json, last_accrued_at, created_at, updated_at
FROM fugue_tenant_billing
WHERE tenant_id = ANY($1::text[])
ORDER BY tenant_id
FOR UPDATE NOWAIT`, lockIDs)
	if err != nil {
		recordReadStage(ctx, "billing_locks", started)
		return TenantBillingSnapshot{}, err
	}
	before := make(map[string]model.TenantBilling, len(lockIDs))
	for rows.Next() {
		record, err := scanTenantBilling(rows)
		if err != nil {
			rows.Close()
			return TenantBillingSnapshot{}, err
		}
		state.TenantBilling = append(state.TenantBilling, record)
		before[record.TenantID] = record
	}
	err = rows.Err()
	rows.Close()
	recordReadStage(ctx, "billing_locks", started)
	if err != nil {
		return TenantBillingSnapshot{}, err
	}
	started = time.Now()
	previousEvents := len(state.BillingEvents)
	summaries, missing := accrueBillingSummaries(&state, ids, time.Now().UTC())
	changed := make([]model.TenantBilling, 0, len(state.TenantBilling))
	for _, record := range state.TenantBilling {
		if original, ok := before[record.TenantID]; !ok || !reflect.DeepEqual(original, record) {
			changed = append(changed, record)
		}
	}
	recordReadStage(ctx, "billing_accrue", started)
	started = time.Now()
	err = s.pgPersistBillingBatchTx(withReadStageName(ctx, "billing_persist"), tx, changed, state.BillingEvents[previousEvents:])
	recordReadStage(ctx, "billing_persist", started)
	if err != nil {
		return TenantBillingSnapshot{}, err
	}
	started = time.Now()
	err = tx.Commit()
	recordReadStage(ctx, "billing_commit", started)
	if err != nil {
		return TenantBillingSnapshot{}, fmt.Errorf("commit billing summaries: %w", err)
	}
	return completeTenantBillingSnapshot(&state, ids, summaries, missing), nil
}

func (s *Store) pgPersistBillingBatchTx(ctx context.Context, tx *sql.Tx, records []model.TenantBilling, events []model.TenantBillingEvent) error {
	if len(records) > 0 {
		payload, err := marshalJSON(records)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_tenant_billing (tenant_id, managed_cap_json, managed_image_storage_gibibytes, balance_microcents, price_book_json, last_accrued_at, created_at, updated_at)
SELECT tenant_id, managed_cap, COALESCE(managed_image_storage_gibibytes, 0), balance_microcents, price_book, last_accrued_at, created_at, updated_at
FROM jsonb_to_recordset($1::jsonb) AS r(tenant_id text, managed_cap jsonb, managed_image_storage_gibibytes bigint, balance_microcents bigint, price_book jsonb, last_accrued_at timestamptz, created_at timestamptz, updated_at timestamptz)
ORDER BY tenant_id
ON CONFLICT (tenant_id) DO UPDATE SET
managed_cap_json = EXCLUDED.managed_cap_json,
managed_image_storage_gibibytes = EXCLUDED.managed_image_storage_gibibytes,
balance_microcents = EXCLUDED.balance_microcents,
price_book_json = EXCLUDED.price_book_json,
last_accrued_at = EXCLUDED.last_accrued_at,
created_at = EXCLUDED.created_at,
updated_at = EXCLUDED.updated_at`, payload); err != nil {
			return fmt.Errorf("persist billing summary ledgers: %w", err)
		}
	}
	if len(events) > 0 {
		payload, err := marshalJSON(events)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_billing_events (id, tenant_id, type, amount_microcents, balance_after_microcents, metadata_json, created_at)
SELECT id, tenant_id, type, amount_microcents, balance_after_microcents, metadata, created_at
FROM jsonb_to_recordset($1::jsonb) AS e(id text, tenant_id text, type text, amount_microcents bigint, balance_after_microcents bigint, metadata jsonb, created_at timestamptz)`, payload); err != nil {
			return fmt.Errorf("persist billing summary events: %w", err)
		}
	}
	return nil
}
