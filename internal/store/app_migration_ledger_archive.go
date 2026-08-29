package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

type latestAppMigrationLedgerArchive struct {
	ledger    model.AppMigrationLedger
	createdAt time.Time
}

// recordAppMigrationLedgerArchive writes the retention authority before the
// diagnostic operation-evidence copy. The archive has no app/operation/tenant
// foreign key, so normal product-data purge cannot shorten its 90-day life.
func (s *Store) recordAppMigrationLedgerArchive(ledger model.AppMigrationLedger) error {
	if s == nil || strings.TrimSpace(ledger.ID) == "" {
		return ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgRecordAppMigrationLedgerArchive(ledger)
	}
	return s.withLockedState(true, func(state *model.State) error {
		for _, existing := range state.AppMigrationLedgers {
			if strings.TrimSpace(existing.ID) == strings.TrimSpace(ledger.ID) {
				return nil
			}
		}
		state.AppMigrationLedgers = append(state.AppMigrationLedgers, cloneAppMigrationLedger(ledger))
		state.AppMigrationLedgers = retainAppMigrationLedgerArchive(state.AppMigrationLedgers, time.Now().UTC())
		return nil
	})
}

func (s *Store) listAppMigrationLedgerArchive(filter model.OperationEvidenceFilter) ([]model.AppMigrationLedger, error) {
	if s == nil {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListAppMigrationLedgerArchive(filter)
	}
	out := []model.AppMigrationLedger{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, ledger := range state.AppMigrationLedgers {
			if !appMigrationLedgerMatchesFilter(ledger, filter) {
				continue
			}
			out = append(out, cloneAppMigrationLedger(ledger))
		}
		return nil
	})
	return out, err
}

// latestAppMigrationLedgerArchiveByOperation is the bounded read used by
// global cleanup protection. Diagnostic callers keep the complete archive
// listing above, while this path only transfers one snapshot per operation.
func (s *Store) latestAppMigrationLedgerArchiveByOperation() (map[string]latestAppMigrationLedgerArchive, error) {
	if s == nil {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgLatestAppMigrationLedgerArchiveByOperation()
	}
	ledgers, err := s.listAppMigrationLedgerArchive(model.OperationEvidenceFilter{PlatformAdmin: true})
	if err != nil {
		return nil, err
	}
	out := make(map[string]latestAppMigrationLedgerArchive, len(ledgers))
	for _, ledger := range ledgers {
		opID := strings.TrimSpace(ledger.OperationID)
		if opID == "" {
			continue
		}
		current, exists := out[opID]
		if !exists || ledger.UpdatedAt.After(current.ledger.UpdatedAt) ||
			(ledger.UpdatedAt.Equal(current.ledger.UpdatedAt) && ledger.ID > current.ledger.ID) {
			current.ledger = ledger
		}
		if current.createdAt.IsZero() || (!ledger.CreatedAt.IsZero() && ledger.CreatedAt.Before(current.createdAt)) {
			current.createdAt = ledger.CreatedAt
		}
		out[opID] = current
	}
	return out, nil
}

func (s *Store) pgRecordAppMigrationLedgerArchive(ledger model.AppMigrationLedger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	payload, err := json.Marshal(ledger)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return mapDBErr(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_app_migration_ledgers (
	id, tenant_id, project_id, app_id, operation_id, observed_at,
	collected_at, retain_until, ledger_json, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (id) DO NOTHING
`, ledger.ID, ledger.TenantID, ledger.ProjectID, ledger.AppID, ledger.OperationID,
		ledger.ObservedAt, ledger.UpdatedAt, ledger.RetainUntil, payload, ledger.CreatedAt); err != nil {
		return mapDBErr(err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM fugue_app_migration_ledgers AS old
WHERE old.retain_until < $1
  AND EXISTS (
	SELECT 1
	FROM fugue_app_migration_ledgers AS newer
	WHERE newer.operation_id = old.operation_id
	  AND (newer.collected_at > old.collected_at OR (newer.collected_at = old.collected_at AND newer.id > old.id))
  )
`, time.Now().UTC()); err != nil {
		return mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return mapDBErr(err)
	}
	return nil
}

func (s *Store) pgListAppMigrationLedgerArchive(filter model.OperationEvidenceFilter) ([]model.AppMigrationLedger, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clauses := []string{}
	args := []any{}
	if !filter.PlatformAdmin || strings.TrimSpace(filter.TenantID) != "" {
		args = append(args, strings.TrimSpace(filter.TenantID))
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if operationID := strings.TrimSpace(filter.OperationID); operationID != "" {
		args = append(args, operationID)
		clauses = append(clauses, fmt.Sprintf("operation_id = $%d", len(args)))
	}
	if appID := strings.TrimSpace(filter.AppID); appID != "" {
		args = append(args, appID)
		clauses = append(clauses, fmt.Sprintf("app_id = $%d", len(args)))
	}
	if filter.Since != nil {
		args = append(args, filter.Since.UTC())
		clauses = append(clauses, fmt.Sprintf("collected_at >= $%d", len(args)))
	}
	query := `SELECT ledger_json, collected_at FROM fugue_app_migration_ledgers`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY collected_at DESC, id DESC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	out := []model.AppMigrationLedger{}
	for rows.Next() {
		var payload []byte
		var collectedAt time.Time
		if err := rows.Scan(&payload, &collectedAt); err != nil {
			return nil, mapDBErr(err)
		}
		var ledger model.AppMigrationLedger
		if err := json.Unmarshal(payload, &ledger); err != nil {
			// A malformed archive row is skipped for listing, but completion's
			// point lookup remains fail-closed because no verified row decodes.
			continue
		}
		out = append(out, model.NormalizeAppMigrationLedger(ledger, collectedAt))
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func (s *Store) pgLatestAppMigrationLedgerArchiveByOperation() (map[string]latestAppMigrationLedgerArchive, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `
WITH latest AS (
	SELECT DISTINCT ON (operation_id)
		operation_id, tenant_id, app_id, ledger_json, collected_at
	FROM fugue_app_migration_ledgers
	WHERE operation_id <> ''
	ORDER BY operation_id, collected_at DESC, id DESC
), earliest AS (
	SELECT operation_id, MIN(created_at) AS created_at
	FROM fugue_app_migration_ledgers
	WHERE operation_id <> ''
	GROUP BY operation_id
)
SELECT latest.operation_id, latest.tenant_id, latest.app_id,
	latest.ledger_json, latest.collected_at, earliest.created_at
FROM latest
JOIN earliest USING (operation_id)
ORDER BY latest.collected_at DESC, latest.operation_id DESC
`)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	out := map[string]latestAppMigrationLedgerArchive{}
	for rows.Next() {
		var operationID string
		var tenantID string
		var appID string
		var payload []byte
		var collectedAt time.Time
		var createdAt time.Time
		if err := rows.Scan(&operationID, &tenantID, &appID, &payload, &collectedAt, &createdAt); err != nil {
			return nil, mapDBErr(err)
		}
		operationID = strings.TrimSpace(operationID)
		if operationID == "" {
			continue
		}
		var ledger model.AppMigrationLedger
		if err := json.Unmarshal(payload, &ledger); err != nil {
			ledger = model.AppMigrationLedger{
				TenantID:              strings.TrimSpace(tenantID),
				AppID:                 strings.TrimSpace(appID),
				OperationID:           operationID,
				CutoverStatus:         model.AppMigrationCutoverBlocked,
				OldArtifactsProtected: true,
				FailureReason:         "migration ledger archive payload is malformed",
			}
		} else {
			ledger = model.NormalizeAppMigrationLedger(ledger, collectedAt)
			ledger.TenantID = strings.TrimSpace(tenantID)
			ledger.AppID = strings.TrimSpace(appID)
			ledger.OperationID = operationID
		}
		out[operationID] = latestAppMigrationLedgerArchive{ledger: ledger, createdAt: createdAt}
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func appMigrationLedgerMatchesFilter(ledger model.AppMigrationLedger, filter model.OperationEvidenceFilter) bool {
	if (!filter.PlatformAdmin || strings.TrimSpace(filter.TenantID) != "") &&
		strings.TrimSpace(ledger.TenantID) != strings.TrimSpace(filter.TenantID) {
		return false
	}
	if value := strings.TrimSpace(filter.OperationID); value != "" && strings.TrimSpace(ledger.OperationID) != value {
		return false
	}
	if value := strings.TrimSpace(filter.AppID); value != "" && strings.TrimSpace(ledger.AppID) != value {
		return false
	}
	if filter.Since != nil && ledger.UpdatedAt.Before(filter.Since.UTC()) {
		return false
	}
	return true
}

func cloneAppMigrationLedger(in model.AppMigrationLedger) model.AppMigrationLedger {
	out := in
	if in.EndpointReady != nil {
		value := *in.EndpointReady
		out.EndpointReady = &value
	}
	if in.PhysicalReplicas != nil {
		value := *in.PhysicalReplicas
		out.PhysicalReplicas = &value
	}
	out.InvariantViolations = append([]string(nil), in.InvariantViolations...)
	return out
}

func retainAppMigrationLedgerArchive(items []model.AppMigrationLedger, now time.Time) []model.AppMigrationLedger {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	latestByOperation := make(map[string]model.AppMigrationLedger, len(items))
	for _, ledger := range items {
		opID := strings.TrimSpace(ledger.OperationID)
		latest, exists := latestByOperation[opID]
		if !exists || ledger.UpdatedAt.After(latest.UpdatedAt) ||
			(ledger.UpdatedAt.Equal(latest.UpdatedAt) && ledger.ID > latest.ID) {
			latestByOperation[opID] = ledger
		}
	}
	out := items[:0]
	for _, ledger := range items {
		ledger = model.NormalizeAppMigrationLedger(ledger, now)
		latest := latestByOperation[strings.TrimSpace(ledger.OperationID)]
		isLatest := strings.TrimSpace(latest.ID) == strings.TrimSpace(ledger.ID)
		if !isLatest && !ledger.RetainUntil.IsZero() && now.After(ledger.RetainUntil) {
			continue
		}
		out = append(out, ledger)
	}
	return out
}

func backfillMigrationLedgerArchiveInState(state *model.State, now time.Time) {
	if state == nil {
		return
	}
	seen := make(map[string]struct{}, len(state.AppMigrationLedgers))
	for _, ledger := range state.AppMigrationLedgers {
		seen[strings.TrimSpace(ledger.ID)] = struct{}{}
	}
	for _, evidence := range state.OperationEvidence {
		if !isLongLivedMigrationEvidenceType(evidence.Type) {
			continue
		}
		ledger, err := migrationLedgerFromEvidence(evidence)
		if err != nil {
			continue
		}
		if _, exists := seen[strings.TrimSpace(ledger.ID)]; exists {
			continue
		}
		state.AppMigrationLedgers = append(state.AppMigrationLedgers, ledger)
		seen[strings.TrimSpace(ledger.ID)] = struct{}{}
	}
	state.AppMigrationLedgers = retainAppMigrationLedgerArchive(state.AppMigrationLedgers, now)
}
