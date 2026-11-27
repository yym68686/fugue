package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) RecordResourceUsageSamples(samples []model.ResourceUsageSample, pruneBefore time.Time) error {
	if len(samples) == 0 && pruneBefore.IsZero() {
		return nil
	}
	return s.pgRecordResourceUsageSamples(samples, pruneBefore)
}

func (s *Store) ListResourceUsageSamples(tenantID, targetKind, targetID string, since time.Time) ([]model.ResourceUsageSample, error) {
	targetKind = strings.TrimSpace(targetKind)
	targetID = strings.TrimSpace(targetID)
	if targetKind == "" || targetID == "" {
		return nil, ErrInvalidInput
	}
	return s.pgListResourceUsageSamples(tenantID, targetKind, targetID, since)
}

func (s *Store) pgRecordResourceUsageSamples(samples []model.ResourceUsageSample, pruneBefore time.Time) error {
	if err := s.ensureDatabaseReady(); err != nil {
		return err
	}
	ctx := context.Background()
	tx, err := beginObservationSampleIngestion(ctx, s.db)
	if err != nil {
		return fmt.Errorf("begin resource usage sample transaction: %w", err)
	}
	defer tx.Rollback()

	if !pruneBefore.IsZero() {
		allowed, err := apiMayPruneDuringSampleIngestion(ctx, tx, resourceUsageSampleRetention, pruneBefore)
		if err != nil {
			return fmt.Errorf("prune resource usage samples: %w", err)
		}
		if allowed {
			if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_resource_usage_samples WHERE observed_at < $1`, pruneBefore); err != nil {
				return fmt.Errorf("prune resource usage samples: %w", err)
			}
		}
	}
	for _, sample := range samples {
		if strings.TrimSpace(sample.TargetKind) == "" || strings.TrimSpace(sample.TargetID) == "" || sample.ObservedAt.IsZero() {
			continue
		}
		if strings.TrimSpace(sample.ID) == "" {
			sample.ID = model.NewID("usage")
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_resource_usage_samples (
	id, tenant_id, project_id, target_kind, target_id, target_name, service_type,
	observed_at, cpu_millicores, memory_bytes, ephemeral_storage_bytes
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (id) DO NOTHING
`,
			sample.ID,
			nullableText(sample.TenantID),
			nullableText(sample.ProjectID),
			sample.TargetKind,
			sample.TargetID,
			sample.TargetName,
			sample.ServiceType,
			sample.ObservedAt,
			nullableInt64(sample.CPUMilliCores),
			nullableInt64(sample.MemoryBytes),
			nullableInt64(sample.EphemeralStorageBytes),
		); err != nil {
			return fmt.Errorf("insert resource usage sample: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit resource usage sample transaction: %w", err)
	}
	return nil
}

func (s *Store) pgListResourceUsageSamples(tenantID, targetKind, targetID string, since time.Time) ([]model.ResourceUsageSample, error) {
	if err := s.ensureDatabaseReady(); err != nil {
		return nil, err
	}
	query := `
SELECT id, tenant_id, project_id, target_kind, target_id, target_name, service_type,
	observed_at, cpu_millicores, memory_bytes, ephemeral_storage_bytes
FROM fugue_resource_usage_samples
WHERE target_kind = $1 AND target_id = $2
`
	args := []any{targetKind, targetID}
	if strings.TrimSpace(tenantID) != "" {
		args = append(args, strings.TrimSpace(tenantID))
		query += fmt.Sprintf(" AND tenant_id = $%d", len(args))
	}
	if !since.IsZero() {
		args = append(args, since)
		query += fmt.Sprintf(" AND observed_at >= $%d", len(args))
	}
	query += " ORDER BY observed_at ASC"

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, fmt.Errorf("list resource usage samples: %w", err)
	}
	defer rows.Close()

	var samples []model.ResourceUsageSample
	for rows.Next() {
		var sample model.ResourceUsageSample
		var tenantID, projectID sql.NullString
		var cpu, memory, ephemeral sql.NullInt64
		if err := rows.Scan(
			&sample.ID,
			&tenantID,
			&projectID,
			&sample.TargetKind,
			&sample.TargetID,
			&sample.TargetName,
			&sample.ServiceType,
			&sample.ObservedAt,
			&cpu,
			&memory,
			&ephemeral,
		); err != nil {
			return nil, fmt.Errorf("scan resource usage sample: %w", err)
		}
		sample.TenantID = tenantID.String
		sample.ProjectID = projectID.String
		sample.CPUMilliCores = int64FromNull(cpu)
		sample.MemoryBytes = int64FromNull(memory)
		sample.EphemeralStorageBytes = int64FromNull(ephemeral)
		samples = append(samples, sample)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate resource usage samples: %w", err)
	}
	return samples, nil
}

func nullableText(value string) sql.NullString {
	value = strings.TrimSpace(value)
	return sql.NullString{String: value, Valid: value != ""}
}

func nullableInt64(value *int64) sql.NullInt64 {
	if value == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *value, Valid: true}
}

func int64FromNull(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	out := value.Int64
	return &out
}
