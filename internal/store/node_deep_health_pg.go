package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgRecordNodeDeepHealthResult(result model.NodeDeepHealthResult) (model.NodeDeepHealthResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result = normalizeNodeDeepHealthResult(result, time.Now().UTC())
	checksJSON, err := json.Marshal(result.Checks)
	if err != nil {
		return model.NodeDeepHealthResult{}, err
	}
	recoveryJSON, err := json.Marshal(result.RecoveryConditions)
	if err != nil {
		return model.NodeDeepHealthResult{}, err
	}
	saved, err := scanNodeDeepHealthResult(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_node_deep_health_results (
	node_updater_id, cluster_node_name, runtime_id, machine_id, observed_only, overall_status,
	quarantine_state, quarantine_reason, quarantine_expires_at, recovery_conditions_json,
	checks_json, reported_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
)
ON CONFLICT (node_updater_id)
DO UPDATE SET
	cluster_node_name = EXCLUDED.cluster_node_name,
	runtime_id = EXCLUDED.runtime_id,
	machine_id = EXCLUDED.machine_id,
	observed_only = EXCLUDED.observed_only,
	overall_status = EXCLUDED.overall_status,
	quarantine_state = EXCLUDED.quarantine_state,
	quarantine_reason = EXCLUDED.quarantine_reason,
	quarantine_expires_at = EXCLUDED.quarantine_expires_at,
	recovery_conditions_json = EXCLUDED.recovery_conditions_json,
	checks_json = EXCLUDED.checks_json,
	reported_at = EXCLUDED.reported_at,
	updated_at = EXCLUDED.updated_at
RETURNING node_updater_id, cluster_node_name, runtime_id, machine_id, observed_only, overall_status,
	quarantine_state, quarantine_reason, quarantine_expires_at, recovery_conditions_json,
	checks_json, reported_at, updated_at
`, result.NodeUpdaterID, result.ClusterNodeName, result.RuntimeID, result.MachineID, result.ObservedOnly,
		result.OverallStatus, result.QuarantineState, result.QuarantineReason, result.QuarantineExpiresAt,
		recoveryJSON, checksJSON, result.ReportedAt, result.UpdatedAt))
	if err != nil {
		return model.NodeDeepHealthResult{}, mapDBErr(err)
	}
	return saved, nil
}

func (s *Store) pgListNodeDeepHealthResults() ([]model.NodeDeepHealthResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
SELECT node_updater_id, cluster_node_name, runtime_id, machine_id, observed_only, overall_status,
	quarantine_state, quarantine_reason, quarantine_expires_at, recovery_conditions_json,
	checks_json, reported_at, updated_at
FROM fugue_node_deep_health_results
ORDER BY updated_at DESC, node_updater_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := []model.NodeDeepHealthResult{}
	for rows.Next() {
		result, err := scanNodeDeepHealthResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *Store) pgGetNodeDeepHealthResult(nodeUpdaterID string) (model.NodeDeepHealthResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := scanNodeDeepHealthResult(s.db.QueryRowContext(ctx, `
SELECT node_updater_id, cluster_node_name, runtime_id, machine_id, observed_only, overall_status,
	quarantine_state, quarantine_reason, quarantine_expires_at, recovery_conditions_json,
	checks_json, reported_at, updated_at
FROM fugue_node_deep_health_results
WHERE node_updater_id = $1`, nodeUpdaterID))
	if err != nil {
		return model.NodeDeepHealthResult{}, mapDBErr(err)
	}
	return result, nil
}

func scanNodeDeepHealthResult(scanner sqlScanner) (model.NodeDeepHealthResult, error) {
	var result model.NodeDeepHealthResult
	var quarantineReason sql.NullString
	var quarantineExpiresAt sql.NullTime
	var recoveryJSON []byte
	var checksJSON []byte
	if err := scanner.Scan(
		&result.NodeUpdaterID,
		&result.ClusterNodeName,
		&result.RuntimeID,
		&result.MachineID,
		&result.ObservedOnly,
		&result.OverallStatus,
		&result.QuarantineState,
		&quarantineReason,
		&quarantineExpiresAt,
		&recoveryJSON,
		&checksJSON,
		&result.ReportedAt,
		&result.UpdatedAt,
	); err != nil {
		return model.NodeDeepHealthResult{}, err
	}
	if quarantineReason.Valid {
		result.QuarantineReason = quarantineReason.String
	}
	if quarantineExpiresAt.Valid {
		result.QuarantineExpiresAt = &quarantineExpiresAt.Time
	}
	if len(recoveryJSON) > 0 {
		if err := json.Unmarshal(recoveryJSON, &result.RecoveryConditions); err != nil {
			return model.NodeDeepHealthResult{}, fmt.Errorf("decode node deep health recovery conditions: %w", err)
		}
	}
	if len(checksJSON) > 0 {
		if err := json.Unmarshal(checksJSON, &result.Checks); err != nil {
			return model.NodeDeepHealthResult{}, fmt.Errorf("decode node deep health checks: %w", err)
		}
	}
	return result, nil
}
