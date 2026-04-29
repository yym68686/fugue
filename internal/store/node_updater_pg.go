package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgUpsertNodeUpdater(key model.NodeKey, machine model.Machine, runtimeID, updaterVersion, joinScriptVersion string, capabilities []string) (model.NodeUpdater, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	token := model.NewSecret("fugue_nu")
	now := time.Now().UTC()
	updater := model.NodeUpdater{
		ID:                model.NewID("nodeupdater"),
		TenantID:          strings.TrimSpace(machine.TenantID),
		NodeKeyID:         strings.TrimSpace(key.ID),
		MachineID:         strings.TrimSpace(machine.ID),
		RuntimeID:         strings.TrimSpace(runtimeID),
		ClusterNodeName:   strings.TrimSpace(machine.ClusterNodeName),
		Status:            model.NodeUpdaterStatusActive,
		TokenPrefix:       model.SecretPrefix(token),
		TokenHash:         model.HashSecret(token),
		Labels:            cloneMap(machine.Labels),
		Capabilities:      normalizeStringList(capabilities),
		UpdaterVersion:    strings.TrimSpace(updaterVersion),
		JoinScriptVersion: strings.TrimSpace(joinScriptVersion),
		LastSeenAt:        &now,
		LastHeartbeatAt:   &now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	existing, err := scanNodeUpdater(s.db.QueryRowContext(ctx, `
SELECT id, tenant_id, node_key_id, machine_id, runtime_id, cluster_node_name, status, token_prefix, token_hash, labels_json, capabilities_json, updater_version, join_script_version, k3s_version, os, arch, last_error, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_node_updaters
WHERE (machine_id <> '' AND machine_id = $1)
   OR (cluster_node_name <> '' AND cluster_node_name = $2 AND (node_key_id = $3 OR $3 IS NULL))
ORDER BY updated_at DESC
LIMIT 1
`, updater.MachineID, updater.ClusterNodeName, nullIfEmpty(updater.NodeKeyID)))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return model.NodeUpdater{}, "", mapDBErr(err)
	}
	if err == nil {
		updater.ID = existing.ID
		updater.CreatedAt = existing.CreatedAt
	}

	labelsJSON, err := marshalNullableJSON(updater.Labels)
	if err != nil {
		return model.NodeUpdater{}, "", err
	}
	capabilitiesJSON, err := marshalNullableJSON(updater.Capabilities)
	if err != nil {
		return model.NodeUpdater{}, "", err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO fugue_node_updaters (
	id, tenant_id, node_key_id, machine_id, runtime_id, cluster_node_name, status,
	token_prefix, token_hash, labels_json, capabilities_json, updater_version,
	join_script_version, k3s_version, os, arch, last_error, last_seen_at,
	last_heartbeat_at, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10, $11, $12,
	$13, $14, $15, $16, $17, $18,
	$19, $20, $21
)
ON CONFLICT (id) DO UPDATE SET
	tenant_id = EXCLUDED.tenant_id,
	node_key_id = EXCLUDED.node_key_id,
	machine_id = EXCLUDED.machine_id,
	runtime_id = EXCLUDED.runtime_id,
	cluster_node_name = EXCLUDED.cluster_node_name,
	status = EXCLUDED.status,
	token_prefix = EXCLUDED.token_prefix,
	token_hash = EXCLUDED.token_hash,
	labels_json = EXCLUDED.labels_json,
	capabilities_json = EXCLUDED.capabilities_json,
	updater_version = EXCLUDED.updater_version,
	join_script_version = EXCLUDED.join_script_version,
	last_seen_at = EXCLUDED.last_seen_at,
	last_heartbeat_at = EXCLUDED.last_heartbeat_at,
	updated_at = EXCLUDED.updated_at
`, updater.ID, nullIfEmpty(updater.TenantID), nullIfEmpty(updater.NodeKeyID), updater.MachineID, updater.RuntimeID, updater.ClusterNodeName, updater.Status, updater.TokenPrefix, updater.TokenHash, labelsJSON, capabilitiesJSON, updater.UpdaterVersion, updater.JoinScriptVersion, updater.K3SVersion, updater.OS, updater.Arch, updater.LastError, updater.LastSeenAt, updater.LastHeartbeatAt, updater.CreatedAt, updater.UpdatedAt)
	if err != nil {
		return model.NodeUpdater{}, "", mapDBErr(err)
	}
	return redactNodeUpdater(updater), token, nil
}

func (s *Store) pgAuthenticateNodeUpdater(secret string) (model.NodeUpdater, model.Principal, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.NodeUpdater{}, model.Principal{}, fmt.Errorf("begin authenticate node updater transaction: %w", err)
	}
	defer tx.Rollback()

	updater, err := scanNodeUpdater(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, node_key_id, machine_id, runtime_id, cluster_node_name, status, token_prefix, token_hash, labels_json, capabilities_json, updater_version, join_script_version, k3s_version, os, arch, last_error, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_node_updaters
WHERE token_hash = $1
`, model.HashSecret(secret)))
	if err != nil {
		return model.NodeUpdater{}, model.Principal{}, mapDBErr(err)
	}
	if updater.Status == model.NodeUpdaterStatusRevoked {
		return model.NodeUpdater{}, model.Principal{}, ErrConflict
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_node_updaters SET last_seen_at = $2, updated_at = $2 WHERE id = $1
`, updater.ID, now); err != nil {
		return model.NodeUpdater{}, model.Principal{}, mapDBErr(err)
	}
	updater.LastSeenAt = &now
	updater.UpdatedAt = now
	if err := tx.Commit(); err != nil {
		return model.NodeUpdater{}, model.Principal{}, fmt.Errorf("commit authenticate node updater transaction: %w", err)
	}
	return redactNodeUpdater(updater), nodeUpdaterPrincipal(updater), nil
}

func (s *Store) pgUpdateNodeUpdaterHeartbeat(updaterID string, heartbeat model.NodeUpdater) (model.NodeUpdater, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	labelsJSON, err := marshalNullableJSON(heartbeat.Labels)
	if err != nil {
		return model.NodeUpdater{}, err
	}
	capabilitiesJSON, err := marshalNullableJSON(normalizeStringList(heartbeat.Capabilities))
	if err != nil {
		return model.NodeUpdater{}, err
	}
	updater, err := scanNodeUpdater(s.db.QueryRowContext(ctx, `
UPDATE fugue_node_updaters SET
	labels_json = $2,
	capabilities_json = $3,
	updater_version = $4,
	join_script_version = $5,
	k3s_version = $6,
	os = $7,
	arch = $8,
	last_error = $9,
	last_seen_at = $10,
	last_heartbeat_at = $10,
	updated_at = $10
WHERE id = $1
RETURNING id, tenant_id, node_key_id, machine_id, runtime_id, cluster_node_name, status, token_prefix, token_hash, labels_json, capabilities_json, updater_version, join_script_version, k3s_version, os, arch, last_error, last_seen_at, last_heartbeat_at, created_at, updated_at
`, strings.TrimSpace(updaterID), labelsJSON, capabilitiesJSON, strings.TrimSpace(heartbeat.UpdaterVersion), strings.TrimSpace(heartbeat.JoinScriptVersion), strings.TrimSpace(heartbeat.K3SVersion), strings.TrimSpace(heartbeat.OS), strings.TrimSpace(heartbeat.Arch), strings.TrimSpace(heartbeat.LastError), now))
	if err != nil {
		return model.NodeUpdater{}, mapDBErr(err)
	}
	return redactNodeUpdater(updater), nil
}

func (s *Store) pgListNodeUpdaters(tenantID string, platformAdmin bool) ([]model.NodeUpdater, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, node_key_id, machine_id, runtime_id, cluster_node_name, status, token_prefix, token_hash, labels_json, capabilities_json, updater_version, join_script_version, k3s_version, os, arch, last_error, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_node_updaters`
	args := []any{}
	if !platformAdmin {
		query += ` WHERE tenant_id = $1`
		args = append(args, strings.TrimSpace(tenantID))
	}
	query += ` ORDER BY updated_at DESC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	updaters := []model.NodeUpdater{}
	for rows.Next() {
		updater, err := scanNodeUpdater(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		updaters = append(updaters, redactNodeUpdater(updater))
	}
	return updaters, mapDBErr(rows.Err())
}

func (s *Store) pgCreateNodeUpdateTask(principal model.Principal, updaterID, clusterNodeName, runtimeID, taskType string, payload map[string]string) (model.NodeUpdateTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	updater, err := s.pgFindNodeUpdaterTarget(ctx, s.db, updaterID, clusterNodeName, runtimeID)
	if err != nil {
		return model.NodeUpdateTask{}, err
	}
	if !principal.IsPlatformAdmin() && strings.TrimSpace(updater.TenantID) != strings.TrimSpace(principal.TenantID) {
		return model.NodeUpdateTask{}, ErrNotFound
	}
	now := time.Now().UTC()
	task := model.NodeUpdateTask{
		ID:              model.NewID("nodeupdate"),
		TenantID:        strings.TrimSpace(updater.TenantID),
		NodeUpdaterID:   updater.ID,
		MachineID:       updater.MachineID,
		RuntimeID:       updater.RuntimeID,
		NodeKeyID:       updater.NodeKeyID,
		ClusterNodeName: updater.ClusterNodeName,
		Type:            taskType,
		Status:          model.NodeUpdateTaskStatusPending,
		Payload:         cloneMap(payload),
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	payloadJSON, err := marshalNullableJSON(task.Payload)
	if err != nil {
		return model.NodeUpdateTask{}, err
	}
	logsJSON, err := marshalNullableJSON(task.Logs)
	if err != nil {
		return model.NodeUpdateTask{}, err
	}
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO fugue_node_update_tasks (
	id, tenant_id, node_updater_id, machine_id, runtime_id, node_key_id,
	cluster_node_name, task_type, status, payload_json, result_message,
	error_message, logs_json, requested_by_type, requested_by_id, created_at,
	updated_at, claimed_at, completed_at
) VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9, $10, $11,
	$12, $13, $14, $15, $16,
	$17, $18, $19
)`, task.ID, nullIfEmpty(task.TenantID), task.NodeUpdaterID, task.MachineID, task.RuntimeID, nullIfEmpty(task.NodeKeyID), task.ClusterNodeName, task.Type, task.Status, payloadJSON, task.ResultMessage, task.ErrorMessage, logsJSON, task.RequestedByType, task.RequestedByID, task.CreatedAt, task.UpdatedAt, task.ClaimedAt, task.CompletedAt); err != nil {
		return model.NodeUpdateTask{}, mapDBErr(err)
	}
	return redactNodeUpdateTask(task), nil
}

func (s *Store) pgListNodeUpdateTasks(tenantID string, platformAdmin bool, updaterID, status string) ([]model.NodeUpdateTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clauses := []string{}
	args := []any{}
	if !platformAdmin {
		args = append(args, strings.TrimSpace(tenantID))
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if strings.TrimSpace(updaterID) != "" {
		args = append(args, strings.TrimSpace(updaterID))
		clauses = append(clauses, fmt.Sprintf("node_updater_id = $%d", len(args)))
	}
	if normalized := normalizeNodeUpdateTaskStatus(status); normalized != "" {
		args = append(args, normalized)
		clauses = append(clauses, fmt.Sprintf("status = $%d", len(args)))
	}
	query := `
SELECT id, tenant_id, node_updater_id, machine_id, runtime_id, node_key_id, cluster_node_name, task_type, status, payload_json, result_message, error_message, logs_json, requested_by_type, requested_by_id, created_at, updated_at, claimed_at, completed_at
FROM fugue_node_update_tasks`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at DESC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	return scanNodeUpdateTaskRows(rows)
}

func (s *Store) pgListPendingNodeUpdateTasks(updaterID string, limit int) ([]model.NodeUpdateTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, tenant_id, node_updater_id, machine_id, runtime_id, node_key_id, cluster_node_name, task_type, status, payload_json, result_message, error_message, logs_json, requested_by_type, requested_by_id, created_at, updated_at, claimed_at, completed_at
FROM fugue_node_update_tasks
WHERE node_updater_id = $1 AND status = $2
ORDER BY created_at ASC
LIMIT $3
`, strings.TrimSpace(updaterID), model.NodeUpdateTaskStatusPending, limit)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	return scanNodeUpdateTaskRows(rows)
}

func (s *Store) pgClaimNodeUpdateTask(taskID, updaterID string) (model.NodeUpdateTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := time.Now().UTC()
	task, err := scanNodeUpdateTask(s.db.QueryRowContext(ctx, `
UPDATE fugue_node_update_tasks
SET status = $3, claimed_at = COALESCE(claimed_at, $4), updated_at = $4
WHERE id = $1 AND node_updater_id = $2 AND status IN ($5, $3)
RETURNING id, tenant_id, node_updater_id, machine_id, runtime_id, node_key_id, cluster_node_name, task_type, status, payload_json, result_message, error_message, logs_json, requested_by_type, requested_by_id, created_at, updated_at, claimed_at, completed_at
`, strings.TrimSpace(taskID), strings.TrimSpace(updaterID), model.NodeUpdateTaskStatusRunning, now, model.NodeUpdateTaskStatusPending))
	if err != nil {
		return model.NodeUpdateTask{}, mapDBErr(err)
	}
	return redactNodeUpdateTask(task), nil
}

func (s *Store) pgAppendNodeUpdateTaskLog(taskID, updaterID, message string) (model.NodeUpdateTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	current, err := s.pgGetNodeUpdateTaskForUpdater(ctx, taskID, updaterID)
	if err != nil {
		return model.NodeUpdateTask{}, err
	}
	now := time.Now().UTC()
	current.Logs = append(current.Logs, model.NodeUpdateTaskLog{At: now, Message: strings.TrimSpace(message)})
	logsJSON, err := marshalNullableJSON(current.Logs)
	if err != nil {
		return model.NodeUpdateTask{}, err
	}
	task, err := scanNodeUpdateTask(s.db.QueryRowContext(ctx, `
UPDATE fugue_node_update_tasks
SET logs_json = $3, updated_at = $4
WHERE id = $1 AND node_updater_id = $2
RETURNING id, tenant_id, node_updater_id, machine_id, runtime_id, node_key_id, cluster_node_name, task_type, status, payload_json, result_message, error_message, logs_json, requested_by_type, requested_by_id, created_at, updated_at, claimed_at, completed_at
`, strings.TrimSpace(taskID), strings.TrimSpace(updaterID), logsJSON, now))
	if err != nil {
		return model.NodeUpdateTask{}, mapDBErr(err)
	}
	return redactNodeUpdateTask(task), nil
}

func (s *Store) pgCompleteNodeUpdateTask(taskID, updaterID, status, message, errorMessage string) (model.NodeUpdateTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	normalizedStatus := normalizeTerminalNodeUpdateTaskStatus(status)
	if normalizedStatus == "" {
		return model.NodeUpdateTask{}, ErrInvalidInput
	}
	now := time.Now().UTC()
	task, err := scanNodeUpdateTask(s.db.QueryRowContext(ctx, `
UPDATE fugue_node_update_tasks
SET status = $3, result_message = $4, error_message = $5, completed_at = $6, updated_at = $6
WHERE id = $1 AND node_updater_id = $2 AND status = $7
RETURNING id, tenant_id, node_updater_id, machine_id, runtime_id, node_key_id, cluster_node_name, task_type, status, payload_json, result_message, error_message, logs_json, requested_by_type, requested_by_id, created_at, updated_at, claimed_at, completed_at
`, strings.TrimSpace(taskID), strings.TrimSpace(updaterID), normalizedStatus, strings.TrimSpace(message), strings.TrimSpace(errorMessage), now, model.NodeUpdateTaskStatusRunning))
	if err != nil {
		return model.NodeUpdateTask{}, mapDBErr(err)
	}
	return redactNodeUpdateTask(task), nil
}

func (s *Store) pgFindNodeUpdaterTarget(ctx context.Context, q sqlQueryer, updaterID, clusterNodeName, runtimeID string) (model.NodeUpdater, error) {
	clauses := []string{}
	args := []any{}
	if strings.TrimSpace(updaterID) != "" {
		args = append(args, strings.TrimSpace(updaterID))
		clauses = append(clauses, fmt.Sprintf("id = $%d", len(args)))
	}
	if strings.TrimSpace(clusterNodeName) != "" {
		args = append(args, strings.TrimSpace(clusterNodeName))
		clauses = append(clauses, fmt.Sprintf("cluster_node_name = $%d", len(args)))
	}
	if strings.TrimSpace(runtimeID) != "" {
		args = append(args, strings.TrimSpace(runtimeID))
		clauses = append(clauses, fmt.Sprintf("runtime_id = $%d", len(args)))
	}
	if len(clauses) == 0 {
		return model.NodeUpdater{}, ErrInvalidInput
	}
	query := `
SELECT id, tenant_id, node_key_id, machine_id, runtime_id, cluster_node_name, status, token_prefix, token_hash, labels_json, capabilities_json, updater_version, join_script_version, k3s_version, os, arch, last_error, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_node_updaters
WHERE ` + strings.Join(clauses, " OR ") + `
ORDER BY updated_at DESC
LIMIT 1`
	updater, err := scanNodeUpdater(q.QueryRowContext(ctx, query, args...))
	if err != nil {
		return model.NodeUpdater{}, mapDBErr(err)
	}
	return updater, nil
}

func (s *Store) pgGetNodeUpdateTaskForUpdater(ctx context.Context, taskID, updaterID string) (model.NodeUpdateTask, error) {
	task, err := scanNodeUpdateTask(s.db.QueryRowContext(ctx, `
SELECT id, tenant_id, node_updater_id, machine_id, runtime_id, node_key_id, cluster_node_name, task_type, status, payload_json, result_message, error_message, logs_json, requested_by_type, requested_by_id, created_at, updated_at, claimed_at, completed_at
FROM fugue_node_update_tasks
WHERE id = $1 AND node_updater_id = $2
`, strings.TrimSpace(taskID), strings.TrimSpace(updaterID)))
	if err != nil {
		return model.NodeUpdateTask{}, mapDBErr(err)
	}
	return task, nil
}

func scanNodeUpdateTaskRows(rows *sql.Rows) ([]model.NodeUpdateTask, error) {
	tasks := []model.NodeUpdateTask{}
	for rows.Next() {
		task, err := scanNodeUpdateTask(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		tasks = append(tasks, redactNodeUpdateTask(task))
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return tasks, nil
}

func scanNodeUpdater(scanner sqlScanner) (model.NodeUpdater, error) {
	var updater model.NodeUpdater
	var tenantID sql.NullString
	var nodeKeyID sql.NullString
	var labelsRaw []byte
	var capabilitiesRaw []byte
	var lastSeenAt sql.NullTime
	var lastHeartbeatAt sql.NullTime
	if err := scanner.Scan(
		&updater.ID,
		&tenantID,
		&nodeKeyID,
		&updater.MachineID,
		&updater.RuntimeID,
		&updater.ClusterNodeName,
		&updater.Status,
		&updater.TokenPrefix,
		&updater.TokenHash,
		&labelsRaw,
		&capabilitiesRaw,
		&updater.UpdaterVersion,
		&updater.JoinScriptVersion,
		&updater.K3SVersion,
		&updater.OS,
		&updater.Arch,
		&updater.LastError,
		&lastSeenAt,
		&lastHeartbeatAt,
		&updater.CreatedAt,
		&updater.UpdatedAt,
	); err != nil {
		return model.NodeUpdater{}, err
	}
	updater.TenantID = tenantID.String
	updater.NodeKeyID = nodeKeyID.String
	labels, err := decodeJSONValue[map[string]string](labelsRaw)
	if err != nil {
		return model.NodeUpdater{}, err
	}
	capabilities, err := decodeJSONValue[[]string](capabilitiesRaw)
	if err != nil {
		return model.NodeUpdater{}, err
	}
	updater.Labels = labels
	updater.Capabilities = normalizeStringList(capabilities)
	if lastSeenAt.Valid {
		updater.LastSeenAt = &lastSeenAt.Time
	}
	if lastHeartbeatAt.Valid {
		updater.LastHeartbeatAt = &lastHeartbeatAt.Time
	}
	return updater, nil
}

func scanNodeUpdateTask(scanner sqlScanner) (model.NodeUpdateTask, error) {
	var task model.NodeUpdateTask
	var tenantID sql.NullString
	var nodeKeyID sql.NullString
	var payloadRaw []byte
	var logsRaw []byte
	var claimedAt sql.NullTime
	var completedAt sql.NullTime
	if err := scanner.Scan(
		&task.ID,
		&tenantID,
		&task.NodeUpdaterID,
		&task.MachineID,
		&task.RuntimeID,
		&nodeKeyID,
		&task.ClusterNodeName,
		&task.Type,
		&task.Status,
		&payloadRaw,
		&task.ResultMessage,
		&task.ErrorMessage,
		&logsRaw,
		&task.RequestedByType,
		&task.RequestedByID,
		&task.CreatedAt,
		&task.UpdatedAt,
		&claimedAt,
		&completedAt,
	); err != nil {
		return model.NodeUpdateTask{}, err
	}
	task.TenantID = tenantID.String
	task.NodeKeyID = nodeKeyID.String
	payload, err := decodeJSONValue[map[string]string](payloadRaw)
	if err != nil {
		return model.NodeUpdateTask{}, err
	}
	logs, err := decodeJSONValue[[]model.NodeUpdateTaskLog](logsRaw)
	if err != nil {
		return model.NodeUpdateTask{}, err
	}
	task.Payload = payload
	task.Logs = logs
	if claimedAt.Valid {
		task.ClaimedAt = &claimedAt.Time
	}
	if completedAt.Valid {
		task.CompletedAt = &completedAt.Time
	}
	return task, nil
}
