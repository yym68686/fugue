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

func (s *Store) pgListMachines(tenantID string, platformAdmin bool) ([]model.Machine, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, name, scope, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, policy_json, last_seen_at, created_at, updated_at
FROM fugue_machines
`
	args := make([]any, 0, 1)
	if !platformAdmin {
		query += ` WHERE tenant_id = $1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list machines: %w", err)
	}
	defer rows.Close()

	machines := make([]model.Machine, 0)
	for rows.Next() {
		machine, err := scanMachine(rows)
		if err != nil {
			return nil, err
		}
		machines = append(machines, machine)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate machines: %w", err)
	}
	return machines, nil
}

func (s *Store) pgListMachinesByNodeKey(nodeKeyID, tenantID string, platformAdmin bool) ([]model.Machine, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, name, scope, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, policy_json, last_seen_at, created_at, updated_at
FROM fugue_machines
WHERE node_key_id = $1
`
	args := []any{nodeKeyID}
	if !platformAdmin {
		query += ` AND tenant_id = $2`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list machines by node key: %w", err)
	}
	defer rows.Close()

	machines := make([]model.Machine, 0)
	for rows.Next() {
		machine, err := scanMachine(rows)
		if err != nil {
			return nil, err
		}
		machines = append(machines, machine)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate machines by node key: %w", err)
	}
	return machines, nil
}

func (s *Store) pgGetMachineByClusterNodeName(name string) (model.Machine, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if machine, found, err := s.pgFindMachineByClusterNodeNameTx(ctx, nil, name, false); err != nil {
		return model.Machine{}, err
	} else if found {
		return machine, nil
	}

	runtimeObj, found, err := s.pgFindNodeRuntimeByClusterNodeNameTx(ctx, nil, name, false)
	if err != nil {
		return model.Machine{}, err
	}
	if !found {
		return model.Machine{}, ErrNotFound
	}
	return machineFromRuntime(runtimeObj, nil, runtimeObj.UpdatedAt), nil
}

func (s *Store) pgEnsureMachineForRuntime(runtimeID string) (model.Machine, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Machine{}, fmt.Errorf("begin ensure machine for runtime transaction: %w", err)
	}
	defer tx.Rollback()

	runtimeObj, err := s.pgGetRuntimeTx(ctx, tx, runtimeID, true)
	if err != nil {
		return model.Machine{}, err
	}
	if runtimeObj.Type != model.RuntimeTypeManagedOwned && runtimeObj.Type != model.RuntimeTypeExternalOwned {
		return model.Machine{}, ErrConflict
	}
	machine, err := s.pgUpsertMachineFromRuntimeTx(ctx, tx, runtimeObj)
	if err != nil {
		return model.Machine{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Machine{}, fmt.Errorf("commit ensure machine for runtime transaction: %w", err)
	}
	return machine, nil
}

func (s *Store) pgEnsurePlatformMachineForClusterNode(nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.Machine, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Machine{}, fmt.Errorf("begin ensure platform machine transaction: %w", err)
	}
	defer tx.Rollback()

	machine, err := s.pgUpsertPlatformClusterMachineTx(ctx, tx, "", nodeName, endpoint, labels, machineName, machineFingerprint)
	if err != nil {
		return model.Machine{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Machine{}, fmt.Errorf("commit ensure platform machine transaction: %w", err)
	}
	return machine, nil
}

func (s *Store) pgSetMachinePolicyByClusterNodeName(name string, policy model.MachinePolicy) (model.Machine, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Machine{}, fmt.Errorf("begin set machine policy transaction: %w", err)
	}
	defer tx.Rollback()

	machine, found, err := s.pgFindMachineByClusterNodeNameTx(ctx, tx, name, true)
	if err != nil {
		return model.Machine{}, err
	}
	if !found {
		runtimeObj, runtimeFound, err := s.pgFindNodeRuntimeByClusterNodeNameTx(ctx, tx, name, true)
		if err != nil {
			return model.Machine{}, err
		}
		if !runtimeFound {
			return model.Machine{}, ErrNotFound
		}
		machine, err = s.pgUpsertMachineFromRuntimeTx(ctx, tx, runtimeObj)
		if err != nil {
			return model.Machine{}, err
		}
	}

	machine.Policy = normalizeMachinePolicy(machine.Scope, policy)
	machine.UpdatedAt = time.Now().UTC()
	if err := s.pgUpsertMachineTx(ctx, tx, machine); err != nil {
		return model.Machine{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Machine{}, fmt.Errorf("commit set machine policy transaction: %w", err)
	}
	return machine, nil
}

func (s *Store) pgAttachPlatformClusterMachine(key model.NodeKey, nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.Machine, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Machine{}, fmt.Errorf("begin attach platform cluster machine transaction: %w", err)
	}
	defer tx.Rollback()

	machine, err := s.pgUpsertPlatformClusterMachineTx(ctx, tx, key.ID, nodeName, endpoint, labels, machineName, machineFingerprint)
	if err != nil {
		return model.Machine{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Machine{}, fmt.Errorf("commit attach platform cluster machine transaction: %w", err)
	}
	return machine, nil
}

func (s *Store) pgFindMachineByRuntimeIDTx(ctx context.Context, tx *sql.Tx, runtimeID string, forUpdate bool) (model.Machine, bool, error) {
	if strings.TrimSpace(runtimeID) == "" {
		return model.Machine{}, false, nil
	}
	query := `
SELECT id, tenant_id, name, scope, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, policy_json, last_seen_at, created_at, updated_at
FROM fugue_machines
WHERE runtime_id = $1
`
	if forUpdate {
		query += ` FOR UPDATE`
	}

	var (
		machine model.Machine
		err     error
	)
	if tx != nil {
		machine, err = scanMachine(tx.QueryRowContext(ctx, query, runtimeID))
	} else {
		machine, err = scanMachine(s.db.QueryRowContext(ctx, query, runtimeID))
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Machine{}, false, nil
		}
		return model.Machine{}, false, fmt.Errorf("find machine by runtime id %s: %w", runtimeID, err)
	}
	return machine, true, nil
}

func (s *Store) pgFindMachineByFingerprintTx(ctx context.Context, tx *sql.Tx, tenantID, fingerprintHash string, forUpdate bool) (model.Machine, bool, error) {
	if strings.TrimSpace(fingerprintHash) == "" {
		return model.Machine{}, false, nil
	}
	query := `
SELECT id, tenant_id, name, scope, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, policy_json, last_seen_at, created_at, updated_at
FROM fugue_machines
WHERE COALESCE(tenant_id, '') = $1
  AND fingerprint_hash = $2
`
	if forUpdate {
		query += ` FOR UPDATE`
	}

	var (
		machine model.Machine
		err     error
	)
	if tx != nil {
		machine, err = scanMachine(tx.QueryRowContext(ctx, query, tenantID, fingerprintHash))
	} else {
		machine, err = scanMachine(s.db.QueryRowContext(ctx, query, tenantID, fingerprintHash))
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Machine{}, false, nil
		}
		return model.Machine{}, false, fmt.Errorf("find machine by fingerprint hash: %w", err)
	}
	return machine, true, nil
}

func (s *Store) pgFindMachineByNodeKeyAndClusterNodeNameTx(ctx context.Context, tx *sql.Tx, nodeKeyID, nodeName string, forUpdate bool) (model.Machine, bool, error) {
	if strings.TrimSpace(nodeKeyID) == "" || strings.TrimSpace(nodeName) == "" {
		return model.Machine{}, false, nil
	}
	query := `
SELECT id, tenant_id, name, scope, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, policy_json, last_seen_at, created_at, updated_at
FROM fugue_machines
WHERE node_key_id = $1
  AND lower(cluster_node_name) = lower($2)
`
	if forUpdate {
		query += ` FOR UPDATE`
	}

	var (
		machine model.Machine
		err     error
	)
	if tx != nil {
		machine, err = scanMachine(tx.QueryRowContext(ctx, query, nodeKeyID, nodeName))
	} else {
		machine, err = scanMachine(s.db.QueryRowContext(ctx, query, nodeKeyID, nodeName))
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Machine{}, false, nil
		}
		return model.Machine{}, false, fmt.Errorf("find machine by node key and cluster node name: %w", err)
	}
	return machine, true, nil
}

func (s *Store) pgFindMachineByClusterNodeNameTx(ctx context.Context, tx *sql.Tx, name string, forUpdate bool) (model.Machine, bool, error) {
	query := `
SELECT id, tenant_id, name, scope, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, policy_json, last_seen_at, created_at, updated_at
FROM fugue_machines
WHERE lower(cluster_node_name) = lower($1)
`
	if forUpdate {
		query += ` FOR UPDATE`
	}

	var (
		machine model.Machine
		err     error
	)
	if tx != nil {
		machine, err = scanMachine(tx.QueryRowContext(ctx, query, name))
	} else {
		machine, err = scanMachine(s.db.QueryRowContext(ctx, query, name))
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Machine{}, false, nil
		}
		return model.Machine{}, false, fmt.Errorf("find machine by cluster node name %s: %w", name, err)
	}
	return machine, true, nil
}

func (s *Store) pgFindPlatformMachineByClusterNodeNameTx(ctx context.Context, tx *sql.Tx, name string, forUpdate bool) (model.Machine, bool, error) {
	query := `
SELECT id, tenant_id, name, scope, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, policy_json, last_seen_at, created_at, updated_at
FROM fugue_machines
WHERE scope = $1
  AND lower(cluster_node_name) = lower($2)
`
	if forUpdate {
		query += ` FOR UPDATE`
	}

	var (
		machine model.Machine
		err     error
	)
	args := []any{model.MachineScopePlatformNode, name}
	if tx != nil {
		machine, err = scanMachine(tx.QueryRowContext(ctx, query, args...))
	} else {
		machine, err = scanMachine(s.db.QueryRowContext(ctx, query, args...))
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Machine{}, false, nil
		}
		return model.Machine{}, false, fmt.Errorf("find platform machine by cluster node name %s: %w", name, err)
	}
	return machine, true, nil
}

func (s *Store) pgFindNodeRuntimeByClusterNodeNameTx(ctx context.Context, tx *sql.Tx, name string, forUpdate bool) (model.Runtime, bool, error) {
	query := `
SELECT id, tenant_id, name, machine_name, type, access_mode, public_offer_json, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE lower(cluster_node_name) = lower($1)
  AND type IN ($2, $3)
ORDER BY updated_at DESC, created_at DESC
LIMIT 1
`
	if forUpdate {
		query += ` FOR UPDATE`
	}

	var (
		runtimeObj model.Runtime
		err        error
	)
	args := []any{name, model.RuntimeTypeManagedOwned, model.RuntimeTypeExternalOwned}
	if tx != nil {
		runtimeObj, err = scanRuntime(tx.QueryRowContext(ctx, query, args...))
	} else {
		runtimeObj, err = scanRuntime(s.db.QueryRowContext(ctx, query, args...))
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Runtime{}, false, nil
		}
		return model.Runtime{}, false, fmt.Errorf("find runtime by cluster node name %s: %w", name, err)
	}
	return runtimeObj, true, nil
}

func (s *Store) pgUpsertMachineFromRuntimeTx(ctx context.Context, tx *sql.Tx, runtimeObj model.Runtime) (model.Machine, error) {
	existing, found, err := s.pgFindMachineByRuntimeIDTx(ctx, tx, runtimeObj.ID, true)
	if err != nil {
		return model.Machine{}, err
	}
	if !found {
		existing, found, err = s.pgFindMachineByFingerprintTx(ctx, tx, runtimeObj.TenantID, runtimeObj.FingerprintHash, true)
		if err != nil {
			return model.Machine{}, err
		}
	}

	var existingPtr *model.Machine
	if found {
		existingPtr = &existing
	}
	machine := machineFromRuntime(runtimeObj, existingPtr, time.Now().UTC())
	if err := s.pgUpsertMachineTx(ctx, tx, machine); err != nil {
		return model.Machine{}, err
	}
	return machine, nil
}

func (s *Store) pgUpsertPlatformClusterMachineTx(ctx context.Context, tx *sql.Tx, nodeKeyID, nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.Machine, error) {
	normalizedNodeName, err := normalizeClusterNodeName(nodeName)
	if err != nil {
		return model.Machine{}, err
	}
	machineFingerprint = normalizedMachineFingerprint(machineFingerprint, machineName, normalizedNodeName, endpoint)
	fingerprintHash := model.HashSecret(machineFingerprint)

	existing, found, err := s.pgFindPlatformMachineByClusterNodeNameTx(ctx, tx, normalizedNodeName, true)
	if err != nil {
		return model.Machine{}, err
	}
	if !found {
		existing, found, err = s.pgFindMachineByFingerprintTx(ctx, tx, "", fingerprintHash, true)
		if err != nil {
			return model.Machine{}, err
		}
	}
	if !found && strings.TrimSpace(nodeKeyID) != "" {
		existing, found, err = s.pgFindMachineByNodeKeyAndClusterNodeNameTx(ctx, tx, nodeKeyID, normalizedNodeName, true)
		if err != nil {
			return model.Machine{}, err
		}
	}

	var existingPtr *model.Machine
	if found {
		existingPtr = &existing
	}
	machine, err := buildPlatformMachineRecord(nodeKeyID, normalizedNodeName, endpoint, labels, machineName, machineFingerprint, existingPtr, time.Now().UTC())
	if err != nil {
		return model.Machine{}, err
	}
	if err := s.pgUpsertMachineTx(ctx, tx, machine); err != nil {
		return model.Machine{}, err
	}
	return machine, nil
}

func (s *Store) pgUpsertMachineTx(ctx context.Context, tx *sql.Tx, machine model.Machine) error {
	labelsJSON, err := marshalJSON(machine.Labels)
	if err != nil {
		return err
	}
	policyJSON, err := marshalJSON(machine.Policy)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_machines (id, tenant_id, name, scope, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, policy_json, last_seen_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
ON CONFLICT (id) DO UPDATE SET
	tenant_id = EXCLUDED.tenant_id,
	name = EXCLUDED.name,
	scope = EXCLUDED.scope,
	connection_mode = EXCLUDED.connection_mode,
	status = EXCLUDED.status,
	endpoint = EXCLUDED.endpoint,
	labels_json = EXCLUDED.labels_json,
	node_key_id = EXCLUDED.node_key_id,
	runtime_id = EXCLUDED.runtime_id,
	runtime_name = EXCLUDED.runtime_name,
	cluster_node_name = EXCLUDED.cluster_node_name,
	fingerprint_prefix = EXCLUDED.fingerprint_prefix,
	fingerprint_hash = EXCLUDED.fingerprint_hash,
	policy_json = EXCLUDED.policy_json,
	last_seen_at = EXCLUDED.last_seen_at,
	updated_at = EXCLUDED.updated_at
`, machine.ID, nullIfEmpty(machine.TenantID), machine.Name, machine.Scope, machine.ConnectionMode, machine.Status, machine.Endpoint, labelsJSON, nullIfEmpty(machine.NodeKeyID), machine.RuntimeID, machine.RuntimeName, machine.ClusterNodeName, machine.FingerprintPrefix, machine.FingerprintHash, policyJSON, machine.LastSeenAt, machine.CreatedAt, machine.UpdatedAt); err != nil {
		return fmt.Errorf("upsert machine %s: %w", machine.ID, err)
	}
	return nil
}

func scanMachine(scanner sqlScanner) (model.Machine, error) {
	var machine model.Machine
	var tenantID sql.NullString
	var scope sql.NullString
	var endpoint sql.NullString
	var labelsRaw []byte
	var nodeKeyID sql.NullString
	var runtimeID sql.NullString
	var runtimeName sql.NullString
	var clusterNodeName sql.NullString
	var fingerprintPrefix sql.NullString
	var fingerprintHash sql.NullString
	var policyRaw []byte
	var lastSeenAt sql.NullTime
	if err := scanner.Scan(
		&machine.ID,
		&tenantID,
		&machine.Name,
		&scope,
		&machine.ConnectionMode,
		&machine.Status,
		&endpoint,
		&labelsRaw,
		&nodeKeyID,
		&runtimeID,
		&runtimeName,
		&clusterNodeName,
		&fingerprintPrefix,
		&fingerprintHash,
		&policyRaw,
		&lastSeenAt,
		&machine.CreatedAt,
		&machine.UpdatedAt,
	); err != nil {
		return model.Machine{}, err
	}
	machine.TenantID = tenantID.String
	machine.Scope = model.NormalizeMachineScope(scope.String)
	machine.Endpoint = endpoint.String
	machine.NodeKeyID = nodeKeyID.String
	machine.RuntimeID = runtimeID.String
	machine.RuntimeName = runtimeName.String
	machine.ClusterNodeName = clusterNodeName.String
	machine.FingerprintPrefix = fingerprintPrefix.String
	machine.FingerprintHash = fingerprintHash.String
	labels, err := decodeJSONValue[map[string]string](labelsRaw)
	if err != nil {
		return model.Machine{}, err
	}
	machine.Labels = labels
	policy, err := decodeJSONValue[model.MachinePolicy](policyRaw)
	if err != nil {
		return model.Machine{}, err
	}
	machine.Policy = normalizeMachinePolicy(machine.Scope, policy)
	if lastSeenAt.Valid {
		machine.LastSeenAt = &lastSeenAt.Time
	}
	normalizeMachineForRead(&machine)
	return machine, nil
}
