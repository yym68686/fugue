package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) SyncManagedOwnedClusterRuntimeStatuses(nodeReadyByName map[string]bool) (int, error) {
	if s.usingDatabase() {
		return s.pgSyncManagedOwnedClusterRuntimeStatuses(nodeReadyByName)
	}

	var count int
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		now := time.Now().UTC()
		for idx := range state.Runtimes {
			if state.Runtimes[idx].Type != model.RuntimeTypeManagedOwned {
				continue
			}
			if syncManagedOwnedClusterRuntimeStatus(&state.Runtimes[idx], nodeReadyByName, now) {
				count++
			}
			// The machine record is a read-model of the runtime registration. Keep
			// its serving status aligned even when the runtime status was already
			// correct (for example after an older controller missed the projection).
			if machineIndex := findMachineByRuntimeID(state, state.Runtimes[idx].ID); machineIndex >= 0 {
				state.Machines[machineIndex].Status = state.Runtimes[idx].Status
				if state.Runtimes[idx].LastSeenAt != nil {
					lastSeen := state.Runtimes[idx].LastSeenAt.UTC()
					state.Machines[machineIndex].LastSeenAt = &lastSeen
				}
				state.Machines[machineIndex].UpdatedAt = now
			}
		}
		return nil
	})
	return count, err
}

func (s *Store) pgSyncManagedOwnedClusterRuntimeStatuses(nodeReadyByName map[string]bool) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin sync managed-owned cluster runtimes transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, name, machine_name, type, access_mode, public_offer_json, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE type = $1
  AND COALESCE(cluster_node_name, '') <> ''
FOR UPDATE
`, model.RuntimeTypeManagedOwned)
	if err != nil {
		return 0, fmt.Errorf("list managed-owned cluster runtimes: %w", err)
	}
	defer rows.Close()

	runtimes := make([]model.Runtime, 0)
	for rows.Next() {
		runtimeObj, err := scanRuntime(rows)
		if err != nil {
			return 0, err
		}
		runtimes = append(runtimes, runtimeObj)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate managed-owned cluster runtimes: %w", err)
	}

	now := time.Now().UTC()
	count := 0
	for _, runtimeObj := range runtimes {
		runtimeChanged := syncManagedOwnedClusterRuntimeStatus(&runtimeObj, nodeReadyByName, now)
		if runtimeChanged {
			if err := s.pgUpdateRuntimeTx(ctx, tx, runtimeObj); err != nil {
				return 0, err
			}
			count++
		}
		if err := s.pgSyncManagedOwnedMachineStatusTx(ctx, tx, runtimeObj); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit sync managed-owned cluster runtimes transaction: %w", err)
	}
	return count, nil
}

func (s *Store) pgSyncManagedOwnedMachineStatusTx(ctx context.Context, tx *sql.Tx, runtimeObj model.Runtime) error {
	if runtimeObj.ID == "" || runtimeObj.Type != model.RuntimeTypeManagedOwned {
		return nil
	}
	lastSeen := runtimeObj.LastSeenAt
	if lastSeen == nil {
		lastSeen = runtimeObj.LastHeartbeatAt
	}
	if lastSeen != nil {
		_, err := tx.ExecContext(ctx, `
UPDATE fugue_machines
SET status = $2, last_seen_at = $3, updated_at = $4
WHERE runtime_id = $1
	AND (status IS DISTINCT FROM $2 OR last_seen_at IS DISTINCT FROM $3)
`, runtimeObj.ID, runtimeObj.Status, lastSeen, time.Now().UTC())
		return err
	}
	_, err := tx.ExecContext(ctx, `
UPDATE fugue_machines
SET status = $2, updated_at = $3
WHERE runtime_id = $1
	AND status IS DISTINCT FROM $2
`, runtimeObj.ID, runtimeObj.Status, time.Now().UTC())
	return err
}

func syncManagedOwnedClusterRuntimeStatus(runtimeObj *model.Runtime, nodeReadyByName map[string]bool, now time.Time) bool {
	if runtimeObj == nil || runtimeObj.Type != model.RuntimeTypeManagedOwned {
		return false
	}

	clusterNodeName := managedOwnedClusterRuntimeNodeName(*runtimeObj)
	if clusterNodeName == "" {
		return false
	}

	nextStatus := model.RuntimeStatusOffline
	if nodeReadyByName[clusterNodeName] {
		nextStatus = model.RuntimeStatusActive
	}
	changed := runtimeObj.Status != nextStatus
	if runtimeObj.Status != nextStatus {
		runtimeObj.Status = nextStatus
	}
	if nextStatus == model.RuntimeStatusActive {
		if runtimeObj.LastSeenAt == nil || !runtimeObj.LastSeenAt.Equal(now) {
			runtimeObj.LastSeenAt = &now
			changed = true
		}
		if runtimeObj.LastHeartbeatAt == nil || !runtimeObj.LastHeartbeatAt.Equal(now) {
			runtimeObj.LastHeartbeatAt = &now
			changed = true
		}
	}
	if changed {
		runtimeObj.UpdatedAt = now
	}
	return changed
}

func managedOwnedClusterRuntimeNodeName(runtimeObj model.Runtime) string {
	if clusterNodeName := strings.TrimSpace(runtimeObj.ClusterNodeName); clusterNodeName != "" {
		return clusterNodeName
	}
	if strings.TrimSpace(runtimeObj.NodeKeyID) != "" || strings.TrimSpace(runtimeObj.FingerprintHash) != "" {
		return strings.TrimSpace(runtimeObj.Name)
	}
	return ""
}
