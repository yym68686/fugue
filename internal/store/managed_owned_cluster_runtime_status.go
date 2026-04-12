package store

import (
	"context"
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
			if syncManagedOwnedClusterRuntimeStatus(&state.Runtimes[idx], nodeReadyByName, now) {
				count++
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
		if !syncManagedOwnedClusterRuntimeStatus(&runtimeObj, nodeReadyByName, now) {
			continue
		}
		if err := s.pgUpdateRuntimeTx(ctx, tx, runtimeObj); err != nil {
			return 0, err
		}
		count++
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit sync managed-owned cluster runtimes transaction: %w", err)
	}
	return count, nil
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
	if runtimeObj.Status == nextStatus {
		return false
	}

	runtimeObj.Status = nextStatus
	runtimeObj.UpdatedAt = now
	return true
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
