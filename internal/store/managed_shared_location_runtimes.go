package store

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

const (
	managedSharedRuntimeID                 = "runtime_managed_shared"
	managedSharedLocationRuntimeIDPrefix   = "runtime_managed_shared_loc_"
	managedSharedLocationRuntimeNamePrefix = "managed-shared-"
	managedSharedLocationKeyLabelKey       = "fugue.io/internal-cluster-location-key"
)

type managedSharedLocationRuntimeSpec struct {
	ID     string
	Key    string
	Name   string
	Labels map[string]string
}

func (s *Store) SyncManagedSharedLocationRuntimes(locations []map[string]string) error {
	normalized := normalizeManagedSharedLocationRuntimeSet(locations)
	if s.usingDatabase() {
		return s.pgSyncManagedSharedLocationRuntimes(normalized)
	}

	return s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)

		baseIndex := findRuntime(state, managedSharedRuntimeID)
		if baseIndex < 0 {
			return ErrNotFound
		}

		now := time.Now().UTC()
		syncManagedSharedBaseRuntime(&state.Runtimes[baseIndex], now)

		desired := make(map[string]managedSharedLocationRuntimeSpec, len(normalized))
		for _, labels := range normalized {
			spec := buildManagedSharedLocationRuntimeSpec(labels)
			desired[spec.ID] = spec

			index := findRuntime(state, spec.ID)
			if index >= 0 {
				syncManagedSharedLocationRuntime(&state.Runtimes[index], spec, now)
				continue
			}

			state.Runtimes = append(state.Runtimes, model.Runtime{
				ID:         spec.ID,
				Name:       spec.Name,
				Type:       model.RuntimeTypeManagedShared,
				AccessMode: model.RuntimeAccessModePlatformShared,
				PoolMode:   model.RuntimePoolModeDedicated,
				Status:     model.RuntimeStatusActive,
				Endpoint:   "in-cluster",
				Labels:     cloneMap(spec.Labels),
				CreatedAt:  now,
				UpdatedAt:  now,
			})
		}

		for index := len(state.Runtimes) - 1; index >= 0; index-- {
			runtimeObj := state.Runtimes[index]
			if !isManagedSharedLocationRuntime(runtimeObj) {
				continue
			}
			if _, ok := desired[runtimeObj.ID]; ok {
				continue
			}
			if runtimeReferencedByState(state, runtimeObj.ID) {
				if state.Runtimes[index].Status != model.RuntimeStatusOffline {
					state.Runtimes[index].Status = model.RuntimeStatusOffline
					state.Runtimes[index].UpdatedAt = now
				}
				continue
			}
			state.Runtimes = append(state.Runtimes[:index], state.Runtimes[index+1:]...)
		}

		return nil
	})
}

func (s *Store) pgSyncManagedSharedLocationRuntimes(locations []map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sync managed shared location runtimes transaction: %w", err)
	}
	defer tx.Rollback()

	baseRuntime, err := s.pgGetRuntimeTx(ctx, tx, managedSharedRuntimeID, true)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	if syncManagedSharedBaseRuntime(&baseRuntime, now) {
		if err := s.pgUpdateRuntimeTx(ctx, tx, baseRuntime); err != nil {
			return err
		}
	}

	existing, err := s.pgListManagedSharedLocationRuntimesTx(ctx, tx)
	if err != nil {
		return err
	}
	existingByID := make(map[string]model.Runtime, len(existing))
	for _, runtimeObj := range existing {
		existingByID[runtimeObj.ID] = runtimeObj
	}

	desired := make(map[string]managedSharedLocationRuntimeSpec, len(locations))
	for _, labels := range locations {
		spec := buildManagedSharedLocationRuntimeSpec(labels)
		desired[spec.ID] = spec

		runtimeObj, ok := existingByID[spec.ID]
		if ok {
			if syncManagedSharedLocationRuntime(&runtimeObj, spec, now) {
				if err := s.pgUpdateRuntimeTx(ctx, tx, runtimeObj); err != nil {
					return err
				}
			}
			delete(existingByID, spec.ID)
			continue
		}

		if err := s.pgInsertRuntimeTx(ctx, tx, model.Runtime{
			ID:         spec.ID,
			Name:       spec.Name,
			Type:       model.RuntimeTypeManagedShared,
			AccessMode: model.RuntimeAccessModePlatformShared,
			PoolMode:   model.RuntimePoolModeDedicated,
			Status:     model.RuntimeStatusActive,
			Endpoint:   "in-cluster",
			Labels:     cloneMap(spec.Labels),
			CreatedAt:  now,
			UpdatedAt:  now,
		}); err != nil {
			return err
		}
	}

	for _, runtimeObj := range existingByID {
		referenced, err := s.pgRuntimeReferencedTx(ctx, tx, runtimeObj.ID)
		if err != nil {
			return err
		}
		if referenced {
			if runtimeObj.Status != model.RuntimeStatusOffline {
				runtimeObj.Status = model.RuntimeStatusOffline
				runtimeObj.UpdatedAt = now
				if err := s.pgUpdateRuntimeTx(ctx, tx, runtimeObj); err != nil {
					return err
				}
			}
			continue
		}
		if err := s.pgDeleteRuntimeAccessGrantsTx(ctx, tx, runtimeObj.ID); err != nil {
			return err
		}
		if err := s.pgDeleteRuntimeTx(ctx, tx, runtimeObj.ID); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sync managed shared location runtimes transaction: %w", err)
	}
	return nil
}

func normalizeManagedSharedLocationRuntimeSet(locations []map[string]string) []map[string]string {
	if len(locations) == 0 {
		return nil
	}

	byKey := make(map[string]map[string]string, len(locations))
	for _, labels := range locations {
		normalized := normalizeManagedSharedLocationLabels(labels)
		key := managedSharedLocationKey(normalized)
		if key == "" {
			continue
		}
		if _, exists := byKey[key]; exists {
			continue
		}
		byKey[key] = normalized
	}
	if len(byKey) == 0 {
		return nil
	}

	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make([]map[string]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, cloneMap(byKey[key]))
	}
	return out
}

func buildManagedSharedLocationRuntimeSpec(labels map[string]string) managedSharedLocationRuntimeSpec {
	normalized := normalizeManagedSharedLocationLabels(labels)
	key := managedSharedLocationKey(normalized)
	return managedSharedLocationRuntimeSpec{
		ID:     managedSharedLocationRuntimeID(key),
		Key:    key,
		Name:   managedSharedLocationRuntimeName(key),
		Labels: managedSharedLocationRuntimeLabels(key, normalized),
	}
}

func managedSharedLocationRuntimeLabels(key string, labels map[string]string) map[string]string {
	out := map[string]string{
		"managed":                        "true",
		managedSharedLocationKeyLabelKey: key,
	}
	for labelKey, value := range normalizeManagedSharedLocationLabels(labels) {
		out[labelKey] = value
	}
	return out
}

func managedSharedLocationKey(labels map[string]string) string {
	labels = normalizeManagedSharedLocationLabels(labels)
	if value := strings.TrimSpace(labels[runtimepkg.LocationCountryCodeLabelKey]); value != "" {
		return "country:" + strings.ToLower(value)
	}
	if value := strings.TrimSpace(labels[runtimepkg.RegionLabelKey]); value != "" {
		return "region:" + strings.ToLower(value)
	}
	return ""
}

func managedSharedLocationRuntimeID(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return managedSharedLocationRuntimeIDPrefix + "unknown"
	}

	sum := sha1.Sum([]byte(key))
	hash := hex.EncodeToString(sum[:4])
	return managedSharedLocationRuntimeIDPrefix + slugifyManagedSharedLocationKey(key) + "-" + hash
}

func managedSharedLocationRuntimeName(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return managedSharedLocationRuntimeNamePrefix + "location"
	}

	sum := sha1.Sum([]byte(key))
	hash := hex.EncodeToString(sum[:2])
	return managedSharedLocationRuntimeNamePrefix + slugifyManagedSharedLocationKey(key) + "-" + hash
}

func slugifyManagedSharedLocationKey(key string) string {
	key = strings.TrimSpace(strings.ToLower(key))
	if key == "" {
		return "location"
	}

	var builder strings.Builder
	lastHyphen := false
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			builder.WriteRune(r)
			lastHyphen = false
		default:
			if !lastHyphen {
				builder.WriteByte('-')
				lastHyphen = true
			}
		}
	}

	slug := strings.Trim(builder.String(), "-")
	if slug == "" {
		return "location"
	}
	return slug
}

func syncManagedSharedBaseRuntime(runtimeObj *model.Runtime, now time.Time) bool {
	if runtimeObj == nil {
		return false
	}

	changed := false
	if runtimeObj.Type != model.RuntimeTypeManagedShared {
		runtimeObj.Type = model.RuntimeTypeManagedShared
		changed = true
	}
	if runtimeObj.AccessMode != model.RuntimeAccessModePlatformShared {
		runtimeObj.AccessMode = model.RuntimeAccessModePlatformShared
		changed = true
	}
	if runtimeObj.PoolMode != model.RuntimePoolModeDedicated {
		runtimeObj.PoolMode = model.RuntimePoolModeDedicated
		changed = true
	}
	if runtimeObj.Status != model.RuntimeStatusActive {
		runtimeObj.Status = model.RuntimeStatusActive
		changed = true
	}
	if runtimeObj.Endpoint != "in-cluster" {
		runtimeObj.Endpoint = "in-cluster"
		changed = true
	}

	labels := stripManagedSharedPlacementLabels(runtimeObj.Labels)
	if !stringMapEqual(runtimeObj.Labels, labels) {
		runtimeObj.Labels = labels
		changed = true
	}

	if changed {
		runtimeObj.UpdatedAt = now
	}
	return changed
}

func syncManagedSharedLocationRuntime(runtimeObj *model.Runtime, spec managedSharedLocationRuntimeSpec, now time.Time) bool {
	if runtimeObj == nil {
		return false
	}

	changed := false
	if runtimeObj.Name != spec.Name {
		runtimeObj.Name = spec.Name
		changed = true
	}
	if runtimeObj.Type != model.RuntimeTypeManagedShared {
		runtimeObj.Type = model.RuntimeTypeManagedShared
		changed = true
	}
	if runtimeObj.TenantID != "" {
		runtimeObj.TenantID = ""
		changed = true
	}
	if runtimeObj.AccessMode != model.RuntimeAccessModePlatformShared {
		runtimeObj.AccessMode = model.RuntimeAccessModePlatformShared
		changed = true
	}
	if runtimeObj.PoolMode != model.RuntimePoolModeDedicated {
		runtimeObj.PoolMode = model.RuntimePoolModeDedicated
		changed = true
	}
	if runtimeObj.Status != model.RuntimeStatusActive {
		runtimeObj.Status = model.RuntimeStatusActive
		changed = true
	}
	if runtimeObj.Endpoint != "in-cluster" {
		runtimeObj.Endpoint = "in-cluster"
		changed = true
	}
	if runtimeObj.NodeKeyID != "" {
		runtimeObj.NodeKeyID = ""
		changed = true
	}
	if runtimeObj.ClusterNodeName != "" {
		runtimeObj.ClusterNodeName = ""
		changed = true
	}
	if runtimeObj.FingerprintPrefix != "" {
		runtimeObj.FingerprintPrefix = ""
		changed = true
	}
	if runtimeObj.FingerprintHash != "" {
		runtimeObj.FingerprintHash = ""
		changed = true
	}
	if runtimeObj.AgentKeyPrefix != "" {
		runtimeObj.AgentKeyPrefix = ""
		changed = true
	}
	if runtimeObj.AgentKeyHash != "" {
		runtimeObj.AgentKeyHash = ""
		changed = true
	}
	if runtimeObj.LastSeenAt != nil {
		runtimeObj.LastSeenAt = nil
		changed = true
	}
	if runtimeObj.LastHeartbeatAt != nil {
		runtimeObj.LastHeartbeatAt = nil
		changed = true
	}

	labels := cloneMap(spec.Labels)
	if !stringMapEqual(runtimeObj.Labels, labels) {
		runtimeObj.Labels = labels
		changed = true
	}

	if changed {
		runtimeObj.UpdatedAt = now
	}
	return changed
}

func stripManagedSharedPlacementLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}

	out := cloneMap(labels)
	delete(out, runtimepkg.RegionLabelKey)
	delete(out, runtimepkg.LegacyRegionLabelKey)
	delete(out, "region")
	delete(out, runtimepkg.ZoneLabelKey)
	delete(out, runtimepkg.LegacyZoneLabelKey)
	delete(out, "zone")
	delete(out, runtimepkg.LocationCountryCodeLabelKey)
	delete(out, "country_code")
	delete(out, "countryCode")
	delete(out, managedSharedLocationKeyLabelKey)
	if len(out) == 0 {
		return nil
	}
	return out
}

func isManagedSharedLocationRuntime(runtimeObj model.Runtime) bool {
	if runtimeObj.Type != model.RuntimeTypeManagedShared {
		return false
	}
	if strings.HasPrefix(runtimeObj.ID, managedSharedLocationRuntimeIDPrefix) {
		return true
	}
	return strings.TrimSpace(runtimeObj.Labels[managedSharedLocationKeyLabelKey]) != ""
}

func runtimeReferencedByState(state *model.State, runtimeID string) bool {
	runtimeID = strings.TrimSpace(runtimeID)
	if state == nil || runtimeID == "" {
		return false
	}

	for _, app := range state.Apps {
		if !isDeletedApp(app) && strings.TrimSpace(app.Spec.RuntimeID) == runtimeID {
			return true
		}
		if app.Spec.Failover != nil && strings.TrimSpace(app.Spec.Failover.TargetRuntimeID) == runtimeID {
			return true
		}
		if app.Spec.Postgres != nil {
			for _, postgresRuntimeID := range managedPostgresReferencedRuntimeIDs(app.Spec.RuntimeID, *app.Spec.Postgres) {
				if postgresRuntimeID == runtimeID {
					return true
				}
			}
		}
		if strings.TrimSpace(app.Status.CurrentRuntimeID) == runtimeID {
			return true
		}
	}
	for _, service := range state.BackingServices {
		if service.Spec.Postgres == nil || isDeletedBackingService(service) {
			continue
		}
		for _, postgresRuntimeID := range managedPostgresReferencedRuntimeIDs("", *service.Spec.Postgres) {
			if postgresRuntimeID == runtimeID {
				return true
			}
		}
	}
	for _, op := range state.Operations {
		if strings.TrimSpace(op.SourceRuntimeID) == runtimeID {
			return true
		}
		if strings.TrimSpace(op.TargetRuntimeID) == runtimeID {
			return true
		}
		if strings.TrimSpace(op.AssignedRuntimeID) == runtimeID {
			return true
		}
		if op.DesiredSpec != nil && strings.TrimSpace(op.DesiredSpec.RuntimeID) == runtimeID {
			return true
		}
		if op.DesiredSpec != nil && op.DesiredSpec.Failover != nil && strings.TrimSpace(op.DesiredSpec.Failover.TargetRuntimeID) == runtimeID {
			return true
		}
		if op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil {
			for _, postgresRuntimeID := range managedPostgresReferencedRuntimeIDs(op.DesiredSpec.RuntimeID, *op.DesiredSpec.Postgres) {
				if postgresRuntimeID == runtimeID {
					return true
				}
			}
		}
	}
	return false
}

func (s *Store) pgListManagedSharedLocationRuntimesTx(ctx context.Context, tx *sql.Tx) ([]model.Runtime, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE type = $1
  AND id LIKE $2
ORDER BY created_at ASC
FOR UPDATE
`, model.RuntimeTypeManagedShared, managedSharedLocationRuntimeIDPrefix+"%")
	if err != nil {
		return nil, fmt.Errorf("list managed shared location runtimes: %w", err)
	}
	defer rows.Close()

	runtimes := make([]model.Runtime, 0)
	for rows.Next() {
		runtimeObj, err := scanRuntime(rows)
		if err != nil {
			return nil, err
		}
		runtimes = append(runtimes, runtimeObj)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate managed shared location runtimes: %w", err)
	}
	return runtimes, nil
}

func (s *Store) pgRuntimeReferencedTx(ctx context.Context, tx *sql.Tx, runtimeID string) (bool, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return false, nil
	}

	var referenced bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_apps
	WHERE (
		COALESCE(spec_json->>'runtime_id', '') = $1
		OR COALESCE(spec_json#>>'{failover,target_runtime_id}', '') = $1
		OR COALESCE(spec_json#>>'{postgres,runtime_id}', '') = $1
		OR COALESCE(spec_json#>>'{postgres,failover_target_runtime_id}', '') = $1
		OR COALESCE(status_json->>'current_runtime_id', '') = $1
	)
	AND lower(COALESCE(status_json->>'phase', '')) <> 'deleted'
) OR EXISTS (
	SELECT 1
	FROM fugue_backing_services
	WHERE (
		COALESCE(spec_json#>>'{postgres,runtime_id}', '') = $1
		OR COALESCE(spec_json#>>'{postgres,failover_target_runtime_id}', '') = $1
	)
	AND lower(COALESCE(status, '')) <> 'deleted'
) OR EXISTS (
	SELECT 1
	FROM fugue_operations
	WHERE source_runtime_id = $1
	   OR target_runtime_id = $1
	   OR assigned_runtime_id = $1
	   OR COALESCE(desired_spec_json->>'runtime_id', '') = $1
	   OR COALESCE(desired_spec_json#>>'{failover,target_runtime_id}', '') = $1
	   OR COALESCE(desired_spec_json#>>'{postgres,runtime_id}', '') = $1
	   OR COALESCE(desired_spec_json#>>'{postgres,failover_target_runtime_id}', '') = $1
)
`, runtimeID).Scan(&referenced); err != nil {
		return false, fmt.Errorf("check runtime %s references: %w", runtimeID, err)
	}
	return referenced, nil
}

func (s *Store) pgDeleteRuntimeTx(ctx context.Context, tx *sql.Tx, runtimeID string) error {
	if strings.TrimSpace(runtimeID) == "" {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM fugue_runtimes
WHERE id = $1
`, runtimeID); err != nil {
		return fmt.Errorf("delete runtime %s: %w", runtimeID, err)
	}
	return nil
}

func stringMapEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}
