package localpvinventory

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

var ErrMissingIdentity = errors.New("localpv inventory is missing its node identity")

// Repository owns the LocalPV snapshot and its row lock. A caller never
// receives a DB handle or a partially updated snapshot.
type Repository struct {
	db *sql.DB
}

func NewPostgres(db *sql.DB) Repository { return Repository{db: db} }

func localPVInventoryColumns() string {
	return `id, node_id, cluster_node_name, runtime_id, node_roles_json, vg_name, image_path, image_size_bytes, loop_device, loop_backing_file, pv_size_bytes, pv_free_bytes, lv_count, lv_names_json, active_lv_count, bound_pv_count, bound_pv_count_known, bound_pvc_refs_json, safe_to_decommission, unsafe_reasons_json, observed_at, reported_by_node_updater_id, created_at, updated_at`
}

func (r Repository) Upsert(in model.LocalPVInventory) (model.LocalPVInventory, error) {
	in = normalizeLocalPVInventory(in)
	if in.NodeID == "" && in.ClusterNodeName == "" {
		return model.LocalPVInventory{}, ErrMissingIdentity
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return model.LocalPVInventory{}, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	existing, err := scanLocalPVInventory(tx.QueryRowContext(ctx, `
SELECT `+localPVInventoryColumns()+`
FROM fugue_localpv_inventories
WHERE ($1 <> '' AND node_id = $1)
   OR ($1 = '' AND cluster_node_name = $2)
ORDER BY updated_at DESC
LIMIT 1
FOR UPDATE
`, in.NodeID, in.ClusterNodeName))
	if err == nil {
		in.ID = existing.ID
		in.CreatedAt = existing.CreatedAt
		in.UpdatedAt = now
	} else if errors.Is(err, sql.ErrNoRows) {
		if in.ID == "" {
			in.ID = model.NewID("localpv")
		}
		in.CreatedAt = now
		in.UpdatedAt = now
	} else {
		return model.LocalPVInventory{}, err
	}
	nodeRolesJSON, err := json.Marshal(in.NodeRoles)
	if err != nil {
		return model.LocalPVInventory{}, err
	}
	lvNamesJSON, err := json.Marshal(in.LVNames)
	if err != nil {
		return model.LocalPVInventory{}, err
	}
	boundPVCJSON, err := json.Marshal(in.BoundPVCRefs)
	if err != nil {
		return model.LocalPVInventory{}, err
	}
	unsafeJSON, err := json.Marshal(in.UnsafeReasons)
	if err != nil {
		return model.LocalPVInventory{}, err
	}
	out, err := scanLocalPVInventory(tx.QueryRowContext(ctx, `
INSERT INTO fugue_localpv_inventories (
	id, node_id, cluster_node_name, runtime_id, node_roles_json, vg_name,
	image_path, image_size_bytes, loop_device, loop_backing_file,
	pv_size_bytes, pv_free_bytes, lv_count, lv_names_json, active_lv_count,
	bound_pv_count, bound_pv_count_known, bound_pvc_refs_json, safe_to_decommission,
	unsafe_reasons_json, observed_at, reported_by_node_updater_id,
	created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9, $10,
	$11, $12, $13, $14, $15,
	$16, $17, $18, $19,
	$20, $21, $22,
	$23, $24
)
ON CONFLICT (id) DO UPDATE SET
	node_id = EXCLUDED.node_id,
	cluster_node_name = EXCLUDED.cluster_node_name,
	runtime_id = EXCLUDED.runtime_id,
	node_roles_json = EXCLUDED.node_roles_json,
	vg_name = EXCLUDED.vg_name,
	image_path = EXCLUDED.image_path,
	image_size_bytes = EXCLUDED.image_size_bytes,
	loop_device = EXCLUDED.loop_device,
	loop_backing_file = EXCLUDED.loop_backing_file,
	pv_size_bytes = EXCLUDED.pv_size_bytes,
	pv_free_bytes = EXCLUDED.pv_free_bytes,
	lv_count = EXCLUDED.lv_count,
	lv_names_json = EXCLUDED.lv_names_json,
	active_lv_count = EXCLUDED.active_lv_count,
	bound_pv_count = EXCLUDED.bound_pv_count,
	bound_pv_count_known = EXCLUDED.bound_pv_count_known,
	bound_pvc_refs_json = EXCLUDED.bound_pvc_refs_json,
	safe_to_decommission = EXCLUDED.safe_to_decommission,
	unsafe_reasons_json = EXCLUDED.unsafe_reasons_json,
	observed_at = EXCLUDED.observed_at,
	reported_by_node_updater_id = EXCLUDED.reported_by_node_updater_id,
	updated_at = EXCLUDED.updated_at
	RETURNING `+localPVInventoryColumns(), in.ID, in.NodeID, in.ClusterNodeName, in.RuntimeID, nodeRolesJSON, in.VGName, in.ImagePath, in.ImageSizeBytes, in.LoopDevice, in.LoopBackingFile, in.PVSizeBytes, in.PVFreeBytes, in.LVCount, lvNamesJSON, in.ActiveLVCount, in.BoundPVCount, in.BoundPVCountKnown, boundPVCJSON, in.SafeToDecommission, unsafeJSON, in.ObservedAt, in.ReportedByNodeUpdaterID, in.CreatedAt, in.UpdatedAt))
	if err != nil {
		return model.LocalPVInventory{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.LocalPVInventory{}, err
	}
	return out, nil
}

func (r Repository) List(filter model.LocalPVInventoryFilter) ([]model.LocalPVInventory, error) {
	filter = normalizeLocalPVInventoryFilter(filter)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clauses := []string{}
	args := []any{}
	if filter.NodeID != "" {
		args = append(args, filter.NodeID)
		clauses = append(clauses, fmt.Sprintf("node_id = $%d", len(args)))
	}
	if filter.ClusterNodeName != "" {
		args = append(args, filter.ClusterNodeName)
		clauses = append(clauses, fmt.Sprintf("cluster_node_name = $%d", len(args)))
	}
	if filter.RuntimeID != "" {
		args = append(args, filter.RuntimeID)
		clauses = append(clauses, fmt.Sprintf("runtime_id = $%d", len(args)))
	}
	if !filter.StaleAfter.IsZero() {
		args = append(args, filter.StaleAfter)
		clauses = append(clauses, fmt.Sprintf("observed_at >= $%d", len(args)))
	}
	query := `SELECT ` + localPVInventoryColumns() + ` FROM fugue_localpv_inventories`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY observed_at DESC"
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.LocalPVInventory
	for rows.Next() {
		inventory, err := scanLocalPVInventory(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, inventory)
	}
	return out, rows.Err()
}

func scanLocalPVInventory(scanner interface{ Scan(...any) error }) (model.LocalPVInventory, error) {
	var out model.LocalPVInventory
	var rolesRaw, lvNamesRaw, pvcRefsRaw, unsafeRaw []byte
	if err := scanner.Scan(
		&out.ID,
		&out.NodeID,
		&out.ClusterNodeName,
		&out.RuntimeID,
		&rolesRaw,
		&out.VGName,
		&out.ImagePath,
		&out.ImageSizeBytes,
		&out.LoopDevice,
		&out.LoopBackingFile,
		&out.PVSizeBytes,
		&out.PVFreeBytes,
		&out.LVCount,
		&lvNamesRaw,
		&out.ActiveLVCount,
		&out.BoundPVCount,
		&out.BoundPVCountKnown,
		&pvcRefsRaw,
		&out.SafeToDecommission,
		&unsafeRaw,
		&out.ObservedAt,
		&out.ReportedByNodeUpdaterID,
		&out.CreatedAt,
		&out.UpdatedAt,
	); err != nil {
		return model.LocalPVInventory{}, err
	}
	var err error
	if out.NodeRoles, err = decodeStringList(rolesRaw); err != nil {
		return model.LocalPVInventory{}, err
	}
	if out.LVNames, err = decodeStringList(lvNamesRaw); err != nil {
		return model.LocalPVInventory{}, err
	}
	if out.BoundPVCRefs, err = decodeStringList(pvcRefsRaw); err != nil {
		return model.LocalPVInventory{}, err
	}
	if out.UnsafeReasons, err = decodeStringList(unsafeRaw); err != nil {
		return model.LocalPVInventory{}, err
	}
	return normalizeLocalPVInventory(out), nil
}

func decodeStringList(raw []byte) ([]string, error) {
	raw = bytes.TrimSpace(raw)
	var values []string
	if len(raw) != 0 {
		if err := json.Unmarshal(raw, &values); err != nil {
			return nil, fmt.Errorf("unmarshal json value: %w", err)
		}
	}
	return values, nil
}

func normalizeLocalPVInventory(in model.LocalPVInventory) model.LocalPVInventory {
	in.ID = strings.TrimSpace(in.ID)
	in.NodeID = strings.TrimSpace(in.NodeID)
	in.ClusterNodeName = strings.TrimSpace(in.ClusterNodeName)
	in.RuntimeID = strings.TrimSpace(in.RuntimeID)
	in.NodeRoles = normalizeStringList(in.NodeRoles)
	in.VGName = strings.TrimSpace(in.VGName)
	in.ImagePath = strings.TrimSpace(in.ImagePath)
	in.LoopDevice = strings.TrimSpace(in.LoopDevice)
	in.LoopBackingFile = strings.TrimSpace(in.LoopBackingFile)
	in.LVNames = normalizeStringList(in.LVNames)
	in.BoundPVCRefs = normalizeStringList(in.BoundPVCRefs)
	in.UnsafeReasons = normalizeStringList(in.UnsafeReasons)
	for _, reason := range in.UnsafeReasons {
		if reason == "kubectl_pv_unavailable" || reason == "bound_pv_count_unknown" {
			in.BoundPVCount = -1
			break
		}
	}
	if in.BoundPVCount < 0 {
		in.BoundPVCountKnown = false
	} else if !in.BoundPVCountKnown {
		// Legacy callers did not have the explicit known bit. A non-negative
		// count from those callers is a real observation; -1 is the unknown
		// sentinel emitted when kubectl was unavailable.
		in.BoundPVCountKnown = true
	}
	in.ReportedByNodeUpdaterID = strings.TrimSpace(in.ReportedByNodeUpdaterID)
	if in.ObservedAt.IsZero() {
		in.ObservedAt = time.Now().UTC()
	}
	return in
}

func normalizeLocalPVInventoryFilter(filter model.LocalPVInventoryFilter) model.LocalPVInventoryFilter {
	filter.NodeID = strings.TrimSpace(filter.NodeID)
	filter.ClusterNodeName = strings.TrimSpace(filter.ClusterNodeName)
	filter.RuntimeID = strings.TrimSpace(filter.RuntimeID)
	return filter
}

func normalizeStringList(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
