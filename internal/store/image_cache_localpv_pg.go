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

func imageCacheNodeColumns() string {
	return `id, node_id, cluster_node_name, runtime_id, cache_endpoint, store_path, filesystem_total_bytes, filesystem_free_bytes, filesystem_used_percent, cache_bytes, manifest_count, blob_count, pin_count, observed_at, reported_by_node_updater_id, status, last_error, created_at, updated_at`
}

func imageCacheManifestColumns() string {
	return `id, node_id, cluster_node_name, runtime_id, image_ref, repo, target, digest, media_type, manifest_size_bytes, total_blob_bytes, referenced_blobs_json, created_at_observed, last_seen_at, pinned_locally, present, created_at, updated_at`
}

func imageCachePrunePlanColumns() string {
	return `id, node_id, cluster_node_name, runtime_id, mode, candidate_manifest_count, protected_manifest_count, planned_delete_bytes, max_delete_bytes, min_manifest_age, protection_summary_json, candidate_summary_json, candidates_json, created_at, executed_at, status, error`
}

func localPVInventoryColumns() string {
	return `id, node_id, cluster_node_name, runtime_id, node_roles_json, vg_name, image_path, image_size_bytes, loop_device, loop_backing_file, pv_size_bytes, pv_free_bytes, lv_count, lv_names_json, active_lv_count, bound_pv_count, bound_pvc_refs_json, safe_to_decommission, unsafe_reasons_json, observed_at, reported_by_node_updater_id, created_at, updated_at`
}

func (s *Store) pgUpsertImageCacheInventory(node model.ImageCacheNodeInventory, manifests []model.ImageCacheManifest) (model.ImageCacheNodeInventory, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ImageCacheNodeInventory{}, mapDBErr(err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	existing, err := scanImageCacheNodeInventory(tx.QueryRowContext(ctx, `
SELECT `+imageCacheNodeColumns()+`
FROM fugue_image_cache_nodes
WHERE ($1 <> '' AND node_id = $1)
   OR ($1 = '' AND cluster_node_name = $2)
ORDER BY updated_at DESC
LIMIT 1
FOR UPDATE
`, node.NodeID, node.ClusterNodeName))
	if err == nil {
		node.ID = existing.ID
		node.CreatedAt = existing.CreatedAt
		node.UpdatedAt = now
		updated, updateErr := scanImageCacheNodeInventory(tx.QueryRowContext(ctx, `
UPDATE fugue_image_cache_nodes
SET node_id = $2, cluster_node_name = $3, runtime_id = $4, cache_endpoint = $5,
	store_path = $6, filesystem_total_bytes = $7, filesystem_free_bytes = $8,
	filesystem_used_percent = $9, cache_bytes = $10, manifest_count = $11,
	blob_count = $12, pin_count = $13, observed_at = $14,
	reported_by_node_updater_id = $15, status = $16, last_error = $17,
	updated_at = $18
WHERE id = $1
RETURNING `+imageCacheNodeColumns(), node.ID, node.NodeID, node.ClusterNodeName, node.RuntimeID, node.CacheEndpoint, node.StorePath, node.FilesystemTotalBytes, node.FilesystemFreeBytes, node.FilesystemUsedPercent, node.CacheBytes, node.ManifestCount, node.BlobCount, node.PinCount, node.ObservedAt, node.ReportedByNodeUpdaterID, node.Status, node.LastError, node.UpdatedAt))
		if updateErr != nil {
			return model.ImageCacheNodeInventory{}, mapDBErr(updateErr)
		}
		node = updated
	} else if errors.Is(err, sql.ErrNoRows) {
		node.ID = firstNonEmptyImageCacheString(node.ID, model.NewID("imgcache"))
		node.CreatedAt = now
		node.UpdatedAt = now
		inserted, insertErr := scanImageCacheNodeInventory(tx.QueryRowContext(ctx, `
INSERT INTO fugue_image_cache_nodes (
	id, node_id, cluster_node_name, runtime_id, cache_endpoint, store_path,
	filesystem_total_bytes, filesystem_free_bytes, filesystem_used_percent,
	cache_bytes, manifest_count, blob_count, pin_count, observed_at,
	reported_by_node_updater_id, status, last_error, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9,
	$10, $11, $12, $13, $14,
	$15, $16, $17, $18, $19
)
RETURNING `+imageCacheNodeColumns(), node.ID, node.NodeID, node.ClusterNodeName, node.RuntimeID, node.CacheEndpoint, node.StorePath, node.FilesystemTotalBytes, node.FilesystemFreeBytes, node.FilesystemUsedPercent, node.CacheBytes, node.ManifestCount, node.BlobCount, node.PinCount, node.ObservedAt, node.ReportedByNodeUpdaterID, node.Status, node.LastError, node.CreatedAt, node.UpdatedAt))
		if insertErr != nil {
			return model.ImageCacheNodeInventory{}, mapDBErr(insertErr)
		}
		node = inserted
	} else {
		return model.ImageCacheNodeInventory{}, mapDBErr(err)
	}

	for _, manifest := range manifests {
		manifest = normalizeImageCacheManifest(manifest)
		manifest.NodeID = firstNonEmptyImageCacheString(manifest.NodeID, node.NodeID)
		manifest.ClusterNodeName = firstNonEmptyImageCacheString(manifest.ClusterNodeName, node.ClusterNodeName)
		manifest.RuntimeID = firstNonEmptyImageCacheString(manifest.RuntimeID, node.RuntimeID)
		if manifest.Repo == "" || manifest.Target == "" {
			continue
		}
		if manifest.LastSeenAt.IsZero() {
			manifest.LastSeenAt = node.ObservedAt
		}
		if manifest.LastSeenAt.IsZero() {
			manifest.LastSeenAt = now
		}
		manifest.ID = firstNonEmptyImageCacheString(manifest.ID, model.NewID("imgcacheman"))
		manifest.CreatedAt = now
		manifest.UpdatedAt = now
		refsJSON, jsonErr := marshalNullableJSON(manifest.ReferencedBlobs)
		if jsonErr != nil {
			return model.ImageCacheNodeInventory{}, jsonErr
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_image_cache_manifests (
	id, node_id, cluster_node_name, runtime_id, image_ref, repo, target, digest,
	media_type, manifest_size_bytes, total_blob_bytes, referenced_blobs_json,
	created_at_observed, last_seen_at, pinned_locally, present, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8,
	$9, $10, $11, $12,
	$13, $14, $15, $16, $17, $18
)
ON CONFLICT (node_id, cluster_node_name, repo, target, digest) DO UPDATE SET
	runtime_id = EXCLUDED.runtime_id,
	image_ref = EXCLUDED.image_ref,
	media_type = EXCLUDED.media_type,
	manifest_size_bytes = EXCLUDED.manifest_size_bytes,
	total_blob_bytes = EXCLUDED.total_blob_bytes,
	referenced_blobs_json = EXCLUDED.referenced_blobs_json,
	created_at_observed = EXCLUDED.created_at_observed,
	last_seen_at = EXCLUDED.last_seen_at,
	pinned_locally = EXCLUDED.pinned_locally,
	present = EXCLUDED.present,
	updated_at = EXCLUDED.updated_at
`, manifest.ID, manifest.NodeID, manifest.ClusterNodeName, manifest.RuntimeID, manifest.ImageRef, manifest.Repo, manifest.Target, manifest.Digest, manifest.MediaType, manifest.ManifestSizeBytes, manifest.TotalBlobBytes, refsJSON, manifest.CreatedAtObserved, manifest.LastSeenAt, manifest.PinnedLocally, manifest.Present, manifest.CreatedAt, manifest.UpdatedAt); err != nil {
			return model.ImageCacheNodeInventory{}, mapDBErr(err)
		}
	}
	if node.SnapshotComplete {
		if err := pgMarkMissingImageCacheManifestsAbsent(ctx, tx, node, now); err != nil {
			return model.ImageCacheNodeInventory{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return model.ImageCacheNodeInventory{}, mapDBErr(err)
	}
	return node, nil
}

func pgMarkMissingImageCacheManifestsAbsent(ctx context.Context, tx *sql.Tx, node model.ImageCacheNodeInventory, now time.Time) error {
	clauses := []string{"present = TRUE", "last_seen_at < $1"}
	args := []any{node.ObservedAt}
	if node.NodeID != "" {
		args = append(args, node.NodeID)
		clauses = append(clauses, fmt.Sprintf("node_id = $%d", len(args)))
	}
	if node.ClusterNodeName != "" {
		args = append(args, node.ClusterNodeName)
		clauses = append(clauses, fmt.Sprintf("cluster_node_name = $%d", len(args)))
	}
	if len(args) == 1 {
		return nil
	}
	args = append(args, now)
	query := `UPDATE fugue_image_cache_manifests SET present = FALSE, updated_at = $` + fmt.Sprint(len(args)) + ` WHERE ` + strings.Join(clauses, " AND ")
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return mapDBErr(err)
	}
	return nil
}

func (s *Store) pgListImageCacheNodeInventories(filter model.ImageCacheNodeInventoryFilter) ([]model.ImageCacheNodeInventory, error) {
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
	query := `SELECT ` + imageCacheNodeColumns() + ` FROM fugue_image_cache_nodes`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY observed_at DESC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	var out []model.ImageCacheNodeInventory
	for rows.Next() {
		node, err := scanImageCacheNodeInventory(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, node)
	}
	return out, mapDBErr(rows.Err())
}

func (s *Store) pgListImageCacheManifests(filter model.ImageCacheManifestFilter) ([]model.ImageCacheManifest, error) {
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
	if filter.Repo != "" {
		args = append(args, filter.Repo)
		clauses = append(clauses, fmt.Sprintf("repo = $%d", len(args)))
	}
	if filter.Target != "" {
		args = append(args, filter.Target)
		clauses = append(clauses, fmt.Sprintf("target = $%d", len(args)))
	}
	if filter.Digest != "" {
		args = append(args, filter.Digest)
		clauses = append(clauses, fmt.Sprintf("digest = $%d", len(args)))
	}
	if filter.PresentOnly {
		clauses = append(clauses, "present = TRUE")
	}
	if !filter.SeenAfter.IsZero() {
		args = append(args, filter.SeenAfter)
		clauses = append(clauses, fmt.Sprintf("last_seen_at >= $%d", len(args)))
	}
	query := `SELECT ` + imageCacheManifestColumns() + ` FROM fugue_image_cache_manifests`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY last_seen_at DESC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	var out []model.ImageCacheManifest
	for rows.Next() {
		manifest, err := scanImageCacheManifest(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, manifest)
	}
	return out, mapDBErr(rows.Err())
}

func (s *Store) pgUpsertImageCachePrunePlan(plan model.ImageCachePrunePlan) (model.ImageCachePrunePlan, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := time.Now().UTC()
	if plan.ID == "" {
		plan.ID = model.NewID("imgcacheprune")
	}
	if plan.CreatedAt.IsZero() {
		plan.CreatedAt = now
	}
	protectionJSON, err := marshalNullableJSON(plan.ProtectionSummary)
	if err != nil {
		return model.ImageCachePrunePlan{}, err
	}
	candidateSummaryJSON, err := marshalNullableJSON(plan.CandidateSummary)
	if err != nil {
		return model.ImageCachePrunePlan{}, err
	}
	candidatesJSON, err := marshalNullableJSON(plan.Candidates)
	if err != nil {
		return model.ImageCachePrunePlan{}, err
	}
	out, err := scanImageCachePrunePlan(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_image_cache_prune_plans (
	id, node_id, cluster_node_name, runtime_id, mode, candidate_manifest_count,
	protected_manifest_count, planned_delete_bytes, max_delete_bytes,
	min_manifest_age, protection_summary_json, candidate_summary_json,
	candidates_json, created_at, executed_at, status, error
) VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9,
	$10, $11, $12,
	$13, $14, $15, $16, $17
)
ON CONFLICT (id) DO UPDATE SET
	node_id = EXCLUDED.node_id,
	cluster_node_name = EXCLUDED.cluster_node_name,
	runtime_id = EXCLUDED.runtime_id,
	mode = EXCLUDED.mode,
	candidate_manifest_count = EXCLUDED.candidate_manifest_count,
	protected_manifest_count = EXCLUDED.protected_manifest_count,
	planned_delete_bytes = EXCLUDED.planned_delete_bytes,
	max_delete_bytes = EXCLUDED.max_delete_bytes,
	min_manifest_age = EXCLUDED.min_manifest_age,
	protection_summary_json = EXCLUDED.protection_summary_json,
	candidate_summary_json = EXCLUDED.candidate_summary_json,
	candidates_json = EXCLUDED.candidates_json,
	executed_at = EXCLUDED.executed_at,
	status = EXCLUDED.status,
	error = EXCLUDED.error
RETURNING `+imageCachePrunePlanColumns(), plan.ID, plan.NodeID, plan.ClusterNodeName, plan.RuntimeID, plan.Mode, plan.CandidateManifestCount, plan.ProtectedManifestCount, plan.PlannedDeleteBytes, plan.MaxDeleteBytes, plan.MinManifestAge, protectionJSON, candidateSummaryJSON, candidatesJSON, plan.CreatedAt, plan.ExecutedAt, plan.Status, plan.Error))
	if err != nil {
		return model.ImageCachePrunePlan{}, mapDBErr(err)
	}
	return out, nil
}

func (s *Store) pgListImageCachePrunePlans(filter model.ImageCachePrunePlanFilter) ([]model.ImageCachePrunePlan, error) {
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
	if filter.Mode != "" {
		args = append(args, filter.Mode)
		clauses = append(clauses, fmt.Sprintf("mode = $%d", len(args)))
	}
	if filter.Status != "" {
		args = append(args, filter.Status)
		clauses = append(clauses, fmt.Sprintf("status = $%d", len(args)))
	}
	query := `SELECT ` + imageCachePrunePlanColumns() + ` FROM fugue_image_cache_prune_plans`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at DESC"
	if filter.Limit > 0 {
		args = append(args, filter.Limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	var out []model.ImageCachePrunePlan
	for rows.Next() {
		plan, err := scanImageCachePrunePlan(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, plan)
	}
	return out, mapDBErr(rows.Err())
}

func (s *Store) pgUpsertLocalPVInventory(in model.LocalPVInventory) (model.LocalPVInventory, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.LocalPVInventory{}, mapDBErr(err)
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
		in.ID = firstNonEmptyImageCacheString(in.ID, model.NewID("localpv"))
		in.CreatedAt = now
		in.UpdatedAt = now
	} else {
		return model.LocalPVInventory{}, mapDBErr(err)
	}
	nodeRolesJSON, err := marshalNullableJSON(in.NodeRoles)
	if err != nil {
		return model.LocalPVInventory{}, err
	}
	lvNamesJSON, err := marshalNullableJSON(in.LVNames)
	if err != nil {
		return model.LocalPVInventory{}, err
	}
	boundPVCJSON, err := marshalNullableJSON(in.BoundPVCRefs)
	if err != nil {
		return model.LocalPVInventory{}, err
	}
	unsafeJSON, err := marshalNullableJSON(in.UnsafeReasons)
	if err != nil {
		return model.LocalPVInventory{}, err
	}
	out, err := scanLocalPVInventory(tx.QueryRowContext(ctx, `
INSERT INTO fugue_localpv_inventories (
	id, node_id, cluster_node_name, runtime_id, node_roles_json, vg_name,
	image_path, image_size_bytes, loop_device, loop_backing_file,
	pv_size_bytes, pv_free_bytes, lv_count, lv_names_json, active_lv_count,
	bound_pv_count, bound_pvc_refs_json, safe_to_decommission,
	unsafe_reasons_json, observed_at, reported_by_node_updater_id,
	created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9, $10,
	$11, $12, $13, $14, $15,
	$16, $17, $18,
	$19, $20, $21,
	$22, $23
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
	bound_pvc_refs_json = EXCLUDED.bound_pvc_refs_json,
	safe_to_decommission = EXCLUDED.safe_to_decommission,
	unsafe_reasons_json = EXCLUDED.unsafe_reasons_json,
	observed_at = EXCLUDED.observed_at,
	reported_by_node_updater_id = EXCLUDED.reported_by_node_updater_id,
	updated_at = EXCLUDED.updated_at
RETURNING `+localPVInventoryColumns(), in.ID, in.NodeID, in.ClusterNodeName, in.RuntimeID, nodeRolesJSON, in.VGName, in.ImagePath, in.ImageSizeBytes, in.LoopDevice, in.LoopBackingFile, in.PVSizeBytes, in.PVFreeBytes, in.LVCount, lvNamesJSON, in.ActiveLVCount, in.BoundPVCount, boundPVCJSON, in.SafeToDecommission, unsafeJSON, in.ObservedAt, in.ReportedByNodeUpdaterID, in.CreatedAt, in.UpdatedAt))
	if err != nil {
		return model.LocalPVInventory{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.LocalPVInventory{}, mapDBErr(err)
	}
	return out, nil
}

func (s *Store) pgListLocalPVInventories(filter model.LocalPVInventoryFilter) ([]model.LocalPVInventory, error) {
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
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	var out []model.LocalPVInventory
	for rows.Next() {
		inventory, err := scanLocalPVInventory(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, inventory)
	}
	return out, mapDBErr(rows.Err())
}

func scanImageCacheNodeInventory(scanner sqlScanner) (model.ImageCacheNodeInventory, error) {
	var out model.ImageCacheNodeInventory
	if err := scanner.Scan(
		&out.ID,
		&out.NodeID,
		&out.ClusterNodeName,
		&out.RuntimeID,
		&out.CacheEndpoint,
		&out.StorePath,
		&out.FilesystemTotalBytes,
		&out.FilesystemFreeBytes,
		&out.FilesystemUsedPercent,
		&out.CacheBytes,
		&out.ManifestCount,
		&out.BlobCount,
		&out.PinCount,
		&out.ObservedAt,
		&out.ReportedByNodeUpdaterID,
		&out.Status,
		&out.LastError,
		&out.CreatedAt,
		&out.UpdatedAt,
	); err != nil {
		return model.ImageCacheNodeInventory{}, err
	}
	return out, nil
}

func scanImageCacheManifest(scanner sqlScanner) (model.ImageCacheManifest, error) {
	var out model.ImageCacheManifest
	var refsRaw []byte
	var createdAtObserved sql.NullTime
	if err := scanner.Scan(
		&out.ID,
		&out.NodeID,
		&out.ClusterNodeName,
		&out.RuntimeID,
		&out.ImageRef,
		&out.Repo,
		&out.Target,
		&out.Digest,
		&out.MediaType,
		&out.ManifestSizeBytes,
		&out.TotalBlobBytes,
		&refsRaw,
		&createdAtObserved,
		&out.LastSeenAt,
		&out.PinnedLocally,
		&out.Present,
		&out.CreatedAt,
		&out.UpdatedAt,
	); err != nil {
		return model.ImageCacheManifest{}, err
	}
	refs, err := decodeJSONValue[[]string](refsRaw)
	if err != nil {
		return model.ImageCacheManifest{}, err
	}
	out.ReferencedBlobs = normalizeStringList(refs)
	if createdAtObserved.Valid {
		out.CreatedAtObserved = &createdAtObserved.Time
	}
	return out, nil
}

func scanImageCachePrunePlan(scanner sqlScanner) (model.ImageCachePrunePlan, error) {
	var out model.ImageCachePrunePlan
	var protectionRaw, candidateSummaryRaw, candidatesRaw []byte
	var executedAt sql.NullTime
	if err := scanner.Scan(
		&out.ID,
		&out.NodeID,
		&out.ClusterNodeName,
		&out.RuntimeID,
		&out.Mode,
		&out.CandidateManifestCount,
		&out.ProtectedManifestCount,
		&out.PlannedDeleteBytes,
		&out.MaxDeleteBytes,
		&out.MinManifestAge,
		&protectionRaw,
		&candidateSummaryRaw,
		&candidatesRaw,
		&out.CreatedAt,
		&executedAt,
		&out.Status,
		&out.Error,
	); err != nil {
		return model.ImageCachePrunePlan{}, err
	}
	protection, err := decodeJSONValue[map[string]int](protectionRaw)
	if err != nil {
		return model.ImageCachePrunePlan{}, err
	}
	candidateSummary, err := decodeJSONValue[map[string]int](candidateSummaryRaw)
	if err != nil {
		return model.ImageCachePrunePlan{}, err
	}
	candidates, err := decodeJSONValue[[]model.ImageCachePruneCandidate](candidatesRaw)
	if err != nil {
		return model.ImageCachePrunePlan{}, err
	}
	out.ProtectionSummary = protection
	out.CandidateSummary = candidateSummary
	out.Candidates = candidates
	if executedAt.Valid {
		out.ExecutedAt = &executedAt.Time
	}
	return out, nil
}

func scanLocalPVInventory(scanner sqlScanner) (model.LocalPVInventory, error) {
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
	if out.NodeRoles, err = decodeJSONValue[[]string](rolesRaw); err != nil {
		return model.LocalPVInventory{}, err
	}
	if out.LVNames, err = decodeJSONValue[[]string](lvNamesRaw); err != nil {
		return model.LocalPVInventory{}, err
	}
	if out.BoundPVCRefs, err = decodeJSONValue[[]string](pvcRefsRaw); err != nil {
		return model.LocalPVInventory{}, err
	}
	if out.UnsafeReasons, err = decodeJSONValue[[]string](unsafeRaw); err != nil {
		return model.LocalPVInventory{}, err
	}
	out.NodeRoles = normalizeStringList(out.NodeRoles)
	out.LVNames = normalizeStringList(out.LVNames)
	out.BoundPVCRefs = normalizeStringList(out.BoundPVCRefs)
	out.UnsafeReasons = normalizeStringList(out.UnsafeReasons)
	return out, nil
}
