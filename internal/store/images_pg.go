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

func imageColumns() string {
	return `id, tenant_id, app_id, image_ref, canonical_digest, media_type, manifest_json, manifest_size_bytes, blob_bytes, source_operation_id, lifecycle_state, required_replica_count, min_available_replica_count, created_at, updated_at`
}

func imageAliasColumns() string {
	return `id, image_id, tenant_id, alias_ref, digest, created_at, updated_at`
}

func imageReplicaColumns() string {
	return `id, image_id, tenant_id, app_id, digest, node_id, runtime_id, cluster_node_name, cache_endpoint, failure_domain, status, source_replica_id, last_verified_at, lease_expires_at, size_bytes, last_error, created_at, updated_at`
}

func imagePinColumns() string {
	return `id, image_id, tenant_id, app_id, operation_id, reason, min_replicas, expires_at, created_at, updated_at`
}

func imageReplicationTaskColumns() string {
	return `id, image_id, tenant_id, app_id, source_replica_id, source_cache_endpoint, target_node_id, target_runtime_id, target_cluster_node_name, priority, status, attempts, last_error, created_at, updated_at, started_at, completed_at`
}

func (s *Store) pgUpsertImage(image model.Image) (model.Image, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Image{}, mapDBErr(err)
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	existing, err := scanImage(tx.QueryRowContext(ctx, `
SELECT `+imageColumns()+`
FROM fugue_images
WHERE ($1 <> '' AND id = $1)
   OR ($1 = '' AND COALESCE(tenant_id, '') = COALESCE($2::text, '') AND image_ref = $3 AND canonical_digest = $4)
FOR UPDATE
`, image.ID, nullIfEmpty(image.TenantID), image.ImageRef, image.CanonicalDigest))
	if err == nil {
		if image.TenantID == "" {
			image.TenantID = existing.TenantID
		}
		if image.AppID == "" {
			image.AppID = existing.AppID
		}
		if image.SourceOperationID == "" {
			image.SourceOperationID = existing.SourceOperationID
		}
		image.ID = existing.ID
		image.CreatedAt = existing.CreatedAt
		image.UpdatedAt = now
		updated, updateErr := scanImage(tx.QueryRowContext(ctx, `
UPDATE fugue_images
SET tenant_id = $2,
	app_id = $3,
	image_ref = $4,
	canonical_digest = $5,
	media_type = $6,
	manifest_json = $7,
	manifest_size_bytes = $8,
	blob_bytes = $9,
	source_operation_id = $10,
	lifecycle_state = $11,
	required_replica_count = $12,
	min_available_replica_count = $13,
	updated_at = $14
WHERE id = $1
RETURNING `+imageColumns(), image.ID, nullIfEmpty(image.TenantID), image.AppID, image.ImageRef, image.CanonicalDigest, image.MediaType, image.ManifestJSON, image.ManifestSizeBytes, image.BlobBytes, image.SourceOperationID, image.LifecycleState, image.RequiredReplicaCount, image.MinAvailableReplicaCount, image.UpdatedAt))
		if updateErr != nil {
			return model.Image{}, mapDBErr(updateErr)
		}
		if err := tx.Commit(); err != nil {
			return model.Image{}, mapDBErr(err)
		}
		return updated, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return model.Image{}, mapDBErr(err)
	}
	image.ID = firstNonEmptyImageString(image.ID, model.NewID("img"))
	image.CreatedAt = now
	image.UpdatedAt = now
	inserted, err := scanImage(tx.QueryRowContext(ctx, `
INSERT INTO fugue_images (
	id, tenant_id, app_id, image_ref, canonical_digest, media_type, manifest_json,
	manifest_size_bytes, blob_bytes, source_operation_id, lifecycle_state,
	required_replica_count, min_available_replica_count, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10, $11,
	$12, $13, $14, $15
)
RETURNING `+imageColumns(), image.ID, nullIfEmpty(image.TenantID), image.AppID, image.ImageRef, image.CanonicalDigest, image.MediaType, image.ManifestJSON, image.ManifestSizeBytes, image.BlobBytes, image.SourceOperationID, image.LifecycleState, image.RequiredReplicaCount, image.MinAvailableReplicaCount, image.CreatedAt, image.UpdatedAt))
	if err != nil {
		return model.Image{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.Image{}, mapDBErr(err)
	}
	return inserted, nil
}

func (s *Store) pgGetImage(id, tenantID string, platformAdmin bool) (model.Image, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	args := []any{strings.TrimSpace(id)}
	query := `SELECT ` + imageColumns() + ` FROM fugue_images WHERE id = $1`
	if !platformAdmin {
		args = append(args, strings.TrimSpace(tenantID))
		query += fmt.Sprintf(" AND tenant_id = $%d", len(args))
	}
	image, err := scanImage(s.db.QueryRowContext(ctx, query, args...))
	if err != nil {
		return model.Image{}, mapDBErr(err)
	}
	return image, nil
}

func (s *Store) pgListImages(filter model.ImageFilter) ([]model.Image, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clauses, args := imageFilterClauses(filter)
	query := `SELECT ` + imageColumns() + ` FROM fugue_images`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY updated_at DESC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	out := []model.Image{}
	for rows.Next() {
		image, err := scanImage(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, image)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func (s *Store) pgUpsertImageAlias(alias model.ImageAlias) (model.ImageAlias, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ImageAlias{}, mapDBErr(err)
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	existing, err := scanImageAlias(tx.QueryRowContext(ctx, `
SELECT `+imageAliasColumns()+`
FROM fugue_image_aliases
WHERE ($1 <> '' AND id = $1) OR ($1 = '' AND image_id = $2 AND alias_ref = $3)
FOR UPDATE
`, alias.ID, alias.ImageID, alias.AliasRef))
	if err == nil {
		alias.ID = existing.ID
		alias.CreatedAt = existing.CreatedAt
		if alias.TenantID == "" {
			alias.TenantID = existing.TenantID
		}
		alias.UpdatedAt = now
		updated, updateErr := scanImageAlias(tx.QueryRowContext(ctx, `
UPDATE fugue_image_aliases
SET image_id = $2, tenant_id = $3, alias_ref = $4, digest = $5, updated_at = $6
WHERE id = $1
RETURNING `+imageAliasColumns(), alias.ID, alias.ImageID, nullIfEmpty(alias.TenantID), alias.AliasRef, alias.Digest, alias.UpdatedAt))
		if updateErr != nil {
			return model.ImageAlias{}, mapDBErr(updateErr)
		}
		if err := tx.Commit(); err != nil {
			return model.ImageAlias{}, mapDBErr(err)
		}
		return updated, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return model.ImageAlias{}, mapDBErr(err)
	}
	alias.ID = firstNonEmptyImageString(alias.ID, model.NewID("imgalias"))
	alias.CreatedAt = now
	alias.UpdatedAt = now
	inserted, err := scanImageAlias(tx.QueryRowContext(ctx, `
INSERT INTO fugue_image_aliases (id, image_id, tenant_id, alias_ref, digest, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING `+imageAliasColumns(), alias.ID, alias.ImageID, nullIfEmpty(alias.TenantID), alias.AliasRef, alias.Digest, alias.CreatedAt, alias.UpdatedAt))
	if err != nil {
		return model.ImageAlias{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.ImageAlias{}, mapDBErr(err)
	}
	return inserted, nil
}

func (s *Store) pgListImageAliases(filter model.ImageAliasFilter) ([]model.ImageAlias, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clauses := []string{}
	args := []any{}
	if !filter.PlatformAdmin {
		args = append(args, strings.TrimSpace(filter.TenantID))
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if filter.ImageID != "" {
		args = append(args, filter.ImageID)
		clauses = append(clauses, fmt.Sprintf("image_id = $%d", len(args)))
	}
	if filter.AliasRef != "" {
		args = append(args, filter.AliasRef)
		clauses = append(clauses, fmt.Sprintf("alias_ref = $%d", len(args)))
	}
	if filter.Digest != "" {
		args = append(args, filter.Digest)
		clauses = append(clauses, fmt.Sprintf("digest = $%d", len(args)))
	}
	query := `SELECT ` + imageAliasColumns() + ` FROM fugue_image_aliases`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY updated_at DESC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	out := []model.ImageAlias{}
	for rows.Next() {
		alias, err := scanImageAlias(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, alias)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func (s *Store) pgUpsertImageReplica(replica model.ImageReplica) (model.ImageReplica, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ImageReplica{}, mapDBErr(err)
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	existing, err := scanImageReplica(tx.QueryRowContext(ctx, `
SELECT `+imageReplicaColumns()+`
FROM fugue_image_replicas
WHERE ($1 <> '' AND id = $1)
   OR ($1 = '' AND image_id = $2 AND node_id = $3 AND runtime_id = $4 AND cluster_node_name = $5)
FOR UPDATE
`, replica.ID, replica.ImageID, replica.NodeID, replica.RuntimeID, replica.ClusterNodeName))
	if err == nil {
		replica.ID = existing.ID
		replica.CreatedAt = existing.CreatedAt
		if replica.TenantID == "" {
			replica.TenantID = existing.TenantID
		}
		if replica.AppID == "" {
			replica.AppID = existing.AppID
		}
		if replica.Digest == "" {
			replica.Digest = existing.Digest
		}
		if replica.CacheEndpoint == "" {
			replica.CacheEndpoint = existing.CacheEndpoint
		}
		replica.UpdatedAt = now
		updated, updateErr := scanImageReplica(tx.QueryRowContext(ctx, `
UPDATE fugue_image_replicas
SET image_id = $2, tenant_id = $3, app_id = $4, digest = $5, node_id = $6,
	runtime_id = $7, cluster_node_name = $8, cache_endpoint = $9,
	failure_domain = $10, status = $11, source_replica_id = $12,
	last_verified_at = $13, lease_expires_at = $14, size_bytes = $15,
	last_error = $16, updated_at = $17
WHERE id = $1
RETURNING `+imageReplicaColumns(), replica.ID, replica.ImageID, nullIfEmpty(replica.TenantID), replica.AppID, replica.Digest, replica.NodeID, replica.RuntimeID, replica.ClusterNodeName, replica.CacheEndpoint, replica.FailureDomain, replica.Status, replica.SourceReplicaID, replica.LastVerifiedAt, replica.LeaseExpiresAt, replica.SizeBytes, replica.LastError, replica.UpdatedAt))
		if updateErr != nil {
			return model.ImageReplica{}, mapDBErr(updateErr)
		}
		if err := tx.Commit(); err != nil {
			return model.ImageReplica{}, mapDBErr(err)
		}
		return updated, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return model.ImageReplica{}, mapDBErr(err)
	}
	replica.ID = firstNonEmptyImageString(replica.ID, model.NewID("imgrep"))
	replica.CreatedAt = now
	replica.UpdatedAt = now
	inserted, err := scanImageReplica(tx.QueryRowContext(ctx, `
INSERT INTO fugue_image_replicas (
	id, image_id, tenant_id, app_id, digest, node_id, runtime_id,
	cluster_node_name, cache_endpoint, failure_domain, status,
	source_replica_id, last_verified_at, lease_expires_at, size_bytes,
	last_error, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10, $11,
	$12, $13, $14, $15,
	$16, $17, $18
)
RETURNING `+imageReplicaColumns(), replica.ID, replica.ImageID, nullIfEmpty(replica.TenantID), replica.AppID, replica.Digest, replica.NodeID, replica.RuntimeID, replica.ClusterNodeName, replica.CacheEndpoint, replica.FailureDomain, replica.Status, replica.SourceReplicaID, replica.LastVerifiedAt, replica.LeaseExpiresAt, replica.SizeBytes, replica.LastError, replica.CreatedAt, replica.UpdatedAt))
	if err != nil {
		return model.ImageReplica{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.ImageReplica{}, mapDBErr(err)
	}
	return inserted, nil
}

func (s *Store) pgListImageReplicas(filter model.ImageReplicaFilter) ([]model.ImageReplica, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clauses, args := imageReplicaFilterClauses(filter)
	query := `SELECT ` + imageReplicaColumns() + ` FROM fugue_image_replicas`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY COALESCE(last_verified_at, updated_at) DESC, updated_at DESC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	out := []model.ImageReplica{}
	for rows.Next() {
		replica, err := scanImageReplica(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, replica)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func (s *Store) pgMarkStaleImageReplicas(cutoff time.Time) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := s.db.ExecContext(ctx, `
UPDATE fugue_image_replicas
SET status = $1, updated_at = $2
WHERE status = $3
  AND COALESCE(last_verified_at, updated_at) < $4
`, model.ImageReplicaStatusStale, time.Now().UTC(), model.ImageReplicaStatusPresent, cutoff)
	if err != nil {
		return 0, mapDBErr(err)
	}
	count, _ := res.RowsAffected()
	return int(count), nil
}

func (s *Store) pgUpsertImagePin(pin model.ImagePin) (model.ImagePin, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ImagePin{}, mapDBErr(err)
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	existing, err := scanImagePin(tx.QueryRowContext(ctx, `
SELECT `+imagePinColumns()+`
FROM fugue_image_pins
WHERE ($1 <> '' AND id = $1)
   OR ($1 = '' AND image_id = $2 AND app_id = $3 AND operation_id = $4 AND reason = $5)
FOR UPDATE
`, pin.ID, pin.ImageID, pin.AppID, pin.OperationID, pin.Reason))
	if err == nil {
		pin.ID = existing.ID
		pin.CreatedAt = existing.CreatedAt
		if pin.TenantID == "" {
			pin.TenantID = existing.TenantID
		}
		if pin.AppID == "" {
			pin.AppID = existing.AppID
		}
		pin.UpdatedAt = now
		updated, updateErr := scanImagePin(tx.QueryRowContext(ctx, `
UPDATE fugue_image_pins
SET image_id = $2, tenant_id = $3, app_id = $4, operation_id = $5,
	reason = $6, min_replicas = $7, expires_at = $8, updated_at = $9
WHERE id = $1
RETURNING `+imagePinColumns(), pin.ID, pin.ImageID, nullIfEmpty(pin.TenantID), pin.AppID, pin.OperationID, pin.Reason, pin.MinReplicas, pin.ExpiresAt, pin.UpdatedAt))
		if updateErr != nil {
			return model.ImagePin{}, mapDBErr(updateErr)
		}
		if err := tx.Commit(); err != nil {
			return model.ImagePin{}, mapDBErr(err)
		}
		return updated, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return model.ImagePin{}, mapDBErr(err)
	}
	pin.ID = firstNonEmptyImageString(pin.ID, model.NewID("imgpin"))
	pin.CreatedAt = now
	pin.UpdatedAt = now
	inserted, err := scanImagePin(tx.QueryRowContext(ctx, `
INSERT INTO fugue_image_pins (
	id, image_id, tenant_id, app_id, operation_id, reason, min_replicas,
	expires_at, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10
)
RETURNING `+imagePinColumns(), pin.ID, pin.ImageID, nullIfEmpty(pin.TenantID), pin.AppID, pin.OperationID, pin.Reason, pin.MinReplicas, pin.ExpiresAt, pin.CreatedAt, pin.UpdatedAt))
	if err != nil {
		return model.ImagePin{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.ImagePin{}, mapDBErr(err)
	}
	return inserted, nil
}

func (s *Store) pgDeleteImagePin(id, tenantID string, platformAdmin bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	args := []any{strings.TrimSpace(id)}
	query := `DELETE FROM fugue_image_pins WHERE id = $1`
	if !platformAdmin {
		args = append(args, strings.TrimSpace(tenantID))
		query += fmt.Sprintf(" AND tenant_id = $%d", len(args))
	}
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return mapDBErr(err)
	}
	if count, _ := res.RowsAffected(); count == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) pgListImagePins(filter model.ImagePinFilter) ([]model.ImagePin, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clauses := []string{}
	args := []any{}
	if !filter.PlatformAdmin {
		args = append(args, strings.TrimSpace(filter.TenantID))
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if filter.ImageID != "" {
		args = append(args, filter.ImageID)
		clauses = append(clauses, fmt.Sprintf("image_id = $%d", len(args)))
	}
	if filter.AppID != "" {
		args = append(args, filter.AppID)
		clauses = append(clauses, fmt.Sprintf("app_id = $%d", len(args)))
	}
	if filter.OperationID != "" {
		args = append(args, filter.OperationID)
		clauses = append(clauses, fmt.Sprintf("operation_id = $%d", len(args)))
	}
	if filter.Reason != "" {
		args = append(args, filter.Reason)
		clauses = append(clauses, fmt.Sprintf("reason = $%d", len(args)))
	}
	query := `SELECT ` + imagePinColumns() + ` FROM fugue_image_pins`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY updated_at DESC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	out := []model.ImagePin{}
	for rows.Next() {
		pin, err := scanImagePin(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, pin)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func (s *Store) pgUpsertImageReplicationTask(task model.ImageReplicationTask) (model.ImageReplicationTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ImageReplicationTask{}, mapDBErr(err)
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	existing, err := scanImageReplicationTask(tx.QueryRowContext(ctx, `
SELECT `+imageReplicationTaskColumns()+`
FROM fugue_image_replication_tasks
WHERE ($1 <> '' AND id = $1)
   OR ($1 = '' AND image_id = $2 AND source_replica_id = $3
       AND target_node_id = $4 AND target_runtime_id = $5
       AND target_cluster_node_name = $6 AND priority = $7
       AND status IN ($8, $9))
FOR UPDATE
`, task.ID, task.ImageID, task.SourceReplicaID, task.TargetNodeID, task.TargetRuntimeID, task.TargetClusterNodeName, task.Priority, model.ImageReplicationTaskStatusPending, model.ImageReplicationTaskStatusRunning))
	if err == nil {
		task.ID = existing.ID
		task.CreatedAt = existing.CreatedAt
		if task.TenantID == "" {
			task.TenantID = existing.TenantID
		}
		if task.AppID == "" {
			task.AppID = existing.AppID
		}
		task.UpdatedAt = now
		updated, updateErr := scanImageReplicationTask(tx.QueryRowContext(ctx, `
UPDATE fugue_image_replication_tasks
SET image_id = $2, tenant_id = $3, app_id = $4, source_replica_id = $5,
	source_cache_endpoint = $6, target_node_id = $7, target_runtime_id = $8,
	target_cluster_node_name = $9, priority = $10, status = $11,
	attempts = $12, last_error = $13, updated_at = $14,
	started_at = $15, completed_at = $16
WHERE id = $1
RETURNING `+imageReplicationTaskColumns(), task.ID, task.ImageID, nullIfEmpty(task.TenantID), task.AppID, task.SourceReplicaID, task.SourceCacheEndpoint, task.TargetNodeID, task.TargetRuntimeID, task.TargetClusterNodeName, task.Priority, task.Status, task.Attempts, task.LastError, task.UpdatedAt, task.StartedAt, task.CompletedAt))
		if updateErr != nil {
			return model.ImageReplicationTask{}, mapDBErr(updateErr)
		}
		if err := tx.Commit(); err != nil {
			return model.ImageReplicationTask{}, mapDBErr(err)
		}
		return updated, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return model.ImageReplicationTask{}, mapDBErr(err)
	}
	task.ID = firstNonEmptyImageString(task.ID, model.NewID("imgtask"))
	task.CreatedAt = now
	task.UpdatedAt = now
	inserted, err := scanImageReplicationTask(tx.QueryRowContext(ctx, `
INSERT INTO fugue_image_replication_tasks (
	id, image_id, tenant_id, app_id, source_replica_id, source_cache_endpoint,
	target_node_id, target_runtime_id, target_cluster_node_name, priority,
	status, attempts, last_error, created_at, updated_at, started_at,
	completed_at
) VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9, $10,
	$11, $12, $13, $14, $15, $16,
	$17
)
RETURNING `+imageReplicationTaskColumns(), task.ID, task.ImageID, nullIfEmpty(task.TenantID), task.AppID, task.SourceReplicaID, task.SourceCacheEndpoint, task.TargetNodeID, task.TargetRuntimeID, task.TargetClusterNodeName, task.Priority, task.Status, task.Attempts, task.LastError, task.CreatedAt, task.UpdatedAt, task.StartedAt, task.CompletedAt))
	if err != nil {
		return model.ImageReplicationTask{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.ImageReplicationTask{}, mapDBErr(err)
	}
	return inserted, nil
}

func (s *Store) pgListImageReplicationTasks(filter model.ImageReplicationTaskFilter) ([]model.ImageReplicationTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clauses := []string{}
	args := []any{}
	if !filter.PlatformAdmin {
		args = append(args, strings.TrimSpace(filter.TenantID))
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if filter.ImageID != "" {
		args = append(args, filter.ImageID)
		clauses = append(clauses, fmt.Sprintf("image_id = $%d", len(args)))
	}
	if filter.AppID != "" {
		args = append(args, filter.AppID)
		clauses = append(clauses, fmt.Sprintf("app_id = $%d", len(args)))
	}
	if filter.SourceReplicaID != "" {
		args = append(args, filter.SourceReplicaID)
		clauses = append(clauses, fmt.Sprintf("source_replica_id = $%d", len(args)))
	}
	if filter.TargetNodeID != "" {
		args = append(args, filter.TargetNodeID)
		clauses = append(clauses, fmt.Sprintf("target_node_id = $%d", len(args)))
	}
	if filter.TargetRuntimeID != "" {
		args = append(args, filter.TargetRuntimeID)
		clauses = append(clauses, fmt.Sprintf("target_runtime_id = $%d", len(args)))
	}
	if filter.TargetClusterNodeName != "" {
		args = append(args, filter.TargetClusterNodeName)
		clauses = append(clauses, fmt.Sprintf("target_cluster_node_name = $%d", len(args)))
	}
	if filter.Priority != "" {
		args = append(args, filter.Priority)
		clauses = append(clauses, fmt.Sprintf("priority = $%d", len(args)))
	}
	if filter.Status != "" {
		args = append(args, filter.Status)
		clauses = append(clauses, fmt.Sprintf("status = $%d", len(args)))
	}
	query := `SELECT ` + imageReplicationTaskColumns() + ` FROM fugue_image_replication_tasks`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY updated_at DESC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	out := []model.ImageReplicationTask{}
	for rows.Next() {
		task, err := scanImageReplicationTask(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, task)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func imageFilterClauses(filter model.ImageFilter) ([]string, []any) {
	clauses := []string{}
	args := []any{}
	if !filter.PlatformAdmin {
		args = append(args, strings.TrimSpace(filter.TenantID))
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if filter.AppID != "" {
		args = append(args, filter.AppID)
		clauses = append(clauses, fmt.Sprintf("app_id = $%d", len(args)))
	}
	if filter.ImageRef != "" {
		args = append(args, filter.ImageRef)
		clauses = append(clauses, fmt.Sprintf("image_ref = $%d", len(args)))
	}
	if filter.CanonicalDigest != "" {
		args = append(args, filter.CanonicalDigest)
		clauses = append(clauses, fmt.Sprintf("canonical_digest = $%d", len(args)))
	}
	if filter.LifecycleState != "" {
		args = append(args, filter.LifecycleState)
		clauses = append(clauses, fmt.Sprintf("lifecycle_state = $%d", len(args)))
	}
	return clauses, args
}

func imageReplicaFilterClauses(filter model.ImageReplicaFilter) ([]string, []any) {
	clauses := []string{}
	args := []any{}
	if !filter.PlatformAdmin {
		args = append(args, strings.TrimSpace(filter.TenantID))
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if filter.ImageID != "" {
		args = append(args, filter.ImageID)
		clauses = append(clauses, fmt.Sprintf("image_id = $%d", len(args)))
	}
	if filter.AppID != "" {
		args = append(args, filter.AppID)
		clauses = append(clauses, fmt.Sprintf("app_id = $%d", len(args)))
	}
	if filter.Digest != "" {
		args = append(args, filter.Digest)
		clauses = append(clauses, fmt.Sprintf("digest = $%d", len(args)))
	}
	if filter.NodeID != "" {
		args = append(args, filter.NodeID)
		clauses = append(clauses, fmt.Sprintf("node_id = $%d", len(args)))
	}
	if filter.RuntimeID != "" {
		args = append(args, filter.RuntimeID)
		clauses = append(clauses, fmt.Sprintf("runtime_id = $%d", len(args)))
	}
	if filter.ClusterNodeName != "" {
		args = append(args, filter.ClusterNodeName)
		clauses = append(clauses, fmt.Sprintf("cluster_node_name = $%d", len(args)))
	}
	if filter.Status != "" {
		args = append(args, filter.Status)
		clauses = append(clauses, fmt.Sprintf("status = $%d", len(args)))
	}
	return clauses, args
}

func scanImage(scanner sqlScanner) (model.Image, error) {
	var image model.Image
	var tenantID sql.NullString
	if err := scanner.Scan(
		&image.ID,
		&tenantID,
		&image.AppID,
		&image.ImageRef,
		&image.CanonicalDigest,
		&image.MediaType,
		&image.ManifestJSON,
		&image.ManifestSizeBytes,
		&image.BlobBytes,
		&image.SourceOperationID,
		&image.LifecycleState,
		&image.RequiredReplicaCount,
		&image.MinAvailableReplicaCount,
		&image.CreatedAt,
		&image.UpdatedAt,
	); err != nil {
		return model.Image{}, err
	}
	image.TenantID = tenantID.String
	return image, nil
}

func scanImageAlias(scanner sqlScanner) (model.ImageAlias, error) {
	var alias model.ImageAlias
	var tenantID sql.NullString
	if err := scanner.Scan(
		&alias.ID,
		&alias.ImageID,
		&tenantID,
		&alias.AliasRef,
		&alias.Digest,
		&alias.CreatedAt,
		&alias.UpdatedAt,
	); err != nil {
		return model.ImageAlias{}, err
	}
	alias.TenantID = tenantID.String
	return alias, nil
}

func scanImageReplica(scanner sqlScanner) (model.ImageReplica, error) {
	var replica model.ImageReplica
	var tenantID sql.NullString
	var lastVerifiedAt, leaseExpiresAt sql.NullTime
	if err := scanner.Scan(
		&replica.ID,
		&replica.ImageID,
		&tenantID,
		&replica.AppID,
		&replica.Digest,
		&replica.NodeID,
		&replica.RuntimeID,
		&replica.ClusterNodeName,
		&replica.CacheEndpoint,
		&replica.FailureDomain,
		&replica.Status,
		&replica.SourceReplicaID,
		&lastVerifiedAt,
		&leaseExpiresAt,
		&replica.SizeBytes,
		&replica.LastError,
		&replica.CreatedAt,
		&replica.UpdatedAt,
	); err != nil {
		return model.ImageReplica{}, err
	}
	replica.TenantID = tenantID.String
	if lastVerifiedAt.Valid {
		replica.LastVerifiedAt = &lastVerifiedAt.Time
	}
	if leaseExpiresAt.Valid {
		replica.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	return replica, nil
}

func scanImagePin(scanner sqlScanner) (model.ImagePin, error) {
	var pin model.ImagePin
	var tenantID sql.NullString
	var expiresAt sql.NullTime
	if err := scanner.Scan(
		&pin.ID,
		&pin.ImageID,
		&tenantID,
		&pin.AppID,
		&pin.OperationID,
		&pin.Reason,
		&pin.MinReplicas,
		&expiresAt,
		&pin.CreatedAt,
		&pin.UpdatedAt,
	); err != nil {
		return model.ImagePin{}, err
	}
	pin.TenantID = tenantID.String
	if expiresAt.Valid {
		pin.ExpiresAt = &expiresAt.Time
	}
	return pin, nil
}

func scanImageReplicationTask(scanner sqlScanner) (model.ImageReplicationTask, error) {
	var task model.ImageReplicationTask
	var tenantID sql.NullString
	var startedAt, completedAt sql.NullTime
	if err := scanner.Scan(
		&task.ID,
		&task.ImageID,
		&tenantID,
		&task.AppID,
		&task.SourceReplicaID,
		&task.SourceCacheEndpoint,
		&task.TargetNodeID,
		&task.TargetRuntimeID,
		&task.TargetClusterNodeName,
		&task.Priority,
		&task.Status,
		&task.Attempts,
		&task.LastError,
		&task.CreatedAt,
		&task.UpdatedAt,
		&startedAt,
		&completedAt,
	); err != nil {
		return model.ImageReplicationTask{}, err
	}
	task.TenantID = tenantID.String
	if startedAt.Valid {
		task.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		task.CompletedAt = &completedAt.Time
	}
	return task, nil
}
