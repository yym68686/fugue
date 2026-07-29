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

func (s *Store) pgUpsertImageLocation(location model.ImageLocation) (model.ImageLocation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ImageLocation{}, mapDBErr(err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	existing, err := scanImageLocation(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, app_id, image_ref, digest, source_operation_id, node_id, runtime_id, cluster_node_name, cache_endpoint, status, last_seen_at, size_bytes, last_error, created_at, updated_at
FROM fugue_image_locations
WHERE ($1 <> '' AND id = $1)
   OR ($1 = '' AND COALESCE(tenant_id, '') = COALESCE($2::text, '')
       AND image_ref = $3
       AND digest = $4
       AND node_id = $5
       AND runtime_id = $6
       AND cluster_node_name = $7)
FOR UPDATE
`, location.ID, nullIfEmpty(location.TenantID), location.ImageRef, location.Digest, location.NodeID, location.RuntimeID, location.ClusterNodeName))
	if err == nil {
		if location.TenantID == "" {
			location.TenantID = existing.TenantID
		}
		if location.AppID == "" {
			location.AppID = existing.AppID
		}
		if location.SourceOperationID == "" {
			location.SourceOperationID = existing.SourceOperationID
		}
		if location.NodeID == "" {
			location.NodeID = existing.NodeID
		}
		if location.RuntimeID == "" {
			location.RuntimeID = existing.RuntimeID
		}
		if location.ClusterNodeName == "" {
			location.ClusterNodeName = existing.ClusterNodeName
		}
		if location.CacheEndpoint == "" {
			location.CacheEndpoint = existing.CacheEndpoint
		}
		if location.LastSeenAt == nil {
			location.LastSeenAt = existing.LastSeenAt
		}
		location.ID = existing.ID
		location.CreatedAt = existing.CreatedAt
		location.UpdatedAt = now
		updated, updateErr := scanImageLocation(tx.QueryRowContext(ctx, `
UPDATE fugue_image_locations
SET tenant_id = $2,
	app_id = $3,
	image_ref = $4,
	digest = $5,
	source_operation_id = $6,
	node_id = $7,
	runtime_id = $8,
	cluster_node_name = $9,
	cache_endpoint = $10,
	status = $11,
	last_seen_at = $12,
	size_bytes = $13,
	last_error = $14,
	updated_at = $15
WHERE id = $1
RETURNING id, tenant_id, app_id, image_ref, digest, source_operation_id, node_id, runtime_id, cluster_node_name, cache_endpoint, status, last_seen_at, size_bytes, last_error, created_at, updated_at
`, location.ID, nullIfEmpty(location.TenantID), location.AppID, location.ImageRef, location.Digest, location.SourceOperationID, location.NodeID, location.RuntimeID, location.ClusterNodeName, location.CacheEndpoint, location.Status, location.LastSeenAt, location.SizeBytes, location.LastError, location.UpdatedAt))
		if updateErr != nil {
			return model.ImageLocation{}, mapDBErr(updateErr)
		}
		if err := tx.Commit(); err != nil {
			return model.ImageLocation{}, mapDBErr(err)
		}
		return updated, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return model.ImageLocation{}, mapDBErr(err)
	}

	location.ID = model.NewID("imgloc")
	location.CreatedAt = now
	location.UpdatedAt = now
	if location.LastSeenAt == nil {
		location.LastSeenAt = &now
	}
	inserted, err := scanImageLocation(tx.QueryRowContext(ctx, `
INSERT INTO fugue_image_locations (
	id, tenant_id, app_id, image_ref, digest, source_operation_id, node_id,
	runtime_id, cluster_node_name, cache_endpoint, status, last_seen_at,
	size_bytes, last_error, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10, $11, $12,
	$13, $14, $15, $16
)
RETURNING id, tenant_id, app_id, image_ref, digest, source_operation_id, node_id, runtime_id, cluster_node_name, cache_endpoint, status, last_seen_at, size_bytes, last_error, created_at, updated_at
`, location.ID, nullIfEmpty(location.TenantID), location.AppID, location.ImageRef, location.Digest, location.SourceOperationID, location.NodeID, location.RuntimeID, location.ClusterNodeName, location.CacheEndpoint, location.Status, location.LastSeenAt, location.SizeBytes, location.LastError, location.CreatedAt, location.UpdatedAt))
	if err != nil {
		return model.ImageLocation{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.ImageLocation{}, mapDBErr(err)
	}
	return inserted, nil
}

func (s *Store) pgListImageLocations(filter model.ImageLocationFilter) ([]model.ImageLocation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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
	if filter.Digest != "" {
		args = append(args, filter.Digest)
		clauses = append(clauses, fmt.Sprintf("digest = $%d", len(args)))
	}
	if filter.Status != "" {
		args = append(args, filter.Status)
		clauses = append(clauses, fmt.Sprintf("status = $%d", len(args)))
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

	query := `
SELECT id, tenant_id, app_id, image_ref, digest, source_operation_id, node_id, runtime_id, cluster_node_name, cache_endpoint, status, last_seen_at, size_bytes, last_error, created_at, updated_at
FROM fugue_image_locations`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY COALESCE(last_seen_at, updated_at) DESC, updated_at DESC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	return scanImageLocationRows(rows)
}

func scanImageLocationRows(rows *sql.Rows) ([]model.ImageLocation, error) {
	out := []model.ImageLocation{}
	for rows.Next() {
		location, err := scanImageLocation(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, location)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func scanImageLocation(scanner sqlScanner) (model.ImageLocation, error) {
	var location model.ImageLocation
	var tenantID sql.NullString
	var lastSeenAt sql.NullTime
	if err := scanner.Scan(
		&location.ID,
		&tenantID,
		&location.AppID,
		&location.ImageRef,
		&location.Digest,
		&location.SourceOperationID,
		&location.NodeID,
		&location.RuntimeID,
		&location.ClusterNodeName,
		&location.CacheEndpoint,
		&location.Status,
		&lastSeenAt,
		&location.SizeBytes,
		&location.LastError,
		&location.CreatedAt,
		&location.UpdatedAt,
	); err != nil {
		return model.ImageLocation{}, err
	}
	location.TenantID = tenantID.String
	if lastSeenAt.Valid {
		location.LastSeenAt = &lastSeenAt.Time
	}
	return location, nil
}
