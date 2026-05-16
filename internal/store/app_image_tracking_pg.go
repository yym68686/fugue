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

func (s *Store) pgUpsertAppImageTracking(tracking model.AppImageTracking) (model.AppImageTracking, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AppImageTracking{}, mapDBErr(err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, tracking.AppID, false)
	if err != nil {
		return model.AppImageTracking{}, mapDBErr(err)
	}
	if tracking.TenantID != "" && tracking.TenantID != app.TenantID {
		return model.AppImageTracking{}, ErrNotFound
	}
	tracking.TenantID = app.TenantID

	now := time.Now().UTC()
	existing, err := scanAppImageTracking(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, app_id, image_ref, enabled, last_seen_digest, last_queued_digest, last_deployed_digest, last_operation_id, last_delivery_id, last_event, last_error, last_checked_at, last_triggered_at, created_at, updated_at
FROM fugue_app_image_trackings
WHERE app_id = $1
FOR UPDATE
`, tracking.AppID))
	if err == nil {
		tracking.ID = existing.ID
		tracking.CreatedAt = existing.CreatedAt
		preserveAppImageTrackingObservedFields(&tracking, existing)
		tracking.UpdatedAt = now
		out, updateErr := scanAppImageTracking(tx.QueryRowContext(ctx, `
UPDATE fugue_app_image_trackings
SET tenant_id = $2,
	image_ref = $3,
	enabled = $4,
	last_seen_digest = $5,
	last_queued_digest = $6,
	last_deployed_digest = $7,
	last_operation_id = $8,
	last_delivery_id = $9,
	last_event = $10,
	last_error = $11,
	last_checked_at = $12,
	last_triggered_at = $13,
	updated_at = $14
WHERE id = $1
RETURNING id, tenant_id, app_id, image_ref, enabled, last_seen_digest, last_queued_digest, last_deployed_digest, last_operation_id, last_delivery_id, last_event, last_error, last_checked_at, last_triggered_at, created_at, updated_at
`, tracking.ID, tracking.TenantID, tracking.ImageRef, tracking.Enabled, tracking.LastSeenDigest, tracking.LastQueuedDigest, tracking.LastDeployedDigest, tracking.LastOperationID, tracking.LastDeliveryID, tracking.LastEvent, tracking.LastError, tracking.LastCheckedAt, tracking.LastTriggeredAt, tracking.UpdatedAt))
		if updateErr != nil {
			return model.AppImageTracking{}, mapDBErr(updateErr)
		}
		if err := tx.Commit(); err != nil {
			return model.AppImageTracking{}, mapDBErr(err)
		}
		return out, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return model.AppImageTracking{}, mapDBErr(err)
	}

	tracking.ID = model.NewID("imgtrack")
	tracking.CreatedAt = now
	tracking.UpdatedAt = now
	out, err := scanAppImageTracking(tx.QueryRowContext(ctx, `
INSERT INTO fugue_app_image_trackings (
	id, tenant_id, app_id, image_ref, enabled, last_seen_digest, last_queued_digest,
	last_deployed_digest, last_operation_id, last_delivery_id, last_event, last_error,
	last_checked_at, last_triggered_at, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10, $11, $12,
	$13, $14, $15, $16
)
RETURNING id, tenant_id, app_id, image_ref, enabled, last_seen_digest, last_queued_digest, last_deployed_digest, last_operation_id, last_delivery_id, last_event, last_error, last_checked_at, last_triggered_at, created_at, updated_at
`, tracking.ID, tracking.TenantID, tracking.AppID, tracking.ImageRef, tracking.Enabled, tracking.LastSeenDigest, tracking.LastQueuedDigest, tracking.LastDeployedDigest, tracking.LastOperationID, tracking.LastDeliveryID, tracking.LastEvent, tracking.LastError, tracking.LastCheckedAt, tracking.LastTriggeredAt, tracking.CreatedAt, tracking.UpdatedAt))
	if err != nil {
		return model.AppImageTracking{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.AppImageTracking{}, mapDBErr(err)
	}
	return out, nil
}

func (s *Store) pgGetAppImageTracking(tenantID string, platformAdmin bool, appID string) (model.AppImageTracking, error) {
	filter := model.AppImageTrackingFilter{
		TenantID:      tenantID,
		PlatformAdmin: platformAdmin,
		AppID:         appID,
	}
	trackings, err := s.pgListAppImageTrackings(filter)
	if err != nil {
		return model.AppImageTracking{}, err
	}
	if len(trackings) == 0 {
		return model.AppImageTracking{}, ErrNotFound
	}
	return trackings[0], nil
}

func (s *Store) pgListAppImageTrackings(filter model.AppImageTrackingFilter) ([]model.AppImageTracking, error) {
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
	if filter.Enabled != nil {
		args = append(args, *filter.Enabled)
		clauses = append(clauses, fmt.Sprintf("enabled = $%d", len(args)))
	}

	query := `
SELECT id, tenant_id, app_id, image_ref, enabled, last_seen_digest, last_queued_digest, last_deployed_digest, last_operation_id, last_delivery_id, last_event, last_error, last_checked_at, last_triggered_at, created_at, updated_at
FROM fugue_app_image_trackings`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY updated_at DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	return scanAppImageTrackingRows(rows)
}

func (s *Store) pgRecordAppImageTrackingCheck(id, digest, deliveryID, event, lastError string) (model.AppImageTracking, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	tracking, err := scanAppImageTracking(s.db.QueryRowContext(ctx, `
UPDATE fugue_app_image_trackings
SET last_seen_digest = CASE WHEN $2 <> '' THEN $2 ELSE last_seen_digest END,
	last_delivery_id = $3,
	last_event = $4,
	last_error = $5,
	last_checked_at = $6,
	updated_at = $6
WHERE id = $1
RETURNING id, tenant_id, app_id, image_ref, enabled, last_seen_digest, last_queued_digest, last_deployed_digest, last_operation_id, last_delivery_id, last_event, last_error, last_checked_at, last_triggered_at, created_at, updated_at
`, id, digest, strings.TrimSpace(deliveryID), strings.TrimSpace(event), strings.TrimSpace(lastError), now))
	return tracking, mapDBErr(err)
}

func (s *Store) pgRecordAppImageTrackingQueued(id, digest, operationID, deliveryID, event string) (model.AppImageTracking, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	tracking, err := scanAppImageTracking(s.db.QueryRowContext(ctx, `
UPDATE fugue_app_image_trackings
SET last_seen_digest = $2,
	last_queued_digest = $2,
	last_operation_id = $3,
	last_delivery_id = $4,
	last_event = $5,
	last_error = '',
	last_checked_at = $6,
	last_triggered_at = $6,
	updated_at = $6
WHERE id = $1
RETURNING id, tenant_id, app_id, image_ref, enabled, last_seen_digest, last_queued_digest, last_deployed_digest, last_operation_id, last_delivery_id, last_event, last_error, last_checked_at, last_triggered_at, created_at, updated_at
`, id, digest, operationID, strings.TrimSpace(deliveryID), strings.TrimSpace(event), now))
	return tracking, mapDBErr(err)
}

func (s *Store) pgUpdateAppImageTrackingDeployedTx(ctx context.Context, tx *sql.Tx, op model.Operation, now time.Time) error {
	if op.Type != model.OperationTypeDeploy || op.DesiredSource == nil || strings.TrimSpace(op.DesiredSource.Type) != model.AppSourceTypeDockerImage {
		return nil
	}
	imageRef := strings.TrimSpace(op.DesiredSource.ImageRef)
	if imageRef == "" {
		return nil
	}
	digest := digestFromImageReference(op.DesiredSource.ResolvedImageRef)
	if digest == "" && op.DesiredSpec != nil {
		digest = digestFromImageReference(op.DesiredSpec.Image)
	}
	if digest == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
UPDATE fugue_app_image_trackings
SET last_deployed_digest = $3,
	last_operation_id = $4,
	last_error = '',
	updated_at = $5
WHERE app_id = $1
  AND image_ref = $2
`, op.AppID, imageRef, digest, op.ID, now)
	return mapDBErr(err)
}

func scanAppImageTrackingRows(rows *sql.Rows) ([]model.AppImageTracking, error) {
	out := []model.AppImageTracking{}
	for rows.Next() {
		tracking, err := scanAppImageTracking(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, tracking)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func scanAppImageTracking(scanner sqlScanner) (model.AppImageTracking, error) {
	var tracking model.AppImageTracking
	var lastCheckedAt sql.NullTime
	var lastTriggeredAt sql.NullTime
	if err := scanner.Scan(
		&tracking.ID,
		&tracking.TenantID,
		&tracking.AppID,
		&tracking.ImageRef,
		&tracking.Enabled,
		&tracking.LastSeenDigest,
		&tracking.LastQueuedDigest,
		&tracking.LastDeployedDigest,
		&tracking.LastOperationID,
		&tracking.LastDeliveryID,
		&tracking.LastEvent,
		&tracking.LastError,
		&lastCheckedAt,
		&lastTriggeredAt,
		&tracking.CreatedAt,
		&tracking.UpdatedAt,
	); err != nil {
		return model.AppImageTracking{}, mapDBErr(err)
	}
	if lastCheckedAt.Valid {
		tracking.LastCheckedAt = &lastCheckedAt.Time
	}
	if lastTriggeredAt.Valid {
		tracking.LastTriggeredAt = &lastTriggeredAt.Time
	}
	return tracking, nil
}
