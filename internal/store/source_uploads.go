package store

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/model"
)

func isQueuedImportSourceType(sourceType string) bool {
	switch strings.TrimSpace(sourceType) {
	case model.AppSourceTypeGitHubPublic, model.AppSourceTypeGitHubPrivate, model.AppSourceTypeDockerImage, model.AppSourceTypeUpload:
		return true
	default:
		return false
	}
}

func (s *Store) CreateSourceUpload(tenantID, filename, contentType string, archive []byte) (model.SourceUpload, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" || len(archive) == 0 {
		return model.SourceUpload{}, ErrInvalidInput
	}

	sum := sha256.Sum256(archive)
	now := time.Now().UTC()
	upload := model.SourceUpload{
		ID:            model.NewID("upload"),
		TenantID:      tenantID,
		Filename:      strings.TrimSpace(filename),
		ContentType:   strings.TrimSpace(contentType),
		SHA256:        hex.EncodeToString(sum[:]),
		SizeBytes:     int64(len(archive)),
		DownloadToken: model.NewSecret("fugue_upload"),
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	return s.pgCreateSourceUpload(upload, archive)
}

// DiscardNewSourceUpload is intentionally narrow: it only accepts the complete
// capability returned by CreateSourceUpload, and refuses to remove an upload
// once any durable product object references it. It exists for compensating a
// failed create immediately after an upload; it is not a general delete API.
func (s *Store) DiscardNewSourceUpload(upload model.SourceUpload) error {
	if upload.ID == "" || upload.TenantID == "" || strings.TrimSpace(upload.DownloadToken) == "" ||
		upload.ID != strings.TrimSpace(upload.ID) || upload.TenantID != strings.TrimSpace(upload.TenantID) ||
		filepath.Base(upload.ID) != upload.ID || strings.ContainsAny(upload.ID, `/\`) {
		return ErrInvalidInput
	}
	return s.pgDiscardNewSourceUpload(upload)
}

func (s *Store) GetSourceUpload(id string) (model.SourceUpload, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.SourceUpload{}, ErrInvalidInput
	}
	return s.pgGetSourceUpload(id)
}

func (s *Store) GetSourceUploadArchive(id string) (model.SourceUpload, []byte, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.SourceUpload{}, nil, ErrInvalidInput
	}
	return s.pgGetSourceUploadArchive(id)
}

func (s *Store) GetSourceUploadArchiveByToken(id, token string) (model.SourceUpload, []byte, error) {
	id = strings.TrimSpace(id)
	token = strings.TrimSpace(token)
	if id == "" || token == "" {
		return model.SourceUpload{}, nil, ErrInvalidInput
	}
	upload, archive, err := s.GetSourceUploadArchive(id)
	if err != nil {
		return model.SourceUpload{}, nil, err
	}
	if subtle.ConstantTimeCompare([]byte(upload.DownloadToken), []byte(token)) != 1 {
		return model.SourceUpload{}, nil, ErrNotFound
	}
	return upload, archive, nil
}

func (s *Store) pgCreateSourceUpload(upload model.SourceUpload, archive []byte) (model.SourceUpload, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := s.db.ExecContext(ctx, `
INSERT INTO fugue_source_uploads (id, tenant_id, filename, content_type, sha256, size_bytes, download_token, archive_bytes, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
`, upload.ID, upload.TenantID, upload.Filename, upload.ContentType, upload.SHA256, upload.SizeBytes, upload.DownloadToken, archive, upload.CreatedAt, upload.UpdatedAt)
	if err != nil {
		return model.SourceUpload{}, mapDBErr(err)
	}
	return upload, nil
}

func (s *Store) pgDiscardNewSourceUpload(upload model.SourceUpload) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin new source upload discard transaction: %w", err)
	}
	defer tx.Rollback()

	var lockedID string
	if err := tx.QueryRowContext(ctx, `
SELECT id
FROM fugue_source_uploads
WHERE id = $1 AND tenant_id = $2 AND download_token = $3
FOR UPDATE
`, upload.ID, upload.TenantID, upload.DownloadToken).Scan(&lockedID); err != nil {
		return mapDBErr(err)
	}
	var referenced bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_app_database_import_jobs
	WHERE source_upload_id = $1 AND tenant_id = $2
	UNION ALL
	SELECT 1
	FROM fugue_apps
	WHERE tenant_id = $2
	  AND (
		source_json->>'upload_id' = $1
		OR source_json->'origin_source'->>'upload_id' = $1
		OR source_json->'build_source'->>'upload_id' = $1
	  )
	UNION ALL
	SELECT 1
	FROM fugue_operations
	WHERE tenant_id = $2
	  AND (
		desired_source_json->>'upload_id' = $1
		OR desired_source_json->'desired_source'->>'upload_id' = $1
		OR desired_source_json->'desired_origin_source'->>'upload_id' = $1
	  )
)
`, upload.ID, upload.TenantID).Scan(&referenced); err != nil {
		return fmt.Errorf("check new source upload references before discard: %w", err)
	}
	if referenced {
		return ErrConflict
	}

	var deletedID string
	if err := tx.QueryRowContext(ctx, `
DELETE FROM fugue_source_uploads AS upload
WHERE upload.id = $1
  AND upload.tenant_id = $2
  AND upload.download_token = $3
  AND NOT EXISTS (
	SELECT 1
	FROM fugue_app_database_import_jobs AS import_job
	WHERE import_job.source_upload_id = upload.id
	)
RETURNING upload.id
`, upload.ID, upload.TenantID, upload.DownloadToken).Scan(&deletedID); err != nil {
		if mapped := mapDBErr(err); mapped == ErrNotFound {
			return ErrConflict
		}
		return mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit new source upload discard transaction: %w", err)
	}
	return nil
}

func (s *Store) pgGetSourceUpload(id string) (model.SourceUpload, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	upload, _, err := s.pgReadSourceUpload(ctx, id, false)
	return upload, err
}

func (s *Store) pgGetSourceUploadArchive(id string) (model.SourceUpload, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	return s.pgReadSourceUpload(ctx, id, true)
}

func (s *Store) pgReadSourceUpload(ctx context.Context, id string, includeArchive bool) (model.SourceUpload, []byte, error) {
	query := `
SELECT id, tenant_id, filename, content_type, sha256, size_bytes, download_token, created_at, updated_at`
	if includeArchive {
		query += `, archive_bytes`
	}
	query += `
FROM fugue_source_uploads
WHERE id = $1
`

	var upload model.SourceUpload
	var archive []byte
	var err error
	if includeArchive {
		err = s.db.QueryRowContext(ctx, query, id).Scan(
			&upload.ID,
			&upload.TenantID,
			&upload.Filename,
			&upload.ContentType,
			&upload.SHA256,
			&upload.SizeBytes,
			&upload.DownloadToken,
			&upload.CreatedAt,
			&upload.UpdatedAt,
			&archive,
		)
	} else {
		err = s.db.QueryRowContext(ctx, query, id).Scan(
			&upload.ID,
			&upload.TenantID,
			&upload.Filename,
			&upload.ContentType,
			&upload.SHA256,
			&upload.SizeBytes,
			&upload.DownloadToken,
			&upload.CreatedAt,
			&upload.UpdatedAt,
		)
	}
	if err != nil {
		return model.SourceUpload{}, nil, mapDBErr(err)
	}
	return upload, archive, nil
}
