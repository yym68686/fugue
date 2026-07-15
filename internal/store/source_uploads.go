package store

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/model"
)

type sourceUploadFileEnvelope struct {
	Upload        model.SourceUpload `json:"upload"`
	DownloadToken string             `json:"download_token"`
}

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
	if s.usingDatabase() {
		return s.pgCreateSourceUpload(upload, archive)
	}
	if err := s.writeFileSourceUpload(upload, archive); err != nil {
		return model.SourceUpload{}, err
	}
	return upload, nil
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
	if s.usingDatabase() {
		return s.pgDiscardNewSourceUpload(upload)
	}

	// withFileLockedState uses an exclusive process/file lock even for read-only
	// state access. Holding it across reference validation and deletion prevents
	// a job/operation writer from publishing this upload between the two steps.
	return s.withLockedState(false, func(state *model.State) error {
		metadataPath := s.sourceUploadMetadataPath(upload.ID)
		metadataBytes, err := os.ReadFile(metadataPath)
		if err != nil {
			if os.IsNotExist(err) {
				return ErrNotFound
			}
			return fmt.Errorf("read new source upload metadata for discard: %w", err)
		}
		var envelope sourceUploadFileEnvelope
		if err := json.Unmarshal(metadataBytes, &envelope); err != nil {
			return fmt.Errorf("unmarshal new source upload metadata for discard: %w", err)
		}
		storedTokenHash := sha256.Sum256([]byte(envelope.DownloadToken))
		providedTokenHash := sha256.Sum256([]byte(upload.DownloadToken))
		tokenMatches := subtle.ConstantTimeCompare(storedTokenHash[:], providedTokenHash[:])
		if envelope.Upload.ID != upload.ID ||
			envelope.Upload.TenantID != upload.TenantID ||
			tokenMatches != 1 {
			return ErrNotFound
		}
		if sourceUploadReferencedInState(state, upload.ID) {
			return ErrConflict
		}

		// Remove the sensitive archive first. If metadata removal then fails, the
		// remaining record contains no dump and the error makes the partial cleanup
		// observable for a later repair.
		if err := os.Remove(s.sourceUploadArchivePath(upload.ID)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove new source upload archive: %w", err)
		}
		if err := os.Remove(metadataPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove new source upload metadata after archive removal: %w", err)
		}
		return nil
	})
}

func sourceUploadReferencedInState(state *model.State, uploadID string) bool {
	if state == nil || strings.TrimSpace(uploadID) == "" {
		return false
	}
	uploadID = strings.TrimSpace(uploadID)
	for _, job := range state.AppDatabaseImportJobs {
		if strings.TrimSpace(job.SourceUploadID) == uploadID {
			return true
		}
	}
	for _, app := range state.Apps {
		if appSourceReferencesUpload(app.Source, uploadID) ||
			appSourceReferencesUpload(app.OriginSource, uploadID) ||
			appSourceReferencesUpload(app.BuildSource, uploadID) {
			return true
		}
	}
	for _, operation := range state.Operations {
		if appSourceReferencesUpload(operation.DesiredSource, uploadID) ||
			appSourceReferencesUpload(operation.DesiredOriginSource, uploadID) {
			return true
		}
	}
	return false
}

func appSourceReferencesUpload(source *model.AppSource, uploadID string) bool {
	return source != nil && strings.TrimSpace(source.UploadID) == strings.TrimSpace(uploadID)
}

func (s *Store) GetSourceUpload(id string) (model.SourceUpload, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.SourceUpload{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetSourceUpload(id)
	}
	upload, _, err := s.readFileSourceUpload(id)
	return upload, err
}

func (s *Store) GetSourceUploadArchive(id string) (model.SourceUpload, []byte, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.SourceUpload{}, nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetSourceUploadArchive(id)
	}
	return s.readFileSourceUpload(id)
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

func (s *Store) sourceUploadDir() string {
	baseDir := filepath.Dir(strings.TrimSpace(s.path))
	if baseDir == "" || baseDir == "." {
		baseDir = "."
	}
	return filepath.Join(baseDir, "source-uploads")
}

func (s *Store) sourceUploadMetadataPath(id string) string {
	return filepath.Join(s.sourceUploadDir(), id+".json")
}

func (s *Store) sourceUploadArchivePath(id string) string {
	return filepath.Join(s.sourceUploadDir(), id+".tgz")
}

func (s *Store) writeFileSourceUpload(upload model.SourceUpload, archive []byte) error {
	if err := os.MkdirAll(s.sourceUploadDir(), 0o755); err != nil {
		return fmt.Errorf("create source upload directory: %w", err)
	}

	envelope := sourceUploadFileEnvelope{
		Upload:        upload,
		DownloadToken: upload.DownloadToken,
	}
	metadataBytes, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal source upload metadata: %w", err)
	}

	metadataPath := s.sourceUploadMetadataPath(upload.ID)
	archivePath := s.sourceUploadArchivePath(upload.ID)
	if err := writeAtomicFile(metadataPath, metadataBytes, 0o600); err != nil {
		return fmt.Errorf("write source upload metadata: %w", err)
	}
	if err := writeAtomicFile(archivePath, archive, 0o600); err != nil {
		return fmt.Errorf("write source upload archive: %w", err)
	}
	return nil
}

func (s *Store) readFileSourceUpload(id string) (model.SourceUpload, []byte, error) {
	metadataBytes, err := os.ReadFile(s.sourceUploadMetadataPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return model.SourceUpload{}, nil, ErrNotFound
		}
		return model.SourceUpload{}, nil, fmt.Errorf("read source upload metadata: %w", err)
	}
	var envelope sourceUploadFileEnvelope
	if err := json.Unmarshal(metadataBytes, &envelope); err != nil {
		return model.SourceUpload{}, nil, fmt.Errorf("unmarshal source upload metadata: %w", err)
	}
	envelope.Upload.DownloadToken = strings.TrimSpace(envelope.DownloadToken)
	archiveBytes, err := os.ReadFile(s.sourceUploadArchivePath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return model.SourceUpload{}, nil, ErrNotFound
		}
		return model.SourceUpload{}, nil, fmt.Errorf("read source upload archive: %w", err)
	}
	return envelope.Upload, archiveBytes, nil
}

func writeAtomicFile(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tempFile, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)
	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		return err
	}
	if err := tempFile.Chmod(mode); err != nil {
		tempFile.Close()
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
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
