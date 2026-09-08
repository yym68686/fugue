package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"time"

	"fugue/internal/model"
)

const SourceUploadChunkSize = 4 << 20

var sourceRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
var sourceSHA256Pattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (s *Store) CreateSourceUploadSession(v model.SourceUploadSession) (model.SourceUploadSession, error) {
	if v.TenantID == "" || !sourceRequestIDPattern.MatchString(v.RequestID) || !sourceSHA256Pattern.MatchString(v.SHA256) || v.SizeBytes < 1 || v.SizeBytes > 128<<20 || v.Filename == "" || len(v.Filename) > 255 || filepath.Base(v.Filename) != v.Filename {
		return v, ErrInvalidInput
	}
	v.ID = model.NewID("uploadsess")
	v.SchemaVersion = 1
	v.ChunkSize = SourceUploadChunkSize
	v.State = "uploading"
	v.Chunks = map[int]string{}
	v.CreatedAt = time.Now().UTC()
	v.UpdatedAt = v.CreatedAt
	v.ExpiresAt = v.CreatedAt.Add(24 * time.Hour)
	same := func(old model.SourceUploadSession) error {
		if old.SHA256 != v.SHA256 || old.SizeBytes != v.SizeBytes || old.Filename != v.Filename || old.ActorType != v.ActorType || old.ActorID != v.ActorID || old.ProjectID != v.ProjectID {
			return ErrConflict
		}
		v = old
		return nil
	}
	if !s.usingDatabase() {
		err := s.withLockedState(true, func(st *model.State) error {
			active := 0
			for _, old := range st.SourceUploadSessions {
				if old.TenantID == v.TenantID {
					if old.RequestID == v.RequestID {
						return same(old)
					}
					if old.State == "uploading" && old.ExpiresAt.After(v.CreatedAt) {
						active++
					}
				}
			}
			if active >= 4 {
				return fmt.Errorf("at most four active upload sessions per tenant: %w", ErrConflict)
			}
			st.SourceUploadSessions = append(st.SourceUploadSessions, v)
			return nil
		})
		return v, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return v, err
	}
	defer tx.Rollback()
	// Serialize quota checks and retries for one tenant, across API replicas.
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 90217))`, v.TenantID); err != nil {
		return v, err
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT document FROM fugue_source_upload_sessions WHERE tenant_id=$1 AND request_id=$2`, v.TenantID, v.RequestID).Scan(&raw)
	if err == nil {
		var old model.SourceUploadSession
		if err = json.Unmarshal(raw, &old); err == nil {
			err = same(old)
		}
		return v, err
	}
	if err != sql.ErrNoRows {
		return v, err
	}
	var active int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM fugue_source_upload_sessions WHERE tenant_id=$1 AND document->>'state'='uploading' AND (document->>'expires_at')::timestamptz>now()`, v.TenantID).Scan(&active); err != nil {
		return v, err
	}
	if active >= 4 {
		return v, ErrConflict
	}
	raw, err = json.Marshal(v)
	if err != nil {
		return v, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_source_upload_sessions (id,tenant_id,request_id,document) VALUES ($1,$2,$3,$4)`, v.ID, v.TenantID, v.RequestID, raw); err != nil {
		return v, mapDBErr(err)
	}
	return v, tx.Commit()
}

// withSourceSession holds an exclusive row/file-state lock. Callback must not
// call another file-state transaction. Archive/chunk writes use the same lock.
func (s *Store) withSourceSession(id string, write bool, fn func(*model.SourceUploadSession, *sql.Tx) error) (model.SourceUploadSession, error) {
	var v model.SourceUploadSession
	if !sourceRequestIDPattern.MatchString(id) {
		return v, ErrInvalidInput
	}
	if !s.usingDatabase() {
		err := s.withLockedState(write, func(st *model.State) error {
			for i := range st.SourceUploadSessions {
				if st.SourceUploadSessions[i].ID == id {
					v = st.SourceUploadSessions[i]
					if err := fn(&v, nil); err != nil {
						return err
					}
					if write {
						v.UpdatedAt = time.Now().UTC()
						st.SourceUploadSessions[i] = v
					}
					return nil
				}
			}
			return ErrNotFound
		})
		return v, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return v, err
	}
	defer tx.Rollback()
	var raw []byte
	query := `SELECT document FROM fugue_source_upload_sessions WHERE id=$1`
	if write {
		query += ` FOR UPDATE`
	}
	if err = tx.QueryRowContext(ctx, query, id).Scan(&raw); err != nil {
		return v, mapDBErr(err)
	}
	if err = json.Unmarshal(raw, &v); err != nil {
		return v, err
	}
	if err = fn(&v, tx); err != nil {
		return v, err
	}
	if write {
		v.UpdatedAt = time.Now().UTC()
		raw, err = json.Marshal(v)
		if err != nil {
			return v, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE fugue_source_upload_sessions SET document=$2 WHERE id=$1`, id, raw); err != nil {
			return v, err
		}
	}
	return v, tx.Commit()
}
func (s *Store) GetSourceUploadSession(id string) (model.SourceUploadSession, error) {
	return s.withSourceSession(id, false, func(*model.SourceUploadSession, *sql.Tx) error { return nil })
}
func (s *Store) GetSourceUploadRequest(tenantID, requestID string) (model.SourceUploadSession, error) {
	var v model.SourceUploadSession
	if !s.usingDatabase() {
		err := s.withLockedState(false, func(st *model.State) error {
			for _, row := range st.SourceUploadSessions {
				if row.TenantID == tenantID && row.RequestID == requestID {
					v = row
					return nil
				}
			}
			return ErrNotFound
		})
		return v, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT document FROM fugue_source_upload_sessions WHERE tenant_id=$1 AND request_id=$2`, tenantID, requestID).Scan(&raw)
	if err != nil {
		return v, mapDBErr(err)
	}
	err = json.Unmarshal(raw, &v)
	return v, err
}
func (s *Store) sourceChunkPath(id string, index int) string {
	return filepath.Join(s.sourceUploadDir(), "sessions", fmt.Sprintf("%s.%d.chunk", id, index))
}
func (s *Store) PutSourceUploadChunk(id string, index int, digest string, data []byte) (model.SourceUploadSession, error) {
	sum := sha256.Sum256(data)
	if index < 0 || index >= 32 || !sourceSHA256Pattern.MatchString(digest) || hex.EncodeToString(sum[:]) != digest {
		return model.SourceUploadSession{}, ErrInvalidInput
	}
	return s.withSourceSession(id, true, func(v *model.SourceUploadSession, tx *sql.Tx) error {
		if v.State != "uploading" || time.Now().After(v.ExpiresAt) {
			return ErrConflict
		}
		expected := min(int64(v.ChunkSize), v.SizeBytes-int64(index*v.ChunkSize))
		if expected <= 0 || int64(len(data)) != expected {
			return ErrInvalidInput
		}
		if old, ok := v.Chunks[index]; ok {
			if old != digest {
				return ErrConflict
			}
			return nil
		}
		if tx == nil {
			if err := os.MkdirAll(filepath.Dir(s.sourceChunkPath(id, index)), 0700); err != nil {
				return err
			}
			if err := writeAtomicFile(s.sourceChunkPath(id, index), data, 0600); err != nil {
				return err
			}
		} else {
			if _, err := tx.Exec(`INSERT INTO fugue_source_upload_chunks (session_id,chunk_index,data) VALUES ($1,$2,$3) ON CONFLICT (session_id,chunk_index) DO UPDATE SET data=EXCLUDED.data`, id, index, data); err != nil {
				return err
			}
		}
		if v.Chunks == nil {
			v.Chunks = map[int]string{}
		}
		v.Chunks[index] = digest
		return nil
	})
}
func (s *Store) CompleteSourceUploadSession(id string) (model.SourceUploadSession, error) {
	return s.withSourceSession(id, true, func(v *model.SourceUploadSession, tx *sql.Tx) error {
		if v.UploadID != "" {
			return nil
		}
		if v.State != "uploading" || time.Now().After(v.ExpiresAt) {
			return ErrConflict
		}
		n := int((v.SizeBytes + int64(v.ChunkSize) - 1) / int64(v.ChunkSize))
		if len(v.Chunks) != n {
			return ErrConflict
		}
		archive := bytes.NewBuffer(make([]byte, 0, v.SizeBytes))
		for i := 0; i < n; i++ {
			var data []byte
			var err error
			if tx == nil {
				data, err = os.ReadFile(s.sourceChunkPath(id, i))
			} else {
				err = tx.QueryRow(`SELECT data FROM fugue_source_upload_chunks WHERE session_id=$1 AND chunk_index=$2`, id, i).Scan(&data)
			}
			if err != nil {
				return err
			}
			sum := sha256.Sum256(data)
			if hex.EncodeToString(sum[:]) != v.Chunks[i] {
				return ErrConflict
			}
			archive.Write(data)
		}
		sum := sha256.Sum256(archive.Bytes())
		if int64(archive.Len()) != v.SizeBytes || hex.EncodeToString(sum[:]) != v.SHA256 {
			return ErrConflict
		}
		now := time.Now().UTC()
		u := model.SourceUpload{ID: model.NewID("upload"), TenantID: v.TenantID, Filename: v.Filename, ContentType: "application/gzip", SHA256: v.SHA256, SizeBytes: v.SizeBytes, DownloadToken: model.NewSecret("fugue_upload"), CreatedAt: now, UpdatedAt: now}
		if tx == nil {
			if err := s.writeFileSourceUpload(u, archive.Bytes()); err != nil {
				return err
			}
		} else {
			if _, err := tx.Exec(`INSERT INTO fugue_source_uploads (id,tenant_id,filename,content_type,sha256,size_bytes,download_token,archive_bytes,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, u.ID, u.TenantID, u.Filename, u.ContentType, u.SHA256, u.SizeBytes, u.DownloadToken, archive.Bytes(), u.CreatedAt, u.UpdatedAt); err != nil {
				return err
			}
		}
		v.UploadID = u.ID
		v.State = "ready"
		return nil
	})
}
func (s *Store) BeginSourceUploadSubmission(id, hash string) (model.SourceUploadSession, bool, error) {
	started := false
	v, err := s.withSourceSession(id, true, func(v *model.SourceUploadSession, _ *sql.Tx) error {
		if !sourceSHA256Pattern.MatchString(hash) {
			return ErrInvalidInput
		}
		if v.RequestHash != "" {
			if v.RequestHash != hash {
				return ErrConflict
			}
			return nil
		}
		if v.State != "ready" || v.UploadID == "" || time.Now().After(v.ExpiresAt) {
			return ErrConflict
		}
		v.RequestHash = hash
		v.State = "submitting"
		started = true
		return nil
	})
	return v, started, err
}
func (s *Store) FinishSourceUploadSubmission(id string, status int) (model.SourceUploadSession, error) {
	return s.withSourceSession(id, true, func(v *model.SourceUploadSession, _ *sql.Tx) error {
		if v.State != "submitting" {
			return ErrConflict
		}
		v.ResponseStatus = status
		if status >= 200 && status < 300 {
			v.State = "submitted"

		} else {
			v.State = "unknown"
		}
		return nil
	})
}

func recordSourceSessionOperation(v *model.SourceUploadSession, op model.Operation) error {
	if v.TenantID != op.TenantID || v.State != "submitting" {
		return ErrConflict
	}
	if !slices.Contains(v.OperationIDs, op.ID) {
		v.OperationIDs = append(v.OperationIDs, op.ID)
	}
	if !slices.Contains(v.AppIDs, op.AppID) {
		v.AppIDs = append(v.AppIDs, op.AppID)
	}
	v.UpdatedAt = time.Now().UTC()
	return nil
}

// CreateSourceSessionOperation commits the operation and its request attribution
// in one transaction. Later rebuilds of the same upload are not attributed here.
func (s *Store) CreateSourceSessionOperation(op model.Operation, sessionID string) (model.Operation, error) {
	if sessionID == "" {
		return s.CreateOperation(op)
	}
	if !sourceRequestIDPattern.MatchString(sessionID) {
		return model.Operation{}, ErrInvalidInput
	}
	created, _, err := s.createOperationWithPolicy(op, operationCreatePolicy{SourceSessionID: sessionID})
	return created, err
}

// Expired incomplete chunks and assembled chunks are disposable. Receipts and
// referenced immutable archives are kept for recovery; this never removes them.
func (s *Store) CleanupSourceUploadChunks() error {
	now := time.Now().UTC()
	if !s.usingDatabase() {
		return s.withLockedState(true, func(st *model.State) error {
			for i := range st.SourceUploadSessions {
				v := &st.SourceUploadSessions[i]
				if v.UploadID == "" && v.ExpiresAt.After(now) {
					continue
				}
				if !sourceRequestIDPattern.MatchString(v.ID) {
					return ErrInvalidInput
				}
				for index := range v.Chunks {
					if err := os.Remove(s.sourceChunkPath(v.ID, index)); err != nil && !os.IsNotExist(err) {
						return err
					}
				}
				if v.UploadID == "" {
					v.State = "expired"
					v.Chunks = nil
					v.UpdatedAt = now
				}
			}
			return nil
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Lock expired rows to fence concurrent PUT/complete before chunk deletion.
	if _, err = tx.ExecContext(ctx, `UPDATE fugue_source_upload_sessions SET document=jsonb_set(document,'{state}','"expired"'::jsonb) - 'chunks' WHERE document->>'state'='uploading' AND (document->>'expires_at')::timestamptz <= now()`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM fugue_source_upload_chunks c USING fugue_source_upload_sessions s WHERE c.session_id=s.id AND (s.document->>'state'='expired' OR COALESCE(s.document->>'upload_id','')<>'')`); err != nil {
		return err
	}
	return tx.Commit()
}
