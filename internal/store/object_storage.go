package store

import (
	"context"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	"strings"
	"time"
)

func (s *Store) GetObjectStorageConfig() (model.ObjectStorageConfig, error) {
	var v model.ObjectStorageConfig
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT body FROM fugue_object_storage_config WHERE id='platform'").Scan(&raw)
	if err != nil {
		return v, mapDBErr(err)
	}
	err = json.Unmarshal(raw, &v)
	return v, err
}
func (s *Store) SaveObjectStorageConfig(v model.ObjectStorageConfig) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = s.db.ExecContext(ctx, "INSERT INTO fugue_object_storage_config(id,body) VALUES('platform',$1::jsonb) ON CONFLICT(id) DO UPDATE SET body=EXCLUDED.body", string(raw))
	return mapDBErr(err)
}

func (s *Store) GetBackblazeB2Config() (*model.ObjectStorageProviderConfig, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var raw []byte
	if err := s.db.QueryRowContext(ctx, "SELECT body FROM fugue_object_storage_providers WHERE provider=$1", model.DataBackendProviderBackblazeB2).Scan(&raw); err != nil {
		return nil, mapDBErr(err)
	}
	var out model.ObjectStorageProviderConfig
	return &out, json.Unmarshal(raw, &out)
}

func (s *Store) SaveBackblazeB2Config(v model.ObjectStorageProviderConfig) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = s.db.ExecContext(ctx, "INSERT INTO fugue_object_storage_providers(provider,body) VALUES($1,$2::jsonb) ON CONFLICT(provider) DO UPDATE SET body=EXCLUDED.body", model.DataBackendProviderBackblazeB2, string(raw))
	return mapDBErr(err)
}
func (s *Store) SealObjectStorageSecret(v model.DataBackendCredentials) (model.DataBackendSecret, error) {
	if _, keyID := dataBackendSecretKey(); keyID == "development-fallback" {
		return model.DataBackendSecret{}, fmt.Errorf("configure production credential encryption before enabling object storage")
	}
	return encryptDataBackendSecret(model.DataBackendSecret{}, v)
}
func (s *Store) OpenObjectStorageSecret(v model.DataBackendSecret) (model.DataBackendCredentials, error) {
	return decryptDataBackendSecret(v)
}
func (s *Store) ListObjectStores(tenantID, projectID string) ([]model.ObjectStore, error) {
	out := []model.ObjectStore{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, "SELECT body FROM fugue_object_stores WHERE ($1='' OR tenant_id=$1) AND ($2='' OR project_id=$2) ORDER BY id", tenantID, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var v model.ObjectStore
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) GetObjectStore(id string) (model.ObjectStore, error) {
	var out model.ObjectStore
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT body FROM fugue_object_stores WHERE id=$1", id).Scan(&raw)
	if err != nil {
		return out, mapDBErr(err)
	}
	err = json.Unmarshal(raw, &out)
	return out, err
}
func (s *Store) SaveObjectStore(v model.ObjectStore) error {
	if v.ID == "" || v.TenantID == "" || v.ProjectID == "" || v.Name == "" {
		return ErrInvalidInput
	}
	v.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = s.db.ExecContext(ctx, `INSERT INTO fugue_object_stores(id,tenant_id,project_id,name,body) VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT(id) DO UPDATE SET body=EXCLUDED.body WHERE fugue_object_stores.tenant_id=EXCLUDED.tenant_id AND fugue_object_stores.project_id=EXCLUDED.project_id AND fugue_object_stores.name=EXCLUDED.name`, v.ID, v.TenantID, v.ProjectID, v.Name, string(raw))
	return mapDBErr(err)
}

// DeleteObjectStore removes the durable metadata after the provider bucket
// has already been emptied and deleted. Credentials are removed first so the
// PostgreSQL foreign-key relationship remains valid.
func (s *Store) DeleteObjectStore(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return mapDBErr(err)
	}
	defer tx.Rollback()
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT body FROM fugue_object_stores WHERE id=$1 FOR UPDATE", id).Scan(&raw); err != nil {
		return mapDBErr(err)
	}
	var v model.ObjectStore
	if err = json.Unmarshal(raw, &v); err != nil {
		return err
	}
	if v.Status != "deleting" {
		return ErrConflict
	}
	rows, err := tx.QueryContext(ctx, "SELECT body FROM fugue_object_storage_credentials WHERE store_id=$1 FOR UPDATE", id)
	if err != nil {
		return mapDBErr(err)
	}
	for rows.Next() {
		var rec model.ObjectStorageCredentialRecord
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &rec)
		}
		if err != nil {
			rows.Close()
			return err
		}
		if rec.Credential.Status != "revoked" {
			rows.Close()
			return ErrConflict
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM fugue_object_storage_credentials WHERE store_id=$1", id); err != nil {
		return mapDBErr(err)
	}
	result, err := tx.ExecContext(ctx, "DELETE FROM fugue_object_stores WHERE id=$1", id)
	if err != nil {
		return mapDBErr(err)
	}
	if n, err := result.RowsAffected(); err != nil {
		return mapDBErr(err)
	} else if n == 0 {
		return ErrNotFound
	}
	return mapDBErr(tx.Commit())
}
func (s *Store) ListObjectStorageCredentials(storeID string) ([]model.ObjectStorageCredentialRecord, error) {
	out := []model.ObjectStorageCredentialRecord{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, "SELECT body FROM fugue_object_storage_credentials WHERE store_id=$1 ORDER BY id", storeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var v model.ObjectStorageCredentialRecord
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) SaveObjectStorageCredential(v model.ObjectStorageCredentialRecord) error {
	c := v.Credential
	if c.ID == "" || c.StoreID == "" || c.Name == "" {
		return ErrInvalidInput
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = s.db.ExecContext(ctx, `INSERT INTO fugue_object_storage_credentials(id,store_id,name,body) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(id) DO UPDATE SET body=EXCLUDED.body WHERE fugue_object_storage_credentials.store_id=EXCLUDED.store_id AND fugue_object_storage_credentials.name=EXCLUDED.name`, c.ID, c.StoreID, c.Name, string(raw))
	return mapDBErr(err)
}
