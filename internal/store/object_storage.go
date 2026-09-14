package store

import (
	"context"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	"time"
)

func (s *Store) GetObjectStorageConfig() (model.ObjectStorageConfig, error) {
	var v model.ObjectStorageConfig
	if s.usingDatabase() {
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
	err := s.withLockedState(false, func(st *model.State) error {
		if st.ObjectStorage.Config == nil {
			return ErrNotFound
		}
		v = *st.ObjectStorage.Config
		return nil
	})
	return v, err
}
func (s *Store) SaveObjectStorageConfig(v model.ObjectStorageConfig) error {
	if s.usingDatabase() {
		raw, err := json.Marshal(v)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = s.db.ExecContext(ctx, "INSERT INTO fugue_object_storage_config(id,body) VALUES('platform',$1::jsonb) ON CONFLICT(id) DO UPDATE SET body=EXCLUDED.body", string(raw))
		return mapDBErr(err)
	}
	return s.withLockedState(true, func(st *model.State) error { st.ObjectStorage.Config = &v; return nil })
}
func (s *Store) SealObjectStorageSecret(v model.DataBackendCredentials) (model.DataBackendSecret, error) {
	if _, keyID := dataBackendSecretKey(); s.usingDatabase() && keyID == "development-fallback" {
		return model.DataBackendSecret{}, fmt.Errorf("configure production credential encryption before enabling object storage")
	}
	return encryptDataBackendSecret(model.DataBackendSecret{}, v)
}
func (s *Store) OpenObjectStorageSecret(v model.DataBackendSecret) (model.DataBackendCredentials, error) {
	return decryptDataBackendSecret(v)
}
func (s *Store) ListObjectStores(tenantID, projectID string) ([]model.ObjectStore, error) {
	out := []model.ObjectStore{}
	if s.usingDatabase() {
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
	err := s.withLockedState(false, func(st *model.State) error {
		for _, v := range st.ObjectStorage.Stores {
			if (tenantID == "" || v.TenantID == tenantID) && (projectID == "" || v.ProjectID == projectID) {
				out = append(out, v)
			}
		}
		return nil
	})
	return out, err
}
func (s *Store) GetObjectStore(id string) (model.ObjectStore, error) {
	var out model.ObjectStore
	if s.usingDatabase() {
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
	err := s.withLockedState(false, func(st *model.State) error {
		for _, v := range st.ObjectStorage.Stores {
			if v.ID == id {
				out = v
				return nil
			}
		}
		return ErrNotFound
	})
	return out, err
}
func (s *Store) SaveObjectStore(v model.ObjectStore) error {
	if v.ID == "" || v.TenantID == "" || v.ProjectID == "" || v.Name == "" {
		return ErrInvalidInput
	}
	v.UpdatedAt = time.Now().UTC()
	if s.usingDatabase() {
		raw, err := json.Marshal(v)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = s.db.ExecContext(ctx, `INSERT INTO fugue_object_stores(id,tenant_id,project_id,name,body) VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT(id) DO UPDATE SET body=EXCLUDED.body WHERE fugue_object_stores.tenant_id=EXCLUDED.tenant_id AND fugue_object_stores.project_id=EXCLUDED.project_id AND fugue_object_stores.name=EXCLUDED.name`, v.ID, v.TenantID, v.ProjectID, v.Name, string(raw))
		return mapDBErr(err)
	}
	return s.withLockedState(true, func(st *model.State) error {
		for i, x := range st.ObjectStorage.Stores {
			if x.ID == v.ID {
				if x.TenantID != v.TenantID || x.ProjectID != v.ProjectID || x.Name != v.Name {
					return ErrConflict
				}
				st.ObjectStorage.Stores[i] = v
				return nil
			}
			if x.TenantID == v.TenantID && x.ProjectID == v.ProjectID && x.Name == v.Name {
				return ErrConflict
			}
		}
		st.ObjectStorage.Stores = append(st.ObjectStorage.Stores, v)
		return nil
	})
}
func (s *Store) ListObjectStorageCredentials(storeID string) ([]model.ObjectStorageCredentialRecord, error) {
	out := []model.ObjectStorageCredentialRecord{}
	if s.usingDatabase() {
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
	err := s.withLockedState(false, func(st *model.State) error {
		for _, v := range st.ObjectStorage.Credentials {
			if v.Credential.StoreID == storeID {
				out = append(out, v)
			}
		}
		return nil
	})
	return out, err
}
func (s *Store) SaveObjectStorageCredential(v model.ObjectStorageCredentialRecord) error {
	c := v.Credential
	if c.ID == "" || c.StoreID == "" || c.Name == "" {
		return ErrInvalidInput
	}
	if s.usingDatabase() {
		raw, err := json.Marshal(v)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = s.db.ExecContext(ctx, `INSERT INTO fugue_object_storage_credentials(id,store_id,name,body) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(id) DO UPDATE SET body=EXCLUDED.body WHERE fugue_object_storage_credentials.store_id=EXCLUDED.store_id AND fugue_object_storage_credentials.name=EXCLUDED.name`, c.ID, c.StoreID, c.Name, string(raw))
		return mapDBErr(err)
	}
	return s.withLockedState(true, func(st *model.State) error {
		for i, x := range st.ObjectStorage.Credentials {
			if x.Credential.ID == c.ID {
				if x.Credential.StoreID != c.StoreID || x.Credential.Name != c.Name {
					return ErrConflict
				}
				st.ObjectStorage.Credentials[i] = v
				return nil
			}
			if x.Credential.StoreID == c.StoreID && x.Credential.Name == c.Name {
				return ErrConflict
			}
		}
		st.ObjectStorage.Credentials = append(st.ObjectStorage.Credentials, v)
		return nil
	})
}
