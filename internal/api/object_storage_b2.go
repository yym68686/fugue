package api

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/objectstorage"
	"fugue/internal/store"
)

type objectStoreManager interface {
	BucketExists(context.Context, string) (bool, error)
	DeleteBucket(context.Context, string) error
	CreateCredential(context.Context, string, string, string) (objectstorage.Token, error)
	Revoke(context.Context, string) error
	RevokeNamed(context.Context, string) error
}

func (s *Server) objectStoreManager(ctx context.Context, v model.ObjectStore) (objectStoreManager, error) {
	switch v.Provider {
	case model.DataBackendProviderCloudflareR2:
		cfg, err := s.store.GetObjectStorageConfig()
		if err != nil {
			return nil, err
		}
		return s.objectStorageProvider(cfg)
	case model.DataBackendProviderBackblazeB2:
		b2, err := s.store.GetBackblazeB2Config()
		if err != nil {
			return nil, err
		}
		return s.b2ObjectStoreManager(ctx, b2, v)
	default:
		return nil, store.ErrInvalidInput
	}
}

func (s *Server) b2ObjectStoreManager(ctx context.Context, cfg *model.ObjectStorageProviderConfig, v model.ObjectStore) (*objectstorage.B2Client, error) {
	if cfg == nil || v.Region != cfg.Region || v.Endpoint != cfg.Endpoint {
		return nil, store.ErrConflict
	}
	secret, err := s.store.OpenObjectStorageSecret(cfg.Secret)
	if err != nil {
		return nil, err
	}
	client, err := objectstorage.AuthorizeB2(ctx, secret.AccessKeyID, secret.SecretAccessKey, s.objectStorageB2Transport)
	if err != nil {
		return nil, err
	}
	if client.AccountID != cfg.AccountID || client.Endpoint != cfg.Endpoint || client.Region != cfg.Region {
		return nil, store.ErrConflict
	}
	client.BucketID = v.BucketID
	return client, nil
}

func objectStoreS3Secret(provider, value string) string {
	if provider == model.DataBackendProviderBackblazeB2 {
		return value
	}
	return objectstorage.SecretAccessKey(value)
}

func b2ConfigStatus(cfg *model.ObjectStorageProviderConfig) map[string]any {
	if cfg == nil {
		return map[string]any{"configured": false}
	}
	return map[string]any{"configured": true, "account_id": cfg.AccountID, "region": cfg.Region, "updated_at": cfg.UpdatedAt}
}

func (s *Server) handleGetBackblazeB2Config(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, 403, "platform administrator required")
		return
	}
	cfg, err := s.store.GetBackblazeB2Config()
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, 200, b2ConfigStatus(cfg))
}

var b2ConfigKeyID = regexp.MustCompile(`^[a-zA-Z0-9]{12,128}$`)
var b2ConfigKey = regexp.MustCompile(`^[a-zA-Z0-9+/_=-]{20,256}$`)

func (s *Server) handleConfigureBackblazeB2(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() {
		httpx.WriteError(w, 403, "platform administrator required")
		return
	}
	var req struct {
		KeyID          string `json:"key_id"`
		ApplicationKey string `json:"application_key"`
	}
	if httpx.DecodeJSON(r, &req) != nil || !b2ConfigKeyID.MatchString(req.KeyID) || !b2ConfigKey.MatchString(req.ApplicationKey) {
		httpx.WriteError(w, 400, "valid key_id and application_key required")
		return
	}
	s.objectStorageRun(w, r, func() error {
		old, err := s.store.GetBackblazeB2Config()
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		client, err := objectstorage.AuthorizeB2(r.Context(), req.KeyID, req.ApplicationKey, s.objectStorageB2Transport)
		if err != nil {
			return err
		}
		if old != nil && (old.AccountID != client.AccountID || old.Endpoint != client.Endpoint || old.Region != client.Region) {
			items, err := s.store.ListObjectStores("", "")
			if err != nil {
				return err
			}
			for _, v := range items {
				if v.Provider == model.DataBackendProviderBackblazeB2 {
					return store.ErrConflict
				}
			}
		}
		secret, err := s.store.SealObjectStorageSecret(model.DataBackendCredentials{AccessKeyID: req.KeyID, SecretAccessKey: req.ApplicationKey})
		if err != nil {
			return err
		}
		cfg := model.ObjectStorageProviderConfig{AccountID: client.AccountID, Endpoint: client.Endpoint, Region: client.Region, Secret: secret, UpdatedAt: time.Now().UTC()}
		if err := s.store.SaveBackblazeB2Config(cfg); err != nil {
			return err
		}
		s.appendAudit(p, "object-storage.configure", "object_storage", model.DataBackendProviderBackblazeB2, "", nil)
		httpx.WriteJSON(w, 200, b2ConfigStatus(&cfg))
		return nil
	})
}
