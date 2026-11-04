package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/objectstorage"
	"fugue/internal/store"
)

func archiveBackendStatus(v *model.CompilerInputArchiveBackend, executed bool) map[string]any {
	if v == nil {
		return map[string]any{"configured": false, "executed": executed, "state": "unconfigured"}
	}
	out := map[string]any{"configured": v.State == "active", "executed": executed, "state": v.State, "bucket": v.Bucket, "endpoint": "https://" + v.AccountID + ".r2.cloudflarestorage.com", "prefix": v.Prefix, "reader_configured": v.ReaderConfigured, "writer_configured": v.WriterConfigured, "retention_policy_sha256": v.RetentionPolicySHA256}
	if v.ProtectionVerifiedAt != nil {
		out["protection_verified_at"] = v.ProtectionVerifiedAt
	}
	return out
}

func (s *Server) handleGetCompilerInputArchiveBackend(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, 403, "platform administrator required")
		return
	}
	cfg, err := s.store.GetObjectStorageConfig()
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		s.writeStoreError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, 200, archiveBackendStatus(cfg.CompilerInputArchive, false))
}

func (s *Server) handleEnableCompilerInputArchiveBackend(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() {
		httpx.WriteError(w, 403, "platform administrator required")
		return
	}
	var req struct {
		Execute bool `json:"execute"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, 400, "invalid archive configuration request")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	s.objectStorageRun(w, r, func() error {
		cfg, err := s.store.GetObjectStorageConfig()
		if err != nil {
			return err
		}
		if !req.Execute {
			httpx.WriteJSON(w, 200, archiveBackendStatus(cfg.CompilerInputArchive, false))
			return nil
		}
		provider, err := s.objectStorageProvider(cfg)
		if err != nil {
			return err
		}
		v := cfg.CompilerInputArchive
		if v == nil {
			id := model.NewID("platform_archive")
			v = &model.CompilerInputArchiveBackend{ID: id, AccountID: cfg.AccountID, Bucket: "fugue-" + strings.ReplaceAll(id, "_", "-"), Prefix: "compiler-inputs", State: "provisioning"}
			// Verify encryption is configured before any remote mutation.
			if _, err = s.store.SealObjectStorageSecret(model.DataBackendCredentials{}); err != nil {
				return err
			}
			cfg.CompilerInputArchive = v
			if err = s.store.SaveObjectStorageConfig(cfg); err != nil {
				return err
			}
		}
		if v.AccountID != cfg.AccountID {
			return store.ErrConflict
		}
		if v.State == "active" {
			exists, err := provider.BucketExists(r.Context(), v.Bucket)
			if err != nil {
				return err
			}
			if !exists {
				return store.ErrConflict
			}
		} else if err = provider.EnsureBucket(r.Context(), v.Bucket); err != nil {
			return err
		}
		policy, err := provider.ProtectArchiveBucket(r.Context(), v.Bucket)
		if err != nil {
			return err
		}
		for _, grant := range []struct {
			name, permission string
			configured       *bool
			secret           *model.DataBackendSecret
		}{
			{"reader", "read-only", &v.ReaderConfigured, &v.Reader}, {"writer", "read-write", &v.WriterConfigured, &v.Writer},
		} {
			if *grant.configured {
				continue
			}
			token, err := provider.CreateCredential(r.Context(), v.ID+"-"+grant.name, v.Bucket, grant.permission)
			if err != nil {
				return err
			}
			sealed, err := s.store.SealObjectStorageSecret(model.DataBackendCredentials{AccessKeyID: token.ID, SecretAccessKey: objectstorage.SecretAccessKey(token.Value)})
			if err != nil {
				return err
			}
			*grant.secret = sealed
			*grant.configured = true
			if err = s.store.SaveObjectStorageConfig(cfg); err != nil {
				return err
			}
		}
		now := time.Now().UTC()
		v.ProtectionVerifiedAt = &now
		v.RetentionPolicySHA256 = policy
		v.State = "active"
		if err = s.store.SaveObjectStorageConfig(cfg); err != nil {
			return err
		}
		s.appendAudit(p, "compiler-input-archive.enable", "object_storage", v.ID, "", map[string]string{"bucket": v.Bucket, "retention_policy_sha256": policy})
		httpx.WriteJSON(w, 200, archiveBackendStatus(v, true))
		return nil
	})
}
