package api

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/objectstorage"
	"fugue/internal/store"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var objectStoreName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)
var objectStoreAccount = regexp.MustCompile(`^[a-f0-9]{32}$`)

const objectStorageLock = "object-storage-management"

const defaultObjectStorageUsageTimeout = 2 * time.Minute

func normalizeObjectStorageUsageTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return defaultObjectStorageUsageTimeout
	}
	return timeout
}

func (s *Server) objectStorageProvider(cfg model.ObjectStorageConfig) (*objectstorage.Client, error) {
	secret, err := s.store.OpenObjectStorageSecret(cfg.Secret)
	if err != nil {
		return nil, err
	}
	if s.newObjectStorageClient != nil {
		return s.newObjectStorageClient(cfg.AccountID, secret.Token), nil
	}
	return objectstorage.New(cfg.AccountID, secret.Token), nil
}
func (s *Server) objectStorageRun(w http.ResponseWriter, r *http.Request, fn func() error) {
	acquired, err := s.store.WithAdvisoryLock(r.Context(), objectStorageLock, fn)
	if err != nil {
		var api *objectstorage.APIError
		if errors.As(err, &api) {
			httpx.WriteError(w, http.StatusBadGateway, api.Error())
			return
		}
		s.writeStoreError(w, err)
		return
	}
	if !acquired {
		httpx.WriteError(w, http.StatusConflict, "object storage operation in progress; retry")
	}
}
func objectStorageScope(w http.ResponseWriter, r *http.Request, write bool) bool {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() && p.TenantID == "" {
		httpx.WriteError(w, 403, "tenant identity required")
		return false
	}
	if p.HasScope("storage.admin") || p.HasScope("data.admin") || (!write && p.HasScope("storage.read")) {
		return true
	}
	httpx.WriteError(w, http.StatusForbidden, "missing object storage scope")
	return false
}
func (s *Server) authorizedObjectStore(w http.ResponseWriter, r *http.Request, write bool) (model.ObjectStore, bool) {
	if !objectStorageScope(w, r, write) {
		return model.ObjectStore{}, false
	}
	p := mustPrincipal(r)
	v, err := s.store.GetObjectStore(r.PathValue("store_id"))
	if err != nil || (!p.IsPlatformAdmin() && (p.TenantID != v.TenantID || !p.AllowsProject(v.ProjectID))) {
		httpx.WriteError(w, http.StatusNotFound, "object store not found")
		return v, false
	}
	return v, true
}
func objectStorageConfigStatus(cfg model.ObjectStorageConfig) map[string]any {
	return map[string]any{"configured": cfg.AccountID != "", "account_id": cfg.AccountID, "updated_at": cfg.UpdatedAt}
}
func (s *Server) handleGetObjectStorageConfig(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, 403, "platform administrator required")
		return
	}
	cfg, err := s.store.GetObjectStorageConfig()
	if errors.Is(err, store.ErrNotFound) {
		httpx.WriteJSON(w, 200, map[string]any{"configured": false})
		return
	}
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, 200, objectStorageConfigStatus(cfg))
}
func (s *Server) handleConfigureObjectStorage(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() {
		httpx.WriteError(w, 403, "platform administrator required")
		return
	}
	var req struct {
		AccountID string `json:"account_id"`
		APIToken  string `json:"api_token"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, 400, "invalid configuration")
		return
	}
	if !objectStoreAccount.MatchString(req.AccountID) || len(req.APIToken) < 20 {
		httpx.WriteError(w, 400, "valid account_id and api_token required")
		return
	}
	s.objectStorageRun(w, r, func() error {
		old, err := s.store.GetObjectStorageConfig()
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		if old.AccountID != "" && old.AccountID != req.AccountID {
			items, err := s.store.ListObjectStores("", "")
			if err != nil {
				return err
			}
			if len(items) > 0 {
				return store.ErrConflict
			}
		}
		secret, err := s.store.SealObjectStorageSecret(model.DataBackendCredentials{Token: req.APIToken})
		if err != nil {
			return err
		}
		cfg := model.ObjectStorageConfig{AccountID: req.AccountID, Secret: secret, UpdatedAt: time.Now().UTC()}
		provider, err := s.objectStorageProvider(cfg)
		if err != nil {
			return err
		}
		if err = provider.Verify(r.Context()); err != nil {
			return err
		}
		id, err := provider.Identity(r.Context())
		if err != nil {
			return err
		}
		cfg.Secret, err = s.store.SealObjectStorageSecret(model.DataBackendCredentials{Token: req.APIToken, AccessKeyID: id, SecretAccessKey: objectstorage.SecretAccessKey(req.APIToken)})
		if err != nil {
			return err
		}
		if err = s.store.SaveObjectStorageConfig(cfg); err != nil {
			return err
		}
		s.appendAudit(p, "object-storage.configure", "object_storage", "platform", "", nil)
		httpx.WriteJSON(w, 200, objectStorageConfigStatus(cfg))
		return nil
	})
}
func (s *Server) handleListObjectStores(w http.ResponseWriter, r *http.Request) {
	if !objectStorageScope(w, r, false) {
		return
	}
	p := mustPrincipal(r)
	tenant, ok := s.resolveTenantID(p, r.URL.Query().Get("tenant_id"))
	if !ok {
		httpx.WriteError(w, 403, "tenant access denied")
		return
	}
	project := projectIDForPrincipal(p, r.URL.Query().Get("project_id"))
	if !p.AllowsProject(project) {
		httpx.WriteError(w, 403, "project access denied")
		return
	}
	items, err := s.store.ListObjectStores(tenant, project)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, 200, map[string]any{"stores": items})
}
func (s *Server) handleGetObjectStore(w http.ResponseWriter, r *http.Request) {
	v, ok := s.authorizedObjectStore(w, r, false)
	if ok {
		httpx.WriteJSON(w, 200, map[string]any{"store": v})
	}
}
func (s *Server) handleCreateObjectStore(w http.ResponseWriter, r *http.Request) {
	if !objectStorageScope(w, r, true) {
		return
	}
	p := mustPrincipal(r)
	var req struct {
		TenantID   string `json:"tenant_id"`
		ProjectID  string `json:"project_id"`
		Name       string `json:"name"`
		QuotaBytes int64  `json:"quota_bytes"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil || !objectStoreName.MatchString(req.Name) || req.QuotaBytes < 0 || req.ProjectID == "" {
		httpx.WriteError(w, 400, "valid name, project_id and nonnegative quota required")
		return
	}
	project, err := s.store.GetProject(req.ProjectID)
	if err != nil || !principalAllowsProject(p, project) || (req.TenantID != "" && req.TenantID != project.TenantID) {
		httpx.WriteError(w, 404, "project not found")
		return
	}
	s.objectStorageRun(w, r, func() error {
		cfg, err := s.store.GetObjectStorageConfig()
		if err != nil {
			return err
		}
		provider, err := s.objectStorageProvider(cfg)
		if err != nil {
			return err
		}
		items, err := s.store.ListObjectStores(project.TenantID, project.ID)
		if err != nil {
			return err
		}
		var v model.ObjectStore
		for _, x := range items {
			if x.Name == req.Name {
				v = x
				break
			}
		}
		if v.ID == "" {
			id := model.NewID("object_store")
			now := time.Now().UTC()
			v = model.ObjectStore{ID: id, TenantID: project.TenantID, ProjectID: project.ID, Name: req.Name, Provider: "cloudflare-r2", Bucket: "fugue-objects-" + strings.ReplaceAll(id, "_", "-"), Endpoint: "https://" + cfg.AccountID + ".r2.cloudflarestorage.com", Region: "auto", Status: "provisioning", QuotaBytes: req.QuotaBytes, QuotaMode: "soft", CreatedAt: now, UpdatedAt: now}
			if err = s.store.SaveObjectStore(v); err != nil {
				return err
			}
		}
		if v.Status == "provisioning" {
			if err = provider.EnsureBucket(r.Context(), v.Bucket); err != nil {
				return err
			}
			v.Status = "active"
			v.UpdatedAt = time.Now().UTC()
			if err = s.store.SaveObjectStore(v); err != nil {
				return err
			}
		}
		s.appendAudit(p, "object-store.create", "object_store", v.ID, v.TenantID, map[string]string{"project_id": v.ProjectID})
		httpx.WriteJSON(w, 200, map[string]any{"store": v})
		return nil
	})
}
func (s *Server) handleUpdateObjectStore(w http.ResponseWriter, r *http.Request) {
	v, ok := s.authorizedObjectStore(w, r, true)
	if !ok {
		return
	}
	var req struct {
		Enabled    *bool  `json:"enabled"`
		QuotaBytes *int64 `json:"quota_bytes"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil || (req.QuotaBytes != nil && *req.QuotaBytes < 0) {
		httpx.WriteError(w, 400, "invalid object store update")
		return
	}
	s.objectStorageRun(w, r, func() error {
		var err error
		v, err = s.store.GetObjectStore(v.ID)
		if err != nil {
			return err
		}
		if req.Enabled != nil {
			if *req.Enabled {
				if v.Status != "active" && v.Status != "disabled" {
					return store.ErrConflict
				}
				v.Status = "active"
			} else {
				v.Status = "disabling"
				if err = s.store.SaveObjectStore(v); err != nil {
					return err
				}
				cfg, err := s.store.GetObjectStorageConfig()
				if err != nil {
					return err
				}
				provider, err := s.objectStorageProvider(cfg)
				if err != nil {
					return err
				}
				creds, err := s.store.ListObjectStorageCredentials(v.ID)
				if err != nil {
					return err
				}
				for _, c := range creds {
					if err = s.revokeObjectStorageCredential(r.Context(), provider, v, c); err != nil {
						return err
					}
				}
				v.Status = "disabled"
			}
		}
		if req.QuotaBytes != nil {
			v.QuotaBytes = *req.QuotaBytes
		}
		v.UpdatedAt = time.Now().UTC()
		if err = s.store.SaveObjectStore(v); err != nil {
			return err
		}
		s.appendAudit(mustPrincipal(r), "object-store.update", "object_store", v.ID, v.TenantID, map[string]string{"status": v.Status})
		httpx.WriteJSON(w, 200, map[string]any{"store": v})
		return nil
	})
}
func (s *Server) handleListObjectStoreCredentials(w http.ResponseWriter, r *http.Request) {
	v, ok := s.authorizedObjectStore(w, r, false)
	if !ok {
		return
	}
	records, err := s.store.ListObjectStorageCredentials(v.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	out := []model.ObjectStorageCredential{}
	for _, rec := range records {
		out = append(out, rec.Credential)
	}
	httpx.WriteJSON(w, 200, map[string]any{"credentials": out})
}
func (s *Server) handleCreateObjectStoreCredential(w http.ResponseWriter, r *http.Request) {
	v, ok := s.authorizedObjectStore(w, r, true)
	if !ok {
		return
	}
	p := mustPrincipal(r)
	var req struct {
		AppID      string `json:"app_id"`
		Name       string `json:"name"`
		Permission string `json:"permission"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil || !objectStoreName.MatchString(req.Name) || (req.Permission != "read-only" && req.Permission != "read-write") {
		httpx.WriteError(w, 400, "valid app_id, name and permission required")
		return
	}
	app, err := s.store.GetApp(req.AppID)
	if err != nil || app.TenantID != v.TenantID || !principalAllowsApp(p, app) {
		httpx.WriteError(w, 404, "application not found")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	s.objectStorageRun(w, r, func() error {
		var err error
		v, err = s.store.GetObjectStore(v.ID)
		if err != nil {
			return err
		}
		if v.Status != "active" {
			return store.ErrConflict
		}
		cfg, err := s.store.GetObjectStorageConfig()
		if err != nil {
			return err
		}
		provider, err := s.objectStorageProvider(cfg)
		if err != nil {
			return err
		}
		records, err := s.store.ListObjectStorageCredentials(v.ID)
		if err != nil {
			return err
		}
		var rec model.ObjectStorageCredentialRecord
		for _, x := range records {
			if x.Credential.Name == req.Name {
				rec = x
				break
			}
		}
		if rec.Credential.ID != "" && (rec.Credential.AppID != req.AppID || rec.Credential.Permission != req.Permission || rec.Credential.Status == "revoked" || rec.Credential.Status == "revoking") {
			return store.ErrConflict
		}
		if rec.Credential.ID == "" {
			rec.Credential = model.ObjectStorageCredential{ID: model.NewID("s3_credential"), StoreID: v.ID, AppID: app.ID, Name: req.Name, Permission: req.Permission, Status: "provisioning", CreatedAt: time.Now().UTC()}
			if err = s.store.SaveObjectStorageCredential(rec); err != nil {
				return err
			}
		}
		var secret model.DataBackendCredentials
		if rec.Credential.Status == "provisioning" {
			token, err := provider.CreateCredential(r.Context(), rec.Credential.ID, v.Bucket, req.Permission)
			if err != nil {
				return err
			}
			secret = model.DataBackendCredentials{AccessKeyID: token.ID, SecretAccessKey: objectstorage.SecretAccessKey(token.Value)}
			rec.Secret, err = s.store.SealObjectStorageSecret(secret)
			if err != nil {
				return err
			}
			rec.Credential.AccessKeyID = token.ID
			rec.Credential.Status = "active"
			if err = s.store.SaveObjectStorageCredential(rec); err != nil {
				return err
			}
		} else {
			secret, err = s.store.OpenObjectStorageSecret(rec.Secret)
			if err != nil {
				return err
			}
		}
		s.appendAudit(p, "object-store.credential.issue", "object_store", v.ID, v.TenantID, map[string]string{"credential_id": rec.Credential.ID, "app_id": app.ID, "permission": req.Permission})
		httpx.WriteJSON(w, 200, map[string]any{"credential": rec.Credential, "endpoint": v.Endpoint, "bucket": v.Bucket, "region": v.Region, "access_key_id": rec.Credential.AccessKeyID, "secret_access_key": secret.SecretAccessKey})
		return nil
	})
}
func (s *Server) revokeObjectStorageCredential(ctx context.Context, provider *objectstorage.Client, v model.ObjectStore, rec model.ObjectStorageCredentialRecord) error {
	if rec.Credential.Status == "revoked" {
		return nil
	}
	rec.Credential.Status = "revoking"
	if err := s.store.SaveObjectStorageCredential(rec); err != nil {
		return err
	}
	if rec.Credential.AccessKeyID == "" {
		if err := provider.RevokeNamed(ctx, rec.Credential.ID); err != nil {
			return err
		}
	} else if err := provider.Revoke(ctx, rec.Credential.AccessKeyID); err != nil {
		return err
	}
	rec.Credential.Status = "revoked"
	rec.Secret = model.DataBackendSecret{}
	return s.store.SaveObjectStorageCredential(rec)
}
func (s *Server) handleRevokeObjectStoreCredential(w http.ResponseWriter, r *http.Request) {
	v, ok := s.authorizedObjectStore(w, r, true)
	if !ok {
		return
	}
	s.objectStorageRun(w, r, func() error {
		records, err := s.store.ListObjectStorageCredentials(v.ID)
		if err != nil {
			return err
		}
		for _, rec := range records {
			if rec.Credential.ID == r.PathValue("credential_id") {
				cfg, err := s.store.GetObjectStorageConfig()
				if err != nil {
					return err
				}
				provider, err := s.objectStorageProvider(cfg)
				if err != nil {
					return err
				}
				if err = s.revokeObjectStorageCredential(r.Context(), provider, v, rec); err != nil {
					return err
				}
				rec.Credential.Status = "revoked"
				s.appendAudit(mustPrincipal(r), "object-store.credential.revoke", "object_store", v.ID, v.TenantID, map[string]string{"credential_id": rec.Credential.ID})
				httpx.WriteJSON(w, 200, map[string]any{"credential": rec.Credential})
				return nil
			}
		}
		return store.ErrNotFound
	})
}
func (s *Server) handleMeasureObjectStoreUsage(w http.ResponseWriter, r *http.Request) {
	v, ok := s.authorizedObjectStore(w, r, false)
	if !ok {
		return
	}
	s.objectStorageRun(w, r, func() error {
		var err error
		v, err = s.store.GetObjectStore(v.ID)
		if err != nil {
			return err
		}
		cfg, err := s.store.GetObjectStorageConfig()
		if err != nil {
			return err
		}
		secret, err := s.store.OpenObjectStorageSecret(cfg.Secret)
		if err != nil {
			return err
		}
		secret.Token = ""
		b, err := newDataObjectBackend(model.DataBackend{Name: v.Name, Provider: v.Provider, Bucket: v.Bucket, Endpoint: v.Endpoint, Region: v.Region, Credentials: secret})
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(r.Context(), normalizeObjectStorageUsageTimeout(s.objectStorageUsageTimeout))
		defer cancel()
		pager := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{Bucket: aws.String(v.Bucket)})
		var used, count int64
		for pager.HasMorePages() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return errors.New("object storage usage measurement failed; previous measurement retained")
			}
			for _, obj := range page.Contents {
				used += aws.ToInt64(obj.Size)
				count++
			}
		}
		now := time.Now().UTC()
		v.UsedBytes = used
		v.ObjectCount = count
		v.UsageMeasuredAt = &now
		if err = s.store.SaveObjectStore(v); err != nil {
			return err
		}
		httpx.WriteJSON(w, 200, map[string]any{"store": v})
		return nil
	})
}
