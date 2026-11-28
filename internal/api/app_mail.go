package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"fugue/internal/appmail"
	"fugue/internal/httpx"
	"fugue/internal/model"
)

var mailLocalPart = regexp.MustCompile(`^[a-z0-9]+(?:[._+%-][a-z0-9]+)*$`)

func (s *Server) mailProvider(ctx context.Context, method, path string, input any) (json.RawMessage, error) {
	return appmail.NewProvider(s.store).Request(ctx, method, path, input)
}

func (s *Server) syncAppMail(ctx context.Context, binding model.AppMailBinding) (json.RawMessage, error) {
	var observation json.RawMessage
	var syncErr error
	executed, err := s.store.WithLegacyAppMailReconciliation(ctx, false, func() error {
		observation, syncErr = appmail.NewProvider(s.store).Sync(ctx, binding)
		return syncErr
	})
	if syncErr != nil {
		return nil, syncErr
	}
	if err != nil {
		return nil, errors.New("mail reconciliation unavailable")
	}
	if executed {
		return observation, nil
	}
	return appmail.AwaitReconciliation(ctx, binding, s.store.GetAppMailReconciliation, func(ctx context.Context) (json.RawMessage, error) {
		return s.mailProvider(ctx, http.MethodGet, url.PathEscape(binding.AppID), nil)
	})
}

func (s *Server) appMailAuthorized(w http.ResponseWriter, r *http.Request, write bool) (model.App, bool) {
	principal := mustPrincipal(r)
	scope := "app.read"
	if write {
		scope = "app.write"
	}
	if !principal.IsPlatformAdmin() && !principal.HasScope(scope) {
		httpx.WriteError(w, http.StatusForbidden, "missing "+scope+" scope")
		return model.App{}, false
	}
	return s.loadAuthorizedApp(w, r, principal)
}

func (s *Server) handleGetMailPlatformConfig(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, 403, "platform administrator required")
		return
	}
	cfg, err := s.store.GetMailPlatformConfig()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, 200, cfg)
}

func (s *Server) handlePutMailPlatformConfig(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() {
		httpx.WriteError(w, 403, "platform administrator required")
		return
	}
	var cfg model.MailPlatformConfig
	if !decodeMailInput(w, r, &cfg) {
		return
	}
	if err := appmail.ValidateDNSConfig(cfg); err != nil {
		s.writeStoreError(w, err)
		return
	}
	app, err := s.store.GetApp(cfg.MailflareAppID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if app.Route == nil || !strings.HasPrefix(app.Route.PublicURL, "https://") || len(app.Spec.Env["FUGUE_MAIL_SERVICE_TOKEN"]) < 32 || normalizeExternalAppDomain(app.Spec.Env["FUGUE_MAIL_APP_BASE_DOMAIN"]) != normalizeExternalAppDomain(s.appBaseDomain) {
		httpx.WriteError(w, 409, "mail provider needs a public HTTPS route, service token and matching app base domain")
		return
	}
	if err = s.store.SaveMailPlatformConfig(cfg); err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(p, "mail.configure", "app", app.ID, app.TenantID, nil)
	httpx.WriteJSON(w, 200, cfg)
}

func decodeMailInput(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 16*1024)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		httpx.WriteError(w, 400, "invalid mail request")
		return false
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		httpx.WriteError(w, 400, "invalid mail request")
		return false
	}
	return true
}

func (s *Server) handleGetAppMail(w http.ResponseWriter, r *http.Request) {
	app, ok := s.appMailAuthorized(w, r, false)
	if !ok {
		return
	}
	binding, err := s.store.GetAppMailBinding(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if binding.TenantID != app.TenantID || binding.ProjectID != app.ProjectID {
		httpx.WriteError(w, 409, "mail binding needs explicit migration after app transfer")
		return
	}
	// GET never mutates: background reconciliation applies saved intent.
	provider, err := s.mailProvider(r.Context(), http.MethodGet, url.PathEscape(app.ID), nil)
	if err == nil {
		err = appmail.ValidateObservation(provider, binding)
	}
	result := map[string]any{"binding": binding, "reconciled": err == nil}
	s.addMailServiceURL(result)
	if err != nil {
		result["error"] = err.Error()
	} else {
		result["provider"] = provider
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, 200, result)
}

func (s *Server) handlePutAppMail(w http.ResponseWriter, r *http.Request) {
	app, ok := s.appMailAuthorized(w, r, true)
	if !ok {
		return
	}
	var input struct {
		State     string  `json:"state"`
		Expected  *uint64 `json:"expected_generation"`
		LocalPart string  `json:"local_part"`
	}
	if !decodeMailInput(w, r, &input) {
		return
	}
	if input.Expected == nil {
		httpx.WriteError(w, 400, "expected_generation required")
		return
	}
	host, err := appmail.Hostname(app, s.appBaseDomain)
	if err != nil {
		httpx.WriteError(w, 400, "only the owned default application subdomain can enable mail")
		return
	}
	if input.LocalPart == "" {
		input.LocalPart = "support"
	}
	if len(input.LocalPart) > 64 || !mailLocalPart.MatchString(input.LocalPart) {
		httpx.WriteError(w, 400, "invalid mailbox local part")
		return
	}
	if input.State == "active" && (app.Status.Phase == "deleting" || app.Status.Phase == "deleted" || app.Status.Phase == "disabled") {
		httpx.WriteError(w, 409, "inactive application cannot enable mail")
		return
	}
	cfg, err := s.store.GetMailPlatformConfig()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if input.State == "active" && !cfg.Enabled {
		httpx.WriteError(w, 409, "mail service is disabled")
		return
	}
	binding, err := s.store.SaveAppMailBinding(model.AppMailBinding{AppID: app.ID, TenantID: app.TenantID, ProjectID: app.ProjectID, Hostname: host, LocalPart: input.LocalPart, State: input.State}, *input.Expected)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	provider, syncErr := s.syncAppMail(r.Context(), binding)
	result := map[string]any{"binding": binding, "reconciled": syncErr == nil}
	s.addMailServiceURL(result)
	if syncErr != nil {
		result["error"] = syncErr.Error()
	} else {
		result["provider"] = provider
	}
	s.appendAudit(mustPrincipal(r), "app.mail."+input.State, "app", app.ID, app.TenantID, nil)
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, 200, result)
}

// Expose only a validated public origin, never the provider's environment or
// path/query credentials. The configured route, not a hardcoded product URL,
// is authoritative for API examples and independent mailbox Web login.
func (s *Server) addMailServiceURL(result map[string]any) {
	cfg, err := s.store.GetMailPlatformConfig()
	if err != nil {
		return
	}
	app, err := s.store.GetApp(cfg.MailflareAppID)
	if err != nil || app.Route == nil {
		return
	}
	if origin := appMailServiceOrigin(*app.Route); origin != "" {
		result["service_url"] = origin
	}
}

func appMailServiceOrigin(route model.AppRoute) string {
	u, err := url.Parse(route.PublicURL)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Hostname() != route.Hostname || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return ""
	}
	return "https://" + u.Host
}

func (s *Server) handleCreateAppMailCredential(w http.ResponseWriter, r *http.Request) {
	app, ok := s.appMailAuthorized(w, r, true)
	if !ok {
		return
	}
	var input struct {
		Generation uint64 `json:"generation"`
		MailboxID  string `json:"mailbox_id"`
	}
	if !decodeMailInput(w, r, &input) {
		return
	}
	binding, err := s.store.GetAppMailBinding(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if binding.State != "active" || binding.Generation != input.Generation || binding.TenantID != app.TenantID || binding.ProjectID != app.ProjectID || len(input.MailboxID) < 1 || len(input.MailboxID) > 160 {
		httpx.WriteError(w, 409, "mail binding changed or inactive")
		return
	}
	if _, err = s.syncAppMail(r.Context(), binding); err != nil {
		httpx.WriteError(w, 502, err.Error())
		return
	}
	raw, err := s.mailProvider(r.Context(), http.MethodPost, url.PathEscape(app.ID)+"/credentials", map[string]any{"generation": input.Generation, "mailboxId": input.MailboxID})
	if err != nil {
		httpx.WriteError(w, 502, err.Error())
		return
	}
	s.appendAudit(mustPrincipal(r), "app.mail.credential.create", "app", app.ID, app.TenantID, nil)
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, 201, raw)
}

func (s *Server) handleSetAppMailboxLogin(w http.ResponseWriter, r *http.Request) {
	app, ok := s.appMailAuthorized(w, r, true)
	if !ok {
		return
	}
	var input struct {
		Generation uint64 `json:"generation"`
		MailboxID  string `json:"mailbox_id"`
		Enabled    *bool  `json:"enabled"`
	}
	if !decodeMailInput(w, r, &input) {
		return
	}
	if input.Enabled == nil || len(input.MailboxID) < 1 || len(input.MailboxID) > 160 || input.Generation == 0 || input.Generation > 9007199254740991 {
		httpx.WriteError(w, 400, "invalid mailbox login request")
		return
	}
	binding, err := s.store.GetAppMailBinding(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if binding.State != "active" || binding.Generation != input.Generation || binding.TenantID != app.TenantID || binding.ProjectID != app.ProjectID {
		httpx.WriteError(w, 409, "mail binding changed or inactive")
		return
	}
	if _, err = s.syncAppMail(r.Context(), binding); err != nil {
		httpx.WriteError(w, 502, err.Error())
		return
	}
	raw, err := s.mailProvider(r.Context(), http.MethodPost, url.PathEscape(app.ID)+"/login", map[string]any{"generation": input.Generation, "mailboxId": input.MailboxID, "enabled": *input.Enabled})
	if err != nil {
		httpx.WriteError(w, 502, err.Error())
		return
	}
	s.appendAudit(mustPrincipal(r), "app.mail.login.update", "app", app.ID, app.TenantID, nil)
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, 200, raw)
}

func (s *Server) handleRevokeAppMailCredential(w http.ResponseWriter, r *http.Request) {
	app, ok := s.appMailAuthorized(w, r, true)
	if !ok {
		return
	}
	var input struct {
		ID string `json:"id"`
	}
	if !decodeMailInput(w, r, &input) {
		return
	}
	if len(input.ID) < 1 || len(input.ID) > 160 {
		httpx.WriteError(w, 400, "invalid credential id")
		return
	}
	binding, err := s.store.GetAppMailBinding(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if binding.TenantID != app.TenantID || binding.ProjectID != app.ProjectID {
		httpx.WriteError(w, 409, "mail binding ownership changed")
		return
	}
	raw, err := s.mailProvider(r.Context(), http.MethodDelete, url.PathEscape(app.ID)+"/credentials", map[string]string{"id": input.ID})
	if err != nil {
		httpx.WriteError(w, 502, err.Error())
		return
	}
	s.appendAudit(mustPrincipal(r), "app.mail.credential.revoke", "app", app.ID, app.TenantID, nil)
	httpx.WriteJSON(w, 200, raw)
}

// One bounded pass per page. Invalid/deleted/transferred app ownership revokes
// rather than silently granting the new owner the previous mailbox contents.
func (s *Server) RunAppMailReconciler(ctx context.Context) {
	reconciler := appmail.Reconciler{
		Store: s.store, Provider: appmail.NewProvider(s.store), BaseDomain: s.appBaseDomain, Log: s.log,
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	after := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				pass, cancel := context.WithTimeout(ctx, 25*time.Second)
				defer cancel()
				_, passErr := s.store.WithLegacyAppMailReconciliation(pass, true, func() error {
					var err error
					after, err = reconciler.Pass(pass, after)
					return err
				})
				if passErr != nil {
					s.log.Printf("mail reconciliation pass failed: %v", passErr)
				}
			}()
		}
	}
}
