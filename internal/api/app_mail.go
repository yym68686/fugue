package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

var mailLocalPart = regexp.MustCompile(`^[a-z0-9]+(?:[._+%-][a-z0-9]+)*$`)
var mailAppLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

func appMailHostname(app model.App, base string) (string, error) {
	base = normalizeExternalAppDomain(base)
	if app.Route == nil || base == "" {
		return "", store.ErrInvalidInput
	}
	host := normalizeExternalAppDomain(app.Route.Hostname)
	label := strings.TrimSuffix(host, "."+base)
	if !strings.HasSuffix(host, "."+base) || !mailAppLabel.MatchString(label) {
		return "", store.ErrInvalidInput
	}
	return host, nil
}

func (s *Server) mailProvider(ctx context.Context, method, path string, input any) (json.RawMessage, error) {
	cfg, err := s.store.GetMailPlatformConfig()
	if err != nil {
		return nil, err
	}
	revocation := false
	if value, ok := input.(map[string]any); ok {
		revocation = value["state"] == "revoked"
	}
	if !cfg.Enabled && method != http.MethodGet && !(method == http.MethodPut && revocation) {
		return nil, errors.New("mail service disabled")
	}
	app, err := s.store.GetApp(cfg.MailflareAppID)
	if err != nil {
		return nil, err
	}
	if app.Route == nil || app.Route.PublicURL == "" {
		return nil, errors.New("mail service has no public endpoint")
	}
	base, err := url.Parse(app.Route.PublicURL)
	if err != nil || base.Scheme != "https" || base.User != nil || base.Hostname() != app.Route.Hostname || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("mail service endpoint is invalid")
	}
	token := app.Spec.Env["FUGUE_MAIL_SERVICE_TOKEN"]
	if len(token) < 32 {
		return nil, errors.New("mail service credentials unavailable")
	}
	var body []byte
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base.String(), "/")+"/api/internal/fugue/mail/"+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return nil, errors.New("mail provider unavailable")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 256*1024+1))
	if err != nil || len(raw) > 256*1024 {
		return nil, errors.New("mail provider response invalid")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("mail provider returned HTTP %d", response.StatusCode)
	}
	if !json.Valid(raw) {
		return nil, errors.New("mail provider response invalid")
	}
	return raw, nil
}

func (s *Server) syncAppMail(ctx context.Context, binding model.AppMailBinding) (json.RawMessage, error) {
	raw, err := s.mailProvider(ctx, http.MethodPut, url.PathEscape(binding.AppID), map[string]any{"tenantId": binding.TenantID, "projectId": binding.ProjectID, "appId": binding.AppID, "hostname": binding.Hostname, "localPart": binding.LocalPart, "generation": binding.Generation, "state": binding.State})
	if err != nil {
		return nil, err
	}
	return raw, validateMailObservation(raw, binding)
}

func validateMailObservation(raw json.RawMessage, binding model.AppMailBinding) error {
	var observation struct {
		AppID      string `json:"appId"`
		TenantID   string `json:"tenantId"`
		ProjectID  string `json:"projectId"`
		Hostname   string `json:"hostname"`
		Generation uint64 `json:"generation"`
		State      string `json:"state"`
	}
	if json.Unmarshal(raw, &observation) != nil || observation.AppID != binding.AppID || observation.TenantID != binding.TenantID || observation.ProjectID != binding.ProjectID || observation.Hostname != binding.Hostname || observation.Generation != binding.Generation || observation.State != binding.State {
		return errors.New("mail provider identity does not match desired state")
	}
	return nil
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
	if err := validateMailDNSPolicy(cfg.DNSRecords); err != nil {
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
		err = validateMailObservation(provider, binding)
	}
	result := map[string]any{"binding": binding, "reconciled": err == nil}
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
	host, err := appMailHostname(app, s.appBaseDomain)
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
	if syncErr != nil {
		result["error"] = syncErr.Error()
	} else {
		result["provider"] = provider
	}
	s.appendAudit(mustPrincipal(r), "app.mail."+input.State, "app", app.ID, app.TenantID, nil)
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, 200, result)
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
				_, passErr := s.store.WithAdvisoryLock(pass, "app-mail-reconcile", func() error {
					bindings, err := s.store.ListAppMailBindings(after, 20)
					if err != nil {
						return err
					}
					if len(bindings) == 0 {
						after = ""
						return nil
					}
					for _, b := range bindings {
						if pass.Err() != nil {
							return pass.Err()
						}
						// Advance past a failing binding; the next sweep retries it.
						after = b.AppID
						app, appErr := s.store.GetAppMetadata(b.AppID)
						if appErr != nil && !errors.Is(appErr, store.ErrNotFound) {
							s.log.Printf("mail reconciliation app=%s lookup failed: %v", b.AppID, appErr)
							continue
						}
						host, _ := appMailHostname(app, s.appBaseDomain)
						if b.State != "revoked" && (appErr != nil || app.TenantID != b.TenantID || app.ProjectID != b.ProjectID || host != b.Hostname || app.Status.Phase == "deleted" || app.Status.Phase == "deleting" || app.Status.Phase == "disabled") {
							b.State = "revoked"
							b, err = s.store.SaveAppMailBinding(b, b.Generation)
							if err != nil {
								s.log.Printf("mail reconciliation app=%s revoke failed: %v", b.AppID, err)
								continue
							}
						}
						if _, err = s.syncAppMail(pass, b); err != nil {
							s.log.Printf("mail reconciliation app=%s sync failed: %v", b.AppID, err)
						}
					}
					if len(bindings) < 20 {
						after = ""
					}
					return nil
				})
				if passErr != nil {
					s.log.Printf("mail reconciliation pass failed: %v", passErr)
				}
			}()
		}
	}
}
