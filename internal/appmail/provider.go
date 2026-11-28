// Package appmail implements the application mail provider protocol without
// depending on the HTTP API server or the application controller.
package appmail

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

	"fugue/internal/model"
	"fugue/internal/store"
)

type ProviderStore interface {
	GetMailPlatformConfig() (model.MailPlatformConfig, error)
	GetApp(string) (model.App, error)
	SaveAppMailProviderDNS(model.AppMailBinding, []model.AppMailDNSRecord) error
}

type Provider struct {
	store     ProviderStore
	transport http.RoundTripper
}

func NewProvider(st ProviderStore) Provider { return Provider{store: st} }

var appLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

func Hostname(app model.App, base string) (string, error) {
	base = normalizeDomain(base)
	if app.Route == nil || base == "" {
		return "", store.ErrInvalidInput
	}
	host := normalizeDomain(app.Route.Hostname)
	label := strings.TrimSuffix(host, "."+base)
	if !strings.HasSuffix(host, "."+base) || !appLabel.MatchString(label) {
		return "", store.ErrInvalidInput
	}
	return host, nil
}

func normalizeDomain(value string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(value)), ".")
}

// CheckReconciliationConfig performs no provider request. In particular an
// inconsistent suffix must never be mistaken for a change of app ownership
// and used to revoke existing mail.
func (p Provider) CheckReconciliationConfig(baseDomain string) error {
	cfg, err := p.store.GetMailPlatformConfig()
	if err != nil {
		return err
	}
	app, err := p.store.GetApp(cfg.MailflareAppID)
	if err != nil {
		return err
	}
	if base := normalizeDomain(baseDomain); base == "" || normalizeDomain(app.Spec.Env["FUGUE_MAIL_APP_BASE_DOMAIN"]) != base {
		return errors.New("mail service application base domain does not match the controller")
	}
	_, _, err = providerEndpoint(app)
	return err
}

func providerEndpoint(app model.App) (*url.URL, string, error) {
	if app.Route == nil || app.Route.PublicURL == "" {
		return nil, "", errors.New("mail service has no public endpoint")
	}
	base, err := url.Parse(app.Route.PublicURL)
	if err != nil || base.Scheme != "https" || base.User != nil || base.Hostname() != app.Route.Hostname || base.RawQuery != "" || base.Fragment != "" {
		return nil, "", errors.New("mail service endpoint is invalid")
	}
	token := app.Spec.Env["FUGUE_MAIL_SERVICE_TOKEN"]
	if len(token) < 32 {
		return nil, "", errors.New("mail service credentials unavailable")
	}
	return base, token, nil
}

func (p Provider) Request(ctx context.Context, method, path string, input any) (json.RawMessage, error) {
	cfg, err := p.store.GetMailPlatformConfig()
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
	app, err := p.store.GetApp(cfg.MailflareAppID)
	if err != nil {
		return nil, err
	}
	base, token, err := providerEndpoint(app)
	if err != nil {
		return nil, err
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
	client := &http.Client{Transport: p.transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
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

// ObserveBinding is a read-only end-to-end check. Callers must not persist its
// response as desired state or treat it as proof that mail was delivered.
func (p Provider) ObserveBinding(ctx context.Context, binding model.AppMailBinding) (json.RawMessage, error) {
	raw, err := p.Request(ctx, http.MethodGet, url.PathEscape(binding.AppID), nil)
	if err != nil {
		return nil, err
	}
	if err = ValidateObservation(raw, binding); err != nil {
		return nil, err
	}
	return raw, nil
}

func (p Provider) Sync(ctx context.Context, binding model.AppMailBinding) (json.RawMessage, error) {
	return p.syncWithDNSWriter(ctx, binding, func(records []model.AppMailDNSRecord) error {
		return p.store.SaveAppMailProviderDNS(binding, records)
	})
}

func (p Provider) syncWithDNSWriter(ctx context.Context, binding model.AppMailBinding, save func([]model.AppMailDNSRecord) error) (json.RawMessage, error) {
	raw, err := p.Request(ctx, http.MethodPut, url.PathEscape(binding.AppID), map[string]any{"tenantId": binding.TenantID, "projectId": binding.ProjectID, "appId": binding.AppID, "hostname": binding.Hostname, "localPart": binding.LocalPart, "generation": binding.Generation, "state": binding.State})
	if err != nil {
		return nil, err
	}
	if err = ValidateObservation(raw, binding); err != nil {
		return nil, err
	}
	var observation struct {
		DNSRecords *[]model.AppMailDNSRecord `json:"dnsRecords"`
	}
	if err = json.Unmarshal(raw, &observation); err != nil {
		return nil, err
	}
	if observation.DNSRecords != nil && binding.State == "active" {
		if err = ValidateDNSPolicy(*observation.DNSRecords); err != nil {
			return nil, errors.New("mail provider DNS policy invalid")
		}
		cfg, configErr := p.store.GetMailPlatformConfig()
		if configErr != nil {
			return nil, configErr
		}
		if _, err = DNSRules(cfg, binding, *observation.DNSRecords); err != nil {
			return nil, err
		}
		if err = save(*observation.DNSRecords); err != nil {
			return nil, err
		}
	}
	return raw, nil
}

func ValidateObservation(raw json.RawMessage, binding model.AppMailBinding) error {
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
