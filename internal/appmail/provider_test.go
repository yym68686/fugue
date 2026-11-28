package appmail

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/model"
)

type providerConfigFixture struct {
	app model.App
}

type providerReadTransport func(*http.Request) (*http.Response, error)

func (f providerReadTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestObserveBindingUsesOnlyGetAndRejectsStaleResult(t *testing.T) {
	binding := model.AppMailBinding{
		AppID: "sample", TenantID: "tenant", ProjectID: "project",
		Hostname: "sample.apps.example.test", Generation: 2, State: "active",
	}
	token := strings.Repeat("t", 32)
	p := NewProvider(providerConfigFixture{model.App{
		Route: &model.AppRoute{Hostname: "mail.example.test", PublicURL: "https://mail.example.test"},
		Spec:  model.AppSpec{Env: map[string]string{"FUGUE_MAIL_SERVICE_TOKEN": token}},
	}})
	for _, generation := range []uint64{2, 1} {
		p.transport = providerReadTransport(func(r *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(r.Body)
			if err != nil || len(body) != 0 || r.Method != http.MethodGet ||
				r.URL.String() != "https://mail.example.test/api/internal/fugue/mail/sample" ||
				r.Header.Get("Authorization") != "Bearer "+token {
				t.Fatal("unexpected standby provider request")
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(fmt.Sprintf(
				`{"appId":"sample","tenantId":"tenant","projectId":"project","hostname":"sample.apps.example.test","generation":%d,"state":"active"}`, generation,
			)))}, nil
		})
		if _, err := p.ObserveBinding(context.Background(), binding); (err != nil) != (generation != binding.Generation) {
			t.Fatalf("generation %d: %v", generation, err)
		}
	}
}

func (f providerConfigFixture) GetMailPlatformConfig() (model.MailPlatformConfig, error) {
	return model.MailPlatformConfig{MailflareAppID: "mail-service", Enabled: true}, nil
}

func (f providerConfigFixture) GetApp(string) (model.App, error) { return f.app, nil }

func (f providerConfigFixture) SaveAppMailProviderDNS(model.AppMailBinding, []model.AppMailDNSRecord) error {
	panic("configuration checks must never write a DNS observation")
}

func TestProviderReadinessRejectsMismatchedSuffixAndCredentials(t *testing.T) {
	app := model.App{
		Route: &model.AppRoute{Hostname: "mail.example.test", PublicURL: "https://mail.example.test"},
		Spec: model.AppSpec{Env: map[string]string{
			"FUGUE_MAIL_APP_BASE_DOMAIN": "apps.example.test",
			"FUGUE_MAIL_SERVICE_TOKEN":   strings.Repeat("t", 32),
		}},
	}
	p := NewProvider(providerConfigFixture{app})
	if err := p.CheckReconciliationConfig(" APPS.EXAMPLE.TEST. "); err != nil {
		t.Fatal(err)
	}
	if err := p.CheckReconciliationConfig("other.example.test"); err == nil {
		t.Fatal("wrong application suffix admitted")
	}
	delete(app.Spec.Env, "FUGUE_MAIL_SERVICE_TOKEN")
	if err := p.CheckReconciliationConfig("apps.example.test"); err == nil {
		t.Fatal("missing mail service credential admitted")
	}
}
