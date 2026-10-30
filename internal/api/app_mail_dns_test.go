package api

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/miekg/dns"
)

func TestAppMailDNSMatchesExactRecordsAndRejectsConflictingPolicies(t *testing.T) {
	for _, tc := range []struct {
		kind      string
		want, got []string
		match     bool
	}{
		{"MX", []string{"10 mx.example.test."}, []string{"010 MX.EXAMPLE.TEST"}, true},
		{"MX", []string{"10 mx.example.test."}, []string{"10 mx.example.test.", "20 foreign.example.test."}, false},
		{"CNAME", []string{"relay.example.test."}, []string{"RELAY.EXAMPLE.TEST"}, true},
		{"TXT", []string{"v=spf1 -all"}, []string{"unrelated-verification", "v=spf1 -all"}, true},
		{"TXT", []string{"v=spf1 -all"}, []string{"v=spf1 -all", "v=spf1 +all"}, false},
		{"TXT", []string{"v=DKIM1; p=CaseSensitive"}, []string{"v=DKIM1; p=casesensitive"}, false},
		{"MX", []string{"10 mx.example.test."}, nil, false},
	} {
		if got := appMailDNSMatches(tc.kind, tc.want, tc.got); got != tc.match {
			t.Fatalf("%+v: %v", tc, got)
		}
	}
}

func TestAppMailDNSAnswersJoinTXTChunksAndIgnoreCNAMEChainTargets(t *testing.T) {
	response := &dns.Msg{}
	for _, text := range []string{
		"_key.app.apps.example.test. 60 IN TXT \"v=DKIM1; \" \"p=CaseSensitive\"",
		"foreign.example.test. 60 IN TXT \"forged\"",
		"_key.app.apps.example.test. 60 IN CNAME foreign.example.test.",
	} {
		rr, err := dns.NewRR(text)
		if err != nil {
			t.Fatal(err)
		}
		response.Answer = append(response.Answer, rr)
	}
	values, err := appMailDNSAnswerValues(response, "_key.app.apps.example.test", "TXT")
	if err != nil || len(values) != 1 || values[0] != "v=DKIM1; p=CaseSensitive" {
		t.Fatal(values, err)
	}
	for len(response.Answer) < 40 {
		response.Answer = append(response.Answer, response.Answer[0])
	}
	if _, err := appMailDNSAnswerValues(response, "_key.app.apps.example.test", "TXT"); err == nil {
		t.Fatal("unbounded answers accepted")
	}
}

func TestAppMailDNSObservationIsBoundedAndNeverTreatsResolverFailureAsReady(t *testing.T) {
	rules := []model.AppMailDNSRecord{{Name: "@", Type: "MX", Values: []string{"10 mx.example.test."}}, {Name: "_key", Type: "TXT", Values: []string{"key"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	records, state := observeAppMailDNS(ctx, "app.apps.example.test", rules, func(ctx context.Context, name, kind string) ([]string, error) {
		if kind == "MX" {
			return []string{"10 mx.example.test."}, nil
		}
		if name != "_key.app.apps.example.test" {
			t.Error(name)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if state != "pending" || records[0].Status != "match" || records[1].Status != "unknown" {
		t.Fatal(records, state)
	}
	if records, state := observeAppMailDNS(ctx, "app.apps.example.test", nil, nil); state != "unconfigured" || len(records) != 0 {
		t.Fatal(state)
	}
}

func TestAppMailDNSAPIRequiresOwnershipAndDoesNotQueryInactiveBindings(t *testing.T) {
	st, server, writer, reader, app, _ := setupAppDomainTestServer(t)
	cfg := model.MailPlatformConfig{MailflareAppID: app.ID, Enabled: true, DNSRecords: []model.AppMailDNSRecord{{Name: "@", Type: "MX", Values: []string{"10 mx.example.test."}, TTL: 60}}}
	cfg.AppDNSRecords = map[string][]model.AppMailDNSRecord{"peer-app": {{Name: "_key", Type: "TXT", Values: []string{"peer-only-key"}, TTL: 60}}}
	if err := st.SaveMailPlatformConfig(cfg); err != nil {
		t.Fatal(err)
	}
	binding, err := st.SaveAppMailBinding(model.AppMailBinding{AppID: app.ID, TenantID: app.TenantID, ProjectID: app.ProjectID, Hostname: app.Route.Hostname, LocalPart: "support", State: "active"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	server.mailDNSLookup = func(_ context.Context, name, kind string) ([]string, error) {
		calls.Add(1)
		if name != app.Route.Hostname || kind != "MX" {
			t.Error(name, kind)
		}
		return []string{"10 mx.example.test."}, nil
	}
	path := "/v1/apps/" + app.ID + "/mail/dns"
	if res := performJSONRequest(t, server, http.MethodGet, path, writer, nil); res.Code != 403 {
		t.Fatal("write-only credential read DNS", res.Code)
	}
	res := performJSONRequest(t, server, http.MethodGet, path, reader, nil)
	var observation struct {
		Status  string                        `json:"status"`
		Records []appMailDNSRecordObservation `json:"records"`
	}
	if res.Code != 200 {
		t.Fatal(res.Code, res.Body.String())
	}
	mustDecodeJSON(t, res, &observation)
	if observation.Status != "verified" || len(observation.Records) != 1 || res.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(observation)
	}
	other, _ := st.CreateTenant("DNS peer")
	_, key, err := st.CreateAPIKey(other.ID, "peer", []string{"app.read"})
	if err != nil {
		t.Fatal(err)
	}
	if res := performJSONRequest(t, server, http.MethodGet, path, key, nil); res.Code != 403 && res.Code != 404 {
		t.Fatal(res.Code)
	}
	if calls.Load() != 1 {
		t.Fatal("foreign tenant queried DNS")
	}
	binding.State = "suspended"
	if _, err = st.SaveAppMailBinding(binding, 1); err != nil {
		t.Fatal(err)
	}
	res = performJSONRequest(t, server, http.MethodGet, path, reader, nil)
	if res.Code != 200 {
		t.Fatal(res.Code)
	}
	mustDecodeJSON(t, res, &observation)
	if observation.Status != "inactive" || len(observation.Records) != 0 || calls.Load() != 1 {
		t.Fatal(observation, calls.Load())
	}
}

func TestAppMailServiceURLNeverContainsCredentialsOrAPath(t *testing.T) {
	st, server, _, _, app, _ := setupAppDomainTestServer(t)
	if err := st.SaveMailPlatformConfig(model.MailPlatformConfig{MailflareAppID: app.ID, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	result := map[string]any{}
	server.addMailServiceURL(result)
	if result["service_url"] != app.Route.PublicURL {
		t.Fatal(result)
	}
	for _, raw := range []string{"http://mail.example.test", "https://user:private@mail.example.test", "https://mail.example.test/?token=private", "https://mail.example.test/#private", "https://foreign.example.test", "https://mail.example.test/nested"} {
		if origin := appMailServiceOrigin(model.AppRoute{Hostname: "mail.example.test", PublicURL: raw}); origin != "" {
			t.Fatal("unsafe origin", origin)
		}
	}
}

func TestAppMailDNSDiscardsConcurrentIntentChange(t *testing.T) {
	st, server, _, reader, app, _ := setupAppDomainTestServer(t)
	if err := st.SaveMailPlatformConfig(model.MailPlatformConfig{MailflareAppID: app.ID, Enabled: true, DNSRecords: []model.AppMailDNSRecord{{Name: "@", Type: "MX", Values: []string{"10 mx.example.test."}, TTL: 60}}}); err != nil {
		t.Fatal(err)
	}
	b, err := st.SaveAppMailBinding(model.AppMailBinding{AppID: app.ID, TenantID: app.TenantID, ProjectID: app.ProjectID, Hostname: app.Route.Hostname, LocalPart: "support", State: "active"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	server.mailDNSLookup = func(context.Context, string, string) ([]string, error) {
		b.State = "suspended"
		_, err := st.SaveAppMailBinding(b, 1)
		if err != nil {
			return nil, errors.New("fixture update failed")
		}
		return []string{"10 mx.example.test."}, nil
	}
	res := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/mail/dns", reader, nil)
	if res.Code != 409 {
		t.Fatal(res.Code, res.Body.String())
	}
}
