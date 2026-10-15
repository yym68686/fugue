package api

import (
	"context"
	"fugue/internal/model"
	"fugue/internal/tlsissuance"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestTLSIssuancePolicyAndPublicChallengeIsolation(t *testing.T) {
	state, server, ordinary, admin, app, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	host := "customer.external.test"
	if _, err := state.PutAppDomain(model.AppDomain{Hostname: host, AppID: app.ID, TenantID: app.TenantID, Status: model.AppDomainStatusVerified}); err != nil {
		t.Fatal(err)
	}
	policy := tlsissuance.Policy{Schema: "fugue.tls-issuance-policy/v1", Generation: 1, Enabled: true, DirectoryURL: "https://ca.example/directory", IssuerID: "issuer-fixture", Contact: "mailto:ops@example.test", RenewBeforeHours: 720, RetryBaseSeconds: 3600, Domains: []tlsissuance.DomainPolicy{{Hostname: host, AppID: app.ID, TenantID: app.TenantID, Mode: "http-01", ValidationTargets: []string{"192.0.2.8"}}}}
	path := "/v1/admin/tls-issuance/policy"
	if r := performJSONRequest(t, server, http.MethodPut, path, ordinary, policy); r.Code != 403 {
		t.Fatal("ordinary writer gained issuance authority", r.Code)
	}
	if r := performJSONRequest(t, server, http.MethodPut, path, admin, policy); r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	policy.Generation = 3
	if r := performJSONRequest(t, server, http.MethodPut, path, admin, policy); r.Code != 409 {
		t.Fatal("generation gap accepted", r.Code, r.Body.String())
	}
	policy.Generation = 1
	token := strings.Repeat("a", 32)
	proof := token + "." + strings.Repeat("b", 43)
	if err := state.UpdateTLSIssuanceState(context.Background(), policy.IssuerID, policy.DirectoryURL, host, func(s *tlsissuance.State) error {
		s.Token = token
		s.KeyAuthorization = proof
		s.ChallengeExpires = time.Now().Add(time.Minute)
		s.AccountKeyPEM = "private-account-fixture"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	challengePath := "/v1/acme/http-01/" + host + "/" + token
	if r := performJSONRequest(t, server, http.MethodGet, challengePath, "", nil); r.Code != 200 || r.Body.String() != proof || r.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("public proof incorrect", r.Code, r.Body.String())
	}
	for _, path := range []string{challengePath + "x", "/v1/acme/http-01/other.external.test/" + token} {
		if r := performJSONRequest(t, server, http.MethodGet, path, "", nil); r.Code != 404 || strings.Contains(r.Body.String(), "private-account-fixture") {
			t.Fatal("challenge escaped identity", r.Code, r.Body.String())
		}
	}
	state.UpdateTLSIssuanceState(context.Background(), policy.IssuerID, policy.DirectoryURL, host, func(s *tlsissuance.State) error { s.ChallengeExpires = time.Now().Add(-time.Second); return nil })
	if r := performJSONRequest(t, server, http.MethodGet, challengePath, "", nil); r.Code != 404 {
		t.Fatal("expired proof served", r.Code)
	}
	policy.Generation = 2
	policy.Domains[0].TenantID = "other-owner"
	if r := performJSONRequest(t, server, http.MethodPut, "/v1/admin/tls-issuance/policy", admin, policy); r.Code != 409 {
		t.Fatal("owner mismatch accepted", r.Code)
	}
}
