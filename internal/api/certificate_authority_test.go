package api

import (
	"context"
	"fugue/internal/model"
	"net/http"
	"testing"
	"time"
)

func TestCertificateAuthorityUsesExactDomain(t *testing.T) {
	s, server, _, app := setupAppConfigTestServer(t, model.AppSpec{Image: "ghcr.io/example/demo:1", Ports: []int{8080}, Replicas: 1, RuntimeID: "runtime_managed_shared"})
	_, ordinary, err := s.CreateAPIKey(app.TenantID, "workspace", []string{"dns.read", "dns.write"})
	if err != nil {
		t.Fatal(err)
	}
	_, issuer, err := s.CreateAPIKey(app.TenantID, "certificate", []string{"dns.acme:service.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	const path = "/v1/dns/acme-challenges"
	payload := map[string]any{"zone": "service.example.test", "name": "_acme-challenge.service.example.test", "value": "sample"}
	if r := performJSONRequest(t, server, http.MethodPost, path, ordinary, payload); r.Code != 403 {
		t.Fatal(r.Code)
	}
	if r := performJSONRequest(t, server, http.MethodGet, path, issuer, nil); r.Code != 403 {
		t.Fatal(r.Code)
	}
	r := performJSONRequest(t, server, http.MethodPost, path, issuer, payload)
	if r.Code != 201 {
		t.Fatal(r.Code, r.Body.String())
	}
	var result struct{ Challenge model.DNSACMEChallenge }
	mustDecodeJSON(t, r, &result)
	payload["name"] = "_acme-challenge.child.service.example.test"
	if r := performJSONRequest(t, server, http.MethodPost, path, issuer, payload); r.Code != 403 {
		t.Fatal(r.Code)
	}
	if r := performJSONRequest(t, server, http.MethodDelete, path+"/"+result.Challenge.ID, issuer, nil); r.Code != 200 {
		t.Fatal(r.Code)
	}
}

func TestHostedZoneRequiresIndependentOwnership(t *testing.T) {
	s, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	_, key, err := s.CreateAPIKey(app.TenantID, "workspace", []string{"dns.read", "dns.write"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err = s.UpdateDNSHeartbeat(model.DNSNode{ID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", PublicIPv4: "192.0.2.8", Healthy: true, Status: model.EdgeHealthHealthy, LastSeenAt: &now}); err != nil {
		t.Fatal(err)
	}
	seedVerifiedDNSDelegationFixture(t, server, "example.test")
	name := "claimed.example.test"
	r := performJSONRequest(t, server, http.MethodPost, "/v1/dns/zones", key, map[string]string{"zone_name": name})
	if r.Code != 201 {
		t.Fatal(r.Code, r.Body.String())
	}
	var result struct{ Zone model.HostedZone }
	mustDecodeJSON(t, r, &result)
	if result.Zone.Status != model.HostedZoneStatusPendingOwnership {
		t.Fatal(result.Zone.Status)
	}
	server.dnsParentNSLookup = func(context.Context, string) ([]string, error) { return nil, nil }
	server.dnsOwnershipTXTLookup = func(context.Context, string) ([]string, error) { return []string{"another registration"}, nil }
	r = performJSONRequest(t, server, http.MethodGet, "/v1/dns/zones/"+name+"/preflight", key, nil)
	if r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	z, err := s.GetHostedZoneByName(name)
	if err != nil || z.Status != model.HostedZoneStatusPendingOwnership {
		t.Fatal(z.Status, err)
	}
	server.dnsOwnershipTXTLookup = func(context.Context, string) ([]string, error) { return []string{result.Zone.ID}, nil }
	r = performJSONRequest(t, server, http.MethodGet, "/v1/dns/zones/"+name+"/preflight", key, nil)
	if r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	z, err = s.GetHostedZoneByName(name)
	if err != nil || z.Status == model.HostedZoneStatusPendingOwnership {
		t.Fatal(z.Status, err)
	}
}
