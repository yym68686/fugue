package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestStaticEdgeCutoverUsesFugueDNSBackend(t *testing.T) {
	zone := model.HostedZone{ID: "dnszone_test", ZoneName: "example.test", Status: model.HostedZoneStatusActive, DelegationStatus: model.HostedZoneDelegationStatusReady}
	record := model.DNSRecord{ID: "dnsrec_test", ZoneID: zone.ID, Name: "@", FQDN: "example.test", Type: model.DNSRecordTypeA, Values: []string{"192.0.2.10"}, TTL: 60, FlattenMode: model.DNSRecordFlattenModeNone, Source: model.DNSRecordSourceUser, Status: model.DNSRecordStatusActive}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/dns/zones/example.test":
			_ = json.NewEncoder(w).Encode(map[string]any{"zone": zone})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/dns/zones/example.test/records":
			_ = json.NewEncoder(w).Encode(map[string]any{"records": []model.DNSRecord{record}})
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/dns/zones/example.test/records/dnsrec_test":
			var req patchHostedDNSRecordClientRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Values) != 1 {
				http.Error(w, "bad patch", http.StatusBadRequest)
				return
			}
			record.Values = append([]string(nil), req.Values...)
			_ = json.NewEncoder(w).Encode(map[string]any{"record": record})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	t.Setenv("FUGUE_API_URL", server.URL)
	t.Setenv("FUGUE_API_KEY", "token")
	t.Setenv("FUGUE_STATIC_EDGE_STATE_DIR", t.TempDir())
	cli := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	o := staticEdgeCutoverOptions{DNSProvider: staticEdgeDNSProviderFugue, Zone: zone.ZoneName, Hostnames: []string{zone.ZoneName}, FromIP: "192.0.2.10", ToIP: "192.0.2.20", Candidate: "candidate", ProbePath: "/health", Observe: 5 * time.Second, Timeout: time.Second, Execute: true}
	if err := cli.runStaticEdgeCutoverWithChecks(context.Background(), o, goodCutoverCheck, goodCutoverCheck, goodCutoverCheck); err != nil {
		t.Fatal(err)
	}
	if got := record.Values; len(got) != 1 || got[0] != "192.0.2.20" {
		t.Fatalf("unexpected patched values: %v", got)
	}
}

func TestHostedDNSStaticEdgeBackendReadPatch(t *testing.T) {
	zone := model.HostedZone{ID: "dnszone_test", ZoneName: "example.test", Status: model.HostedZoneStatusActive, DelegationStatus: model.HostedZoneDelegationStatusReady}
	record := model.DNSRecord{ID: "dnsrec_test", ZoneID: zone.ID, Name: "@", FQDN: "example.test", Type: model.DNSRecordTypeA, Values: []string{"192.0.2.10"}, TTL: 60, FlattenMode: model.DNSRecordFlattenModeNone, Source: model.DNSRecordSourceUser, Status: model.DNSRecordStatusActive}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Fatal("missing authorization")
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/dns/zones/example.test":
			_ = json.NewEncoder(w).Encode(map[string]any{"zone": zone})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/dns/zones/example.test/records":
			_ = json.NewEncoder(w).Encode(map[string]any{"records": []model.DNSRecord{record}})
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/dns/zones/example.test/records/dnsrec_test":
			var req patchHostedDNSRecordClientRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Values) != 1 {
				t.Fatalf("unexpected patch: %+v err=%v", req, err)
			}
			record.Values = append([]string(nil), req.Values...)
			_ = json.NewEncoder(w).Encode(map[string]any{"record": record})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := newClientWithOptions(server.URL, "token", clientOptions{RequireToken: true})
	if err != nil {
		t.Fatal(err)
	}
	backend := &fugueHostedDNSClient{client: client}
	zoneID, err := backend.ensureZoneID(nil, "example.test")
	if err != nil || zoneID != zone.ID {
		t.Fatalf("ensure zone: id=%s err=%v", zoneID, err)
	}
	before, err := backend.staticEdgeA(nil, zoneID, "example.test")
	if err != nil || before.str("content") != "192.0.2.10" {
		t.Fatalf("read record: %v %#v", err, before)
	}
	if err := backend.staticEdgePatch(nil, zoneID, before, "192.0.2.20"); err != nil {
		t.Fatal(err)
	}
}

func TestHostedDNSStaticEdgeBackendRejectsUnreadyZone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"zone": model.HostedZone{ID: "z", ZoneName: "example.test", Status: model.HostedZoneStatusPendingDelegation, DelegationStatus: model.HostedZoneDelegationStatusPending}})
	}))
	defer server.Close()
	client, err := newClientWithOptions(server.URL, "token", clientOptions{RequireToken: true})
	if err != nil {
		t.Fatal(err)
	}
	backend := &fugueHostedDNSClient{client: client}
	if _, err := backend.ensureZoneID(nil, "example.test"); err != nil {
		t.Fatalf("zone lookup should remain readable: %v", err)
	}
	if err := backend.requirePublicCutoverReady(); err == nil || !strings.Contains(err.Error(), "not ready for public cutover") {
		t.Fatalf("expected readiness error, got %v", err)
	}
}

func TestHostedDNSStaticEdgeBackendIgnoresNonRoutingRecords(t *testing.T) {
	zone := model.HostedZone{ID: "dnszone_test", ZoneName: "example.test", Status: model.HostedZoneStatusActive, DelegationStatus: model.HostedZoneDelegationStatusReady}
	records := []model.DNSRecord{
		{ID: "txt", ZoneID: zone.ID, Name: "@", FQDN: "example.test", Type: model.DNSRecordTypeTXT, Values: []string{"v=spf1"}, Status: model.DNSRecordStatusActive},
		{ID: "a", ZoneID: zone.ID, Name: "@", FQDN: "example.test", Type: model.DNSRecordTypeA, Values: []string{"192.0.2.10"}, Status: model.DNSRecordStatusActive},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/dns/zones/example.test" {
			_ = json.NewEncoder(w).Encode(map[string]any{"zone": zone})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"records": records})
	}))
	defer server.Close()
	client, err := newClientWithOptions(server.URL, "token", clientOptions{RequireToken: true})
	if err != nil {
		t.Fatal(err)
	}
	backend := &fugueHostedDNSClient{client: client}
	zoneID, err := backend.ensureZoneID(nil, zone.ZoneName)
	if err != nil {
		t.Fatal(err)
	}
	got, err := backend.staticEdgeA(nil, zoneID, zone.ZoneName)
	if err != nil || got.str("content") != "192.0.2.10" {
		t.Fatalf("expected A record alongside TXT, got=%v err=%v", got, err)
	}
}

func TestHostedDNSStaticEdgeBackendRejectsMultipleValues(t *testing.T) {
	_, err := hostedDNSRecordToStatic(model.DNSRecord{ID: "r", FQDN: "example.test", Type: model.DNSRecordTypeA, Values: []string{"192.0.2.1", "192.0.2.2"}, Status: model.DNSRecordStatusActive})
	if err == nil || !strings.Contains(err.Error(), "exactly one IPv4 A value") {
		t.Fatalf("expected single-value error, got %v", err)
	}
}

func TestStaticEdgeCutoverCloudflareProviderKeepsLegacyOperationIdentity(t *testing.T) {
	base := staticEdgeCutoverOptions{Zone: "example.test", Hostnames: []string{"example.test"}, FromIP: "192.0.2.10", ToIP: "192.0.2.20", Candidate: "candidate", ProbePath: "/health", Observe: 5 * time.Second, Timeout: time.Second}
	explicit := base
	explicit.DNSProvider = staticEdgeDNSProviderCloudflare
	normalBase, err := normalizeStaticEdgeCutover(base)
	if err != nil {
		t.Fatal(err)
	}
	normalExplicit, err := normalizeStaticEdgeCutover(explicit)
	if err != nil {
		t.Fatal(err)
	}
	if normalExplicit.DNSProvider != "" || staticEdgeCutoverID(normalBase) != staticEdgeCutoverID(normalExplicit) {
		t.Fatalf("explicit Cloudflare changed legacy operation identity: base=%q explicit=%q", staticEdgeCutoverID(normalBase), staticEdgeCutoverID(normalExplicit))
	}
}
