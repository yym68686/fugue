package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func TestEdgeDNSBundleDerivesCustomDomainTargetsAndProbe(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	target := server.primaryCustomDomainTarget(app)
	if _, err := storeState.PutAppDomain(model.AppDomain{
		Hostname:    "www.example.com",
		AppID:       app.ID,
		TenantID:    app.TenantID,
		Status:      model.AppDomainStatusVerified,
		TLSStatus:   model.AppDomainTLSStatusReady,
		RouteTarget: target,
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("put verified app domain: %v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&answer_ip=203.0.113.10&ttl=120", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("ETag") == "" {
		t.Fatal("expected DNS bundle ETag header")
	}

	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	if bundle.Version == "" || bundle.Zone != "dns.fugue.pro" {
		t.Fatalf("expected version and dns.fugue.pro zone, got %+v", bundle)
	}
	probe := edgeDNSRecordByNameAndType(bundle.Records, "d-test.dns.fugue.pro", model.EdgeDNSRecordTypeA)
	if probe == nil || probe.RecordKind != model.EdgeDNSRecordKindProbe || strings.Join(probe.Values, ",") != "203.0.113.10" {
		t.Fatalf("expected probe A record, got %+v in %+v", probe, bundle.Records)
	}
	customTarget := edgeDNSRecordByNameAndType(bundle.Records, target, model.EdgeDNSRecordTypeA)
	if customTarget == nil {
		t.Fatalf("expected custom-domain target %s in bundle: %+v", target, bundle.Records)
	}
	if customTarget.RecordKind != model.EdgeDNSRecordKindCustomDomainTarget || customTarget.AppID != app.ID || customTarget.TTL != 120 {
		t.Fatalf("unexpected custom-domain DNS record: %+v", customTarget)
	}
	if edgeDNSRecordByNameAndType(bundle.Records, "www.example.com", model.EdgeDNSRecordTypeA) != nil {
		t.Fatalf("DNS bundle must contain stable d- target, not customer host: %+v", bundle.Records)
	}
}

func TestEdgeDNSBundleSupportsGroupFilterAndConditionalFetch(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.EnsureManagedSharedLocationLabels(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "HK",
	}); err != nil {
		t.Fatalf("set managed shared location labels: %v", err)
	}
	app = deployAppForEdgeRouteTest(t, storeState, app)
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	if _, err := storeState.PutAppDomain(model.AppDomain{
		Hostname:    "www.example.com",
		AppID:       app.ID,
		TenantID:    app.TenantID,
		Status:      model.AppDomainStatusVerified,
		TLSStatus:   model.AppDomainTLSStatusReady,
		RouteTarget: server.primaryCustomDomainTarget(app),
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("put verified app domain: %v", err)
	}

	first := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&edge_group_id=edge-group-country-hk&answer_ip=203.0.113.10", nil)
	server.Handler().ServeHTTP(first, req)
	if first.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, first.Code, first.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, first, &bundle)
	etag := first.Header().Get("ETag")
	if etag == "" || bundle.Version == "" {
		t.Fatalf("expected stable version and ETag, bundle=%+v etag=%q", bundle, etag)
	}
	if edgeDNSRecordByNameAndType(bundle.Records, server.primaryCustomDomainTarget(app), model.EdgeDNSRecordTypeA) == nil {
		t.Fatalf("expected HK custom-domain target in filtered bundle: %+v", bundle.Records)
	}

	repeated := httptest.NewRecorder()
	repeatedReq := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&edge_group_id=edge-group-country-hk&answer_ip=203.0.113.10", nil)
	server.Handler().ServeHTTP(repeated, repeatedReq)
	if repeated.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, repeated.Code, repeated.Body.String())
	}
	var repeatedBundle model.EdgeDNSBundle
	mustDecodeJSON(t, repeated, &repeatedBundle)
	if repeatedBundle.Version != bundle.Version || repeated.Header().Get("ETag") != etag {
		t.Fatalf("expected unchanged DNS content to keep version/ETag, first=%s/%s repeated=%s/%s", bundle.Version, etag, repeatedBundle.Version, repeated.Header().Get("ETag"))
	}

	conditional := httptest.NewRecorder()
	conditionalReq := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&edge_group_id=edge-group-country-hk&answer_ip=203.0.113.10", nil)
	conditionalReq.Header.Set("If-None-Match", etag)
	server.Handler().ServeHTTP(conditional, conditionalReq)
	if conditional.Code != http.StatusNotModified {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusNotModified, conditional.Code, conditional.Body.String())
	}

	changed := httptest.NewRecorder()
	changedReq := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&edge_group_id=edge-group-country-hk&answer_ip=203.0.113.11", nil)
	changedReq.Header.Set("If-None-Match", etag)
	server.Handler().ServeHTTP(changed, changedReq)
	if changed.Code != http.StatusOK {
		t.Fatalf("expected status %d after answer IP change, got %d body=%s", http.StatusOK, changed.Code, changed.Body.String())
	}
	var changedBundle model.EdgeDNSBundle
	mustDecodeJSON(t, changed, &changedBundle)
	if changedBundle.Version == bundle.Version {
		t.Fatalf("expected answer IP change to update DNS bundle version %s", bundle.Version)
	}
}

func edgeDNSRecordByNameAndType(records []model.EdgeDNSRecord, name, recordType string) *model.EdgeDNSRecord {
	for index := range records {
		if records[index].Name == name && records[index].Type == recordType {
			return &records[index]
		}
	}
	return nil
}
