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

func TestEdgeDNSBundleKeepsRouteAOnlyCustomTargetsGlobal(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.EnsureManagedSharedLocationLabels(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "HK",
	}); err != nil {
		t.Fatalf("set managed shared location labels: %v", err)
	}
	app = deployAppForEdgeRouteTest(t, storeState, app)
	target := server.primaryCustomDomainTarget(app)
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
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
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&edge_group_id=edge-group-country-us&answer_ip=15.204.94.71&route_a_answer_ip=136.112.185.40", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	customTarget := edgeDNSRecordByNameAndType(bundle.Records, target, model.EdgeDNSRecordTypeA)
	if customTarget == nil {
		t.Fatalf("expected Route A-only custom-domain target %s in every DNS bundle: %+v", target, bundle.Records)
	}
	if strings.Join(customTarget.Values, ",") != "136.112.185.40" {
		t.Fatalf("expected Route A-only target to use route_a_answer_ip, got %+v", customTarget)
	}
}

func TestEdgeDNSBundleUsesHealthyPolicyEdgeGroupIPsForOptInTargets(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.EnsureManagedSharedLocationLabels(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "HK",
	}); err != nil {
		t.Fatalf("set managed shared location labels: %v", err)
	}
	app = deployAppForEdgeRouteTest(t, storeState, app)
	target := server.primaryCustomDomainTarget(app)
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
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
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:          "edge-hk-1",
		EdgeGroupID: "edge-group-country-hk",
		PublicIPv4:  "203.0.113.20",
		Status:      model.EdgeHealthHealthy,
		Healthy:     true,
	}); err != nil {
		t.Fatalf("record healthy HK edge node: %v", err)
	}
	put := performJSONRequest(t, server, http.MethodPut, "/v1/edge/route-policies/www.example.com", platformAdminKey, map[string]any{
		"edge_group_id": "edge-group-country-hk",
		"route_policy":  model.EdgeRoutePolicyCanary,
	})
	if put.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, put.Code, put.Body.String())
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&edge_group_id=edge-group-country-de&answer_ip=198.51.100.10&route_a_answer_ip=136.112.185.40", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	customTarget := edgeDNSRecordByNameAndType(bundle.Records, target, model.EdgeDNSRecordTypeA)
	if customTarget == nil {
		t.Fatalf("expected opt-in custom-domain target %s in DNS bundle: %+v", target, bundle.Records)
	}
	if strings.Join(customTarget.Values, ",") != "203.0.113.20" {
		t.Fatalf("expected opt-in target to use healthy policy edge IP, got %+v", customTarget)
	}
}

func TestEdgeDNSBundleDerivesFullZonePlatformAppRecords(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.EnsureManagedSharedLocationLabels(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "US",
	}); err != nil {
		t.Fatalf("set managed shared location labels: %v", err)
	}
	app = deployAppForEdgeRouteTest(t, storeState, app)
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:          "edge-us-1",
		EdgeGroupID: "edge-group-country-us",
		PublicIPv4:  "15.204.94.71",
		Status:      model.EdgeHealthHealthy,
		Healthy:     true,
	}); err != nil {
		t.Fatalf("record healthy US edge node: %v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&answer_ip=203.0.113.10&route_a_answer_ip=136.112.185.40", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	platform := edgeDNSRecordByNameAndType(bundle.Records, app.Route.Hostname, model.EdgeDNSRecordTypeA)
	if platform == nil {
		t.Fatalf("expected platform app DNS record for %s: %+v", app.Route.Hostname, bundle.Records)
	}
	if platform.RecordKind != model.EdgeDNSRecordKindPlatform || platform.EdgeGroupID != "edge-group-country-us" || strings.Join(platform.Values, ",") != "15.204.94.71" {
		t.Fatalf("unexpected platform DNS record: %+v", platform)
	}

	otherGroupRecorder := httptest.NewRecorder()
	otherGroupReq := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103&route_a_answer_ip=136.112.185.40", nil)
	server.Handler().ServeHTTP(otherGroupRecorder, otherGroupReq)
	if otherGroupRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, otherGroupRecorder.Code, otherGroupRecorder.Body.String())
	}
	var otherGroupBundle model.EdgeDNSBundle
	mustDecodeJSON(t, otherGroupRecorder, &otherGroupBundle)
	otherGroupPlatform := edgeDNSRecordByNameAndType(otherGroupBundle.Records, app.Route.Hostname, model.EdgeDNSRecordTypeA)
	if otherGroupPlatform == nil {
		t.Fatalf("expected platform app DNS record for %s in other group bundle: %+v", app.Route.Hostname, otherGroupBundle.Records)
	}
	if strings.Join(otherGroupPlatform.Values, ",") != "15.204.94.71" {
		t.Fatalf("expected other DNS node to return target edge IP, got %+v", otherGroupPlatform)
	}
}

func TestEdgeDNSBundleKeepsStaticProtectedZoneRecordsAndWildcardFallback(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.EnsureManagedSharedLocationLabels(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "US",
	}); err != nil {
		t.Fatalf("set managed shared location labels: %v", err)
	}
	app = deployAppForEdgeRouteTest(t, storeState, app)
	server.dnsStaticRecords = parseEdgeDNSStaticRecords(`[
		{"name":"fugue.pro","type":"NS","values":["ns1.dns.fugue.pro","ns2.dns.fugue.pro"],"ttl":300},
		{"name":"fugue.pro","type":"MX","values":["10 mail.fugue.pro"],"ttl":300},
		{"name":"fugue.pro","type":"TXT","values":["v=spf1 include:_spf.example.com -all"],"ttl":300},
		{"name":"fugue.pro","type":"CAA","values":["0 issue \"letsencrypt.org\""],"ttl":300},
		{"name":"demo.fugue.pro","type":"A","values":["198.51.100.7"],"ttl":300},
		{"name":"*.fugue.pro","type":"A","values":["198.51.100.9"],"ttl":300}
	]`, nil)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&answer_ip=203.0.113.10", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)

	demoA := edgeDNSRecordByNameAndType(bundle.Records, "demo.fugue.pro", model.EdgeDNSRecordTypeA)
	if demoA == nil || strings.Join(demoA.Values, ",") != "198.51.100.7" || demoA.RecordKind != model.EdgeDNSRecordKindProtected {
		t.Fatalf("expected static protected demo record to survive, got %+v", demoA)
	}
	if edgeDNSRecordByNameAndType(bundle.Records, "demo.fugue.pro", model.EdgeDNSRecordTypeAAAA) != nil {
		t.Fatalf("unexpected AAAA record for demo.fugue.pro: %+v", bundle.Records)
	}
	if wildcard := edgeDNSRecordByNameAndType(bundle.Records, "*.fugue.pro", model.EdgeDNSRecordTypeA); wildcard == nil || strings.Join(wildcard.Values, ",") != "198.51.100.9" {
		t.Fatalf("expected wildcard fallback record to be present, got %+v", wildcard)
	}
	if got := edgeDNSRecordByNameAndType(bundle.Records, "fugue.pro", model.EdgeDNSRecordTypeNS); got == nil || strings.Join(got.Values, ",") != "ns1.dns.fugue.pro,ns2.dns.fugue.pro" {
		t.Fatalf("expected static NS records for fugue.pro, got %+v", got)
	}
	if got := edgeDNSRecordByNameAndType(bundle.Records, "fugue.pro", model.EdgeDNSRecordTypeMX); got == nil || strings.Join(got.Values, ",") != "10 mail.fugue.pro" {
		t.Fatalf("expected static MX record for fugue.pro, got %+v", got)
	}
	if got := edgeDNSRecordByNameAndType(bundle.Records, "fugue.pro", model.EdgeDNSRecordTypeTXT); got == nil || len(got.Values) == 0 {
		t.Fatalf("expected static TXT record for fugue.pro, got %+v", got)
	}
	if got := edgeDNSRecordByNameAndType(bundle.Records, "fugue.pro", model.EdgeDNSRecordTypeCAA); got == nil || len(got.Values) == 0 {
		t.Fatalf("expected static CAA record for fugue.pro, got %+v", got)
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
