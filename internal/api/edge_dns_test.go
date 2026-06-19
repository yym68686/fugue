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

func TestDedupeAndSortEdgeDNSRecordsPreservesLatencyAwarePolicy(t *testing.T) {
	t.Parallel()

	records := dedupeAndSortEdgeDNSRecords([]model.EdgeDNSRecord{
		{
			Name:   "d-target.dns.fugue.pro",
			Type:   model.EdgeDNSRecordTypeA,
			Values: []string{"95.169.10.156"},
			AnswerPolicy: model.DNSAnswerPolicy{
				PolicyKind:          model.DNSAnswerPolicyKindLatencyAware,
				Reason:              "latency_aware_stable_window_24h",
				SelectedEdgeGroupID: "edge-group-country-us",
				Weight:              200,
			},
			Candidates: []model.EdgeDNSAnswerCandidate{
				{
					IP:             "95.169.10.156",
					EdgeID:         "edge-us-fast",
					EdgeGroupID:    "edge-group-country-us",
					Weight:         200,
					Reason:         "node_quality_ttfb_100ms",
					TrafficClass:   "streaming",
					Score:          100,
					ScoreBreakdown: map[string]float64{"latency": 100},
					Healthy:        true,
					RouteReady:     true,
					TLSReady:       true,
				},
			},
		},
		{
			Name:   "d-target.dns.fugue.pro",
			Type:   model.EdgeDNSRecordTypeA,
			Values: []string{"51.38.126.103"},
			AnswerPolicy: model.DNSAnswerPolicy{
				PolicyKind:          model.DNSAnswerPolicyKindGeo,
				PreferredEdgeGroups: []string{"edge-group-country-de"},
				Reason:              "geo_healthy_route_ready",
			},
			Candidates: []model.EdgeDNSAnswerCandidate{
				{
					IP:          "51.38.126.103",
					EdgeID:      "edge-de-slow",
					EdgeGroupID: "edge-group-country-de",
					Priority:    0,
					Weight:      100,
					Healthy:     true,
					RouteReady:  true,
					TLSReady:    true,
				},
			},
		},
	})

	record := edgeDNSRecordByNameAndType(records, "d-target.dns.fugue.pro", model.EdgeDNSRecordTypeA)
	if record == nil {
		t.Fatalf("expected merged record, got %+v", records)
	}
	if record.AnswerPolicy.PolicyKind != model.DNSAnswerPolicyKindLatencyAware ||
		record.AnswerPolicy.SelectedEdgeGroupID != "edge-group-country-us" {
		t.Fatalf("expected latency-aware policy to survive geo merge, got %+v", record.AnswerPolicy)
	}
	if !stringSliceContains(record.Values, "95.169.10.156") || !stringSliceContains(record.Values, "51.38.126.103") {
		t.Fatalf("expected merged answer values, got %+v", record.Values)
	}
}

func TestMergeEdgeDNSAnswerCandidatesPreservesRicherQualityMetadata(t *testing.T) {
	t.Parallel()

	candidates := mergeEdgeDNSAnswerCandidates(
		[]model.EdgeDNSAnswerCandidate{
			{IP: "95.169.10.156", EdgeGroupID: "edge-group-country-us", Weight: 100, Healthy: true, RouteReady: true, TLSReady: true},
		},
		[]model.EdgeDNSAnswerCandidate{
			{
				IP:             "95.169.10.156",
				EdgeGroupID:    "edge-group-country-us",
				Weight:         200,
				Reason:         "node_quality_ttfb_100ms",
				TrafficClass:   "streaming",
				Score:          100,
				ScoreBreakdown: map[string]float64{"latency": 100},
				Healthy:        true,
				RouteReady:     true,
				TLSReady:       true,
			},
		},
	)

	if len(candidates) != 1 {
		t.Fatalf("expected one merged candidate, got %+v", candidates)
	}
	if candidates[0].Score != 100 ||
		candidates[0].Weight != 200 ||
		candidates[0].TrafficClass != "streaming" ||
		candidates[0].ScoreBreakdown["latency"] != 100 {
		t.Fatalf("expected richer quality metadata to survive merge, got %+v", candidates[0])
	}
}

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
	recordHealthyEdgeForRouteTest(t, storeState, "edge-default-1", defaultEdgeGroupID, "203.0.113.20")

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

func TestEdgeDNSBundlePublishesCustomDomainTargetsBeforeVerification(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	recordHealthyEdgeForRouteTest(t, storeState, "edge-default-1", defaultEdgeGroupID, "203.0.113.20")

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&answer_ip=203.0.113.10&ttl=120", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	target := server.primaryCustomDomainTarget(app)
	customTarget := edgeDNSRecordByNameAndType(bundle.Records, target, model.EdgeDNSRecordTypeA)
	if customTarget == nil {
		t.Fatalf("expected custom-domain target %s in bundle: %+v", target, bundle.Records)
	}
	if customTarget.RecordKind != model.EdgeDNSRecordKindCustomDomainTarget || customTarget.AppID != app.ID {
		t.Fatalf("unexpected custom-domain DNS record: %+v", customTarget)
	}
}

func TestEdgeDNSBundleSkipsPreVerificationTargetsForExternalAppRoutes(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	externalRoute := *app.Route
	externalRoute.Hostname = "music.chikai.de"
	externalRoute.BaseDomain = "chikai.de"
	externalRoute.PublicURL = "https://music.chikai.de"
	app, err := storeState.UpdateAppRoute(app.ID, externalRoute)
	if err != nil {
		t.Fatalf("update app route: %v", err)
	}
	app = deployAppForEdgeRouteTest(t, storeState, app)
	recordHealthyEdgeForRouteTest(t, storeState, "edge-default-1", defaultEdgeGroupID, "203.0.113.20")

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&answer_ip=203.0.113.10&ttl=120", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	target := server.primaryCustomDomainTarget(app)
	if customTarget := edgeDNSRecordByNameAndType(bundle.Records, target, model.EdgeDNSRecordTypeA); customTarget != nil {
		t.Fatalf("expected external route custom-domain target %s to be skipped, got %+v", target, customTarget)
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
	recordHealthyEdgeForRouteTest(t, storeState, "edge-hk-1", "edge-group-country-hk", "203.0.113.10")

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
	if conditional.Code != http.StatusOK {
		t.Fatalf("expected signed DNS bundle refresh status %d, got %d body=%s", http.StatusOK, conditional.Code, conditional.Body.String())
	}
	var conditionalBundle model.EdgeDNSBundle
	mustDecodeJSON(t, conditional, &conditionalBundle)
	if conditionalBundle.Version != bundle.Version {
		t.Fatalf("expected conditional signed refresh to keep content version %s, got %s", bundle.Version, conditionalBundle.Version)
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

func TestEdgeDNSBundleUsesDefaultEdgeCustomTargets(t *testing.T) {
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
	recordHealthyEdgeForRouteTest(t, storeState, "edge-us-1", "edge-group-country-us", "15.204.94.71")

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
		t.Fatalf("expected default-edge custom-domain target %s in DNS bundle: %+v", target, bundle.Records)
	}
	if strings.Join(customTarget.Values, ",") != "15.204.94.71" {
		t.Fatalf("expected default-edge target to use healthy edge IP, got %+v", customTarget)
	}
}

func TestEdgeDNSBundleKeepsCustomDomainTargetWhileTLSIsPending(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	target := server.primaryCustomDomainTarget(app)
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	if _, err := storeState.PutAppDomain(model.AppDomain{
		Hostname:    "www.example.com",
		AppID:       app.ID,
		TenantID:    app.TenantID,
		Status:      model.AppDomainStatusVerified,
		DNSStatus:   model.AppDomainDNSStatusReady,
		TLSStatus:   model.AppDomainTLSStatusPending,
		RouteTarget: target,
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("put verified TLS-pending app domain: %v", err)
	}
	recordHealthyEdgeForRouteTest(t, storeState, "edge-default-1", defaultEdgeGroupID, "203.0.113.20")

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&answer_ip=203.0.113.10&ttl=120", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	customTarget := edgeDNSRecordByNameAndType(bundle.Records, target, model.EdgeDNSRecordTypeA)
	if customTarget == nil {
		t.Fatalf("expected TLS-pending custom-domain target %s to stay in DNS bundle: %+v", target, bundle.Records)
	}
	if customTarget.RecordKind != model.EdgeDNSRecordKindCustomDomainTarget || customTarget.AppID != app.ID {
		t.Fatalf("unexpected TLS-pending custom-domain DNS record: %+v", customTarget)
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

func TestEdgeDNSBundleVerifiedCustomDomainOwnsStableTarget(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	target := server.primaryCustomDomainTarget(app)
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	if _, err := storeState.PutAppDomain(model.AppDomain{
		Hostname:    "music.chikai.de",
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
	recordHealthyEdgeForRouteTest(t, storeState, "edge-us-1", "edge-group-country-us", "15.204.94.71")
	recordHealthyEdgeForRouteTest(t, storeState, "edge-de-1", "edge-group-country-de", "51.38.126.103")
	put := performJSONRequest(t, server, http.MethodPut, "/v1/edge/route-policies/music.chikai.de", platformAdminKey, map[string]any{
		"edge_group_id": "edge-group-country-de",
		"route_policy":  model.EdgeRoutePolicyEnabled,
	})
	if put.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, put.Code, put.Body.String())
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&answer_ip=15.204.94.71", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	customTarget := edgeDNSRecordByNameAndType(bundle.Records, target, model.EdgeDNSRecordTypeA)
	if customTarget == nil {
		t.Fatalf("expected verified custom-domain target %s in DNS bundle: %+v", target, bundle.Records)
	}
	if strings.Join(customTarget.Values, ",") != "51.38.126.103" {
		t.Fatalf("expected verified custom-domain target to avoid app pre-verification merge, got %+v", customTarget)
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
		ID:              "edge-us-1",
		EdgeGroupID:     "edge-group-country-us",
		Region:          "us",
		Country:         "us",
		PublicIPv4:      "15.204.94.71",
		Status:          model.EdgeHealthHealthy,
		Healthy:         true,
		CaddyRouteCount: 1,
		TLSStatus:       model.EdgeTLSStatusReady,
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
	if platform.AnswerPolicy.PolicyKind != model.DNSAnswerPolicyKindGeo || !platform.AnswerPolicy.ECSEnabled || len(platform.Candidates) != 1 {
		t.Fatalf("expected geo answer policy and edge candidate metadata, got %+v", platform)
	}
	if platform.Candidates[0].EdgeGroupID != "edge-group-country-us" || platform.Candidates[0].Country != "us" || !platform.Candidates[0].TLSReady {
		t.Fatalf("unexpected DNS answer candidate metadata: %+v", platform.Candidates)
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

func TestEdgeDNSBundleUsesAllRouteReadyEdgesForDefaultPlatformDomain(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.EnsureManagedSharedLocationLabels(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "HK",
	}); err != nil {
		t.Fatalf("set managed shared location labels: %v", err)
	}
	app = deployAppForEdgeRouteTest(t, storeState, app)
	recordHealthyEdgeForRouteTest(t, storeState, "edge-us-1", "edge-group-country-us", "15.204.94.71")
	recordHealthyEdgeForRouteTest(t, storeState, "edge-de-1", "edge-group-country-de", "51.38.126.103")
	if _, err := storeState.PutAppDomain(model.AppDomain{
		Hostname:  "fugue.pro",
		AppID:     app.ID,
		TenantID:  app.TenantID,
		Status:    model.AppDomainStatusVerified,
		TLSStatus: model.AppDomainTLSStatusReady,
	}); err != nil {
		t.Fatalf("put platform domain binding: %v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103&route_a_answer_ip=136.112.185.40", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	rootA := edgeDNSRecordByNameAndType(bundle.Records, "fugue.pro", model.EdgeDNSRecordTypeA)
	if rootA == nil {
		t.Fatalf("expected fugue.pro A record: %+v", bundle.Records)
	}
	if len(rootA.Values) != 2 ||
		!stringSliceContains(rootA.Values, "15.204.94.71") ||
		!stringSliceContains(rootA.Values, "51.38.126.103") {
		t.Fatalf("expected DNS answer set to include all route-ready public edges, got %+v", rootA)
	}
	if len(rootA.AnswerPolicy.AllowedEdgeGroups) != 2 ||
		!stringSliceContains(rootA.AnswerPolicy.AllowedEdgeGroups, "edge-group-country-us") ||
		!stringSliceContains(rootA.AnswerPolicy.AllowedEdgeGroups, "edge-group-country-de") {
		t.Fatalf("expected answer policy to allow both route-ready edge groups, got %+v", rootA.AnswerPolicy)
	}
	if len(rootA.Candidates) != 2 {
		t.Fatalf("expected two DNS candidates, got %+v", rootA.Candidates)
	}
}

func TestEdgeDNSBundleUsesDegradedServingLKGEdgeIPsForPlatformAppRecords(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.EnsureManagedSharedLocationLabels(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "US",
	}); err != nil {
		t.Fatalf("set managed shared location labels: %v", err)
	}
	app = deployAppForEdgeRouteTest(t, storeState, app)
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:                 "edge-us-1",
		EdgeGroupID:        "edge-group-country-us",
		PublicIPv4:         "15.204.94.71",
		Status:             model.EdgeHealthDegraded,
		Healthy:            true,
		RouteBundleVersion: "routegen_lkg",
		ServingGeneration:  "routegen_lkg",
		LKGGeneration:      "routegen_lkg",
		CaddyRouteCount:    44,
		CacheStatus:        "stale",
	}); err != nil {
		t.Fatalf("record degraded serving US edge node: %v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103&route_a_answer_ip=136.112.185.40", nil)
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
	if strings.Join(platform.Values, ",") != "15.204.94.71" {
		t.Fatalf("expected degraded serving LKG edge IP instead of Route A fallback, got %+v", platform)
	}
}

func TestEdgeDNSBundleDoesNotFallbackToRouteAForUnavailableEdgeTraffic(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")

	put := performJSONRequest(t, server, http.MethodPut, "/v1/edge/route-policies/demo.fugue.pro", platformAdminKey, map[string]any{
		"edge_group_id": "edge-group-country-hk",
		"route_policy":  model.EdgeRoutePolicyCanary,
	})
	if put.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, put.Code, put.Body.String())
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103&route_a_answer_ip=136.112.185.40", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	if platform := edgeDNSRecordByNameAndType(bundle.Records, app.Route.Hostname, model.EdgeDNSRecordTypeA); platform != nil {
		t.Fatalf("edge traffic DNS must fail closed instead of publishing Route A fallback, got %+v", platform)
	}
}

func TestEdgeDNSBundlePublishesCustomDomainTargetForDisabledEdgeEnabledApp(t *testing.T) {
	t.Parallel()

	for _, tlsStatus := range []string{model.AppDomainTLSStatusPending, model.AppDomainTLSStatusReady} {
		tlsStatus := tlsStatus
		t.Run(tlsStatus, func(t *testing.T) {
			t.Parallel()

			storeState, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
			disabledSpec := app.Spec
			disabledSpec.Replicas = 0
			disableOp, err := storeState.CreateOperation(model.Operation{
				TenantID:        app.TenantID,
				Type:            model.OperationTypeDeploy,
				RequestedByType: model.ActorTypeAPIKey,
				RequestedByID:   "test-key",
				AppID:           app.ID,
				DesiredSpec:     &disabledSpec,
				ExecutionMode:   model.ExecutionModeManaged,
			})
			if err != nil {
				t.Fatalf("create disable operation: %v", err)
			}
			if _, err := storeState.CompleteManagedOperationWithResult(disableOp.ID, "", "disabled", &disabledSpec, nil); err != nil {
				t.Fatalf("complete disable operation: %v", err)
			}
			reloaded, err := storeState.GetApp(app.ID)
			if err != nil {
				t.Fatalf("reload disabled app: %v", err)
			}
			app = reloaded
			target := server.primaryCustomDomainTarget(app)
			now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
			if _, err := storeState.PutAppDomain(model.AppDomain{
				Hostname:    "www.example.com",
				AppID:       app.ID,
				TenantID:    app.TenantID,
				Status:      model.AppDomainStatusVerified,
				DNSStatus:   model.AppDomainDNSStatusReady,
				TLSStatus:   tlsStatus,
				RouteTarget: target,
				CreatedAt:   now,
				UpdatedAt:   now,
			}); err != nil {
				t.Fatalf("put verified app domain: %v", err)
			}
			recordHealthyEdgeForRouteTest(t, storeState, "edge-us-1", "edge-group-country-us", "15.204.94.71")
			put := performJSONRequest(t, server, http.MethodPut, "/v1/edge/route-policies/demo.fugue.pro", platformAdminKey, map[string]any{
				"edge_group_id": "edge-group-country-us",
				"route_policy":  model.EdgeRoutePolicyEnabled,
			})
			if put.Code != http.StatusOK {
				t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, put.Code, put.Body.String())
			}

			recorder := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-us&answer_ip=15.204.94.71&route_a_answer_ip=136.112.185.40", nil)
			server.Handler().ServeHTTP(recorder, req)
			if recorder.Code != http.StatusOK {
				t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
			}
			var bundle model.EdgeDNSBundle
			mustDecodeJSON(t, recorder, &bundle)
			customTarget := edgeDNSRecordByNameAndType(bundle.Records, target, model.EdgeDNSRecordTypeA)
			if customTarget == nil {
				t.Fatalf("expected disabled edge-enabled app to keep custom-domain target %s in DNS bundle: %+v", target, bundle.Records)
			}
			if customTarget.RecordKind != model.EdgeDNSRecordKindCustomDomainTarget || strings.Join(customTarget.Values, ",") != "15.204.94.71" {
				t.Fatalf("unexpected custom-domain target record: %+v", customTarget)
			}
		})
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

func TestEdgeDNSBundleLetsPlatformDomainBindingOverrideStaticAddressRecords(t *testing.T) {
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
	server.dnsStaticRecords = parseEdgeDNSStaticRecords(`[
		{"name":"fugue.pro","type":"A","values":["136.112.185.40"],"ttl":300},
		{"name":"fugue.pro","type":"MX","values":["10 mail.fugue.pro"],"ttl":300}
	]`, nil)
	if _, err := storeState.PutAppDomain(model.AppDomain{
		Hostname:  "fugue.pro",
		AppID:     app.ID,
		TenantID:  app.TenantID,
		Status:    model.AppDomainStatusVerified,
		TLSStatus: model.AppDomainTLSStatusReady,
	}); err != nil {
		t.Fatalf("put platform domain binding: %v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&answer_ip=203.0.113.10&route_a_answer_ip=136.112.185.40", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	rootA := edgeDNSRecordByNameAndType(bundle.Records, "fugue.pro", model.EdgeDNSRecordTypeA)
	if rootA == nil {
		t.Fatalf("expected fugue.pro A record: %+v", bundle.Records)
	}
	if rootA.RecordKind != model.EdgeDNSRecordKindPlatformDomain || strings.Join(rootA.Values, ",") != "15.204.94.71" {
		t.Fatalf("expected platform-domain A record to override static Route A address, got %+v", rootA)
	}
	rootMX := edgeDNSRecordByNameAndType(bundle.Records, "fugue.pro", model.EdgeDNSRecordTypeMX)
	if rootMX == nil || rootMX.RecordKind != model.EdgeDNSRecordKindProtected || strings.Join(rootMX.Values, ",") != "10 mail.fugue.pro" {
		t.Fatalf("expected static protected MX record to survive, got %+v", rootMX)
	}
}

func TestEdgeDNSBundleLetsConfiguredPlatformRouteOverrideStaticAddressRecords(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	server.platformRoutes = parsePlatformRoutes(`{"routes":[{
		"hostname":"api.fugue.pro",
		"kind":"control-plane-api",
		"upstream_url":"http://fugue-fugue.fugue-system.svc.cluster.local:80",
		"edge_group_mode":"region_aware"
	}]}`, nil)
	server.dnsStaticRecords = parseEdgeDNSStaticRecords(`[
		{"name":"api.fugue.pro","type":"A","values":["136.112.185.40"],"ttl":300},
		{"name":"api.fugue.pro","type":"TXT","values":["verification=keep"],"ttl":300}
	]`, nil)
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:          "edge-us-1",
		EdgeGroupID: "edge-group-country-us",
		PublicIPv4:  "15.204.94.71",
		Status:      model.EdgeHealthHealthy,
		Healthy:     true,
	}); err != nil {
		t.Fatalf("record healthy US edge node: %v", err)
	}
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:          "edge-de-1",
		EdgeGroupID: "edge-group-country-de",
		PublicIPv4:  "51.38.126.103",
		Status:      model.EdgeHealthHealthy,
		Healthy:     true,
	}); err != nil {
		t.Fatalf("record healthy DE edge node: %v", err)
	}

	us := httptest.NewRecorder()
	usReq := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-us&answer_ip=15.204.94.71&route_a_answer_ip=136.112.185.40", nil)
	server.Handler().ServeHTTP(us, usReq)
	if us.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, us.Code, us.Body.String())
	}
	var usBundle model.EdgeDNSBundle
	mustDecodeJSON(t, us, &usBundle)
	apiA := edgeDNSRecordByNameAndType(usBundle.Records, "api.fugue.pro", model.EdgeDNSRecordTypeA)
	if apiA == nil ||
		apiA.RecordKind != model.EdgeDNSRecordKindPlatformRoute ||
		strings.Join(apiA.Values, ",") != "15.204.94.71,51.38.126.103" ||
		apiA.EdgeGroupID != "edge-group-country-us" {
		t.Fatalf("expected platform route to override static API A on US DNS node, got %+v", apiA)
	}
	apiTXT := edgeDNSRecordByNameAndType(usBundle.Records, "api.fugue.pro", model.EdgeDNSRecordTypeTXT)
	if apiTXT == nil || apiTXT.RecordKind != model.EdgeDNSRecordKindProtected || strings.Join(apiTXT.Values, ",") != "verification=keep" {
		t.Fatalf("expected same-name protected TXT to survive, got %+v", apiTXT)
	}

	de := httptest.NewRecorder()
	deReq := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103&route_a_answer_ip=136.112.185.40", nil)
	server.Handler().ServeHTTP(de, deReq)
	if de.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, de.Code, de.Body.String())
	}
	var deBundle model.EdgeDNSBundle
	mustDecodeJSON(t, de, &deBundle)
	apiA = edgeDNSRecordByNameAndType(deBundle.Records, "api.fugue.pro", model.EdgeDNSRecordTypeA)
	if apiA == nil || strings.Join(apiA.Values, ",") != "51.38.126.103,15.204.94.71" || apiA.EdgeGroupID != "edge-group-country-de" {
		t.Fatalf("expected platform route DNS answers to put local edge first on DE DNS node, got %+v", apiA)
	}
}

func TestEdgeDNSBundleAppliesLatencyAwareWeights(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	server.platformRoutes = []model.PlatformRoute{
		{
			Hostname:      "api.fugue.pro",
			Kind:          model.EdgeRouteKindControlPlaneAPI,
			UpstreamKind:  model.EdgeRouteUpstreamKindMesh,
			UpstreamScope: model.EdgeRouteUpstreamScopeMesh,
			UpstreamURL:   "http://api.fugue.internal",
			TLSPolicy:     model.EdgeRouteTLSPolicyPlatform,
			RoutePolicy:   model.EdgeRoutePolicyEnabled,
			EdgeGroupMode: model.PlatformRouteEdgeGroupModeAllHealthy,
			Status:        model.EdgeRouteStatusActive,
			TTL:           60,
		},
	}
	for _, node := range []model.EdgeNode{
		{
			ID:              "edge-us-1",
			EdgeGroupID:     "edge-group-country-us",
			Country:         "us",
			Region:          "us-east",
			PublicIPv4:      "15.204.94.71",
			Status:          model.EdgeHealthHealthy,
			Healthy:         true,
			CaddyRouteCount: 1,
			TLSStatus:       model.EdgeTLSStatusReady,
		},
		{
			ID:              "edge-de-1",
			EdgeGroupID:     "edge-group-country-de",
			Country:         "de",
			Region:          "eu-central",
			PublicIPv4:      "51.38.126.103",
			Status:          model.EdgeHealthHealthy,
			Healthy:         true,
			CaddyRouteCount: 1,
			TLSStatus:       model.EdgeTLSStatusReady,
		},
	} {
		if _, _, err := storeState.UpdateEdgeHeartbeat(node); err != nil {
			t.Fatalf("record edge heartbeat: %v", err)
		}
	}
	now := time.Now().UTC()
	if err := storeState.RecordEdgePerformanceSamples([]model.EdgePerformanceSample{
		{
			ID:                    "api-us-fast",
			EdgeGroupID:           "edge-group-country-us",
			Hostname:              "api.fugue.pro",
			ClientCountry:         "de",
			ClientRegion:          "eu-central",
			ClientASN:             "as3320",
			DNSPolicy:             "client_scope_header",
			TTFBMS:                120,
			UpstreamMS:            80,
			TotalMS:               140,
			StatusCode:            200,
			SampleCount:           12,
			CacheHitCount:         10,
			CacheObservationCount: 12,
			SampledAt:             now.Add(-10 * time.Minute),
		},
		{
			ID:                    "api-de-slow",
			EdgeGroupID:           "edge-group-country-de",
			Hostname:              "api.fugue.pro",
			ClientCountry:         "de",
			ClientRegion:          "eu-central",
			ClientASN:             "as3320",
			DNSPolicy:             "client_scope_header",
			TTFBMS:                650,
			UpstreamMS:            520,
			TotalMS:               700,
			StatusCode:            200,
			SampleCount:           12,
			CacheHitCount:         1,
			CacheObservationCount: 12,
			SampledAt:             now.Add(-10 * time.Minute),
		},
	}, time.Time{}); err != nil {
		t.Fatalf("record performance samples: %v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	apiA := edgeDNSRecordByNameAndType(bundle.Records, "api.fugue.pro", model.EdgeDNSRecordTypeA)
	if apiA == nil {
		t.Fatalf("expected api.fugue.pro A record, got %+v", bundle.Records)
	}
	if apiA.AnswerPolicy.PolicyKind != model.DNSAnswerPolicyKindLatencyAware ||
		!strings.Contains(apiA.AnswerPolicy.Reason, "latency_aware") ||
		!apiA.AnswerPolicy.HealthRequired ||
		!apiA.AnswerPolicy.RouteReadyRequired ||
		apiA.AnswerPolicy.ExplorationPercent != edgeDNSExplorationPercent ||
		apiA.AnswerPolicy.SwitchCooldownSec != int(edgeDNSDecisionCooldown.Seconds()) {
		t.Fatalf("expected latency-aware policy with safety gates, got %+v", apiA.AnswerPolicy)
	}
	var usCandidate, deCandidate *model.EdgeDNSAnswerCandidate
	for index := range apiA.Candidates {
		switch apiA.Candidates[index].EdgeGroupID {
		case "edge-group-country-us":
			usCandidate = &apiA.Candidates[index]
		case "edge-group-country-de":
			deCandidate = &apiA.Candidates[index]
		}
	}
	if usCandidate == nil || deCandidate == nil {
		t.Fatalf("expected US and DE candidates, got %+v", apiA.Candidates)
	}
	if usCandidate.Weight <= deCandidate.Weight ||
		!strings.Contains(usCandidate.Reason, "latency_fast") ||
		strings.Contains(deCandidate.Reason, "latency_fast") ||
		usCandidate.Score <= 0 ||
		deCandidate.Score <= usCandidate.Score ||
		usCandidate.ScoreBreakdown["latency"] <= 0 {
		t.Fatalf("expected latency weights and latency explanation, us=%+v de=%+v", usCandidate, deCandidate)
	}
	countryScoped := edgeDNSScopedCandidatesByScope(apiA.ScopedCandidates, "country:de")
	if countryScoped == nil || countryScoped.SelectedEdgeGroupID != "edge-group-country-us" || countryScoped.PolicyKind != model.DNSAnswerPolicyKindLatencyAware {
		t.Fatalf("expected DE scoped latency profile selecting US edge, got %+v", apiA.ScopedCandidates)
	}
	asnScoped := edgeDNSScopedCandidatesByScope(apiA.ScopedCandidates, "asn:as3320")
	if asnScoped == nil || asnScoped.SelectedEdgeGroupID != "edge-group-country-us" {
		t.Fatalf("expected ASN scoped latency profile selecting US edge, got %+v", apiA.ScopedCandidates)
	}
}

func TestEdgeDNSLatencyProfilePenalizesSlowRequestBodyReads(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	groups := map[string]*edgeDNSLatencyGroupAccumulator{}
	edgeDNSLatencyAccumulate(groups, "edge-group-fast-upload", model.EdgePerformanceSample{
		ID:                 "fast-upload",
		EdgeID:             "edge-fast-1",
		EdgeGroupID:        "edge-group-fast-upload",
		Hostname:           "api.fugue.pro",
		TrafficClass:       "large_body_api",
		TTFBMS:             180,
		UpstreamMS:         120,
		TotalMS:            220,
		StatusCode:         200,
		SampleCount:        12,
		UploadEffectiveBPS: 2 * 1024 * 1024,
		MinWindowBPS:       1536 * 1024,
		BodyReadBlockMS:    40,
		MaxReadGapMS:       120,
		SampledAt:          now.Add(-5 * time.Minute),
	})
	edgeDNSLatencyAccumulate(groups, "edge-group-slow-upload", model.EdgePerformanceSample{
		ID:                  "slow-upload",
		EdgeID:              "edge-slow-1",
		EdgeGroupID:         "edge-group-slow-upload",
		Hostname:            "api.fugue.pro",
		TrafficClass:        "large_body_api",
		TTFBMS:              185,
		UpstreamMS:          125,
		TotalMS:             225,
		StatusCode:          200,
		SampleCount:         12,
		UploadEffectiveBPS:  64 * 1024,
		MinWindowBPS:        32 * 1024,
		BodyReadBlockMS:     1200,
		MaxReadGapMS:        8000,
		BodyIncompleteCount: 2,
		SampledAt:           now.Add(-5 * time.Minute),
	})

	profile := buildEdgeDNSLatencyProfile("api.fugue.pro", edgeDNSLatencyScope{}, groups)
	if profile == nil {
		t.Fatal("expected slow request-body read metrics to create a latency profile")
	}
	if profile.BestEdgeGroupID != "edge-group-fast-upload" {
		t.Fatalf("expected fast upload edge group to win, got %+v", profile)
	}
	fast := profile.Candidates["edge-group-fast-upload"]
	slow := profile.Candidates["edge-group-slow-upload"]
	if fast.Score <= 0 || slow.Score <= fast.Score || slow.Weight >= fast.Weight {
		t.Fatalf("expected slow upload candidate to be scored and weighted lower, fast=%+v slow=%+v", fast, slow)
	}
	if slow.ScoreBreakdown["upload"] <= 0 || slow.ScoreBreakdown["upload_peer"] <= 0 {
		t.Fatalf("expected upload penalties in score breakdown, slow=%+v", slow)
	}
	if profile.NodeCandidates["edge-fast-1"].Score <= 0 || profile.NodeCandidates["edge-slow-1"].Score <= 0 {
		t.Fatalf("expected node-level quality candidates, got %+v", profile.NodeCandidates)
	}
}

func TestEdgeDNSBundleDoesNotCreateScopedLatencyProfileWithoutClientScopePolicy(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	server.platformRoutes = []model.PlatformRoute{
		{
			Hostname:      "api.fugue.pro",
			Kind:          model.EdgeRouteKindControlPlaneAPI,
			UpstreamKind:  model.EdgeRouteUpstreamKindMesh,
			UpstreamScope: model.EdgeRouteUpstreamScopeMesh,
			UpstreamURL:   "http://api.fugue.internal",
			TLSPolicy:     model.EdgeRouteTLSPolicyPlatform,
			RoutePolicy:   model.EdgeRoutePolicyEnabled,
			EdgeGroupMode: model.PlatformRouteEdgeGroupModeAllHealthy,
			Status:        model.EdgeRouteStatusActive,
			TTL:           60,
		},
	}
	for _, node := range []model.EdgeNode{
		{
			ID:              "edge-us-1",
			EdgeGroupID:     "edge-group-country-us",
			Country:         "us",
			Region:          "us-east",
			PublicIPv4:      "15.204.94.71",
			Status:          model.EdgeHealthHealthy,
			Healthy:         true,
			CaddyRouteCount: 1,
			TLSStatus:       model.EdgeTLSStatusReady,
		},
		{
			ID:              "edge-de-1",
			EdgeGroupID:     "edge-group-country-de",
			Country:         "de",
			Region:          "eu-central",
			PublicIPv4:      "51.38.126.103",
			Status:          model.EdgeHealthHealthy,
			Healthy:         true,
			CaddyRouteCount: 1,
			TLSStatus:       model.EdgeTLSStatusReady,
		},
	} {
		if _, _, err := storeState.UpdateEdgeHeartbeat(node); err != nil {
			t.Fatalf("record edge heartbeat: %v", err)
		}
	}
	now := time.Now().UTC()
	if err := storeState.RecordEdgePerformanceSamples([]model.EdgePerformanceSample{
		{
			ID:            "api-us-fast-no-scope",
			EdgeGroupID:   "edge-group-country-us",
			Hostname:      "api.fugue.pro",
			ClientCountry: "de",
			ClientRegion:  "eu-central",
			TTFBMS:        120,
			UpstreamMS:    80,
			TotalMS:       140,
			StatusCode:    200,
			SampleCount:   12,
			SampledAt:     now.Add(-10 * time.Minute),
		},
		{
			ID:            "api-de-slow-no-scope",
			EdgeGroupID:   "edge-group-country-de",
			Hostname:      "api.fugue.pro",
			ClientCountry: "de",
			ClientRegion:  "eu-central",
			TTFBMS:        650,
			UpstreamMS:    520,
			TotalMS:       700,
			StatusCode:    200,
			SampleCount:   12,
			SampledAt:     now.Add(-10 * time.Minute),
		},
	}, time.Time{}); err != nil {
		t.Fatalf("record performance samples: %v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	apiA := edgeDNSRecordByNameAndType(bundle.Records, "api.fugue.pro", model.EdgeDNSRecordTypeA)
	if apiA == nil {
		t.Fatalf("expected api.fugue.pro A record, got %+v", bundle.Records)
	}
	if len(apiA.ScopedCandidates) != 0 {
		t.Fatalf("samples without client-scope DNS policy must not create scoped profiles, got %+v", apiA.ScopedCandidates)
	}
	if apiA.AnswerPolicy.PolicyKind != model.DNSAnswerPolicyKindLatencyAware {
		t.Fatalf("global latency profile should still be available, got %+v", apiA.AnswerPolicy)
	}
}

func TestEdgeDNSBundleHoldsLatencyAwareDecisionDuringCooldown(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	server.platformRoutes = []model.PlatformRoute{
		{
			Hostname:      "api.fugue.pro",
			Kind:          model.EdgeRouteKindControlPlaneAPI,
			UpstreamKind:  model.EdgeRouteUpstreamKindMesh,
			UpstreamScope: model.EdgeRouteUpstreamScopeMesh,
			UpstreamURL:   "http://api.fugue.internal",
			TLSPolicy:     model.EdgeRouteTLSPolicyPlatform,
			RoutePolicy:   model.EdgeRoutePolicyEnabled,
			EdgeGroupMode: model.PlatformRouteEdgeGroupModeAllHealthy,
			Status:        model.EdgeRouteStatusActive,
			TTL:           60,
		},
	}
	for _, node := range []model.EdgeNode{
		{
			ID:              "edge-us-1",
			EdgeGroupID:     "edge-group-country-us",
			Country:         "us",
			Region:          "us-east",
			PublicIPv4:      "15.204.94.71",
			Status:          model.EdgeHealthHealthy,
			Healthy:         true,
			CaddyRouteCount: 1,
			TLSStatus:       model.EdgeTLSStatusReady,
		},
		{
			ID:              "edge-de-1",
			EdgeGroupID:     "edge-group-country-de",
			Country:         "de",
			Region:          "eu-central",
			PublicIPv4:      "51.38.126.103",
			Status:          model.EdgeHealthHealthy,
			Healthy:         true,
			CaddyRouteCount: 1,
			TLSStatus:       model.EdgeTLSStatusReady,
		},
	} {
		if _, _, err := storeState.UpdateEdgeHeartbeat(node); err != nil {
			t.Fatalf("record edge heartbeat: %v", err)
		}
	}
	now := time.Now().UTC()
	if err := storeState.RecordEdgePerformanceSamples([]model.EdgePerformanceSample{
		{
			ID:            "api-us-now-fast",
			EdgeGroupID:   "edge-group-country-us",
			Hostname:      "api.fugue.pro",
			ClientCountry: "de",
			DNSPolicy:     "client_scope_header",
			TTFBMS:        100,
			UpstreamMS:    70,
			TotalMS:       120,
			StatusCode:    200,
			SampleCount:   10,
			SampledAt:     now.Add(-2 * time.Minute),
		},
		{
			ID:            "api-de-previous",
			EdgeGroupID:   "edge-group-country-de",
			Hostname:      "api.fugue.pro",
			ClientCountry: "de",
			DNSPolicy:     "client_scope_header",
			TTFBMS:        600,
			UpstreamMS:    500,
			TotalMS:       650,
			StatusCode:    200,
			SampleCount:   10,
			SampledAt:     now.Add(-2 * time.Minute),
		},
	}, time.Time{}); err != nil {
		t.Fatalf("record performance samples: %v", err)
	}
	cooldownUntil := now.Add(20 * time.Minute)
	if err := storeState.UpsertEdgeDNSRoutingDecisions([]model.EdgeDNSRoutingDecision{
		{
			Hostname:            "api.fugue.pro",
			ScopeKey:            "country:de",
			Country:             "de",
			SelectedEdgeGroupID: "edge-group-country-de",
			Reason:              "previous",
			SwitchedAt:          now.Add(-10 * time.Minute),
			CooldownUntil:       cooldownUntil,
			CreatedAt:           now.Add(-10 * time.Minute),
			UpdatedAt:           now.Add(-10 * time.Minute),
		},
	}); err != nil {
		t.Fatalf("upsert routing decision: %v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	apiA := edgeDNSRecordByNameAndType(bundle.Records, "api.fugue.pro", model.EdgeDNSRecordTypeA)
	if apiA == nil {
		t.Fatalf("expected api.fugue.pro A record, got %+v", bundle.Records)
	}
	countryScoped := edgeDNSScopedCandidatesByScope(apiA.ScopedCandidates, "country:de")
	if countryScoped == nil || countryScoped.SelectedEdgeGroupID != "edge-group-country-de" || countryScoped.Reason != "latency_aware_cooldown_hold" {
		t.Fatalf("expected country scoped decision to hold DE during cooldown, got %+v", apiA.ScopedCandidates)
	}
	if apiA.AnswerPolicy.ShadowSelectedEdgeGroupID != "edge-group-country-us" ||
		apiA.AnswerPolicy.ShadowReason != "latency_aware_stable_window_24h" {
		t.Fatalf("expected answer policy to expose shadow winner during cooldown, got %+v", apiA.AnswerPolicy)
	}
	first := countryScoped.Candidates[0]
	if first.EdgeGroupID != "edge-group-country-de" || !strings.Contains(first.Reason, "latency_cooldown_hold") {
		t.Fatalf("expected DE candidate promoted during cooldown, got %+v", countryScoped.Candidates)
	}
	decisions, err := storeState.ListEdgeDNSRoutingDecisions("api.fugue.pro")
	if err != nil {
		t.Fatalf("list routing decisions: %v", err)
	}
	var held *model.EdgeDNSRoutingDecision
	for index := range decisions {
		if decisions[index].ScopeKey == "country:de" {
			held = &decisions[index]
			break
		}
	}
	if held == nil || held.SelectedEdgeGroupID != "edge-group-country-de" || !held.CooldownUntil.Equal(cooldownUntil) {
		t.Fatalf("expected persisted cooldown decision to remain on DE, got %+v", decisions)
	}
}

func edgeDNSScopedCandidatesByScope(scoped []model.EdgeDNSScopedAnswerCandidates, scopeKey string) *model.EdgeDNSScopedAnswerCandidates {
	for index := range scoped {
		if scoped[index].ScopeKey == scopeKey {
			return &scoped[index]
		}
	}
	return nil
}

func TestDNSACMEChallengeAPIDrivesEdgeDNSBundleTXTRecords(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	first := performJSONRequest(t, server, http.MethodPost, "/v1/dns/acme-challenges", platformAdminKey, map[string]any{
		"zone":               "fugue.pro",
		"name":               "_acme-challenge.fugue.pro",
		"value":              "token-one",
		"ttl":                60,
		"expires_in_seconds": 3600,
	})
	if first.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, first.Code, first.Body.String())
	}
	var firstResponse struct {
		Challenge model.DNSACMEChallenge `json:"challenge"`
	}
	mustDecodeJSON(t, first, &firstResponse)
	if firstResponse.Challenge.ID == "" || firstResponse.Challenge.Name != "_acme-challenge.fugue.pro" || firstResponse.Challenge.Value != "token-one" {
		t.Fatalf("unexpected first challenge response: %+v", firstResponse.Challenge)
	}

	second := performJSONRequest(t, server, http.MethodPost, "/v1/dns/acme-challenges", platformAdminKey, map[string]any{
		"zone":               "fugue.pro",
		"name":               "_acme-challenge.fugue.pro",
		"value":              "token-two",
		"ttl":                60,
		"expires_in_seconds": 3600,
	})
	if second.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, second.Code, second.Body.String())
	}
	var secondResponse struct {
		Challenge model.DNSACMEChallenge `json:"challenge"`
	}
	mustDecodeJSON(t, second, &secondResponse)

	list := performJSONRequest(t, server, http.MethodGet, "/v1/dns/acme-challenges?zone=fugue.pro", platformAdminKey, nil)
	if list.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, list.Code, list.Body.String())
	}
	var listResponse struct {
		Challenges []model.DNSACMEChallenge `json:"challenges"`
	}
	mustDecodeJSON(t, list, &listResponse)
	if len(listResponse.Challenges) != 2 {
		t.Fatalf("expected two active challenges, got %+v", listResponse.Challenges)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&answer_ip=203.0.113.10", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	txt := edgeDNSRecordByNameAndType(bundle.Records, "_acme-challenge.fugue.pro", model.EdgeDNSRecordTypeTXT)
	if txt == nil {
		t.Fatalf("expected ACME TXT record in bundle: %+v", bundle.Records)
	}
	if txt.RecordKind != model.EdgeDNSRecordKindACMEChallenge || txt.TTL != 60 || strings.Join(txt.Values, ",") != "token-one,token-two" {
		t.Fatalf("unexpected ACME TXT record: %+v", txt)
	}

	deleted := performJSONRequest(t, server, http.MethodDelete, "/v1/dns/acme-challenges/"+firstResponse.Challenge.ID, platformAdminKey, nil)
	if deleted.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, deleted.Code, deleted.Body.String())
	}

	afterDelete := httptest.NewRecorder()
	afterDeleteReq := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&answer_ip=203.0.113.10", nil)
	server.Handler().ServeHTTP(afterDelete, afterDeleteReq)
	if afterDelete.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, afterDelete.Code, afterDelete.Body.String())
	}
	var afterDeleteBundle model.EdgeDNSBundle
	mustDecodeJSON(t, afterDelete, &afterDeleteBundle)
	txt = edgeDNSRecordByNameAndType(afterDeleteBundle.Records, "_acme-challenge.fugue.pro", model.EdgeDNSRecordTypeTXT)
	if txt == nil || strings.Join(txt.Values, ",") != secondResponse.Challenge.Value {
		t.Fatalf("expected only second ACME TXT value after cleanup, got %+v", txt)
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
