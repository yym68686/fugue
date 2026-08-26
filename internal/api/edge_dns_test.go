package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

func TestDedupeAndSortEdgeDNSRecordsPreservesLatencyAwarePolicy(t *testing.T) {
	t.Parallel()

	records := dedupeAndSortEdgeDNSRecords([]model.EdgeDNSRecord{
		{
			Name:       "demo.fugue.pro",
			Type:       model.EdgeDNSRecordTypeA,
			Values:     []string{"95.169.10.156"},
			RecordKind: model.EdgeDNSRecordKindPlatform,
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
			Name:       "demo.fugue.pro",
			Type:       model.EdgeDNSRecordTypeA,
			Values:     []string{"51.38.126.103"},
			RecordKind: model.EdgeDNSRecordKindPlatform,
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

	record := edgeDNSRecordByNameAndType(records, "demo.fugue.pro", model.EdgeDNSRecordTypeA)
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

func TestSharedCustomDomainTargetKeepsCoherentLatencySensitiveRoutingProfile(t *testing.T) {
	t.Parallel()

	apiProfile := model.EdgeDNSRecord{
		Name:       "shared-target.dns.fugue.pro",
		Type:       model.EdgeDNSRecordTypeA,
		Values:     []string{"15.204.94.71", "51.38.126.103"},
		TTL:        60,
		RecordKind: model.EdgeDNSRecordKindCustomDomainTarget,
		Status:     model.EdgeRouteStatusActive,
		AnswerPolicy: model.DNSAnswerPolicy{
			PolicyKind:          model.DNSAnswerPolicyKindLatencyAware,
			Reason:              "api_quality_profile",
			SelectedEdgeGroupID: "edge-group-country-us",
		},
		Candidates: []model.EdgeDNSAnswerCandidate{
			{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", TrafficClass: "dynamic_api", Score: 100, Weight: 200, Healthy: true, RouteReady: true, TLSReady: true},
			{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", TrafficClass: "dynamic_api", Score: 500, Weight: 20, Healthy: true, RouteReady: true, TLSReady: true},
		},
		ScopedCandidates: []model.EdgeDNSScopedAnswerCandidates{
			{
				ScopeKey:            "country:us",
				Country:             "us",
				PolicyKind:          model.DNSAnswerPolicyKindLatencyAware,
				Reason:              "api_scoped_quality_profile",
				SelectedEdgeGroupID: "edge-group-country-us",
				Candidates: []model.EdgeDNSAnswerCandidate{
					{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", TrafficClass: "dynamic_api", Score: 90, Weight: 200, Healthy: true, RouteReady: true, TLSReady: true},
					{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", TrafficClass: "dynamic_api", Score: 550, Weight: 20, Healthy: true, RouteReady: true, TLSReady: true},
				},
			},
		},
	}
	staticProfile := model.EdgeDNSRecord{
		Name:       apiProfile.Name,
		Type:       model.EdgeDNSRecordTypeA,
		Values:     append([]string(nil), apiProfile.Values...),
		TTL:        60,
		RecordKind: model.EdgeDNSRecordKindCustomDomainTarget,
		Status:     model.EdgeRouteStatusActive,
		AnswerPolicy: model.DNSAnswerPolicy{
			PolicyKind:          model.DNSAnswerPolicyKindLatencyAware,
			Reason:              "static_quality_profile",
			SelectedEdgeGroupID: "edge-group-country-de",
		},
		Candidates: []model.EdgeDNSAnswerCandidate{
			{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", TrafficClass: "static_cacheable", Score: 700, Weight: 20, Healthy: true, RouteReady: true, TLSReady: true},
			{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", TrafficClass: "static_cacheable", Score: 40, Weight: 200, Healthy: true, RouteReady: true, TLSReady: true},
		},
		ScopedCandidates: []model.EdgeDNSScopedAnswerCandidates{
			{
				ScopeKey:            "country:us",
				Country:             "us",
				PolicyKind:          model.DNSAnswerPolicyKindLatencyAware,
				Reason:              "static_scoped_quality_profile",
				SelectedEdgeGroupID: "edge-group-country-de",
				Candidates: []model.EdgeDNSAnswerCandidate{
					{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", TrafficClass: "static_cacheable", Score: 800, Weight: 20, Healthy: true, RouteReady: true, TLSReady: true},
					{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", TrafficClass: "static_cacheable", Score: 30, Weight: 200, Healthy: true, RouteReady: true, TLSReady: true},
				},
			},
		},
	}

	for _, input := range [][]model.EdgeDNSRecord{
		{apiProfile, staticProfile},
		{staticProfile, apiProfile},
	} {
		records := dedupeAndSortEdgeDNSRecords(input)
		if len(records) != 1 {
			t.Fatalf("expected one shared target record, got %+v", records)
		}
		record := records[0]
		if record.AnswerPolicy.Reason != "api_quality_profile" || record.AnswerPolicy.SelectedEdgeGroupID != "edge-group-country-us" {
			t.Fatalf("expected API policy to remain coherent, got %+v", record.AnswerPolicy)
		}
		if len(record.Candidates) != 2 {
			t.Fatalf("expected two API candidates, got %+v", record.Candidates)
		}
		for _, candidate := range record.Candidates {
			if candidate.TrafficClass != "dynamic_api" {
				t.Fatalf("expected candidate metadata from the same API profile, got %+v", record.Candidates)
			}
		}
		if len(record.ScopedCandidates) != 1 ||
			record.ScopedCandidates[0].Reason != "api_scoped_quality_profile" ||
			record.ScopedCandidates[0].SelectedEdgeGroupID != "edge-group-country-us" {
			t.Fatalf("expected scoped candidates from the same API profile, got %+v", record.ScopedCandidates)
		}
		for _, candidate := range record.ScopedCandidates[0].Candidates {
			if candidate.TrafficClass != "dynamic_api" {
				t.Fatalf("expected scoped candidate metadata from the same API profile, got %+v", record.ScopedCandidates)
			}
		}
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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

func TestEdgeDNSBundleServesPublishedArtifact(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	now := time.Now().UTC()
	options := edgeDNSBundleOptions{
		DNSNodeID:       "dns-us-1",
		EdgeGroupID:     "edge-group-country-us",
		Zone:            "fugue.pro",
		AnswerIPs:       []string{"203.0.113.10"},
		RouteAAnswerIPs: []string{"136.112.185.40"},
		TTL:             60,
	}
	bundle := model.EdgeDNSBundle{
		SchemaVersion: model.BundleSchemaVersionV1,
		Version:       "dnsgen_published_artifact",
		Generation:    "dnsgen_published_artifact",
		GeneratedAt:   now,
		ValidUntil:    now.Add(10 * time.Minute),
		Issuer:        model.BundleIssuerFugue,
		DNSNodeID:     options.DNSNodeID,
		EdgeGroupID:   options.EdgeGroupID,
		Zone:          options.Zone,
		Records: []model.EdgeDNSRecord{{
			Name:       "artifact.fugue.pro",
			Type:       model.EdgeDNSRecordTypeA,
			Values:     []string{"203.0.113.10"},
			TTL:        60,
			RecordKind: model.EdgeDNSRecordKindProbe,
			Status:     model.EdgeRouteStatusActive,
		}},
	}
	bundle = signEdgeDNSBundle(bundle, server.bundleKeyring(), 10*time.Minute)
	immutable := newEdgeDNSBundleArtifact(options, bundle, now)
	publishFullEdgeDNSArtifactForTest(t, storeState, server, immutable)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&dns_node_id=dns-us-1&zone=fugue.pro&edge_group_id=edge-group-country-us&answer_ip=203.0.113.10&route_a_answer_ip=136.112.185.40&ttl=60", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var got model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &got)
	if got.Version != bundle.Version {
		t.Fatalf("expected published artifact version %s, got %s", bundle.Version, got.Version)
	}
	if edgeDNSRecordByNameAndType(got.Records, "artifact.fugue.pro", model.EdgeDNSRecordTypeA) == nil {
		t.Fatalf("expected artifact record, got %+v", got.Records)
	}
	var metrics strings.Builder
	server.writeEdgeDNSArtifactMetrics(&metrics)
	for _, sample := range []string{
		`fugue_edge_dns_artifact_handler_lookups_total{outcome="hit"} 1.000000`,
		`fugue_edge_dns_artifact_handler_lookups_total{outcome="miss"} 0.000000`,
		`fugue_edge_dns_artifact_handler_lookups_total{outcome="error"} 0.000000`,
		`fugue_edge_dns_artifact_handler_source_total{source="immutable_full"} 1.000000`,
	} {
		if !strings.Contains(metrics.String(), sample) {
			t.Fatalf("expected metric sample %q, got:\n%s", sample, metrics.String())
		}
	}
	for _, retired := range []string{"legacy_derivations", "legacy_writes", "parity_mismatches", "legacy_fallback"} {
		if strings.Contains(metrics.String(), retired) {
			t.Fatalf("retired compatibility metric %q remains:\n%s", retired, metrics.String())
		}
	}
}

func TestEdgeDNSBundleRejectsArtifactMissWithoutDerive(t *testing.T) {
	t.Parallel()

	_, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&answer_ip=203.0.113.10", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "current verified LKG") {
		t.Fatalf("expected explicit LKG retention response, got %s", recorder.Body.String())
	}

	var metrics strings.Builder
	server.writeEdgeDNSArtifactMetrics(&metrics)
	for _, sample := range []string{
		`fugue_edge_dns_artifact_handler_lookups_total{outcome="hit"} 0.000000`,
		`fugue_edge_dns_artifact_handler_lookups_total{outcome="miss"} 1.000000`,
		`fugue_edge_dns_artifact_handler_lookups_total{outcome="error"} 0.000000`,
	} {
		if !strings.Contains(metrics.String(), sample) {
			t.Fatalf("expected metric sample %q, got:\n%s", sample, metrics.String())
		}
	}
}

func TestEdgeDNSBundleRejectsInvalidSignedArtifact(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	now := time.Now().UTC()
	options := edgeDNSBundleOptions{
		DNSNodeID:   "dns-us-1",
		EdgeGroupID: "edge-group-country-us",
		Zone:        "fugue.pro",
		AnswerIPs:   []string{"203.0.113.10"},
		TTL:         60,
	}
	bundle := signEdgeDNSBundle(model.EdgeDNSBundle{
		Version:     "dnsgen_invalid_artifact",
		Generation:  "dnsgen_invalid_artifact",
		GeneratedAt: now,
		DNSNodeID:   options.DNSNodeID,
		EdgeGroupID: options.EdgeGroupID,
		Zone:        options.Zone,
		Records: []model.EdgeDNSRecord{{
			Name:       "artifact.fugue.pro",
			Type:       model.EdgeDNSRecordTypeA,
			Values:     []string{"203.0.113.10"},
			TTL:        60,
			RecordKind: model.EdgeDNSRecordKindProbe,
			Status:     model.EdgeRouteStatusActive,
		}},
	}, server.bundleKeyring(), 10*time.Minute)
	bundle.Records[0].Values = []string{"198.51.100.99"}
	publishFullEdgeDNSArtifactForTest(t, storeState, server, newEdgeDNSBundleArtifact(options, bundle, now))

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&dns_node_id=dns-us-1&zone=fugue.pro&edge_group_id=edge-group-country-us&answer_ip=203.0.113.10&ttl=60", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	}
	var metrics strings.Builder
	server.writeEdgeDNSArtifactMetrics(&metrics)
	if !strings.Contains(metrics.String(), `fugue_edge_dns_artifact_handler_lookups_total{outcome="error"} 1.000000`) {
		t.Fatalf("expected invalid artifact lookup error metric, got:\n%s", metrics.String())
	}
}

func TestEdgeDNSBundleRejectsInvalidImmutableFullRelease(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	now := time.Now().UTC()
	options := edgeDNSBundleOptions{
		DNSNodeID: "dns-us-1", EdgeGroupID: "edge-group-country-us", Zone: "fugue.pro",
		AnswerIPs: []string{"203.0.113.10"}, TTL: 60,
	}
	bundle := signEdgeDNSBundle(model.EdgeDNSBundle{
		Version: "dnsgen_valid_full", Generation: "dnsenv_valid_full", GeneratedAt: now,
		DNSNodeID: options.DNSNodeID, EdgeGroupID: options.EdgeGroupID, Zone: options.Zone,
		Records: []model.EdgeDNSRecord{{
			Name: "artifact.fugue.pro", Type: model.EdgeDNSRecordTypeA, Values: []string{"203.0.113.10"},
			TTL: 60, RecordKind: model.EdgeDNSRecordKindProbe, Status: model.EdgeRouteStatusActive,
		}},
	}, server.bundleKeyring(), 10*time.Minute)
	publishFullEdgeDNSArtifactForTest(t, storeState, server, newEdgeDNSBundleArtifact(options, bundle, now))
	artifact, release, found, err := storeState.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle,
		edgeDNSBundleArtifactScopeKey(options),
		model.PlatformArtifactReleaseChannelFull,
	)
	if err != nil || !found {
		t.Fatalf("load immutable full fixture: found=%t err=%v", found, err)
	}
	if err := server.validateEdgeDNSFullRelease(artifact, release); err != nil {
		t.Fatalf("expected valid immutable full fixture: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*model.PlatformArtifactRelease)
	}{
		{name: "wrong channel", mutate: func(got *model.PlatformArtifactRelease) {
			got.ReleaseChannel = model.PlatformArtifactReleaseChannelShadow
		}},
		{name: "missing pinned rollback", mutate: func(got *model.PlatformArtifactRelease) {
			got.PinnedRollbackGeneration = ""
		}},
		{name: "unsupported verification state", mutate: func(got *model.PlatformArtifactRelease) {
			got.VerificationState = "unknown"
		}},
		{name: "bypassed invariant", mutate: func(got *model.PlatformArtifactRelease) {
			got.BypassedInvariants = []string{"artifact.signature_valid"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := release
			test.mutate(&invalid)
			if err := server.validateEdgeDNSFullRelease(artifact, invalid); err == nil {
				t.Fatal("expected invalid immutable full release to be rejected")
			}
		})
	}
}

func publishFullEdgeDNSArtifactForTest(t *testing.T, storeState *store.Store, server *Server, projection edgeDNSBundleArtifact) {
	t.Helper()
	content, err := edgeDNSImmutableArtifactContentMap(projection)
	if err != nil {
		t.Fatalf("encode immutable DNS fixture: %v", err)
	}
	generation, err := edgeDNSImmutablePlatformGeneration(content)
	if err != nil {
		t.Fatalf("derive immutable DNS fixture generation: %v", err)
	}
	artifact, _, err := storeState.EnsurePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle,
		Scope: model.PlatformArtifactScope{
			ScopeType: "dns-node", Key: projection.ScopeKey, NodeID: projection.DNSNodeID, EdgeGroupID: projection.EdgeGroupID,
		},
		Generation: generation, Content: content, CompatibilityFloor: model.PlatformArtifactSchemaVersionV1,
		CreatedByType: model.ActorTypeSystem, CreatedByID: "edge-dns-handler-test", CreatedAt: projection.GeneratedAt,
	})
	if err != nil {
		t.Fatalf("ensure immutable DNS fixture: %v", err)
	}
	artifact, err = storeState.ValidatePlatformArtifact(artifact.ID, []model.PlatformArtifactValidationResult{{
		Name: "edge_dns_bundle_integrity", Pass: true, Severity: model.RobustnessSeverityInfo,
	}})
	if err != nil {
		t.Fatalf("validate immutable DNS fixture: %v", err)
	}
	lkg, err := storeState.GetPlatformLKG(model.PlatformArtifactKindDNSAnswerBundle, projection.ScopeKey)
	if err != nil {
		t.Fatalf("load immutable DNS rollback fixture: %v", err)
	}
	if lkg == nil {
		_, shadow, _, _, releaseErr := storeState.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
			ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
			Reason:         "seed handler test rollback generation",
			IdempotencyKey: "edge-dns-handler-test-shadow:" + generation,
		}, model.Principal{ActorType: model.ActorTypeSystem, ActorID: "edge-dns-handler-test"})
		if releaseErr != nil {
			t.Fatalf("release immutable DNS shadow fixture: %v", releaseErr)
		}
		_, _, _, lkg, err = storeState.VerifyPlatformArtifactReleaseLKG(shadow.ID, model.PlatformArtifactVerifyLKGRequest{
			FencingToken: shadow.FencingToken, Reason: "handler test observed signed fixture", AllowInitialLKG: true,
			Evidence: model.PlatformArtifactVerificationEvidence{
				ConsumerConvergence: true, LocalProbe: true, PlatformEvidence: true, WatchWindow: true,
				BaselineMonotonic: true, DatabaseRollbackCompatible: true,
				ExpectedConsumerSetID: "dns-node:" + projection.DNSNodeID,
				EvidenceRefs:          []string{"handler-test:" + generation},
			},
		}, model.Principal{ActorType: model.ActorTypeSystem, ActorID: "edge-dns-handler-test"})
		if err != nil || lkg == nil {
			t.Fatalf("verify immutable DNS rollback fixture: lkg=%+v err=%v", lkg, err)
		}
	}
	if err := server.releaseFullEdgeDNSBundleArtifact(artifact); err != nil {
		t.Fatalf("release immutable DNS full fixture: %v", err)
	}
}

func TestEdgeDNSArtifactPublisherProjectsMultipleNodesFromOneSnapshot(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	for _, node := range []model.DNSNode{
		{
			ID: "dns-us-1", EdgeGroupID: "edge-group-country-us", Zone: "fugue.pro",
			PublicIPv4: "203.0.113.10", Status: model.EdgeHealthHealthy, Healthy: true,
		},
		{
			ID: "dns-de-1", EdgeGroupID: "edge-group-country-de", Zone: "fugue.pro",
			PublicIPv4: "203.0.113.20", Status: model.EdgeHealthHealthy, Healthy: true,
		},
	} {
		if _, err := storeState.UpdateDNSHeartbeat(node); err != nil {
			t.Fatalf("record DNS heartbeat for %s: %v", node.ID, err)
		}
	}

	now := time.Now().UTC()
	server.runEdgeDNSArtifactController(context.Background(), now)
	server.edgeDNSArtifactMu.Lock()
	artifactCount := server.edgeDNSArtifactLastCount
	snapshotCount := server.edgeDNSArtifactLastSourceSnapshots
	projectionCount := server.edgeDNSArtifactLastNodeProjections
	routeCompilationCount := server.edgeDNSArtifactLastRouteCompilations
	immutableWriteCount := server.edgeDNSArtifactLastImmutableWrites
	shadowActiveCount := server.edgeDNSArtifactLastShadowActive
	lastError := server.edgeDNSArtifactLastError
	server.edgeDNSArtifactMu.Unlock()
	if lastError != "" {
		t.Fatalf("publish node projections: %s", lastError)
	}
	if artifactCount != 2 || snapshotCount != 1 || projectionCount != 2 {
		t.Fatalf("expected one snapshot and two projections; artifacts=%d snapshots=%d projections=%d", artifactCount, snapshotCount, projectionCount)
	}
	if routeCompilationCount != 1 {
		t.Fatalf("expected shared TrafficEpoch route binding to compile once, got %d", routeCompilationCount)
	}
	if immutableWriteCount != 2 || shadowActiveCount != 2 {
		t.Fatalf("expected two immutable shadow writes; immutable=%d shadow=%d", immutableWriteCount, shadowActiveCount)
	}

	var artifacts []edgeDNSBundleArtifact
	for _, options := range []edgeDNSBundleOptions{
		{DNSNodeID: "dns-us-1", EdgeGroupID: "edge-group-country-us", Zone: "fugue.pro", AnswerIPs: []string{"203.0.113.10"}, TTL: defaultEdgeDNSTTL},
		{DNSNodeID: "dns-de-1", EdgeGroupID: "edge-group-country-de", Zone: "fugue.pro", AnswerIPs: []string{"203.0.113.20"}, TTL: defaultEdgeDNSTTL},
	} {
		platformArtifact, release, found, err := storeState.GetActivePlatformArtifact(
			model.PlatformArtifactKindDNSAnswerBundle,
			edgeDNSBundleArtifactScopeKey(options),
			model.PlatformArtifactReleaseChannelShadow,
		)
		if err != nil {
			t.Fatalf("load immutable shadow artifact for %s: %v", options.DNSNodeID, err)
		}
		if !found || platformArtifact.Status != model.PlatformArtifactStatusValidated || release.Status != model.PlatformArtifactReleaseStatusActive {
			t.Fatalf("expected active validated immutable shadow for %s, found=%t artifact=%+v release=%+v", options.DNSNodeID, found, platformArtifact, release)
		}
		projected, err := edgeDNSBundleArtifactFromPlatformArtifact(platformArtifact)
		if err != nil {
			t.Fatalf("decode immutable shadow artifact for %s: %v", options.DNSNodeID, err)
		}
		artifacts = append(artifacts, projected)
	}
	canonicalNow := canonicalEdgeDNSArtifactTime(now)
	if !artifacts[0].GeneratedAt.Equal(canonicalNow) || !artifacts[1].GeneratedAt.Equal(canonicalNow) {
		t.Fatalf("expected projections to share canonical generation time %s, got %s and %s", canonicalNow, artifacts[0].GeneratedAt, artifacts[1].GeneratedAt)
	}
	if artifacts[0].DNSNodeID == artifacts[1].DNSNodeID || artifacts[0].ScopeKey == artifacts[1].ScopeKey || artifacts[0].Bundle.Signature == artifacts[1].Bundle.Signature {
		t.Fatalf("expected node-scoped artifact identities and signatures to remain isolated: %+v %+v", artifacts[0], artifacts[1])
	}

	var metrics strings.Builder
	server.writeEdgeDNSArtifactMetrics(&metrics)
	for _, sample := range []string{
		`fugue_edge_dns_artifact_last_source_snapshots 1.000000`,
		`fugue_edge_dns_artifact_last_node_projections 2.000000`,
		`fugue_edge_dns_artifact_last_route_compilations 1.000000`,
		`fugue_edge_dns_artifact_last_immutable_writes 2.000000`,
		`fugue_edge_dns_artifact_last_shadow_active 2.000000`,
		`fugue_edge_dns_artifact_last_verified_lkg 0.000000`,
		`fugue_edge_dns_artifact_last_full_active 0.000000`,
		`fugue_edge_dns_artifact_last_verification_deferred 0.000000`,
	} {
		if !strings.Contains(metrics.String(), sample) {
			t.Fatalf("expected metric sample %q, got:\n%s", sample, metrics.String())
		}
	}
	for _, retired := range []string{"legacy_derivations", "legacy_writes", "parity_mismatches"} {
		if strings.Contains(metrics.String(), retired) {
			t.Fatalf("retired publication metric %q remains:\n%s", retired, metrics.String())
		}
	}
}

func TestEdgeDNSArtifactPublisherRejectsInvalidBundleBeforeReplacingImmutableCurrent(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	node := model.DNSNode{
		ID: "dns-us-1", EdgeGroupID: "edge-group-country-us", Zone: "fugue.pro",
		PublicIPv4: "203.0.113.10", Status: model.EdgeHealthHealthy, Healthy: true,
	}
	if _, err := storeState.UpdateDNSHeartbeat(node); err != nil {
		t.Fatalf("record DNS heartbeat: %v", err)
	}
	now := time.Now().UTC()
	server.runEdgeDNSArtifactController(context.Background(), now)
	options, ok := server.edgeDNSBundleOptionsForDNSNode(node)
	if !ok {
		t.Fatal("expected DNS node publication options")
	}
	scopeKey := edgeDNSBundleArtifactScopeKey(options)
	platformBefore, releaseBefore, found, err := storeState.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle,
		scopeKey,
		model.PlatformArtifactReleaseChannelShadow,
	)
	if err != nil || !found {
		t.Fatalf("load immutable current before invalid publication: found=%t err=%v", found, err)
	}
	before, err := edgeDNSBundleArtifactFromPlatformArtifact(platformBefore)
	if err != nil {
		t.Fatalf("decode immutable current before invalid publication: %v", err)
	}
	before.ActivatedAt = releaseBefore.ReleasedAt
	before.UpdatedAt = releaseBefore.UpdatedAt
	if _, err := server.publishEdgeDNSBundleArtifact(before, options, now); err != nil {
		t.Fatalf("retry identical generation: %v", err)
	}
	_, retryRelease, found, err := storeState.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle,
		scopeKey,
		model.PlatformArtifactReleaseChannelShadow,
	)
	if err != nil || !found || retryRelease.ID != releaseBefore.ID || retryRelease.FencingToken != releaseBefore.FencingToken {
		t.Fatalf("identical generation was not idempotent: found=%t err=%v before=%+v after=%+v", found, err, releaseBefore, retryRelease)
	}

	invalid := before
	encoded, err := json.Marshal(before)
	if err != nil {
		t.Fatalf("clone immutable current: %v", err)
	}
	if err := json.Unmarshal(encoded, &invalid); err != nil {
		t.Fatalf("decode immutable current clone: %v", err)
	}
	invalid.Bundle.Records[0].Values = []string{"198.51.100.99"}
	if _, err := server.publishEdgeDNSBundleArtifact(invalid, options, now.Add(time.Second)); err == nil {
		t.Fatal("expected tampered signed bundle publication to fail")
	}
	platformAfter, releaseAfter, found, err := storeState.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle,
		scopeKey,
		model.PlatformArtifactReleaseChannelShadow,
	)
	if err != nil || !found {
		t.Fatalf("load immutable current after invalid publication: found=%t err=%v", found, err)
	}
	if platformAfter.ID != platformBefore.ID || releaseAfter.ID != releaseBefore.ID {
		t.Fatalf("invalid bundle replaced immutable current: before=%s/%s after=%s/%s", platformBefore.ID, releaseBefore.ID, platformAfter.ID, releaseAfter.ID)
	}
}

func TestEdgeDNSArtifactPublisherRefreshesImmutableEnvelopeForStableBundleVersion(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	node := model.DNSNode{
		ID: "dns-us-1", EdgeGroupID: "edge-group-country-us", Zone: "fugue.pro",
		PublicIPv4: "203.0.113.10", Status: model.EdgeHealthHealthy, Healthy: true,
	}
	if _, err := storeState.UpdateDNSHeartbeat(node); err != nil {
		t.Fatalf("record DNS heartbeat: %v", err)
	}
	options, ok := server.edgeDNSBundleOptionsForDNSNode(node)
	if !ok {
		t.Fatal("expected DNS node publication options")
	}
	scopeKey := edgeDNSBundleArtifactScopeKey(options)
	now := time.Now().UTC()
	server.runEdgeDNSArtifactController(context.Background(), now)
	before, beforeRelease, found, err := storeState.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle,
		scopeKey,
		model.PlatformArtifactReleaseChannelShadow,
	)
	if err != nil || !found {
		t.Fatalf("load first immutable envelope: found=%t err=%v", found, err)
	}
	beforeContent, err := edgeDNSBundleArtifactFromPlatformArtifact(before)
	if err != nil {
		t.Fatalf("decode first immutable envelope: %v", err)
	}

	server.runEdgeDNSArtifactController(context.Background(), now.Add(time.Minute))
	server.edgeDNSArtifactMu.Lock()
	lastError := server.edgeDNSArtifactLastError
	server.edgeDNSArtifactMu.Unlock()
	if lastError != "" {
		t.Fatalf("refresh stable semantic generation: %s", lastError)
	}
	after, afterRelease, found, err := storeState.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle,
		scopeKey,
		model.PlatformArtifactReleaseChannelShadow,
	)
	if err != nil || !found {
		t.Fatalf("load refreshed immutable envelope: found=%t err=%v", found, err)
	}
	afterContent, err := edgeDNSBundleArtifactFromPlatformArtifact(after)
	if err != nil {
		t.Fatalf("decode refreshed immutable envelope: %v", err)
	}
	if beforeContent.Version != afterContent.Version {
		t.Fatalf("expected stable semantic bundle version, before=%s after=%s", beforeContent.Version, afterContent.Version)
	}
	if beforeContent.Bundle.Generation == afterContent.Bundle.Generation {
		t.Fatalf("expected refreshed signed envelopes to have distinct exact generations, got %s", beforeContent.Bundle.Generation)
	}
	if before.Generation == after.Generation || beforeRelease.ID == afterRelease.ID || !afterContent.GeneratedAt.After(beforeContent.GeneratedAt) {
		t.Fatalf("expected refreshed envelope to get a new content generation and current pointer: before=%s/%s after=%s/%s", before.Generation, beforeRelease.ID, after.Generation, afterRelease.ID)
	}
}

func TestEdgeDNSArtifactPublisherPromotesObservedImmutableEnvelopeToFull(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	node := model.DNSNode{
		ID: "dns-us-1", EdgeGroupID: "edge-group-country-us", Zone: "fugue.pro",
		PublicIPv4: "203.0.113.10", Status: model.EdgeHealthHealthy, Healthy: true,
		CacheStatus: "ready", UDPListen: true, TCPListen: true,
	}
	if _, err := storeState.UpdateDNSHeartbeat(node); err != nil {
		t.Fatalf("record initial DNS heartbeat: %v", err)
	}
	now := time.Now().UTC()
	server.runEdgeDNSArtifactController(context.Background(), now)
	options, ok := server.edgeDNSBundleOptionsForDNSNode(node)
	if !ok {
		t.Fatal("expected DNS node publication options")
	}
	scopeKey := edgeDNSBundleArtifactScopeKey(options)
	firstArtifact, _, found, err := storeState.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle, scopeKey, model.PlatformArtifactReleaseChannelShadow,
	)
	if err != nil || !found {
		t.Fatalf("load first immutable current: found=%t err=%v", found, err)
	}
	firstProjection, err := edgeDNSBundleArtifactFromPlatformArtifact(firstArtifact)
	if err != nil {
		t.Fatalf("decode first immutable current: %v", err)
	}
	if firstProjection.Bundle.Generation == firstProjection.Bundle.Version || !strings.HasPrefix(firstProjection.Bundle.Generation, "dnsenv_") {
		t.Fatalf("expected exact envelope generation distinct from semantic version, got version=%s generation=%s", firstProjection.Bundle.Version, firstProjection.Bundle.Generation)
	}
	if _, _, found, err := storeState.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle,
		scopeKey,
		model.PlatformArtifactReleaseChannelFull,
	); err != nil || found {
		t.Fatalf("full current must not exist before consumer convergence: found=%t err=%v", found, err)
	}

	node.DNSBundleVersion = firstProjection.Bundle.Version
	node.ServingGeneration = firstProjection.Bundle.Generation
	node.LKGGeneration = firstProjection.Bundle.Generation
	node.RecordCount = len(firstProjection.Bundle.Records)
	if _, err := storeState.UpdateDNSHeartbeat(node); err != nil {
		t.Fatalf("record converged DNS heartbeat: %v", err)
	}
	server.runEdgeDNSArtifactController(context.Background(), time.Now().UTC())
	server.edgeDNSArtifactMu.Lock()
	verifiedLKGCount := server.edgeDNSArtifactLastVerifiedLKG
	fullActiveCount := server.edgeDNSArtifactLastFullActive
	verificationDeferredCount := server.edgeDNSArtifactLastVerifyDeferred
	lastError := server.edgeDNSArtifactLastError
	server.edgeDNSArtifactMu.Unlock()
	if lastError != "" {
		t.Fatalf("promote observed immutable envelope: %s", lastError)
	}
	if verifiedLKGCount != 1 || fullActiveCount != 1 || verificationDeferredCount != 0 {
		t.Fatalf("expected one verified LKG and full current with no deferred verification; lkg=%d full=%d deferred=%d", verifiedLKGCount, fullActiveCount, verificationDeferredCount)
	}
	verifiedLKG, err := storeState.GetPlatformLKG(model.PlatformArtifactKindDNSAnswerBundle, scopeKey)
	if err != nil || verifiedLKG == nil {
		t.Fatalf("load verified immutable LKG: lkg=%+v err=%v", verifiedLKG, err)
	}
	firstShadow, firstShadowRelease, found, err := storeState.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle,
		scopeKey,
		model.PlatformArtifactReleaseChannelShadow,
	)
	if err != nil || !found {
		t.Fatalf("load refreshed shadow current: found=%t err=%v", found, err)
	}
	if verifiedLKG.Generation == firstShadow.Generation || firstShadowRelease.VerificationState != model.PlatformArtifactVerificationStateServingUnverified {
		t.Fatalf("new shadow must remain unverified while the prior observed envelope is LKG: lkg=%+v shadow=%+v release=%+v", verifiedLKG, firstShadow, firstShadowRelease)
	}
	full, fullRelease, found, err := storeState.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle,
		scopeKey,
		model.PlatformArtifactReleaseChannelFull,
	)
	if err != nil || !found {
		t.Fatalf("load immutable full current: found=%t err=%v", found, err)
	}
	if full.ID != firstShadow.ID || fullRelease.VerificationState != model.PlatformArtifactVerificationStateServingUnverified || fullRelease.PinnedRollbackGeneration != verifiedLKG.Generation {
		t.Fatalf("full current did not pin the verified predecessor: full=%+v release=%+v lkg=%+v", full, fullRelease, verifiedLKG)
	}

	secondProjection, err := edgeDNSBundleArtifactFromPlatformArtifact(full)
	if err != nil {
		t.Fatalf("decode immutable full projection: %v", err)
	}
	node.DNSBundleVersion = secondProjection.Bundle.Version
	node.ServingGeneration = secondProjection.Bundle.Generation
	node.LKGGeneration = secondProjection.Bundle.Generation
	node.RecordCount = len(secondProjection.Bundle.Records)
	if _, err := storeState.UpdateDNSHeartbeat(node); err != nil {
		t.Fatalf("record full-current DNS heartbeat: %v", err)
	}
	server.runEdgeDNSArtifactController(context.Background(), time.Now().UTC())
	advancedLKG, err := storeState.GetPlatformLKG(model.PlatformArtifactKindDNSAnswerBundle, scopeKey)
	if err != nil || advancedLKG == nil {
		t.Fatalf("load advanced immutable LKG: lkg=%+v err=%v", advancedLKG, err)
	}
	if advancedLKG.ArtifactID != full.ID || advancedLKG.Generation != full.Generation {
		t.Fatalf("observed full current did not become the next verified LKG: full=%+v lkg=%+v", full, advancedLKG)
	}
}

func TestEdgeDNSArtifactPublisherDoesNotSeedLKGFromAmbiguousLegacyGeneration(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	node := model.DNSNode{
		ID: "dns-us-1", EdgeGroupID: "edge-group-country-us", Zone: "fugue.pro",
		PublicIPv4: "203.0.113.10", Status: model.EdgeHealthHealthy, Healthy: true,
		CacheStatus: "ready", UDPListen: true, TCPListen: true,
	}
	if _, err := storeState.UpdateDNSHeartbeat(node); err != nil {
		t.Fatalf("record initial DNS heartbeat: %v", err)
	}
	now := time.Now().UTC()
	server.runEdgeDNSArtifactController(context.Background(), now)
	options, ok := server.edgeDNSBundleOptionsForDNSNode(node)
	if !ok {
		t.Fatal("expected DNS node publication options")
	}
	scopeKey := edgeDNSBundleArtifactScopeKey(options)
	shadow, _, found, err := storeState.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle, scopeKey, model.PlatformArtifactReleaseChannelShadow,
	)
	if err != nil || !found {
		t.Fatalf("load exact immutable current: found=%t err=%v", found, err)
	}
	legacy, err := edgeDNSBundleArtifactFromPlatformArtifact(shadow)
	if err != nil {
		t.Fatalf("decode exact immutable current: %v", err)
	}
	legacy.ActivatedAt = time.Now().UTC()
	legacy.UpdatedAt = legacy.ActivatedAt
	legacy.Bundle.Generation = legacy.Bundle.Version
	legacy.Bundle = signEdgeDNSBundle(legacy.Bundle, server.bundleKeyring(), server.discoveryBundleTTL())
	legacy.ValidUntil = legacy.Bundle.ValidUntil
	if _, err := server.publishEdgeDNSBundleArtifact(legacy, options, now); err != nil {
		t.Fatalf("publish legacy-generation migration fixture: %v", err)
	}
	node.DNSBundleVersion = legacy.Bundle.Version
	node.ServingGeneration = legacy.Bundle.Generation
	node.LKGGeneration = legacy.Bundle.Generation
	node.RecordCount = len(legacy.Bundle.Records)
	if _, err := storeState.UpdateDNSHeartbeat(node); err != nil {
		t.Fatalf("record ambiguous legacy-generation heartbeat: %v", err)
	}
	reconciliation, err := server.reconcileEdgeDNSArtifactRelease(node, options, time.Now().UTC())
	if err != nil {
		t.Fatalf("reconcile ambiguous legacy generation: %v", err)
	}
	if reconciliation.VerifiedLKG || reconciliation.ReadyForFull || !reconciliation.VerificationDeferred {
		t.Fatalf("ambiguous legacy generation must remain shadow-only: %+v", reconciliation)
	}
	if lkg, err := storeState.GetPlatformLKG(model.PlatformArtifactKindDNSAnswerBundle, scopeKey); err != nil || lkg != nil {
		t.Fatalf("ambiguous legacy generation seeded an LKG: lkg=%+v err=%v", lkg, err)
	}
}

func TestEdgeDNSArtifactPublisherSkipsWhenAdvisoryLockIsHeld(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := storeState.WithAdvisoryLock(context.Background(), edgeDNSArtifactControllerLockName, func() error {
			close(locked)
			<-release
			return nil
		})
		done <- err
	}()
	<-locked

	server.runEdgeDNSArtifactController(context.Background(), time.Now().UTC())
	server.edgeDNSArtifactMu.Lock()
	skippedCount := server.edgeDNSArtifactSkippedCount
	runCount := server.edgeDNSArtifactRunCount
	server.edgeDNSArtifactMu.Unlock()
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("hold advisory lock: %v", err)
	}
	if skippedCount != 1 || runCount != 0 {
		t.Fatalf("expected lock contention to skip without running publisher, skipped=%d runs=%d", skippedCount, runCount)
	}
}

func TestEdgeDNSStaticAnswerIPsDoNotBecomeBusinessCandidatesWithoutEvidence(t *testing.T) {
	t.Parallel()

	server := &Server{}
	options := edgeDNSBundleOptions{
		EdgeGroupID: "edge-group-country-us",
		Zone:        "fugue.pro",
		AnswerIPs:   []string{"203.0.113.10"},
	}
	now := time.Now().UTC()

	byGroup, err := server.edgeDNSAnswerIPsByGroup(context.Background())
	if err != nil {
		t.Fatalf("build answer IPs by group: %v", err)
	}
	if len(byGroup) != 0 {
		t.Fatalf("expected no business answer IPs without inventory and route evidence, got %+v", byGroup)
	}

	candidates, err := server.edgeDNSAnswerCandidateByIP(context.Background(), options, now)
	if err != nil {
		t.Fatalf("build answer candidates: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("expected no business candidates without inventory and route evidence, got %+v", candidates)
	}
}

func TestEdgeDNSBundlePublishesCustomDomainTargetsBeforeVerification(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	recordHealthyEdgeForRouteTest(t, storeState, "edge-default-1", defaultEdgeGroupID, "203.0.113.20")

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&answer_ip=203.0.113.10&ttl=120", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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

func TestEdgeDNSBundlePublishesHostedZoneRecords(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	zone := putHostedDNSZoneForEdgeDNSTest(t, storeState, app.TenantID, "example.net")
	putHostedDNSRecordForEdgeDNSTest(t, storeState, zone, model.DNSRecord{
		Name:   "www",
		Type:   model.DNSRecordTypeA,
		Values: []string{"203.0.113.44"},
		TTL:    120,
		Source: model.DNSRecordSourceUser,
		Status: model.DNSRecordStatusActive,
	})
	putHostedDNSRecordForEdgeDNSTest(t, storeState, zone, model.DNSRecord{
		Name:   "mail",
		Type:   model.DNSRecordTypeMX,
		Values: []string{"10 mailhost.example.net"},
		TTL:    300,
		Source: model.DNSRecordSourceUser,
		Status: model.DNSRecordStatusActive,
	})

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=example.net&answer_ip=203.0.113.10&ttl=120", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	a := edgeDNSRecordByNameAndType(bundle.Records, "www.example.net", model.EdgeDNSRecordTypeA)
	if a == nil || a.RecordKind != model.EdgeDNSRecordKindHosted || strings.Join(a.Values, ",") != "203.0.113.44" {
		t.Fatalf("expected hosted www A record, got %+v in %+v", a, bundle.Records)
	}
	mx := edgeDNSRecordByNameAndType(bundle.Records, "mail.example.net", model.EdgeDNSRecordTypeMX)
	if mx == nil || mx.RecordKind != model.EdgeDNSRecordKindHosted || strings.Join(mx.Values, ",") != "10 mailhost.example.net" {
		t.Fatalf("expected hosted mail MX record, got %+v in %+v", mx, bundle.Records)
	}
	if edgeDNSRecordByNameAndType(bundle.Records, "www.fugue.pro", model.EdgeDNSRecordTypeA) != nil {
		t.Fatalf("hosted records from example.net must not leak into other zones: %+v", bundle.Records)
	}
}

func TestEdgeDNSBundlePublishesDynamicHostedZoneInventory(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	putZone := func(name, status string) {
		t.Helper()
		now := time.Now().UTC()
		if _, err := storeState.PutHostedZone(model.HostedZone{
			TenantID:            app.TenantID,
			ZoneName:            name,
			Status:              status,
			DelegationStatus:    model.HostedZoneDelegationStatusPending,
			ExpectedNameservers: []string{"ns1.dns.fugue.pro", "ns2.dns.fugue.pro"},
			CreatedAt:           now,
			UpdatedAt:           now,
		}); err != nil {
			t.Fatalf("put hosted DNS zone %s: %v", name, err)
		}
	}
	putZone("pending.example", model.HostedZoneStatusPendingDelegation)
	putZone("active.example", model.HostedZoneStatusActive)
	putZone("degraded.example", model.HostedZoneStatusDegraded)
	putZone("suspended.example", model.HostedZoneStatusSuspended)

	fetchBundle := func() model.EdgeDNSBundle {
		t.Helper()
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&answer_ip=203.0.113.10", nil)
		serveEdgeDNSBundleRequest(t, server, recorder, req)
		if recorder.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
		}
		var bundle model.EdgeDNSBundle
		mustDecodeJSON(t, recorder, &bundle)
		return bundle
	}

	first := fetchBundle()
	if got, want := strings.Join(first.HostedZones, ","), "active.example,degraded.example,pending.example"; got != want {
		t.Fatalf("unexpected publishable hosted zone inventory: got %q want %q", got, want)
	}
	putZone("later.example", model.HostedZoneStatusActive)
	second := fetchBundle()
	if first.Version == second.Version {
		t.Fatalf("expected hosted zone inventory change to update bundle version %q", first.Version)
	}
	if got, want := strings.Join(second.HostedZones, ","), "active.example,degraded.example,later.example,pending.example"; got != want {
		t.Fatalf("unexpected updated hosted zone inventory: got %q want %q", got, want)
	}
}

func TestEdgeDNSBundlePublishesHostedFlattenedRecords(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	now := time.Now().UTC()
	zone := putHostedDNSZoneForEdgeDNSTest(t, storeState, app.TenantID, "example.net")
	putHostedDNSRecordForEdgeDNSTest(t, storeState, zone, model.DNSRecord{
		Name:                  "@",
		Type:                  model.DNSRecordTypeALIAS,
		Values:                []string{"target.example.org"},
		TTL:                   180,
		FlattenMode:           model.DNSRecordFlattenModeAlways,
		FlattenTarget:         "target.example.org",
		FlattenFallbackPolicy: model.DNSRecordFlattenFallbackStaleIfError,
		FlattenStatus:         model.DNSRecordFlattenStatusResolved,
		FlattenedA:            []string{"198.51.100.20"},
		FlattenedAAAA:         []string{"2001:db8::20"},
		LastResolvedAt:        &now,
		Source:                model.DNSRecordSourceUser,
		Status:                model.DNSRecordStatusActive,
	})
	putHostedDNSRecordForEdgeDNSTest(t, storeState, zone, model.DNSRecord{
		Name:                  "closed",
		Type:                  model.DNSRecordTypeANAME,
		Values:                []string{"missing.example.org"},
		TTL:                   180,
		FlattenMode:           model.DNSRecordFlattenModeAlways,
		FlattenTarget:         "missing.example.org",
		FlattenFallbackPolicy: model.DNSRecordFlattenFallbackFailClosed,
		FlattenStatus:         model.DNSRecordFlattenStatusError,
		ResolveError:          "target lookup failed",
		Source:                model.DNSRecordSourceUser,
		Status:                model.DNSRecordStatusDegraded,
	})

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=example.net&answer_ip=203.0.113.10&ttl=120", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	a := edgeDNSRecordByNameAndType(bundle.Records, "example.net", model.EdgeDNSRecordTypeA)
	if a == nil || a.RecordKind != model.EdgeDNSRecordKindHosted || strings.Join(a.Values, ",") != "198.51.100.20" {
		t.Fatalf("expected flattened apex A record, got %+v in %+v", a, bundle.Records)
	}
	aaaa := edgeDNSRecordByNameAndType(bundle.Records, "example.net", model.EdgeDNSRecordTypeAAAA)
	if aaaa == nil || strings.Join(aaaa.Values, ",") != "2001:db8::20" {
		t.Fatalf("expected flattened apex AAAA record, got %+v in %+v", aaaa, bundle.Records)
	}
	if edgeDNSRecordByNameAndType(bundle.Records, "example.net", model.EdgeDNSRecordTypeCNAME) != nil {
		t.Fatalf("flattened hosted record must not publish CNAME: %+v", bundle.Records)
	}
	if edgeDNSRecordByNameAndType(bundle.Records, "closed.example.net", model.EdgeDNSRecordTypeA) != nil {
		t.Fatalf("fail-closed flattened record without cache must not publish: %+v", bundle.Records)
	}
}

func TestEdgeDNSBundleExpandsHostedFugueAppRecord(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	recordHealthyEdgeForRouteTest(t, storeState, "edge-default-1", defaultEdgeGroupID, "203.0.113.20")
	zone := putHostedDNSZoneForEdgeDNSTest(t, storeState, app.TenantID, "example.net")
	putHostedDNSRecordForEdgeDNSTest(t, storeState, zone, model.DNSRecord{
		Name:          "@",
		Type:          model.DNSRecordTypeFUGUEAPP,
		Values:        []string{app.ID},
		TTL:           90,
		FlattenMode:   model.DNSRecordFlattenModeApp,
		Source:        model.DNSRecordSourceAppDomain,
		SourceRefType: model.DNSRecordSourceRefTypeAppDomain,
		SourceRefID:   "example.net",
		Status:        model.DNSRecordStatusActive,
	})

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=example.net&answer_ip=203.0.113.10&ttl=120", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	record := edgeDNSRecordByNameAndType(bundle.Records, "example.net", model.EdgeDNSRecordTypeA)
	if record == nil {
		t.Fatalf("expected hosted FUGUE_APP apex A record in bundle: %+v", bundle.Records)
	}
	if record.RecordKind != model.EdgeDNSRecordKindCustomDomainTarget || record.AppID != app.ID || strings.Join(record.Values, ",") != "203.0.113.20" {
		t.Fatalf("unexpected hosted FUGUE_APP record: %+v", record)
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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
	serveEdgeDNSBundleRequest(t, server, first, req)
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
	serveEdgeDNSBundleRequest(t, server, repeated, repeatedReq)
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
	serveEdgeDNSBundleRequest(t, server, conditional, conditionalReq)
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
	serveEdgeDNSBundleRequest(t, server, changed, changedReq)
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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
	if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, model.EdgeNode{
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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

func TestEdgeDNSBundleDoesNotTreatDNSHeartbeatAsEdgeInventory(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	target := server.primaryCustomDomainTarget(app)
	now := time.Now().UTC()
	if _, err := storeState.PutAppDomain(model.AppDomain{
		Hostname: "service.example.com", AppID: app.ID, TenantID: app.TenantID,
		Status: model.AppDomainStatusVerified, DNSStatus: model.AppDomainDNSStatusReady,
		TLSStatus: model.AppDomainTLSStatusReady, RouteTarget: target, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("put verified app domain: %v", err)
	}
	if _, err := storeState.UpdateDNSHeartbeat(model.DNSNode{
		ID: "dns-us", PhysicalNodeID: "edge-us-1", EdgeGroupID: "edge-group-country-us", Zone: "fugue.pro",
		PublicIPv4: "198.51.100.20", Status: model.EdgeHealthHealthy, Healthy: true,
		DNSBundleVersion: "dnsgen-us", ServingGeneration: "dnsgen-us", LKGGeneration: "dnsgen-us", CacheStatus: "ready",
	}); err != nil {
		t.Fatalf("record DNS node heartbeat: %v", err)
	}
	byGroup, err := server.edgeDNSAnswerIPsByGroup(context.Background())
	if err != nil {
		t.Fatalf("build answer IPs by group: %v", err)
	}
	if len(byGroup) != 0 {
		t.Fatalf("DNS heartbeat must not create Edge group answer inventory: %+v", byGroup)
	}
	candidateByIP, err := server.edgeDNSAnswerCandidateByIP(context.Background(), edgeDNSBundleOptions{
		Zone: "fugue.pro", EdgeGroupID: "edge-group-country-us",
	}, now)
	if err != nil {
		t.Fatalf("build Edge answer candidates: %v", err)
	}
	if len(candidateByIP) != 0 {
		t.Fatalf("DNS heartbeat must not create Edge answer candidates: %+v", candidateByIP)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-us&answer_ip=198.51.100.20", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	record := edgeDNSRecordByNameAndType(bundle.Records, target, model.EdgeDNSRecordTypeA)
	if record == nil || !stringSliceContains(record.Values, "198.51.100.20") {
		t.Fatalf("expected configured answer IP to remain represented for answer-time fail-closed filtering: %+v", record)
	}
	if len(record.Candidates) != 1 || record.Candidates[0].EdgeID != "" || record.Candidates[0].EdgeGroupID != "" ||
		record.Candidates[0].RouteReady || record.Candidates[0].DNSEligible || record.Candidates[0].Reason == "dns_node_inventory" {
		t.Fatalf("DNS heartbeat must not synthesize a route-ready or DNS-eligible Edge candidate: %+v", record.Candidates)
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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
	if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, model.EdgeNode{
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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
	serveEdgeDNSBundleRequest(t, server, otherGroupRecorder, otherGroupReq)
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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

func TestEdgeDNSBundleExcludesPolicyBlockedEdgeNodeAnswers(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	recordHealthyEdgeForRouteTest(t, storeState, "edge-us-1", "edge-group-country-us", "15.204.94.71")
	recordHealthyEdgeForRouteTest(t, storeState, "edge-de-1", "edge-group-country-de", "51.38.126.103")

	put := performJSONRequest(t, server, http.MethodPut, "/v1/edge/route-policies/"+app.Route.Hostname, platformAdminKey, map[string]any{
		"route_policy":      model.EdgeRoutePolicyEnabled,
		"excluded_edge_ids": []string{"edge-de-1"},
		"exclusion_reason":  "edge DNS policy test",
	})
	if put.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, put.Code, put.Body.String())
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	record := edgeDNSRecordByNameAndType(bundle.Records, app.Route.Hostname, model.EdgeDNSRecordTypeA)
	if record == nil {
		t.Fatalf("expected app route DNS record for %s: %+v", app.Route.Hostname, bundle.Records)
	}
	if stringSliceContains(record.Values, "51.38.126.103") || !stringSliceContains(record.Values, "15.204.94.71") {
		t.Fatalf("expected DNS values to exclude DE edge and keep US edge, got %+v", record.Values)
	}
	for _, candidate := range record.Candidates {
		if candidate.EdgeID == "edge-de-1" || candidate.IP == "51.38.126.103" {
			t.Fatalf("expected candidates to exclude blocked edge node, got %+v", record.Candidates)
		}
	}
	if len(record.Candidates) != 1 || record.Candidates[0].EdgeID != "edge-us-1" {
		t.Fatalf("expected only US candidate after edge exclusion, got %+v", record.Candidates)
	}
}

func TestEdgeDNSBundleExcludesDrainingEdgeNodeAnswers(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	recordRouteReadyEdgeForDNSAnswerTest(t, storeState, "edge-us-1", "edge-group-country-us", "15.204.94.71", false)
	recordRouteReadyEdgeForDNSAnswerTest(t, storeState, "edge-de-1", "edge-group-country-de", "51.38.126.103", true)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	record := edgeDNSRecordByNameAndType(bundle.Records, app.Route.Hostname, model.EdgeDNSRecordTypeA)
	if record == nil {
		t.Fatalf("expected app route DNS record for %s: %+v", app.Route.Hostname, bundle.Records)
	}
	if stringSliceContains(record.Values, "51.38.126.103") || !stringSliceContains(record.Values, "15.204.94.71") {
		t.Fatalf("expected DNS values to exclude draining DE edge and keep US edge, got %+v", record.Values)
	}
	for _, candidate := range record.Candidates {
		if candidate.EdgeID == "edge-de-1" || candidate.IP == "51.38.126.103" {
			t.Fatalf("expected candidates to exclude draining edge node, got %+v", record.Candidates)
		}
	}
}

func TestEdgeDNSBundleKeepsObserveOnlyDeepHealthFailuresInAnswers(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	recordRouteReadyEdgeForDNSAnswerTest(t, storeState, "edge-us-1", "edge-group-country-us", "15.204.94.71", false)
	recordRouteReadyEdgeForDNSAnswerTest(t, storeState, "edge-de-1", "edge-group-country-de", "51.38.126.103", false)
	if _, err := storeState.RecordNodeDeepHealthResult(model.NodeDeepHealthResult{
		NodeUpdaterID: "edge-de-1",
		Checks: []model.NodeDeepHealthCheck{{
			Name:     model.NodeDeepHealthCheckPodDNSToKubeDNSService,
			Status:   model.NodeDeepHealthStatusFail,
			HardFail: true,
		}},
		ReportedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("record quarantined edge node health: %v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	record := edgeDNSRecordByNameAndType(bundle.Records, app.Route.Hostname, model.EdgeDNSRecordTypeA)
	if record == nil {
		t.Fatalf("expected app route DNS record for %s: %+v", app.Route.Hostname, bundle.Records)
	}
	if !stringSliceContains(record.Values, "51.38.126.103") || !stringSliceContains(record.Values, "15.204.94.71") {
		t.Fatalf("expected observed-only deep health failure to keep both edge answers, got %+v", record.Values)
	}
	seenDE := false
	for _, candidate := range record.Candidates {
		if candidate.EdgeID == "edge-de-1" || candidate.IP == "51.38.126.103" {
			seenDE = true
		}
	}
	if !seenDE {
		t.Fatalf("expected observed-only deep health failure to keep DE candidate, got %+v", record.Candidates)
	}
}

func TestEdgeDNSBundleExcludesInvalidLKGEdgeNodeAnswers(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	recordRouteReadyEdgeForDNSAnswerTest(t, storeState, "edge-us-1", "edge-group-country-us", "15.204.94.71", false)
	if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, model.EdgeNode{
		ID:                "edge-de-1",
		EdgeGroupID:       "edge-group-country-de",
		PublicIPv4:        "51.38.126.103",
		Status:            model.EdgeHealthHealthy,
		Healthy:           true,
		CaddyRouteCount:   1,
		TLSStatus:         model.EdgeTLSStatusReady,
		ServingGeneration: "routegen_bad",
		LKGGeneration:     "routegen_bad",
		CacheStatus:       "cache-error",
	}); err != nil {
		t.Fatalf("record cache-invalid edge node: %v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	record := edgeDNSRecordByNameAndType(bundle.Records, app.Route.Hostname, model.EdgeDNSRecordTypeA)
	if record == nil {
		t.Fatalf("expected app route DNS record for %s: %+v", app.Route.Hostname, bundle.Records)
	}
	if stringSliceContains(record.Values, "51.38.126.103") || !stringSliceContains(record.Values, "15.204.94.71") {
		t.Fatalf("expected DNS values to exclude cache-invalid DE edge and keep US edge, got %+v", record.Values)
	}
	for _, candidate := range record.Candidates {
		if candidate.EdgeID == "edge-de-1" || candidate.IP == "51.38.126.103" {
			t.Fatalf("expected candidates to exclude cache-invalid edge node, got %+v", record.Candidates)
		}
	}
}

func recordRouteReadyEdgeForDNSAnswerTest(t *testing.T, storeState edgeRouteHeartbeatStore, id, groupID, publicIPv4 string, draining bool) {
	t.Helper()
	if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, model.EdgeNode{
		ID:              id,
		EdgeGroupID:     groupID,
		PublicIPv4:      publicIPv4,
		Status:          model.EdgeHealthHealthy,
		Healthy:         true,
		Draining:        draining,
		CaddyRouteCount: 1,
		TLSStatus:       model.EdgeTLSStatusReady,
	}); err != nil {
		t.Fatalf("record route-ready edge node: %v", err)
	}
}

func TestEdgeDNSBundleSharedCustomDomainTargetKeepsStrictestEdgeExclusions(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	target := server.primaryCustomDomainTarget(app)
	now := time.Date(2026, 6, 22, 6, 0, 0, 0, time.UTC)
	for _, hostname := range []string{"shared-api.example.net", "shared-web.example.net"} {
		if _, err := storeState.PutAppDomain(model.AppDomain{
			Hostname:    hostname,
			AppID:       app.ID,
			TenantID:    app.TenantID,
			Status:      model.AppDomainStatusVerified,
			DNSStatus:   model.AppDomainDNSStatusReady,
			TLSStatus:   model.AppDomainTLSStatusReady,
			RouteTarget: target,
			CreatedAt:   now,
			UpdatedAt:   now,
		}); err != nil {
			t.Fatalf("put verified shared target domain %s: %v", hostname, err)
		}
	}
	recordHealthyEdgeForRouteTest(t, storeState, "edge-us-1", "edge-group-country-us", "15.204.94.71")
	recordHealthyEdgeForRouteTest(t, storeState, "edge-de-1", "edge-group-country-de", "51.38.126.103")
	put := performJSONRequest(t, server, http.MethodPut, "/v1/edge/route-policies/shared-api.example.net", platformAdminKey, map[string]any{
		"route_policy":      model.EdgeRoutePolicyEnabled,
		"excluded_edge_ids": []string{"edge-de-1"},
		"exclusion_reason":  "shared-target-edge-exclusion",
	})
	if put.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, put.Code, put.Body.String())
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	record := edgeDNSRecordByNameAndType(bundle.Records, target, model.EdgeDNSRecordTypeA)
	if record == nil {
		t.Fatalf("expected shared custom-domain target %s: %+v", target, bundle.Records)
	}
	if stringSliceContains(record.Values, "51.38.126.103") || !stringSliceContains(record.Values, "15.204.94.71") {
		t.Fatalf("expected shared target values to keep only edges valid for every hostname, got %+v", record.Values)
	}
	for _, candidate := range record.Candidates {
		if candidate.EdgeID == "edge-de-1" || candidate.EdgeGroupID == "edge-group-country-de" || candidate.IP == "51.38.126.103" {
			t.Fatalf("expected shared target candidates to exclude DE edge, got %+v", record.Candidates)
		}
	}
	if stringSliceContains(record.AnswerPolicy.AllowedEdgeGroups, "edge-group-country-de") ||
		!stringSliceContains(record.AnswerPolicy.AllowedEdgeGroups, "edge-group-country-us") {
		t.Fatalf("expected answer policy to exclude DE group, got %+v", record.AnswerPolicy)
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
	if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, model.EdgeNode{
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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
			serveEdgeDNSBundleRequest(t, server, recorder, req)
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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
	if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, model.EdgeNode{
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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
	if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, model.EdgeNode{
		ID:          "edge-us-1",
		EdgeGroupID: "edge-group-country-us",
		PublicIPv4:  "15.204.94.71",
		Status:      model.EdgeHealthHealthy,
		Healthy:     true,
	}); err != nil {
		t.Fatalf("record healthy US edge node: %v", err)
	}
	if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, model.EdgeNode{
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
	serveEdgeDNSBundleRequest(t, server, us, usReq)
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
	serveEdgeDNSBundleRequest(t, server, de, deReq)
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

func TestEdgeDNSBundleGatesDynamicEdgeUntilCanaryAndProbePass(t *testing.T) {
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
			ID:                "edge-jp-1",
			EdgeGroupID:       "edge-group-country-jp",
			WorkloadMode:      model.EdgeWorkloadModeDynamic,
			CanaryState:       model.EdgeCanaryStateJoined,
			PublicProbeStatus: model.EdgePublicProbeStatusUnknown,
			Country:           "jp",
			Region:            "asia",
			PublicIPv4:        "203.0.113.44",
			Status:            model.EdgeHealthHealthy,
			Healthy:           true,
			CaddyRouteCount:   1,
			TLSStatus:         model.EdgeTLSStatusReady,
		},
	} {
		if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, node); err != nil {
			t.Fatalf("record edge heartbeat: %v", err)
		}
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-us&answer_ip=15.204.94.71", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	apiA := edgeDNSRecordByNameAndType(bundle.Records, "api.fugue.pro", model.EdgeDNSRecordTypeA)
	if apiA == nil {
		t.Fatalf("expected api.fugue.pro A record, got %+v", bundle.Records)
	}
	if stringSliceContains(apiA.Values, "203.0.113.44") || edgeDNSCandidateByEdgeID(apiA.Candidates, "edge-jp-1") != nil {
		t.Fatalf("expected joined dynamic edge to stay out of DNS answers, got record=%+v", apiA)
	}

	probedAt := time.Now().UTC()
	if _, _, err := storeState.UpdateEdgeNodeControlState("edge-jp-1", model.EdgeNode{
		CanaryState:       model.EdgeCanaryStateCanary,
		CanaryWeight:      1,
		PublicProbeStatus: model.EdgePublicProbeStatusPassing,
		PublicProbeLastAt: &probedAt,
	}); err != nil {
		t.Fatalf("promote dynamic edge canary: %v", err)
	}

	recorder = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-us&answer_ip=15.204.94.71", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &bundle)
	apiA = edgeDNSRecordByNameAndType(bundle.Records, "api.fugue.pro", model.EdgeDNSRecordTypeA)
	if apiA == nil || !stringSliceContains(apiA.Values, "203.0.113.44") {
		t.Fatalf("expected canary dynamic edge answer, got %+v", apiA)
	}
	jpCandidate := edgeDNSCandidateByEdgeID(apiA.Candidates, "edge-jp-1")
	if jpCandidate == nil ||
		jpCandidate.WorkloadMode != model.EdgeWorkloadModeDynamic ||
		jpCandidate.CanaryState != model.EdgeCanaryStateCanary ||
		jpCandidate.CanaryWeight != 1 ||
		jpCandidate.PublicProbeStatus != model.EdgePublicProbeStatusPassing ||
		!jpCandidate.DNSEligible ||
		jpCandidate.Weight != 1 {
		t.Fatalf("unexpected dynamic canary candidate: %+v candidates=%+v", jpCandidate, apiA.Candidates)
	}

	if _, err := storeState.PutEdgeRoutePolicy(model.EdgeRoutePolicy{
		Hostname:        "api.fugue.pro",
		AppID:           "platform-api",
		TenantID:        "platform",
		RoutePolicy:     model.EdgeRoutePolicyEnabled,
		ExcludedEdgeIDs: []string{"edge-jp-1"},
		ExclusionReason: "dynamic edge canary blocked for service",
	}); err != nil {
		t.Fatalf("put dynamic edge exclusion policy: %v", err)
	}
	recorder = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-us&answer_ip=15.204.94.71", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &bundle)
	apiA = edgeDNSRecordByNameAndType(bundle.Records, "api.fugue.pro", model.EdgeDNSRecordTypeA)
	if apiA == nil {
		t.Fatalf("expected api.fugue.pro A record after exclusion, got %+v", bundle.Records)
	}
	if stringSliceContains(apiA.Values, "203.0.113.44") || edgeDNSCandidateByEdgeID(apiA.Candidates, "edge-jp-1") != nil {
		t.Fatalf("expected service exclusion to remove dynamic edge from DNS answers, got %+v", apiA)
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
		if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, node); err != nil {
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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
		apiA.AnswerPolicy.SwitchCooldownSec != int(edgeDNSDecisionCooldown.Seconds()) ||
		apiA.AnswerPolicy.RankingVersion != edgeDNSQualityRankingVersion ||
		apiA.AnswerPolicy.RankingScope != "global" {
		t.Fatalf("expected latency-aware policy with safety gates, got %+v", apiA.AnswerPolicy)
	}
	decisions, err := storeState.ListEdgeDNSRoutingDecisions("api.fugue.pro")
	if err != nil {
		t.Fatalf("list routing decisions: %v", err)
	}
	if len(decisions) != 0 {
		t.Fatalf("GET /v1/edge/dns must not persist routing decisions, got %+v", decisions)
	}
	if written, err := server.reconcileEdgeDNSRoutingDecisions(now); err != nil {
		t.Fatalf("reconcile routing decisions: %v", err)
	} else if written == 0 {
		t.Fatal("expected background reconciler to persist routing decisions")
	}
	decisions, err = storeState.ListEdgeDNSRoutingDecisions("api.fugue.pro")
	if err != nil {
		t.Fatalf("list reconciled routing decisions: %v", err)
	}
	if len(decisions) == 0 {
		t.Fatal("expected background reconciler decisions")
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

func TestEdgeDNSShadowModeDoesNotChangeAnswerButRecordsShadowWinner(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	server.edgeQualityRankingMode = "shadow"
	seedEdgeDNSLatencyFixture(t, storeState, server)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	apiA := edgeDNSRecordByNameAndType(bundle.Records, "api.fugue.pro", model.EdgeDNSRecordTypeA)
	if apiA == nil {
		t.Fatalf("expected api.fugue.pro A record, got %+v", bundle.Records)
	}
	if apiA.AnswerPolicy.PolicyKind != model.DNSAnswerPolicyKindGeo ||
		apiA.AnswerPolicy.SelectedEdgeGroupID != "edge-group-country-de" ||
		apiA.AnswerPolicy.ShadowSelectedEdgeGroupID != "edge-group-country-us" ||
		apiA.AnswerPolicy.RankingVersion != edgeDNSQualityRankingVersion ||
		apiA.AnswerPolicy.RankingScope != "global" ||
		!strings.Contains(apiA.AnswerPolicy.ShadowReason, "shadow_latency_aware") {
		t.Fatalf("expected shadow mode to keep geo answer and expose shadow winner, got %+v", apiA.AnswerPolicy)
	}
	if len(apiA.ScopedCandidates) != 0 {
		t.Fatalf("expected shadow mode not to publish scoped answer candidates, got %+v", apiA.ScopedCandidates)
	}
}

func TestEdgeDNSLegacyModeDisablesScopedQualityRanking(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	server.edgeQualityRankingMode = "legacy"
	seedEdgeDNSLatencyFixture(t, storeState, server)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/dns?token=edge-secret&zone=fugue.pro&edge_group_id=edge-group-country-de&answer_ip=51.38.126.103", nil)
	serveEdgeDNSBundleRequest(t, server, recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeDNSBundle
	mustDecodeJSON(t, recorder, &bundle)
	apiA := edgeDNSRecordByNameAndType(bundle.Records, "api.fugue.pro", model.EdgeDNSRecordTypeA)
	if apiA == nil {
		t.Fatalf("expected api.fugue.pro A record, got %+v", bundle.Records)
	}
	if apiA.AnswerPolicy.PolicyKind != model.DNSAnswerPolicyKindGeo ||
		apiA.AnswerPolicy.SelectedEdgeGroupID != "edge-group-country-de" ||
		apiA.AnswerPolicy.ShadowSelectedEdgeGroupID != "" ||
		len(apiA.ScopedCandidates) != 0 {
		t.Fatalf("expected legacy mode to bypass scoped quality ranking, policy=%+v scoped=%+v", apiA.AnswerPolicy, apiA.ScopedCandidates)
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

func seedEdgeDNSLatencyFixture(t *testing.T, storeState *store.Store, server *Server) {
	t.Helper()

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
		if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, node); err != nil {
			t.Fatalf("record edge heartbeat: %v", err)
		}
	}
	now := time.Now().UTC()
	if err := storeState.RecordEdgePerformanceSamples([]model.EdgePerformanceSample{
		{
			ID:            "api-us-fast-shadow",
			EdgeGroupID:   "edge-group-country-us",
			Hostname:      "api.fugue.pro",
			ClientCountry: "de",
			ClientRegion:  "eu-central",
			ClientASN:     "as3320",
			DNSPolicy:     "client_scope_header",
			TTFBMS:        120,
			UpstreamMS:    80,
			TotalMS:       140,
			StatusCode:    200,
			SampleCount:   20,
			SampledAt:     now.Add(-10 * time.Minute),
		},
		{
			ID:            "api-de-slow-shadow",
			EdgeGroupID:   "edge-group-country-de",
			Hostname:      "api.fugue.pro",
			ClientCountry: "de",
			ClientRegion:  "eu-central",
			ClientASN:     "as3320",
			DNSPolicy:     "client_scope_header",
			TTFBMS:        650,
			UpstreamMS:    520,
			TotalMS:       700,
			StatusCode:    200,
			SampleCount:   20,
			SampledAt:     now.Add(-10 * time.Minute),
		},
	}, time.Time{}); err != nil {
		t.Fatalf("record performance samples: %v", err)
	}
}

func TestEdgeDNSLatencyScoreIncludesNetworkDamage(t *testing.T) {
	t.Parallel()

	clean := edgeDNSLatencyCandidateProfile{
		TrafficClass:   "large_body_api",
		SampleCount:    60,
		TTFBMS:         120,
		UpstreamMS:     80,
		TotalMS:        150,
		UploadBPS:      2 * 1024 * 1024,
		ScoreBreakdown: map[string]float64{},
		Confidence:     1,
	}
	damaged := clean
	damaged.ScoreBreakdown = map[string]float64{}
	damaged.ClientTCPRTTMS = 260
	damaged.ClientTCPRTTVarMS = 90
	damaged.ClientTCPRetransRate = 0.15
	damaged.ClientTCPBytesRetransRate = 0.10
	damaged.ClientTCPRTORate = 0.08
	damaged.ClientTCPDeliveryBPS = 64 * 1024

	cleanScore := edgeDNSLatencyScore(clean)
	damagedScore := edgeDNSLatencyScore(damaged)
	if damagedScore <= cleanScore {
		t.Fatalf("expected damaged TCP candidate to score worse, clean=%f damaged=%f", cleanScore, damagedScore)
	}
	if damaged.ScoreBreakdown["network"] <= clean.ScoreBreakdown["network"] {
		t.Fatalf("expected network penalty in score breakdown, clean=%+v damaged=%+v", clean.ScoreBreakdown, damaged.ScoreBreakdown)
	}
}

func TestEdgeDNSBundleCreatesScopedLatencyProfileFromClientMetadata(t *testing.T) {
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
		if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, node); err != nil {
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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
	if countryScoped == nil || countryScoped.SelectedEdgeGroupID != "edge-group-country-us" {
		t.Fatalf("expected samples with client metadata to create scoped profiles, got %+v", apiA.ScopedCandidates)
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
		if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, node); err != nil {
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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

func edgeDNSCandidateByEdgeID(candidates []model.EdgeDNSAnswerCandidate, edgeID string) *model.EdgeDNSAnswerCandidate {
	for index := range candidates {
		if candidates[index].EdgeID == edgeID {
			return &candidates[index]
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
	serveEdgeDNSBundleRequest(t, server, recorder, req)
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
	serveEdgeDNSBundleRequest(t, server, afterDelete, afterDeleteReq)
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

func putHostedDNSZoneForEdgeDNSTest(t *testing.T, storeState *store.Store, tenantID, zoneName string) model.HostedZone {
	t.Helper()
	now := time.Now().UTC()
	zone, err := storeState.PutHostedZone(model.HostedZone{
		TenantID:            tenantID,
		ZoneName:            zoneName,
		Status:              model.HostedZoneStatusActive,
		DelegationStatus:    model.HostedZoneDelegationStatusReady,
		ExpectedNameservers: []string{"ns1.dns.fugue.pro", "ns2.dns.fugue.pro"},
		CreatedAt:           now,
		UpdatedAt:           now,
	})
	if err != nil {
		t.Fatalf("put hosted DNS zone %s: %v", zoneName, err)
	}
	return zone
}

func putHostedDNSRecordForEdgeDNSTest(t *testing.T, storeState *store.Store, zone model.HostedZone, record model.DNSRecord) model.DNSRecord {
	t.Helper()
	out, err := storeState.PutDNSRecord(zone, record, false)
	if err != nil {
		t.Fatalf("put hosted DNS record %s %s: %v", record.Name, record.Type, err)
	}
	return out
}

func edgeDNSRecordByNameAndType(records []model.EdgeDNSRecord, name, recordType string) *model.EdgeDNSRecord {
	for index := range records {
		if records[index].Name == name && records[index].Type == recordType {
			return &records[index]
		}
	}
	return nil
}

func serveEdgeDNSBundleRequest(t *testing.T, server *Server, recorder *httptest.ResponseRecorder, req *http.Request) {
	t.Helper()
	options, err := server.edgeDNSBundleOptionsFromRequest(req)
	if err == nil {
		bundle, deriveErr := server.deriveEdgeDNSBundle(req, options)
		if deriveErr == nil {
			now := time.Now().UTC()
			publishFullEdgeDNSArtifactForTest(t, server.store, server, newEdgeDNSBundleArtifact(options, bundle, now))
		}
	}
	server.Handler().ServeHTTP(recorder, req)
}
