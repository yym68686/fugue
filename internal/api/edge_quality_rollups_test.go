package api

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestBuildEdgeQualityRollupsIncludesWindowsQuantilesAndPlatformFallback(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	samples := []model.EdgePerformanceSample{
		{
			ID:                    "fast-majority",
			EdgeID:                "edge-us-1",
			EdgeGroupID:           "edge-group-country-us",
			Hostname:              "api.fugue.pro",
			Method:                "GET",
			TrafficClass:          "static_cacheable",
			ClientCountry:         "cn",
			ClientRegion:          "hk",
			ClientASN:             "as4134",
			TTFBMS:                100,
			TotalMS:               120,
			SampleCount:           30,
			CacheHitCount:         28,
			CacheObservationCount: 30,
			ResponseEgressBPS:     2 * 1024 * 1024,
			SampledAt:             now.Add(-2 * time.Minute),
		},
		{
			ID:                    "slow-tail",
			EdgeID:                "edge-us-1",
			EdgeGroupID:           "edge-group-country-us",
			Hostname:              "api.fugue.pro",
			Method:                "GET",
			TrafficClass:          "static_cacheable",
			ClientCountry:         "cn",
			ClientRegion:          "hk",
			ClientASN:             "as4134",
			TTFBMS:                300,
			TotalMS:               330,
			SampleCount:           10,
			CacheHitCount:         8,
			CacheObservationCount: 10,
			ResponseEgressBPS:     1024 * 1024,
			SampledAt:             now.Add(-1 * time.Minute),
		},
	}

	rollups := buildEdgeQualityRollupsForWindow(samples, "5m", now.Add(-5*time.Minute), now, now)
	hostnameGroup := findEdgeQualityRollup(t, rollups, "api.fugue.pro", "global", "global", "edge-group-country-us", "")
	if hostnameGroup.RequestCount != 40 || hostnameGroup.SampleCount != 2 {
		t.Fatalf("unexpected hostname group counts: %+v", hostnameGroup)
	}
	if hostnameGroup.P50TTFBMS != 100 || hostnameGroup.P95TTFBMS != 300 || hostnameGroup.P99TTFBMS != 300 {
		t.Fatalf("expected weighted TTFB quantiles, got p50=%f p95=%f p99=%f", hostnameGroup.P50TTFBMS, hostnameGroup.P95TTFBMS, hostnameGroup.P99TTFBMS)
	}
	if hostnameGroup.CacheHitRate != 0.9 {
		t.Fatalf("expected cache hit ratio to roll up, got %+v", hostnameGroup)
	}
	hostnameNode := findEdgeQualityRollup(t, rollups, "api.fugue.pro", "asn", "as4134", "edge-group-country-us", "edge-us-1")
	if hostnameNode.EdgeID != "edge-us-1" || hostnameNode.ClientScopeKind != "asn" {
		t.Fatalf("expected node-level ASN rollup, got %+v", hostnameNode)
	}
	platformGroup := findEdgeQualityRollup(t, rollups, edgeQualityPlatformRollupHostname, "region", "cn:hk", "edge-group-country-us", "")
	if platformGroup.Hostname != edgeQualityPlatformRollupHostname || platformGroup.RequestCount != 40 {
		t.Fatalf("expected platform fallback rollup, got %+v", platformGroup)
	}
}

func TestEdgeQualityRollupStaticCacheHitRatioAffectsScore(t *testing.T) {
	t.Parallel()

	highHit := model.EdgeQualityRollup{
		Window:                "30m",
		TrafficClass:          "static_cacheable",
		RequestCount:          100,
		SampleCount:           20,
		P50TTFBMS:             80,
		P95TTFBMS:             110,
		AvgTotalMS:            130,
		CacheHitCount:         95,
		CacheObservationCount: 100,
		CacheHitRate:          0.95,
		AvgResponseEgressBPS:  2 * 1024 * 1024,
		Confidence:            1,
	}
	lowHit := highHit
	lowHit.CacheHitCount = 10
	lowHit.CacheHitRate = 0.10

	highScore, highBreakdown := scoreEdgeQualityRollup(highHit)
	lowScore, lowBreakdown := scoreEdgeQualityRollup(lowHit)
	if lowScore <= highScore {
		t.Fatalf("expected low cache hit ratio to score worse, high=%f low=%f", highScore, lowScore)
	}
	if lowBreakdown["cache"] <= highBreakdown["cache"] {
		t.Fatalf("expected cache breakdown to increase for low-hit static asset, high=%+v low=%+v", highBreakdown, lowBreakdown)
	}
}

func TestEdgeQualityStreamingDoesNotTreatLongTotalDurationLikeDynamicLatency(t *testing.T) {
	t.Parallel()

	streamShort := model.EdgeQualityRollup{
		Window:       "30m",
		TrafficClass: "streaming",
		RequestCount: 100,
		SampleCount:  20,
		P95TTFBMS:    120,
		AvgTotalMS:   1000,
		Confidence:   1,
	}
	streamLong := streamShort
	streamLong.AvgTotalMS = 600000
	dynamicShort := streamShort
	dynamicShort.TrafficClass = "dynamic_api"
	dynamicLong := streamLong
	dynamicLong.TrafficClass = "dynamic_api"

	streamShortScore, _ := scoreEdgeQualityRollup(streamShort)
	streamLongScore, _ := scoreEdgeQualityRollup(streamLong)
	dynamicShortScore, _ := scoreEdgeQualityRollup(dynamicShort)
	dynamicLongScore, _ := scoreEdgeQualityRollup(dynamicLong)
	streamDelta := streamLongScore - streamShortScore
	dynamicDelta := dynamicLongScore - dynamicShortScore
	if streamDelta <= 0 || dynamicDelta <= 0 || streamDelta >= dynamicDelta/5 {
		t.Fatalf("expected streaming total duration weight to be much lower than dynamic API, streamDelta=%f dynamicDelta=%f", streamDelta, dynamicDelta)
	}
}

func TestEdgeQualityOriginGlobalSlowDoesNotPromoteSingleEdgeDecision(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	groups := map[string]*edgeDNSLatencyGroupAccumulator{}
	for _, groupID := range []string{"edge-group-a", "edge-group-b"} {
		edgeDNSLatencyAccumulate(groups, groupID, model.EdgePerformanceSample{
			ID:            groupID,
			EdgeGroupID:   groupID,
			Hostname:      "api.fugue.pro",
			TrafficClass:  "dynamic_api",
			TTFBMS:        120,
			UpstreamMS:    90,
			TotalMS:       160,
			OriginTTFBMS:  3000,
			OriginTotalMS: 3500,
			SampleCount:   80,
			SampledAt:     now.Add(-10 * time.Minute),
		})
	}
	profile := buildEdgeDNSLatencyProfile("api.fugue.pro", edgeDNSLatencyScope{}, groups)
	if profile != nil {
		t.Fatalf("expected equal global-origin slowness to avoid a single-edge routing decision, got %+v", profile)
	}
}

func TestEdgeQualitySingleEdgeOriginSlowAffectsThatEdge(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	groups := map[string]*edgeDNSLatencyGroupAccumulator{}
	edgeDNSLatencyAccumulate(groups, "edge-group-fast-origin", model.EdgePerformanceSample{
		ID:            "fast-origin",
		EdgeGroupID:   "edge-group-fast-origin",
		Hostname:      "api.fugue.pro",
		TrafficClass:  "dynamic_api",
		TTFBMS:        120,
		UpstreamMS:    90,
		TotalMS:       160,
		OriginTTFBMS:  100,
		OriginTotalMS: 140,
		SampleCount:   80,
		SampledAt:     now.Add(-10 * time.Minute),
	})
	edgeDNSLatencyAccumulate(groups, "edge-group-slow-origin", model.EdgePerformanceSample{
		ID:            "slow-origin",
		EdgeGroupID:   "edge-group-slow-origin",
		Hostname:      "api.fugue.pro",
		TrafficClass:  "dynamic_api",
		TTFBMS:        120,
		UpstreamMS:    90,
		TotalMS:       160,
		OriginTTFBMS:  3000,
		OriginTotalMS: 3500,
		SampleCount:   80,
		SampledAt:     now.Add(-10 * time.Minute),
	})

	profile := buildEdgeDNSLatencyProfile("api.fugue.pro", edgeDNSLatencyScope{}, groups)
	if profile == nil || profile.BestEdgeGroupID != "edge-group-fast-origin" {
		t.Fatalf("expected edge with fast origin path to win, got %+v", profile)
	}
	slow := profile.Candidates["edge-group-slow-origin"]
	if slow.ScoreBreakdown["origin"] <= profile.Candidates["edge-group-fast-origin"].ScoreBreakdown["origin"] {
		t.Fatalf("expected origin breakdown to penalize slow-origin edge, got %+v", profile.Candidates)
	}
}

func TestEdgeQualityFiveMinuteSevereDegradePenalizesQuickly(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	samples := []model.EdgePerformanceSample{
		{
			ID:           "healthy",
			EdgeGroupID:  "edge-group-healthy",
			Hostname:     "api.fugue.pro",
			TrafficClass: "large_body_api",
			TTFBMS:       120,
			TotalMS:      160,
			SampleCount:  40,
			SampledAt:    now.Add(-2 * time.Minute),
		},
		{
			ID:                  "degraded",
			EdgeGroupID:         "edge-group-degraded",
			Hostname:            "api.fugue.pro",
			TrafficClass:        "large_body_api",
			TTFBMS:              120,
			TotalMS:             160,
			SampleCount:         40,
			UploadEffectiveBPS:  16 * 1024,
			BodyReadErrorCount:  4,
			BodyIncompleteCount: 2,
			SampledAt:           now.Add(-2 * time.Minute),
		},
	}

	degraded := edgeDNSSevereDegradeGroups(samples, now)
	if degraded["edge-group-degraded"] <= 0 || degraded["edge-group-healthy"] != 0 {
		t.Fatalf("expected only degraded group to receive severe degrade penalty, got %+v", degraded)
	}
	profile := &edgeDNSLatencyProfile{
		Enabled:         true,
		BestEdgeGroupID: "edge-group-degraded",
		Candidates: map[string]edgeDNSLatencyCandidateProfile{
			"edge-group-degraded": {EdgeGroupID: "edge-group-degraded", Score: 100, Weight: edgeDNSLatencyWeightMax, ScoreBreakdown: map[string]float64{}},
			"edge-group-healthy":  {EdgeGroupID: "edge-group-healthy", Score: 120, Weight: edgeDNSLatencyWeightMin, ScoreBreakdown: map[string]float64{}},
		},
	}
	if !edgeDNSApplySevereDegradeToProfile(profile, degraded) {
		t.Fatal("expected severe degrade profile update")
	}
	if profile.BestEdgeGroupID != "edge-group-healthy" ||
		!strings.Contains(profile.Reason, "severe_degrade") ||
		profile.Candidates["edge-group-degraded"].ScoreBreakdown["severe_degrade"] <= 0 {
		t.Fatalf("expected severe degrade to demote degraded candidate, got %+v", profile)
	}
}

func TestEdgeQualityRankResponseUsesPlatformFallbackRollup(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	response := buildEdgeQualityRankResponseFromRollups(edgeQualityRankQuery{
		Hostname:       "api.fugue.pro",
		TrafficClass:   "dynamic_api",
		RequestedScope: edgeQualityRankScope{Kind: "asn", Value: "as4134", ASN: "as4134"},
		Window:         "30m",
		Since:          now.Add(-30 * time.Minute),
		GeneratedAt:    now,
	}, []model.EdgeNode{
		{
			ID:                 "edge-us-1",
			EdgeGroupID:        "edge-group-country-us",
			Healthy:            true,
			RouteBundleVersion: "routegen",
			CaddyRouteCount:    1,
			TLSStatus:          model.EdgeTLSStatusReady,
		},
		{
			ID:                 "edge-de-1",
			EdgeGroupID:        "edge-group-country-de",
			Healthy:            true,
			RouteBundleVersion: "routegen",
			CaddyRouteCount:    1,
			TLSStatus:          model.EdgeTLSStatusReady,
		},
	}, model.EdgeRoutePolicy{}, []model.EdgeQualityRollup{
		{
			Window:                "30m",
			WindowStartedAt:       now.Add(-30 * time.Minute),
			WindowEndedAt:         now,
			Hostname:              edgeQualityPlatformRollupHostname,
			TrafficClass:          "dynamic_api",
			ClientScopeKind:       "asn",
			ClientScopeValue:      "as4134",
			EdgeGroupID:           "edge-group-country-us",
			RequestCount:          80,
			SampleCount:           10,
			P95TTFBMS:             100,
			AvgTotalMS:            140,
			CacheObservationCount: 80,
			Confidence:            1,
			Score:                 100,
			ScoreBreakdown:        map[string]float64{"latency": 100},
		},
		{
			Window:                "30m",
			WindowStartedAt:       now.Add(-30 * time.Minute),
			WindowEndedAt:         now,
			Hostname:              edgeQualityPlatformRollupHostname,
			TrafficClass:          "dynamic_api",
			ClientScopeKind:       "asn",
			ClientScopeValue:      "as4134",
			EdgeGroupID:           "edge-group-country-de",
			RequestCount:          80,
			SampleCount:           10,
			P95TTFBMS:             500,
			AvgTotalMS:            620,
			CacheObservationCount: 80,
			Confidence:            1,
			Score:                 500,
			ScoreBreakdown:        map[string]float64{"latency": 500},
		},
	})

	if response.FallbackLevel == 0 || !strings.Contains(response.FallbackReason, "platform global") {
		t.Fatalf("expected platform fallback to be explicit, got %+v", response)
	}
	if len(response.Candidates) != 2 || response.Candidates[0].EdgeGroupID != "edge-group-country-us" {
		t.Fatalf("expected platform rollup candidates to rank edges, got %+v", response.Candidates)
	}
}

func TestEdgeQualityRecoveryUsesHysteresisCooldown(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	profile := &edgeDNSLatencyProfile{
		Hostname:        "api.fugue.pro",
		Scope:           edgeDNSLatencyScope{Country: "de"},
		Enabled:         true,
		Reason:          "latency_aware_stable_window_24h",
		BestEdgeGroupID: "edge-group-new-winner",
		Candidates: map[string]edgeDNSLatencyCandidateProfile{
			"edge-group-new-winner": {EdgeGroupID: "edge-group-new-winner", Score: 100, Weight: edgeDNSLatencyWeightMax, ScoreBreakdown: map[string]float64{}},
			"edge-group-previous":   {EdgeGroupID: "edge-group-previous", Score: 130, Weight: edgeDNSLatencyWeightMin, ScoreBreakdown: map[string]float64{}},
		},
	}
	existing := model.EdgeDNSRoutingDecision{
		Hostname:            "api.fugue.pro",
		ScopeKey:            "country:de",
		SelectedEdgeGroupID: "edge-group-previous",
		SwitchedAt:          now.Add(-5 * time.Minute),
		CooldownUntil:       now.Add(20 * time.Minute),
		CreatedAt:           now.Add(-5 * time.Minute),
		UpdatedAt:           now.Add(-5 * time.Minute),
	}

	updated, decision := applyEdgeDNSRoutingDecision(profile, existing, now)
	if updated.BestEdgeGroupID != "edge-group-previous" ||
		decision.SelectedEdgeGroupID != "edge-group-previous" ||
		updated.ShadowBestEdgeGroupID != "edge-group-new-winner" ||
		updated.Reason != "latency_aware_cooldown_hold" {
		t.Fatalf("expected cooldown to hold previous edge while recording shadow winner, profile=%+v decision=%+v", updated, decision)
	}
}

func findEdgeQualityRollup(t *testing.T, rollups []model.EdgeQualityRollup, hostname, scopeKind, scopeValue, edgeGroupID, edgeID string) model.EdgeQualityRollup {
	t.Helper()

	for _, rollup := range rollups {
		if rollup.Hostname == hostname &&
			rollup.ClientScopeKind == scopeKind &&
			rollup.ClientScopeValue == scopeValue &&
			rollup.EdgeGroupID == edgeGroupID &&
			rollup.EdgeID == edgeID {
			return rollup
		}
	}
	t.Fatalf("rollup not found hostname=%s scope=%s:%s group=%s edge=%s all=%+v", hostname, scopeKind, scopeValue, edgeGroupID, edgeID, rollups)
	return model.EdgeQualityRollup{}
}
