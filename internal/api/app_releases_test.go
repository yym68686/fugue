package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/observability"
)

func TestAppReleaseTrafficAPI(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, _, app, _, _, _, _ := setupAppImagesTestServer(t)

	getTraffic := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/traffic", apiKey, nil)
	if getTraffic.Code != http.StatusOK {
		t.Fatalf("expected get traffic status 200, got %d body=%s", getTraffic.Code, getTraffic.Body.String())
	}
	var trafficResponse appTrafficResponse
	mustDecodeJSON(t, getTraffic, &trafficResponse)
	if trafficResponse.Traffic.StableWeight != 100 || trafficResponse.Traffic.CandidateWeight != 0 {
		t.Fatalf("expected stable-only default traffic, got %+v", trafficResponse.Traffic)
	}
	if trafficResponse.Traffic.StableReleaseID == "" {
		t.Fatalf("expected stable release id: %+v", trafficResponse.Traffic)
	}

	createCandidate := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/releases", apiKey, appReleaseCreateRequest{
		Role:        model.AppReleaseRoleCandidate,
		UpstreamURL: "http://candidate.internal",
		Status:      model.AppReleaseStatusReady,
	})
	if createCandidate.Code != http.StatusCreated {
		t.Fatalf("expected create release status 201, got %d body=%s", createCandidate.Code, createCandidate.Body.String())
	}
	var releaseResponse appReleaseResponse
	mustDecodeJSON(t, createCandidate, &releaseResponse)

	stableWeight := 90
	candidateWeight := 10
	patchTraffic := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/traffic", apiKey, appTrafficPatchRequest{
		Mode:               model.AppTrafficModeCanary,
		CandidateReleaseID: releaseResponse.Release.ID,
		StableWeight:       &stableWeight,
		CandidateWeight:    &candidateWeight,
	})
	if patchTraffic.Code != http.StatusOK {
		t.Fatalf("expected patch traffic status 200, got %d body=%s", patchTraffic.Code, patchTraffic.Body.String())
	}
	mustDecodeJSON(t, patchTraffic, &trafficResponse)
	if trafficResponse.Traffic.StableWeight != 90 || trafficResponse.Traffic.CandidateWeight != 10 {
		t.Fatalf("unexpected patched traffic: %+v", trafficResponse.Traffic)
	}
	if trafficResponse.Traffic.CandidateReleaseID != releaseResponse.Release.ID {
		t.Fatalf("expected candidate release %q, got %+v", releaseResponse.Release.ID, trafficResponse.Traffic)
	}
}

func TestAppReleaseGateMetricsUseReleaseID(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, _, app, _, _, _, _ := setupAppImagesTestServer(t)
	candidateUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/health" {
			t.Fatalf("expected candidate health probe, got %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(candidateUpstream.Close)

	var clickHouseQuery string
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clickHouseQuery = r.URL.Query().Get("query")
		_, _ = w.Write([]byte(`{"request_count":12,"error_5xx_count":0,"edge_upstream_error_count":0,"p95_ttfb_ms":91,"p99_duration_ms":142}` + "\n"))
	}))
	t.Cleanup(clickHouse.Close)
	server.observabilityConfig = observability.Config{
		Enabled:       true,
		ClickHouseDSN: clickHouse.URL + "?database=fugue_observability",
	}.Normalize()

	createCandidate := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/releases", apiKey, appReleaseCreateRequest{
		Role:        model.AppReleaseRoleCandidate,
		UpstreamURL: candidateUpstream.URL,
		Status:      model.AppReleaseStatusReady,
	})
	if createCandidate.Code != http.StatusCreated {
		t.Fatalf("expected create release status 201, got %d body=%s", createCandidate.Code, createCandidate.Body.String())
	}
	var releaseResponse appReleaseResponse
	mustDecodeJSON(t, createCandidate, &releaseResponse)

	gateRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/releases/"+releaseResponse.Release.ID+"/gate/evaluate", apiKey, appReleaseGateRequest{
		Policy: &model.AppReleaseGatePolicy{
			WindowSeconds:        60,
			MinCandidateRequests: 5,
		},
	})
	if gateRecorder.Code != http.StatusOK {
		t.Fatalf("expected gate status 200, got %d body=%s", gateRecorder.Code, gateRecorder.Body.String())
	}
	var gateResponse appReleaseGateResponse
	mustDecodeJSON(t, gateRecorder, &gateResponse)
	if gateResponse.Gate.Status != model.AppReleaseGateStatusPass {
		t.Fatalf("expected gate pass, got %+v", gateResponse.Gate)
	}
	for _, want := range []string{
		"FROM request_facts",
		"app_id = '" + app.ID + "'",
		"JSONExtractString(summary_json, 'release_id') = '" + releaseResponse.Release.ID + "'",
		"quantileTDigestIf(0.99)(toFloat64(duration_ms), NOT (JSONExtractBool(summary_json, 'sse') OR JSONExtractBool(summary_json, 'stream') OR JSONExtractBool(summary_json, 'streaming'))) AS p99_duration_ms",
		"FORMAT JSONEachRow",
	} {
		if !strings.Contains(clickHouseQuery, want) {
			t.Fatalf("expected ClickHouse query to contain %q, got %q", want, clickHouseQuery)
		}
	}
	if strings.Contains(clickHouseQuery, "JSONExtractString(summary_json, 'release_role')") {
		t.Fatalf("expected release gate query to avoid role-only filtering, got %q", clickHouseQuery)
	}
}

func TestAppReleaseGateFailsDefaultWhenCandidateHasNoObservedRequests(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, _, app, _, _, _, _ := setupAppImagesTestServer(t)
	candidateUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(candidateUpstream.Close)

	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"request_count":0,"error_5xx_count":0,"edge_upstream_error_count":0,"p95_ttfb_ms":0,"p99_duration_ms":0}` + "\n"))
	}))
	t.Cleanup(clickHouse.Close)
	server.observabilityConfig = observability.Config{
		Enabled:       true,
		ClickHouseDSN: clickHouse.URL + "?database=fugue_observability",
	}.Normalize()

	createCandidate := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/releases", apiKey, appReleaseCreateRequest{
		Role:        model.AppReleaseRoleCandidate,
		UpstreamURL: candidateUpstream.URL,
		Status:      model.AppReleaseStatusReady,
	})
	if createCandidate.Code != http.StatusCreated {
		t.Fatalf("expected create release status 201, got %d body=%s", createCandidate.Code, createCandidate.Body.String())
	}
	var releaseResponse appReleaseResponse
	mustDecodeJSON(t, createCandidate, &releaseResponse)

	gateRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/releases/"+releaseResponse.Release.ID+"/gate/evaluate", apiKey, appReleaseGateRequest{
		Policy: &model.AppReleaseGatePolicy{WindowSeconds: 60},
	})
	if gateRecorder.Code != http.StatusOK {
		t.Fatalf("expected gate status 200, got %d body=%s", gateRecorder.Code, gateRecorder.Body.String())
	}
	var gateResponse appReleaseGateResponse
	mustDecodeJSON(t, gateRecorder, &gateResponse)
	if gateResponse.Policy.MinCandidateRequests != defaultAppReleaseGateMinRequests {
		t.Fatalf("expected default min candidate requests %d, got %+v", defaultAppReleaseGateMinRequests, gateResponse.Policy)
	}
	if gateResponse.Gate.Status != model.AppReleaseGateStatusFail {
		t.Fatalf("expected gate fail with zero observed requests, got %+v", gateResponse.Gate)
	}
	if len(gateResponse.Gate.Failures) == 0 || !strings.Contains(gateResponse.Gate.Failures[0], "below minimum") {
		t.Fatalf("expected below-minimum failure, got %+v", gateResponse.Gate.Failures)
	}
}
