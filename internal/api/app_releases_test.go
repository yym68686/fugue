package api

import (
	"net/http"
	"testing"

	"fugue/internal/model"
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
