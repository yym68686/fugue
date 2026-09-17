package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgecontrol"
)

func TestParseEdgeAuthorityServicesDefaultsToEmpty(t *testing.T) {
	services, err := parseEdgeAuthorityServices("")
	if err != nil || len(services) != 0 {
		t.Fatalf("empty authority services = %#v, %v", services, err)
	}
	services, err = parseEdgeAuthorityServices(`{"edge-group-country-us":"edge-control-us"}`)
	if err != nil || services["edge-group-country-us"] != "edge-control-us" {
		t.Fatalf("parsed authority services = %#v, %v", services, err)
	}
	if _, err := parseEdgeAuthorityServices(`{"edge-group-country-us":"not a service"}`); err == nil {
		t.Fatal("invalid service mapping accepted")
	}
}

func TestAdminListEdgeAuthoritiesUsesRealEdgeControlProjection(t *testing.T) {
	_, server, _, adminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	server.controlPlaneNamespace = "fugue-system"
	server.edgeAuthorityServices = map[string]string{"edge-group-country-us": "edge-control-us"}
	server.edgeDNSAuthorityHTTPClient = &http.Client{Transport: edgeDNSAuthorityRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Accept") != "" {
			t.Fatalf("authority request Accept = %q", request.Header.Get("Accept"))
		}
		now := time.Now().UTC()
		body := `{"edge_group_id":"edge-group-country-us","status":"ready","ready":true,"serving_healthy":true,"bootstrap_eligible":true,"inventory_sequence":7,"inventory_generation":"inventory-7","inventory_producer_generation":8,"inventory_producer_nodes":2,"inventory_heartbeat_at":"` + now.Add(-time.Second).Format(time.RFC3339) + `","authority_sequence":9,"publication_sequence":9,"current_publication_sequence":9,"publication_decision":"published","bundle_generation":"edgegroupbundle_test","published_bundle_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bundle_valid_until":"` + now.Add(time.Minute).Format(time.RFC3339) + `","lkg_state":"current"}`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/edge/authorities", adminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("authority status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Configured  bool   `json:"configured"`
		AnswerModel string `json:"answer_model"`
		AllReady    bool   `json:"all_ready"`
		Authorities []struct {
			Ready  bool   `json:"ready"`
			Source string `json:"source"`
			Status struct {
				ServingHealthy             bool   `json:"serving_healthy"`
				BootstrapEligible          bool   `json:"bootstrap_eligible"`
				AuthoritySequence          uint64 `json:"authority_sequence"`
				CurrentPublicationSequence uint64 `json:"current_publication_sequence"`
				InventorySequence          uint64 `json:"inventory_sequence"`
				PublicationSequence        uint64 `json:"publication_sequence"`
				InventoryProducerNodes     int    `json:"inventory_producer_nodes"`
			} `json:"status"`
		} `json:"authorities"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !response.Configured || !response.AllReady || response.AnswerModel != "edge-control-authority" || len(response.Authorities) != 1 ||
		!response.Authorities[0].Ready || response.Authorities[0].Source != "edge-control-authority" ||
		!response.Authorities[0].Status.ServingHealthy || !response.Authorities[0].Status.BootstrapEligible ||
		response.Authorities[0].Status.AuthoritySequence != 9 || response.Authorities[0].Status.CurrentPublicationSequence != 9 ||
		response.Authorities[0].Status.InventorySequence != 7 || response.Authorities[0].Status.PublicationSequence != 9 || response.Authorities[0].Status.InventoryProducerNodes != 2 {
		t.Fatalf("unexpected authority projection: %+v", response)
	}
}

type emptyAuthorityStatusStore struct{}

func (emptyAuthorityStatusStore) ReadGroupAuthorityStatus(context.Context, string) (edgecontrol.AuthorityGroupStoreSnapshot, error) {
	return edgecontrol.AuthorityGroupStoreSnapshot{}, nil
}

// Use the real content-negotiating handler: application/json selects the
// inventory cursor and would silently discard the two boolean health facts.
func TestAdminEdgeAuthoritiesReadsCompleteControlHandlerResponse(t *testing.T) {
	_, server, _, adminKey, _, _ := setupAppDomainTestServerWithDomains(t, "platform.example.test")
	server.controlPlaneNamespace = "fugue-system"
	server.edgeAuthorityServices = map[string]string{"edge-group-region-test": "edge-control-test"}
	handler, err := edgecontrol.NewAuthorityStatusHandler(emptyAuthorityStatusStore{}, []string{"edge-group-region-test"}, edgecontrol.NewAuthorityRuntimeState(time.Now), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	server.edgeDNSAuthorityHTTPClient = &http.Client{Transport: edgeDNSAuthorityRoundTripper(func(request *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Result(), nil
	})}
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/edge/authorities", adminKey, nil)
	var response edgeAuthorityReadResponse
	mustDecodeJSON(t, recorder, &response)
	if len(response.Authorities) != 1 {
		t.Fatalf("missing authority: %+v", response)
	}
	record := response.Authorities[0]
	if response.AllReady || record.Ready || record.Status == nil || record.Status.ServingHealthy || record.Status.BootstrapEligible || !strings.Contains(record.Error, "http_status=503") {
		t.Fatalf("explicit unavailable health facts were lost: %+v", record)
	}
}

func TestAdminEdgeAuthoritiesRejectsMissingHealthFacts(t *testing.T) {
	for _, body := range []string{
		`{"ready":true}`,
		`{"ready":true,"serving_healthy":true}`,
		`{"ready":true,"bootstrap_eligible":false}`,
		`{"ready":true,"serving_healthy":null,"bootstrap_eligible":false}`,
		`{"ready":true,"serving_healthy":true,"bootstrap_eligible":null}`,
	} {
		t.Run(body, func(t *testing.T) {
			_, server, _, adminKey, _, _ := setupAppDomainTestServerWithDomains(t, "platform.example.test")
			server.controlPlaneNamespace = "fugue-system"
			server.edgeAuthorityServices = map[string]string{"edge-group-region-test": "edge-control-test"}
			server.edgeDNSAuthorityHTTPClient = &http.Client{Transport: edgeDNSAuthorityRoundTripper(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			})}
			recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/edge/authorities", adminKey, nil)
			var response edgeAuthorityReadResponse
			mustDecodeJSON(t, recorder, &response)
			if response.AllReady || len(response.Authorities) != 1 || response.Authorities[0].Ready || response.Authorities[0].Status != nil || !strings.Contains(response.Authorities[0].Error, "missing serving health or bootstrap evidence") {
				t.Fatalf("missing facts became a health observation: %+v", response)
			}
		})
	}
}

func TestAdminListEdgeAuthoritiesReportsUnconfiguredWithoutSyntheticNodes(t *testing.T) {
	_, server, _, adminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/edge/authorities", adminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("authority status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response edgeAuthorityReadResponse
	mustDecodeJSON(t, recorder, &response)
	if response.Configured || response.AllReady || len(response.Authorities) != 0 || response.AnswerModel != "edge-control-authority" || response.Error == "" {
		t.Fatalf("unexpected unconfigured authority response: %+v", response)
	}
}
