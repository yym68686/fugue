package api

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
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
		if request.Header.Get("Accept") != "application/json" {
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
				InventorySequence      uint64 `json:"inventory_sequence"`
				PublicationSequence    uint64 `json:"publication_sequence"`
				InventoryProducerNodes int    `json:"inventory_producer_nodes"`
			} `json:"status"`
		} `json:"authorities"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !response.Configured || !response.AllReady || response.AnswerModel != "edge-control-authority" || len(response.Authorities) != 1 ||
		!response.Authorities[0].Ready || response.Authorities[0].Source != "edge-control-authority" ||
		response.Authorities[0].Status.InventorySequence != 7 || response.Authorities[0].Status.PublicationSequence != 9 || response.Authorities[0].Status.InventoryProducerNodes != 2 {
		t.Fatalf("unexpected authority projection: %+v", response)
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
