package api

import (
	"net/http"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestAdminSSHFrontObservationSeparatesStoredFreshAndCurrentEvidence(test *testing.T) {
	stateStore, server, tenantKey, adminKey, _, _ := setupAppDomainTestServerWithDomains(test, "fugue.pro")
	_, token, err := stateStore.CreateEdgeNodeToken(model.EdgeNode{ID: "edge-a", EdgeGroupID: "edge-group-country-us", Status: model.EdgeHealthUnknown})
	if err != nil {
		test.Fatal(err)
	}
	bundle, err := server.deriveEdgeSSHRouteBundle(store.AppSSHRouteOptions{EdgeID: "edge-a", EdgeGroupID: "edge-group-country-us"})
	if err != nil {
		test.Fatal(err)
	}
	now := time.Now().UTC()
	ack := model.EdgeSSHAcknowledgement{EdgeSSHAcknowledgementRequest: model.EdgeSSHAcknowledgementRequest{EdgeID: "edge-a", EdgeGroupID: "edge-group-country-us", InstanceUID: "pod-a", BundleVersion: bundle.Version, RouteDigest: model.SSHRoutesDigest(bundle.Routes), Persisted: true, LoadedAt: now, Connections: []model.EdgeSSHRouteConnections{}}, Routes: bundle.Routes, ObservedAt: now, ExpiresAt: now.Add(time.Minute)}
	path := "/v1/admin/ssh/front-acknowledgements/edge-a/pod-a"
	for _, key := range []string{tenantKey, token, ""} {
		response := performJSONRequest(test, server, http.MethodGet, path, key, nil)
		if response.Code != http.StatusForbidden && response.Code != http.StatusUnauthorized {
			test.Fatalf("non-admin received evidence: %d", response.Code)
		}
	}
	missing := performJSONRequest(test, server, http.MethodGet, path, adminKey, nil)
	if missing.Code != http.StatusNotFound {
		test.Fatal("missing exact instance was not reported", missing.Code)
	}
	for _, scenario := range []struct {
		name    string
		fresh   bool
		current bool
	}{
		{"current", true, true},
		{"expired", false, true},
		{"obsolete", true, false},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			candidate := ack
			candidate.InstanceUID = scenario.name
			if !scenario.fresh {
				candidate.LoadedAt = now.Add(-40 * time.Second)
				candidate.ObservedAt = now.Add(-30 * time.Second)
				candidate.ExpiresAt = now.Add(-time.Second)
			}
			if !scenario.current {
				candidate.BundleVersion = "older-route-generation"
			}
			if err := stateStore.RecordEdgeSSHAcknowledgement(candidate); err != nil {
				test.Fatal(err)
			}
			response := performJSONRequest(test, server, http.MethodGet, "/v1/admin/ssh/front-acknowledgements/edge-a/"+candidate.InstanceUID, adminKey, nil)
			if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
				test.Fatal(response.Code, response.Body.String())
			}
			var result struct {
				Acknowledgement model.EdgeSSHAcknowledgement `json:"acknowledgement"`
				Fresh           bool                         `json:"fresh"`
				Current         bool                         `json:"current_routes_match"`
			}
			mustDecodeJSON(test, response, &result)
			stored, err := stateStore.GetEdgeSSHAcknowledgement(candidate.EdgeID, candidate.InstanceUID)
			if err != nil || result.Fresh != scenario.fresh || result.Current != scenario.current || !reflect.DeepEqual(stored, result.Acknowledgement) || !stored.LoadedAt.Equal(candidate.LoadedAt) || !stored.ObservedAt.Equal(candidate.ObservedAt) || !stored.ExpiresAt.Equal(candidate.ExpiresAt) {
				test.Fatal("read changed evidence or conflated freshness and current routing", result, err)
			}
		})
	}
}

func TestSSHAcknowledgementRequiresExactScopedNodeAndCurrentBundle(test *testing.T) {
	stateStore, server, _, _, _, _ := setupAppDomainTestServerWithDomains(test, "fugue.pro")
	_, token, err := stateStore.CreateEdgeNodeToken(model.EdgeNode{ID: "edge-a", EdgeGroupID: "edge-group-country-us", Status: model.EdgeHealthUnknown})
	if err != nil {
		test.Fatal(err)
	}
	bundle, err := server.deriveEdgeSSHRouteBundle(store.AppSSHRouteOptions{EdgeID: "edge-a", EdgeGroupID: "edge-group-country-us"})
	if err != nil {
		test.Fatal(err)
	}
	input := model.EdgeSSHAcknowledgementRequest{EdgeID: "edge-a", EdgeGroupID: "edge-group-country-us", InstanceUID: "pod-a", BundleVersion: bundle.Version, RouteDigest: model.SSHRoutesDigest(bundle.Routes), Persisted: true, LoadedAt: time.Now().UTC(), Connections: []model.EdgeSSHRouteConnections{}}
	for _, scenario := range []struct {
		name   string
		alter  func(*model.EdgeSSHAcknowledgementRequest)
		status int
	}{
		{"wrong node", func(value *model.EdgeSSHAcknowledgementRequest) { value.EdgeID = "edge-b" }, http.StatusForbidden},
		{"wrong group", func(value *model.EdgeSSHAcknowledgementRequest) { value.EdgeGroupID = "other" }, http.StatusForbidden},
		{"not persisted", func(value *model.EdgeSSHAcknowledgementRequest) { value.Persisted = false }, http.StatusBadRequest},
		{"old bundle", func(value *model.EdgeSSHAcknowledgementRequest) { value.BundleVersion = "old" }, http.StatusConflict},
		{"different digest", func(value *model.EdgeSSHAcknowledgementRequest) { value.RouteDigest = "other" }, http.StatusConflict},
		{"expired evidence", func(value *model.EdgeSSHAcknowledgementRequest) { value.LoadedAt = time.Now().Add(-5 * time.Minute) }, http.StatusBadRequest},
		{"unknown connections", func(value *model.EdgeSSHAcknowledgementRequest) { value.Connections = nil }, http.StatusBadRequest},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			candidate := input
			scenario.alter(&candidate)
			response := performJSONRequest(test, server, http.MethodPost, "/v1/edge/ssh/ack", token, candidate)
			if response.Code != scenario.status {
				test.Fatal(response.Code, response.Body.String())
			}
		})
	}
	response := performJSONRequest(test, server, http.MethodPost, "/v1/edge/ssh/ack", token, input)
	if response.Code != http.StatusOK {
		test.Fatal(response.Code, response.Body.String())
	}
	ack, err := stateStore.GetEdgeSSHAcknowledgement("edge-a", "pod-a")
	if err != nil || ack.RouteDigest != input.RouteDigest || !ack.ExpiresAt.After(time.Now()) {
		test.Fatal("accepted acknowledgement not independently persisted", err)
	}
	response = performJSONRequest(test, server, http.MethodPost, "/v1/edge/ssh/ack", token, input)
	if response.Code != http.StatusConflict {
		test.Fatal("replayed acknowledgement accepted", response.Code)
	}
}
