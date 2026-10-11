package api

import (
	"net/http"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

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
