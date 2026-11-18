package platformconsumer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"fugue/internal/model"
)

func TestPassiveASNSeparatesAuthorityMetadataFromRequestURL(t *testing.T) {
	assignment := model.PlatformConsumerAssignment{ArtifactID: "dns-artifact", ExpectedConsumerSetID: "set+one"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/platform-state/consumers/artifacts/dns-artifact/passive-asn" ||
			len(r.URL.Query()) != 1 || r.URL.Query().Get("expected_consumer_set_id") != assignment.ExpectedConsumerSetID ||
			r.Header.Get("Authorization") != "Bearer component-token" {
			t.Errorf("unexpected passive ASN request: %s %s", r.Method, r.URL)
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(model.PlatformArtifact{ID: "passive-observation"})
	}))
	defer server.Close()
	for _, suffix := range []string{"?authority_service=discovery#unused", "?"} {
		client := Client{BaseURL: server.URL + "/api/" + suffix}
		result, err := client.PassiveASN(context.Background(), Identity{Token: "component-token"}, assignment)
		if err != nil || result.ID != "passive-observation" {
			t.Fatalf("passive observation unavailable: %v", err)
		}
	}
}
