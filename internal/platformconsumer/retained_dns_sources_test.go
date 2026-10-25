package platformconsumer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestRetainedDNSRouteSourcesPinsImmutableReleaseAndAssignment(t *testing.T) {
	for _, scenario := range []string{"verified", "superseded", "changed fence", "changed parent", "changed set", "changed capability", "changed time", "unverified", "rolled back", "wrong LKG", "missing LKG"} {
		t.Run(scenario, func(t *testing.T) {
			at := time.Now().UTC()
			a := model.PlatformConsumerAssignment{ArtifactID: "dns", ArtifactReleaseID: "full", ExpectedConsumerSetID: "set", ReleaseChannel: "full", FencingToken: 4}
			r := model.PlatformArtifactRelease{ID: "full", ArtifactID: "parent", ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: "authority-cell:dns", Generation: "generation", ReleaseChannel: "full", FencingToken: 4, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: at}
			got := r
			got.VerificationState = model.PlatformArtifactVerificationStateVerified
			got.VerifiedLKGGeneration = r.Generation
			got.VerifiedAt = &at
			response := model.PlatformConsumerRetainedDNSRouteSourcesResponse{PlatformConsumerDNSRouteSourcesResponse: model.PlatformConsumerDNSRouteSourcesResponse{Assignment: a, Release: got}, RetainedLKG: model.PlatformLKGSnapshot{ID: "lkg", VerifiedByReleaseID: r.ID}}
			switch scenario {
			case "superseded":
				response.Release.Status = model.PlatformArtifactReleaseStatusSuperseded
			case "changed fence":
				response.Release.FencingToken++
			case "changed parent":
				response.Release.ArtifactID = "other"
			case "changed set":
				response.Assignment.ExpectedConsumerSetID = "other"
			case "changed capability":
				response.Assignment.CompatibilityCapabilities = []string{"new-capability"}
			case "changed time":
				response.Release.ReleasedAt = at.Add(time.Second)
			case "unverified":
				response.Release.VerificationState = "pending"
			case "rolled back":
				response.Release.Status = model.PlatformArtifactReleaseStatusRolledBack
			case "wrong LKG":
				response.RetainedLKG.VerifiedByReleaseID = "other"
			case "missing LKG":
				response.RetainedLKG = model.PlatformLKGSnapshot{}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/platform-state/consumers/artifacts/dns/retained-route-sources" || r.URL.Query().Get("expected_consumer_set_id") != "set" || r.Header.Get("Authorization") != "Bearer test" {
					t.Error("lost scoped retained source read")
				}
				json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			_, err := (Client{BaseURL: server.URL}).RetainedDNSRouteSources(context.Background(), Identity{Token: "test"}, a, r)
			positive := scenario == "verified" || scenario == "superseded"
			if (err == nil) != positive {
				t.Fatalf("retained release binding validation: %v", err)
			}
		})
	}
}
