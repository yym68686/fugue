package platformconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fugue/internal/model"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDNSRouteSourcesAreBoundToExactAssignment(t *testing.T) {
	for _, mode := range []string{"valid", "assignment", "release", "conflict", "forbidden", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			a := model.PlatformConsumerAssignment{ArtifactID: "dns", ExpectedConsumerSetID: "expected", FencingToken: 2}
			r := model.PlatformArtifactRelease{ID: "release"}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path != "/v1/platform-state/consumers/artifacts/dns/route-sources" || req.URL.Query().Get("expected_consumer_set_id") != "expected" || req.Header.Get("Authorization") != "Bearer scoped" {
					t.Error("private assignment binding missing")
				}
				response := model.PlatformConsumerDNSRouteSourcesResponse{Assignment: a, Release: r, Snapshot: model.PlatformDNSRouteSourceSnapshot{SelectionDigest: "selection"}}
				switch mode {
				case "assignment":
					response.Assignment.FencingToken++
				case "release":
					response.Release.ID = "other"
				case "conflict":
					w.WriteHeader(409)
					return
				case "forbidden":
					w.WriteHeader(403)
					return
				case "unavailable":
					w.WriteHeader(503)
					return
				}
				json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			got, err := (Client{BaseURL: server.URL}).DNSRouteSources(context.Background(), Identity{Token: "scoped"}, a, r)
			if mode == "valid" {
				if err != nil || got.SelectionDigest != "selection" {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid source response accepted")
			}
			race := mode == "assignment" || mode == "release" || mode == "conflict"
			if errors.Is(err, ErrAssignmentChanged) != race {
				t.Fatal("authorization/outage hidden as convergence", err)
			}
		})
	}
}
