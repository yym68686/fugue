package platformconsumer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/dnsroutewire"
	"fugue/internal/model"
)

func TestDNSRouteSourceRevalidationRequiresFreshMatchingObservation(t *testing.T) {
	for _, mode := range []string{"fresh", "missing time", "stale time", "future time", "wrong validator", "revoked", "changed"} {
		t.Run(mode, func(t *testing.T) {
			a := model.PlatformConsumerAssignment{ArtifactID: "dns", ContentHash: "digest", ExpectedConsumerSetID: "set"}
			r := model.PlatformArtifactRelease{ID: "release"}
			previous := model.PlatformDNSRouteSourceSnapshot{DNSArtifactID: a.ArtifactID, DNSArtifactDigest: a.ContentHash, SelectionDigest: "selection", ObservedAt: time.Now().Add(-time.Second)}
			prior := model.PlatformConsumerDNSRouteSourcesResponse{Assignment: a, Release: r, Snapshot: previous}
			validator, _ := dnsroutewire.Validator(prior, nil)
			observed := time.Now().UTC()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Header.Get("Authorization") != "Bearer scoped" || req.Header.Get("If-None-Match") != validator {
					t.Error("revalidation lost exact validator or authentication")
				}
				if mode == "revoked" {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				if mode == "changed" {
					prior.Snapshot.SelectionDigest = "new-selection"
					prior.Snapshot.ObservedAt = observed
					_ = json.NewEncoder(w).Encode(prior)
					return
				}
				w.Header().Set("ETag", validator)
				stamp := observed
				switch mode {
				case "stale time":
					stamp = previous.ObservedAt
				case "future time":
					stamp = observed.Add(time.Hour)
				case "wrong validator":
					w.Header().Set("ETag", "foreign")
				}
				if mode != "missing time" {
					w.Header().Set(dnsroutewire.ObservedAtHeader, stamp.Format(time.RFC3339Nano))
				}
				w.WriteHeader(http.StatusNotModified)
			}))
			defer server.Close()
			got, err := (Client{BaseURL: server.URL}).DNSRouteSources(t.Context(), Identity{Token: "scoped"}, a, r, previous)
			if mode == "fresh" || mode == "changed" {
				if err != nil || !got.ObservedAt.Equal(observed) {
					t.Fatal("fresh recheck failed", err)
				}
				if mode == "changed" && got.SelectionDigest != "new-selection" {
					t.Fatal("changed response reused old selection")
				}
			} else if err == nil {
				t.Fatal("invalid recheck reused cached observation")
			}
		})
	}
}
