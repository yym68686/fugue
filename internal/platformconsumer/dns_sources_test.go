package platformconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/model"
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

func TestDNSRouteSourcesAllowInPlaceFullVerification(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Second)
	verifiedAt := at.Add(time.Second)
	a := model.PlatformConsumerAssignment{ArtifactID: "dns", ContentHash: "dns-digest", ExpectedConsumerSetID: "expected", ArtifactReleaseID: "release", FencingToken: 42, ReleaseChannel: "full"}
	before := model.PlatformArtifactRelease{ID: a.ArtifactReleaseID, ArtifactID: "parent", ScopeKey: "authority-cell:cell-dns", Generation: "generation", ReleaseChannel: "full", Status: model.PlatformArtifactReleaseStatusActive, FencingToken: a.FencingToken, Version: 1, VerificationState: model.PlatformArtifactVerificationStateServingUnverified, ServingUnverifiedGeneration: "generation", ReleasedAt: at, UpdatedAt: at}
	verified := before
	verified.Version++
	verified.VerificationState = model.PlatformArtifactVerificationStateVerified
	verified.VerificationEvidence = map[string]string{"outcome": "passed"}
	verified.ServingUnverifiedGeneration = ""
	verified.VerifiedLKGGeneration = before.Generation
	verified.VerifiedAt = &verifiedAt
	verified.UpdatedAt = verifiedAt
	for _, tc := range []struct {
		name   string
		change func(*model.PlatformArtifactRelease)
	}{
		{name: "same full release verified"},
		{name: "fenced off", change: func(r *model.PlatformArtifactRelease) { r.FencingToken++ }},
		{name: "superseded", change: func(r *model.PlatformArtifactRelease) { r.Status = model.PlatformArtifactReleaseStatusSuperseded }},
		{name: "rolled back", change: func(r *model.PlatformArtifactRelease) { r.RollbackTargetGeneration = "older" }},
		{name: "verification failed", change: func(r *model.PlatformArtifactRelease) {
			r.VerificationState = model.PlatformArtifactVerificationStateFailed
		}},
		{name: "verification regressed", change: func(r *model.PlatformArtifactRelease) { r.Version = before.Version }},
		{name: "different verified generation", change: func(r *model.PlatformArtifactRelease) { r.VerifiedLKGGeneration = "other" }},
		{name: "verification lacks timestamp", change: func(r *model.PlatformArtifactRelease) { r.VerifiedAt = nil }},
		{name: "artifact replaced", change: func(r *model.PlatformArtifactRelease) { r.ArtifactID = "other" }},
		{name: "override changed", change: func(r *model.PlatformArtifactRelease) { r.OverrideMode = model.PlatformArtifactOverrideModeSoft }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fresh := verified
			if tc.change != nil {
				tc.change(&fresh)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				json.NewEncoder(w).Encode(model.PlatformConsumerDNSRouteSourcesResponse{Assignment: a, Release: fresh, Snapshot: model.PlatformDNSRouteSourceSnapshot{SelectionDigest: "selection"}})
			}))
			defer server.Close()
			got, err := (Client{BaseURL: server.URL}).DNSRouteSources(context.Background(), Identity{Token: "scoped"}, a, before)
			if tc.change == nil && (err != nil || got.SelectionDigest != "selection") {
				t.Fatalf("in-place verification invalidated exact source observation: %v", err)
			}
			if tc.change != nil && !errors.Is(err, ErrAssignmentChanged) {
				t.Fatalf("invalid release change accepted: %v", err)
			}
		})
	}
	if sameDNSRouteSourceRelease(verified, before) {
		t.Fatal("a previously verified release regressed to unverified")
	}
}

func TestDNSRouteSourceNotFoundRequiresConfirmedAssignmentChange(t *testing.T) {
	for _, mode := range []string{"rollback", "shadow_changed", "unchanged", "unauthorized", "recheck_unavailable", "missing"} {
		t.Run(mode, func(t *testing.T) {
			a := model.PlatformConsumerAssignment{ArtifactID: "dns-candidate", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: "authority-cell:cell-dns", ExpectedConsumerSetID: "candidate-set", ReleaseChannel: "gray", FencingToken: 4}
			if mode == "shadow_changed" {
				a.ReleaseChannel = "shadow"
			}
			rechecks := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Header.Get("Authorization") != "Bearer scoped" {
					t.Error("assignment recheck lost scoped identity")
				}
				if req.URL.Path == "/v1/platform-state/consumers/assignment" {
					rechecks++
					if (req.URL.Query().Get("serving_only") == "true") != (a.ReleaseChannel != "shadow") {
						t.Error("recheck did not use actual serving selection")
					}
					if mode == "recheck_unavailable" {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					current := a
					if mode == "rollback" || mode == "shadow_changed" {
						current.ArtifactID, current.ExpectedConsumerSetID = "selected-dns", "selected-set"
						current.FencingToken++
						if mode == "rollback" {
							current.ReleaseChannel = "full"
						}
					}
					items := []model.PlatformConsumerAssignment{current}
					if mode == "missing" {
						items = nil
					}
					json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: items})
					return
				}
				if mode == "unauthorized" {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				w.WriteHeader(http.StatusNotFound)
			}))
			defer server.Close()
			_, err := (Client{BaseURL: server.URL}).DNSRouteSources(context.Background(), Identity{Token: "scoped"}, a, model.PlatformArtifactRelease{})
			changed := mode == "rollback" || mode == "shadow_changed" || mode == "missing"
			if err == nil || errors.Is(err, ErrAssignmentChanged) != changed || IsUnavailable(err) {
				t.Fatal("missing source misclassified without exact assignment evidence", err)
			}
			if mode == "unauthorized" && rechecks != 0 || mode != "unauthorized" && rechecks != 1 {
				t.Fatal("source rejection recheck was not bounded and authorized", rechecks)
			}
		})
	}
}
