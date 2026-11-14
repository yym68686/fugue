package platformconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestRefreshIdentityRetainsExactAuthorityAndUsesRotatedPodCredential(t *testing.T) {
	before := Identity{Token: "expired-component-token", ExpiresAt: time.Now().Add(-time.Minute), Component: "dns-server", NodeID: "node-a", AuthorityID: "cell-a", ConsumerID: "dns-server:cell-a:node-a", ScopeKey: "authority-cell:cell-a", ArtifactKinds: []string{model.PlatformArtifactKindDNSAnswerBundle}}
	for _, mode := range []string{"same", "fresh", "node", "scope", "component", "authority", "consumer", "kind", "extra-kind", "expiry", "empty-token", "forbidden", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			input := before
			if mode == "fresh" {
				input.ExpiresAt = time.Now().Add(2 * time.Minute)
			}
			issued := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				issued++
				if r.Method != http.MethodPost || r.URL.Path != "/v1/platform-state/consumers/identity" || r.Header.Get("Authorization") != "Bearer rotated-pod-token" {
					t.Error("renewal did not use the live Pod credential on the exact issuance endpoint")
				}
				after := before
				after.Token, after.ExpiresAt = "fresh-component-token", time.Now().Add(2*time.Minute)
				switch mode {
				case "node":
					after.NodeID = "node-b"
				case "scope":
					after.ScopeKey = "global"
				case "component":
					after.Component = "edge-worker"
				case "authority":
					after.AuthorityID = "cell-b"
				case "consumer":
					after.ConsumerID = "dns-server:cell-b:node-a"
				case "kind":
					after.ArtifactKinds = []string{model.PlatformArtifactKindEdgeRouteBundle}
				case "extra-kind":
					after.ArtifactKinds = append([]string{model.PlatformArtifactKindEdgeRouteBundle}, before.ArtifactKinds...)
				case "expiry":
					after.ExpiresAt = time.Now().Add(5 * time.Second)
				case "empty-token":
					after.Token = ""
				case "forbidden":
					w.WriteHeader(http.StatusForbidden)
					return
				}
				json.NewEncoder(w).Encode(after)
			}))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "pod-token")
			if err := os.WriteFile(path, []byte("rotated-pod-token\n"), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			got, err := (Client{BaseURL: server.URL, TokenFile: path, AuthorityID: before.AuthorityID}).RefreshIdentity(ctx, input)
			if mode == "fresh" {
				if err != nil || !reflect.DeepEqual(got, input) || issued != 0 {
					t.Fatal("fresh identity caused an unnecessary issuance", err)
				}
			} else if mode == "same" {
				if err != nil || got.Token != "fresh-component-token" || issued != 1 {
					t.Fatal("long observation did not renew its exact identity", err)
				}
			} else if err == nil || issued > 1 || mode == "cancelled" && (!errors.Is(err, context.Canceled) || issued != 0) {
				t.Fatal("invalid renewal was accepted or retried", err)
			}
		})
	}
}

func TestExpiredIdentityRechecksExactAssignmentAndNeverRetriesHeartbeat(t *testing.T) {
	for _, mode := range []string{"same", "changed", "lost-response", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			id := Identity{Token: "expired", ExpiresAt: time.Now().Add(-time.Second), Component: "dns-server", NodeID: "node-a", ScopeKey: "global", ArtifactKinds: []string{model.PlatformArtifactKindDNSAnswerBundle}}
			a := model.PlatformConsumerAssignment{ArtifactID: "dns", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: "global", ReleaseChannel: "full", ExpectedConsumerSetID: "expected", FencingToken: 7}
			posts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/platform-state/consumers/identity" {
					if mode == "revoked" {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					next := id
					next.Token, next.ExpiresAt = "renewed", time.Now().Add(2*time.Minute)
					json.NewEncoder(w).Encode(next)
					return
				}
				if r.Header.Get("Authorization") != "Bearer renewed" {
					t.Error("expired bearer escaped renewal")
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if r.URL.Path == "/v1/platform-state/consumers/assignment" {
					next := a
					if mode == "changed" {
						next.FencingToken++
					}
					json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{next}})
					return
				}
				posts++
				var body map[string]string
				if json.NewDecoder(r.Body).Decode(&body) != nil || body["evidence"] != "original-observation" {
					t.Error("renewal rewrote the observation")
				}
				if mode == "lost-response" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				json.NewEncoder(w).Encode(map[string]bool{"accepted": true})
			}))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(path, []byte("pod"), 0600); err != nil {
				t.Fatal(err)
			}
			client := Client{BaseURL: server.URL, TokenFile: path}
			err := client.CheckServingAssignment(context.Background(), id, a)
			if mode == "changed" || mode == "revoked" {
				if err == nil || mode == "changed" && !errors.Is(err, ErrAssignmentChanged) || posts != 0 {
					t.Fatal("renewal waived the current assignment or revocation gate", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var reply map[string]bool
			err = client.PostIdentityJSON(context.Background(), "/v1/platform-state/consumers/trusted-heartbeat", id, map[string]string{"evidence": "original-observation"}, &reply)
			if posts != 1 || mode == "same" && (err != nil || !reply["accepted"]) || mode == "lost-response" && err == nil {
				t.Fatal("heartbeat was retried or renewed reporting failed", err, posts)
			}
		})
	}
}
