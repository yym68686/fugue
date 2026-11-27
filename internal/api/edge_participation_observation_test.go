package api

import (
	"net/http"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestParticipationObservationIsScopedAndReadOnly(t *testing.T) {
	storeState, server, tenantKey, adminKey, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	const node, group = "member-a", "group-a"
	const endpoint = "/v1/edge/nodes/member-a/participation-observation"
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID: node, EdgeGroupID: group, PublicIPv4: "192.0.2.8", Status: model.EdgeHealthHealthy, Healthy: true,
	}); err != nil {
		t.Fatal(err)
	}
	before, _, err := storeState.GetEdgeNode(node)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately repeat the seed and key ID to verify domain separation.
	observerKeys := platformcontrol.DeriveParticipationObserverKeyring("isolated fixture", "test-key")
	generalKeys := platformcontrol.DerivePlatformComponentIdentityKeyring("isolated fixture", "test-key", "", "", nil)
	server.auth.ParticipationObserverKeyring = observerKeys
	server.auth.PlatformComponentIdentityKeyring = generalKeys
	server.auth.EdgeRouteIntentIdentityKeyring = generalKeys
	issue := func(keys platformcontrol.PlatformComponentIdentityKeyring, subject, cohort string, when time.Time, ttl time.Duration) string {
		t.Helper()
		claims, err := platformcontrol.ParticipationObserverClaims(subject, cohort)
		if err != nil {
			t.Fatal(err)
		}
		token, err := platformcontrol.IssuePlatformComponentIdentity(keys, claims, when, ttl)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	now := time.Now()
	token := issue(observerKeys, node, group, now, platformcontrol.ParticipationObserverTTL)
	for _, tc := range []struct {
		name, method, path, key string
		status                  int
	}{
		{"observer", http.MethodGet, endpoint, token, http.StatusOK},
		{"missing token", http.MethodGet, endpoint, "", http.StatusUnauthorized},
		{"admin token", http.MethodGet, endpoint, adminKey, http.StatusUnauthorized},
		{"tenant token", http.MethodGet, endpoint, tenantKey, http.StatusUnauthorized},
		{"general keyring", http.MethodGet, endpoint, issue(generalKeys, node, group, now, time.Minute), http.StatusUnauthorized},
		{"expired token", http.MethodGet, endpoint, issue(observerKeys, node, group, now.Add(-time.Hour), time.Minute), http.StatusUnauthorized},
		{"excessive lifetime", http.MethodGet, endpoint, issue(observerKeys, node, group, now, 3*time.Minute), http.StatusForbidden},
		{"other node", http.MethodGet, "/v1/edge/nodes/member-b/participation-observation", token, http.StatusForbidden},
		{"other group", http.MethodGet, endpoint, issue(observerKeys, node, "group-b", now, time.Minute), http.StatusForbidden},
		{"HEAD", http.MethodHead, endpoint, token, http.StatusMethodNotAllowed},
		{"admin read", http.MethodGet, "/v1/edge/nodes/member-a", token, http.StatusUnauthorized},
		{"admin mutation", http.MethodPost, "/v1/admin/edge/nodes/member-a/drain", token, http.StatusUnauthorized},
		{"route intent", http.MethodGet, "/v1/edge/route-intents?edge_group_id=group-a", token, http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := performJSONRequest(t, server, tc.method, tc.path, tc.key, nil)
			if response.Code != tc.status {
				t.Fatalf("got %d, want %d: %s", response.Code, tc.status, response.Body.String())
			}
			if response.Code == http.StatusOK {
				if response.Header().Get("Cache-Control") != "private, no-store" {
					t.Fatal("participation observation is cacheable")
				}
				var got model.EdgeParticipationObservation
				mustDecodeJSON(t, response, &got)
				if got.Node != node || got.Group != group || got.PublicIPv4 != before.PublicIPv4 ||
					got.EvaluatedAt.IsZero() || got.DNSState != "not_configured" || got.DNSServingEvidence != "not_evaluated" {
					t.Fatalf("wrong or overclaimed observation: %+v", got)
				}
			}
		})
	}
	after, _, err := storeState.GetEdgeNode(node)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("observing participation changed the stored node: %v", err)
	}
}
