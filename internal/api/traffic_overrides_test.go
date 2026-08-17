package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/trafficoverride"
)

func TestTrafficOverrideAdminLifecycleIsSignedCASAndSecretSafe(t *testing.T) {
	t.Parallel()
	stateStore, server, _, adminKey, _, _ := setupAppDomainTestServerWithDomains(t, "example.com")
	hostname := "app.example.com"
	request := map[string]any{
		"answers":              []string{"192.0.2.10"},
		"required_host_routes": []string{hostname},
		"route_generation":     "route-generation-1",
		"route_digest":         "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"expires_at":           time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		"reason":               "emergency route verification",
		"expected_generation":  0,
	}
	unauthorized := performJSONRequest(t, server, http.MethodPut, "/v1/admin/traffic-overrides/"+hostname, "", request)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("traffic override must require auth: %d", unauthorized.Code)
	}
	created := performJSONRequest(t, server, http.MethodPut, "/v1/admin/traffic-overrides/"+hostname, adminKey, request)
	if created.Code != http.StatusOK {
		t.Fatalf("create override: %d body=%s", created.Code, created.Body.String())
	}
	var createdBody struct {
		Override model.TrafficOverride `json:"override"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &createdBody); err != nil {
		t.Fatal(err)
	}
	keyring, err := stateStore.GetTrafficOverrideSigningKeyring()
	if err != nil || trafficoverride.VerifyWithKeyring(createdBody.Override, keyring) != nil {
		t.Fatalf("created override is not signed by the independent keyring: %v", err)
	}
	stale := performJSONRequest(t, server, http.MethodPut, "/v1/admin/traffic-overrides/"+hostname, adminKey, request)
	if stale.Code != http.StatusConflict {
		t.Fatalf("stale override generation must conflict: %d body=%s", stale.Code, stale.Body.String())
	}
	listed := performJSONRequest(t, server, http.MethodGet, "/v1/admin/traffic-overrides", adminKey, nil)
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), hostname) {
		t.Fatalf("list override: %d body=%s", listed.Code, listed.Body.String())
	}
	keyStatus := performJSONRequest(t, server, http.MethodGet, "/v1/admin/traffic-override-signing-key", adminKey, nil)
	if keyStatus.Code != http.StatusOK || strings.Contains(keyStatus.Body.String(), keyring.CurrentPrivateKey) || !strings.Contains(keyStatus.Body.String(), keyring.CurrentPublicKey) {
		t.Fatalf("signing key status leaked secret material: %d body=%s", keyStatus.Code, keyStatus.Body.String())
	}
	rotated := performJSONRequest(t, server, http.MethodPost, "/v1/admin/traffic-override-signing-key/rotate", adminKey, map[string]any{"expected_generation": keyring.Generation})
	if rotated.Code != http.StatusOK {
		t.Fatalf("rotate signing key: %d body=%s", rotated.Code, rotated.Body.String())
	}
	rotatedKeyring, err := stateStore.GetTrafficOverrideSigningKeyring()
	if err != nil || trafficoverride.VerifyWithKeyring(createdBody.Override, rotatedKeyring) != nil {
		t.Fatalf("previous key did not verify existing artifact after rotation: %v", err)
	}
	revoked := performJSONRequest(t, server, http.MethodPost, "/v1/admin/traffic-overrides/"+hostname+"/revoke", adminKey, map[string]any{"reason": "incident resolved safely", "expected_generation": 1})
	if revoked.Code != http.StatusOK || !strings.Contains(revoked.Body.String(), `"state":"revoked"`) {
		t.Fatalf("revoke override: %d body=%s", revoked.Code, revoked.Body.String())
	}
	events, err := stateStore.ListAuditEvents("", true, 100)
	if err != nil {
		t.Fatal(err)
	}
	actions := map[string]bool{}
	for _, event := range events {
		if event.TargetType == "traffic_override" || event.TargetType == "traffic_override_signing_key" {
			actions[event.Action] = true
			encoded, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), keyring.CurrentPrivateKey) {
				t.Fatal("traffic override audit metadata leaked private signing key")
			}
		}
	}
	for _, action := range []string{"traffic_override.stage", "traffic_override.signing_key.rotate", "traffic_override.revoke"} {
		if !actions[action] {
			t.Fatalf("missing traffic override audit action %q: %+v", action, actions)
		}
	}
}
