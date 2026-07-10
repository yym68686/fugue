package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestRequirePlatformComponentBindsVerifiedClaims(t *testing.T) {
	t.Parallel()

	keyring := platformComponentAuthTestKeyring()
	now := time.Now().UTC()
	token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID:  "edge-credential-1",
		Component:     model.PlatformConsumerComponentEdgeWorker,
		NodeID:        "edge-node-1",
		ScopeKey:      "global",
		ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle},
	}, now, time.Minute)
	if err != nil {
		t.Fatalf("issue platform component identity: %v", err)
	}

	authenticator := &Authenticator{PlatformComponentIdentityKeyring: keyring}
	handler := authenticator.RequirePlatformComponent(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := PlatformComponentIdentityFromContext(r.Context())
		if !ok {
			t.Fatal("verified platform component claims are missing from context")
		}
		if claims.CredentialID != "edge-credential-1" ||
			claims.Component != model.PlatformConsumerComponentEdgeWorker ||
			claims.NodeID != "edge-node-1" ||
			claims.ScopeKey != "global" {
			t.Fatalf("unexpected platform component claims: %+v", claims)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusNoContent, recorder.Code, recorder.Body.String())
	}
}

func TestRequirePlatformComponentRejectsOtherBearerCredentials(t *testing.T) {
	t.Parallel()

	authenticator := &Authenticator{PlatformComponentIdentityKeyring: platformComponentAuthTestKeyring()}
	handler := authenticator.RequirePlatformComponent(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("handler must not run for a non-component bearer credential")
	}))

	for _, token := range []string{"tenant-api-key", "runtime-key", "node-updater-key", ""} {
		req := httptest.NewRequest(http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("token %q: expected status %d, got %d", token, http.StatusUnauthorized, recorder.Code)
		}
	}
}

func TestRequirePlatformComponentIsFailClosedWithoutKeyring(t *testing.T) {
	t.Parallel()

	handler := (&Authenticator{}).RequirePlatformComponent(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("handler must not run without a configured keyring")
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", nil)
	req.Header.Set("Authorization", "Bearer fugue_pc_v1.invalid.invalid.invalid")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, recorder.Code)
	}
}

func platformComponentAuthTestKeyring() platformcontrol.PlatformComponentIdentityKeyring {
	return platformcontrol.PlatformComponentIdentityKeyring{
		ActiveKeyID: "component-key-1",
		Keys: map[string]string{
			"component-key-1": "component-signing-secret",
		},
	}
}
