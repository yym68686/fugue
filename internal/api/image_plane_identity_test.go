package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

func TestIssueNodeUpdaterImageCacheIdentityBindsAuthenticatedNode(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	_, nodeKeySecret, err := stateStore.CreateScopedNodeKey("", "platform", model.NodeKeyScopePlatformNode)
	if err != nil {
		t.Fatalf("create platform node key: %v", err)
	}
	_, updaterToken, err := stateStore.EnrollNodeUpdater(
		nodeKeySecret,
		"Worker-A",
		"198.51.100.10",
		nil,
		"worker-a",
		"machine-a",
		model.NodeUpdaterCurrentVersion,
		"join-v1",
		[]string{"heartbeat", model.NodeUpdaterCapabilityImageCachePlatformIdentityV1},
	)
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}

	keyring := platformcontrol.DerivePlatformComponentIdentityKeyring(
		"image-plane-secret",
		"image-plane-key-1",
		"",
		"",
		nil,
	)
	authenticator := auth.New(stateStore, "bootstrap-secret")
	authenticator.PlatformComponentIdentityKeyring = keyring
	server := NewServer(stateStore, authenticator, nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/node-updater/image-cache/identity", updaterToken, nil)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("Cache-Control") != "no-store" || recorder.Header().Get("Pragma") != "no-cache" {
		t.Fatalf("credential response is cacheable: headers=%v", recorder.Header())
	}
	var response model.PlatformComponentCredentialResponse
	mustDecodeJSON(t, recorder, &response)
	credential := response.Credential
	if credential.APIVersion != model.PlatformComponentCredentialAPIVersionV1 ||
		credential.Kind != model.PlatformComponentCredentialKind ||
		credential.CredentialID != "image-cache:worker-a" ||
		credential.Component != model.PlatformConsumerComponentImageCache ||
		credential.NodeID != "worker-a" ||
		credential.ScopeKey != "node:worker-a" ||
		len(credential.ArtifactKinds) != 1 ||
		credential.ArtifactKinds[0] != model.PlatformArtifactKindImageReplicationPlan ||
		credential.Token == "" || credential.TokenID == "" {
		t.Fatalf("unexpected image-cache credential: %+v", credential)
	}
	if !credential.RenewAfter.After(credential.IssuedAt) || !credential.RenewAfter.Before(credential.ExpiresAt) {
		t.Fatalf("credential renewal window is invalid: %+v", credential)
	}
	if got := credential.ExpiresAt.Sub(credential.IssuedAt); got != imageCachePlatformIdentityTTL {
		t.Fatalf("credential TTL=%s, want %s", got, imageCachePlatformIdentityTTL)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(keyring, credential.Token, time.Now().UTC())
	if err != nil {
		t.Fatalf("parse issued credential: %v", err)
	}
	if claims.TokenID != credential.TokenID || claims.CredentialID != credential.CredentialID {
		t.Fatalf("response/token identity mismatch: claims=%+v credential=%+v", claims, credential)
	}
	if _, err := platformcontrol.BindPlatformConsumerHeartbeat(claims, platformcontrol.PlatformConsumerHeartbeatEnvelope{
		ConsumerID:   "image-cache:worker-b",
		Component:    model.PlatformConsumerComponentImageCache,
		NodeID:       "worker-b",
		ArtifactKind: model.PlatformArtifactKindImageReplicationPlan,
		ScopeKey:     "node:worker-b",
	}); !errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatImpersonation) {
		t.Fatalf("issued credential was usable for another node: %v", err)
	}

	events, err := stateStore.ListAuditEvents("", true, 20)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	found := false
	for _, event := range events {
		if event.Action != "image_cache.platform_identity.issued" {
			continue
		}
		found = true
		metadata, _ := json.Marshal(event.Metadata)
		if strings.Contains(string(metadata), credential.Token) {
			t.Fatalf("audit metadata leaked credential token: %s", metadata)
		}
		if event.TargetID != credential.CredentialID || event.Metadata["token_id"] != credential.TokenID {
			t.Fatalf("unexpected credential audit event: %+v", event)
		}
	}
	if !found {
		t.Fatal("missing image-cache credential audit event")
	}
}

func TestImageCachePlatformIdentityWindowCoversNodeUpdaterCadence(t *testing.T) {
	t.Parallel()

	const (
		pollInterval    = 5 * time.Minute
		randomizedDelay = 30 * time.Second
		minimumValidity = 30 * time.Second
	)
	maximumCadence := pollInterval + randomizedDelay
	if imageCachePlatformIdentityRenewAfter > pollInterval {
		t.Fatalf("renewal boundary %s exceeds the node updater poll interval %s", imageCachePlatformIdentityRenewAfter, pollInterval)
	}
	if imageCachePlatformIdentityTTL < 2*maximumCadence+minimumValidity {
		t.Fatalf("credential TTL %s cannot tolerate two failed refresh cycles at cadence %s", imageCachePlatformIdentityTTL, maximumCadence)
	}
	var server Server
	joinScript := server.joinClusterInstallScript("https://api.fugue.pro")
	for _, contract := range []string{
		`FUGUE_NODE_UPDATER_POLL_INTERVAL="${FUGUE_NODE_UPDATER_POLL_INTERVAL:-5min}"`,
		`RandomizedDelaySec=30s`,
	} {
		if !strings.Contains(joinScript, contract) {
			t.Fatalf("join script drifted from the credential cadence contract: missing %q", contract)
		}
	}
}

func TestIssueNodeUpdaterImageCacheIdentityFailsClosedWithoutSigner(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	_, nodeKeySecret, err := stateStore.CreateScopedNodeKey("", "platform", model.NodeKeyScopePlatformNode)
	if err != nil {
		t.Fatalf("create platform node key: %v", err)
	}
	_, updaterToken, err := stateStore.EnrollNodeUpdater(nodeKeySecret, "worker-a", "198.51.100.10", nil, "worker-a", "machine-a", model.NodeUpdaterCurrentVersion, "join-v1", []string{"heartbeat", model.NodeUpdaterCapabilityImageCachePlatformIdentityV1})
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}
	authenticator := auth.New(stateStore, "bootstrap-secret")
	server := NewServer(stateStore, authenticator, nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/node-updater/image-cache/identity", updaterToken, nil)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("Retry-After") != "30" || strings.Contains(recorder.Body.String(), "fugue_pc_v1") {
		t.Fatalf("signer failure leaked details or lacked retry guidance: headers=%v body=%s", recorder.Header(), recorder.Body.String())
	}
}

func TestIssueNodeUpdaterImageCacheIdentityRejectsNonUpdaterCredentials(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	authenticator := auth.New(stateStore, "bootstrap-secret")
	authenticator.PlatformComponentIdentityKeyring = platformcontrol.DerivePlatformComponentIdentityKeyring("secret", "key", "", "", nil)
	server := NewServer(stateStore, authenticator, nil, ServerConfig{})
	for _, test := range []struct {
		name   string
		token  string
		status int
	}{
		{name: "bootstrap", token: "bootstrap-secret", status: http.StatusForbidden},
		{name: "invalid", token: "not-a-credential", status: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := performJSONRequest(t, server, http.MethodPost, "/v1/node-updater/image-cache/identity", test.token, nil)
			if recorder.Code != test.status {
				t.Fatalf("expected status %d, got %d body=%s", test.status, recorder.Code, recorder.Body.String())
			}
		})
	}
	_, nodeKeySecret, err := stateStore.CreateScopedNodeKey("", "platform", model.NodeKeyScopePlatformNode)
	if err != nil {
		t.Fatalf("create platform node key: %v", err)
	}
	_, legacyUpdaterToken, err := stateStore.EnrollNodeUpdater(nodeKeySecret, "worker-a", "198.51.100.10", nil, "worker-a", "machine-a", model.NodeUpdaterCurrentVersion, "join-v1", []string{"heartbeat"})
	if err != nil {
		t.Fatalf("enroll legacy-capability updater: %v", err)
	}
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/node-updater/image-cache/identity", legacyUpdaterToken, nil)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected unsupported updater capability status %d, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}
}
