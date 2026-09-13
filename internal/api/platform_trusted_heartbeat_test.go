package api

import (
	"net/http"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

func TestTrustedPlatformConsumerHeartbeatEndpointBindsIdentityAndRejectsReplay(t *testing.T) {
	t.Parallel()

	storeState, server, tenantKey, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	keyring := platformcontrol.PlatformComponentIdentityKeyring{
		ActiveKeyID: "component-key-1",
		Keys: map[string]string{
			"component-key-1": "component-signing-secret",
		},
	}
	server.auth.PlatformComponentIdentityKeyring = keyring
	server.heartbeatAuditKeyring = trustedHeartbeatAuditTestKeyring()
	now := time.Now().UTC().Truncate(time.Second)
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
		ReleaseSetID: "release-set-api-trusted",
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:     "global",
		Generation:   "generation-42",
		PreparedAt:   now,
		Topology: platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{
			ID: "edge-node-api-trusted", EdgeGroupID: "edge-group-api-trusted", Country: "US",
		}}},
	})
	if err != nil {
		t.Fatalf("build expected consumer set: %v", err)
	}
	if _, err := storeState.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatalf("persist expected consumer set: %v", err)
	}
	token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID:  "credential-api-trusted",
		Component:     model.PlatformConsumerComponentEdgeWorker,
		NodeID:        "edge-node-api-trusted",
		ScopeKey:      "global",
		ArtifactKinds: []string{model.PlatformArtifactKindEdgeRankingPolicy},
	}, now.Add(-time.Second), 5*time.Minute)
	if err != nil {
		t.Fatalf("issue platform component identity: %v", err)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(keyring, token, now)
	if err != nil {
		t.Fatalf("parse platform component identity: %v", err)
	}

	boundHeartbeat := trustedPlatformHeartbeatRequest(t, claims, set, now, 12, 42, 8, "nonce-value-api-0001")
	heartbeat := boundHeartbeat
	heartbeat.ConsumerID = ""
	heartbeat.Component = ""
	heartbeat.NodeID = ""
	heartbeat.ScopeKey = ""
	heartbeat.ReleaseSetID = ""
	heartbeat.DesiredGeneration = ""
	accepted := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", token, heartbeat)
	if accepted.Code != http.StatusOK {
		t.Fatalf("expected trusted heartbeat status %d, got %d body=%s", http.StatusOK, accepted.Code, accepted.Body.String())
	}
	var acceptedResponse model.PlatformConsumerHeartbeatResponse
	mustDecodeJSON(t, accepted, &acceptedResponse)
	if !acceptedResponse.Consumer.IdentityVerified ||
		acceptedResponse.Consumer.ConsumerID != model.PlatformConsumerComponentEdgeWorker+":edge-node-api-trusted" ||
		acceptedResponse.Consumer.Component != model.PlatformConsumerComponentEdgeWorker ||
		acceptedResponse.Consumer.NodeID != "edge-node-api-trusted" ||
		acceptedResponse.Consumer.ScopeKey != "global" ||
		acceptedResponse.Consumer.ReleaseSetID != set.ReleaseSetID ||
		acceptedResponse.Consumer.DesiredGeneration != set.ExpectedGeneration {
		t.Fatalf("trusted heartbeat was not server-bound: %+v", acceptedResponse.Consumer)
	}
	auditEvents, err := storeState.ListAuditEvents("", true, 100)
	if err != nil {
		t.Fatalf("list trusted heartbeat audit: %v", err)
	}
	chainID := "platform-consumer-heartbeat:" + acceptedResponse.Consumer.ID
	var heartbeatAudit []model.AuditEvent
	for _, event := range auditEvents {
		if event.ChainID == chainID {
			heartbeatAudit = append(heartbeatAudit, event)
		}
	}
	if len(heartbeatAudit) != 1 || heartbeatAudit[0].Action != "platform_consumer.heartbeat_accepted" ||
		heartbeatAudit[0].Metadata["evidence_hash"] != acceptedResponse.Consumer.EvidenceHash {
		t.Fatalf("trusted endpoint did not atomically persist heartbeat audit: %+v", heartbeatAudit)
	}
	if err := platformsafety.VerifyTamperEvidentAuditChain(heartbeatAudit, chainID, server.heartbeatAuditKeyring); err != nil {
		t.Fatalf("verify trusted endpoint heartbeat audit: %v", err)
	}

	replayed := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", token, heartbeat)
	if replayed.Code != http.StatusConflict {
		t.Fatalf("replayed heartbeat must be rejected with %d, got %d body=%s", http.StatusConflict, replayed.Code, replayed.Body.String())
	}
	auditEvents, err = storeState.ListAuditEvents("", true, 100)
	if err != nil {
		t.Fatalf("list heartbeat audit after replay: %v", err)
	}
	heartbeatAudit = heartbeatAudit[:0]
	for _, event := range auditEvents {
		if event.ChainID == chainID {
			heartbeatAudit = append(heartbeatAudit, event)
		}
	}
	if len(heartbeatAudit) != 1 {
		t.Fatalf("rejected replay must not append a trusted audit event: %+v", heartbeatAudit)
	}

	impersonationTests := []struct {
		name   string
		nonce  string
		mutate func(*platformcontrol.PlatformConsumerHeartbeatEnvelope)
	}{
		{
			name:  "component",
			nonce: "nonce-value-api-0002",
			mutate: func(candidate *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
				candidate.Component = model.PlatformConsumerComponentDNSServer
				candidate.ConsumerID = model.PlatformConsumerComponentDNSServer + ":" + claims.NodeID
			},
		},
		{
			name:  "node",
			nonce: "nonce-value-api-0003",
			mutate: func(candidate *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
				candidate.NodeID = "other-edge-node"
				candidate.ConsumerID = claims.Component + ":other-edge-node"
			},
		},
		{
			name:  "scope",
			nonce: "nonce-value-api-0004",
			mutate: func(candidate *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
				candidate.ScopeKey = "other-scope"
			},
		},
	}
	for index, test := range impersonationTests {
		test := test
		t.Run("rejects "+test.name+" impersonation", func(t *testing.T) {
			impersonated := boundHeartbeat
			impersonated.Sequence += int64(index + 1)
			impersonated.Nonce = test.nonce
			test.mutate(&impersonated)
			evidenceHash, hashErr := platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(impersonated)
			if hashErr != nil {
				t.Fatalf("compute valid evidence hash for %s impersonation: %v", test.name, hashErr)
			}
			impersonated.EvidenceHash = evidenceHash
			response := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", token, impersonated)
			if response.Code != http.StatusForbidden {
				t.Fatalf("cross-%s impersonation with a valid evidence hash must be rejected with %d, got %d body=%s", test.name, http.StatusForbidden, response.Code, response.Body.String())
			}
		})
	}

	tenantResponse := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", tenantKey, heartbeat)
	if tenantResponse.Code != http.StatusUnauthorized {
		t.Fatalf("tenant API key must be rejected with %d, got %d body=%s", http.StatusUnauthorized, tenantResponse.Code, tenantResponse.Body.String())
	}
}

func TestTrustedPlatformConsumerHeartbeatEndpointRejectsFutureAndGenerationRollback(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	keyring := platformcontrol.PlatformComponentIdentityKeyring{
		ActiveKeyID: "component-key-1",
		Keys: map[string]string{
			"component-key-1": "component-signing-secret",
		},
	}
	server.auth.PlatformComponentIdentityKeyring = keyring
	server.heartbeatAuditKeyring = trustedHeartbeatAuditTestKeyring()
	now := time.Now().UTC().Truncate(time.Second)
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
		ReleaseSetID: "release-set-api-monotonic",
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:     "global",
		Generation:   "generation-42",
		PreparedAt:   now,
		Topology: platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{
			ID: "edge-node-api-monotonic", EdgeGroupID: "edge-group-api-monotonic", Country: "US",
		}}},
	})
	if err != nil {
		t.Fatalf("build expected consumer set: %v", err)
	}
	if _, err := storeState.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatalf("persist expected consumer set: %v", err)
	}
	token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID:  "credential-api-monotonic",
		Component:     model.PlatformConsumerComponentEdgeWorker,
		NodeID:        "edge-node-api-monotonic",
		ScopeKey:      "global",
		ArtifactKinds: []string{model.PlatformArtifactKindEdgeRankingPolicy},
	}, now.Add(-time.Second), 5*time.Minute)
	if err != nil {
		t.Fatalf("issue platform component identity: %v", err)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(keyring, token, now)
	if err != nil {
		t.Fatalf("parse platform component identity: %v", err)
	}

	acceptedHeartbeat := trustedPlatformHeartbeatRequest(t, claims, set, now, 20, 42, 8, "nonce-value-api-1001")
	accepted := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", token, acceptedHeartbeat)
	if accepted.Code != http.StatusOK {
		t.Fatalf("seed heartbeat failed: status=%d body=%s", accepted.Code, accepted.Body.String())
	}

	rollbackHeartbeat := trustedPlatformHeartbeatRequest(t, claims, set, now.Add(time.Second), 21, 41, 8, "nonce-value-api-1002")
	rollbackResponse := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", token, rollbackHeartbeat)
	if rollbackResponse.Code != http.StatusConflict {
		t.Fatalf("generation rollback must be rejected with %d, got %d body=%s", http.StatusConflict, rollbackResponse.Code, rollbackResponse.Body.String())
	}

	futureHeartbeat := trustedPlatformHeartbeatRequest(t, claims, set, now.Add(2*time.Minute), 22, 43, 8, "nonce-value-api-1003")
	futureResponse := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", token, futureHeartbeat)
	if futureResponse.Code != http.StatusUnprocessableEntity {
		t.Fatalf("future heartbeat must be rejected with %d, got %d body=%s", http.StatusUnprocessableEntity, futureResponse.Code, futureResponse.Body.String())
	}
}

func TestTrustedPlatformConsumerHeartbeatEndpointFailsClosedWithoutAuditKey(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	keyring := platformcontrol.PlatformComponentIdentityKeyring{
		ActiveKeyID: "component-key-audit-required",
		Keys: map[string]string{
			"component-key-audit-required": "component-signing-secret-audit-required",
		},
	}
	server.auth.PlatformComponentIdentityKeyring = keyring
	server.heartbeatAuditKeyring = bundleauth.Keyring{}
	now := time.Now().UTC().Truncate(time.Second)
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
		ReleaseSetID: "release-set-audit-required",
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:     "global",
		Generation:   "generation-audit-required",
		PreparedAt:   now,
		Topology: platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{
			ID: "edge-audit-required", EdgeGroupID: "edge-group-audit-required", Country: "US",
		}}},
	})
	if err != nil {
		t.Fatalf("build expected consumer set: %v", err)
	}
	if _, err := storeState.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatalf("persist expected consumer set: %v", err)
	}
	token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID:  "credential-audit-required",
		Component:     model.PlatformConsumerComponentEdgeWorker,
		NodeID:        "edge-audit-required",
		ScopeKey:      "global",
		ArtifactKinds: []string{model.PlatformArtifactKindEdgeRankingPolicy},
	}, now.Add(-time.Second), 5*time.Minute)
	if err != nil {
		t.Fatalf("issue component identity: %v", err)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(keyring, token, now)
	if err != nil {
		t.Fatalf("parse component identity: %v", err)
	}
	heartbeat := trustedPlatformHeartbeatRequest(t, claims, set, now, 1, 1, 1, "nonce-audit-required")
	response := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", token, heartbeat)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("missing audit key must fail closed with %d, got %d body=%s", http.StatusInternalServerError, response.Code, response.Body.String())
	}
	consumers, err := storeState.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
	if err != nil {
		t.Fatalf("list consumers after audit failure: %v", err)
	}
	if len(consumers) != 0 {
		t.Fatalf("audit failure must roll back trusted heartbeat: %+v", consumers)
	}
}

func trustedHeartbeatAuditTestKeyring() bundleauth.Keyring {
	return bundleauth.NewKeyring(
		"trusted-heartbeat-audit-test-key",
		"heartbeat-audit:test-current",
		"",
		"",
		nil,
	)
}

func trustedPlatformHeartbeatRequest(
	t *testing.T,
	claims platformcontrol.PlatformComponentIdentityClaims,
	set model.PlatformExpectedConsumerSet,
	issuedAt time.Time,
	sequence int64,
	generationSequence int64,
	fencingToken int64,
	nonce string,
) platformcontrol.PlatformConsumerHeartbeatEnvelope {
	t.Helper()
	heartbeat, err := platformcontrol.BindPlatformConsumerHeartbeatToExpectedSet(claims, set, platformcontrol.PlatformConsumerHeartbeatEnvelope{
		ArtifactKind:       set.ArtifactKind,
		FencingToken:       fencingToken,
		ProtocolVersion:    model.PlatformConsumerProtocolVersionV1,
		SchemaVersion:      model.PlatformConsumerSchemaVersionV1,
		Sequence:           sequence,
		IssuedAt:           issuedAt,
		Nonce:              nonce,
		GenerationSequence: generationSequence,
		ActualGeneration:   set.ExpectedGeneration,
		ApplyStatus:        model.PlatformConsumerApplyStatusApplied,
		ProbeStatus:        model.PlatformConsumerProbeStatusPassed,
	})
	if err != nil {
		t.Fatalf("bind trusted heartbeat: %v", err)
	}
	heartbeat.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat)
	if err != nil {
		t.Fatalf("hash trusted heartbeat: %v", err)
	}
	return heartbeat
}
