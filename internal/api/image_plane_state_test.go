package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

func TestImageReplicationPlanStateIsIdentityBoundShadowAndRecoverable(t *testing.T) {
	t.Parallel()

	storeState, server, tenantKey, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	keyring := imagePlaneStateTestKeyring()
	server.auth.PlatformComponentIdentityKeyring = keyring
	server.heartbeatAuditKeyring = trustedHeartbeatAuditTestKeyring()

	workerA := seedImageReplicationPlanState(t, storeState, server, platformAdminKey, "worker-a", "image-plan-a-1", true)
	_ = seedImageReplicationPlanState(t, storeState, server, platformAdminKey, "worker-b", "image-plan-b-1", true)
	workerAToken := issueImagePlaneStateTestToken(t, keyring, "worker-a")

	response := performJSONRequest(t, server, http.MethodGet, "/v1/image-plane/replication-plan", workerAToken, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("expected state status %d, got %d body=%s", http.StatusOK, response.Code, response.Body.String())
	}
	if cacheControl := response.Header().Get("Cache-Control"); cacheControl != "private, no-store, max-age=0" {
		t.Fatalf("image-plane state is cacheable: Cache-Control=%q", cacheControl)
	}
	if response.Header().Get("Pragma") != "no-cache" {
		t.Fatalf("image-plane state is missing legacy no-cache protection: headers=%v", response.Header())
	}
	var state model.ImageReplicationPlanStateResponse
	mustDecodeJSON(t, response, &state)
	if state.APIVersion != model.ImagePlaneAPIVersionV1 ||
		state.Kind != model.ImageReplicationPlanStateKind ||
		state.Component != model.PlatformConsumerComponentImageCache ||
		state.NodeID != "worker-a" ||
		state.ScopeKey != "node:worker-a" ||
		state.ArtifactKind != model.PlatformArtifactKindImageReplicationPlan ||
		state.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow ||
		state.Generation != workerA.Artifact.Generation ||
		state.ServerTime.IsZero() || state.Waited {
		t.Fatalf("unexpected versioned image-plane state envelope: %+v", state)
	}
	if state.Artifact == nil || state.Artifact.ID != workerA.Artifact.ID || state.Artifact.ScopeKey != "node:worker-a" {
		t.Fatalf("worker-a received the wrong desired artifact: %+v", state.Artifact)
	}
	if state.Release == nil || state.Release.ID != workerA.Release.ID || state.Release.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow {
		t.Fatalf("worker-a received a non-shadow release: %+v", state.Release)
	}
	if state.ExpectedConsumerSetID != workerA.ExpectedConsumerSet.ID {
		t.Fatalf("heartbeat binding=%q, want %q", state.ExpectedConsumerSetID, workerA.ExpectedConsumerSet.ID)
	}
	if state.Heartbeat == nil ||
		state.Heartbeat.ExpectedConsumerSetID != workerA.ExpectedConsumerSet.ID ||
		state.Heartbeat.ArtifactReleaseID != workerA.Release.ID ||
		state.Heartbeat.FencingToken != workerA.Release.FencingToken ||
		state.Heartbeat.SequenceFloor != 0 || state.Heartbeat.IssuedAtFloor != nil ||
		state.Heartbeat.ProtocolVersion != model.PlatformConsumerProtocolVersionV1 ||
		state.Heartbeat.SchemaVersion != model.PlatformConsumerSchemaVersionV1 {
		t.Fatalf("unexpected initial image-cache heartbeat contract: %+v", state.Heartbeat)
	}
	if state.LKG == nil || state.LKGArtifact == nil ||
		state.LKG.ArtifactID != state.LKGArtifact.ID ||
		state.LKG.Generation != state.LKGArtifact.Generation ||
		state.LKG.ContentHash != state.LKGArtifact.ContentHash ||
		state.LKGArtifact.ID != workerA.Artifact.ID {
		t.Fatalf("fresh component did not receive a coherent signed LKG input: lkg=%+v artifact=%+v", state.LKG, state.LKGArtifact)
	}
	if got := state.LKGArtifact.Content["apiVersion"]; got != model.ImagePlaneAPIVersionV1 {
		t.Fatalf("unexpected LKG artifact version %v", got)
	}
	if strings.Contains(response.Body.String(), "worker-b") {
		t.Fatalf("worker-a response disclosed worker-b state: %s", response.Body.String())
	}
	if strings.Contains(response.Body.String(), workerAToken) {
		t.Fatal("image-plane state response reflected its bearer credential")
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(keyring, workerAToken, time.Now().UTC())
	if err != nil {
		t.Fatalf("parse worker-a identity: %v", err)
	}
	heartbeatIssuedAt := time.Now().UTC().Truncate(time.Millisecond)
	heartbeat := trustedPlatformHeartbeatRequest(
		t,
		claims,
		workerA.ExpectedConsumerSet,
		heartbeatIssuedAt,
		7,
		workerA.Artifact.GenerationSequence,
		workerA.Release.FencingToken,
		"nonce-image-plane-state-0001",
	)
	acceptedHeartbeat := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", workerAToken, heartbeat)
	if acceptedHeartbeat.Code != http.StatusOK {
		t.Fatalf("server-bound image-cache heartbeat failed: status=%d body=%s", acceptedHeartbeat.Code, acceptedHeartbeat.Body.String())
	}
	stateAfterHeartbeatResponse := performJSONRequest(t, server, http.MethodGet, "/v1/image-plane/replication-plan", workerAToken, nil)
	if stateAfterHeartbeatResponse.Code != http.StatusOK {
		t.Fatalf("reload image-plane state after heartbeat: status=%d body=%s", stateAfterHeartbeatResponse.Code, stateAfterHeartbeatResponse.Body.String())
	}
	var stateAfterHeartbeat model.ImageReplicationPlanStateResponse
	mustDecodeJSON(t, stateAfterHeartbeatResponse, &stateAfterHeartbeat)
	if stateAfterHeartbeat.Heartbeat == nil ||
		stateAfterHeartbeat.Heartbeat.SequenceFloor != heartbeat.Sequence ||
		stateAfterHeartbeat.Heartbeat.IssuedAtFloor == nil ||
		!stateAfterHeartbeat.Heartbeat.IssuedAtFloor.Equal(heartbeatIssuedAt) {
		t.Fatalf("image-cache restart cursor did not advance: %+v", stateAfterHeartbeat.Heartbeat)
	}
	wrongFence := heartbeat
	wrongFence.Sequence++
	wrongFence.IssuedAt = heartbeatIssuedAt.Add(time.Second)
	wrongFence.Nonce = "nonce-image-plane-state-0002"
	wrongFence.FencingToken++
	wrongFence.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(wrongFence)
	if err != nil {
		t.Fatalf("hash wrong-fence heartbeat: %v", err)
	}
	rejectedFence := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", workerAToken, wrongFence)
	if rejectedFence.Code != http.StatusConflict || !strings.Contains(rejectedFence.Body.String(), "active artifact release") {
		t.Fatalf("client-selected heartbeat fence was not rejected: status=%d body=%s", rejectedFence.Code, rejectedFence.Body.String())
	}
	tamperedArtifact := *state.Artifact
	tamperedArtifact.Content = map[string]any{"apiVersion": model.ImagePlaneAPIVersionV1, "kind": model.ImageReplicationPlanKind, "tampered": true}
	if err := server.validateImageReplicationPlanDesiredState(
		platformcontrol.PlatformComponentIdentityClaims{
			CredentialID:  "image-cache:worker-a",
			Component:     model.PlatformConsumerComponentImageCache,
			NodeID:        "worker-a",
			ScopeKey:      "node:worker-a",
			ArtifactKinds: []string{model.PlatformArtifactKindImageReplicationPlan},
		},
		tamperedArtifact,
		*state.Release,
	); err == nil {
		t.Fatal("tampered signed desired artifact passed the image-plane read boundary")
	}

	for _, path := range []string{
		"/v1/image-plane/replication-plan?scope_key=node:worker-b",
		"/v1/image-plane/replication-plan?channel=full",
		"/v1/image-plane/replication-plan?artifact_kind=node_desired_state",
		"/v1/image-plane/replication-plan?current_generation=a&current_generation=b",
	} {
		injected := performJSONRequest(t, server, http.MethodGet, path, workerAToken, nil)
		if injected.Code != http.StatusBadRequest {
			t.Fatalf("caller-controlled trust boundary %q must be rejected, got %d body=%s", path, injected.Code, injected.Body.String())
		}
		if injected.Header().Get("Cache-Control") != "private, no-store, max-age=0" {
			t.Fatalf("rejected image-plane response is cacheable: headers=%v", injected.Header())
		}
	}

	wrongComponentToken, err := platformcontrol.IssuePlatformComponentIdentity(keyring, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID:  "edge-credential-image-state-test",
		Component:     model.PlatformConsumerComponentEdgeWorker,
		NodeID:        "edge-a",
		ScopeKey:      "global",
		ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle},
	}, time.Now().UTC().Add(-time.Second), 5*time.Minute)
	if err != nil {
		t.Fatalf("issue wrong-component credential: %v", err)
	}
	wrongComponent := performJSONRequest(t, server, http.MethodGet, "/v1/image-plane/replication-plan", wrongComponentToken, nil)
	if wrongComponent.Code != http.StatusForbidden {
		t.Fatalf("another platform component must be forbidden, got %d body=%s", wrongComponent.Code, wrongComponent.Body.String())
	}
	tenantCredential := performJSONRequest(t, server, http.MethodGet, "/v1/image-plane/replication-plan", tenantKey, nil)
	if tenantCredential.Code != http.StatusUnauthorized {
		t.Fatalf("tenant API key must not enter the image-plane trust boundary, got %d body=%s", tenantCredential.Code, tenantCredential.Body.String())
	}
	if tenantCredential.Header().Get("Cache-Control") != "private, no-store, max-age=0" {
		t.Fatalf("component authentication failure is cacheable: headers=%v", tenantCredential.Header())
	}
}

func TestImageReplicationPlanStateWithoutDesiredStateIsSafeAndImmediate(t *testing.T) {
	t.Parallel()

	_, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	keyring := imagePlaneStateTestKeyring()
	server.auth.PlatformComponentIdentityKeyring = keyring
	token := issueImagePlaneStateTestToken(t, keyring, "worker-empty")

	started := time.Now()
	response := performJSONRequest(t, server, http.MethodGet,
		"/v1/image-plane/replication-plan?current_generation=missing&wait_seconds=30", token, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("expected empty state status %d, got %d body=%s", http.StatusOK, response.Code, response.Body.String())
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("missing desired state unnecessarily long-polled for %s", elapsed)
	}
	var state model.ImageReplicationPlanStateResponse
	mustDecodeJSON(t, response, &state)
	if state.Artifact != nil || state.Release != nil || state.LKG != nil || state.LKGArtifact != nil ||
		state.ExpectedConsumerSetID != "" || state.Heartbeat != nil || state.Generation != "" || state.Waited ||
		state.NodeID != "worker-empty" || state.ScopeKey != "node:worker-empty" {
		t.Fatalf("unexpected empty image-plane state: %+v", state)
	}
}

func TestImageReplicationPlanStateLongPollStopsOnRequestCancellation(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	keyring := imagePlaneStateTestKeyring()
	server.auth.PlatformComponentIdentityKeyring = keyring
	seeded := seedImageReplicationPlanState(t, storeState, server, platformAdminKey, "worker-cancel", "image-plan-cancel-1", false)
	token := issueImagePlaneStateTestToken(t, keyring, "worker-cancel")

	ctx, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(50*time.Millisecond, cancel)
	defer timer.Stop()
	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/image-plane/replication-plan?current_generation="+seeded.Artifact.Generation+"&wait_seconds=30",
		nil,
	).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()

	started := time.Now()
	server.Handler().ServeHTTP(recorder, req)
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("canceled image-plane long poll took %s to stop", elapsed)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("canceled request wrote a partial state response: %s", recorder.Body.String())
	}
}

type seededImageReplicationPlanState struct {
	Artifact            model.PlatformArtifact
	Release             model.PlatformArtifactRelease
	ExpectedConsumerSet model.PlatformExpectedConsumerSet
}

func seedImageReplicationPlanState(
	t *testing.T,
	storeState *store.Store,
	server *Server,
	platformAdminKey string,
	nodeID string,
	generation string,
	verifyLKG bool,
) seededImageReplicationPlanState {
	t.Helper()
	nodeID = strings.ToLower(strings.TrimSpace(nodeID))
	scopeKey := "node:" + nodeID
	create := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", platformAdminKey, model.PlatformArtifactCreateRequest{
		ArtifactKind: model.PlatformArtifactKindImageReplicationPlan,
		Scope: model.PlatformArtifactScope{
			ScopeType: "node",
			Key:       scopeKey,
			NodeID:    nodeID,
		},
		Generation: generation,
		Content: map[string]any{
			"apiVersion": model.ImagePlaneAPIVersionV1,
			"kind":       model.ImageReplicationPlanKind,
			"spec": map[string]any{
				"nodeID": nodeID,
				"images": []any{},
			},
		},
		CompatibilityFloor: "v1",
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("create %s image plan: status=%d body=%s", nodeID, create.Code, create.Body.String())
	}
	var created model.PlatformArtifactResponse
	mustDecodeJSON(t, create, &created)
	validation := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/validate", platformAdminKey, model.PlatformArtifactValidateRequest{DryRun: false})
	if validation.Code != http.StatusOK {
		t.Fatalf("validate %s image plan: status=%d body=%s", nodeID, validation.Code, validation.Body.String())
	}
	var validated model.PlatformArtifactValidationResponse
	mustDecodeJSON(t, validation, &validated)
	if !validated.Pass {
		t.Fatalf("image plan validation failed: %+v", validated.Results)
	}
	releaseResponse := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/release", platformAdminKey, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		IdempotencyKey: "image-plan-shadow-" + nodeID + "-" + generation,
		Reason:         "image-plane state API test",
	})
	if releaseResponse.Code != http.StatusOK {
		t.Fatalf("release %s image plan: status=%d body=%s", nodeID, releaseResponse.Code, releaseResponse.Body.String())
	}
	var released model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, releaseResponse, &released)
	if verifyLKG {
		released = verifyPlatformArtifactReleaseAPI(t, server, platformAdminKey, released.Release, true)
	}

	now := time.Now().UTC()
	expectedSet, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
		ReleaseSetID:      "image-plan-release-set-" + nodeID + "-" + generation,
		ArtifactReleaseID: released.Release.ID,
		ArtifactKind:      model.PlatformArtifactKindImageReplicationPlan,
		Scope:             released.Artifact.Scope,
		ScopeKey:          scopeKey,
		Generation:        released.Artifact.Generation,
		Revision:          1,
		PreparedAt:        now,
		Topology: platformcontrol.ExpectedConsumerTopology{NodeUpdaters: []model.NodeUpdater{{
			ID:              "updater-" + nodeID,
			ClusterNodeName: nodeID,
			Status:          model.NodeUpdaterStatusActive,
		}}},
	})
	if err != nil {
		t.Fatalf("build %s expected image-cache set: %v", nodeID, err)
	}
	if _, err := storeState.CreatePlatformExpectedConsumerSet(expectedSet); err != nil {
		t.Fatalf("persist %s expected image-cache set: %v", nodeID, err)
	}
	return seededImageReplicationPlanState{
		Artifact:            released.Artifact,
		Release:             released.Release,
		ExpectedConsumerSet: expectedSet,
	}
}

func imagePlaneStateTestKeyring() platformcontrol.PlatformComponentIdentityKeyring {
	return platformcontrol.DerivePlatformComponentIdentityKeyring(
		"image-plane-state-test-signing-key",
		"image-plane-state-test-v1",
		"",
		"",
		nil,
	)
}

func issueImagePlaneStateTestToken(
	t *testing.T,
	keyring platformcontrol.PlatformComponentIdentityKeyring,
	nodeID string,
) string {
	t.Helper()
	nodeID = strings.ToLower(strings.TrimSpace(nodeID))
	token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID:  model.PlatformConsumerComponentImageCache + ":" + nodeID,
		Component:     model.PlatformConsumerComponentImageCache,
		NodeID:        nodeID,
		ScopeKey:      "node:" + nodeID,
		ArtifactKinds: []string{model.PlatformArtifactKindImageReplicationPlan},
	}, time.Now().UTC().Add(-time.Second), 5*time.Minute)
	if err != nil {
		t.Fatalf("issue %s image-cache identity: %v", nodeID, err)
	}
	return token
}
