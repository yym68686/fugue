package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/api"
	"fugue/internal/auth"
	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

const testImageCachePlatformToken = "fugue_pc_v1.test.payload.signature"

func TestImageCachePlatformCredentialIsStrictlyBoundAndFresh(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 29, 23, 0, 0, 0, time.UTC)
	root := t.TempDir()
	path := filepath.Join(root, "platform-component-credential.json")
	writeTestImageCachePlatformCredential(t, path, "worker-a", now, testImageCachePlatformToken, 0o640)
	credential, err := readImageCachePlatformCredential(path, "worker-a", now, 30*time.Second)
	if err != nil {
		t.Fatalf("read valid credential: %v", err)
	}
	if credential.Token != testImageCachePlatformToken || credential.ScopeKey != "node:worker-a" {
		t.Fatalf("unexpected credential: %+v", credential)
	}
	if _, err := readImageCachePlatformCredential(path, "worker-b", now, 30*time.Second); err == nil {
		t.Fatal("cross-node credential was accepted")
	}
	if err := os.Chmod(path, 0o660); err != nil {
		t.Fatalf("chmod credential: %v", err)
	}
	if _, err := readImageCachePlatformCredential(path, "worker-a", now, 30*time.Second); err == nil {
		t.Fatal("group-writable credential was accepted")
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatalf("restore credential mode: %v", err)
	}
	symlink := filepath.Join(root, "credential-link")
	if err := os.Symlink(path, symlink); err != nil {
		t.Fatalf("symlink credential: %v", err)
	}
	if _, err := readImageCachePlatformCredential(symlink, "worker-a", now, 30*time.Second); err == nil {
		t.Fatal("symlink credential path was accepted")
	}
	writeTestImageCachePlatformCredential(t, path, "worker-a", now.Add(-20*time.Minute), testImageCachePlatformToken, 0o640)
	if _, err := readImageCachePlatformCredential(path, "worker-a", now, 30*time.Second); err == nil {
		t.Fatal("expired credential was accepted")
	}
}

func TestImageCachePlatformPlanRequiresHTTPSByDefault(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	config := imageCachePlatformPlanConfig{
		APIBaseURL: "http://api.fugue.test", NodeID: "worker-a",
		CredentialPath: filepath.Join(root, "credential.json"), ObservationPath: filepath.Join(root, "plan.json"),
		LongPoll: time.Second, RequestTimeout: 5 * time.Second, RetryMin: time.Second,
		RetryMax: time.Minute, NoPlanRetry: time.Second, MinCredentialLife: time.Second, ArchiveLimit: 5,
	}
	if _, err := newImageCachePlatformPlanConsumer(config, nil); err == nil || !strings.Contains(err.Error(), "requires HTTPS") {
		t.Fatalf("plaintext platform API was accepted by default: %v", err)
	}
	config.APIBaseURL = "https://api.fugue.test"
	if _, err := newImageCachePlatformPlanConsumer(config, nil); err != nil {
		t.Fatalf("HTTPS platform API was rejected: %v", err)
	}
}

func TestImageCachePlatformHeartbeatEvidenceMatchesControlPlaneCanonicalContract(t *testing.T) {
	t.Parallel()
	heartbeat := imageCachePlatformHeartbeat{
		ConsumerID:                "image-cache:worker-a",
		Component:                 "image-cache",
		NodeID:                    "worker-a",
		ArtifactKind:              "image_replication_plan",
		ScopeKey:                  "node:worker-a",
		ReleaseSetID:              "release-set-a",
		ExpectedConsumerSetID:     "expected-set-a",
		FencingToken:              7,
		ProtocolVersion:           "v1",
		SchemaVersion:             "v1",
		CompatibilityCapabilities: []string{"SHADOW-OBSERVATION-V1", "shadow-observation-v1"},
		Sequence:                  8,
		IssuedAt:                  time.Date(2026, 7, 29, 23, 0, 0, 123456789, time.UTC),
		Nonce:                     "nonce-image-cache-contract-0001",
		GenerationSequence:        3,
		DesiredGeneration:         "image-plan-3",
		ActualGeneration:          "image-plan-3",
		LKGGeneration:             "image-plan-2",
		ApplyStatus:               "Observed",
		ProbeStatus:               "Passed",
		LastError:                 "",
	}
	got, err := computeImageCachePlatformHeartbeatEvidenceHash(heartbeat)
	if err != nil {
		t.Fatalf("compute image-cache evidence: %v", err)
	}
	want, err := platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(platformcontrol.PlatformConsumerHeartbeatEnvelope{
		ConsumerID:                heartbeat.ConsumerID,
		Component:                 heartbeat.Component,
		NodeID:                    heartbeat.NodeID,
		ArtifactKind:              heartbeat.ArtifactKind,
		ScopeKey:                  heartbeat.ScopeKey,
		ReleaseSetID:              heartbeat.ReleaseSetID,
		ExpectedConsumerSetID:     heartbeat.ExpectedConsumerSetID,
		FencingToken:              heartbeat.FencingToken,
		ProtocolVersion:           heartbeat.ProtocolVersion,
		SchemaVersion:             heartbeat.SchemaVersion,
		CompatibilityCapabilities: heartbeat.CompatibilityCapabilities,
		Sequence:                  heartbeat.Sequence,
		IssuedAt:                  heartbeat.IssuedAt,
		Nonce:                     heartbeat.Nonce,
		GenerationSequence:        heartbeat.GenerationSequence,
		DesiredGeneration:         heartbeat.DesiredGeneration,
		ActualGeneration:          heartbeat.ActualGeneration,
		LKGGeneration:             heartbeat.LKGGeneration,
		ApplyStatus:               heartbeat.ApplyStatus,
		ProbeStatus:               heartbeat.ProbeStatus,
		ServingLKG:                heartbeat.ServingLKG,
		LKGExpired:                heartbeat.LKGExpired,
		LastError:                 heartbeat.LastError,
	})
	if err != nil {
		t.Fatalf("compute control-plane evidence: %v", err)
	}
	if got != want {
		t.Fatalf("client/control-plane canonical evidence drift: got=%s want=%s", got, want)
	}
}

func TestImageCacheManagementHealthExposesNestedShadowStatusWithoutFailingRegistry(t *testing.T) {
	t.Parallel()
	consumer := &imageCachePlatformPlanConsumer{status: imageCachePlatformPlanStatus{
		Enabled: true, ObservationOnly: true, State: "degraded", LastError: "credential unavailable",
	}}
	cache := &imageCache{
		cacheEndpoint: "http://127.0.0.1:5000",
		clusterNode:   "worker-a",
		platformPlan:  consumer,
	}
	request := httptest.NewRequest(http.MethodGet, "http://image-cache.test/fugue/cache/v1/health", nil)
	recorder := httptest.NewRecorder()
	cache.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("shadow degradation failed registry health: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Status             string                       `json:"status"`
		PlatformPlanShadow imageCachePlatformPlanStatus `json:"platform_plan_shadow"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode management health: %v", err)
	}
	if response.Status != "ok" || response.PlatformPlanShadow.State != "degraded" || !response.PlatformPlanShadow.ObservationOnly {
		t.Fatalf("unexpected nested shadow status: %+v", response)
	}
}

func TestImageCachePlatformPlanReadinessIsIndependentAndFresh(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	request := httptest.NewRequest(http.MethodGet, "http://image-cache.test/fugue/cache/v1/platform-plan/readyz", nil)
	for name, test := range map[string]struct {
		cache *imageCache
		code  int
	}{
		"disabled": {cache: &imageCache{}, code: http.StatusServiceUnavailable},
		"configuration error": {
			cache: &imageCache{platformPlanErr: "invalid config"}, code: http.StatusServiceUnavailable,
		},
		"degraded": {
			cache: &imageCache{platformPlan: &imageCachePlatformPlanConsumer{status: imageCachePlatformPlanStatus{
				Enabled: true, ObservationOnly: true, State: "degraded", LastObservationAt: &now,
			}}},
			code: http.StatusServiceUnavailable,
		},
		"stale observation": {
			cache: &imageCache{platformPlan: &imageCachePlatformPlanConsumer{status: imageCachePlatformPlanStatus{
				Enabled: true, ObservationOnly: true, State: "observed", LastObservationAt: timePointer(now.Add(-imageCachePlatformPlanReadinessMaxAge - time.Second)),
			}}},
			code: http.StatusServiceUnavailable,
		},
		"fresh observation": {
			cache: &imageCache{platformPlan: &imageCachePlatformPlanConsumer{status: imageCachePlatformPlanStatus{
				Enabled: true, ObservationOnly: true, State: "observed", LastObservationAt: &now,
			}}},
			code: http.StatusOK,
		},
	} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			test.cache.ServeHTTP(recorder, request.Clone(context.Background()))
			if recorder.Code != test.code {
				t.Fatalf("platform-plan readiness status=%d, want %d body=%s", recorder.Code, test.code, recorder.Body.String())
			}
			liveness := httptest.NewRecorder()
			test.cache.ServeHTTP(liveness, httptest.NewRequest(http.MethodGet, "http://image-cache.test/healthz", nil))
			if liveness.Code != http.StatusOK {
				t.Fatalf("platform-plan state leaked into registry liveness: %d", liveness.Code)
			}
		})
	}
}

func timePointer(value time.Time) *time.Time {
	return &value
}

func TestImageCachePlatformShadowObservationAndHeartbeatAreAtomicAndObservationOnly(t *testing.T) {
	now := time.Date(2026, 7, 29, 23, 0, 0, 0, time.UTC)
	fixture := newTestImageCachePlatformPlan("worker-a", now, 7, 5)
	root := t.TempDir()
	credentialPath := filepath.Join(root, "identity", "platform-component-credential.json")
	observationPath := filepath.Join(root, "state", "replication-plan.json")
	writeTestImageCachePlatformCredential(t, credentialPath, "worker-a", now, testImageCachePlatformToken, 0o640)

	var getCount atomic.Int64
	var postCount atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testImageCachePlatformToken {
			t.Fatalf("unexpected authorization header %q", r.Header.Get("Authorization"))
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == imageCachePlatformPlanPath:
			getCount.Add(1)
			if r.URL.Query().Get("current_generation") != "" || r.URL.Query().Get("wait_seconds") != "" {
				t.Fatalf("forced first plan read carried unexpected long-poll query: %s", r.URL.RawQuery)
			}
			w.Header().Set("Cache-Control", "private, no-store, max-age=0")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(fixture.raw)
		case r.Method == http.MethodPost && r.URL.Path == imageCachePlatformHeartbeatPath:
			postCount.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read heartbeat: %v", err)
			}
			if bytes.Contains(body, []byte(testImageCachePlatformToken)) {
				t.Fatal("heartbeat reflected the bearer token")
			}
			var heartbeat imageCachePlatformHeartbeat
			if err := json.Unmarshal(body, &heartbeat); err != nil {
				t.Fatalf("decode heartbeat: %v", err)
			}
			if heartbeat.Sequence != 6 || heartbeat.FencingToken != 7 || heartbeat.ApplyStatus != imageCachePlatformPlanApplyObserved || heartbeat.ProbeStatus != imageCachePlatformPlanProbePassed || heartbeat.ActualGeneration != fixture.plan.Generation || heartbeat.DesiredGeneration != fixture.plan.Generation {
				t.Fatalf("unexpected observation-only heartbeat: %+v", heartbeat)
			}
			expectedHash, err := computeImageCachePlatformHeartbeatEvidenceHash(heartbeat)
			if err != nil || expectedHash != heartbeat.EvidenceHash {
				t.Fatalf("heartbeat evidence hash mismatch: expected=%s got=%s err=%v", expectedHash, heartbeat.EvidenceHash, err)
			}
			writeTestImageCachePlatformHeartbeatResponse(t, w, heartbeat)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	consumer := newTestImageCachePlatformConsumer(t, server.URL, "worker-a", credentialPath, observationPath, now, server.Client())
	cycle, err := consumer.reconcileOnce(context.Background(), true)
	if err != nil {
		t.Fatalf("reconcile shadow plan: %v", err)
	}
	if !cycle.HasDesired || cycle.Generation != fixture.plan.Generation || cycle.LKGGeneration != fixture.plan.Generation {
		t.Fatalf("unexpected reconcile cycle: %+v", cycle)
	}
	if getCount.Load() != 1 || postCount.Load() != 1 {
		t.Fatalf("unexpected API call counts: GET=%d POST=%d", getCount.Load(), postCount.Load())
	}
	body, err := os.ReadFile(observationPath)
	if err != nil {
		t.Fatalf("read persisted observation: %v", err)
	}
	if bytes.Contains(body, []byte(testImageCachePlatformToken)) || bytes.Contains(body, []byte("Bearer")) {
		t.Fatal("persisted observation contains a bearer credential")
	}
	var observation imageCachePlatformPlanObservation
	if err := json.Unmarshal(body, &observation); err != nil {
		t.Fatalf("decode persisted observation: %v", err)
	}
	if !observation.ObservationOnly || observation.Generation != fixture.plan.Generation || observation.ContentHash != fixture.plan.Artifact.ContentHash {
		t.Fatalf("unexpected persisted observation: %+v", observation)
	}
	info, err := os.Stat(observationPath)
	if err != nil {
		t.Fatalf("stat persisted observation: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("persisted observation mode=%o, want 600", info.Mode().Perm())
	}
	entries, err := os.ReadDir(observationPath + ".versions")
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected one archived observation, entries=%v err=%v", entries, err)
	}
	status := consumer.Status()
	if status.State != "observed" || status.LastHeartbeatAt == nil || status.LastObservationAt == nil || status.LastError != "" {
		t.Fatalf("unexpected consumer status: %+v", status)
	}
}

func TestImageCachePlatformShadowInteroperatesWithRealControlPlane(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	root := t.TempDir()
	stateStore := store.New(filepath.Join(root, "control-plane-state.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init control-plane store: %v", err)
	}
	identityKeyring := platformcontrol.DerivePlatformComponentIdentityKeyring(
		"image-cache-integration-identity-key",
		"image-cache-integration-identity-v1",
		"",
		"",
		nil,
	)
	authenticator := auth.New(stateStore, "integration-admin-key")
	authenticator.PlatformComponentIdentityKeyring = identityKeyring
	auditKeyring := bundleauth.NewKeyring("image-cache-integration-audit-key", "image-cache-integration-audit-v1", "", "", nil)
	server := api.NewServer(stateStore, authenticator, log.New(io.Discard, "", 0), api.ServerConfig{
		BundleSigningKey:      "image-cache-integration-artifact-key",
		BundleSigningKeyID:    "image-cache-integration-artifact-v1",
		HeartbeatAuditKeyring: auditKeyring,
	})
	nodeID := "worker-integration"
	scopeKey := "node:" + nodeID
	artifact, err := stateStore.CreatePlatformArtifact(model.PlatformArtifact{
		ID:           "artifact-image-cache-integration",
		ArtifactKind: model.PlatformArtifactKindImageReplicationPlan,
		Scope: model.PlatformArtifactScope{
			ScopeType: "node",
			Key:       scopeKey,
			NodeID:    nodeID,
		},
		ScopeKey:      scopeKey,
		SchemaVersion: model.PlatformArtifactSchemaVersionV1,
		Generation:    "image-plan-integration-1",
		Content: map[string]any{
			"apiVersion": model.ImagePlaneAPIVersionV1,
			"kind":       model.ImageReplicationPlanKind,
			"spec": map[string]any{
				"nodeID": nodeID,
				"images": []any{},
			},
		},
	})
	if err != nil {
		t.Fatalf("create integration artifact: %v", err)
	}
	artifact, err = stateStore.ValidatePlatformArtifact(artifact.ID, []model.PlatformArtifactValidationResult{{
		Name: "integration.image-plan-binding", Pass: true, Severity: model.RobustnessSeverityBlockPublish,
	}})
	if err != nil || artifact.Status != model.PlatformArtifactStatusValidated {
		t.Fatalf("validate integration artifact: status=%q err=%v", artifact.Status, err)
	}
	artifact, release, _, _, err := stateStore.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		IdempotencyKey: "image-cache-integration-shadow",
		Reason:         "image-cache shadow consumer integration test",
	}, model.Principal{ActorType: model.ActorTypeSystem, ActorID: "integration-test"})
	if err != nil {
		t.Fatalf("release integration artifact: %v", err)
	}
	expectedSet, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
		ReleaseSetID:      "release-set-image-cache-integration",
		ArtifactReleaseID: release.ID,
		ArtifactKind:      artifact.ArtifactKind,
		Scope:             artifact.Scope,
		ScopeKey:          artifact.ScopeKey,
		Generation:        artifact.Generation,
		Revision:          1,
		PreparedAt:        now,
		Topology: platformcontrol.ExpectedConsumerTopology{NodeUpdaters: []model.NodeUpdater{{
			ID: "updater-integration", ClusterNodeName: nodeID, Status: model.NodeUpdaterStatusActive,
		}}},
	})
	if err != nil {
		t.Fatalf("build integration expected set: %v", err)
	}
	if _, err := stateStore.CreatePlatformExpectedConsumerSet(expectedSet); err != nil {
		t.Fatalf("persist integration expected set: %v", err)
	}
	token, err := platformcontrol.IssuePlatformComponentIdentity(identityKeyring, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID: imageCachePlatformPlanComponent + ":" + nodeID,
		Component:    imageCachePlatformPlanComponent,
		NodeID:       nodeID,
		ScopeKey:     scopeKey,
		ArtifactKinds: []string{
			imageCachePlatformPlanArtifactKind,
		},
	}, now, 15*time.Minute)
	if err != nil {
		t.Fatalf("issue integration component identity: %v", err)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(identityKeyring, token, now)
	if err != nil {
		t.Fatalf("parse integration component identity: %v", err)
	}
	credentialPath := filepath.Join(root, "identity", "platform-component-credential.json")
	writeTestImageCachePlatformCredentialValue(t, credentialPath, imageCachePlatformCredential{
		APIVersion: imageCachePlatformPlanCredentialVersion, Kind: imageCachePlatformPlanCredentialKind,
		CredentialID: claims.CredentialID, Token: token, TokenID: claims.TokenID,
		Component: claims.Component, NodeID: claims.NodeID, ScopeKey: claims.ScopeKey,
		ArtifactKinds: claims.ArtifactKinds, IssuedAt: time.Unix(claims.IssuedAtUnix, 0).UTC(),
		ExpiresAt: time.Unix(claims.ExpiresAtUnix, 0).UTC(), RenewAfter: time.Unix(claims.IssuedAtUnix, 0).UTC().Add(5 * time.Minute),
	}, 0o640)
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	observationPath := filepath.Join(root, "image-cache", "replication-plan.json")
	consumer := newTestImageCachePlatformConsumer(t, httpServer.URL, nodeID, credentialPath, observationPath, now, httpServer.Client())
	cycle, err := consumer.reconcileOnce(context.Background(), true)
	if err != nil {
		t.Fatalf("reconcile against real control plane: %v", err)
	}
	if !cycle.HasDesired || cycle.Generation != artifact.Generation {
		t.Fatalf("unexpected real control-plane cycle: %+v", cycle)
	}
	consumers, err := stateStore.ListPlatformConsumers(model.PlatformArtifactKindImageReplicationPlan, scopeKey)
	if err != nil || len(consumers) != 1 {
		t.Fatalf("read accepted integration heartbeat: consumers=%+v err=%v", consumers, err)
	}
	accepted := consumers[0]
	if !accepted.IdentityVerified || accepted.ConsumerID != "image-cache:"+nodeID ||
		accepted.ExpectedConsumerSetID != expectedSet.ID || accepted.ReleaseSetID != expectedSet.ReleaseSetID ||
		accepted.FencingToken != release.FencingToken || accepted.Sequence != 1 ||
		accepted.DesiredGeneration != artifact.Generation || accepted.ActualGeneration != artifact.Generation ||
		accepted.ApplyStatus != imageCachePlatformPlanApplyObserved || accepted.ProbeStatus != imageCachePlatformPlanProbePassed {
		t.Fatalf("real control plane accepted unexpected heartbeat: %+v", accepted)
	}
}

func TestImageCachePlatformShadowRejectsTamperedPlanAndPreservesExistingObservation(t *testing.T) {
	now := time.Date(2026, 7, 29, 23, 0, 0, 0, time.UTC)
	fixture := newTestImageCachePlatformPlan("worker-a", now, 7, 5)
	root := t.TempDir()
	credentialPath := filepath.Join(root, "platform-component-credential.json")
	observationPath := filepath.Join(root, "replication-plan.json")
	writeTestImageCachePlatformCredential(t, credentialPath, "worker-a", now, testImageCachePlatformToken, 0o640)
	if err := writeImageCachePlatformPlanObservation(observationPath, newImageCachePlatformPlanObservation(fixture.plan, fixture.raw, now), 5); err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	before, err := os.ReadFile(observationPath)
	if err != nil {
		t.Fatalf("read seeded observation: %v", err)
	}

	tampered := fixture.plan
	tampered.Artifact.ContentHash = "sha256:" + strings.Repeat("0", 64)
	tamperedRaw, err := json.Marshal(tampered)
	if err != nil {
		t.Fatalf("marshal tampered plan: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Cache-Control", "private, no-store, max-age=0")
			_, _ = w.Write(tamperedRaw)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	consumer := newTestImageCachePlatformConsumer(t, server.URL, "worker-a", credentialPath, observationPath, now, server.Client())
	if _, err := consumer.reconcileOnce(context.Background(), true); err == nil || !strings.Contains(err.Error(), "content hash") {
		t.Fatalf("tampered plan was not rejected: %v", err)
	}
	after, err := os.ReadFile(observationPath)
	if err != nil {
		t.Fatalf("read observation after rejection: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("tampered plan changed the last-known-good observation")
	}
}

func TestImageCachePlatformShadowRefetchesCursorAfterConflict(t *testing.T) {
	now := time.Date(2026, 7, 29, 23, 0, 0, 0, time.UTC)
	fixture := newTestImageCachePlatformPlan("worker-a", now, 7, 5)
	root := t.TempDir()
	credentialPath := filepath.Join(root, "platform-component-credential.json")
	observationPath := filepath.Join(root, "replication-plan.json")
	writeTestImageCachePlatformCredential(t, credentialPath, "worker-a", now, testImageCachePlatformToken, 0o640)
	var posts atomic.Int64
	var gets atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets.Add(1)
			response := fixture.plan
			if gets.Load() > 1 {
				response.Heartbeat.SequenceFloor = 6
			}
			raw, _ := json.Marshal(response)
			w.Header().Set("Cache-Control", "private, no-store, max-age=0")
			_, _ = w.Write(raw)
			return
		}
		posts.Add(1)
		if posts.Load() == 1 {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"trusted heartbeat is not monotonic"}`))
			return
		}
		var heartbeat imageCachePlatformHeartbeat
		if err := json.NewDecoder(r.Body).Decode(&heartbeat); err != nil {
			t.Fatalf("decode retry heartbeat: %v", err)
		}
		if heartbeat.Sequence != 7 {
			t.Fatalf("retry did not use refreshed sequence floor: %+v", heartbeat)
		}
		writeTestImageCachePlatformHeartbeatResponse(t, w, heartbeat)
	}))
	defer server.Close()
	consumer := newTestImageCachePlatformConsumer(t, server.URL, "worker-a", credentialPath, observationPath, now, server.Client())
	if _, err := consumer.reconcileCycle(context.Background()); err != nil {
		t.Fatalf("conflict cursor refetch: %v", err)
	}
	if gets.Load() != 2 || posts.Load() != 2 {
		t.Fatalf("cursor conflict was not retried exactly once: GET=%d POST=%d", gets.Load(), posts.Load())
	}
}

func TestImageCachePlatformShadowCancellationStopsLongPoll(t *testing.T) {
	now := time.Date(2026, 7, 29, 23, 0, 0, 0, time.UTC)
	root := t.TempDir()
	credentialPath := filepath.Join(root, "platform-component-credential.json")
	observationPath := filepath.Join(root, "replication-plan.json")
	writeTestImageCachePlatformCredential(t, credentialPath, "worker-a", now, testImageCachePlatformToken, 0o640)
	requestStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-r.Context().Done()
	}))
	defer server.Close()
	consumer := newTestImageCachePlatformConsumer(t, server.URL, "worker-a", credentialPath, observationPath, now, server.Client())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		consumer.Run(ctx)
	}()
	select {
	case <-requestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("shadow plan request did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shadow plan consumer did not stop after lifecycle cancellation")
	}
	if _, err := os.Stat(observationPath); !os.IsNotExist(err) {
		t.Fatalf("canceled request wrote an observation: %v", err)
	}
}

func TestImageCachePlatformShadowRefusesRedirectWithBearerCredential(t *testing.T) {
	now := time.Date(2026, 7, 29, 23, 0, 0, 0, time.UTC)
	root := t.TempDir()
	credentialPath := filepath.Join(root, "platform-component-credential.json")
	observationPath := filepath.Join(root, "replication-plan.json")
	writeTestImageCachePlatformCredential(t, credentialPath, "worker-a", now, testImageCachePlatformToken, 0o640)
	var targetRequests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+imageCachePlatformPlanPath, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	consumer, err := newImageCachePlatformPlanConsumer(imageCachePlatformPlanConfig{
		APIBaseURL: redirect.URL, NodeID: "worker-a", CredentialPath: credentialPath, ObservationPath: observationPath,
		AllowInsecureHTTP: true,
		LongPoll:          time.Second, RequestTimeout: 5 * time.Second, RetryMin: 100 * time.Millisecond,
		RetryMax: time.Second, NoPlanRetry: time.Second, MinCredentialLife: time.Second, ArchiveLimit: 5,
	}, nil)
	if err != nil {
		t.Fatalf("new production HTTP client: %v", err)
	}
	consumer.now = func() time.Time { return now }
	if _, err := consumer.reconcileOnce(context.Background(), true); err == nil || !strings.Contains(err.Error(), "redirects are forbidden") {
		t.Fatalf("redirect was not rejected: %v", err)
	}
	if targetRequests.Load() != 0 {
		t.Fatalf("bearer credential followed a redirect to %d target request(s)", targetRequests.Load())
	}
}

func TestImageCachePlatformObservationRetainsPreviousGeneration(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 29, 23, 0, 0, 0, time.UTC)
	first := newTestImageCachePlatformPlan("worker-a", now, 7, 0)
	path := filepath.Join(t.TempDir(), "replication-plan.json")
	if err := writeImageCachePlatformPlanObservation(path, newImageCachePlatformPlanObservation(first.plan, first.raw, now), 5); err != nil {
		t.Fatalf("write first observation: %v", err)
	}
	secondPlan := first.plan
	secondArtifact := *first.plan.Artifact
	secondArtifact.Generation = "image-plan-worker-a-2"
	secondArtifact.GenerationSequence = 2
	secondRelease := *first.plan.Release
	secondRelease.ID = "release-worker-a-2"
	secondRelease.Generation = secondArtifact.Generation
	secondRelease.ArtifactID = secondArtifact.ID
	secondHeartbeat := *first.plan.Heartbeat
	secondHeartbeat.ArtifactReleaseID = secondRelease.ID
	secondHeartbeat.FencingToken = 8
	secondHeartbeat.SequenceFloor = 1
	secondPlan.Artifact = &secondArtifact
	secondPlan.Release = &secondRelease
	secondPlan.Heartbeat = &secondHeartbeat
	secondPlan.Generation = secondArtifact.Generation
	secondRaw, err := json.Marshal(secondPlan)
	if err != nil {
		t.Fatalf("marshal second plan: %v", err)
	}
	if err := writeImageCachePlatformPlanObservation(path, newImageCachePlatformPlanObservation(secondPlan, secondRaw, now.Add(time.Minute)), 5); err != nil {
		t.Fatalf("write second observation: %v", err)
	}
	previous, err := readImageCachePlatformPlanObservation(path+".previous", "worker-a")
	if err != nil {
		t.Fatalf("read previous observation: %v", err)
	}
	current, err := readImageCachePlatformPlanObservation(path, "worker-a")
	if err != nil {
		t.Fatalf("read current observation: %v", err)
	}
	if previous.Generation != first.plan.Generation || current.Generation != secondPlan.Generation {
		t.Fatalf("generation history drifted: previous=%q current=%q", previous.Generation, current.Generation)
	}
	entries, err := os.ReadDir(path + ".versions")
	if err != nil || len(entries) != 2 {
		t.Fatalf("expected two generation archives, entries=%v err=%v", entries, err)
	}
}

type testImageCachePlatformPlanFixture struct {
	plan imageCachePlatformPlanResponse
	raw  []byte
}

func newTestImageCachePlatformPlan(nodeID string, now time.Time, fencingToken, sequenceFloor int64) testImageCachePlatformPlanFixture {
	scopeKey := "node:" + nodeID
	content := map[string]any{
		"apiVersion": imageCachePlatformPlanAPIVersion,
		"kind":       imageCachePlatformPlanKind,
		"spec": map[string]any{
			"nodeID": nodeID,
			"images": []any{},
		},
	}
	contentRaw, _ := json.Marshal(content)
	contentHash := sha256.Sum256(contentRaw)
	artifact := &imageCachePlatformArtifact{
		ID:                 "artifact-" + nodeID,
		ArtifactKind:       imageCachePlatformPlanArtifactKind,
		Scope:              imageCachePlatformArtifactScope{ScopeType: "node", Key: scopeKey, NodeID: nodeID},
		ScopeKey:           scopeKey,
		SchemaVersion:      imageCachePlatformPlanArtifactSchema,
		Generation:         "image-plan-" + nodeID + "-1",
		GenerationSequence: 1,
		Status:             imageCachePlatformPlanArtifactStatus,
		ContentHash:        "sha256:" + hex.EncodeToString(contentHash[:]),
		Content:            content,
		Provenance: imageCachePlatformArtifactProvenance{
			Issuer: "fugue-control-plane", KeyID: "key-1", Algorithm: "hmac-sha256", Signature: "sig", SignedAt: now,
		},
	}
	release := &imageCachePlatformRelease{
		ID: "release-" + nodeID, ArtifactID: artifact.ID, ArtifactKind: artifact.ArtifactKind,
		Scope: artifact.Scope, ScopeKey: scopeKey, Generation: artifact.Generation,
		ReleaseChannel: imageCachePlatformPlanReleaseChannel, Status: imageCachePlatformPlanReleaseStatus,
		FencingToken: fencingToken, VerificationState: imageCachePlatformPlanVerificationNew,
	}
	lkg := &imageCachePlatformLKG{
		ID: "lkg-" + nodeID, ArtifactID: artifact.ID, ArtifactKind: artifact.ArtifactKind,
		Scope: artifact.Scope, ScopeKey: scopeKey, SchemaVersion: imageCachePlatformPlanArtifactSchema,
		Generation: artifact.Generation, GenerationSequence: artifact.GenerationSequence,
		ContentHash: artifact.ContentHash, ExpiresAt: now.Add(time.Hour),
	}
	issuedFloor := now.Add(-time.Second)
	plan := imageCachePlatformPlanResponse{
		APIVersion: imageCachePlatformPlanAPIVersion, Kind: imageCachePlatformPlanStateKind,
		Component: imageCachePlatformPlanComponent, NodeID: nodeID, ScopeKey: scopeKey,
		ArtifactKind: imageCachePlatformPlanArtifactKind, ReleaseChannel: imageCachePlatformPlanReleaseChannel,
		Artifact: artifact, Release: release, LKG: lkg, LKGArtifact: artifact,
		ExpectedConsumerSetID: "expected-" + nodeID, Generation: artifact.Generation, ServerTime: now,
		Heartbeat: &imageCachePlatformHeartbeatContract{
			ExpectedConsumerSetID: "expected-" + nodeID, ReleaseSetID: "set-" + nodeID,
			ArtifactReleaseID: release.ID, FencingToken: fencingToken, SequenceFloor: sequenceFloor,
			IssuedAtFloor: &issuedFloor, ProtocolVersion: imageCachePlatformPlanProtocolVersion,
			SchemaVersion: imageCachePlatformPlanSchemaVersion,
		},
	}
	raw, _ := json.Marshal(plan)
	return testImageCachePlatformPlanFixture{plan: plan, raw: raw}
}

func newTestImageCachePlatformConsumer(t *testing.T, apiURL, nodeID, credentialPath, observationPath string, now time.Time, client *http.Client) *imageCachePlatformPlanConsumer {
	t.Helper()
	consumer, err := newImageCachePlatformPlanConsumer(imageCachePlatformPlanConfig{
		APIBaseURL: apiURL, NodeID: nodeID, CredentialPath: credentialPath, ObservationPath: observationPath,
		AllowInsecureHTTP: true,
		LongPoll:          time.Second, RequestTimeout: 5 * time.Second, RetryMin: 100 * time.Millisecond,
		RetryMax: time.Second, NoPlanRetry: time.Second, MinCredentialLife: time.Second, ArchiveLimit: 5,
	}, client)
	if err != nil {
		t.Fatalf("new image-cache platform consumer: %v", err)
	}
	consumer.now = func() time.Time { return now }
	consumer.random = bytes.NewReader(bytes.Repeat([]byte{0x42}, 24*16))
	return consumer
}

func writeTestImageCachePlatformCredential(t *testing.T, path, nodeID string, issuedAt time.Time, token string, mode os.FileMode) {
	t.Helper()
	writeTestImageCachePlatformCredentialValue(t, path, imageCachePlatformCredential{
		APIVersion: imageCachePlatformPlanCredentialVersion, Kind: imageCachePlatformPlanCredentialKind,
		CredentialID: imageCachePlatformPlanComponent + ":" + nodeID, Token: token, TokenID: "token-" + nodeID,
		Component: imageCachePlatformPlanComponent, NodeID: nodeID, ScopeKey: "node:" + nodeID,
		ArtifactKinds: []string{imageCachePlatformPlanArtifactKind}, IssuedAt: issuedAt,
		ExpiresAt: issuedAt.Add(15 * time.Minute), RenewAfter: issuedAt.Add(5 * time.Minute),
	}, mode)
}

func writeTestImageCachePlatformCredentialValue(t *testing.T, path string, credential imageCachePlatformCredential, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir credential directory: %v", err)
	}
	raw, err := json.Marshal(imageCachePlatformCredentialEnvelope{Credential: credential})
	if err != nil {
		t.Fatalf("marshal credential: %v", err)
	}
	if err := os.WriteFile(path, raw, mode); err != nil {
		t.Fatalf("write credential: %v", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod credential: %v", err)
	}
}

func writeTestImageCachePlatformHeartbeatResponse(t *testing.T, w http.ResponseWriter, heartbeat imageCachePlatformHeartbeat) {
	t.Helper()
	response := imageCachePlatformHeartbeatResponse{}
	response.Consumer.ConsumerID = heartbeat.ConsumerID
	response.Consumer.Component = heartbeat.Component
	response.Consumer.NodeID = heartbeat.NodeID
	response.Consumer.ArtifactKind = heartbeat.ArtifactKind
	response.Consumer.ScopeKey = heartbeat.ScopeKey
	response.Consumer.ReleaseSetID = heartbeat.ReleaseSetID
	response.Consumer.ExpectedConsumerSetID = heartbeat.ExpectedConsumerSetID
	response.Consumer.FencingToken = heartbeat.FencingToken
	response.Consumer.Sequence = heartbeat.Sequence
	response.Consumer.DesiredGeneration = heartbeat.DesiredGeneration
	response.Consumer.ActualGeneration = heartbeat.ActualGeneration
	response.Consumer.ApplyStatus = heartbeat.ApplyStatus
	response.Consumer.ProbeStatus = heartbeat.ProbeStatus
	response.Consumer.IdentityVerified = true
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		t.Fatalf("encode heartbeat response: %v", err)
	}
}
