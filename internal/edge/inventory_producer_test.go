package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/edgecontrol"
	"fugue/internal/edgegroupfront"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestInventoryProducerBindsPlatformIdentityNodeGroupAndMonotonicCursor(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	groupID := "edge-group-country-us"
	nodeID := "edge-node-us-1"
	activationSourceCommit := strings.Repeat("a", 40)
	workerSourceCommit := strings.Repeat("b", 40)
	activationFile := writeInventoryActivationFixture(t, now, groupID, model.EdgeSlotB, activationSourceCommit)
	keyringFile, activeKey := writeInventoryProducerKeyringFixture(t, groupID)
	identityKeyring := platformcontrol.DerivePlatformComponentIdentityKeyring(activeKey, "inventory-platform-current", "", "", nil)

	var requests atomic.Int32
	client := &http.Client{Transport: inventoryRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests.Add(1)
		switch {
		case request.Method == http.MethodGet && request.URL.Path == edgecontrol.AuthorityGroupReadyPrefixV1+groupID+"/readyz":
			if request.URL.RawQuery != "" || request.Header.Get("Authorization") != "" {
				t.Fatalf("cursor request leaked credential or query: %s headers=%v", request.URL.String(), request.Header)
			}
			bootstrapUntil := now.Add(time.Hour)
			return inventoryJSONResponse(http.StatusServiceUnavailable, edgecontrol.AuthorityGroupStatus{
				GroupID: groupID, Status: edgecontrol.GroupAuthorityHealthServingLKG, ServingHealthy: true,
				BootstrapEligible: true, BootstrapValidUntil: &bootstrapUntil,
				InventorySequence: 9, InventoryProducerGeneration: 7, AuthoritySequence: 12,
				PublicationSequence: 12, CurrentPublicationSequence: 11, CandidateEpoch: 14,
			}), nil
		case request.Method == http.MethodPost && request.URL.Path == edgecontrol.GroupAuthorityInventoryHeartbeatPathV1:
			if request.URL.RawQuery != "" || !strings.HasPrefix(request.Header.Get("Authorization"), "Bearer ") || request.Header.Get("Content-Type") != "application/json" {
				t.Fatalf("heartbeat transport is not fixed bearer auth: url=%s headers=%v", request.URL.String(), request.Header)
			}
			claims, err := platformcontrol.ParsePlatformComponentIdentity(identityKeyring, strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "), now)
			if err != nil || claims.NodeID != nodeID || claims.ScopeKey != groupID || claims.Component != model.PlatformConsumerComponentEdgeWorker {
				t.Fatalf("heartbeat bearer is not freshly group/node bound: claims=%+v err=%v", claims, err)
			}
			var heartbeat edgecontrol.GroupInventoryHeartbeat
			decoder := json.NewDecoder(request.Body)
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&heartbeat); err != nil {
				t.Fatal(err)
			}
			if heartbeat.GroupID != groupID || heartbeat.ProducerNodeID != nodeID || heartbeat.ProducerGeneration != 8 ||
				heartbeat.FaultDomainID != "fault-domain-primary-b" || heartbeat.EdgePoolID != "edge-pool-public-b" ||
				heartbeat.ExpectedSequence != 9 || heartbeat.Inventory.Sequence != 10 || heartbeat.Inventory.ActiveEpoch.FenceSequence != 1 ||
				heartbeat.Inventory.ActiveEpoch.ReleaseEpoch != workerSourceCommit || len(heartbeat.Inventory.Instances) != 1 ||
				heartbeat.Inventory.Instances[0].EdgeID != nodeID || heartbeat.Inventory.Instances[0].InstanceUID != "pod-uid-us-b" ||
				heartbeat.Inventory.Instances[0].ReleaseEpoch != workerSourceCommit {
				t.Fatalf("heartbeat is not group/node/cursor bound: %+v", heartbeat)
			}
			return inventoryJSONResponse(http.StatusCreated, edgecontrol.GroupInventoryHeartbeatReceipt{
				Schema: edgecontrol.GroupInventoryHeartbeatReceiptSchemaV1, GroupID: groupID, Sequence: 10,
				Generation: "inventory-server-generation", InventoryDigest: "sha256:" + strings.Repeat("c", 64),
				Authority: "edge-control", Publication: true, ProducerNodeID: nodeID, ProducerGeneration: 8,
			}), nil
		default:
			t.Fatalf("unexpected inventory producer request: %s %s", request.Method, request.URL.String())
			return nil, nil
		}
	})}

	edgeConfig := config.EdgeConfig{
		EdgeID: nodeID, EdgeGroupID: groupID, EdgeSlot: model.EdgeSlotB, EdgeInstanceUID: "pod-uid-us-b", EdgeReleaseEpoch: workerSourceCommit, FaultDomainID: "fault-domain-primary-b", EdgePoolID: "edge-pool-public-b",
		HTTPTimeout: time.Second,
	}
	producer := InventoryProducerConfig{
		URL:                 "http://edge-control-us.fugue-system.svc:8092" + edgecontrol.GroupAuthorityInventoryHeartbeatPathV1,
		AuthorityService:    "edge-control-us",
		IdentityKeyringFile: keyringFile, ActivationStateFile: activationFile, Interval: 30 * time.Second,
	}
	service := NewServiceWithEdgeSources(edgeConfig, RouteBundleSourceConfig{}, producer, log.New(io.Discard, "", 0))
	service.InventoryProducerHTTPClient = client
	service.mu.Lock()
	service.snapshot.Healthy = true
	service.snapshot.Status = "healthy"
	service.mu.Unlock()
	if err := service.InventoryHeartbeatOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := service.Status()
	if !status.InventoryProducerActive || status.InventoryHeartbeatGeneration != 8 || status.InventoryHeartbeatAt == nil || status.InventoryHeartbeatError != "" || requests.Load() != 2 {
		t.Fatalf("inventory producer status is not complete: %+v requests=%d", status, requests.Load())
	}
}

func TestInventoryProducerStatusSurvivesRouteSyncSnapshotReplacement(t *testing.T) {
	now := time.Date(2026, 8, 7, 20, 30, 0, 0, time.UTC)
	service := NewService(config.EdgeConfig{
		EdgeID: "edge-node-de-1", EdgeGroupID: "edge-group-country-de", EdgeSlot: model.EdgeSlotA,
	}, log.New(io.Discard, "", 0))
	service.recordInventoryHeartbeatSuccess(42, now)
	service.recordSyncSuccess(testBundle("route-generation-after-heartbeat"), `"etag"`, now.Add(time.Second), false)

	status := service.Status()
	if !status.InventoryProducerActive || status.InventoryHeartbeatGeneration != 42 || status.InventoryHeartbeatAt == nil ||
		!status.InventoryHeartbeatAt.Equal(now) || status.InventoryHeartbeatError != "" {
		t.Fatalf("route sync dropped verified inventory heartbeat evidence: %+v", status)
	}
}

func TestInventoryProducerBreaksOnlyGroupBundleBootstrapDeadlock(t *testing.T) {
	t.Parallel()

	base := Status{
		Status:       "unhealthy",
		Healthy:      false,
		CaddyEnabled: true,
		LastError:    `edge routes returned status 503: {"schema":"edge-control-error/v1","error":"group_bundle_unavailable"}`,
	}
	nodeStatus, nodeHealthy, servingHealthy, bootstrapEligible := inventoryProducerHealth(base, config.EdgeConfig{CaddyEnabled: true})
	if nodeStatus != model.EdgeHealthHealthy || !nodeHealthy || servingHealthy || !bootstrapEligible {
		t.Fatalf("group bundle bootstrap health = %q, node=%t serving=%t bootstrap=%t", nodeStatus, nodeHealthy, servingHealthy, bootstrapEligible)
	}

	tests := []struct {
		name   string
		status Status
		config config.EdgeConfig
	}{
		{name: "different authority error", status: func() Status {
			value := base
			value.LastError = `edge routes returned status 503: {"schema":"edge-control-error/v1","error":"store_unavailable"}`
			return value
		}(), config: config.EdgeConfig{CaddyEnabled: true}},
		{name: "signature failure", status: func() Status {
			value := base
			value.FailureClass = model.EdgeInstanceFailureSignatureInvalid
			return value
		}(), config: config.EdgeConfig{CaddyEnabled: true}},
		{name: "caddy failure", status: func() Status {
			value := base
			value.CaddyLastError = "admin endpoint unavailable"
			return value
		}(), config: config.EdgeConfig{CaddyEnabled: true}},
		{name: "draining", status: base, config: config.EdgeConfig{CaddyEnabled: true, Draining: true}},
		{name: "caddy disabled", status: base, config: config.EdgeConfig{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodeStatus, nodeHealthy, servingHealthy, bootstrapEligible := inventoryProducerHealth(test.status, test.config)
			if nodeStatus == model.EdgeHealthHealthy || nodeHealthy || servingHealthy || bootstrapEligible {
				t.Fatalf("unsafe bootstrap health = %q, node=%t serving=%t bootstrap=%t", nodeStatus, nodeHealthy, servingHealthy, bootstrapEligible)
			}
		})
	}
}

func TestInventoryProducerPublishesBootstrapHealthyHeartbeat(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	groupID := "edge-group-country-us"
	nodeID := "edge-node-us-bootstrap"
	sourceCommit := strings.Repeat("f", 40)
	activationFile := writeInventoryActivationFixture(t, now, groupID, model.EdgeSlotA, sourceCommit)
	keyringFile, _ := writeInventoryProducerKeyringFixture(t, groupID)

	service := NewServiceWithEdgeSources(config.EdgeConfig{
		EdgeID: nodeID, EdgeGroupID: groupID, EdgeSlot: model.EdgeSlotA, EdgeInstanceUID: "pod-uid-us-bootstrap", EdgeReleaseEpoch: sourceCommit, FaultDomainID: "fault-domain-primary-b", EdgePoolID: "edge-pool-public-b",
		HTTPTimeout: time.Second, CaddyEnabled: true,
	}, RouteBundleSourceConfig{}, InventoryProducerConfig{
		URL:                 "http://edge-control-us.fugue-system.svc:8092" + edgecontrol.GroupAuthorityInventoryHeartbeatPathV1,
		AuthorityService:    "edge-control-us",
		IdentityKeyringFile: keyringFile, ActivationStateFile: activationFile, Interval: 30 * time.Second,
	}, log.New(io.Discard, "", 0))
	service.mu.Lock()
	service.snapshot = Status{
		Status:       "unhealthy",
		Healthy:      false,
		CaddyEnabled: true,
		LastError:    `edge routes returned status 503: {"schema":"edge-control-error/v1","error":"group_bundle_unavailable"}`,
	}
	service.mu.Unlock()
	service.InventoryProducerHTTPClient = &http.Client{Transport: inventoryRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet {
			return inventoryJSONResponse(http.StatusServiceUnavailable, edgecontrol.AuthorityGroupStatus{
				GroupID: groupID, InventorySequence: 2, InventoryProducerGeneration: 2,
			}), nil
		}
		var heartbeat edgecontrol.GroupInventoryHeartbeat
		if err := json.NewDecoder(request.Body).Decode(&heartbeat); err != nil {
			t.Fatal(err)
		}
		instance := heartbeat.Inventory.Instances[0]
		if !instance.NodeHealthy || instance.EffectiveHealthy || instance.ServingHealthy == nil || *instance.ServingHealthy ||
			instance.NodeStatus != model.EdgeHealthHealthy || instance.BootstrapEligibility == nil ||
			instance.BootstrapEligibility.GroupID != groupID || instance.BootstrapEligibility.ReleaseEpoch != sourceCommit ||
			instance.BootstrapEligibility.ProducerGeneration != 3 || instance.BootstrapEligibility.ValidUntil.IsZero() {
			t.Fatalf("bootstrap heartbeat did not separate bootstrap from serving health: %+v", instance)
		}
		return inventoryJSONResponse(http.StatusCreated, edgecontrol.GroupInventoryHeartbeatReceipt{
			Schema: edgecontrol.GroupInventoryHeartbeatReceiptSchemaV1, GroupID: groupID, Sequence: 3,
			Generation: "inventory-bootstrap", InventoryDigest: "sha256:" + strings.Repeat("d", 64),
			Authority: "edge-control", Publication: true, ProducerNodeID: nodeID, ProducerGeneration: 3,
		}), nil
	})}
	if err := service.InventoryHeartbeatOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestInventoryProducerInteroperatesWithGroupAuthorityVerifierAndDurableLedger(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	groupID := "edge-group-country-de"
	nodeID := "edge-node-de-1"
	sourceCommit := strings.Repeat("c", 40)
	activeKey := "inventory-platform-identity-key-0123456789abcdef"
	activeKeyID := "inventory-platform-current"

	keyringDir := t.TempDir()
	if err := os.Chmod(keyringDir, 0o700); err != nil {
		t.Fatal(err)
	}
	keyringRaw, err := json.Marshal(map[string]any{
		"schema": edgecontrol.InventoryPlatformIdentityKeyringSchemaV1, "generation": 1, "edge_group_id": groupID,
		"active_key_id": activeKeyID, "active_key": activeKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(keyringDir, groupID+".json"), keyringRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	stateDir := t.TempDir()
	if err := os.Chmod(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := edgecontrol.OpenPersistentGroupStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	heartbeatHandler, err := edgecontrol.NewGroupInventoryHeartbeatHandler(edgecontrol.GroupInventoryHeartbeatHandlerConfig{
		Store: store, GroupIDs: []string{groupID}, KeyringDir: keyringDir, Authority: "edge-control", PublicationEnabled: true,
		Path: edgecontrol.GroupAuthorityInventoryHeartbeatPathV1, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	statusHandler, err := edgecontrol.NewAuthorityStatusHandler(store, []string{groupID}, edgecontrol.NewAuthorityRuntimeState(func() time.Time { return now }), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}

	keyringFile := filepath.Join(t.TempDir(), "keyring.json")
	if err := os.WriteFile(keyringFile, keyringRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	activationFile := writeInventoryActivationFixture(t, now, groupID, model.EdgeSlotA, sourceCommit)
	service := NewServiceWithEdgeSources(config.EdgeConfig{
		EdgeID: nodeID, EdgeGroupID: groupID, EdgeSlot: model.EdgeSlotA, EdgeInstanceUID: "pod-uid-de-a", EdgeReleaseEpoch: sourceCommit, FaultDomainID: "fault-domain-primary-a", EdgePoolID: "edge-pool-public-a",
		HTTPTimeout: time.Second,
	}, RouteBundleSourceConfig{}, InventoryProducerConfig{
		URL:                 "http://edge-control-de.fugue-system.svc:8092" + edgecontrol.GroupAuthorityInventoryHeartbeatPathV1,
		AuthorityService:    "edge-control-de",
		IdentityKeyringFile: keyringFile, ActivationStateFile: activationFile, Interval: 30 * time.Second,
	}, log.New(io.Discard, "", 0))
	service.InventoryProducerHTTPClient = &http.Client{Transport: inventoryRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		if request.Method == http.MethodGet {
			statusHandler.ServeHTTP(recorder, request)
		} else {
			heartbeatHandler.ServeHTTP(recorder, request)
		}
		return recorder.Result(), nil
	})}
	service.mu.Lock()
	service.snapshot.Healthy = true
	service.snapshot.Status = "healthy"
	service.mu.Unlock()
	if err := service.InventoryHeartbeatOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	producer, exists, err := store.ReadGroupInventoryProducerState(context.Background(), groupID)
	if err != nil || !exists || producer.Generation != 1 || len(producer.Observations) != 1 || producer.Observations[0].NodeID != nodeID {
		t.Fatalf("durable producer ledger=%+v exists=%t err=%v", producer, exists, err)
	}
}

func TestInventoryProducerInactiveSlotDoesNotWriteAndCrossGroupIdentityFailsClosed(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	groupID := "edge-group-country-de"
	nodeID := "edge-node-de-1"
	sourceCommit := strings.Repeat("d", 40)
	activationFile := writeInventoryActivationFixture(t, now, groupID, model.EdgeSlotA, sourceCommit)
	keyringFile, _ := writeInventoryProducerKeyringFixture(t, "edge-group-country-us")
	producer := InventoryProducerConfig{
		URL:                 "http://edge-control-de.fugue-system.svc:8092" + edgecontrol.GroupAuthorityInventoryHeartbeatPathV1,
		AuthorityService:    "edge-control-de",
		IdentityKeyringFile: keyringFile, ActivationStateFile: activationFile, Interval: 30 * time.Second,
	}
	edgeConfig := config.EdgeConfig{
		EdgeID: nodeID, EdgeGroupID: groupID, EdgeSlot: model.EdgeSlotB, EdgeInstanceUID: "pod-uid-de-b", EdgeReleaseEpoch: sourceCommit, FaultDomainID: "fault-domain-primary-a", EdgePoolID: "edge-pool-public-a",
		HTTPTimeout: time.Second,
	}
	service := NewServiceWithEdgeSources(edgeConfig, RouteBundleSourceConfig{}, producer, log.New(io.Discard, "", 0))
	service.InventoryProducerHTTPClient = &http.Client{Transport: inventoryRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		t.Fatalf("inactive slot unexpectedly contacted inventory authority: %s", request.URL.String())
		return nil, nil
	})}
	if err := service.InventoryHeartbeatOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if service.Status().InventoryProducerActive {
		t.Fatal("inactive slot claimed inventory producer authority")
	}

	edgeConfig.EdgeSlot = model.EdgeSlotA
	active := NewServiceWithEdgeSources(edgeConfig, RouteBundleSourceConfig{}, producer, log.New(io.Discard, "", 0))
	active.InventoryProducerHTTPClient = service.InventoryProducerHTTPClient
	if err := active.InventoryHeartbeatOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "keyring is invalid") {
		t.Fatalf("cross-group projected identity was not rejected locally: %v", err)
	}
}

func TestInventoryProducerTransportFailureDoesNotExposeProjectedIdentity(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	groupID := "edge-group-country-us"
	nodeID := "edge-node-us-1"
	sourceCommit := strings.Repeat("e", 40)
	activationFile := writeInventoryActivationFixture(t, now, groupID, model.EdgeSlotA, sourceCommit)
	keyringFile, secret := writeInventoryProducerKeyringFixture(t, groupID)
	producer := InventoryProducerConfig{
		URL:                 "http://edge-control-us.fugue-system.svc:8092" + edgecontrol.GroupAuthorityInventoryHeartbeatPathV1,
		AuthorityService:    "edge-control-us",
		IdentityKeyringFile: keyringFile, ActivationStateFile: activationFile, Interval: 30 * time.Second,
	}
	service := NewServiceWithEdgeSources(config.EdgeConfig{
		EdgeID: nodeID, EdgeGroupID: groupID, EdgeSlot: model.EdgeSlotA, EdgeInstanceUID: "pod-uid-us-a", EdgeReleaseEpoch: sourceCommit, FaultDomainID: "fault-domain-primary-b", EdgePoolID: "edge-pool-public-b",
		HTTPTimeout: time.Second,
	}, RouteBundleSourceConfig{}, producer, log.New(io.Discard, "", 0))
	service.InventoryProducerHTTPClient = &http.Client{Transport: inventoryRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet {
			return inventoryJSONResponse(http.StatusServiceUnavailable, edgecontrol.AuthorityGroupStatus{GroupID: groupID}), nil
		}
		return nil, &url.Error{Op: "Post", URL: request.URL.String(), Err: errors.New("connection refused")}
	})}
	err := service.InventoryHeartbeatOnce(context.Background())
	if err == nil || strings.Contains(err.Error(), secret) || strings.Contains(service.Status().InventoryHeartbeatError, secret) {
		t.Fatalf("transport error exposed projected identity: err=%v status=%+v", err, service.Status())
	}
}

func TestInventoryProducerRetriesTransportFailureWithFreshCursor(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	groupID := "edge-group-country-de"
	nodeID := "edge-node-de-1"
	sourceCommit := strings.Repeat("f", 40)
	activationFile := writeInventoryActivationFixture(t, now, groupID, model.EdgeSlotB, sourceCommit)
	keyringFile, _ := writeInventoryProducerKeyringFixture(t, groupID)
	producer := InventoryProducerConfig{
		URL:                 "http://edge-control-de.fugue-system.svc:8092" + edgecontrol.GroupAuthorityInventoryHeartbeatPathV1,
		AuthorityService:    "edge-control-de",
		IdentityKeyringFile: keyringFile, ActivationStateFile: activationFile, Interval: 30 * time.Second,
	}
	service := NewServiceWithEdgeSources(config.EdgeConfig{
		EdgeID: nodeID, EdgeGroupID: groupID, EdgeSlot: model.EdgeSlotB, EdgeInstanceUID: "pod-uid-de-b", EdgeReleaseEpoch: sourceCommit, FaultDomainID: "fault-domain-primary-a", EdgePoolID: "edge-pool-public-a",
		HTTPTimeout: time.Second,
	}, RouteBundleSourceConfig{}, producer, log.New(io.Discard, "", 0))
	var postAttempts atomic.Int32
	var getAttempts atomic.Int32
	var firstHeartbeat edgecontrol.GroupInventoryHeartbeat
	service.InventoryProducerHTTPClient = &http.Client{Transport: inventoryRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet {
			attempt := getAttempts.Add(1)
			return inventoryJSONResponse(http.StatusServiceUnavailable, edgecontrol.AuthorityGroupStatus{
				GroupID: groupID, InventorySequence: 10 + uint64(attempt), InventoryProducerGeneration: 8 + uint64(attempt),
			}), nil
		}
		var heartbeat edgecontrol.GroupInventoryHeartbeat
		if err := json.NewDecoder(request.Body).Decode(&heartbeat); err != nil {
			t.Fatal(err)
		}
		attempt := postAttempts.Add(1)
		if attempt == 1 {
			firstHeartbeat = heartbeat
			return nil, &url.Error{Op: "Post", URL: request.URL.String(), Err: errors.New("timeout")}
		}
		if request.Header.Get("Authorization") == "" || heartbeat.ExpectedSequence != firstHeartbeat.ExpectedSequence+1 ||
			heartbeat.ProducerGeneration != firstHeartbeat.ProducerGeneration+1 || heartbeat.Nonce == firstHeartbeat.Nonce {
			t.Fatalf("retried heartbeat did not use a fresh authority cursor: first=%+v retry=%+v", firstHeartbeat, heartbeat)
		}
		return inventoryJSONResponse(http.StatusCreated, edgecontrol.GroupInventoryHeartbeatReceipt{
			Schema: edgecontrol.GroupInventoryHeartbeatReceiptSchemaV1, GroupID: groupID, Sequence: 13,
			Generation: "inventory-server-generation", InventoryDigest: "sha256:" + strings.Repeat("a", 64),
			Authority: "edge-control", Publication: true, ProducerNodeID: nodeID, ProducerGeneration: 11,
		}), nil
	})}
	service.mu.Lock()
	service.snapshot.Healthy = true
	service.snapshot.Status = "healthy"
	service.mu.Unlock()

	if err := service.InventoryHeartbeatOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if getAttempts.Load() != 2 || postAttempts.Load() != 2 || service.Status().InventoryHeartbeatGeneration != 11 {
		t.Fatalf("heartbeat retry did not converge: gets=%d posts=%d status=%+v", getAttempts.Load(), postAttempts.Load(), service.Status())
	}
}

func TestInventoryProducerDoesNotRetryHTTPFailure(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	groupID := "edge-group-country-de"
	nodeID := "edge-node-de-1"
	sourceCommit := strings.Repeat("1", 40)
	activationFile := writeInventoryActivationFixture(t, now, groupID, model.EdgeSlotA, sourceCommit)
	keyringFile, _ := writeInventoryProducerKeyringFixture(t, groupID)
	service := NewServiceWithEdgeSources(config.EdgeConfig{
		EdgeID: nodeID, EdgeGroupID: groupID, EdgeSlot: model.EdgeSlotA, EdgeInstanceUID: "pod-uid-de-a", EdgeReleaseEpoch: sourceCommit, FaultDomainID: "fault-domain-primary-a", EdgePoolID: "edge-pool-public-a",
		HTTPTimeout: time.Second,
	}, RouteBundleSourceConfig{}, InventoryProducerConfig{
		URL:              "http://edge-control-de.fugue-system.svc:8092" + edgecontrol.GroupAuthorityInventoryHeartbeatPathV1,
		AuthorityService: "edge-control-de", IdentityKeyringFile: keyringFile, ActivationStateFile: activationFile, Interval: 30 * time.Second,
	}, log.New(io.Discard, "", 0))
	var attempts atomic.Int32
	service.InventoryProducerHTTPClient = &http.Client{Transport: inventoryRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet {
			return inventoryJSONResponse(http.StatusServiceUnavailable, edgecontrol.AuthorityGroupStatus{GroupID: groupID}), nil
		}
		attempts.Add(1)
		return inventoryJSONResponse(http.StatusServiceUnavailable, map[string]string{"error": "store_unavailable"}), nil
	})}

	err := service.InventoryHeartbeatOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "returned status 503") || attempts.Load() != 1 {
		t.Fatalf("HTTP failure retry behavior is invalid: err=%v attempts=%d", err, attempts.Load())
	}
}

func TestInventoryProducerUsesExplicitAuthorityServiceNotCountryDerivedName(t *testing.T) {
	groupID := "edge-group-country-de"
	producer := InventoryProducerConfig{
		URL:                 "http://edge-control-eu-backup.fugue-system.svc:8092" + groupAuthorityInventoryHeartbeatPathV1,
		AuthorityService:    "edge-control-eu-backup",
		IdentityKeyringFile: "/var/run/keyring.json", ActivationStateFile: "/var/run/activation.json",
		Interval: 30 * time.Second,
	}
	if err := validateInventoryProducerConfig(producer, config.EdgeConfig{
		EdgeID: "edge-01", EdgeGroupID: groupID, EdgeSlot: "a", EdgeInstanceUID: "uid-1", EdgeReleaseEpoch: strings.Repeat("a", 40), FaultDomainID: "fault-domain-primary-a", EdgePoolID: "edge-pool-public-a",
	}); err != nil {
		t.Fatalf("explicit authority service was rejected: %v", err)
	}
	producer.URL = "http://edge-control-de.fugue-system.svc:8092" + groupAuthorityInventoryHeartbeatPathV1
	producer.AuthorityService = "edge-control-eu-backup"
	if err := validateInventoryProducerConfig(producer, config.EdgeConfig{
		EdgeID: "edge-01", EdgeGroupID: "edge-group-neutral-a", EdgeSlot: "a", EdgeInstanceUID: "uid-1", EdgeReleaseEpoch: strings.Repeat("a", 40), FaultDomainID: "fault-domain-primary-a", EdgePoolID: "edge-pool-public-a",
	}); err == nil {
		t.Fatal("endpoint accepted when it did not match the explicit authority service")
	}
}

func TestInventoryProducerRejectsMissingExplicitAuthorityService(t *testing.T) {
	producer := InventoryProducerConfig{
		URL:                 "http://edge-control-de.fugue-system.svc:8092" + groupAuthorityInventoryHeartbeatPathV1,
		IdentityKeyringFile: "/var/run/keyring.json", ActivationStateFile: "/var/run/activation.json", Interval: 30 * time.Second,
	}
	if err := validateInventoryProducerConfig(producer, config.EdgeConfig{
		EdgeID: "edge-01", EdgeGroupID: "edge-group-country-de", EdgeSlot: "a", EdgeInstanceUID: "uid-1", EdgeReleaseEpoch: strings.Repeat("a", 40), FaultDomainID: "fault-domain-primary-a", EdgePoolID: "edge-pool-public-a",
	}); err == nil || !strings.Contains(err.Error(), "authority service") {
		t.Fatalf("missing authority service was not rejected: %v", err)
	}
}

type inventoryRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn inventoryRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func inventoryJSONResponse(status int, value any) *http.Response {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(append(raw, '\n'))),
	}
}

func writeInventoryActivationFixture(t *testing.T, now time.Time, groupID, slot, sourceCommit string) string {
	return writeInventoryActivationFixtureWithBundle(t, now, groupID, slot, sourceCommit, "bundle-generation-1")
}

func writeInventoryActivationFixtureWithBundle(t *testing.T, now time.Time, groupID, slot, sourceCommit, bundleGeneration string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "activation.json")
	_, err := edgegroupfront.ApplyActivationCAS(path, edgegroupfront.ActivationCASRequest{
		GroupID: groupID, ExpectedGeneration: 0, ExpectedSlot: slot, TargetSlot: slot,
		BundleGeneration: bundleGeneration, WorkerSourceCommit: sourceCommit,
		WorkerImageDigest: "sha256:" + strings.Repeat("b", 64), Operation: edgegroupfront.ActivationOperationInit,
		Reason: "inventory producer test activation",
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func writeInventoryProducerKeyringFixture(t *testing.T, groupID string) (string, string) {
	t.Helper()
	activeKey := "inventory-platform-identity-key-0123456789abcdef"
	raw, err := json.Marshal(map[string]any{
		"schema": edgecontrol.InventoryPlatformIdentityKeyringSchemaV1, "generation": 1, "edge_group_id": groupID,
		"active_key_id": "inventory-platform-current", "active_key": activeKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "keyring.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, activeKey
}
