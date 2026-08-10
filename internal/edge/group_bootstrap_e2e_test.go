package edge

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
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/edgecontrol"
	"fugue/internal/model"
)

func TestEmptyGroupBootstrapsSignedBundleThenServesRealRoute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	groupID := "edge-group-test-bootstrap"
	edgeID := "edge-bootstrap-1"
	releaseEpoch := strings.Repeat("a", 40)
	activationFile := writeInventoryActivationFixture(t, now, groupID, model.EdgeSlotA, releaseEpoch)
	identityKeyringFile, _ := writeInventoryProducerKeyringFixture(t, groupID)

	service := NewServiceWithEdgeSources(config.EdgeConfig{
		EdgeID: edgeID, EdgeGroupID: groupID, EdgeSlot: model.EdgeSlotA, EdgeInstanceUID: "worker-bootstrap-uid",
		EdgeReleaseEpoch: releaseEpoch, APIURL: "http://core.invalid", EdgeToken: "bootstrap-heartbeat-token",
		HTTPTimeout: 2 * time.Second, CaddyEnabled: true,
	}, RouteBundleSourceConfig{}, InventoryProducerConfig{
		URL:              "http://edge-control-bootstrap.fugue-system.svc:8092" + edgecontrol.GroupAuthorityInventoryHeartbeatPathV1,
		AuthorityService: "edge-control-bootstrap", IdentityKeyringFile: identityKeyringFile,
		ActivationStateFile: activationFile, Interval: 30 * time.Second,
	}, log.New(io.Discard, "", 0))
	service.mu.Lock()
	service.snapshot = Status{
		Status: "unhealthy", Healthy: false, CaddyEnabled: true,
		LastError: `edge routes returned status 503: {"schema":"edge-control-error/v1","error":"group_bundle_unavailable"}`,
	}
	service.mu.Unlock()

	var heartbeat edgecontrol.GroupInventoryHeartbeat
	service.InventoryProducerHTTPClient = &http.Client{Transport: inventoryRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet {
			return inventoryJSONResponse(http.StatusServiceUnavailable, edgecontrol.AuthorityGroupStatus{GroupID: groupID}), nil
		}
		if err := json.NewDecoder(request.Body).Decode(&heartbeat); err != nil {
			t.Fatal(err)
		}
		return inventoryJSONResponse(http.StatusCreated, edgecontrol.GroupInventoryHeartbeatReceipt{
			Schema: edgecontrol.GroupInventoryHeartbeatReceiptSchemaV1, GroupID: groupID, Sequence: 1,
			Generation: edgecontrol.ProducerInventoryEnvelopeGeneration(1), InventoryDigest: "sha256:" + strings.Repeat("1", 64),
			Authority: "edge-control", Publication: true, ProducerNodeID: edgeID, ProducerGeneration: 1,
		}), nil
	})}
	if err := service.InventoryHeartbeatOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	instance := heartbeat.Inventory.Instances[0]
	if instance.ServingHealthy == nil || *instance.ServingHealthy || instance.EffectiveHealthy || instance.BootstrapEligibility == nil {
		t.Fatalf("worker without a bundle claimed serving health: %+v", instance)
	}

	store, err := edgecontrol.OpenPersistentGroupStore(privateBootstrapStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StoreGroupInventoryCAS(context.Background(), groupID, 0, heartbeat.Inventory); err != nil {
		t.Fatal(err)
	}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("generic-edge-route-ok"))
	}))
	defer origin.Close()
	intent := model.EdgeRouteIntentSnapshot{
		SchemaVersion: model.EdgeRouteIntentSchemaVersionV1, Generation: "route-intent-bootstrap-1", GeneratedAt: now,
		Routes: []model.EdgeRouteIntent{{
			Generation: "route-bootstrap-1", Hostname: "bootstrap.example.test", PathPrefix: "/", RouteKind: model.EdgeRouteKindPlatform,
			TargetGroupMode: model.EdgeRouteIntentGroupModePinnedGroup, PinnedEdgeGroupID: groupID, MinHealthyEdgeNodes: 1,
			RoutePolicy: model.EdgeRoutePolicyEnabled, UpstreamKind: model.EdgeRouteUpstreamKindMesh,
			UpstreamScope: model.EdgeRouteUpstreamScopeMesh, UpstreamURL: origin.URL, ServicePort: 80,
			TLSPolicy: model.EdgeRouteTLSPolicyPlatform, Streaming: true, OriginStatus: model.EdgeRouteStatusActive,
			CreatedAt: now, UpdatedAt: now,
		}},
	}
	compiler := edgecontrol.GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }, InventoryMaxAge: edgecontrol.GroupInventoryHeartbeatMaxAge}
	compiled, err := compiler.Reconcile(context.Background(), intent, []string{groupID})
	if err != nil || compiled.Succeeded != 1 {
		t.Fatalf("bootstrap compile = %+v, %v", compiled, err)
	}
	signingKey := bytes.Repeat([]byte{0x42}, 32)
	keyID := "bootstrap-signing-key-1"
	publisher := edgecontrol.GroupAuthorityPublisher{Store: store, Signer: bootstrapBundleSigner{key: signingKey, keyID: keyID}, Now: func() time.Time { return now }}
	published, err := publisher.Publish(context.Background(), compiled)
	if err != nil || published.Published != 1 {
		t.Fatalf("bootstrap publish = %+v, %v", published, err)
	}

	readerToken := "bootstrap-reader-token-0123456789abcdef"
	readerDir := t.TempDir()
	readerDigest := sha256.Sum256([]byte(readerToken))
	readerKeyring := map[string]any{
		"schema": edgecontrol.GroupBundleReaderKeyringSchemaV1, "generation": 1, "edge_group_id": groupID,
		"credentials": []map[string]any{{
			"credential_id": "bootstrap-reader", "edge_id": edgeID, "token_digest": "sha256:" + hex.EncodeToString(readerDigest[:]),
			"not_before_unix": now.Add(-time.Minute).Unix(), "not_after_unix": now.Add(time.Hour).Unix(),
		}},
	}
	writePrivateJSON(t, filepath.Join(readerDir, groupID+".json"), readerKeyring)
	bundleHandler, err := edgecontrol.NewGroupBundleHandler(edgecontrol.GroupBundleHandlerConfig{Store: store, GroupIDs: []string{groupID}, KeyringDir: readerDir, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	bundleServer := httptest.NewServer(bundleHandler)
	defer bundleServer.Close()
	credentialDir := t.TempDir()
	tokenFile := filepath.Join(credentialDir, "token")
	if err := os.WriteFile(tokenFile, []byte(readerToken), 0o600); err != nil {
		t.Fatal(err)
	}
	verifierFile := filepath.Join(credentialDir, "verifier.json")
	writeEdgeVerifierKeyring(t, verifierFile, groupID, keyID, signingKey)
	// The process/Caddy eligibility was already carried by the signed bootstrap
	// heartbeat. The in-process route assertion below exercises the Edge proxy
	// directly and therefore does not start an external Caddy admin endpoint.
	service.Config.CaddyEnabled = false
	service.RouteBundleSource = RouteBundleSourceConfig{URL: bundleServer.URL + edgecontrol.GroupBundleReadPathV1, TokenFile: tokenFile, VerifierKeyringFile: verifierFile}
	service.RouteBundleHTTPClient = bundleServer.Client()
	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	nodeStatus, nodeHealthy, servingHealthy, bootstrapEligible := inventoryProducerHealth(service.Status(), service.Config)
	if nodeStatus != model.EdgeHealthHealthy || !nodeHealthy || !servingHealthy || bootstrapEligible {
		t.Fatalf("worker did not become serving only after signed bundle sync: status=%+v", service.Status())
	}

	request := httptest.NewRequest(http.MethodGet, "http://bootstrap.example.test/canary", nil)
	request.Host = "bootstrap.example.test"
	response := httptest.NewRecorder()
	service.ProxyHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != "generic-edge-route-ok" {
		t.Fatalf("real route canary failed: status=%d body=%q", response.Code, response.Body.String())
	}
}

type bootstrapBundleSigner struct {
	key   []byte
	keyID string
}

func (signer bootstrapBundleSigner) SignGroupBundle(_ context.Context, _ string, bundle model.EdgeRouteBundle) (model.EdgeRouteBundle, error) {
	return bundleauth.SignEdgeRouteBundleWithKeyring(bundle, bundleauth.NewKeyring(string(signer.key), signer.keyID, "", "", nil), 30*time.Minute), nil
}

func privateBootstrapStateDir(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}

func writePrivateJSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
