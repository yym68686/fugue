package edge

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/model"
)

func TestEdgeControlRouteSourceSurvivesCoreBlackholeAndRejectsGroupReplayAndBlackhole(t *testing.T) {
	const groupID = "edge-group-country-us"
	const edgeID = "edge-us-1"
	const readerToken = "reader-token-0123456789-abcdef-0123456789"
	const keyID = "edge-us-key-v1"
	key := []byte("0123456789abcdef0123456789abcdef")
	root := t.TempDir()
	tokenFile := filepath.Join(root, "reader-token")
	if err := os.WriteFile(tokenFile, []byte(readerToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	keyringFile := filepath.Join(root, "verifier-keyring.json")
	writeEdgeVerifierKeyring(t, keyringFile, groupID, keyID, key)
	cachePath := filepath.Join(root, "routes-cache.json")

	type publication struct {
		bundle   model.EdgeRouteBundle
		sequence uint64
		epoch    uint64
	}
	var publicationMu sync.Mutex
	current := publication{bundle: signedEdgeControlTestBundle(groupID, "generation-one", 1, 0, keyID, key), sequence: 1}
	routeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != edgeControlBundlePath || request.URL.Query().Get("token") != "" || request.Header.Get("Authorization") != "Bearer "+readerToken ||
			request.URL.Query().Get("edge_id") != edgeID || request.URL.Query().Get("edge_group_id") != groupID {
			t.Fatalf("unexpected route request: %s %s", request.Method, request.URL.String())
		}
		if strings.Contains(request.URL.String(), readerToken) {
			t.Fatal("reader token leaked into route URL")
		}
		publicationMu.Lock()
		observed := current
		publicationMu.Unlock()
		w.Header().Set("ETag", strconv.Quote(observed.bundle.Version))
		w.Header().Set(edgeControlGroupHeader, groupID)
		w.Header().Set(edgeControlGenerationHeader, observed.bundle.Generation)
		w.Header().Set(edgeControlPublicationHeader, strconv.FormatUint(observed.sequence, 10))
		w.Header().Set(edgeControlRecoveryEpochHeader, strconv.FormatUint(observed.epoch, 10))
		if request.Header.Get("If-None-Match") == strconv.Quote(observed.bundle.Version) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_ = json.NewEncoder(w).Encode(observed.bundle)
	}))
	defer routeServer.Close()

	service := NewServiceWithRouteBundleSource(config.EdgeConfig{
		APIURL:              "http://127.0.0.1:1",
		EdgeDesiredStateURL: "http://127.0.0.1:1/v1/edge/nodes/edge-us-1/desired-state",
		EdgeToken:           "heartbeat-token",
		EdgeID:              edgeID,
		EdgeGroupID:         groupID,
		CachePath:           cachePath,
		HTTPTimeout:         100 * time.Millisecond,
	}, RouteBundleSourceConfig{
		URL:                 routeServer.URL + edgeControlBundlePath,
		TokenFile:           tokenFile,
		VerifierKeyringFile: keyringFile,
	}, log.New(io.Discard, "", 0))
	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatalf("edge-control sync failed while Core was blackholed: %v", err)
	}
	status := service.Status()
	if status.RouteBundleSource != edgeControlRouteSourceV1 || status.PublicationSequence != 1 || status.RecoveryEpoch != 0 || status.ServingGeneration != "generation-one" {
		t.Fatalf("unexpected first publication status: %+v", status)
	}

	// An HTTP 200 replay cannot advance the worker's durable publication CAS.
	service.mu.Lock()
	service.etag = ""
	service.mu.Unlock()
	if err := service.SyncOnce(context.Background()); err == nil {
		t.Fatal("replayed edge-control publication unexpectedly replaced LKG")
	}
	if got := service.Status(); got.PublicationSequence != 1 || got.ServingGeneration != "generation-one" || !got.Healthy {
		t.Fatalf("replay did not preserve the group LKG: %+v", got)
	}

	// A newer, correctly signed publication that blackholes every route is also
	// rejected before cache promotion.
	blackhole := signedEdgeControlTestBundle(groupID, "generation-blackhole", 2, 0, keyID, key)
	blackhole.Routes = nil
	blackhole.Signature = ""
	blackhole.Signatures = nil
	blackhole = bundleauth.SignEdgeRouteBundleWithKeyring(blackhole, bundleauth.NewKeyring(string(key), keyID, "", "", nil), 30*time.Minute)
	publicationMu.Lock()
	current = publication{bundle: blackhole, sequence: 2}
	publicationMu.Unlock()
	if err := service.SyncOnce(context.Background()); err == nil {
		t.Fatal("catastrophic edge-control route candidate unexpectedly replaced LKG")
	}
	if got := service.Status(); got.PublicationSequence != 1 || got.ServingGeneration != "generation-one" || !got.Healthy {
		t.Fatalf("catastrophic candidate did not preserve the group LKG: %+v", got)
	}

	// A process restart verifies and restores the independently signed group LKG
	// without consulting Core.
	restarted := NewServiceWithRouteBundleSource(service.Config, service.RouteBundleSource, log.New(io.Discard, "", 0))
	if err := restarted.LoadCache(); err != nil {
		t.Fatalf("restart could not restore group LKG: %v", err)
	}
	if got := restarted.Status(); got.PublicationSequence != 1 || got.ServingGeneration != "generation-one" || !got.Healthy {
		t.Fatalf("restart restored the wrong group LKG: %+v", got)
	}
}

func TestEdgeControlRouteSourceRejectsMismatchedGroupVerifier(t *testing.T) {
	root := t.TempDir()
	keyringFile := filepath.Join(root, "verifier-keyring.json")
	writeEdgeVerifierKeyring(t, keyringFile, "edge-group-country-de", "edge-de-key-v1", []byte("0123456789abcdef0123456789abcdef"))
	if _, err := loadEdgeRouteVerifierKeyring(keyringFile, "edge-group-country-us"); err == nil {
		t.Fatal("cross-group verifier keyring unexpectedly accepted")
	}
}

func TestAdoptingWorkerBootstrapsFromLegacyRouteThenConvergesToGroupAuthority(t *testing.T) {
	const groupID = "edge-group-country-de"
	const edgeID = "edge-de-1"
	const readerToken = "reader-token-0123456789-abcdef-0123456789"
	const groupKeyID = "edge-de-key-v1"
	const legacyKeyID = "control-plane"
	groupKey := []byte("0123456789abcdef0123456789abcdef")
	legacyKey := "abcdef0123456789abcdef0123456789"
	root := t.TempDir()
	tokenFile := filepath.Join(root, "reader-token")
	if err := os.WriteFile(tokenFile, []byte(readerToken), 0o600); err != nil {
		t.Fatal(err)
	}
	keyringFile := filepath.Join(root, "verifier-keyring.json")
	writeEdgeVerifierKeyring(t, keyringFile, groupID, groupKeyID, groupKey)

	legacyBundle := testBundle("legacy-bootstrap")
	legacyBundle.GeneratedAt = time.Now().UTC()
	legacyBundle.EdgeGroupID = groupID
	legacyBundle.Routes[0].EdgeGroupID = groupID
	legacyBundle = bundleauth.SignEdgeRouteBundleWithKeyring(legacyBundle, bundleauth.NewKeyring(legacyKey, legacyKeyID, "", "", nil), 30*time.Minute)
	var legacyRequests atomic.Int32
	legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		legacyRequests.Add(1)
		if request.URL.Path != edgeControlBundlePath || request.URL.Query().Get("token") != "legacy-token" || request.URL.Query().Get("edge_group_id") != groupID {
			t.Fatalf("unexpected legacy bootstrap request: %s", request.URL.String())
		}
		_ = json.NewEncoder(w).Encode(legacyBundle)
	}))
	defer legacy.Close()

	groupBundle := signedEdgeControlTestBundle(groupID, "group-generation", 1, 0, groupKeyID, groupKey)
	var groupReady atomic.Bool
	group := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if !groupReady.Load() {
			http.Error(w, "group_bundle_unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set(edgeControlGroupHeader, groupID)
		w.Header().Set(edgeControlGenerationHeader, groupBundle.Generation)
		w.Header().Set(edgeControlPublicationHeader, "1")
		w.Header().Set(edgeControlRecoveryEpochHeader, "0")
		_ = json.NewEncoder(w).Encode(groupBundle)
	}))
	defer group.Close()

	cfg := config.EdgeConfig{APIURL: legacy.URL, EdgeToken: "legacy-token", EdgeID: edgeID, EdgeGroupID: groupID,
		CachePath: filepath.Join(root, "routes-cache.json"), BundleSigningKey: legacyKey, BundleSigningKeyID: legacyKeyID, HTTPTimeout: time.Second}
	routeCfg := RouteBundleSourceConfig{URL: group.URL + edgeControlBundlePath, TokenFile: tokenFile, VerifierKeyringFile: keyringFile, AdoptionLegacyBootstrap: true}
	service := NewServiceWithRouteBundleSource(cfg, routeCfg, log.New(io.Discard, "", 0))
	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatalf("adoption bootstrap sync: %v", err)
	}
	if status := service.Status(); !status.Healthy || status.RouteBundleSource != "" || status.BundleVersion != legacyBundle.Version || legacyRequests.Load() != 1 {
		t.Fatalf("legacy bootstrap was not bounded and healthy: status=%+v requests=%d", status, legacyRequests.Load())
	}

	groupReady.Store(true)
	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatalf("group authority convergence: %v", err)
	}
	if status := service.Status(); status.RouteBundleSource != edgeControlRouteSourceV1 || status.PublicationSequence != 1 || status.ServingGeneration != groupBundle.Generation || legacyRequests.Load() != 1 {
		t.Fatalf("group authority did not supersede bootstrap: status=%+v requests=%d", status, legacyRequests.Load())
	}

	independent := NewServiceWithRouteBundleSource(cfg, RouteBundleSourceConfig{URL: routeCfg.URL, TokenFile: tokenFile, VerifierKeyringFile: keyringFile}, log.New(io.Discard, "", 0))
	if err := independent.LoadCache(); err != nil {
		t.Fatalf("independent worker rejected the group-authority LKG: %v", err)
	}
	groupReady.Store(false)
	independent.Config.CachePath = filepath.Join(root, "independent-empty.json")
	if err := independent.SyncOnce(context.Background()); err == nil {
		t.Fatal("independent worker unexpectedly used adoption legacy bootstrap")
	}
	if legacyRequests.Load() != 1 {
		t.Fatal("independent worker contacted the legacy route source")
	}
}

func TestEdgeControlRouteHTTPClientIgnoresEnvironmentProxyAndRedirects(t *testing.T) {
	client := newEdgeRouteBundleHTTPClient(time.Second)
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil || transport.ForceAttemptHTTP2 {
		t.Fatalf("route client can leak group credentials through proxy or HTTP/2: %#v", client.Transport)
	}
	if err := client.CheckRedirect(nil, nil); err == nil {
		t.Fatal("route client unexpectedly follows redirects")
	}
}

func TestEdgeControlReaderCredentialNeverEntersRequestURLOrURLError(t *testing.T) {
	const readerToken = "reader-token-0123456789-abcdef-0123456789"
	root := t.TempDir()
	tokenFile := filepath.Join(root, "token")
	if err := os.WriteFile(tokenFile, []byte(readerToken), 0o600); err != nil {
		t.Fatal(err)
	}
	service := NewServiceWithRouteBundleSource(config.EdgeConfig{
		APIURL: "http://core.invalid", EdgeToken: "heartbeat-token", EdgeID: "edge-us-1", EdgeGroupID: "edge-group-country-us",
	}, RouteBundleSourceConfig{
		URL: "http://edge-control-us.fugue-system.svc:8092/v1/edge/routes", TokenFile: tokenFile,
		VerifierKeyringFile: filepath.Join(root, "verifier.json"),
	}, log.New(io.Discard, "", 0))
	request, err := service.newRoutesRequest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	urlError := fmt.Errorf("request failed: %s", request.URL.String())
	if request.URL.Query().Get("token") != "" || strings.Contains(request.URL.String(), readerToken) || strings.Contains(urlError.Error(), readerToken) ||
		request.Header.Get("Authorization") != "Bearer "+readerToken {
		t.Fatalf("reader credential escaped fixed Authorization transport: url=%s", request.URL.Redacted())
	}
}

func TestRouteBundleSourceFromEnvIsIndependentFromHeartbeatAPI(t *testing.T) {
	t.Setenv("FUGUE_API_URL", "https://api.example.test")
	t.Setenv("FUGUE_EDGE_ROUTE_BUNDLE_URL", "http://edge-control-us.fugue-system.svc:8092/v1/edge/routes")
	t.Setenv("FUGUE_EDGE_ROUTE_BUNDLE_TOKEN_FILE", "/var/run/secrets/fugue-edge/route-reader/token")
	t.Setenv("FUGUE_EDGE_ROUTE_BUNDLE_VERIFIER_KEYRING_FILE", "/var/run/secrets/fugue-edge/bundle-verifier/keyring.json")
	t.Setenv("FUGUE_EDGE_ROUTE_BUNDLE_ADOPTION_LEGACY_BOOTSTRAP", "true")

	heartbeat := config.EdgeFromEnv()
	routes := RouteBundleSourceFromEnv()
	if heartbeat.APIURL != "https://api.example.test" ||
		routes.URL != "http://edge-control-us.fugue-system.svc:8092/v1/edge/routes" ||
		routes.TokenFile != "/var/run/secrets/fugue-edge/route-reader/token" ||
		routes.VerifierKeyringFile != "/var/run/secrets/fugue-edge/bundle-verifier/keyring.json" || !routes.AdoptionLegacyBootstrap {
		t.Fatalf("edge route source was not independently configured: heartbeat=%+v routes=%+v", heartbeat, routes)
	}
}

func signedEdgeControlTestBundle(groupID, generation string, sequence, recoveryEpoch uint64, keyID string, key []byte) model.EdgeRouteBundle {
	now := time.Now().UTC()
	bundle := testBundle(groupPublicationVersion(generation, sequence, recoveryEpoch))
	bundle.Generation = generation
	bundle.EdgeGroupID = groupID
	bundle.Issuer = "fugue-edge-control"
	bundle.GeneratedAt = now
	bundle.Routes[0].EdgeGroupID = groupID
	bundle.Routes[0].RuntimeEdgeGroupID = groupID
	return bundleauth.SignEdgeRouteBundleWithKeyring(bundle, bundleauth.NewKeyring(string(key), keyID, "", "", nil), 30*time.Minute)
}

func writeEdgeVerifierKeyring(t *testing.T, path, groupID, keyID string, key []byte) {
	t.Helper()
	value := edgeRouteVerifierKeyringFile{
		Schema: edgeControlSigningKeyringSchemaV1, Generation: 1,
		Group: edgeRouteVerifierKeyring{GroupID: groupID, PrimaryKeyID: keyID, PrimaryKey: base64.RawURLEncoding.EncodeToString(key)},
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
