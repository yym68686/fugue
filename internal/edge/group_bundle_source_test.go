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
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/edgegroupfront"
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
	request, _, err := service.newRoutesRequestWithSelection(context.Background())
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
	heartbeat := config.EdgeFromEnv()
	routes := RouteBundleSourceFromEnv()
	if heartbeat.APIURL != "https://api.example.test" ||
		routes.URL != "http://edge-control-us.fugue-system.svc:8092/v1/edge/routes" ||
		routes.TokenFile != "/var/run/secrets/fugue-edge/route-reader/token" ||
		routes.VerifierKeyringFile != "/var/run/secrets/fugue-edge/bundle-verifier/keyring.json" {
		t.Fatalf("edge route source was not independently configured: heartbeat=%+v routes=%+v", heartbeat, routes)
	}
}

func TestInactiveWorkerLoadsCandidateWithoutChangingActiveWorkerOrAuthority(t *testing.T) {
	const groupID = "edge-group-country-us"
	const readerToken = "reader-token-0123456789-abcdef-0123456789"
	const keyID = "edge-us-key-v1"
	const candidateRecord = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const releaseRecord = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	key := []byte("0123456789abcdef0123456789abcdef")
	now := time.Now().UTC()
	root := t.TempDir()
	tokenFile := filepath.Join(root, "reader-token")
	if err := os.WriteFile(tokenFile, []byte(readerToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	keyringFile := filepath.Join(root, "verifier-keyring.json")
	writeEdgeVerifierKeyring(t, keyringFile, groupID, keyID, key)
	activationFile := writeInventoryActivationFixtureWithBundle(t, now, groupID, model.EdgeSlotA, strings.Repeat("1", 40), "generation-current")

	current := signedEdgeControlTestBundle(groupID, "generation-current", 1, 0, keyID, key)
	canaryRoute := current.Routes[0]
	canaryRoute.Hostname = "candidate-canary.example.test"
	stableRoute := current.Routes[0]
	stableRoute.Hostname = "stable.example.test"
	current.Routes = []model.EdgeRouteBinding{canaryRoute, stableRoute}
	current.Signature, current.Signatures = "", nil
	current = bundleauth.SignEdgeRouteBundleWithKeyring(current, bundleauth.NewKeyring(string(key), keyID, "", "", nil), 30*time.Minute)

	// This is a valid, signed and routable candidate, but deliberately lacks
	// the canary route. The inactive worker may load it; CurrentAuthority must
	// remain on slot A and ordinary traffic must retain the current bundle.
	candidate := signedEdgeControlTestBundle(groupID, "generation-candidate", 2, 0, keyID, key)
	candidate.Routes = []model.EdgeRouteBinding{stableRoute}
	candidate.Signature, candidate.Signatures = "", nil
	candidate = bundleauth.SignEdgeRouteBundleWithKeyring(candidate, bundleauth.NewKeyring(string(key), keyID, "", "", nil), 30*time.Minute)

	var currentRequests, candidateRequests int
	routeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.Header.Get("Authorization") != "Bearer "+readerToken ||
			request.URL.Query().Get("edge_group_id") != groupID {
			t.Fatalf("unexpected route request: %s %s", request.Method, request.URL.String())
		}
		var bundle model.EdgeRouteBundle
		var sequence uint64
		switch request.URL.Path {
		case edgeControlBundlePath:
			currentRequests++
			bundle, sequence = current, 1
		case edgeControlCandidateBundlePath:
			candidateRequests++
			bundle, sequence = candidate, 2
			w.Header().Set(edgeControlCandidateRecordHeader, candidateRecord)
			w.Header().Set(edgeControlReleaseRecordHeader, releaseRecord)
			w.Header().Set(edgeControlCandidateSlotHeader, model.EdgeSlotB)
		default:
			t.Fatalf("unexpected route path: %s", request.URL.Path)
		}
		w.Header().Set("ETag", strconv.Quote(bundle.Version))
		w.Header().Set(edgeControlGroupHeader, groupID)
		w.Header().Set(edgeControlGenerationHeader, bundle.Generation)
		w.Header().Set(edgeControlPublicationHeader, strconv.FormatUint(sequence, 10))
		w.Header().Set(edgeControlRecoveryEpochHeader, "0")
		_ = json.NewEncoder(w).Encode(bundle)
	}))
	defer routeServer.Close()

	newWorker := func(slot, cacheName string) *Service {
		return NewServiceWithRouteBundleSource(config.EdgeConfig{
			APIURL: "http://127.0.0.1:1", EdgeToken: "heartbeat-token", EdgeID: "edge-us-" + slot,
			EdgeGroupID: groupID, EdgeSlot: slot, CachePath: filepath.Join(root, cacheName), HTTPTimeout: time.Second,
		}, RouteBundleSourceConfig{
			URL: routeServer.URL + edgeControlBundlePath, CandidateURL: routeServer.URL + edgeControlCandidateBundlePath,
			TokenFile: tokenFile, VerifierKeyringFile: keyringFile, ActivationStateFile: activationFile,
		}, log.New(io.Discard, "", 0))
	}
	active := newWorker(model.EdgeSlotA, "active-cache.json")
	inactive := newWorker(model.EdgeSlotB, "inactive-cache.json")
	if err := active.SyncOnce(context.Background()); err != nil {
		t.Fatalf("active worker current sync: %v", err)
	}
	if err := inactive.SyncOnce(context.Background()); err != nil {
		t.Fatalf("inactive worker candidate sync: %v", err)
	}
	activeStatus, inactiveStatus := active.Status(), inactive.Status()
	if activeStatus.CandidateBundleLoaded || activeStatus.ServingGeneration != "generation-current" || activeStatus.RouteCount != 2 {
		t.Fatalf("active worker was changed by candidate loading: %+v", activeStatus)
	}
	if !inactiveStatus.CandidateBundleLoaded || inactiveStatus.CandidateRecordDigest != candidateRecord ||
		inactiveStatus.CandidateReleaseRecordDigest != releaseRecord || inactiveStatus.CandidateWorkerSlot != model.EdgeSlotB ||
		inactiveStatus.ServingGeneration != "generation-candidate" || inactiveStatus.RouteCount != 1 {
		t.Fatalf("inactive worker did not bind the candidate: %+v", inactiveStatus)
	}
	activeBundle, ok := active.Bundle()
	if !ok || len(activeBundle.Routes) != 2 || activeBundle.Routes[0].Hostname != "candidate-canary.example.test" {
		t.Fatalf("ordinary current bundle changed: %+v", activeBundle)
	}
	activation, exists, err := edgegroupfront.ReadActivationState(activationFile)
	if err != nil || !exists || activation.ActiveSlot != model.EdgeSlotA || activation.Generation != 1 {
		t.Fatalf("candidate loading changed CurrentAuthority: state=%+v exists=%t err=%v", activation, exists, err)
	}
	if currentRequests != 1 || candidateRequests != 1 {
		t.Fatalf("route source selection current=%d candidate=%d", currentRequests, candidateRequests)
	}
}

func TestCandidateRouteSourceRejectsActivationChangeAndCrossSlotRecord(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	activationFile := writeInventoryActivationFixture(t, now, "edge-group-country-us", model.EdgeSlotA, strings.Repeat("1", 40))
	service := NewServiceWithRouteBundleSource(config.EdgeConfig{EdgeGroupID: "edge-group-country-us", EdgeSlot: model.EdgeSlotB}, RouteBundleSourceConfig{
		URL:          "http://edge-control-us.fugue-system.svc:8092" + edgeControlBundlePath,
		CandidateURL: "http://edge-control-us.fugue-system.svc:8092" + edgeControlCandidateBundlePath,
		TokenFile:    filepath.Join(root, "token"), VerifierKeyringFile: filepath.Join(root, "keyring"),
		ActivationStateFile: activationFile,
	}, log.New(io.Discard, "", 0))
	if err := validateEdgeControlRouteSourceConfig(service.edgeRouteSourceConfig()); err != nil {
		t.Fatal(err)
	}
	selection, err := service.selectRouteBundleSource()
	if err != nil || !selection.candidate {
		t.Fatalf("inactive candidate selection failed: selection=%+v err=%v", selection, err)
	}
	if _, err := edgegroupfront.ApplyActivationCAS(activationFile, edgegroupfront.ActivationCASRequest{
		GroupID: "edge-group-country-us", ExpectedGeneration: 1, ExpectedSlot: model.EdgeSlotA, TargetSlot: model.EdgeSlotB,
		BundleGeneration: "bundle-generation-2", WorkerSourceCommit: strings.Repeat("2", 40),
		WorkerImageDigest: "sha256:" + strings.Repeat("c", 64), Operation: edgegroupfront.ActivationOperationPromote,
		Reason: "candidate source selection race test",
	}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := service.validateRouteBundleSourceSelection(selection); err == nil {
		t.Fatal("candidate bundle selection survived an authority CAS change")
	}
	publication := routePublicationMetadata{Source: edgeControlRouteSourceV1, GroupID: "edge-group-country-us", Generation: "generation", PublicationSequence: 1}
	headers := http.Header{}
	headers.Set(edgeControlCandidateRecordHeader, "sha256:"+strings.Repeat("a", 64))
	headers.Set(edgeControlReleaseRecordHeader, "sha256:"+strings.Repeat("b", 64))
	headers.Set(edgeControlCandidateSlotHeader, model.EdgeSlotA)
	if _, err := bindCandidatePublication(headers, publication, model.EdgeSlotB); err == nil {
		t.Fatal("candidate record for another worker slot was accepted")
	}
}

func TestActiveWorkerRejectsCurrentPublicationOutsideActivationGeneration(t *testing.T) {
	selection := routeSourceSelection{candidate: false, activeSlot: model.EdgeSlotB, activationGeneration: 2, expectedGeneration: "verified-candidate-generation"}
	oldCurrent := routePublicationMetadata{Source: edgeControlRouteSourceV1, GroupID: "edge-group-country-de",
		Generation: "old-current-generation", PublicationSequence: 40, RecoveryEpoch: 3}
	if err := validateRouteBundleActivationBinding(selection, oldCurrent); err == nil {
		t.Fatal("active worker accepted old current during authority CAS window")
	}
	promoted := oldCurrent
	promoted.Generation = selection.expectedGeneration
	promoted.PublicationSequence++
	if err := validateRouteBundleActivationBinding(selection, promoted); err != nil {
		t.Fatalf("active worker rejected activation-bound current: %v", err)
	}
	selection.candidate = true
	if err := validateRouteBundleActivationBinding(selection, oldCurrent); err != nil {
		t.Fatalf("inactive candidate was incorrectly bound to current generation: %v", err)
	}
}

func TestActiveWorkerAcceptsOnlyActivationBoundCurrentRefresh(t *testing.T) {
	selection := routeSourceSelection{candidate: false, activeSlot: model.EdgeSlotA, activationGeneration: 37,
		expectedGeneration: "routes-current", expectedPublicationSequence: 11341, expectedRecoveryEpoch: 109}
	current := routePublicationMetadata{Source: edgeControlRouteSourceV1, GroupID: "edge-group-country-de",
		Generation: "routes-current", PublicationSequence: 11341, RecoveryEpoch: 109}
	if err := validateRouteBundleActivationBinding(selection, current); err != nil {
		t.Fatalf("activation-bound promoted current was rejected: %v", err)
	}
	refreshed := current
	refreshed.PublicationSequence++
	refreshed.RecoveryEpoch++
	refreshed.Generation = "routes-refreshed"
	if err := validateRouteBundleActivationBinding(selection, refreshed); err != nil {
		t.Fatalf("newer current publication after the activation anchor was rejected: %v", err)
	}
	for name, mutate := range map[string]func(*routePublicationMetadata){
		"old sequence":        func(value *routePublicationMetadata) { value.PublicationSequence = 11340 },
		"recovery regression": func(value *routePublicationMetadata) { value.RecoveryEpoch = 108 },
		"wrong generation":    func(value *routePublicationMetadata) { value.Generation = "routes-other" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := current
			mutate(&changed)
			if err := validateRouteBundleActivationBinding(selection, changed); err == nil {
				t.Fatal("publication outside the Front authority was accepted")
			}
		})
	}
	generation, sequence, recovery, err := parseActivatedPublicationVersion("routes-current.p11341.r109")
	if err != nil || generation != "routes-current" || sequence != 11341 || recovery != 109 {
		t.Fatalf("activation publication version was not parsed exactly: generation=%q sequence=%d recovery=%d err=%v", generation, sequence, recovery, err)
	}
}

func TestActivatedCandidateAttestationFollowsPromotedCurrentPublications(t *testing.T) {
	selection := routeSourceSelection{activeSlot: model.EdgeSlotB, expectedGeneration: "candidate-generation", expectedPublicationSequence: 11502}
	loadedCandidate := routePublicationMetadata{
		Source: edgeControlRouteSourceV1, GroupID: "edge-group-country-us", Generation: "candidate-generation",
		PublicationSequence: 11502, Candidate: true, CandidateRecord: "sha256:" + strings.Repeat("a", 64),
		ReleaseRecord: "sha256:" + strings.Repeat("b", 64), WorkerSlot: model.EdgeSlotB,
	}
	promoted := routePublicationMetadata{
		Source: edgeControlRouteSourceV1, GroupID: loadedCandidate.GroupID, Generation: loadedCandidate.Generation,
		PublicationSequence: 11502, RecoveryEpoch: 41,
	}
	promoted = retainActivatedCandidateAttestation(selection, model.EdgeSlotB, loadedCandidate, promoted)
	if promoted.Candidate || promoted.CandidateRecord != loadedCandidate.CandidateRecord ||
		promoted.ReleaseRecord != loadedCandidate.ReleaseRecord || promoted.WorkerSlot != loadedCandidate.WorkerSlot {
		t.Fatalf("promoted current lost candidate attestation: %+v", promoted)
	}
	refreshed := promoted
	refreshed.Generation = "current-refresh"
	refreshed.PublicationSequence++
	refreshed.CandidateRecord, refreshed.ReleaseRecord, refreshed.WorkerSlot = "", "", ""
	refreshed = retainActivatedCandidateAttestation(selection, model.EdgeSlotB, promoted, refreshed)
	if refreshed.CandidateRecord != loadedCandidate.CandidateRecord || refreshed.ReleaseRecord != loadedCandidate.ReleaseRecord || refreshed.WorkerSlot != model.EdgeSlotB {
		t.Fatalf("current refresh lost release attestation: %+v", refreshed)
	}
	otherSlotCurrent := promoted
	otherSlotCurrent.CandidateRecord, otherSlotCurrent.ReleaseRecord, otherSlotCurrent.WorkerSlot = "", "", ""
	if got := retainActivatedCandidateAttestation(selection, model.EdgeSlotA, loadedCandidate, otherSlotCurrent); got.CandidateRecord != "" {
		t.Fatal("attestation helper unexpectedly rewrote another worker slot")
	}
}

func TestCandidatePublicationMayStartAtIndependentRecoveryEpoch(t *testing.T) {
	current := routePublicationMetadata{
		Source:              edgeControlRouteSourceV1,
		GroupID:             "edge-group-country-de",
		Generation:          "current-generation",
		PublicationSequence: 11233,
		RecoveryEpoch:       3,
	}
	candidate := current
	candidate.Generation = "candidate-generation"
	candidate.PublicationSequence = 11448
	candidate.RecoveryEpoch = 0
	candidate.Candidate = true
	candidate.CandidateRecord = "sha256:" + strings.Repeat("a", 64)
	candidate.ReleaseRecord = "sha256:" + strings.Repeat("b", 64)
	candidate.WorkerSlot = model.EdgeSlotB
	if err := validateRoutePublicationAdvance(current, candidate); err != nil {
		t.Fatalf("candidate was rejected solely for independent recovery epoch: %v", err)
	}
	candidate.Candidate = false
	if err := validateRoutePublicationAdvance(current, candidate); err == nil {
		t.Fatal("ordinary publication regressed recovery epoch was accepted")
	}
}

func TestCandidatePublicationCanPromoteToCurrentOnRecoveryAdvance(t *testing.T) {
	previousCandidate := routePublicationMetadata{
		Source:              edgeControlRouteSourceV1,
		GroupID:             "edge-group-country-de",
		Generation:          "candidate-generation",
		PublicationSequence: 11502,
		RecoveryEpoch:       0,
		Candidate:           true,
		CandidateRecord:     "sha256:" + strings.Repeat("a", 64),
		ReleaseRecord:       "sha256:" + strings.Repeat("b", 64),
		WorkerSlot:          model.EdgeSlotB,
	}
	current := previousCandidate
	current.Generation = "current-generation"
	current.PublicationSequence = 11271
	current.RecoveryEpoch = 41
	current.Candidate = false
	current.CandidateRecord = ""
	current.ReleaseRecord = ""
	current.WorkerSlot = ""
	if err := validateRoutePublicationAdvance(previousCandidate, current); err != nil {
		t.Fatalf("candidate-to-current authority transition was rejected: %v", err)
	}
	current.RecoveryEpoch = previousCandidate.RecoveryEpoch
	if err := validateRoutePublicationAdvance(previousCandidate, current); err == nil {
		t.Fatal("candidate-to-current transition without recovery advance was accepted")
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
