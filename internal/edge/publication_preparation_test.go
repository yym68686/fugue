package edge

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/edgegroupfront"
	"fugue/internal/model"
	"fugue/internal/routeproof"
)

func TestPreparedWorkerLoadsSignedCurrentBeforeTrafficActivation(t *testing.T) {
	const group = "edge-group-fixture"
	const keyID = "edge-fixture-key"
	key := []byte("0123456789abcdef0123456789abcdef")
	root := t.TempDir()
	now := time.Now().UTC()
	source := strings.Repeat("2", 40)
	activation := writeInventoryActivationFixture(t, now, group, model.EdgeSlotA, strings.Repeat("1", 40))
	token := filepath.Join(root, "token")
	if err := os.WriteFile(token, []byte(strings.Repeat("x", 40)), 0o600); err != nil {
		t.Fatal(err)
	}
	keyring := filepath.Join(root, "keyring")
	writeEdgeVerifierKeyring(t, keyring, group, keyID, key)
	candidate := signedEdgeControlTestBundle(group, "config-next", 7, 0, keyID, key)
	current := signedEdgeControlTestBundle(group, "config-next", 7, 4, keyID, key)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := current
		epoch := "4"
		if r.URL.Path == edgeControlCandidateBundlePath {
			b = candidate
			epoch = "0"
			w.Header().Set(edgeControlCandidateRecordHeader, "sha256:"+strings.Repeat("a", 64))
			w.Header().Set(edgeControlReleaseRecordHeader, "sha256:"+strings.Repeat("b", 64))
			w.Header().Set(edgeControlCandidateSlotHeader, "b")
		}
		w.Header().Set(edgeControlGroupHeader, group)
		w.Header().Set(edgeControlGenerationHeader, b.Generation)
		w.Header().Set(edgeControlPublicationHeader, "7")
		w.Header().Set(edgeControlRecoveryEpochHeader, epoch)
		w.Header().Set("ETag", strconv.Quote(b.Version))
		json.NewEncoder(w).Encode(b)
	}))
	defer server.Close()
	caddy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/load" {
			t.Error("unexpected Caddy request")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer caddy.Close()
	s := NewServiceWithRouteBundleSource(config.EdgeConfig{APIURL: "http://127.0.0.1:1", EdgeToken: "heartbeat", EdgeID: "edge-fixture", EdgeGroupID: group, EdgeSlot: "b", EdgeReleaseEpoch: source, CachePath: filepath.Join(root, "cache.json"), HTTPTimeout: time.Second,
		CaddyEnabled: true, CaddyAdminURL: caddy.URL, CaddyListenAddr: "127.0.0.1:18080", CaddyProxyListenAddr: "127.0.0.1:18081", CaddyTLSMode: caddyTLSModeOff}, RouteBundleSourceConfig{URL: server.URL + edgeControlBundlePath, CandidateURL: server.URL + edgeControlCandidateBundlePath, TokenFile: token, VerifierKeyringFile: keyring, ActivationStateFile: activation}, log.New(io.Discard, "", 0))
	defer s.cancelCurrentCaddyWarmup()
	proof := func() int {
		r := httptest.NewRequest(http.MethodHead, "http://"+candidate.Routes[0].Hostname+"/", nil)
		r.Header.Set(routeproof.RequestHeader, "1")
		r.Header.Set(routeproof.NonceHeader, strings.Repeat("a", 32))
		w := httptest.NewRecorder()
		s.ProxyHandler().ServeHTTP(w, r)
		return w.Code
	}
	if err := s.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if proof() != 503 {
		t.Fatal("unprepared candidate obtained ordinary route proof")
	}
	before, _ := os.ReadFile(activation)
	request := edgegroupfront.ActivationCASRequest{GroupID: group, ExpectedGeneration: 1, ExpectedSlot: "a", TargetSlot: "b", BundleGeneration: current.Version, WorkerSourceCommit: source, WorkerImageDigest: "sha256:" + strings.Repeat("c", 64), Operation: edgegroupfront.ActivationOperationPromote, Reason: "prepare exact signed publication for traffic", RequirePublicationPreparation: true}
	if _, err := edgegroupfront.PreparePublication(activation, request, now); err != nil {
		t.Fatal(err)
	}
	selection, err := s.selectRouteBundleSource()
	if err != nil || selection.candidate || selection.preparation == nil {
		t.Fatal("inactive Worker did not select prepared current stream", selection, err)
	}
	if active, known := s.servingActiveForHeartbeat(); !known || active {
		t.Fatal("preparation advertised active traffic")
	}
	validCurrent := current
	current.Signature = "invalid"
	current.Signatures = nil
	if err := s.SyncOnce(context.Background()); err == nil {
		t.Fatal("prepared worker accepted unsigned current publication")
	}
	stillOld, _ := os.ReadFile(activation)
	if string(stillOld) != string(before) || proof() != 503 {
		t.Fatal("failed preparation changed traffic or candidate proof")
	}
	current = validCurrent
	if err := s.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(activation)
	if string(before) != string(after) || s.Status().CandidateBundleLoaded || s.Status().CaddyAppliedVersion != current.Version || proof() != 204 {
		t.Fatal("formal publication not ready while previous traffic remained unchanged")
	}
	if _, err := edgegroupfront.ApplyActivationCAS(activation, request, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if proof() != 204 {
		t.Fatal("activation introduced DNS proof gap")
	}
	if err := s.validateRouteBundleSourceSelection(selection); err == nil {
		t.Fatal("preparation load accepted after predecessor changed")
	}
	if active, known := s.servingActiveForHeartbeat(); !known || !active {
		t.Fatal("committed traffic not reflected in heartbeat")
	}
}

func TestPublicationPreparationCannotSelectForeignOrExpiredWorker(t *testing.T) {
	now := time.Now().UTC()
	group := "edge-group-fixture"
	source := strings.Repeat("2", 40)
	activation := writeInventoryActivationFixture(t, now.Add(-3*time.Minute), group, "a", strings.Repeat("1", 40))
	request := edgegroupfront.ActivationCASRequest{GroupID: group, ExpectedGeneration: 1, ExpectedSlot: "a", TargetSlot: "b", BundleGeneration: "next.p7.r4", WorkerSourceCommit: source, WorkerImageDigest: "sha256:" + strings.Repeat("c", 64), Operation: edgegroupfront.ActivationOperationPromote, Reason: "test exact code publication preparation"}
	if _, err := edgegroupfront.PreparePublication(activation, request, now.Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	s := NewServiceWithRouteBundleSource(config.EdgeConfig{EdgeGroupID: group, EdgeSlot: "b", EdgeReleaseEpoch: source}, RouteBundleSourceConfig{URL: "http://control.test" + edgeControlBundlePath, CandidateURL: "http://control.test" + edgeControlCandidateBundlePath, TokenFile: filepath.Join(t.TempDir(), "token"), VerifierKeyringFile: filepath.Join(t.TempDir(), "key"), ActivationStateFile: activation}, nil)
	sel, err := s.selectRouteBundleSource()
	if err != nil || !sel.candidate || sel.preparation != nil {
		t.Fatal("expired preparation selected current", err)
	}
	if _, err := edgegroupfront.PreparePublication(activation, request, now); err != nil {
		t.Fatal(err)
	}
	s.Config.EdgeReleaseEpoch = strings.Repeat("3", 40)
	sel, err = s.selectRouteBundleSource()
	if err != nil || !sel.candidate || sel.preparation != nil {
		t.Fatal("foreign code selected current", err)
	}
}

func TestPreparedWorkerPublicationAdvanceWithoutRecovery(t *testing.T) {
	old := routePublicationMetadata{Source: edgeControlRouteSourceV1, GroupID: "edge-group-fixture", Generation: "config", PublicationSequence: 7, Candidate: true}
	current := old
	current.Candidate = false
	if err := validateRoutePublicationAdvance(old, current); err != nil {
		t.Fatal("exact candidate promotion needs no artificial recovery epoch", err)
	}
	for name, mutate := range map[string]func(*routePublicationMetadata){
		"other group":                 func(p *routePublicationMetadata) { p.GroupID = "edge-group-other"; p.RecoveryEpoch = 1 },
		"other authority":             func(p *routePublicationMetadata) { p.Source = "other"; p.RecoveryEpoch = 1 },
		"different config same epoch": func(p *routePublicationMetadata) { p.Generation = "other" },
		"candidate replay":            func(p *routePublicationMetadata) { p.Candidate = true },
		"old sequence":                func(p *routePublicationMetadata) { p.PublicationSequence-- },
	} {
		t.Run(name, func(t *testing.T) {
			p := current
			mutate(&p)
			if validateRoutePublicationAdvance(old, p) == nil {
				t.Fatal("unbound publication accepted")
			}
		})
	}
	if validateRoutePublicationAdvance(current, current) == nil {
		t.Fatal("ordinary current publication replay accepted")
	}
}
