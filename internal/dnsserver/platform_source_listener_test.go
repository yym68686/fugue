package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeprobe"
	"fugue/internal/testfixture/celldns"
	"github.com/miekg/dns"
)

func TestSourceContextFailureKeepsVerifiedListenerWithoutAdmittingNegativeCandidate(t *testing.T) {
	request, policies, activations := celldns.SourceAuthorizedRequest(t, false)
	request.Policy.EdgeSelectionConstraints[0].FactMaxAgeSeconds = 60
	compiled := celldns.Compile(t, request)
	keys := celldns.Keys()
	now := time.Now().UTC()
	snapshot := celldns.SourceSnapshot(t, request, compiled.DNSArtifact, policies, activations, now)
	release := model.PlatformArtifactRelease{ID: "dns-full", ArtifactID: compiled.ReleaseArtifact.ID, ArtifactKind: compiled.ReleaseArtifact.ArtifactKind, ScopeKey: compiled.ReleaseArtifact.ScopeKey, Generation: compiled.ReleaseArtifact.Generation, ReleaseChannel: "full", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, VerificationState: model.PlatformArtifactVerificationStateVerified, ReleasedAt: now}
	release.VerifiedAt, release.VerifiedLKGGeneration = &now, release.Generation
	assignment := model.PlatformConsumerAssignment{ArtifactID: compiled.DNSArtifact.ID, ArtifactKind: compiled.DNSArtifact.ArtifactKind, ArtifactReleaseID: release.ID, ReleaseSetID: compiled.ReleaseArtifact.ID, ExpectedConsumerSetID: "dns-expected", Revision: 1, ScopeKey: compiled.DNSArtifact.ScopeKey, ExpectedGeneration: compiled.DNSArtifact.Generation, GenerationSequence: compiled.DNSArtifact.GenerationSequence, ContentHash: compiled.DNSArtifact.ContentHash, FencingToken: 1, ReleaseChannel: "full"}
	candidate := dnsPlatformCandidate{Artifact: compiled.DNSArtifact, Assignment: assignment, Release: release}
	consumer, _ := platformcontrol.PlatformConsumerID(model.PlatformConsumerComponentDNSServer, "dns-a", "cell-dns")
	var assignmentReads, sourceReads, receipts, driftOnRead, driftOnAssignmentRead atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, incoming *http.Request) {
		var value any
		switch {
		case strings.HasSuffix(incoming.URL.Path, "/identity"):
			value = platformconsumer.Identity{Token: "scoped", ExpiresAt: now.Add(time.Hour), Component: "dns-server", NodeID: "dns-a", AuthorityID: "cell-dns", ConsumerID: consumer, ScopeKey: compiled.DNSArtifact.ScopeKey, ArtifactKinds: []string{compiled.DNSArtifact.ArtifactKind}}
		case strings.HasSuffix(incoming.URL.Path, "/assignment"):
			candidate := assignment
			if assignmentReads.Add(1) == driftOnAssignmentRead.Load() {
				candidate.Revision++
			}
			value = model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{candidate}}
		case strings.HasSuffix(incoming.URL.Path, "/retained-route-sources"):
			// This fixture has no remotely verified positive LKG. A failed current
			// source scan must still preserve the listener without inventing quorum.
			writer.WriteHeader(http.StatusNotFound)
			return
		case strings.HasSuffix(incoming.URL.Path, "/route-sources"):
			read := sourceReads.Add(1)
			response := model.PlatformConsumerDNSRouteSourcesResponse{Assignment: assignment, Release: release, Snapshot: snapshot}
			if read == driftOnRead.Load() {
				if read == 3 {
					response.Assignment.Revision++
				} else {
					response.Snapshot.SelectionDigest = "changed"
				}
			}
			value = response
		case strings.HasSuffix(incoming.URL.Path, "/trusted-heartbeat"):
			receipts.Add(1)
			var heartbeat platformcontrol.PlatformConsumerHeartbeatEnvelope
			json.NewDecoder(incoming.Body).Decode(&heartbeat)
			value = model.PlatformConsumerHeartbeatResponse{Consumer: model.PlatformConsumerInstance{IdentityVerified: true, ConsumerID: heartbeat.ConsumerID, Sequence: heartbeat.Sequence, EvidenceHash: heartbeat.EvidenceHash, ExpectedConsumerSetID: heartbeat.ExpectedConsumerSetID}}
		case strings.HasSuffix(incoming.URL.Path, "/"+compiled.DNSArtifact.ID):
			value = candidate
		case strings.HasSuffix(incoming.URL.Path, "/"+compiled.ReleaseArtifact.ID):
			value = map[string]any{"artifact": compiled.ReleaseArtifact, "assignment": assignment, "release": release}
		default:
			t.Errorf("unexpected source lifecycle endpoint %s", incoming.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		json.NewEncoder(writer).Encode(value)
	}))
	defer server.Close()
	newService := func(t *testing.T) *Service {
		t.Helper()
		directory := t.TempDir()
		service := NewService(config.DNSConfig{APIURL: server.URL, DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: compiled.DNSArtifact.ScopeKey, Zone: "example.test", CachePath: filepath.Join(directory, "cache"), BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, nil)
		service.PlatformTokenFile = filepath.Join(directory, "token")
		if err := os.WriteFile(service.PlatformTokenFile, []byte("scoped"), 0600); err != nil {
			t.Fatal(err)
		}
		return service
	}
	plans, err := dnsroutesource.Build(compiled.DNSArtifact, snapshot, keys, now)
	if err != nil {
		t.Fatal(err)
	}
	negative := false
	probe := func(_ context.Context, hostname, path, address, state string, _ time.Duration) (routeprobe.Proof, error) {
		for _, requirement := range plans.Probes {
			if requirement.Hostname != hostname || requirement.Path != path || requirement.Address != address {
				continue
			}
			if negative && requirement.EdgeID == "edge-b" {
				return routeprobe.Proof{}, errors.New("authenticated route rejection")
			}
			return routeprobe.Proof{EdgeID: requirement.EdgeID, GroupID: requirement.EdgeGroupID, Digest: requirement.RouteDigest, Version: "actual", State: state, TrafficRelease: plans.Bindings[requirement.CellPublicationDigest], CheckedAt: time.Now().Add(-time.Millisecond), ValidUntil: time.Now().Add(time.Minute)}, nil
		}
		return routeprobe.Proof{}, errors.New("missing probe")
	}
	for _, tc := range []struct {
		name string
		read int32
		wire bool
	}{{"assignment drift before wire", 3, false}, {"source drift after wire", 4, true}} {
		t.Run(tc.name, func(t *testing.T) {
			sourceReads.Store(0)
			driftOnRead.Store(tc.read)
			service := newService(t)
			wire := false
			err := service.syncPlatformDNSServingOnce(context.Background(), probe, func(*dnsServingState) error { wire = true; return nil })
			if !errors.Is(err, platformconsumer.ErrAssignmentChanged) || wire != tc.wire || receipts.Load() != 0 || service.platformServing.Load() != nil || service.platformCheckpoint.Load() != nil {
				t.Fatalf("drift admitted: error=%v wire=%v receipts=%d", err, wire, receipts.Load())
			}
		})
	}
	assignmentReads.Store(0)
	sourceReads.Store(0)
	driftOnRead.Store(0)
	service := newService(t)
	if err := service.syncPlatformDNSServingOnce(context.Background(), probe, func(*dnsServingState) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if assignmentReads.Load() != 2 || sourceReads.Load() != 4 || receipts.Load() != 1 {
		t.Fatalf("unexpected pre/post apply and receipt checks: assignments=%d sources=%d receipts=%d", assignmentReads.Load(), sourceReads.Load(), receipts.Load())
	}
	previous := service.platformServing.Load()
	if previous == nil || !previous.record.Positive {
		t.Fatal("initial source context not admitted")
	}
	checkpoint := service.platformCheckpoint.Load()
	unexpectedProbe := func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		t.Error("fresh exact receipt repeated a network sweep")
		return routeprobe.Proof{}, errors.New("unexpected probe")
	}
	wireCalls := 0
	checkWire := func(state *dnsServingState) error {
		wireCalls++
		if state != previous {
			t.Error("heartbeat changed the loaded publication")
		}
		return nil
	}
	driftOnAssignmentRead.Store(4)
	if err := service.syncPlatformDNSServingOnce(t.Context(), unexpectedProbe, checkWire); !errors.Is(err, platformconsumer.ErrAssignmentChanged) || receipts.Load() != 1 || wireCalls != 1 {
		t.Fatalf("superseded assignment was not fenced: %v (receipts=%d, wire=%d)", err, receipts.Load(), wireCalls)
	}
	driftOnAssignmentRead.Store(0)
	if err := service.syncPlatformDNSServingOnce(t.Context(), unexpectedProbe, checkWire); err != nil {
		t.Fatal(err)
	}
	if assignmentReads.Load() != 6 || sourceReads.Load() != 6 || receipts.Load() != 2 || wireCalls != 2 || service.platformServing.Load() != previous || service.platformCheckpoint.Load() != checkpoint {
		t.Fatalf("receipt did not take the bounded no-apply path: assignments=%d sources=%d receipts=%d wire=%d", assignmentReads.Load(), sourceReads.Load(), receipts.Load(), wireCalls)
	}
	listenerFailure := errors.New("listener stopped")
	if tried, err := service.reportFreshDNSRouteSourceHeartbeat(t.Context(), service.platformConsumerClient(), platformconsumer.Identity{}, previous, func(*dnsServingState) error { return listenerFailure }); tried || !errors.Is(err, listenerFailure) || receipts.Load() != 2 {
		t.Fatalf("listener failure could acknowledge the old publication: tried=%v err=%v", tried, err)
	}
	failedRenewal := *previous
	failedRenewal.checkedAt = time.Now().UTC()
	failedRenewal.facts = nil
	service.platformRenewal.Store(&dnsServingRenewal{base: previous, state: &failedRenewal})
	if tried, err := service.reportFreshDNSRouteSourceHeartbeat(t.Context(), service.platformConsumerClient(), platformconsumer.Identity{}, previous, func(*dnsServingState) error {
		t.Error("used an older positive scan despite newer negative evidence")
		return nil
	}); tried || err != nil || receipts.Load() != 2 {
		t.Fatal("negative evidence produced a positive heartbeat", err)
	}
	service.platformRenewal.Store(nil)
	t.Run("completed independent sweep reports only its already verified checkpoint", func(t *testing.T) {
		worker := service.newDNSRenewalWorker()
		var o *dnsServingRenewal
		var id platformconsumer.Identity
		if err := service.observeDNSRenewalWithSelection(t.Context(), worker, probe, func(observed *dnsServingRenewal, identity platformconsumer.Identity) {
			o, id = observed, identity
		}); err != nil || o == nil || o.state != service.currentDNSServingState() {
			t.Fatalf("no independently selected physical sweep: %v", err)
		}
		originalCount := receipts.Load()
		// Move only the in-process last-report clock, not the signed proof or its deadlines.
		staleReport := func() {
			service.mu.Lock()
			service.platformServingReported = time.Now().Add(-time.Minute)
			service.mu.Unlock()
		}
		staleReport()
		wire := 0
		check := func(st *dnsServingState) error {
			wire++
			if st != o.state {
				t.Fatal("listener did not verify observed state")
			}
			return nil
		}
		if err := service.reportCompletedDNSRenewal(t.Context(), o, id, check); err != nil || receipts.Load() != originalCount+1 || wire != 1 ||
			service.platformCheckpoint.Load() != checkpoint || service.platformServing.Load() != previous {
			t.Fatalf("heartbeat missed, changed configuration, or extended checkpoint: receipts=%d wire=%d err=%v", receipts.Load()-originalCount, wire, err)
		}
		if err := service.reportCompletedDNSRenewal(t.Context(), o, id, check); err != nil || receipts.Load() != originalCount+1 || wire != 1 {
			t.Fatal("replayed a completed observation")
		}
		staleReport()
		service.mu.Lock()
		service.platformReceiptSignature = ""
		service.mu.Unlock()
		if err := service.reportCompletedDNSRenewal(t.Context(), o, id, check); err != nil || wire != 1 || receipts.Load() != originalCount+1 {
			t.Fatal("a newly loaded, never acknowledged checkpoint was reported by the observer")
		}
		service.mu.Lock()
		service.platformReceiptSignature = previous.record.Signature
		service.mu.Unlock()
		driftOnRead.Store(sourceReads.Load() + 1)
		if err := service.reportCompletedDNSRenewal(t.Context(), o, id, check); !errors.Is(err, platformconsumer.ErrAssignmentChanged) || receipts.Load() != originalCount+1 {
			t.Fatal("authority changed after the wire probe")
		}
		driftOnRead.Store(0)
		service.platformReceiptMu.Lock()
		err = service.reportCompletedDNSRenewal(t.Context(), o, id, check)
		service.platformReceiptMu.Unlock()
		if err != nil || receipts.Load() != originalCount+1 {
			t.Fatal("raced the main receipt")
		}
		failWire := errors.New("dead listener")
		if err := service.reportCompletedDNSRenewal(t.Context(), o, id, func(*dnsServingState) error { return failWire }); !errors.Is(err, failWire) || receipts.Load() != originalCount+1 {
			t.Fatal("dead listener received a receipt")
		}
		if err := service.reportCompletedDNSRenewal(t.Context(), o, id, func(*dnsServingState) error { service.platformRenewal.Store(nil); return nil }); err != nil || receipts.Load() != originalCount+1 {
			t.Fatal("changed observation received a receipt")
		}
		service.platformRenewal.Store(nil)
	})
	for index := range snapshot.Scopes {
		scope := &snapshot.Scopes[index]
		scope.Publications[0].Release.ID += "-next"
		scope.Publications[0].Release.FencingToken++
		scope.Lanes[0].ActiveReleaseID = scope.Publications[0].Release.ID
		scope.Lanes[0].FencingToken = scope.Publications[0].Release.FencingToken
		scope.Lanes[0].Version++
	}
	snapshot.SelectionDigest, _ = dnsroutesource.SelectionDigest(snapshot)
	plans, err = dnsroutesource.Build(compiled.DNSArtifact, snapshot, keys, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	negative = true
	if err := service.syncPlatformDNSServingOnce(context.Background(), probe, func(*dnsServingState) error { return nil }); err == nil {
		t.Fatal("incomplete changed source context admitted")
	}
	current := service.platformServing.Load()
	if current == nil || !reflect.DeepEqual(current.record, previous.record) {
		t.Fatal("changed source failure invalidated or rewrote positive DNS identity")
	}
	service.udpListening.Store(true)
	service.tcpListening.Store(true)
	writer := httptest.NewRecorder()
	service.Handler().ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if writer.Code != http.StatusOK {
		t.Fatal("record failure removed authoritative listener", writer.Body.String())
	}
	for _, record := range []struct {
		host string
		kind uint16
		code int
	}{{"example.test", dns.TypeNS, dns.RcodeSuccess}, {"app.example.test", dns.TypeA, dns.RcodeServerFailure}} {
		query := new(dns.Msg)
		query.SetQuestion(dns.Fqdn(record.host), record.kind)
		if answer := current.answer(query, "", time.Now()); answer.Rcode != record.code {
			t.Fatal("source failure leaked across records or fabricated quorum", answer)
		}
	}
	if _, err := service.decodeDNSCheckpoint(mustDNSJSON(t, current.record)); err != nil {
		t.Fatal("retained context no longer authenticates", err)
	}
}
