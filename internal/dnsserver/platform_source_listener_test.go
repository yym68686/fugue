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
	compiled := celldns.Compile(t, request)
	keys := celldns.Keys()
	now := time.Now().UTC()
	snapshot := celldns.SourceSnapshot(t, request, compiled.DNSArtifact, policies, activations, now)
	release := model.PlatformArtifactRelease{ID: "dns-full", ArtifactID: compiled.ReleaseArtifact.ID, ArtifactKind: compiled.ReleaseArtifact.ArtifactKind, ScopeKey: compiled.ReleaseArtifact.ScopeKey, Generation: compiled.ReleaseArtifact.Generation, ReleaseChannel: "full", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now}
	assignment := model.PlatformConsumerAssignment{ArtifactID: compiled.DNSArtifact.ID, ArtifactKind: compiled.DNSArtifact.ArtifactKind, ArtifactReleaseID: release.ID, ReleaseSetID: compiled.ReleaseArtifact.ID, ExpectedConsumerSetID: "dns-expected", Revision: 1, ScopeKey: compiled.DNSArtifact.ScopeKey, ExpectedGeneration: compiled.DNSArtifact.Generation, GenerationSequence: compiled.DNSArtifact.GenerationSequence, ContentHash: compiled.DNSArtifact.ContentHash, FencingToken: 1, ReleaseChannel: "full"}
	candidate := dnsPlatformCandidate{Artifact: compiled.DNSArtifact, Assignment: assignment, Release: release}
	consumer, _ := platformcontrol.PlatformConsumerID(model.PlatformConsumerComponentDNSServer, "dns-a", "cell-dns")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, incoming *http.Request) {
		var value any
		switch {
		case strings.HasSuffix(incoming.URL.Path, "/identity"):
			value = platformconsumer.Identity{Token: "scoped", ExpiresAt: now.Add(time.Hour), Component: "dns-server", NodeID: "dns-a", AuthorityID: "cell-dns", ConsumerID: consumer, ScopeKey: compiled.DNSArtifact.ScopeKey, ArtifactKinds: []string{compiled.DNSArtifact.ArtifactKind}}
		case strings.HasSuffix(incoming.URL.Path, "/assignment"):
			value = model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{assignment}}
		case strings.HasSuffix(incoming.URL.Path, "/retained-route-sources"):
			// This fixture has no remotely verified positive LKG. A failed current
			// source scan must still preserve the listener without inventing quorum.
			writer.WriteHeader(http.StatusNotFound)
			return
		case strings.HasSuffix(incoming.URL.Path, "/route-sources"):
			value = model.PlatformConsumerDNSRouteSourcesResponse{Assignment: assignment, Release: release, Snapshot: snapshot}
		case strings.HasSuffix(incoming.URL.Path, "/trusted-heartbeat"):
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
	directory := t.TempDir()
	service := NewService(config.DNSConfig{APIURL: server.URL, DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: compiled.DNSArtifact.ScopeKey, Zone: "example.test", CachePath: filepath.Join(directory, "cache"), BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, nil)
	service.PlatformTokenFile = filepath.Join(directory, "token")
	if err := os.WriteFile(service.PlatformTokenFile, []byte("scoped"), 0600); err != nil {
		t.Fatal(err)
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
			return routeprobe.Proof{EdgeID: requirement.EdgeID, GroupID: requirement.EdgeGroupID, Digest: requirement.RouteDigest, Version: "actual", State: state, TrafficRelease: plans.Bindings[requirement.CellPublicationDigest], CheckedAt: time.Now().Add(-time.Millisecond), ValidUntil: time.Now().Add(20 * time.Second)}, nil
		}
		return routeprobe.Proof{}, errors.New("missing probe")
	}
	if err := service.syncPlatformDNSServingOnce(context.Background(), probe, func(*dnsServingState) error { return nil }); err != nil {
		t.Fatal(err)
	}
	previous := service.platformServing.Load()
	if previous == nil || !previous.record.Positive {
		t.Fatal("initial source context not admitted")
	}
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
