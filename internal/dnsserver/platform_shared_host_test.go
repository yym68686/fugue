package dnsserver

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

func TestSharedHostnamePublishesDNSOnlyWithEveryPathProof(t *testing.T) {
	const host, group, edge, address = "app.example.test", "edge-group-a", "edge-a", "8.8.8.8"
	parent, candidate := dnsServingFixtureWithIntent(t, group, "global", func(r *platformconfig.CompileRequest) {
		root := &r.Intent.Routes[0]
		api := *root
		root.AppID, root.PathPrefix = "web", "/"
		api.PathPrefix, api.UpstreamURL = "/api", "http://api:8080"
		r.Intent.Routes = append(r.Intent.Routes, api)
		record := &r.Intent.DNS[0]
		options := *record.Application
		record.Type, record.Values, record.Application = "FUGUE_ROUTE", nil, nil
		record.Route = &platformconfig.DNSRouteIntent{
			DNSApplicationIntent: options,
			Hostnames:            []string{host},
			Bindings:             []platformconfig.DNSRouteBinding{{Hostname: host, PathPrefix: "/", AppID: "web"}, {Hostname: host, PathPrefix: "/api", AppID: "app"}},
		}
	}, true)
	s := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: group, Zone: "example.test",
		BundleSigningKey: "synthetic-dns-serving-secret", BundleSigningKeyID: "key"}, nil)
	payload, routeID, err := s.verifyDNSServingRelease(parent, candidate)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{}
	for _, p := range payload.Plan.Probes {
		if p.Hostname == host && p.Address == address {
			expected[p.Path] = p.RouteDigest
		}
	}
	if len(expected) != 2 || expected["/"] == "" || expected["/api"] == "" || expected["/"] == expected["/api"] {
		t.Fatalf("expected independent root and API proof requirements, got %v", expected)
	}
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcp.Close()
	udpServer, tcpServer := &dns.Server{PacketConn: udp, Handler: s}, &dns.Server{Listener: tcp, Handler: s}
	go udpServer.ActivateAndServe()
	go tcpServer.ActivateAndServe()
	defer udpServer.Shutdown()
	defer tcpServer.Shutdown()

	for _, missing := range []string{"/api", ""} {
		now := time.Now().UTC()
		checkpoint := dnsServingCheckpoint{AppliedAt: now, Parent: parent, Candidate: candidate, Positive: true}
		facts := collectDNSReadinessFacts(context.Background(), payload.Plan, payload.Policy.DNSReadiness,
			func(_ context.Context, hostname, path, ip, state string, _ time.Duration) (routeprobe.Proof, error) {
				if hostname != host || ip != address || path == missing {
					return routeprobe.Proof{}, errors.New("proof unavailable")
				}
				return routeprobe.Proof{Digest: expected[path], State: state, Version: "observed", EdgeID: edge, GroupID: group, CheckedAt: now, ValidUntil: now.Add(time.Minute),
					TrafficRelease: &model.TrafficReleaseBinding{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, RouteArtifactID: routeID, PolicyDigest: payload.Lineage.PolicyDigest, IntentDigest: payload.Lineage.IntentDigest, InputSnapshotDigest: payload.Lineage.InputSnapshotDigest, ReleaseID: candidate.Release.ID, ReleaseChannel: candidate.Release.ReleaseChannel, FencingToken: candidate.Release.FencingToken, ScopeKey: "global"}}, nil
			})
		for _, fact := range facts {
			if fact.Ready && !dnsFactMatchesRelease(fact, parent, candidate, routeID, payload) {
				t.Fatal("probe was not bound to the exact publication")
			}
		}
		state, err := buildDNSServingState(checkpoint, payload, routeID, "dns-a", group, facts, now)
		if err != nil || dnsServingReady(state, now) != (missing == "") {
			t.Fatal("unexpected readiness", missing, err)
		}
		s.platformServing.Store(state)
		s.platformServingBound.Store(true)
		for _, transport := range []struct{ network, target string }{{"udp", udp.LocalAddr().String()}, {"tcp", tcp.Addr().String()}} {
			q := new(dns.Msg).SetQuestion(dns.Fqdn(host), dns.TypeA)
			answer, _, err := (&dns.Client{Net: transport.network, Timeout: time.Second}).Exchange(q, transport.target)
			if err != nil {
				t.Fatal(transport.network, err)
			}
			if missing != "" {
				if answer.Rcode != dns.RcodeServerFailure || len(answer.Answer) != 0 {
					t.Fatal("missing API proof became an answer", transport.network, answer)
				}
				continue
			}
			if !answer.Authoritative || answer.Rcode != dns.RcodeSuccess || len(answer.Answer) != 1 {
				t.Fatal("no verified authoritative answer", transport.network, answer)
			}
			a, ok := answer.Answer[0].(*dns.A)
			if !ok || a.A.String() != address || a.Hdr.Ttl > 60 || a.Hdr.Ttl == 0 {
				t.Fatal("address or TTL policy changed", transport.network, answer)
			}
		}
	}
}
