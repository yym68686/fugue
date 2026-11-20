package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/edge/relayroute"
	"fugue/internal/model"
	"fugue/internal/routeproof"
)

func TestReadSigningKeyFileRejectsLoosePermissionsAndLinks(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "private.pem")
	if err := os.WriteFile(name, []byte("dummy"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readFile(name, 64, true); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "alias.pem")
	if err := os.Symlink(name, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := readFile(alias, 64, true); err == nil {
		t.Fatal("signing key reader accepted a symbolic link")
	}
	if err := os.Chmod(name, 0640); err != nil {
		t.Fatal(err)
	}
	if _, err := readFile(name, 64, true); err == nil {
		t.Fatal("signing key reader accepted group read access")
	}
}

func TestIssueBindsBothExactStableRoutes(t *testing.T) {
	now := time.Unix(1800000000, 0).UTC()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	key, err = parseSigningKey(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	if err != nil {
		t.Fatal(err)
	}
	source := model.EdgeRouteBinding{Hostname: "app.example.test", PathPrefix: "/v1", TenantID: "tenant-test", AppID: "app-test",
		EdgeGroupID: "west", RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusActive,
		UpstreamURL:  "http://service.ns.svc.cluster.local:8000",
		UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService}
	source.Upstreams = []model.EdgeRouteUpstream{{Role: "stable", ReleaseID: "release-test", Weight: 100,
		UpstreamURL: source.UpstreamURL, UpstreamKind: source.UpstreamKind, UpstreamScope: source.UpstreamScope}}
	peer := source
	peer.EdgeGroupID = "east"
	sourceDigest, _ := routeproof.Digest(source)
	peerDigest, _ := routeproof.Digest(peer)
	p := relayroute.Policy{Schema: relayroute.Schema, Mode: "shadow", Generation: 5, NotBefore: now.Add(-time.Minute), Expires: now.Add(time.Minute),
		MaxConcurrent: 1,
		Endpoints: []relayroute.Endpoint{
			{Edge: "edge-a", Slot: "a", Address: "10.0.0.1:8443", TLSName: "edge-a.example.test"},
			{Edge: "edge-b", Slot: "b", Address: "10.0.0.2:8443", TLSName: "edge-b.example.test"},
		},
		Bindings: []relayroute.Binding{{ID: "pilot", Source: "edge-a", Peer: "edge-b", Tenant: source.TenantID, App: source.AppID,
			Release: "release-test", Origin: source.UpstreamURL, MaxBodyBytes: 4096, Host: source.Hostname, Path: "/v1/stream",
			SourceDigest: sourceDigest, PeerDigest: peerDigest}}}
	tests := []struct {
		name            string
		edit            func(*relayroute.Policy, *model.EdgeRouteBinding)
		active, allowed bool
	}{
		{name: "signed shadow", allowed: true},
		{name: "wrong proof", edit: func(p *relayroute.Policy, _ *model.EdgeRouteBinding) { p.Bindings[0].PeerDigest = sourceDigest }},
		{name: "other tenant", edit: func(_ *relayroute.Policy, r *model.EdgeRouteBinding) { r.TenantID = "other-tenant" }},
		{name: "outside prefix", edit: func(p *relayroute.Policy, _ *model.EdgeRouteBinding) { p.Bindings[0].Path = "/v11/stream" }},
		{name: "expired", edit: func(p *relayroute.Policy, _ *model.EdgeRouteBinding) { p.Expires = now }},
		{name: "active unconfirmed", edit: func(p *relayroute.Policy, _ *model.EdgeRouteBinding) { p.Mode, p.RelayPercent = "active", 1 }},
		{name: "active confirmed", edit: func(p *relayroute.Policy, _ *model.EdgeRouteBinding) { p.Mode, p.RelayPercent = "active", 1 }, active: true, allowed: true},
		{name: "off without witnesses", edit: func(p *relayroute.Policy, r *model.EdgeRouteBinding) {
			p.Mode, p.Bindings, *r = "off", nil, model.EdgeRouteBinding{}
		}, allowed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, b := p, peer
			p.Bindings = append([]relayroute.Binding(nil), p.Bindings...)
			if tt.edit != nil {
				tt.edit(&p, &b)
			}
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			got, err := issue(raw, source, b, key, tt.active, now)
			if !tt.allowed {
				if err == nil || len(got) != 0 {
					t.Fatal("unauthorized policy was signed")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			verified, err := relayroute.Verify(got, pub, now)
			if err != nil || verified.Mode != p.Mode || verified.Generation != p.Generation {
				t.Fatalf("worker could not verify issued policy: %v", err)
			}
		})
	}
}
