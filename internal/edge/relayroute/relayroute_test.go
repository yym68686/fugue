package relayroute

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestOriginWindowAndSelector(t *testing.T) {
	now := time.Unix(1000, 0)
	var w Window
	w.Add(Sample{At: now, Round: 1, Failed: true}, now)
	w.Add(Sample{At: now, Round: 1, Failed: true}, now)
	if got := w.Health(now); got.State != Suspect || got.Samples != 1 {
		t.Fatalf("duplicate round voted: %+v", got)
	}
	w.Add(Sample{At: now.Add(time.Second), Round: 3, Failed: true}, now)
	if w.Health(now).State != Suspect {
		t.Fatal("future sample changed health")
	}
	now = now.Add(time.Second)
	w.Add(Sample{At: now, Round: 2, Failed: true}, now)
	local := w.Health(now)
	if local.State != Degraded {
		t.Fatalf("two bad rounds: %+v", local)
	}
	healthy := Health{State: Healthy, At: now, Samples: 2, RTT: time.Millisecond}
	unproven := Candidate{ID: "unproven", Origin: healthy, Capacity: 1}
	if got := Select(now, local, []Candidate{unproven}); got.Peer != "" {
		t.Fatalf("unproven A-to-B selected: %+v", got)
	}
	good := Candidate{ID: "good", Link: healthy, Origin: healthy, Capacity: 1}
	if got := Select(now, local, []Candidate{unproven, good}); got.Peer != "good" {
		t.Fatalf("healthy backup excluded: %+v", got)
	}
	if got := Select(now.Add(EvidenceTTL), local, []Candidate{good}); got.Peer != "" {
		t.Fatalf("stale evidence selected: %+v", got)
	}
	for i := 3; i <= 5; i++ {
		now = now.Add(time.Second)
		w.Add(Sample{At: now, Round: uint64(i)}, now)
	}
	if got := w.Health(now); got.State != Recovering {
		t.Fatalf("flapped back to healthy: %+v", got)
	}
	now = now.Add(RecoveryHold)
	for i := 6; i <= 8; i++ {
		now = now.Add(time.Second)
		w.Add(Sample{At: now, Round: uint64(i)}, now)
	}
	if got := w.Health(now); got.State != Healthy {
		t.Fatalf("failed to recover: %+v", got)
	}
}

func TestRelaySignedPolicy(t *testing.T) {
	now := time.Unix(1500, 0)
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("0", 64)
	good := Policy{Schema: Schema, Generation: 1, NotBefore: now.Add(-time.Minute), Expires: now.Add(time.Minute),
		Mode: "active", RelayPercent: 1, MaxConcurrent: 2,
		Endpoints: []Endpoint{{"edge-a", "a", "10.0.0.1:8001", "worker-a.example.test"}, {"edge-b", "b", "10.0.0.2:8001", "worker-b.example.test"}},
		Bindings: []Binding{{ID: "test-binding", Source: "edge-a", Peer: "edge-b", Path: "/v1/stream", Host: "app.example.test",
			Tenant: "tenant-test", App: "app-test", Release: "release-test", Origin: "http://service.ns.svc.cluster.local:80",
			SourceDigest: digest, PeerDigest: digest, MaxBodyBytes: 500 << 10}}}
	sign := func(p Policy) []byte {
		raw, _ := json.Marshal(p)
		envelope, _ := json.Marshal(SignedPolicy{raw, ed25519.Sign(key, raw)})
		return envelope
	}
	if verified, err := Verify(sign(good), pub, now); err != nil || verified.Hash == ([32]byte{}) {
		t.Fatalf("valid policy: %v", err)
	}
	raw := sign(good)
	var envelope SignedPolicy
	_ = json.Unmarshal(raw, &envelope)
	envelope.Payload[0] = '['
	raw, _ = json.Marshal(envelope)
	if _, err := Verify(raw, pub, now); err == nil {
		t.Fatal("tampered payload accepted")
	}
	for _, change := range []func(*Policy){
		func(p *Policy) { p.RelayPercent = 26 },
		func(p *Policy) { p.Expires = now },
		func(p *Policy) { p.Mode = "shadow" },
		func(p *Policy) { p.Endpoints[0].Address = "192.0.2.1:8443" },
		func(p *Policy) { p.Endpoints[0].TLSName = "*.example.test" },
		func(p *Policy) { p.Bindings[0].Peer = "edge-a" },
		func(p *Policy) { p.Bindings[0].Path = "/v1/%61" },
	} {
		bad := good
		bad.Endpoints = append([]Endpoint(nil), good.Endpoints...)
		bad.Bindings = append([]Binding(nil), good.Bindings...)
		change(&bad)
		if _, err := Verify(sign(bad), pub, now); err == nil {
			t.Fatalf("invalid policy accepted: %+v", bad)
		}
	}
}
