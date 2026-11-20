package edge

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/edge/relayroute"
)

func TestRelayWithdrawalPersistsWithoutEndpointAcrossRestart(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	dir := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	state, policy := filepath.Join(dir, "state.json"), filepath.Join(dir, "policy.json")
	ep := relayroute.Endpoint{Edge: "edge-a", Slot: "a", Address: "10.42.1.2:8443", TLSName: "a.example.test"}
	mount := func(generation uint64, mode string, endpoint bool) {
		t.Helper()
		p := relayroute.Policy{Schema: relayroute.Schema, Generation: generation, Mode: mode,
			NotBefore: now.Add(-time.Minute), Expires: now.Add(time.Minute), MaxConcurrent: 1}
		if mode == "active" {
			p.RelayPercent = 1
		}
		if endpoint {
			p.Endpoints = []relayroute.Endpoint{ep}
		}
		payload, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(relayroute.SignedPolicy{Payload: payload, Signature: ed25519.Sign(priv, payload)})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(policy, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	_, cert := relayFixtureCertificates(t, now)
	leaf := cert(ep.TLSName, 7)
	start := func() *edgeOriginRelay {
		t.Helper()
		f, err := openRelayPolicyFence(state, ep.Edge, ep.Slot, pub)
		if err != nil {
			t.Fatal(err)
		}
		return &edgeOriginRelay{fence: f, key: pub, policyFile: policy, local: ep, certificate: leaf,
			s: &Service{Config: config.EdgeConfig{EdgeID: ep.Edge, EdgeSlot: ep.Slot}}}
	}
	e := start()
	mount(2, "active", true)
	e.refresh(now)
	if e.current(now) != nil {
		t.Fatal("active policy bootstrapped an uninitialized fence")
	}
	mount(1, "off", true)
	e.refresh(now)
	mount(2, "active", true)
	e.refresh(now)
	if e.current(now) == nil {
		t.Fatal("active policy did not follow the durable off bootstrap")
	}
	mount(3, "off", false)
	e.refresh(now)
	if e.current(now) != nil || e.fence.last.Generation != 3 {
		t.Fatal("off without endpoints did not withdraw and advance the fence")
	}
	mount(2, "active", true)
	for _, worker := range []*edgeOriginRelay{e, start()} {
		worker.refresh(now)
		if worker.current(now) != nil {
			t.Fatal("a withdrawn active lease was restored")
		}
	}
	mount(3, "active", true)
	e = start()
	e.refresh(now)
	if e.current(now) != nil {
		t.Fatal("conflicting lease with the off generation was restored")
	}
	if _, err := openRelayPolicyFence(state, ep.Edge, "b", pub); err == nil {
		t.Fatal("accepted a fence from another slot")
	}
}

func TestRelayFenceFailsClosedWhenPersistenceFails(t *testing.T) {
	key := make(ed25519.PublicKey, ed25519.PublicKeySize)
	dir := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "state.json")
	f, err := openRelayPolicyFence(path, "edge-a", "a", key)
	if err != nil {
		t.Fatal(err)
	}
	before := &relayroute.Policy{Mode: "off", Generation: 1}
	if err := f.accept(before); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	off := &relayroute.Policy{Mode: "off", Generation: 2}
	if err := f.accept(off); err == nil || f.durable {
		t.Fatal("acknowledged an unpersisted fence")
	}
	if err := f.accept(before); err == nil {
		t.Fatal("old policy was accepted after failed persistence")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := f.accept(off); err != nil || !f.durable {
		t.Fatalf("same policy did not persist on retry: %v", err)
	}
}
