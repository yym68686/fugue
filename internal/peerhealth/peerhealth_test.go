package peerhealth

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPeerSignalRequiresEvidenceHashExpiryAndSignature(t *testing.T) {
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	secret := []byte("node-identity-secret")
	signal := NewSignal("node-a", "edge-b", model.PeerSignalStatusSuspect, map[string]string{"probe": "tls_timeout"}, now, time.Minute)
	signal.FailureDomain = "provider:a/region:us"
	signed, err := Sign(signal, "node-a-key", secret)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if signed.EvidenceHash == "" || signed.ExpiresAt.IsZero() || signed.Signature == "" {
		t.Fatalf("expected hash, expiry, signature: %+v", signed)
	}
	if err := Verify(signed, secret, now.Add(10*time.Second)); err != nil {
		t.Fatalf("verify signed signal: %v", err)
	}
	signed.Evidence["probe"] = "ok"
	if err := Verify(signed, secret, now.Add(10*time.Second)); err == nil || !strings.Contains(err.Error(), "evidence hash") {
		t.Fatalf("expected evidence hash verification failure, got %v", err)
	}
}

func TestPeerDecisionRequiresMultipleFailureDomainsOrSelfQuarantine(t *testing.T) {
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	a := NewSignal("node-a", "edge-x", model.PeerSignalStatusSuspect, map[string]string{"probe": "tls_timeout"}, now, time.Minute)
	a.FailureDomain = "provider:a"
	one := Decide([]model.PeerHealthSignal{a}, now.Add(10*time.Second))
	if one.Decision != model.PeerHealthDecisionSuspect {
		t.Fatalf("single failure domain should only be suspect, got %+v", one)
	}
	b := NewSignal("node-b", "edge-x", model.PeerSignalStatusUnhealthy, map[string]string{"probe": "tcp_timeout"}, now, time.Minute)
	b.FailureDomain = "provider:b"
	multi := Decide([]model.PeerHealthSignal{a, b}, now.Add(10*time.Second))
	if multi.Decision != model.PeerHealthDecisionTemporaryFilter || multi.Reason != "multi_failure_domain_peer_failure" {
		t.Fatalf("expected multi-domain temporary filter, got %+v", multi)
	}
	stale := Decide([]model.PeerHealthSignal{a, b}, now.Add(2*time.Minute))
	if stale.Decision != model.PeerHealthDecisionClear || stale.SignalCount != 0 {
		t.Fatalf("expired signals should be ignored, got %+v", stale)
	}
	self := NewSignal("node-x", "edge-x", model.PeerSignalStatusSelfQuarantine, map[string]string{"reason": "local_guardian"}, now, time.Minute)
	selfDecision := Decide([]model.PeerHealthSignal{self}, now.Add(10*time.Second))
	if selfDecision.Decision != model.PeerHealthDecisionTemporaryFilter || selfDecision.Reason != "subject_self_quarantine" {
		t.Fatalf("expected self quarantine filter, got %+v", selfDecision)
	}
}
