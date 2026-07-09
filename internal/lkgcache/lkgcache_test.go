package lkgcache

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEnvelopeWriteReadValidatesHashAndExpiry(t *testing.T) {
	now := time.Date(2026, 7, 9, 10, 0, 0, 0, time.UTC)
	payload := []byte(`{"routes":[{"hostname":"api.example.com"}]}`)
	envelope, err := NewEnvelope("edge_route_bundle", "routegen_1", payload, now.Add(time.Hour), now)
	if err != nil {
		t.Fatalf("new envelope: %v", err)
	}
	path := filepath.Join(t.TempDir(), "edge-route-lkg.json")
	if err := WriteEnvelope(path, envelope, 2); err != nil {
		t.Fatalf("write envelope: %v", err)
	}
	decoded, gotPayload, err := ReadEnvelope(path, ReadEnvelopeOptions{Now: now.Add(time.Minute), ExpectedKind: "edge_route_bundle"})
	if err != nil {
		t.Fatalf("read envelope: %v", err)
	}
	if decoded.Generation != "routegen_1" || payloadHash(gotPayload) != payloadHash(payload) {
		t.Fatalf("unexpected envelope: generation=%q payload=%s", decoded.Generation, gotPayload)
	}
}

func TestEnvelopeRejectsCorruptionAndExpiry(t *testing.T) {
	now := time.Date(2026, 7, 9, 10, 0, 0, 0, time.UTC)
	envelope, err := NewEnvelope("dns_answer_bundle", "dnsgen_1", []byte(`{"records":[]}`), now.Add(time.Minute), now)
	if err != nil {
		t.Fatalf("new envelope: %v", err)
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	corrupt := strings.Replace(string(body), `"records":[]`, `"records":[1]`, 1)
	if _, err := DecodeEnvelope([]byte(corrupt), ReadEnvelopeOptions{Now: now}); err == nil || !strings.Contains(err.Error(), "content hash mismatch") {
		t.Fatalf("expected content hash mismatch, got %v", err)
	}
	if _, err := DecodeEnvelope(body, ReadEnvelopeOptions{Now: now.Add(2 * time.Minute)}); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expired envelope, got %v", err)
	}
}

func TestEnvelopeSignatureHook(t *testing.T) {
	now := time.Date(2026, 7, 9, 10, 0, 0, 0, time.UTC)
	envelope, err := NewEnvelope("caddy_route_config", "caddygen_1", []byte(`{"apps":{}}`), now.Add(time.Hour), now)
	if err != nil {
		t.Fatalf("new envelope: %v", err)
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if _, err := DecodeEnvelope(body, ReadEnvelopeOptions{Now: now, RequireSignature: true}); err == nil || !strings.Contains(err.Error(), "signature is required") {
		t.Fatalf("expected signature requirement error, got %v", err)
	}
	called := false
	if _, err := DecodeEnvelope(body, ReadEnvelopeOptions{Now: now, VerifySignature: func(Envelope) error {
		called = true
		return nil
	}}); err != nil {
		t.Fatalf("signature hook should allow envelope: %v", err)
	}
	if !called {
		t.Fatal("expected signature hook to be called")
	}
}
