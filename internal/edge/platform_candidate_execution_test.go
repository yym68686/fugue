package edge

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func isolatedTestBundle() model.EdgeRouteBundle {
	return model.EdgeRouteBundle{Version: "candidate-test", EdgeGroupID: "edge-group-test", Routes: []model.EdgeRouteBinding{
		{Hostname: "app.example.test", PathPrefix: "/", EdgeGroupID: "edge-group-test", RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusActive, UpstreamURL: "http://127.0.0.1:1"},
		{Hostname: "app.example.test", PathPrefix: "/api", AppID: "child", EdgeGroupID: "edge-group-test", RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusActive, UpstreamURL: "http://127.0.0.1:1"},
		{Hostname: "app.example.test", PathPrefix: "/offline", EdgeGroupID: "edge-group-test", RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusUnavailable},
		{Hostname: "disabled.example.test", PathPrefix: "/", EdgeGroupID: "edge-group-test", RoutePolicy: model.EdgeRoutePolicyRouteAOnly, Status: model.EdgeRouteStatusDisabled},
	}}
}

func TestCandidateHTTPConfigIsIsolated(t *testing.T) {
	raw, err := candidateHTTPConfig(isolatedTestBundle(), "127.0.0.1:12345", "127.0.0.1:23456")
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	admin := document["admin"].(map[string]any)
	if admin["disabled"] != true || admin["config"].(map[string]any)["persist"] != false {
		t.Fatal("candidate admin or config persistence enabled")
	}
	apps := document["apps"].(map[string]any)
	if apps["tls"] != nil {
		t.Fatal("candidate attempted TLS or ACME")
	}
	server := apps["http"].(map[string]any)["servers"].(map[string]any)["fugue_edge"].(map[string]any)
	if server["listen"].([]any)[0] != "127.0.0.1:12345" || server["automatic_https"].(map[string]any)["disable"] != true {
		t.Fatal("candidate listener is not isolated HTTP")
	}
	if strings.Contains(string(raw), "127.0.0.1:1\"") || strings.Contains(string(raw), "disabled.example.test") {
		t.Fatal("origin or disabled hostname entered Caddy config")
	}
}

func TestCandidateHandlerHasNoServingProofOrOriginPath(t *testing.T) {
	index := buildEdgeRouteIndex(isolatedTestBundle(), "edge-group-test", routePublicationMetadata{Candidate: true})
	handler := candidateProofHandler(index, "fresh-nonce")
	for _, tc := range []struct {
		method, path, nonce string
		status              int
	}{
		{"HEAD", "/api", "fresh-nonce", 204}, {"HEAD", "/offline", "fresh-nonce", 503},
		{"GET", "/api", "fresh-nonce", 403}, {"HEAD", "/api", "wrong", 403},
	} {
		req := httptest.NewRequest(tc.method, "http://app.example.test"+tc.path, nil)
		req.Header.Set(candidateExecutionHeader, tc.nonce)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != tc.status || w.Header().Get("X-Fugue-Route-Proof") != "" {
			t.Fatalf("unexpected candidate response: %d %+v", w.Code, w.Header())
		}
	}
}

// Run with the official Caddy version used by Dockerfile.edge. CI's unit
// suite exercises the failure paths even when the optional binary is absent.
func TestRealCaddyIsolatedCandidateApplyAndProbe(t *testing.T) {
	binary := os.Getenv("FUGUE_TEST_CADDY_BINARY")
	if binary == "" {
		t.Skip("set FUGUE_TEST_CADDY_BINARY for real Caddy integration")
	}
	dir := t.TempDir()
	digest, probes, err := runIsolatedCandidate(context.Background(), isolatedTestBundle(), dir, binary)
	if err != nil || !strings.HasPrefix(digest, "sha256:") || probes != 3 {
		t.Fatalf("real candidate apply failed: digest=%s probes=%d err=%v", digest, probes, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("candidate workspace leaked: %v %v", entries, err)
	}
}

func TestCandidateExecutionFailuresAreBoundedAndCleaned(t *testing.T) {
	for _, tc := range []struct {
		name, script string
		cancel       bool
	}{
		{"missing", "", false}, {"early exit", "#!/bin/sh\nexit 1\n", false},
		{"cancel", "#!/bin/sh\nexec sleep 60\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			binary := filepath.Join(t.TempDir(), "caddy-test")
			if tc.script != "" {
				if err := os.WriteFile(binary, []byte(tc.script), 0700); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			cancel := func() {}
			if tc.cancel {
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
			}
			defer cancel()
			start := time.Now()
			digest, count, err := runIsolatedCandidate(ctx, isolatedTestBundle(), dir, binary)
			if err == nil || digest != "" || count != 0 || time.Since(start) > 3*time.Second {
				t.Fatalf("invalid execution receipt: %s %d %v", digest, count, err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 {
				t.Fatal("candidate leaked workspace", entries, err)
			}
		})
	}
}

func TestCandidateExecutionReceiptRejectsStaleOrDifferentExecution(t *testing.T) {
	now := time.Now().UTC()
	artifact := model.PlatformArtifact{ID: "candidate", ContentHash: "sha256:artifact", Generation: "route-generation"}
	assignment := model.PlatformConsumerAssignment{ReleaseSetID: "release-set", ExpectedConsumerSetID: "consumers", GenerationSequence: 7, FencingToken: 11}
	receipt := PlatformCandidateExecution{
		Schema: candidateExecutionSchema, Mode: "isolated_http", Result: "passed",
		ArtifactID: artifact.ID, ArtifactDigest: artifact.ContentHash, ArtifactGeneration: artifact.Generation,
		NodeID: "node", EdgeGroupID: "group", ReleaseSetID: assignment.ReleaseSetID,
		ExpectedConsumerSetID: assignment.ExpectedConsumerSetID, GenerationSequence: 7, FencingToken: 11,
		RouteIndexDigest: "sha256:index", CaddyConfigDigest: "sha256:config", ProbeCount: 3,
		ObservedAt: now, ExpiresAt: now.Add(2 * time.Minute),
	}
	receipt.ReceiptDigest, _ = receipt.digest()
	if !receipt.matches(artifact, assignment, "node", "group", "sha256:index", now) {
		t.Fatal("fresh bound receipt rejected")
	}
	for name, mutate := range map[string]func(*PlatformCandidateExecution){
		"node":      func(r *PlatformCandidateExecution) { r.NodeID = "other" },
		"group":     func(r *PlatformCandidateExecution) { r.EdgeGroupID = "other" },
		"artifact":  func(r *PlatformCandidateExecution) { r.ArtifactDigest = "other" },
		"fence":     func(r *PlatformCandidateExecution) { r.FencingToken++ },
		"index":     func(r *PlatformCandidateExecution) { r.RouteIndexDigest = "other" },
		"config":    func(r *PlatformCandidateExecution) { r.CaddyConfigDigest = "" },
		"expired":   func(r *PlatformCandidateExecution) { r.ExpiresAt = now },
		"future":    func(r *PlatformCandidateExecution) { r.ObservedAt = now.Add(time.Hour) },
		"unbounded": func(r *PlatformCandidateExecution) { r.ExpiresAt = now.Add(time.Hour) },
		"no probes": func(r *PlatformCandidateExecution) { r.ProbeCount = 0 },
		"serving":   func(r *PlatformCandidateExecution) { r.Serving = true },
		"TLS":       func(r *PlatformCandidateExecution) { r.TLSVerified = true },
		"origin":    func(r *PlatformCandidateExecution) { r.OriginVerified = true },
	} {
		t.Run(name, func(t *testing.T) {
			changed := receipt
			mutate(&changed)
			changed.ReceiptDigest, _ = changed.digest()
			if changed.matches(artifact, assignment, "node", "group", "sha256:index", now) {
				t.Fatal("different or invalid execution reused")
			}
		})
	}
	tampered := receipt
	tampered.ProbeCount++
	if tampered.matches(artifact, assignment, "node", "group", "sha256:index", now) {
		t.Fatal("modified receipt with old digest reused")
	}
}
