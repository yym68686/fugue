package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

func TestAdminEdgeCacheCheckCommandReportsSecondHit(t *testing.T) {
	t.Parallel()

	var count atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/_next/static/chunks/app.js" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		seen := count.Add(1)
		if seen == 1 {
			w.Header().Set("X-Fugue-Cache", "miss")
		} else {
			w.Header().Set("X-Fugue-Cache", "hit")
		}
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("app.js"))
	}))
	defer server.Close()
	parsedServerURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	targetURL := "http://asset.example.com:" + parsedServerURL.Port() + "/_next/static/chunks/app.js"

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err = runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--json",
		"admin", "edge", "cache-check",
		targetURL,
		"--edge-ip", "127.0.0.1",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run cache-check: %v", err)
	}

	var report edgeCacheCheckReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if !report.Pass {
		t.Fatalf("expected cache check to pass, got %+v", report)
	}
	if len(report.Attempts) != 2 {
		t.Fatalf("expected 2 attempts, got %+v", report.Attempts)
	}
	if got := strings.ToLower(strings.TrimSpace(report.Attempts[0].CacheStatus)); got != "miss" {
		t.Fatalf("expected first attempt miss, got %+v", report.Attempts[0])
	}
	if got := strings.ToLower(strings.TrimSpace(report.Attempts[1].CacheStatus)); got != "hit" {
		t.Fatalf("expected second attempt hit, got %+v", report.Attempts[1])
	}
	if count.Load() != 2 {
		t.Fatalf("expected two upstream requests, got %d", count.Load())
	}
}
