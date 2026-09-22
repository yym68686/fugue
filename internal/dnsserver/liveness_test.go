package dnsserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/config"
)

func TestLivenessPreservesAnEnrolledExecutorAwaitingRecovery(t *testing.T) {
	s := NewService(config.DNSConfig{Zone: "example.test"}, nil)
	s.PlatformTokenFile = "/identity/token"
	handler := s.Handler()
	check := func(path string, want int) {
		t.Helper()
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != want {
			t.Fatalf("%s: got %d want %d", path, w.Code, want)
		}
	}
	check("/healthz", http.StatusServiceUnavailable)
	check("/livez", http.StatusOK)
	// A stalled configuration/readiness lock cannot make liveness block.
	s.mu.Lock()
	done := make(chan int, 1)
	go func() {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/livez", nil))
		done <- w.Code
	}()
	select {
	case code := <-done:
		s.mu.Unlock()
		if code != http.StatusOK {
			t.Fatal(code)
		}
	case <-time.After(time.Second):
		s.mu.Unlock()
		t.Fatal("liveness waited for serving state")
	}
	s.listenerFailed.Store(true)
	check("/livez", http.StatusServiceUnavailable)
	check("/healthz", http.StatusServiceUnavailable)
	// A failed execution listener does not change the signed serving state.
	if s.platformServing.Load() != nil {
		t.Fatal("liveness manufactured serving state")
	}
}
