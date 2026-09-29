package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	o "fugue/internal/staticedgeobserve"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"golang.org/x/net/http2"
)

// Cleanup runs when a replaced Caddy configuration releases its modules, even
// though an unlimited-grace request can still be serving in that generation.
func TestObservationFinishesAfterConfigurationCleanup(t *testing.T) {
	s, socket := collector(t)
	h := observation(t, socket, true)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	w := httptest.NewRecorder()
	go func() {
		done <- h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/stream", nil), caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
			_, err := io.WriteString(w, "start\n")
			close(entered)
			<-release
			if err != nil {
				return err
			}
			_, err = io.WriteString(w, "done\n")
			return err
		}))
	}()
	<-entered
	workers := h.workers
	if err := h.Cleanup(); err != nil {
		t.Fatal(err)
	}
	// Give a canceled worker a chance to exit before the request completes.
	time.Sleep(30 * time.Millisecond)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if w.Body.String() != "start\ndone\n" {
		t.Fatal("serving stream changed")
	}
	if got := records(t, s, w.Header().Get("X-Fugue-Observation-ID")); len(got) != 1 || !got[0].Finished {
		t.Fatal("terminal observation lost during cleanup")
	}
	select {
	case <-workers.done:
	case <-time.After(3 * time.Second):
		t.Fatal("retired observation workers did not finish")
	}
}

func TestObservationAcceptsHandlerScheduledAfterCleanup(t *testing.T) {
	s, socket := collector(t)
	h := observation(t, socket, true)
	old := h.workers
	h.Cleanup()
	<-old.done
	w := httptest.NewRecorder()
	err := h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil), caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error { w.WriteHeader(204); return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if got := records(t, s, w.Header().Get("X-Fugue-Observation-ID")); len(got) != 1 || !got[0].Finished {
		t.Fatal("late scheduled handler lost terminal")
	}
}

func TestActualCaddyReloadPreservesLongRequestObservations(t *testing.T) {
	store, socket := collector(t)
	h := observation(t, socket, true)
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-ID", "fixture-long-request")
		io.WriteString(w, "start\n")
		http.NewResponseController(w).Flush()
		select {
		case <-release:
			io.WriteString(w, "done\n")
		case <-r.Context().Done():
		}
	}))
	defer backend.Close()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reserved.Addr().String()
	reserved.Close()
	config := map[string]any{"admin": map[string]any{"disabled": true, "config": map[string]any{"persist": false}}, "storage": map[string]any{"module": "file_system", "root": t.TempDir()}, "apps": map[string]any{"tls": map[string]any{"disable_storage_clean": true}, "http": map[string]any{"grace_period": 0, "servers": map[string]any{"fixture": map[string]any{
		"listen": []string{address}, "protocols": []string{"h1", "h2c"}, "fugue_observations": true, "automatic_https": map[string]any{"disable": true}, "routes": []any{map[string]any{"handle": []any{map[string]any{"handler": "fugue_observation", "node_id": h.NodeID, "hop": "entry", "build": "fixture", "config_digest": "generation-one", "socket": socket, "correlation_key_file": h.CorrelationKeyFile, "entry": true}, map[string]any{"handler": "reverse_proxy", "upstreams": []any{map[string]any{"dial": strings.TrimPrefix(backend.URL, "http://")}}, "transport": map[string]any{"protocol": "fugue_observed_http", "versions": []string{"1.1"}}, "flush_interval": -1}}}},
	}}}}}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err = caddy.Load(raw, true); err != nil {
		t.Fatal(err)
	}
	defer caddy.Stop()
	tr := &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 10 * time.Second}
	resp, err := client.Get("http://" + address + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	prefix := make([]byte, 6)
	if _, err = io.ReadFull(resp.Body, prefix); err != nil || string(prefix) != "start\n" {
		t.Fatal(err)
	}
	id := resp.Header.Get("X-Fugue-Observation-ID")
	// Force real configuration cancellation/Cleanup while the old H2 stream
	// and its forward attempt are still active.
	raw = []byte(strings.ReplaceAll(string(raw), "generation-one", "generation-two"))
	if err = caddy.Load(raw, true); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2500 * time.Millisecond)
	live := false
	for time.Now().Before(deadline) {
		out, e := store.Query(o.Query{RequestID: id, Since: time.Now().Add(-time.Minute), Until: time.Now(), Limit: 10})
		if e != nil {
			t.Fatal(e)
		}
		for _, r := range out.Records {
			if !r.Finished && r.ElapsedMS >= 900 && r.ConfigDigest == "generation-one" {
				live = true
			}
		}
		if live {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !live {
		t.Fatal("old generation stopped live observations during reload")
	}
	close(release)
	end, err := io.ReadAll(resp.Body)
	if err != nil || string(end) != "done\n" {
		t.Fatal("long stream changed", err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		out, e := store.Query(o.Query{RequestID: id, Since: time.Now().Add(-time.Minute), Until: time.Now(), Limit: 10})
		if e != nil {
			t.Fatal(e)
		}
		finished := 0
		for _, r := range out.Records {
			if r.Finished && r.Status == 200 && r.ConfigDigest == "generation-one" && r.ApplicationRequestID == "fixture-long-request" {
				finished++
			}
		}
		if finished == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("old ingress/attempt terminal observations missing after actual reload")
}
