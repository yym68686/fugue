package edge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func poolTestBundle(hosts []string) model.EdgeRouteBundle {
	bundle := model.EdgeRouteBundle{EdgeGroupID: "edge-group-test"}
	for _, host := range hosts {
		bundle.Routes = append(bundle.Routes, model.EdgeRouteBinding{Hostname: host, EdgeGroupID: bundle.EdgeGroupID, RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusActive})
	}
	return bundle
}

func TestCaddyConnectionPoolCountDoesNotGrowWithHostCount(t *testing.T) {
	for _, count := range []int{0, 1, 164} {
		hosts := make([]string, count)
		for i := range hosts {
			hosts[i] = fmt.Sprintf("app-%d.example.test", i)
		}
		s := NewService(config.EdgeConfig{EdgeGroupID: "edge-group-test", CaddyListenAddr: "127.0.0.1:18080", CaddyProxyListenAddr: "127.0.0.1:7833"}, nil)
		raw, actual, err := s.buildCaddyConfig(poolTestBundle(hosts))
		want := 0
		if count > 0 {
			want = 1
		}
		if err != nil || actual != count || strings.Count(string(raw), `"handler":"reverse_proxy"`) != want {
			t.Fatalf("%d hosts: routes=%d err=%v", count, actual, err)
		}
	}
}

func TestRealCaddySharedPoolPreservesHostBoundary(t *testing.T) {
	binary := os.Getenv("FUGUE_TEST_CADDY_BINARY")
	if binary == "" {
		t.Skip("set FUGUE_TEST_CADDY_BINARY for real Caddy integration")
	}
	type observation struct{ Host, RouteHost, ForwardedFor, ForwardedHost, Path, Connection string }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(observation{r.Host, r.Header.Get("X-Fugue-Edge-Route-Host"), r.Header.Get("X-Forwarded-For"), r.Header.Get("X-Forwarded-Host"), r.URL.RequestURI(), r.RemoteAddr})
	}))
	defer upstream.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	s := NewService(config.EdgeConfig{EdgeGroupID: "edge-group-test", CaddyListenAddr: address, CaddyProxyListenAddr: upstream.Listener.Addr().String()}, nil)
	raw, _, err := s.buildCaddyConfig(poolTestBundle([]string{"alpha.example.test", "beta.example.test", "*.wild.example.test", "exact.wild.example.test"}))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	document["admin"] = map[string]any{"disabled": true, "config": map[string]any{"persist": false}}
	document["logging"] = map[string]any{"logs": map[string]any{
		"default":           map[string]any{"writer": map[string]string{"output": "discard"}},
		"fugue_edge_access": map[string]any{"writer": map[string]string{"output": "discard"}, "include": []string{"http.log.access.fugue_edge_access"}},
	}}
	raw, _ = json.Marshal(document)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "run", "--config", path)
	log, err := os.Create(filepath.Join(dir, "caddy.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() { cancel(); <-done }()
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := client.Get("http://" + address + "/")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Caddy listener unavailable", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	connections := map[string]bool{}
	for _, tc := range []struct{ host, selected string }{
		{"alpha.example.test", "alpha.example.test"},
		{"beta.example.test", "beta.example.test"},
		{"ALPHA.EXAMPLE.TEST:8080", "alpha.example.test"},
		{"leaf.wild.example.test", "*.wild.example.test"},
		// Existing sorted routes select the wildcard before the exact match.
		{"exact.wild.example.test", "*.wild.example.test"},
		{"unknown.example.test", ""},
	} {
		req, _ := http.NewRequest(http.MethodGet, "http://"+address+"/path/child?q=one", nil)
		req.Host = tc.host
		req.Header.Set("X-Fugue-Edge-Route-Host", "spoofed.example.test")
		req.Header.Set("X-Forwarded-For", "192.0.2.1")
		req.Header.Set("X-Forwarded-Host", "spoofed.example.test")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var got observation
		decodeErr := json.NewDecoder(resp.Body).Decode(&got)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if tc.selected == "" {
			if resp.StatusCode != http.StatusNotFound {
				t.Fatal("unknown hostname reached proxy")
			}
			continue
		}
		forwardedHost := tc.host
		if host, _, err := net.SplitHostPort(tc.host); err == nil {
			forwardedHost = host // Existing http.request.host placeholder omits the port.
		}
		if decodeErr != nil || resp.StatusCode != http.StatusOK || got.RouteHost != tc.selected || got.Host != tc.host || got.ForwardedHost != forwardedHost || got.ForwardedFor != "127.0.0.1" || got.Path != "/path/child?q=one" {
			t.Fatalf("host boundary changed for %s: %+v (%v)", tc.host, got, decodeErr)
		}
		connections[got.Connection] = true
	}
	if len(connections) != 1 {
		t.Fatalf("hostnames did not share the local Worker connection pool: %v", connections)
	}
	errors := make(chan error, 32)
	for i := 0; i < cap(errors); i++ {
		host := []string{"alpha.example.test", "beta.example.test"}[i%2]
		go func() {
			req, _ := http.NewRequest(http.MethodGet, "http://"+address+"/", nil)
			req.Host = host
			req.Header.Set("X-Fugue-Edge-Route-Host", "spoofed.example.test")
			resp, err := client.Do(req)
			if err != nil {
				errors <- err
				return
			}
			defer resp.Body.Close()
			var got observation
			if err = json.NewDecoder(resp.Body).Decode(&got); err == nil && (got.RouteHost != host || got.Host != host) {
				err = fmt.Errorf("concurrent hostname crossed request boundary: want=%s got=%+v", host, got)
			}
			errors <- err
		}()
	}
	for i := 0; i < cap(errors); i++ {
		if err := <-errors; err != nil {
			t.Error(err)
		}
	}
}
