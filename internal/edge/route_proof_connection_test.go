package edge

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
)

func TestCaddyRouteProofRejectionDrainIsRequestScoped(t *testing.T) {
	service := NewService(config.EdgeConfig{EdgeGroupID: "group-a", CaddyListenAddr: "127.0.0.1:18443", CaddyProxyListenAddr: "127.0.0.1:7833"}, nil)
	bundle := model.EdgeRouteBundle{Routes: []model.EdgeRouteBinding{{Hostname: "app.example.test", EdgeGroupID: "group-a", RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusActive}}}
	raw, _, err := service.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	routes := document["apps"].(map[string]any)["http"].(map[string]any)["servers"].(map[string]any)["fugue_edge"].(map[string]any)["routes"].([]any)
	proxies := 0
	for _, route := range routes {
		for _, rawHandler := range route.(map[string]any)["handle"].([]any) {
			handler := rawHandler.(map[string]any)
			if handler["handler"] != "reverse_proxy" {
				continue
			}
			proxies++
			responses := handler["handle_response"].([]any)
			response := responses[0].(map[string]any)
			if len(responses) != 1 || !reflect.DeepEqual(response["match"], map[string]any{"status_code": []any{float64(400), float64(404), float64(503)}}) {
				t.Fatal("drain affects successful proofs or unrelated statuses", response)
			}
			rejectionRoutes := response["routes"].([]any)
			drain, fallback := rejectionRoutes[0].(map[string]any), rejectionRoutes[1].(map[string]any)
			if len(rejectionRoutes) != 2 || !reflect.DeepEqual(drain["match"], []any{map[string]any{"header": map[string]any{routeproof.RequestHeader: []any{"1"}}}}) ||
				!reflect.DeepEqual(drain["handle"], []any{map[string]any{"handler": "static_response", "status_code": "{http.reverse_proxy.status_code}", "headers": map[string]any{"Cache-Control": []any{"no-store"}}, "close": true}}) ||
				!reflect.DeepEqual(fallback["handle"], []any{map[string]any{"handler": "copy_response"}}) || len(fallback) != 1 {
				t.Fatal("drain is not isolated from ordinary application responses", rejectionRoutes)
			}
		}
	}
	if proxies != 1 {
		t.Fatal("proof drain changed the shared proxy pool count", proxies)
	}
}

// Model a legacy observer which reuses its transport indefinitely and a Front
// which chooses a Worker only on TCP connect. A rejected probe must let that
// observer reconnect, without turning a candidate into a serving proof.
func TestRejectedRouteProofReleasesBackendAffinity(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		name := "http1"
		if h2 {
			name = "http2"
		}
		t.Run(name, func(t *testing.T) {
			bundle := model.EdgeRouteBundle{Version: "previous", ValidUntil: time.Now().Add(time.Minute), Routes: []model.EdgeRouteBinding{{Hostname: "app.example.test", PathPrefix: "/", EdgeGroupID: "group-a", Status: model.EdgeRouteStatusActive, RoutePolicy: model.EdgeRoutePolicyEnabled}}}
			old := NewService(config.EdgeConfig{EdgeID: "edge-a", EdgeGroupID: "group-a"}, nil)
			old.recordSyncSuccess(bundle, "", time.Now(), false)
			current := NewService(config.EdgeConfig{EdgeID: "edge-a", EdgeGroupID: "group-a"}, nil)
			next := bundle
			next.Version = "current"
			current.recordSyncSuccess(next, "", time.Now(), false)

			streamDone := make(chan struct{})
			var once sync.Once
			releaseStream := func() { once.Do(func() { close(streamDone) }) }
			defer releaseStream()
			backend := func(s *Service) *httptest.Server {
				handler := s.ProxyHandler()
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if (r.ProtoMajor == 2) != h2 {
						t.Error("fixture negotiated another HTTP version", r.Proto)
					}
					if r.URL.Path == "/stream" {
						io.WriteString(w, "before\n")
						w.(http.Flusher).Flush()
						select {
						case <-streamDone:
							io.WriteString(w, "after\n")
						case <-r.Context().Done():
						}
						return
					}
					if strings.HasPrefix(r.URL.Path, "/app-error/") {
						status, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/app-error/"))
						w.Header().Set("X-Application-Error", "unchanged")
						w.WriteHeader(status)
						io.WriteString(w, "application error")
						return
					}
					handler.ServeHTTP(w, r)
				}))
				server.EnableHTTP2 = h2
				server.StartTLS()
				t.Cleanup(server.Close)
				return server
			}
			previousServer, currentServer := backend(old), backend(current)
			previousAddress, currentAddress := previousServer.Listener.Addr().String(), currentServer.Listener.Addr().String()
			previousCertificate, currentCertificate := previousServer.Certificate(), currentServer.Certificate()
			// Exercise the exact production TLS terminator too when requested;
			// a hop-by-hop Connection header alone cannot cross a reverse proxy.
			if binary := os.Getenv("FUGUE_TEST_CADDY_BINARY"); binary != "" {
				previousAddress, previousCertificate = routeProofAffinityCaddy(t, binary, previousServer, bundle, h2)
				currentAddress, currentCertificate = routeProofAffinityCaddy(t, binary, currentServer, next, h2)
			}
			var target atomic.Value
			target.Store(previousAddress)
			var connects atomic.Int32
			roots := x509.NewCertPool()
			roots.AddCert(previousCertificate)
			roots.AddCert(currentCertificate)
			transport := &http.Transport{
				ForceAttemptHTTP2: h2,
				TLSClientConfig:   &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
				DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
					connects.Add(1)
					return (&net.Dialer{}).DialContext(ctx, network, target.Load().(string))
				},
			}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
			probe := func(wantStatus int, wantVersion string) {
				t.Helper()
				r, _ := http.NewRequest(http.MethodHead, "https://example.com/", nil)
				r.Host = "app.example.test"
				nonce := "0123456789abcdef0123456789abcdef"
				r.Header.Set(routeproof.RequestHeader, "1")
				r.Header.Set(routeproof.NonceHeader, nonce)
				response, err := client.Do(r)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				if response.StatusCode != wantStatus {
					t.Fatalf("status=%d want=%d", response.StatusCode, wantStatus)
				}
				proof, err := routeprobe.ParseResponse(response, nonce, time.Now())
				if wantStatus == http.StatusNoContent {
					if err != nil || proof.Version != wantVersion {
						t.Fatal("unexpected proof", proof, err)
					}
				} else if err == nil || response.Header.Get(routeproof.DigestHeader) != "" {
					t.Fatal("rejected probe obtained a serving proof")
				}
			}
			probe(http.StatusNoContent, "previous")
			probe(http.StatusNoContent, "previous")
			if connects.Load() != 1 {
				t.Fatal("healthy probes lost connection reuse", connects.Load())
			}
			for _, status := range []int{400, 404, 503} {
				r, _ := http.NewRequest(http.MethodGet, "https://example.com/app-error/"+strconv.Itoa(status), nil)
				r.Host = "app.example.test"
				response, err := client.Do(r)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || response.StatusCode != status || string(body) != "application error" || response.Header.Get("X-Application-Error") != "unchanged" || response.Close || connects.Load() != 1 {
					t.Fatal("ordinary application error changed", status, string(body), err, connects.Load())
				}
			}

			// HTTP/2 can multiplex business traffic on the same connection. A
			// GOAWAY must leave this stream alive until it finishes normally.
			var stream *http.Response
			if h2 {
				var err error
				r, _ := http.NewRequest(http.MethodGet, "https://example.com/stream", nil)
				r.Host = "app.example.test"
				stream, err = client.Do(r)
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Body.Close()
				prefix := make([]byte, len("before\n"))
				if _, err = io.ReadFull(stream.Body, prefix); err != nil || string(prefix) != "before\n" {
					t.Fatal("stream failed before handoff", string(prefix), err)
				}
				if connects.Load() != 1 {
					t.Fatal("stream did not share the existing HTTP/2 connection")
				}
			}
			target.Store(currentAddress)
			old.recordSyncSuccessWithPublication(bundle, "", time.Now(), false, routePublicationMetadata{Candidate: true})
			probe(http.StatusServiceUnavailable, "")
			probe(http.StatusNoContent, "current")
			if connects.Load() != 2 {
				t.Fatal("rejected probe did not reconnect to current backend", connects.Load())
			}
			if h2 {
				releaseStream()
				rest, err := io.ReadAll(stream.Body)
				if err != nil || strings.TrimSpace(string(rest)) != "after" {
					t.Fatal("probe rejection interrupted an in-flight stream", string(rest), err)
				}
			}
		})
	}
}

func routeProofAffinityCaddy(t *testing.T, binary string, upstream *httptest.Server, bundle model.EdgeRouteBundle, h2 bool) (string, *x509.Certificate) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	certificate, key := testCaddyTLSKeyPairAt(t, "example.com", time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	block, _ := pem.Decode([]byte(certificate))
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(config.EdgeConfig{EdgeGroupID: "group-a", CaddyListenAddr: address, CaddyProxyListenAddr: upstream.Listener.Addr().String()}, nil)
	raw, _, err := service.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	apps := document["apps"].(map[string]any)
	server := apps["http"].(map[string]any)["servers"].(map[string]any)["fugue_edge"].(map[string]any)
	server["automatic_https"] = map[string]any{"disable": true}
	server["tls_connection_policies"] = []any{map[string]any{}}
	server["protocols"] = []string{"h1"}
	if h2 {
		server["protocols"] = []string{"h1", "h2"}
	}
	// The loopback backend still uses verified TLS, just like the observer.
	routes := server["routes"].([]any)
	for _, rawRoute := range routes {
		for _, rawHandler := range rawRoute.(map[string]any)["handle"].([]any) {
			handler := rawHandler.(map[string]any)
			if handler["handler"] == "reverse_proxy" {
				handler["transport"] = map[string]any{"protocol": "http", "tls": map[string]any{"server_name": "example.com", "ca": map[string]any{"provider": "inline", "trusted_ca_certs": []string{base64.StdEncoding.EncodeToString(upstream.Certificate().Raw)}}}}
			}
		}
	}
	apps["tls"] = map[string]any{"disable_storage_clean": true, "certificates": map[string]any{"load_pem": []any{map[string]any{"certificate": certificate, "key": key}}}}
	directory := t.TempDir()
	document["storage"] = map[string]any{"module": "file_system", "root": filepath.Join(directory, "storage")}
	document["admin"] = map[string]any{"disabled": true, "config": map[string]any{"persist": false}}
	document["logging"] = map[string]any{"logs": map[string]any{"default": map[string]any{"writer": map[string]any{"output": "discard"}}, "fugue_edge_access": map[string]any{"writer": map[string]any{"output": "discard"}, "include": []string{"http.log.access.fugue_edge_access"}}}}
	raw, _ = json.Marshal(document)
	path := filepath.Join(directory, "config.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	command := exec.CommandContext(ctx, binary, "run", "--config", path)
	logPath := filepath.Join(directory, "caddy.log")
	log, err := os.Create(logPath)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		cancel()
		log.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() { cancel(); <-done; log.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for {
		connection, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err == nil {
			connection.Close()
			break
		}
		if time.Now().After(deadline) {
			output, _ := os.ReadFile(logPath)
			t.Fatal("Caddy listener unavailable", err, string(output))
		}
		time.Sleep(10 * time.Millisecond)
	}
	return address, leaf
}
