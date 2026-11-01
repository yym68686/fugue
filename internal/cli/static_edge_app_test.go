package cli

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"fugue/internal/model"
	c "fugue/internal/staticedgecontract"
)

func TestStaticEdgeAppCommandPreviewsAndAppliesWithIndependentJournals(t *testing.T) {
	for _, mode := range []string{"preview", "execute", "edge-activation-failure"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("FUGUE_CONTEXT_FILE", filepath.Join(dir, "fugue-contexts.json"))
			t.Setenv("FUGUE_STATIC_EDGE_CONTEXT_FILE", filepath.Join(dir, "static-contexts.json"))
			publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
			if err != nil {
				t.Fatal(err)
			}
			keyFile := filepath.Join(dir, "signer.key")
			if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
				t.Fatal(err)
			}
			outDir := filepath.Join(dir, "output")
			var mu sync.Mutex
			var writes []string
			makeManager := func(role string) (staticEdgeContext, string) {
				cfg, credentials := tlsFixture(t)
				cfg.Name, cfg.EdgeID = role, "test-"+role
				config := json.RawMessage(`{"apps":{"http":{"servers":{"main":{"routes":[]}}}}}`)
				if role == "edge" {
					config = json.RawMessage(`{"apps":{"http":{"servers":{"main":{"routes":[{"match":[{"host":["old.example.test"]}],"handle":[{"handler":"reverse_proxy","upstreams":[{"dial":"127.0.0.1:18480"}]}]}]}}}}}`)
				}
				baseline := c.Bundle{Schema: c.SchemaV1, EdgeID: cfg.EdgeID, Role: role, Generation: 2, Mode: "serving", CaddyConfig: config, HealthChecks: []string{"local-health"}}
				if err := c.SignBundle(&baseline, privateKey, "operator"); err != nil {
					t.Fatal(err)
				}
				baselinePath := filepath.Join(dir, role+".json")
				if err := writeStaticEdgeAppBundle(baselinePath, baseline); err != nil {
					t.Fatal(err)
				}
				revision := uint64(7)
				candidateDigest := ""
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					if r.Header.Get("Authorization") != "" {
						t.Error("Fugue API credential leaked to static manager")
					}
					var req c.Request
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
						http.Error(w, "bad request", http.StatusBadRequest)
						return
					}
					response := c.Response{Schema: c.RPCSchema, EdgeID: cfg.EdgeID, RequestID: req.RequestID, OK: true, Status: 200}
					if req.Operation == "status" {
						response.Result = &c.Observed{EdgeID: cfg.EdgeID, Role: role, Ready: true, RuntimeMatches: true, ActiveGeneration: baseline.Generation, ActiveDigest: baseline.BundleDigest, Revision: revision}
					} else {
						writes = append(writes, role+":"+req.Operation)
						if req.ExpectedRevision == nil || *req.ExpectedRevision != revision {
							t.Errorf("%s %s: wrong expected revision: %v", role, req.Operation, req.ExpectedRevision)
						}
						journalRaw, err := os.ReadFile(filepath.Join(outDir, "operation.json"))
						if err != nil || !bytes.Contains(journalRaw, []byte(req.RequestID)) {
							t.Errorf("request was not durably recorded before sending: %s: %v", req.RequestID, err)
						}
						if req.Operation == "stage" {
							if req.Bundle == nil || c.VerifyBundle(*req.Bundle, publicKey) != nil || req.Bundle.Generation != 3 {
								t.Error("manager did not receive a signed next-generation bundle")
							}
							candidateDigest = req.Bundle.BundleDigest
						} else if req.Operation != "activate" || req.TargetDigest != candidateDigest {
							t.Errorf("unexpected operation or candidate digest: %+v", req)
						}
						if mode == "edge-activation-failure" && role == "edge" && req.Operation == "activate" {
							response.OK, response.Status, response.Error = false, 503, "fixture activation failed"
						} else {
							revision++
							outcome := "staged"
							if req.Operation == "activate" {
								outcome = "activated"
							}
							response.Receipt = &c.Receipt{RequestID: req.RequestID, Operation: req.Operation, Outcome: outcome, Revision: revision, BundleDigest: candidateDigest, RuntimeVerified: req.Operation == "activate"}
						}
					}
					_ = json.NewEncoder(w).Encode(response)
				}))
				server.TLS = credentials.Config()
				server.StartTLS()
				t.Cleanup(server.Close)
				cfg.ManagerURL = server.URL
				return cfg, baselinePath
			}
			edge, edgeFile := makeManager("edge")
			origin, originFile := makeManager("origin")
			if err := saveStaticEdgeContexts(staticEdgeContextFile{SchemaVersion: 1, Contexts: []staticEdgeContext{edge, origin}}); err != nil {
				t.Fatal(err)
			}
			app := model.App{ID: "app_fixture", Name: "sample", Route: &model.AppRoute{Hostname: "app.example.test", PathPrefix: "/api"}, InternalService: &model.AppInternalService{Host: "app-fixture.ns.svc.cluster.local", Port: 8080}}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("app integration mutated Fugue API: %s %s", r.Method, r.URL.Path)
				}
				switch r.URL.Path {
				case "/v1/apps":
					_ = json.NewEncoder(w).Encode(map[string]any{"apps": []model.App{app}})
				case "/v1/apps/" + app.ID:
					_ = json.NewEncoder(w).Encode(map[string]any{"app": app})
				default:
					t.Errorf("unexpected Fugue API request: %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer api.Close()
			args := []string{"--base-url", api.URL, "--token", "fixture-token", "--json", "static-edge", "app", "add", app.Name, "--edge-context", edge.Name, "--origin-context", origin.Name, "--edge-bundle", edgeFile, "--origin-bundle", originFile, "--edge-template-host", "old.example.test", "--signing-key", keyFile, "--out-dir", outDir}
			if mode != "preview" {
				args = append(args, "--execute")
			}
			var stdout, stderr bytes.Buffer
			err = runWithStreams(args, &stdout, &stderr)
			if mode == "edge-activation-failure" {
				if err == nil || !strings.Contains(err.Error(), "fixture activation failed") {
					t.Fatalf("expected activation failure, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("command failed: %v\n%s\n%s", err, stdout.String(), stderr.String())
			}
			mu.Lock()
			defer mu.Unlock()
			if mode == "preview" && len(writes) != 0 {
				t.Fatalf("preview wrote manager state: %v", writes)
			}
			if mode != "preview" && strings.Join(writes, ",") != "origin:stage,edge:stage,origin:activate,edge:activate" {
				t.Fatalf("unexpected operation order or implicit rollback: %v", writes)
			}
			journalRaw, err := os.ReadFile(filepath.Join(outDir, "operation.json"))
			if err != nil {
				t.Fatal(err)
			}
			var result staticEdgeAppAddResult
			if err := json.Unmarshal(journalRaw, &result); err != nil {
				t.Fatal(err)
			}
			if result.Executed != (mode == "execute") || result.DNSChanged {
				t.Fatalf("inaccurate operation result: %+v", result)
			}
			if mode == "edge-activation-failure" && (result.Operations[2].State != "succeeded" || result.Operations[3].State != "error_or_unknown") {
				t.Fatalf("partial application was not preserved in the journal: %+v", result)
			}
		})
	}
}

func TestStaticEdgeAppRoutesPreserveBaselineAndBindAppService(t *testing.T) {
	edgeConfig, err := decodeStaticEdgeAppCaddyConfig([]byte(`{"apps":{"http":{"servers":{"public":{"routes":[{"match":[{"host":["old.example.test"]}],"handle":[{"handler":"reverse_proxy","upstreams":[{"dial":"127.0.0.1:18480"}]}]}]}}},"tls":{"automation":{"policies":[{"subjects":["old.example.test"]}]}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	originConfig, err := decodeStaticEdgeAppCaddyConfig([]byte(`{"apps":{"http":{"servers":{"origin":{"routes":[{"handle":[{"handler":"static_response","status_code":404}]}]}}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	edgeServer, err := staticEdgeAppHTTPServer(edgeConfig, "")
	if err != nil {
		t.Fatal(err)
	}
	originServer, err := staticEdgeAppHTTPServer(originConfig, "")
	if err != nil {
		t.Fatal(err)
	}
	if err = staticEdgeAppAddEdgeRoute(edgeConfig, edgeServer, "old.example.test", "app.example.test", "/api"); err != nil {
		t.Fatal(err)
	}
	staticEdgeAppAddTLSSubject(edgeConfig, "old.example.test", "app.example.test")
	if err = staticEdgeAppAddOriginRoute(originServer, "app.example.test", "/api", "app-123.namespace.svc.cluster.local:8080"); err != nil {
		t.Fatal(err)
	}

	edgeRoutes := edgeServer["routes"].([]any)
	if len(edgeRoutes) != 2 || !routeHasStaticEdgeAppHostname(edgeRoutes[0], "app.example.test") || !routeHasStaticEdgeAppHostname(edgeRoutes[1], "old.example.test") {
		t.Fatalf("edge route insertion did not preserve the template: %#v", edgeRoutes)
	}
	edgePath := edgeRoutes[0].(map[string]any)["match"].([]any)[0].(map[string]any)["path"].([]any)
	if len(edgePath) != 2 || edgePath[0] != "/api" || edgePath[1] != "/api/*" {
		t.Fatalf("edge path matcher did not follow the app route: %#v", edgePath)
	}
	policies := edgeConfig["apps"].(map[string]any)["tls"].(map[string]any)["automation"].(map[string]any)["policies"].([]any)
	subjects := policies[0].(map[string]any)["subjects"].([]any)
	if !staticEdgeAppStringSliceContains(subjects, "app.example.test") {
		t.Fatalf("TLS subject not added: %#v", subjects)
	}

	originRoutes := originServer["routes"].([]any)
	if len(originRoutes) != 2 || !routeHasStaticEdgeAppForwardedHost(originRoutes[0], "app.example.test") {
		t.Fatalf("origin route was not inserted before fallback: %#v", originRoutes)
	}
	route := originRoutes[0].(map[string]any)
	matcher := route["match"].([]any)[0].(map[string]any)
	paths := matcher["path"].([]any)
	if len(paths) != 2 || paths[0] != "/api" || paths[1] != "/api/*" {
		t.Fatalf("unexpected path match: %#v", paths)
	}
	handler := route["handle"].([]any)[0].(map[string]any)
	if got := handler["upstreams"].([]any)[0].(map[string]any)["dial"]; got != "app-123.namespace.svc.cluster.local:8080" {
		t.Fatalf("unexpected origin upstream %v", got)
	}
	setHost := handler["headers"].(map[string]any)["request"].(map[string]any)["set"].(map[string]any)["Host"].([]any)
	if len(setHost) != 1 || setHost[0] != "{http.request.header.X-Forwarded-Host}" {
		t.Fatalf("origin must restore the original public Host: %#v", setHost)
	}
}

func TestStaticEdgeAppRouteRejectsDuplicateHostsAndMalformedConfig(t *testing.T) {
	config, err := decodeStaticEdgeAppCaddyConfig([]byte(`{"apps":{"http":{"servers":{"public":{"routes":[{"match":[{"host":["app.example.test"]}],"handle":[]}]}}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	server, err := staticEdgeAppHTTPServer(config, "public")
	if err != nil {
		t.Fatal(err)
	}
	if err = staticEdgeAppAddEdgeRoute(config, server, "app.example.test", "app.example.test", "/"); err == nil || !strings.Contains(err.Error(), "template") {
		t.Fatalf("expected same-template rejection, got %v", err)
	}
	if err = staticEdgeAppAddEdgeRoute(config, server, "other.example.test", "app.example.test", "/"); err == nil || !strings.Contains(err.Error(), "already contains") {
		t.Fatalf("expected duplicate hostname rejection, got %v", err)
	}
	if _, err = staticEdgeAppHTTPServer(config, "missing"); err == nil {
		t.Fatal("missing server was accepted")
	}
	ambiguous, err := decodeStaticEdgeAppCaddyConfig([]byte(`{"apps":{"http":{"servers":{"public":{"routes":[{"match":[{"host":["template.example.test"]}],"handle":[]},{"match":[{"host":["template.example.test"],"path":["/assets/*"]}],"handle":[]}]}}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	ambiguousServer, err := staticEdgeAppHTTPServer(ambiguous, "public")
	if err != nil {
		t.Fatal(err)
	}
	if err = staticEdgeAppAddEdgeRoute(ambiguous, ambiguousServer, "template.example.test", "new.example.test", "/"); err == nil || !strings.Contains(err.Error(), "path-specific") {
		t.Fatalf("expected path-specific template route rejection, got %v", err)
	}
}

func TestStaticEdgeAppRouteClonesHostOnlyRouteSet(t *testing.T) {
	config, err := decodeStaticEdgeAppCaddyConfig([]byte(`{"apps":{"http":{"servers":{"public":{"routes":[{"match":[{"host":["template.example.test"]}],"handle":[{"handler":"headers"}]},{"match":[{"host":["template.example.test"]}],"handle":[{"handler":"reverse_proxy"}]}]}}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	server, err := staticEdgeAppHTTPServer(config, "public")
	if err != nil {
		t.Fatal(err)
	}
	if err = staticEdgeAppAddEdgeRoute(config, server, "template.example.test", "new.example.test", "/"); err != nil {
		t.Fatal(err)
	}
	routes := server["routes"].([]any)
	if len(routes) != 4 {
		t.Fatalf("expected cloned route set, got %d routes", len(routes))
	}
	for i := 0; i < 2; i++ {
		if !routeHasStaticEdgeAppHostname(routes[i], "new.example.test") {
			t.Fatalf("route %d was not cloned for new host: %#v", i, routes[i])
		}
	}
	for i := 2; i < 4; i++ {
		if !routeHasStaticEdgeAppHostname(routes[i], "template.example.test") {
			t.Fatalf("template route %d was not preserved: %#v", i, routes[i])
		}
	}
}

func TestStaticEdgeAppTargetAndUpstreamValidation(t *testing.T) {
	app := model.App{Route: &model.AppRoute{Hostname: "primary.example.test", PathPrefix: "/v1"}}
	for _, hostname := range []string{"https://example.test", "bad host", "*.example.test", "example.test:443", "localhost"} {
		if _, _, err := staticEdgeAppRouteTarget(app, staticEdgeAppAddOptions{Hostname: hostname}); err == nil {
			t.Fatalf("invalid hostname %q was accepted", hostname)
		}
	}
	if _, _, err := staticEdgeAppRouteTarget(app, staticEdgeAppAddOptions{PathPrefix: "/api?x=1"}); err == nil {
		t.Fatal("query string in path prefix was accepted")
	}
	for _, upstream := range []string{"https://service.example.test:443", "service.example.test", "service.example.test:0", "service.example.test:65536", "service.example.test:80/path"} {
		if err := validateStaticEdgeAppUpstream(upstream); err == nil {
			t.Fatalf("invalid upstream %q was accepted", upstream)
		}
	}
	if err := validateStaticEdgeAppUpstream("app-123.ns.svc.cluster.local:8080"); err != nil {
		t.Fatal(err)
	}
}

func TestStaticEdgeAppBundleBaselineRequiresExactActiveDigest(t *testing.T) {
	bundle := c.Bundle{Generation: 3, BundleDigest: "sha256:active"}
	observed := c.Observed{ActiveGeneration: 3, ActiveDigest: "sha256:active"}
	if err := matchStaticEdgeAppBaseline(bundle, observed); err != nil {
		t.Fatal(err)
	}
	observed.ActiveGeneration++
	if err := matchStaticEdgeAppBaseline(bundle, observed); err == nil {
		t.Fatal("stale bundle generation was accepted")
	}
	observed.ActiveGeneration = bundle.Generation
	observed.ActiveDigest = "sha256:other"
	if err := matchStaticEdgeAppBaseline(bundle, observed); err == nil {
		t.Fatal("stale bundle digest was accepted")
	}
}

func TestStaticEdgeAppCaddyConfigRoundTripPreservesJSONNumbers(t *testing.T) {
	config, err := decodeStaticEdgeAppCaddyConfig([]byte(`{"apps":{"http":{"servers":{"public":{"read_timeout":9007199254740993,"routes":[]}}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "9007199254740993") {
		t.Fatalf("large number was changed by config round trip: %s", raw)
	}
}

func TestStaticEdgeAppGeneratedOriginRoutePassesCaddyValidation(t *testing.T) {
	caddy := strings.TrimSpace(os.Getenv("FUGUE_TEST_CADDY"))
	if caddy == "" {
		t.Skip("FUGUE_TEST_CADDY is not configured")
	}
	config, err := decodeStaticEdgeAppCaddyConfig([]byte(`{"apps":{"http":{"servers":{"origin":{"listen":["127.0.0.1:19880"],"routes":[]}}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	server, err := staticEdgeAppHTTPServer(config, "origin")
	if err != nil {
		t.Fatal(err)
	}
	if err = staticEdgeAppAddOriginRoute(server, "app.example.test", "/api", "app-123.namespace.svc.cluster.local:8080"); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "caddy.json")
	if err = os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(caddy, "validate", "--config", configPath)
	cmd.Env = append(os.Environ(), "XDG_CONFIG_HOME="+t.TempDir(), "XDG_DATA_HOME="+t.TempDir())
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Caddy rejected generated origin route: %v\n%s", err, output)
	}
}
