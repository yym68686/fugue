package edge

import (
	"encoding/json"
	"log"
	"strings"
	"testing"

	"fugue/internal/config"
)

func TestBuildCaddyConfigOverwritesForwardedForAtLoopbackTrustBoundary(t *testing.T) {
	t.Parallel()

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      ":18443",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))
	configBody, _, err := service.buildCaddyConfig(testBundle("routegen_caddy_xff_clean"))
	if err != nil {
		t.Fatalf("build caddy config: %v", err)
	}

	var document map[string]any
	if err := json.Unmarshal(configBody, &document); err != nil {
		t.Fatalf("decode Caddy config: %v", err)
	}
	apps := document["apps"].(map[string]any)
	httpApp := apps["http"].(map[string]any)
	servers := httpApp["servers"].(map[string]any)
	server := servers["fugue_edge"].(map[string]any)
	routes := server["routes"].([]any)
	firstRoute := routes[0].(map[string]any)
	handlers := firstRoute["handle"].([]any)
	reverseProxy := handlers[0].(map[string]any)
	headers := reverseProxy["headers"].(map[string]any)
	requestHeaders := headers["request"].(map[string]any)
	setHeaders := requestHeaders["set"].(map[string]any)
	forwardedFor := setHeaders["X-Forwarded-For"].([]any)
	if len(forwardedFor) != 1 || forwardedFor[0] != "{http.request.remote.host}" {
		t.Fatalf("expected Caddy to replace any client XFF with the connection identity, got %#v", forwardedFor)
	}
	if strings.Contains(string(configBody), "{http.request.header.X-Forwarded-For}") {
		t.Fatalf("Caddy config must not copy an untrusted client XFF chain:\n%s", configBody)
	}
}

func TestCaddyProxyListenerRequiresExplicitLoopbackIP(t *testing.T) {
	t.Parallel()

	base := config.EdgeConfig{
		APIURL:          "https://api.example.invalid",
		EdgeToken:       "edge-secret",
		EdgeGroupID:     "edge-group-default",
		CaddyEnabled:    true,
		CaddyAdminURL:   "http://127.0.0.1:2019",
		CaddyListenAddr: ":18443",
	}
	for _, listenAddr := range []string{"127.0.0.1:7833", "[::1]:7833"} {
		cfg := base
		cfg.CaddyProxyListenAddr = listenAddr
		if err := NewService(cfg, log.New(ioDiscard{}, "", 0)).validateConfig(); err != nil {
			t.Fatalf("expected loopback listener %q to be accepted: %v", listenAddr, err)
		}
	}
	for _, listenAddr := range []string{"0.0.0.0:7833", "[::]:7833", ":7833", "10.0.0.8:7833", "localhost:7833"} {
		cfg := base
		cfg.CaddyProxyListenAddr = listenAddr
		err := NewService(cfg, log.New(ioDiscard{}, "", 0)).validateConfig()
		if err == nil || !strings.Contains(err.Error(), "loopback") {
			t.Fatalf("expected non-explicit-loopback listener %q to be rejected, got %v", listenAddr, err)
		}
	}
}
