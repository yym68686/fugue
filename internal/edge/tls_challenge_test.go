package edge

import (
	"encoding/json"
	"fugue/internal/config"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAllConsumersForwardSameChallengeAndFailClosedOffline(t *testing.T) {
	token := strings.Repeat("a", 32)
	proof := token + "." + strings.Repeat("b", 43)
	calls := 0
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "" || r.URL.RawQuery != "" {
			t.Error("challenge forwarding leaked credentials")
		}
		if r.URL.Path != "/v1/acme/http-01/site.example.test/"+token {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(proof))
	}))
	defer authority.Close()
	for range 6 {
		service := NewService(config.EdgeConfig{APIURL: "http://internal-api.invalid", ACMEChallengeAPIURL: authority.URL, EdgeToken: "private-fixture-token", CaddyTLSMode: caddyTLSModeSharedOnly, CaddyHTTPListenAddr: ":18080"}, nil)
		service.HTTPClient = authority.Client()
		request := httptest.NewRequest(http.MethodGet, "http://site.example.test/.well-known/acme-challenge/"+token, nil)
		output := httptest.NewRecorder()
		service.handleProxy(output, request)
		if output.Code != 200 || output.Body.String() != proof {
			t.Fatal("wrong edge did not serve shared challenge", output.Code, output.Body.String())
		}
	}
	if calls != 6 {
		t.Fatal(calls)
	}
	service := NewService(config.EdgeConfig{APIURL: authority.URL, CaddyTLSMode: caddyTLSModeSharedOnly, CaddyHTTPListenAddr: ":18080"}, nil)
	service.HTTPClient = authority.Client()
	authority.Close()
	out := httptest.NewRecorder()
	service.handleProxy(out, httptest.NewRequest(http.MethodGet, "http://site.example.test/.well-known/acme-challenge/"+token, nil))
	if out.Code != 503 {
		t.Fatal("control outage returned stale success", out.Code)
	}
}

func TestChallengeForwardingRejectsRedirectsAndWrongProof(t *testing.T) {
	token := strings.Repeat("a", 32)
	for _, mode := range []string{"redirect", "wrong-proof", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "redirect":
					http.Redirect(w, r, "https://untrusted.example/", 302)
				case "wrong-proof":
					io.WriteString(w, "different-token.key")
				case "unknown":
					http.NotFound(w, r)
				}
			}))
			defer backend.Close()
			service := NewService(config.EdgeConfig{APIURL: backend.URL, CaddyHTTPListenAddr: ":18080"}, nil)
			service.HTTPClient = backend.Client()
			out := httptest.NewRecorder()
			service.handleSharedHTTP01Challenge(out, httptest.NewRequest(http.MethodGet, "http://site.example.test/.well-known/acme-challenge/"+token, nil))
			if out.Code != 503 && out.Code != 404 {
				t.Fatal("untrusted challenge response passed", out.Code)
			}
		})
	}
}

func TestSharedOnlyCaddyHasExplicitHTTPChallengeListener(t *testing.T) {
	service, bundle, _ := sharedOnlyTLSFixture(t)
	service.Config.CaddyHTTPListenAddr = "127.0.0.1:18080"
	service.Config.CaddyAdminURL = "http://127.0.0.1:2019"
	raw, _, err := service.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err = json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	servers := document["apps"].(map[string]any)["http"].(map[string]any)["servers"].(map[string]any)
	if servers["fugue_http_challenges"] == nil || !strings.Contains(string(raw), "/.well-known/acme-challenge/*") || strings.Contains(string(raw), "on_demand") {
		t.Fatal("consumer has no challenge listener or can issue", string(raw))
	}
	// Signature changes ensure the listener is actually applied.
	before, _ := service.caddyConfigSignature(bundle)
	service.Config.CaddyHTTPListenAddr = "127.0.0.1:28080"
	after, _ := service.caddyConfigSignature(bundle)
	if before == after {
		t.Fatal("HTTP policy did not invalidate config")
	}
}
