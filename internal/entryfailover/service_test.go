package entryfailover

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testService(t *testing.T) (*Service, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := Sign(testPolicy(), "signer", priv)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(signed)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	policyPath := filepath.Join(dir, "policy.json")
	if err = os.WriteFile(policyPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	vault := CredentialVault{StateDir: filepath.Join(dir, "state"), Key: bytes.Repeat([]byte{1}, 32)}
	service, err := NewService(policyPath, map[string]ed25519.PublicKey{"signer": pub}, vault, "")
	if err != nil {
		t.Fatal(err)
	}
	return service, priv
}

func callService(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
	return w
}

func TestServiceManagementGrantsAndSecretRedaction(t *testing.T) {
	service, priv := testService(t)
	auth := func(*http.Request) (string, string, error) { return "reader", "read", nil }
	read := service.Handler(auth)
	if got := callService(read, "GET", "/v1/entry-failover/status", ""); got.Code != 200 {
		t.Fatalf("read status=%d", got.Code)
	}
	if got := callService(read, "POST", "/v1/entry-failover/credential", `{"token":"secret-test"}`); got.Code != 403 {
		t.Fatalf("reader imported token: %d", got.Code)
	}
	if got := callService(read, "POST", "/v1/entry-failover/switch", "{}"); got.Code != 403 {
		t.Fatalf("reader switched DNS: %d", got.Code)
	}
	admin := service.Handler(func(*http.Request) (string, string, error) { return "admin", "admin", nil })
	got := callService(admin, "POST", "/v1/entry-failover/credential", `{"token":"secret-test"}`)
	if got.Code != 200 || strings.Contains(got.Body.String(), "secret-test") {
		t.Fatalf("credential response leaked or failed: %d %s", got.Code, got.Body.String())
	}
	stored, err := os.ReadFile(service.vault.path())
	if err != nil || strings.Contains(string(stored), "secret-test") {
		t.Fatalf("credential stored plaintext: %v", err)
	}
	next := testPolicy()
	next.Generation = 2
	next.Mode = "off"
	signed, err := Sign(next, "signer", priv)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(signed)
	got = callService(admin, "POST", "/v1/entry-failover/policy", string(raw))
	if got.Code != 200 || service.signed.Policy.Mode != "off" {
		t.Fatalf("signed policy update failed: %d %s", got.Code, got.Body.String())
	}
	next.Generation = 3
	next.Hostnames[1] = "another.example.test"
	raw, _ = json.Marshal(next)
	got = callService(admin, "POST", "/v1/entry-failover/policy", string(raw))
	if got.Code != 409 {
		t.Fatalf("unsigned scope expansion accepted: %d", got.Code)
	}
	denied := service.Handler(func(*http.Request) (string, string, error) { return "", "", errors.New("not authorized") })
	if got = callService(denied, "GET", "/v1/entry-failover/status", ""); got.Code != 403 {
		t.Fatalf("unauthorized status=%d", got.Code)
	}
}

func TestServiceRestartRetainsSignedLKGWhenCurrentPolicyCorrupt(t *testing.T) {
	service, _ := testService(t)
	if err := os.WriteFile(service.policyPath, []byte(`{"corrupt":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewService(service.policyPath, service.keys, service.vault, "")
	if err != nil {
		t.Fatal(err)
	}
	if restarted.signed.Digest != service.signed.Digest || restarted.signed.Policy.Mode != "shadow" {
		t.Fatalf("signed LKG not retained: %+v", restarted.signed)
	}
}

func TestPreflightIsReadOnlyAndDoesNotHoldTheDecisionLock(t *testing.T) {
	service, _ := testService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "/v1/entry-failover/preflight", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	service.Handler(func(*http.Request) (string, string, error) { return "reader", "read", nil }).ServeHTTP(w, request)
	if w.Code != http.StatusOK {
		t.Fatalf("preflight status=%d: %s", w.Code, w.Body.String())
	}
	var result Preflight
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.CredentialReady || result.Current != "" || len(result.Targets) != 2 || len(result.Errors) == 0 {
		t.Fatalf("preflight reported unverified readiness: %+v", result)
	}
	if _, err := os.Stat(service.executor.activePath()); !os.IsNotExist(err) {
		t.Fatalf("preflight created a DNS operation: %v", err)
	}
	if !service.mu.TryLock() {
		t.Fatal("preflight retained the decision lock")
	}
	service.mu.Unlock()
}

func TestExpiredPolicyStartsForConfigurationRecoveryButCannotSwitch(t *testing.T) {
	pub, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	policy := testPolicy()
	policy.Mode = "automatic"
	policy.Vantages = append(policy.Vantages, Vantage{ID: "remote", Transport: "ssh", SSHHost: "separate", BinaryPath: "/usr/local/bin/fugue-entry-failover", PublicKeyPath: "/etc/fugue-entry-failover/policy.pub"})
	policy.ExpiresAt = time.Now().Add(-time.Minute)
	signed, err := Sign(policy, "signer", private)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	policyPath := filepath.Join(dir, "policy.json")
	raw, _ := json.Marshal(signed)
	if err = os.WriteFile(policyPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	vault := CredentialVault{StateDir: filepath.Join(dir, "state"), Key: bytes.Repeat([]byte{1}, 32)}
	service, err := NewService(policyPath, map[string]ed25519.PublicKey{"signer": pub}, vault, "")
	if err != nil {
		t.Fatalf("expired policy blocked management recovery: %v", err)
	}
	if _, err = service.Cycle(context.Background()); err == nil {
		t.Fatal("expired policy allowed automatic cycle")
	}
	if _, err = service.executor.Switch(context.Background(), "managed", false); err == nil {
		t.Fatal("expired policy allowed a manual DNS write")
	}
	policy.ExpiresAt = time.Now().Add(time.Hour)
	policy.Generation++
	renewed, err := Sign(policy, "signer", private)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(renewed)
	if err = service.applyPolicy(raw); err != nil {
		t.Fatalf("renewal failed after expired-policy start: %v", err)
	}
}
