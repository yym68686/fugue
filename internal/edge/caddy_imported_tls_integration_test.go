package edge

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestImportedCertificateReloadWithRealCaddy(t *testing.T) {
	image := os.Getenv("FUGUE_TEST_CADDY_IMAGE")
	if image == "" {
		t.Skip("set FUGUE_TEST_CADDY_IMAGE to run isolated Docker Caddy handshake test")
	}
	host := "customer.external.test"
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	writePair := func() {
		cert, key := testCaddyTLSKeyPairAt(t, host, time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour))
		if err := os.WriteFile(certPath, []byte(cert+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyPath, []byte(key+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writePair()
	config := map[string]any{
		"admin": map[string]any{"listen": "0.0.0.0:2019"},
		"logging": map[string]any{"logs": map[string]any{
			"default": map[string]any{"encoder": caddySecretRedactionEncoder(), "exclude": []string{"http.log.access.log0"}},
			"log0":    map[string]any{"encoder": caddySecretRedactionEncoder(), "include": []string{"http.log.access.log0"}},
		}},
		"apps": map[string]any{
			"http": map[string]any{"servers": map[string]any{"test": map[string]any{
				"listen": []string{":8443"}, "tls_connection_policies": []any{map[string]any{}},
				"logs":   map[string]any{"logger_names": map[string][]string{host: {"log0"}}},
				"routes": []any{map[string]any{"handle": []any{map[string]any{"handler": "reverse_proxy", "upstreams": []any{map[string]string{"dial": "127.0.0.1:1"}}}}}},
			}}},
			"tls": map[string]any{"certificates": map[string]any{"load_files": []any{map[string]string{
				"certificate": "/certs/tls.crt", "key": "/certs/tls.key",
			}}}},
		},
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	start := exec.Command("docker", "run", "--rm", "-d", "-p", "127.0.0.1::2019", "-p", "127.0.0.1::8443",
		"-v", dir+":/certs:ro", "-v", configPath+":/etc/caddy/config.json:ro", image,
		"caddy", "run", "--config", "/etc/caddy/config.json")
	output, err := start.CombinedOutput()
	if err != nil {
		t.Fatalf("start isolated Caddy: %v: %s", err, output)
	}
	containerID := strings.TrimSpace(string(output))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", containerID).Run() })
	port := func(containerPort string) string {
		out, err := exec.Command("docker", "port", containerID, containerPort).CombinedOutput()
		if err != nil {
			t.Fatalf("inspect Caddy port %s: %v: %s", containerPort, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	adminAddr, tlsAddr := port("2019/tcp"), port("8443/tcp")
	httpClient := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := httpClient.Get("http://" + adminAddr + "/config/")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("isolated Caddy did not start: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	fingerprint := func() [32]byte {
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", tlsAddr,
			&tls.Config{ServerName: host, InsecureSkipVerify: true}) // isolated self-signed fixture
		if err != nil {
			t.Fatalf("isolated TLS handshake: %v", err)
		}
		defer conn.Close()
		return sha256.Sum256(conn.ConnectionState().PeerCertificates[0].Raw)
	}
	before := fingerprint()
	writePair()
	req, err := http.NewRequest(http.MethodPost, "http://"+adminAddr+"/load", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cache-Control", "must-revalidate")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reload returned %s", resp.Status)
	}
	if after := fingerprint(); before == after {
		t.Fatal(fmt.Sprintf("real Caddy kept the old certificate after forced reload: %x", after))
	}
	secret := "isolated-redaction-sentinel"
	request, err := http.NewRequest(http.MethodGet, "https://"+tlsAddr+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Host = host
	request.Header.Set("X-Api-Key", secret)
	transport := &http.Transport{TLSClientConfig: &tls.Config{ServerName: host, InsecureSkipVerify: true}} // isolated fixture
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected isolated proxy error, got %s", response.Status)
	}
	logs, err := exec.Command("docker", "logs", containerID).CombinedOutput()
	if err != nil || !bytes.Contains(logs, []byte("http.log.error.log0")) || !bytes.Contains(logs, []byte("http.log.access.log0")) || strings.Contains(string(logs), secret) {
		t.Fatalf("Caddy error/access log redaction failed: command_error=%v", err)
	}
}
