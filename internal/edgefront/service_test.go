package edgefront

import (
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestFrontProxiesHTTPSConnectionsToActiveSlot(t *testing.T) {
	slotA := tcpEchoServer(t, "a")
	slotB := tcpEchoServer(t, "b")
	activeFile := t.TempDir() + "/active-slot"
	if err := os.WriteFile(activeFile, []byte("b\n"), 0o600); err != nil {
		t.Fatalf("write active slot: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen front: %v", err)
	}
	frontAddr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close front listener: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := NewService(Config{
		HTTPSListenAddr: frontAddr,
		HTTPMode:        HTTPModeDisabled,
		ActiveSlotFile:  activeFile,
		DefaultSlot:     "a",
		DialTimeout:     time.Second,
		Slots: map[string]SlotTargets{
			"a": {HTTPSAddress: slotA},
			"b": {HTTPSAddress: slotB},
		},
	}, log.New(io.Discard, "", 0))
	errCh := make(chan error, 1)
	go func() { errCh <- service.Run(ctx) }()
	waitForTCP(t, frontAddr)

	got := roundTripTCP(t, frontAddr, "x")
	if got != "b:x" {
		t.Fatalf("expected slot b response, got %q", got)
	}
	if err := os.WriteFile(activeFile, []byte("a\n"), 0o600); err != nil {
		t.Fatalf("switch active slot: %v", err)
	}
	got = roundTripTCP(t, frontAddr, "y")
	if got != "a:y" {
		t.Fatalf("expected slot a response after switch, got %q", got)
	}

	cancel()
	if err := <-errCh; err != nil && err != context.Canceled {
		t.Fatalf("front exited with error: %v", err)
	}
}

func TestFrontWritesProxyProtocolHeaderWhenEnabled(t *testing.T) {
	backend := tcpProxyProtocolCaptureServer(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen front: %v", err)
	}
	frontAddr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close front listener: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := NewService(Config{
		HTTPSListenAddr: frontAddr,
		HTTPMode:        HTTPModeDisabled,
		DefaultSlot:     "a",
		DialTimeout:     time.Second,
		ProxyProtocol:   true,
		Slots: map[string]SlotTargets{
			"a": {HTTPSAddress: backend},
			"b": {HTTPSAddress: backend},
		},
	}, log.New(io.Discard, "", 0))
	errCh := make(chan error, 1)
	go func() { errCh <- service.Run(ctx) }()
	waitForTCP(t, frontAddr)

	got := roundTripTCP(t, frontAddr, "payload")
	if !strings.HasPrefix(got, "PROXY TCP4 127.0.0.1 127.0.0.1 ") {
		t.Fatalf("expected PROXY TCP4 header, got %q", got)
	}
	if !strings.HasSuffix(got, "\r\n|payload") {
		t.Fatalf("expected proxied payload after header, got %q", got)
	}

	cancel()
	if err := <-errCh; err != nil && err != context.Canceled {
		t.Fatalf("front exited with error: %v", err)
	}
}

func TestFrontHealthServerExposesTCPDebugAndMetrics(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen health: %v", err)
	}
	healthAddr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close health listener: %v", err)
	}
	frontListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen front: %v", err)
	}
	frontAddr := frontListener.Addr().String()
	if err := frontListener.Close(); err != nil {
		t.Fatalf("close front listener: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := NewService(Config{
		HealthAddr:         healthAddr,
		HTTPSListenAddr:    frontAddr,
		HTTPMode:           HTTPModeDisabled,
		DefaultSlot:        "a",
		EdgeID:             "edge_123",
		EdgeGroupID:        "edge-group-us",
		NodeHost:           "10.0.0.1",
		ProcNetSNMPPath:    "/path/does/not/exist/snmp",
		ProcNetNetstatPath: "/path/does/not/exist/netstat",
		Slots: map[string]SlotTargets{
			"a": {HTTPSAddress: "127.0.0.1:1"},
			"b": {HTTPSAddress: "127.0.0.1:2"},
		},
	}, log.New(io.Discard, "", 0))
	errCh := make(chan error, 1)
	go func() { errCh <- service.Run(ctx) }()
	waitForTCP(t, healthAddr)

	resp, err := http.Get("http://" + healthAddr + "/metrics")
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	metrics := string(body)
	for _, want := range []string{
		"fugue_edge_front_info",
		`edge_id="edge_123"`,
		"fugue_edge_node_tcp_proc_read_error",
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, metrics)
		}
	}

	resp, err = http.Get("http://" + healthAddr + "/edge/tcp-connections")
	if err != nil {
		t.Fatalf("get tcp connections: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(body), `"active":[]`) {
		t.Fatalf("unexpected tcp connections body %s", string(body))
	}

	resp, err = http.Get("http://" + healthAddr + "/edge/tcp-capture-hints?remote=203.0.113.10:45678")
	if err != nil {
		t.Fatalf("get tcp capture hints: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(body), `"bpf_filter":"tcp and host 203.0.113.10 and port 45678"`) {
		t.Fatalf("unexpected capture hints body %s", string(body))
	}

	cancel()
	if err := <-errCh; err != nil && err != context.Canceled {
		t.Fatalf("front exited with error: %v", err)
	}
}

func TestFrontRedirectsHTTPToHTTPS(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen front: %v", err)
	}
	frontAddr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close front listener: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := NewService(Config{
		HTTPListenAddr: frontAddr,
		HTTPMode:       HTTPModeRedirect,
		DefaultSlot:    "a",
		Slots: map[string]SlotTargets{
			"a": {HTTPSAddress: "127.0.0.1:18443"},
			"b": {HTTPSAddress: "127.0.0.1:28443"},
		},
	}, log.New(io.Discard, "", 0))
	errCh := make(chan error, 1)
	go func() { errCh <- service.Run(ctx) }()
	waitForTCP(t, frontAddr)

	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: time.Second,
	}
	req, err := http.NewRequest(http.MethodGet, "http://"+frontAddr+"/path?q=1", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Host = "app.example.com"
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("send request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPermanentRedirect {
		t.Fatalf("expected 308 redirect, got %d", resp.StatusCode)
	}
	if location := resp.Header.Get("Location"); location != "https://app.example.com/path?q=1" {
		t.Fatalf("unexpected redirect location %q", location)
	}

	cancel()
	if err := <-errCh; err != nil && err != context.Canceled {
		t.Fatalf("front exited with error: %v", err)
	}
}

func tcpProxyProtocolCaptureServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen capture: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				line, _ := reader.ReadString('\n')
				data, _ := io.ReadAll(reader)
				_, _ = io.WriteString(conn, line+"|"+string(data))
			}()
		}
	}()
	return listener.Addr().String()
}

func tcpEchoServer(t *testing.T, prefix string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen echo: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				data, _ := io.ReadAll(conn)
				_, _ = io.WriteString(conn, prefix+":"+string(data))
			}()
		}
	}()
	return listener.Addr().String()
}

func roundTripTCP(t *testing.T, addr string, body string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial front: %v", err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, body); err != nil {
		t.Fatalf("write front: %v", err)
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.CloseWrite()
	}
	data, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read front: %v", err)
	}
	return string(data)
}

func waitForTCP(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %v", addr, lastErr)
}

func TestNormalizeSlot(t *testing.T) {
	for _, value := range []string{"a", "slot-a", "worker-a", " A "} {
		if got := normalizeSlot(value); got != "a" {
			t.Fatalf("normalizeSlot(%q) = %q", value, got)
		}
	}
	if got := normalizeSlot("slot-c"); got != "" || strings.TrimSpace(got) != "" {
		t.Fatalf("expected invalid slot to normalize empty, got %q", got)
	}
}
