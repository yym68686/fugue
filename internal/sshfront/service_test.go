package sshfront

import (
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestReconcileAddsProxiesAndRemovesListeners(t *testing.T) {
	t.Parallel()

	targetAddr, stopTarget := startEchoTarget(t)
	defer stopTarget()
	publicPort := freeTCPPort(t)
	targetHost, targetPortText, err := net.SplitHostPort(targetAddr)
	if err != nil {
		t.Fatalf("split target addr: %v", err)
	}
	targetPort, err := strconv.Atoi(targetPortText)
	if err != nil {
		t.Fatalf("parse target port: %v", err)
	}

	service := NewService(Config{}, log.New(io.Discard, "", 0))
	cfg := Config{
		ListenHost:      "127.0.0.1",
		PublicPortStart: publicPort,
		PublicPortEnd:   publicPort,
		DialTimeout:     time.Second,
	}
	route := model.EdgeSSHRoute{
		AppID:      "app_demo",
		TenantID:   "tenant_demo",
		PublicPort: publicPort,
		TargetHost: targetHost,
		TargetPort: targetPort,
		Status:     model.AppSSHEndpointStatusReady,
	}
	if err := service.reconcile(cfg, []model.EdgeSSHRoute{route}); err != nil {
		t.Fatalf("reconcile add route: %v", err)
	}
	assertProxyEcho(t, publicPort, "hello")

	if err := service.reconcile(cfg, nil); err != nil {
		t.Fatalf("reconcile remove route: %v", err)
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)), 100*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected listener to be removed")
	}
}

func TestLoadCachedBundleReconcilesLastKnownGoodRoutes(t *testing.T) {
	t.Parallel()

	targetAddr, stopTarget := startEchoTarget(t)
	defer stopTarget()
	publicPort := freeTCPPort(t)
	targetHost, targetPortText, err := net.SplitHostPort(targetAddr)
	if err != nil {
		t.Fatalf("split target addr: %v", err)
	}
	targetPort, err := strconv.Atoi(targetPortText)
	if err != nil {
		t.Fatalf("parse target port: %v", err)
	}
	cachePath := filepath.Join(t.TempDir(), "ssh-routes.json")
	bundle := model.EdgeSSHRouteBundle{
		Version:     "test",
		GeneratedAt: time.Now().UTC(),
		ValidUntil:  time.Now().UTC().Add(time.Hour),
		Routes: []model.EdgeSSHRoute{
			{
				AppID:      "app_demo",
				TenantID:   "tenant_demo",
				PublicPort: publicPort,
				TargetHost: targetHost,
				TargetPort: targetPort,
				Status:     model.AppSSHEndpointStatusReady,
			},
		},
	}
	cfg := Config{
		CachePath:       cachePath,
		ListenHost:      "127.0.0.1",
		PublicPortStart: publicPort,
		PublicPortEnd:   publicPort,
		DialTimeout:     time.Second,
	}
	if err := writeCachedBundle(cfg, bundle); err != nil {
		t.Fatalf("write cache: %v", err)
	}

	service := NewService(Config{}, log.New(io.Discard, "", 0))
	if err := service.loadCachedBundle(cfg); err != nil {
		t.Fatalf("load cache: %v", err)
	}
	assertProxyEcho(t, publicPort, "cached")
	service.closeAllListeners()
}

func TestReconcileSkipsRoutesOutsidePortRange(t *testing.T) {
	t.Parallel()

	publicPort := freeTCPPort(t)
	service := NewService(Config{}, log.New(io.Discard, "", 0))
	cfg := Config{
		ListenHost:      "127.0.0.1",
		PublicPortStart: publicPort + 1,
		PublicPortEnd:   publicPort + 1,
		DialTimeout:     time.Second,
	}
	route := model.EdgeSSHRoute{
		AppID:      "app_demo",
		TenantID:   "tenant_demo",
		PublicPort: publicPort,
		TargetHost: "127.0.0.1",
		TargetPort: 22,
		Status:     model.AppSSHEndpointStatusReady,
	}
	if err := service.reconcile(cfg, []model.EdgeSSHRoute{route}); err != nil {
		t.Fatalf("reconcile skipped route: %v", err)
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)), 100*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected route outside public range to have no listener")
	}
}

func TestDialFailureIncrementsMetricAndClosesConnection(t *testing.T) {
	t.Parallel()

	publicPort := freeTCPPort(t)
	targetPort := freeTCPPort(t)
	service := NewService(Config{}, log.New(io.Discard, "", 0))
	cfg := Config{
		ListenHost:      "127.0.0.1",
		PublicPortStart: publicPort,
		PublicPortEnd:   publicPort,
		DialTimeout:     50 * time.Millisecond,
	}
	route := model.EdgeSSHRoute{
		AppID:      "app_demo",
		TenantID:   "tenant_demo",
		PublicPort: publicPort,
		TargetHost: "127.0.0.1",
		TargetPort: targetPort,
		Status:     model.AppSSHEndpointStatusReady,
	}
	if err := service.reconcile(cfg, []model.EdgeSSHRoute{route}); err != nil {
		t.Fatalf("reconcile route: %v", err)
	}
	defer service.closeAllListeners()

	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)), time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	_ = conn.Close()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		service.mu.Lock()
		dialErrors := service.dialErrors
		active := service.activeConns
		service.mu.Unlock()
		if dialErrors > 0 && active == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	t.Fatalf("expected dial error metric and released connection, dial_errors=%d active=%d", service.dialErrors, service.activeConns)
}

func TestAcquireConnectionAppliesPerIPRateLimit(t *testing.T) {
	t.Parallel()

	service := NewService(Config{}, log.New(io.Discard, "", 0))
	cfg := Config{MaxConnectionAttemptsPerIPPerMinute: 1}
	route := model.EdgeSSHRoute{AppID: "app_demo", TenantID: "tenant_demo"}
	remote := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}

	release, ok := service.acquireConnection(cfg, route, remote)
	if !ok {
		t.Fatal("expected first connection attempt to be accepted")
	}
	release()

	if _, ok := service.acquireConnection(cfg, route, remote); ok {
		t.Fatal("expected second connection attempt in the same rate window to be rejected")
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.limitRejects != 1 {
		t.Fatalf("expected one limit rejection, got %d", service.limitRejects)
	}
}

func TestFetchBundleKeepsTokenOutOfError(t *testing.T) {
	t.Parallel()

	service := NewService(Config{}, log.New(io.Discard, "", 0))
	cfg := Config{
		APIURL:       "http://127.0.0.1:1",
		EdgeToken:    "secret-token",
		HTTPTimeout:  50 * time.Millisecond,
		ListenHost:   "127.0.0.1",
		DialTimeout:  time.Second,
		SyncInterval: time.Hour,
	}
	service.syncAndLog(context.Background(), cfg)
	service.mu.Lock()
	defer service.mu.Unlock()
	if strings.Contains(service.lastError, cfg.EdgeToken) {
		t.Fatalf("sync error leaked token: %s", service.lastError)
	}
}

func startEchoTarget(t *testing.T) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return listener.Addr().String(), func() {
		_ = listener.Close()
		<-done
	}
}

func assertProxyEcho(t *testing.T, publicPort int, message string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)), time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(message + "\n")); err != nil {
		t.Fatalf("write proxy: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read proxy echo: %v", err)
	}
	if strings.TrimSpace(line) != message {
		t.Fatalf("expected echo %q, got %q", message, line)
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen free port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
