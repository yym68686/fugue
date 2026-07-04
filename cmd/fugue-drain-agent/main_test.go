package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const tcpFixture = `  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:1F90 0100007F:C001 01 00000000:00000000 00:00000000 00000000     0        0 1 1 0000000000000000 20 4 30 10 -1
   1: 0100007F:1F91 0100007F:C002 0A 00000000:00000000 00:00000000 00000000     0        0 2 1 0000000000000000 20 4 30 10 -1
   2: 0100007F:1F92 0100007F:C003 06 00000000:00000000 00:00000000 00000000     0        0 3 1 0000000000000000 20 4 30 10 -1
   3: 0100007F:1F93 0100007F:C004 08 00000000:00000000 00:00000000 00000000     0        0 4 1 0000000000000000 20 4 30 10 -1
`

const tcp6Fixture = `  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000000000000000000001000000:2328 00000000000000000000000001000000:C001 01 00000000:00000000 00:00000000 00000000     0        0 1 1 0000000000000000 20 4 30 10 -1
`

func TestParseTCPEntriesIPv4AndStates(t *testing.T) {
	entries, err := parseTCPEntries(strings.NewReader(tcpFixture))
	if err != nil {
		t.Fatalf("parse tcp entries: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(entries))
	}
	if entries[0].LocalPort != 8080 || entries[0].State != tcpStateEstablished {
		t.Fatalf("unexpected first entry %+v", entries[0])
	}
	if !tcpStateActive(entries[0].State) {
		t.Fatal("ESTABLISHED should be active")
	}
	if tcpStateActive(entries[1].State) {
		t.Fatal("LISTEN should not be active")
	}
	if tcpStateActive(entries[2].State) {
		t.Fatal("TIME_WAIT should not be active")
	}
	if !tcpStateActive(entries[3].State) {
		t.Fatal("CLOSE_WAIT should be active")
	}
}

func TestParseTCPEntriesIPv6(t *testing.T) {
	entries, err := parseTCPEntries(strings.NewReader(tcp6Fixture))
	if err != nil {
		t.Fatalf("parse tcp6 entries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].LocalPort != 9000 || entries[0].State != tcpStateEstablished {
		t.Fatalf("unexpected tcp6 entry %+v", entries[0])
	}
}

func TestServerObserveFiltersPortsAndActiveStates(t *testing.T) {
	tcpPath := writeTempFile(t, tcpFixture)
	tcp6Path := writeTempFile(t, tcp6Fixture)
	srv := newTestServer(t, config{
		AppPorts:     map[int]struct{}{8080: {}, 8083: {}, 9000: {}},
		ProcTCPPath:  tcpPath,
		ProcTCP6Path: tcp6Path,
		Timeout:      time.Second,
		QuietPeriod:  time.Millisecond,
		PollInterval: time.Millisecond,
		FailClosed:   true,
	})
	observed, err := srv.observe()
	if err != nil {
		t.Fatalf("observe: %v", err)
	}
	if observed.Active != 3 {
		t.Fatalf("expected 3 active connections, got %+v", observed)
	}
	if observed.States["ESTABLISHED"] != 2 || observed.States["CLOSE_WAIT"] != 1 {
		t.Fatalf("unexpected states %+v", observed.States)
	}
}

func TestDrainIdleFastExitAfterQuietPeriod(t *testing.T) {
	tcpPath := writeTempFile(t, "  sl  local_address rem_address   st\n")
	srv := newTestServer(t, config{
		AppPorts:     map[int]struct{}{8080: {}},
		ProcTCPPath:  tcpPath,
		ProcTCP6Path: tcpPath,
		Timeout:      time.Second,
		QuietPeriod:  2 * time.Millisecond,
		PollInterval: time.Millisecond,
		FailClosed:   true,
	})
	result := srv.drain(context.Background())
	if result.Reason != "idle" {
		t.Fatalf("expected idle result, got %+v", result)
	}
	if result.ActiveConnections != 0 {
		t.Fatalf("expected zero active connections, got %+v", result)
	}
}

func TestDrainActiveToIdle(t *testing.T) {
	active := "  sl  local_address rem_address   st\n   0: 0100007F:1F90 0100007F:C001 01 00000000:00000000 00:00000000 00000000 0 0 1\n"
	idle := "  sl  local_address rem_address   st\n"
	path := writeTempFile(t, active)
	srv := newTestServer(t, config{
		AppPorts:     map[int]struct{}{8080: {}},
		ProcTCPPath:  path,
		ProcTCP6Path: path,
		Timeout:      time.Second,
		QuietPeriod:  time.Millisecond,
		PollInterval: time.Millisecond,
		FailClosed:   true,
	})
	calls := 0
	srv.sleep = func(ctx context.Context, d time.Duration) error {
		calls++
		if calls == 1 {
			if err := osWriteFile(path, idle); err != nil {
				t.Fatalf("write idle fixture: %v", err)
			}
		}
		return nil
	}
	result := srv.drain(context.Background())
	if result.Reason != "idle" || result.MaxActive == 0 {
		t.Fatalf("expected active-to-idle result, got %+v", result)
	}
}

func TestDrainTimeout(t *testing.T) {
	active := "  sl  local_address rem_address   st\n   0: 0100007F:1F90 0100007F:C001 01 00000000:00000000 00:00000000 00000000 0 0 1\n"
	path := writeTempFile(t, active)
	srv := newTestServer(t, config{
		AppPorts:     map[int]struct{}{8080: {}},
		ProcTCPPath:  path,
		ProcTCP6Path: path,
		Timeout:      time.Millisecond,
		QuietPeriod:  time.Hour,
		PollInterval: time.Millisecond,
		FailClosed:   true,
	})
	start := time.Unix(0, 0)
	now := start
	srv.now = func() time.Time { return now }
	srv.sleep = func(context.Context, time.Duration) error {
		now = now.Add(2 * time.Millisecond)
		return nil
	}
	result := srv.drain(context.Background())
	if result.Reason != "timeout" {
		t.Fatalf("expected timeout, got %+v", result)
	}
}

func TestDrainObserverErrorFailClosed(t *testing.T) {
	srv := newTestServer(t, config{
		AppPorts:     map[int]struct{}{8080: {}},
		ProcTCPPath:  "/path/does/not/exist",
		ProcTCP6Path: "/path/does/not/exist6",
		Timeout:      time.Millisecond,
		QuietPeriod:  time.Millisecond,
		PollInterval: time.Millisecond,
		FailClosed:   true,
	})
	start := time.Unix(0, 0)
	now := start
	srv.now = func() time.Time { return now }
	srv.sleep = func(context.Context, time.Duration) error {
		now = now.Add(2 * time.Millisecond)
		return nil
	}
	result := srv.drain(context.Background())
	if result.Reason != "timeout" || result.ObserverErrors == 0 {
		t.Fatalf("expected fail-closed timeout with observer errors, got %+v", result)
	}
}

func TestMetricsOutput(t *testing.T) {
	srv := newTestServer(t, config{AppPorts: map[int]struct{}{8080: {}}, Timeout: time.Second, QuietPeriod: time.Millisecond, PollInterval: time.Millisecond})
	srv.metrics.Active = 2
	srv.metrics.PreStopRequests = 3
	rw := httptest.NewRecorder()
	srv.handleMetrics(rw, nil)
	out := rw.Body.String()
	for _, want := range []string{"fugue_app_drain_active_connections 2", "fugue_app_drain_prestop_requests_total 3"} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q in\n%s", want, out)
		}
	}
}

func TestPreStopResponseClosesConnection(t *testing.T) {
	tcpPath := writeTempFile(t, "  sl  local_address rem_address   st\n")
	srv := newTestServer(t, config{
		AppPorts:     map[int]struct{}{8080: {}},
		ProcTCPPath:  tcpPath,
		ProcTCP6Path: tcpPath,
		Timeout:      time.Second,
		QuietPeriod:  time.Millisecond,
		PollInterval: time.Millisecond,
		FailClosed:   true,
	})
	rw := httptest.NewRecorder()
	srv.handlePreStop(rw, httptest.NewRequest("GET", "/drain/prestop", nil))
	if got := rw.Header().Get("Connection"); got != "close" {
		t.Fatalf("expected Connection: close, got %q", got)
	}
	if got := rw.Header().Get("Content-Length"); got != strconv.Itoa(rw.Body.Len()) {
		t.Fatalf("expected Content-Length %d, got %q", rw.Body.Len(), got)
	}
	var result drainResult
	if err := json.Unmarshal(rw.Body.Bytes(), &result); err != nil {
		t.Fatalf("preStop response is not JSON: %v", err)
	}
	if result.Reason != "idle" {
		t.Fatalf("expected idle response, got %+v", result)
	}
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	path := t.TempDir() + "/tcp"
	if err := osWriteFile(path, content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return path
}

func osWriteFile(path string, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

func newTestServer(t *testing.T, cfg config) *server {
	t.Helper()
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Second
	}
	if cfg.QuietPeriod == 0 {
		cfg.QuietPeriod = time.Millisecond
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = time.Millisecond
	}
	return newServer(cfg, nil)
}
