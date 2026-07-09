package watchdog

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRunReportsHTTPProbeAndObserveOnlyProviderAction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	report := Run(context.Background(), Config{
		RunnerURL:    server.URL,
		Timeout:      time.Second,
		ProviderMode: ProviderActionModeObserve,
		ProviderTargets: []ProviderPowerTarget{{
			Provider:   "test-provider",
			InstanceID: "vm-1",
		}},
	}, NoopProviderPowerClient{Mode: ProviderActionModeObserve})

	if report.Pass {
		t.Fatalf("expected failed report for HTTP 503, got %+v", report)
	}
	if len(report.ProviderActions) != 1 || report.ProviderActions[0].Status != "not_executed" || report.ProviderActions[0].Mode != ProviderActionModeObserve {
		t.Fatalf("expected observe-only provider action evidence, got %+v", report.ProviderActions)
	}
	foundRunner := false
	for _, probe := range report.Probes {
		if probe.Name == "github_runner" {
			foundRunner = true
			if probe.Status != ProbeStatusFail || probe.Evidence["http_status"] != "503" {
				t.Fatalf("expected runner probe to fail with 503 evidence, got %+v", probe)
			}
		}
		if probe.Name == "authoritative_dns" && probe.Status != ProbeStatusSkipped {
			t.Fatalf("DNS probe without config should be skipped, got %+v", probe)
		}
	}
	if !foundRunner {
		t.Fatalf("expected github_runner probe in %+v", report.Probes)
	}
}

func TestParseProviderTargets(t *testing.T) {
	targets, err := ParseProviderTargets(`[{"provider":"vultr","region":"nrt","instance_id":"123","failure_domain":"provider:vultr/region:nrt"}]`)
	if err != nil {
		t.Fatalf("parse provider targets: %v", err)
	}
	if len(targets) != 1 || targets[0].Provider != "vultr" || targets[0].InstanceID != "123" {
		t.Fatalf("unexpected targets: %+v", targets)
	}
}

func TestRunReportsDBTCPProbe(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			_ = conn.Close()
		}
	}()
	report := Run(context.Background(), Config{DBTCPAddr: listener.Addr().String(), Timeout: time.Second}, nil)
	found := false
	for _, probe := range report.Probes {
		if probe.Name == "control_plane_db_tcp" {
			found = true
			if probe.Status != ProbeStatusPass {
				t.Fatalf("expected DB TCP pass, got %+v", probe)
			}
		}
	}
	if !found {
		t.Fatalf("expected DB TCP probe in %+v", report.Probes)
	}
}

func TestClassifyProviderPowerEvent(t *testing.T) {
	event := ClassifyProviderPowerEvent(
		ProviderPowerTarget{Provider: "dmit", Region: "tokyo", InstanceID: "vm-1", FailureDomain: "provider:dmit/region:tokyo"},
		ProviderActionResult{Action: "power_on", ActionID: "act-1", Status: "completed", RecordedAt: time.Date(2026, 7, 9, 10, 0, 0, 0, time.UTC)},
		"poweroff",
		"hypervisor stopped instance",
		time.Date(2026, 7, 9, 9, 0, 0, 0, time.UTC),
	)
	if event.EventClass != "provider_unplanned" || event.ActionID != "act-1" || event.Evidence["failure_domain"] == "" {
		t.Fatalf("unexpected provider event classification: %+v", event)
	}
	missing := ClassifyProviderPowerEvent(ProviderPowerTarget{}, ProviderActionResult{}, "", "", time.Time{})
	if missing.EventClass != "no_provider_evidence" {
		t.Fatalf("expected no provider evidence, got %+v", missing)
	}
}
