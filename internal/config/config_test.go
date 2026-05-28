package config

import "testing"

func TestDNSFromEnvDefaultsEdgeHealthProbeEnabled(t *testing.T) {
	t.Setenv("FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED", "")

	cfg := DNSFromEnv()
	if !cfg.EdgeHealthProbeEnabled {
		t.Fatal("expected DNS edge health probe to default enabled")
	}
}

func TestDNSFromEnvAllowsDisablingEdgeHealthProbe(t *testing.T) {
	t.Setenv("FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED", "false")

	cfg := DNSFromEnv()
	if cfg.EdgeHealthProbeEnabled {
		t.Fatal("expected explicit env value to disable DNS edge health probe")
	}
}
