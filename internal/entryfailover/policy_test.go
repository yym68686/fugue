package entryfailover

import (
	"testing"
	"time"
)

func testPolicy() Policy {
	return Policy{
		Schema: PolicySchema, PoolID: "sample", TenantID: "tenant-example", ProjectID: "project-example", Generation: 1, ExpiresAt: time.Now().Add(24 * time.Hour), Zone: "example.test", ZoneID: "zone",
		Hostnames: []string{"example.test", "api.example.test"},
		DNSBaseline: []Record{{ID: "a", Name: "example.test", Type: "A", Content: "192.0.2.10", TTL: 1},
			{ID: "b", Name: "api.example.test", Type: "A", Content: "192.0.2.10", TTL: 1}},
		Checks:   []Check{{"example.test", "/", 307}, {"api.example.test", "/v1/health", 200}},
		Targets:  []Target{{ID: "west", Kind: "static-ip", Address: "192.0.2.10", EdgeID: "west-edge", Priority: 10}, {ID: "managed", Kind: "fugue-domain", Address: "d-canonical.dns.fugue.test", Priority: 20}},
		Vantages: []Vantage{{ID: "local", Transport: "local"}},
		Mode:     "shadow", FailureThreshold: 3, SuccessThreshold: 2, IntervalSeconds: 10, ProbeTimeoutSecs: 4, MinTLSValidityHours: 24, Failback: "manual",
	}
}

func TestPolicyValidatesExactScopeAndSharedManagedTarget(t *testing.T) {
	p := testPolicy()
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	first := p.Digest()
	p.Targets[1].Address = "api.example.test"
	if err := p.Validate(); err == nil {
		t.Fatal("alias loop accepted")
	}
	p = testPolicy()
	p.Hostnames = append(p.Hostnames, "other.test")
	if err := p.Validate(); err == nil {
		t.Fatal("out-of-zone hostname accepted")
	}
	p = testPolicy()
	p.Checks = p.Checks[:1]
	if err := p.Validate(); err == nil {
		t.Fatal("missing API check accepted")
	}
	p = testPolicy()
	p.Mode = "automatic"
	if err := p.Validate(); err == nil {
		t.Fatal("automatic mode accepted one vantage")
	}
	p.Vantages = append(p.Vantages, Vantage{ID: "remote", Transport: "ssh", SSHHost: "remote-probe", BinaryPath: "/usr/local/bin/fugue-entry-failover", PublicKeyPath: "/etc/fugue-entry-failover/policy.pub"})
	if err := p.Validate(); err != nil || p.Digest() == first {
		t.Fatalf("valid mode change did not alter digest: %v", err)
	}
}
