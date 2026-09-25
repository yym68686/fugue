package entryfailover

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeCollector struct {
	health map[string]bool
	miss   map[string]bool
	now    *time.Time
}

func (f *fakeCollector) Collect(_ context.Context, _ Policy, target Target, vantage Vantage) (ProbeResult, error) {
	if f.miss[vantage.ID] {
		return ProbeResult{}, errors.New("vantage unavailable")
	}
	return ProbeResult{TargetID: target.ID, Healthy: f.health[target.ID], At: *f.now}, nil
}

func TestAutomaticFailoverRequiresTwoVantagesAndManualFailback(t *testing.T) {
	exec, dns := newExecutorTest(t)
	exec.Policy.Mode = "automatic"
	exec.Policy.Vantages = append(exec.Policy.Vantages, Vantage{ID: "second", Transport: "ssh", SSHHost: "other-vantage", BinaryPath: "/usr/local/bin/fugue-entry-failover", PublicKeyPath: "/etc/fugue-entry-failover/policy.pub"})
	now := time.Now().UTC()
	probe := &fakeCollector{health: map[string]bool{"west": true, "managed": true}, miss: map[string]bool{}, now: &now}
	engine := Engine{Executor: exec, Collector: probe, Clock: func() time.Time { return now }}
	cycle := func() Decision {
		t.Helper()
		got, err := engine.Cycle(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		now = now.Add(10 * time.Second)
		return got
	}
	if got := cycle(); got.Reason != "current_healthy_manual_failback" {
		t.Fatalf("initial=%+v", got)
	}
	probe.health["west"] = false
	for i := 0; i < 2; i++ {
		if got := cycle(); got.Reason != "current_failure_unconfirmed" {
			t.Fatalf("early=%+v", got)
		}
	}
	got := cycle()
	if got.Reason != "switched" || got.Selected != "managed" || dns.writes != 1 {
		t.Fatalf("failover=%+v writes=%d", got, dns.writes)
	}
	probe.health["west"] = true
	for i := 0; i < 4; i++ {
		got = cycle()
	}
	if got.Selected != "managed" || dns.writes != 1 {
		t.Fatalf("manual failback violated: %+v writes=%d", got, dns.writes)
	}
}

func TestAutomaticFailoverDoesNotInferFailureFromMissingVantage(t *testing.T) {
	exec, dns := newExecutorTest(t)
	exec.Policy.Mode = "automatic"
	exec.Policy.Vantages = append(exec.Policy.Vantages, Vantage{ID: "second", Transport: "ssh", SSHHost: "other-vantage", BinaryPath: "/usr/local/bin/fugue-entry-failover", PublicKeyPath: "/etc/fugue-entry-failover/policy.pub"})
	now := time.Now().UTC()
	probe := &fakeCollector{health: map[string]bool{"west": false, "managed": true}, miss: map[string]bool{"second": true}, now: &now}
	engine := Engine{Executor: exec, Collector: probe, Clock: func() time.Time { return now }}
	for i := 0; i < 5; i++ {
		got, err := engine.Cycle(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got.Selected != "west" || got.Health["west"] != "unknown" {
			t.Fatalf("missing vantage: %+v", got)
		}
		now = now.Add(10 * time.Second)
	}
	if dns.writes != 0 {
		t.Fatalf("DNS written with one vantage: %d", dns.writes)
	}
}

func TestThreeVantageQuorumAllowsOneDisagreementButNotTwo(t *testing.T) {
	for _, tc := range []struct {
		name string
		good int
		want string
	}{
		{name: "two good one bad", good: 2, want: "healthy"},
		{name: "one good two bad", good: 1, want: "unhealthy"},
		{name: "one good one bad one missing", good: 1, want: "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var results []ProbeResult
			for i := 0; i < 3; i++ {
				if tc.name == "one good one bad one missing" && i == 2 {
					continue
				}
				results = append(results, ProbeResult{Healthy: i < tc.good})
			}
			missing := 0
			if tc.name == "one good one bad one missing" {
				missing = 1
			}
			if got := summarize(results, missing, 2); got != tc.want {
				t.Fatalf("quorum=%q want %q", got, tc.want)
			}
		})
	}
}
