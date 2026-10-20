package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

func TestDNSReadinessBlackholeDoesNotStarveHealthyRoutes(t *testing.T) {
	plan := &platformconfig.DNSReadinessPlan{}
	for index := 0; index < 671; index++ {
		address := "192.0.2.1"
		if index >= 171 {
			address = fmt.Sprintf("192.0.2.%d", 2+index%3)
		}
		plan.Probes = append(plan.Probes, platformconfig.DNSReadinessProbe{ID: fmt.Sprint(index), Hostname: fmt.Sprintf("route-%d.example.test", index), Address: address})
	}
	policy := &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 4096}
	var active, peak, healthy atomic.Int32
	started := time.Now()
	facts := collectDNSReadinessFacts(context.Background(), plan, policy, func(ctx context.Context, _, _, address, _ string, timeout time.Duration) (routeprobe.Proof, error) {
		count := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); count > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, count) {
				break
			}
		}
		delay := 200 * time.Millisecond
		if address == "192.0.2.1" {
			delay = timeout
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return routeprobe.Proof{}, ctx.Err()
		case <-timer.C:
		}
		if address == "192.0.2.1" {
			return routeprobe.Proof{}, errors.Join(routeprobe.ErrConnectionUnavailable, routeprobe.ErrUnavailable)
		}
		healthy.Add(1)
		return routeprobe.Proof{}, errors.New("synthetic route rejection")
	})
	if healthy.Load() != 500 || peak.Load() > 8 || time.Since(started) >= 30*time.Second {
		t.Fatalf("healthy routes starved: completed=%d peak=%d duration=%s", healthy.Load(), peak.Load(), time.Since(started))
	}
	for _, fact := range facts[171:] {
		if fact.Reason != "probe_failed" || fact.Ready {
			t.Fatal("route observation missing or fabricated", fact)
		}
	}
}

func TestDNSReadinessReusesCapacityAfterAnotherTargetCompletes(t *testing.T) {
	plan := &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{{ID: "fast", Address: "192.0.2.1"}}}
	for index := 0; index < 8; index++ {
		plan.Probes = append(plan.Probes, platformconfig.DNSReadinessProbe{ID: fmt.Sprint(index), Address: "192.0.2.2"})
	}
	policy := &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 1, MaxConcurrency: 8}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	gate := make(chan struct{})
	var entered atomic.Int32
	collectDNSReadinessFacts(ctx, plan, policy, func(ctx context.Context, _, _, address, _ string, _ time.Duration) (routeprobe.Proof, error) {
		if address == "192.0.2.2" {
			if entered.Add(1) == 8 {
				close(gate)
			}
			select {
			case <-ctx.Done():
			case <-gate:
			}
		}
		return routeprobe.Proof{}, errors.New("synthetic rejection")
	})
	if ctx.Err() != nil || entered.Load() != 8 {
		t.Fatal("completed target kept capacity reserved", entered.Load())
	}
}

func TestDNSReadinessRetainsBoundedNonSensitiveFailureObservations(t *testing.T) {
	plan, policy := readinessTestPlan()
	path := filepath.Join(t.TempDir(), "cache")
	scheduler := (&dnsReadinessScheduler{}).bind(model.PlatformConsumerAssignment{FencingToken: 1}, nil, path)
	probe := func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		return routeprobe.Proof{}, errors.New("https://private.example.test private-token")
	}
	for cycle := 0; cycle < 10; cycle++ {
		collectDNSReadinessFacts(context.Background(), &plan, &policy, probe, scheduler)
	}
	raw, err := os.ReadFile(path + ".probe-observations.json")
	if err != nil || strings.Contains(string(raw), "private") {
		t.Fatal("failure diagnostics missing or containing upstream details", err)
	}
	var observations []map[string]any
	if json.Unmarshal(raw, &observations) != nil || len(observations) != 8 {
		t.Fatal("failure history is not bounded")
	}
	stat, err := os.Stat(path + ".probe-observations.json")
	if err != nil || stat.Mode().Perm() != 0600 {
		t.Fatal("failure observation permissions are not private", err)
	}
	scheduler.path = path + "/unwritable/observation.json"
	if err := os.WriteFile(path, []byte("block directory creation"), 0600); err != nil {
		t.Fatal(err)
	}
	facts := collectDNSReadinessFacts(context.Background(), &plan, &policy, probe, scheduler)
	for _, fact := range facts {
		if fact.Ready || fact.Reason != "probe_failed" {
			t.Fatal("diagnostic persistence failure changed runtime facts", fact)
		}
	}
}

func TestDNSReadinessProgressSurvivesShortObservationWindows(t *testing.T) {
	plan := &platformconfig.DNSReadinessPlan{}
	for index := 0; index < 4; index++ {
		plan.Probes = append(plan.Probes, platformconfig.DNSReadinessProbe{ID: fmt.Sprint(index), Hostname: fmt.Sprint(index), Address: "192.0.2.1"})
	}
	policy := &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, MaxConcurrency: 1}
	scheduler := &dnsReadinessScheduler{}
	seen := map[string]int{}
	var mutex sync.Mutex
	for cycle := 0; cycle < 4; cycle++ {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		collectDNSReadinessFacts(ctx, plan, policy, func(ctx context.Context, host, _, _, _ string, _ time.Duration) (routeprobe.Proof, error) {
			mutex.Lock()
			seen[host]++
			mutex.Unlock()
			<-ctx.Done()
			return routeprobe.Proof{}, routeprobe.ErrUnavailable
		}, scheduler)
		cancel()
	}
	for _, requirement := range plan.Probes {
		if seen[requirement.Hostname] != 1 {
			t.Fatal("repeated queue head starved another requirement", seen)
		}
	}
}

func TestDNSReadinessBackoffIsConnectionScopedAndAssignmentBound(t *testing.T) {
	plan, policy := readinessTestPlan()
	scheduler := &dnsReadinessScheduler{}
	assignment := model.PlatformConsumerAssignment{FencingToken: 1}
	var unavailable atomic.Int32
	probe := func(_ context.Context, _, _, address, _ string, _ time.Duration) (routeprobe.Proof, error) {
		if address == plan.Probes[0].Address {
			unavailable.Add(1)
			return routeprobe.Proof{}, errors.Join(routeprobe.ErrConnectionUnavailable, routeprobe.ErrUnavailable)
		}
		return routeprobe.Proof{}, errors.New("route rejected")
	}
	collectDNSReadinessFacts(context.Background(), &plan, &policy, probe, scheduler.bind(assignment, nil))
	first := unavailable.Load()
	if first < 1 || first > int32(policy.MaxConcurrency) {
		t.Fatal("failed physical target exceeded bounded in-flight attempts", first)
	}
	facts := collectDNSReadinessFacts(context.Background(), &plan, &policy, probe, scheduler.bind(assignment, nil))
	if unavailable.Load() != first || facts[2].Reason != "probe_failed" || facts[3].Reason != "probe_failed" {
		t.Fatal("backoff failed or route rejection disabled a different hostname", facts)
	}
	assignment.FencingToken++
	collectDNSReadinessFacts(context.Background(), &plan, &policy, probe, scheduler.bind(assignment, nil))
	if unavailable.Load() <= first || unavailable.Load() > first+int32(policy.MaxConcurrency) {
		t.Fatal("new assignment inherited old scheduling state")
	}
	scheduler.targets[plan.Probes[0].Address] = dnsProbeTargetState{retryAt: time.Now().Add(-time.Second), failures: 4}
	var recovered atomic.Int32
	collectDNSReadinessFacts(context.Background(), &plan, &policy, func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		recovered.Add(1)
		return routeprobe.Proof{}, errors.New("reachable route rejected")
	}, scheduler)
	if recovered.Load() != 4 || scheduler.targets[plan.Probes[0].Address].failures != 0 {
		t.Fatal("recovered connection did not resume independent route checks")
	}
}

func TestDNSReadinessRefreshPrioritizesExpiryWithoutStarvingUnobservedOrAgedWork(t *testing.T) {
	plan := &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{
		{ID: "comfortable", Hostname: "comfortable.example.test", Address: "192.0.2.1"},
		{ID: "expiring", Hostname: "expiring.example.test", Address: "192.0.2.1"},
		{ID: "waiting", Hostname: "waiting.example.test", Address: "192.0.2.1"},
	}}
	policy := &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, MaxConcurrency: 1}
	for _, scenario := range []string{"expiry", "never observed", "aged"} {
		t.Run(scenario, func(t *testing.T) {
			scheduler := &dnsReadinessScheduler{}
			scheduler.collect(context.Background(), plan, policy, func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
				return routeprobe.Proof{}, errors.New("synthetic rejection")
			})
			now := time.Now()
			for _, requirement := range plan.Probes {
				scheduler.attempts[requirement.ID] = now.Add(-time.Second)
				scheduler.expires[requirement.ID] = now.Add(2 * time.Minute)
			}
			want := "expiring.example.test"
			if scenario == "never observed" {
				delete(scheduler.attempts, "waiting")
				want = "waiting.example.test"
			}
			if scenario == "aged" {
				scheduler.attempts["waiting"] = now.Add(-time.Minute)
				want = "waiting.example.test"
			}
			if scenario == "expiry" {
				scheduler.expires["expiring"] = now.Add(5 * time.Second)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first := ""
			scheduler.collect(ctx, plan, policy, func(_ context.Context, hostname, _, _, _ string, _ time.Duration) (routeprobe.Proof, error) {
				first = hostname
				cancel()
				return routeprobe.Proof{}, errors.New("synthetic rejection")
			})
			if first != want {
				t.Fatal("expiry scheduling lost bounded fairness", scenario, first, want)
			}
		})
	}
}

func TestDNSReadinessRefreshesProofNearExpiryBeforeUnobservedRoutes(t *testing.T) {
	started := time.Now().UTC()
	plan := &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{
		{ID: "unobserved", Hostname: "unobserved.example.test", Address: "192.0.2.1"},
		{ID: "primary", Hostname: "primary.example.test", Address: "192.0.2.1"},
		{ID: "comfortable", Hostname: "comfortable.example.test", Address: "192.0.2.1"},
	}}
	policy := &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, MaxConcurrency: 1}
	scheduler := &dnsReadinessScheduler{targets: map[string]dnsProbeTargetState{}}
	scheduler.digest, _ = platformconfig.Digest(struct {
		Scope  string
		Plan   *platformconfig.DNSReadinessPlan
		Policy *platformconfig.DNSReadinessPolicy
	}{scheduler.scope, plan, policy})
	scheduler.attempts = map[string]time.Time{
		"primary":     started.Add(-110 * time.Second),
		"comfortable": started.Add(-20 * time.Second),
	}
	scheduler.expires = map[string]time.Time{
		"primary":     started.Add(10 * time.Second),
		"comfortable": started.Add(10 * time.Minute),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := ""
	scheduler.collect(ctx, plan, policy, func(_ context.Context, hostname, _, _, _ string, _ time.Duration) (routeprobe.Proof, error) {
		first = hostname
		cancel()
		return routeprobe.Proof{}, errors.New("synthetic probe")
	})
	if first != "primary.example.test" {
		t.Fatalf("readiness scheduler spent its bounded window before refreshing the expiring primary: got %q", first)
	}
}

func TestDNSReadinessReportsBudgetExhaustionAndBindsEveryFactToSweep(t *testing.T) {
	plan := &platformconfig.DNSReadinessPlan{}
	for index := 0; index < 8; index++ {
		plan.Probes = append(plan.Probes, platformconfig.DNSReadinessProbe{ID: fmt.Sprint(index), Address: fmt.Sprintf("192.0.2.%d", 1+index%2)})
	}
	policy := &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, MaxConcurrency: 2}
	path := filepath.Join(t.TempDir(), "cache")
	scheduler := (&dnsReadinessScheduler{}).bind(model.PlatformConsumerAssignment{FencingToken: 1}, nil, path)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	facts := scheduler.collect(ctx, plan, policy, func(ctx context.Context, _, _, _, _ string, _ time.Duration) (routeprobe.Proof, error) {
		<-ctx.Done()
		return routeprobe.Proof{}, ctx.Err()
	})
	raw, err := os.ReadFile(path + ".probe-observations.json")
	if err != nil {
		t.Fatal(err)
	}
	var observations []struct {
		ObservationID   string `json:"observation_id"`
		BudgetExhausted bool   `json:"budget_exhausted"`
		Unobserved      int    `json:"unobserved"`
	}
	if json.Unmarshal(raw, &observations) != nil || len(observations) != 1 || !observations[0].BudgetExhausted || observations[0].Unobserved != 6 || observations[0].ObservationID == "" {
		t.Fatal("bounded exhaustion has no explicit causal observation", string(raw))
	}
	for _, fact := range facts {
		if fact.Ready || fact.ObservationID != observations[0].ObservationID {
			t.Fatal("diagnostic sweep fabricated readiness or lost fact association", fact)
		}
	}
}
