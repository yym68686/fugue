package dnsserver

import (
	"context"
	"errors"
	"fmt"
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
	if first != 1 {
		t.Fatal("failed physical target consumed repeated attempts", first)
	}
	facts := collectDNSReadinessFacts(context.Background(), &plan, &policy, probe, scheduler.bind(assignment, nil))
	if unavailable.Load() != first || facts[2].Reason != "probe_failed" || facts[3].Reason != "probe_failed" {
		t.Fatal("backoff failed or route rejection disabled a different hostname", facts)
	}
	assignment.FencingToken++
	collectDNSReadinessFacts(context.Background(), &plan, &policy, probe, scheduler.bind(assignment, nil))
	if unavailable.Load() != first+1 {
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
