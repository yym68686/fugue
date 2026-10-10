package api

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDynamicCaptureBudgetDoesNotWaitForEveryQueuedDomain(t *testing.T) {
	jobs := make([]dynamicQualityJob, 128)
	ctx, cancel := context.WithCancel(context.Background())
	var started atomic.Int32
	ready := make(chan struct{})
	go func() {
		<-ready
		cancel()
	}()
	completed := captureDynamicQualityJobs(ctx, jobs, 2, func(captureContext context.Context, job dynamicQualityJob) {
		if deadline, ok := captureContext.Deadline(); !ok || time.Until(deadline) > 8*time.Second {
			t.Error("per-query capture deadline missing")
		}
		if started.Add(1) == 2 {
			close(ready)
		}
		<-captureContext.Done()
	})
	if completed != 2 || started.Load() != 2 {
		t.Fatal("expired capture budget started additional domain work", completed, started.Load())
	}
	if completed := captureDynamicQualityJobs(ctx, jobs, 8, func(context.Context, dynamicQualityJob) {
		t.Error("cancelled budget ran another probe")
	}); completed != 0 {
		t.Fatal(completed)
	}
}

func TestDynamicQualityCoversFutureDomainsAndPreservesStaticPinned(t *testing.T) {
	projection, nodes, policy, now := directQueryFixture()
	policy.ECSEnabled, policy.ExplorationPercent = false, 0
	policy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
	quality := edgequality.DefaultNetworkPolicy()
	quality.Version = edgequality.DeliveryNetworkPolicyVersion
	policy.DynamicQuality = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: quality, RefreshQueriesPerCycle: 4, RefreshConcurrency: 2}
	projection.Intent.DNS = append(projection.Intent.DNS, platformconfig.DNSIntent{Hostname: "static.example.test", Type: "A", Values: []string{"8.8.4.4"}, TTL: 300})
	if err := projectDirectDNSQueries(&projection, policy, nodes, now); err != nil {
		t.Fatal(err)
	}
	jobs, states, err := dynamicQualityRoutes(projection, policy)
	if err != nil || len(jobs) != 1 || states["static.example.test"] != "static_constraint" || jobs[0].Route.Hostname != "app.example.test" {
		t.Fatal(jobs, states, err)
	}
	projection.Intent.Routes[0].EdgeGroupMode = model.PlatformRouteEdgeGroupModePinned
	projection.Intent.Routes[0].EdgeGroupID = "edge-group-a"
	jobs, states, err = dynamicQualityRoutes(projection, policy)
	if err != nil || len(jobs) != 0 || states["app.example.test"] != "pinned_constraint" {
		t.Fatal(jobs, states, err)
	}
}

func TestDynamicQualityAliasesUseDeclaredServiceOwner(t *testing.T) {
	projection, nodes, policy, now := directQueryFixture()
	policy.ECSEnabled, policy.ExplorationPercent = false, 0
	policy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
	policy.DynamicQuality = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: edgequality.DefaultDeliveryNetworkPolicy(), RefreshQueriesPerCycle: 8, RefreshConcurrency: 2}
	record := projection.Intent.DNS[0]
	record.Hostname = "alias.example.test"
	record.Type = "FUGUE_ROUTE"
	record.Values = nil
	record.Application = nil
	record.Route = &platformconfig.DNSRouteIntent{DNSApplicationIntent: platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}, Hostnames: []string{"app.example.test"}}
	projection.Intent.DNS = append(projection.Intent.DNS, record)
	if err := projectDirectDNSQueries(&projection, policy, nodes, now); err != nil {
		t.Fatal(err)
	}
	jobs, states, err := dynamicQualityRoutes(projection, policy)
	if err != nil || len(jobs) != 2 || states[record.Hostname] != "learning_queued" {
		t.Fatal(jobs, states, err)
	}
	for _, job := range jobs {
		if job.EvidenceHostname != "app.example.test" {
			t.Fatal("alias fabricated a service route for its DNS-only name", job)
		}
	}
}

func TestDynamicQualityRefreshRotatesEvenWithoutTraffic(t *testing.T) {
	state := dynamicQualityState{}
	jobs := []dynamicQualityJob{}
	for index := 0; index < 13; index++ {
		jobs = append(jobs, dynamicQualityJob{Key: fmt.Sprintf("query-%02d", index)})
	}
	seen := map[string]int{}
	now := time.Now()
	for round := 0; round < 4; round++ {
		for _, job := range state.schedule(jobs, 4, now.Add(time.Duration(round)*time.Minute)) {
			seen[job.Key]++
			state.started(job.Key, now.Add(time.Duration(round)*time.Minute))
		}
	}
	if len(seen) != 13 {
		t.Fatal("quiet domains were permanently starved", seen)
	}
	state.schedule(jobs[:2], 1, now.Add(time.Hour))
	if len(state.entries) != 2 {
		t.Fatal("removed domain state leaked")
	}
}

func TestDynamicCaptureUnstartedQueriesKeepPriorityAfterBudgetExhaustion(t *testing.T) {
	state := dynamicQualityState{}
	now := time.Now().UTC()
	jobs := []dynamicQualityJob{{Key: "first"}, {Key: "second"}, {Key: "third"}}
	selected := state.schedule(jobs, 3, now)
	state.started(selected[0].Key, now)
	next := state.schedule(jobs, 2, now.Add(time.Minute))
	if len(next) != 2 || next[0].Key != "second" || next[1].Key != "third" {
		t.Fatal("unstarted domains were delayed as if actually probed", next)
	}
}
