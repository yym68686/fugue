package api

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDynamicQualityPreservesVerifiedMembershipWhenEvidenceExpires(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		previous   []string
		candidates []string
		want       []string
	}{
		{"admitted fallback", []string{"edge-b", "edge-new", "edge-a"}, []string{"edge-a", "edge-new", "edge-b"}, []string{"edge-b", "edge-new", "edge-a"}},
		{"admitted primary", []string{"edge-new", "edge-b", "edge-a"}, []string{"edge-a", "edge-new", "edge-b"}, []string{"edge-new", "edge-b", "edge-a"}},
		{"removed by current constraints", []string{"edge-b", "edge-new", "edge-a"}, []string{"edge-a", "edge-b"}, []string{"edge-b", "edge-a"}},
		{"new unmeasured candidate", []string{"edge-b", "edge-a"}, []string{"edge-a", "edge-new", "edge-b"}, []string{"edge-b", "edge-a"}},
		{"removed primary", []string{"edge-b", "edge-new", "edge-a"}, []string{"edge-new", "edge-a"}, []string{"edge-new", "edge-a"}},
		{"no authorized endpoint", []string{"edge-b", "edge-new", "edge-a"}, nil, nil},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			candidates := []platformconfig.DNSSelectionCandidate{}
			for _, edgeID := range scenario.candidates {
				candidates = append(candidates, platformconfig.DNSSelectionCandidate{EdgeID: edgeID})
			}
			order := preservedDynamicQualityOrder(scenario.previous, []string{"edge-a", "edge-b"}, candidates)
			if !slices.Equal(order, scenario.want) {
				t.Fatalf("verified order changed outside current constraints: got %v, want %v", order, scenario.want)
			}
		})
	}
}

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
	other := projection.Intent.Routes[0]
	other.Hostname = "other.example.test"
	projection.Intent.Routes = append(projection.Intent.Routes, other)
	projection.Intent.DNS[1].Route.Hostnames = append(projection.Intent.DNS[1].Route.Hostnames, other.Hostname)
	jobs, states, err = dynamicQualityRoutes(projection, policy)
	if err != nil || len(jobs) != 2 || states[record.Hostname] != "learning_queued" {
		t.Fatal("shared owners were permanently excluded", jobs, states, err)
	}
	for _, job := range jobs {
		if job.Route.Hostname == record.Hostname && (job.EvidenceHostname != record.Hostname || len(job.EvidenceHostnames) != 2) {
			t.Fatal("shared alias did not retain every declared service", job)
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

func TestDynamicQualityCapturesEveryDeclaredPathAndClass(t *testing.T) {
	projection, nodes, policy, now := directQueryFixture()
	policy.ECSEnabled, policy.ExplorationPercent = false, 0
	policy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
	policy.DynamicQuality = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: edgequality.DefaultDeliveryNetworkPolicy(), RefreshQueriesPerCycle: 8, RefreshConcurrency: 2}
	streaming, dynamic := true, false
	projection.Intent.Routes[0].Streaming = &dynamic
	other := projection.Intent.Routes[0]
	other.PathPrefix, other.Streaming = "/stream", &streaming
	projection.Intent.Routes = append(projection.Intent.Routes, other)
	if err := projectDirectDNSQueries(&projection, policy, nodes, now); err != nil {
		t.Fatal(err)
	}
	jobs, _, err := dynamicQualityRoutes(projection, policy)
	if err != nil || len(jobs) != 1 || len(jobs[0].EvidenceServices) != 2 {
		t.Fatal("multi-path owner was skipped", jobs, err)
	}
	if jobs[0].EvidenceServices[0].PathPrefix != "/" || jobs[0].EvidenceServices[0].TrafficClass != "dynamic_api" || jobs[0].EvidenceServices[1].PathPrefix != "/stream" || jobs[0].EvidenceServices[1].TrafficClass != "streaming" {
		t.Fatal("service paths or classes were mixed", jobs[0])
	}
}

func TestDynamicQualityRefreshPairsConsumersWithinEvidenceWindow(t *testing.T) {
	state := dynamicQualityState{}
	jobs := []dynamicQualityJob{}
	for _, node := range []string{"dns-a", "dns-b"} {
		for index := 0; index < 130; index++ {
			hostname := fmt.Sprintf("service-%03d.example.test", index)
			jobs = append(jobs, dynamicQualityJob{NodeID: node, Key: node + "\x00" + hostname, Route: platformconfig.PhysicalQualityRoute{Hostname: hostname}})
		}
	}
	seen := map[string]bool{}
	now := time.Now().UTC()
	for round := 0; round < 3; round++ {
		captured := map[string]int{}
		at := now.Add(time.Duration(round) * 11 * time.Minute)
		for _, job := range state.schedule(jobs, 128, at) {
			captured[job.Route.Hostname]++
			state.started(job.Key, at)
		}
		for hostname, count := range captured {
			if count != 2 {
				t.Fatalf("hostname evidence requires another publication cycle: %s has %d consumers", hostname, count)
			}
			seen[hostname] = true
		}
	}
	if len(seen) != 130 {
		t.Fatalf("domain rotation skipped %d hosts", 130-len(seen))
	}
}

func TestDynamicQualityRefreshResumesPartiallyCapturedHost(t *testing.T) {
	state := dynamicQualityState{}
	jobs := []dynamicQualityJob{
		{Key: "a-first", Route: platformconfig.PhysicalQualityRoute{Hostname: "first.example.test"}},
		{Key: "a-second", Route: platformconfig.PhysicalQualityRoute{Hostname: "second.example.test"}},
		{Key: "b-first", Route: platformconfig.PhysicalQualityRoute{Hostname: "first.example.test"}},
		{Key: "b-second", Route: platformconfig.PhysicalQualityRoute{Hostname: "second.example.test"}},
	}
	now := time.Now().UTC()
	first := state.schedule(jobs, 1, now)
	state.started(first[0].Key, now)
	second := state.schedule(jobs, 1, now.Add(time.Second))
	if first[0].Key != "a-first" || second[0].Key != "b-first" {
		t.Fatal("partial host was skipped or its completed query repeated", first, second)
	}
}

func TestDynamicQualityCoverageRetainsDynamicOwnerWithStaticMetadata(t *testing.T) {
	projection, nodes, policy, now := directQueryFixture()
	policy.ECSEnabled, policy.ExplorationPercent = false, 0
	policy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
	policy.DynamicQuality = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: edgequality.DefaultComparableDeliveryPolicy(), RefreshQueriesPerCycle: 8, RefreshConcurrency: 2}
	projection.Intent.DNS = append(projection.Intent.DNS, platformconfig.DNSIntent{Hostname: "app.example.test", Type: "TXT", Values: []string{"verification-token"}, TTL: 300})
	if err := projectDirectDNSQueries(&projection, policy, nodes, now); err != nil {
		t.Fatal(err)
	}
	jobs, states, err := dynamicQualityRoutes(projection, policy)
	if err != nil || len(jobs) != 1 || states["app.example.test"] != "learning_queued" {
		t.Fatal("static metadata hid dynamic coverage", jobs, states, err)
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

func TestDynamicQualityRefreshFailureRetainsValidCachedSelection(t *testing.T) {
	now := time.Now().UTC()
	compiled := compiledPhysicalDNSSelection{Selection: &model.DNSPhysicalSelection{CapturedAt: now.Add(-time.Minute)}, Evidence: json.RawMessage(`{"receipt":"original"}`)}
	state := dynamicQualityState{entries: map[string]dynamicQualityEntry{
		"job": {ContextDigest: "same", Compiled: compiled},
	}}
	policy := model.PhysicalEdgeQualityPolicy{EvidenceMaxAgeSeconds: 120}
	for _, elapsed := range []time.Duration{0, 30 * time.Second, time.Minute} {
		at := now.Add(elapsed)
		state.started("job", at)
		state.finish("job", dynamicQualityEntry{Attempted: at, ContextDigest: "same", Failure: "probe timeout"}, policy, at)
		got := state.entries["job"]
		if !reflect.DeepEqual(got.Compiled, compiled) || got.Failure != "probe timeout" || !got.Attempted.Equal(at) {
			t.Fatalf("refresh changed evidence or lost the failed attempt: %+v", got)
		}
	}
	expired := now.Add(time.Minute + time.Nanosecond)
	state.finish("job", dynamicQualityEntry{Attempted: expired, ContextDigest: "same", Failure: "probe timeout"}, policy, expired)
	if got := state.entries["job"]; got.Compiled.Selection != nil || len(got.Compiled.Evidence) != 0 {
		t.Fatal("repeated refresh failures extended the original evidence expiry", got)
	}
}

func TestDynamicQualityRefreshFailureDropsChangedOrExpiredSelection(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name string
		ctx  string
		at   time.Time
	}{
		{name: "context changed", ctx: "new", at: now.Add(-time.Minute)},
		{name: "context missing", at: now.Add(-time.Minute)},
		{name: "evidence expired", ctx: "same", at: now.Add(-3 * time.Minute)},
		{name: "evidence in future", ctx: "same", at: now.Add(time.Nanosecond)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := dynamicQualityState{entries: map[string]dynamicQualityEntry{
				"job": {ContextDigest: "same", Compiled: compiledPhysicalDNSSelection{Selection: &model.DNSPhysicalSelection{CapturedAt: tc.at}}},
			}}
			state.finish("job", dynamicQualityEntry{Attempted: now, ContextDigest: tc.ctx, Failure: "probe timeout"}, model.PhysicalEdgeQualityPolicy{EvidenceMaxAgeSeconds: 120}, now)
			got := state.entries["job"]
			if got.Compiled.Selection != nil {
				t.Fatalf("invalid cached evidence survived %s: %+v", tc.name, got)
			}
		})
	}
}

func TestDynamicQualitySuccessfulRefreshReplacesRetainedEvidence(t *testing.T) {
	now := time.Now().UTC()
	state := dynamicQualityState{entries: map[string]dynamicQualityEntry{
		"job": {ContextDigest: "same", Failure: "probe timeout", Compiled: compiledPhysicalDNSSelection{Selection: &model.DNSPhysicalSelection{CapturedAt: now.Add(-time.Minute)}}},
	}}
	entry := dynamicQualityEntry{Attempted: now, ContextDigest: "same", Compiled: compiledPhysicalDNSSelection{Selection: &model.DNSPhysicalSelection{CapturedAt: now}, Evidence: json.RawMessage(`{"receipt":"new"}`)}}
	state.finish("job", entry, model.PhysicalEdgeQualityPolicy{EvidenceMaxAgeSeconds: 120}, now)
	if !reflect.DeepEqual(state.entries["job"], entry) {
		t.Fatal("successful refresh retained old evidence or failure", state.entries["job"])
	}
}

func TestDynamicQualityRetainedEvidenceRequiresPublicationRevalidation(t *testing.T) {
	for _, scenario := range []string{"valid", "tampered receipt", "removed candidate", "changed owner", "expired"} {
		t.Run(scenario, func(t *testing.T) {
			receipt, route := businessPublicationFixture(t)
			route.BusinessExploration = nil
			route.PathPrefix = ""
			captured := receipt.Snapshot.CapturedAt
			selection, err := compileBoundPhysicalQualitySelection(receipt, captured)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(receipt)
			if err != nil {
				t.Fatal(err)
			}
			projection, policy, _, _ := physicalQueryProjectionFixture(t)
			policy.PhysicalRoutes = []platformconfig.PhysicalQualityRoute{route}
			policy.DynamicQuality = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: route.Policy, RefreshQueriesPerCycle: 4, RefreshConcurrency: 2}
			key := "dns-a\x00" + route.Hostname
			state := dynamicQualityState{entries: map[string]dynamicQualityEntry{
				key: {ContextDigest: "same", Compiled: compiledPhysicalDNSSelection{Selection: selection, Evidence: raw}},
			}}
			now := captured.Add(time.Second)
			state.finish(key, dynamicQualityEntry{Attempted: now, ContextDigest: "same", Failure: "probe timeout"}, route.Policy, now)
			compiled := state.entries[key].Compiled
			if compiled.Selection == nil {
				t.Fatal("refresh failure discarded the valid selection")
			}
			switch scenario {
			case "tampered receipt":
				receipt.Snapshot.Hostname = "foreign.example.test"
				compiled.Evidence, _ = json.Marshal(receipt)
			case "removed candidate":
				projection.RuntimeSnapshot.DNSSelections[0].Candidates = nil
			case "changed owner":
				projection.Intent.Routes[0].PathPrefix = "/different"
			case "expired":
				now = captured.Add(time.Duration(route.Policy.EvidenceMaxAgeSeconds+1) * time.Second)
			}
			before, _ := json.Marshal(projection)
			err = applyPhysicalDNSSelections(&projection, policy, map[string]compiledPhysicalDNSSelection{key: compiled}, now)
			if scenario == "valid" {
				if err != nil {
					t.Fatal("valid retained evidence could not preserve dynamic quality", err)
				}
				if projection.Policy.DNSAnswerRules[0].SelectionMode != model.DNSAnswerPolicyKindPhysicalQuality || !projection.RuntimeSnapshot.DNSSelections[0].PhysicalSelection.CapturedAt.Equal(captured) {
					t.Fatal("retained evidence changed policy mode or freshness")
				}
				return
			}
			if err == nil {
				t.Fatal("cached selection bypassed publication validation")
			}
			after, _ := json.Marshal(projection)
			if string(before) != string(after) {
				t.Fatal("rejected cache entry partially changed the projection")
			}
		})
	}
}
