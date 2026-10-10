package api

import (
	"fmt"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

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
