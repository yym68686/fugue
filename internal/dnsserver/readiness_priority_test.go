package dnsserver

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

func TestDNSProbePreferencesFollowOnlyLocalDeclaredSelectors(t *testing.T) {
	for _, kind := range []string{"order", "selection"} {
		t.Run(kind, func(t *testing.T) {
			view, plan, _, _, _ := queryExecutionFixture()
			primary := "edge-a"
			if kind == "selection" {
				view.Records[0].AnswerPolicy.PhysicalOrder = nil
				view.Records[0].AnswerPolicy.PhysicalSelection = &model.DNSPhysicalSelection{PrimaryEdgeID: "edge-b"}
				primary = "edge-b"
			}
			foreign := view
			foreign.NodeID = "other-node"
			foreign.Records = append([]model.EdgeDNSRecord(nil), view.Records...)
			otherPrimary := "edge-b"
			if primary == otherPrimary {
				otherPrimary = "edge-a"
			}
			foreign.Records[0].AnswerPolicy.PhysicalSelection = nil
			foreign.Records[0].AnswerPolicy.PhysicalOrder = &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{otherPrimary}}
			payload := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view, foreign}}
			before := mustDNSJSON(t, payload)
			preferences := dnsPreferredProbeKeys(payload, view.NodeID)
			for _, probe := range plan.Probes {
				if preferences[dnsroutesource.ProbeKey(probe)] != (probe.EdgeID == primary) {
					t.Fatal("priority differs from the declared primary dependencies", probe, preferences)
				}
			}
			if len(dnsPreferredProbeKeys(payload, "absent-node")) != 0 || !reflect.DeepEqual(before, mustDNSJSON(t, payload)) {
				t.Fatal("priority changed signed inputs or crossed DNS identity")
			}
		})
	}
}

func TestDNSProbePreferencesPreserveExpiryAgingAndConnectionBackoff(t *testing.T) {
	plan := &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{
		{ID: "other", Hostname: "other.example.test", Address: "192.0.2.1", Path: "/"},
		{ID: "primary", Hostname: "primary.example.test", Address: "192.0.2.1", Path: "/"},
	}}
	policy := &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, MaxConcurrency: 1}
	for _, scenario := range []string{"expiry", "aged", "backoff"} {
		t.Run(scenario, func(t *testing.T) {
			scheduler := (&dnsReadinessScheduler{}).prioritize(map[string]bool{dnsroutesource.ProbeKey(plan.Probes[1]): true})
			scheduler.collect(context.Background(), plan, policy, func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
				return routeprobe.Proof{}, errors.New("observed rejection")
			})
			now := time.Now()
			for _, requirement := range plan.Probes {
				scheduler.attempts[requirement.ID] = now.Add(-time.Second)
				scheduler.expires[requirement.ID] = now.Add(2 * time.Minute)
			}
			switch scenario {
			case "expiry":
				scheduler.expires["other"] = now.Add(time.Second)
			case "aged":
				scheduler.attempts["other"] = now.Add(-time.Minute)
			case "backoff":
				scheduler.targets[plan.Probes[0].Address] = dnsProbeTargetState{retryAt: now.Add(time.Minute), failures: 1}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls []string
			scheduler.collect(ctx, plan, policy, func(_ context.Context, host, _, _, _ string, _ time.Duration) (routeprobe.Proof, error) {
				calls = append(calls, host)
				cancel()
				return routeprobe.Proof{}, errors.New("observed rejection")
			})
			if scenario == "backoff" && len(calls) != 0 || scenario != "backoff" && !reflect.DeepEqual(calls, []string{"other.example.test"}) {
				t.Fatal("preference bypassed expiry, fairness or connection backoff", scenario, calls)
			}
		})
	}
}

func TestDNSProbePreferencesStartNewAssignmentsWithoutStarvingOtherWork(t *testing.T) {
	plan := &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{
		{ID: "first", Hostname: "first.example.test", Address: "192.0.2.1", Path: "/"},
		{ID: "second", Hostname: "second.example.test", Address: "192.0.2.1", Path: "/"},
		{ID: "primary", Hostname: "primary.example.test", Address: "192.0.2.1", Path: "/"},
	}}
	policy := &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, MaxConcurrency: 1}
	preferences := map[string]bool{dnsroutesource.ProbeKey(plan.Probes[2]): true}
	assignment := model.PlatformConsumerAssignment{FencingToken: 1}
	scheduler := (&dnsReadinessScheduler{}).bind(assignment, nil).prioritize(preferences)
	delete(preferences, dnsroutesource.ProbeKey(plan.Probes[2]))
	before := mustDNSJSON(t, plan)
	for _, expected := range []string{"primary.example.test", "first.example.test", "second.example.test"} {
		ctx, cancel := context.WithCancel(context.Background())
		var called []string
		facts := scheduler.collect(ctx, plan, policy, func(_ context.Context, host, _, _, _ string, _ time.Duration) (routeprobe.Proof, error) {
			called = append(called, host)
			cancel()
			return routeprobe.Proof{}, errors.New("observed rejection")
		})
		cancel()
		if !reflect.DeepEqual(called, []string{expected}) {
			t.Fatal("priority or bounded fairness was lost", called, expected)
		}
		for _, fact := range facts {
			if fact.Ready {
				t.Fatal("scheduling preference fabricated readiness")
			}
		}
	}
	assignment.FencingToken++
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := ""
	scheduler.bind(assignment, nil).collect(ctx, plan, policy, func(_ context.Context, host, _, _, _ string, _ time.Duration) (routeprobe.Proof, error) {
		first = host
		cancel()
		return routeprobe.Proof{}, errors.New("observed rejection")
	})
	if first != "primary.example.test" || !reflect.DeepEqual(before, mustDNSJSON(t, plan)) {
		t.Fatal("new assignment lost declared preference or changed plan", first)
	}
}
