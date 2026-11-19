package dnsserver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

func TestDNSProbePreferencesFollowLocalDeclaredCandidates(t *testing.T) {
	for _, kind := range []string{"order", "selection", "availability"} {
		t.Run(kind, func(t *testing.T) {
			view, plan, _, _, _ := queryExecutionFixture()
			wantPreferred := map[string]bool{"edge-a": true}
			wantCandidates := map[string]bool{"edge-a": true, "edge-b": true}
			if kind == "selection" {
				view.Records[0].AnswerPolicy.PhysicalOrder = nil
				view.Records[0].AnswerPolicy.PhysicalSelection = &model.DNSPhysicalSelection{PrimaryEdgeID: "edge-b"}
				wantPreferred = map[string]bool{"edge-b": true}
				wantCandidates = map[string]bool{}
			} else if kind == "availability" {
				view.Records[0].AnswerPolicy.PhysicalOrder = nil
				view.Records[0].AnswerPolicy.Availability = &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
				wantPreferred = map[string]bool{}
			}
			foreign := view
			foreign.NodeID = "other-node"
			foreign.Records = append([]model.EdgeDNSRecord(nil), view.Records...)
			foreign.Records[0].AnswerPolicy.PhysicalSelection = nil
			foreign.Records[0].AnswerPolicy.PhysicalOrder = &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}
			payload := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view, foreign}}
			before := mustDNSJSON(t, payload)
			preferred, candidates := dnsProbePriorities(payload, view.NodeID, nil)
			for _, probe := range plan.Probes {
				key := dnsroutesource.ProbeKey(probe)
				if preferred[key] != wantPreferred[probe.EdgeID] || candidates[key] != wantCandidates[probe.EdgeID] {
					t.Fatal("priority differs from local declared DNS policy", probe, preferred, candidates)
				}
			}
			if preferred, candidates := dnsProbePriorities(payload, "absent-node", nil); len(preferred) != 0 || len(candidates) != 0 || !reflect.DeepEqual(before, mustDNSJSON(t, payload)) {
				t.Fatal("priority changed signed inputs or crossed DNS identity")
			}
		})
	}
}

func TestDNSProbePreferencesFollowVerifiedRuntimePathWithoutExtendingEligibility(t *testing.T) {
	view, plan, _, _, _ := queryExecutionFixture()
	runtime := &platformconfig.DNSReadinessPlan{}
	for _, baseline := range plan.Probes {
		current := baseline
		current.ID += "-runtime"
		current.Path = "/current"
		foreign := current
		foreign.ID += "-foreign"
		foreign.Address = "192.0.2.44"
		runtime.Probes = append(runtime.Probes, current, foreign)
	}
	preferred, candidates := dnsProbePriorities(dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}}, view.NodeID, runtime)
	for i, p := range runtime.Probes {
		key := dnsroutesource.ProbeKey(p)
		if candidates[key] != (i%2 == 0) || preferred[key] != (i%2 == 0 && p.EdgeID == "edge-a") {
			t.Fatal("runtime priority omitted a verified path or admitted a foreign target", p, preferred, candidates)
		}
	}
}

func TestDNSReadinessPrioritizesEveryAvailabilityCandidate(t *testing.T) {
	view, plan, policy, _, _ := queryExecutionFixture()
	policy.MaxConcurrency = 1
	policy.FactFreshnessSeconds = 120
	background := platformconfig.DNSReadinessProbe{ID: "background", EdgeID: "edge-b", EdgeGroupID: "edge-group-b", Address: "9.9.9.9", Hostname: "other.example.test", Path: "/", RouteDigest: "sha256:" + strings.Repeat("c", 64)}
	plan.Probes = append([]platformconfig.DNSReadinessProbe{background}, plan.Probes...)
	view.Records[0].AnswerPolicy.Availability = &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
	preferred, availability := dnsProbePriorities(dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}}, view.NodeID, nil)
	scheduler := (&dnsReadinessScheduler{}).prioritize(preferred, availability)
	for _, age := range []time.Duration{70 * time.Second, 35 * time.Second, 0} {
		ctx, cancel := context.WithCancel(context.Background())
		var observed []string
		at := time.Now().UTC().Add(-age)
		facts := scheduler.collect(ctx, &plan, &policy, func(_ context.Context, host, path, address, _ string, _ time.Duration) (routeprobe.Proof, error) {
			observed = append(observed, host+"@"+address)
			if len(observed) == len(plan.Probes)-1 {
				cancel()
			}
			for _, probe := range plan.Probes[1:] {
				if probe.Hostname == host && probe.Address == address && probe.Path == path {
					return routeprobe.Proof{Version: "example", EdgeID: probe.EdgeID, GroupID: probe.EdgeGroupID, Digest: probe.RouteDigest, CheckedAt: at, ValidUntil: at.Add(120 * time.Second)}, nil
				}
			}
			return routeprobe.Proof{}, errors.New("unrelated route")
		})
		cancel()
		if !reflect.DeepEqual(observed, []string{"app.example.test@8.8.8.8", "app.example.test@9.9.9.9", "app.example.test@8.8.8.8", "app.example.test@9.9.9.9"}) {
			t.Fatalf("background work displaced an availability candidate during a partial scan: %v", observed)
		}
		if age == 0 {
			now := time.Now().UTC()
			records, err := materializeDNSQueries(view, &plan, &policy, facts, now)
			if err != nil || len(records) == 0 || len(records[0].Candidates) != 2 {
				t.Fatalf("partial sweeps did not recover both DNS candidates: %+v %v", records, err)
			}
			answers, err := executeDNSQueryRecord(records[0], dnsGeoHint{}, now)
			if err != nil || len(answers) != 1 {
				t.Fatalf("recovery did not produce a single DNS answer: %v %v", answers, err)
			}
		}
	}
	if availability[dnsroutesource.ProbeKey(background)] {
		t.Fatal("unrelated hostname was incorrectly promoted by the signed availability pool")
	}
}

func TestDNSReadinessRotatesAndBoundsAvailabilityPriority(t *testing.T) {
	plan, policy := readinessTestPlan()
	plan.Probes = nil
	policy.MaxConcurrency = 1
	availability := map[string]bool{}
	for i := range 11 {
		p := platformconfig.DNSReadinessProbe{ID: fmt.Sprint(i), Hostname: fmt.Sprintf("route-%d.example.test", i), Address: "8.8.8.8", Path: "/"}
		plan.Probes = append(plan.Probes, p)
		if i != 0 {
			availability[dnsroutesource.ProbeKey(p)] = true
		}
	}
	scheduler := (&dnsReadinessScheduler{}).prioritize(nil, availability)
	for _, first := range []string{"route-1.example.test", "route-9.example.test"} {
		ctx, cancel := context.WithCancel(context.Background())
		var observed []string
		scheduler.collect(ctx, &plan, &policy, func(_ context.Context, host, _, _, _ string, _ time.Duration) (routeprobe.Proof, error) {
			observed = append(observed, host)
			if len(observed) == 9 {
				cancel()
			}
			return routeprobe.Proof{}, errors.New("synthetic route rejection")
		})
		cancel()
		if len(observed) != 9 || observed[0] != first || observed[8] != "route-0.example.test" {
			t.Fatalf("bounded priority lost rotation or displaced ordinary work: %v", observed)
		}
		// An ordinary proof due for renewal still leads the non-priority tail.
		scheduler.expires["0"] = time.Now().Add(time.Second)
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
			scheduler := (&dnsReadinessScheduler{}).prioritize(map[string]bool{dnsroutesource.ProbeKey(plan.Probes[1]): true}, nil)
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
	scheduler := (&dnsReadinessScheduler{}).bind(assignment, nil).prioritize(preferences, nil)
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
