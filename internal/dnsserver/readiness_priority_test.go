package dnsserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

func TestDNSSoleChoiceIsBoundToSignedLocalFamilyAndPhysicalTarget(t *testing.T) {
	for _, kind := range []string{"order", "quality", "multiple", "other family", "other node", "availability"} {
		t.Run(kind, func(t *testing.T) {
			view, plan, _, _, _ := queryExecutionFixture()
			node := view.NodeID
			rr := &view.Records[0]
			rr.AnswerPolicy.PhysicalOrder = &model.DNSPhysicalOrder{OrderedEdgeIDs: []string{"edge-b"}}
			rr.AnswerPolicy.PhysicalSelection = nil
			switch kind {
			case "quality":
				rr.AnswerPolicy.PhysicalOrder = nil
				rr.AnswerPolicy.PhysicalSelection = &model.DNSPhysicalSelection{PrimaryEdgeID: "edge-b", OrderedEdgeIDs: []string{"edge-b"}}
			case "multiple":
				rr.AnswerPolicy.PhysicalOrder.OrderedEdgeIDs = append(rr.AnswerPolicy.PhysicalOrder.OrderedEdgeIDs, "edge-a")
			case "other family":
				rr.Type = "AAAA"
			case "other node":
				node = "another-node"
			case "availability":
				rr.AnswerPolicy.Availability = &model.DNSAvailabilityPolicy{OrderedEdgeIDs: []string{"edge-b", "edge-a"}}
			}
			pr := dnsProbePriorities(dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}}, node, nil)
			n := 0
			for _, probe := range plan.Probes {
				want := (kind == "order" || kind == "quality") && probe.EdgeID == "edge-b"
				if pr.sole[dnsroutesource.ProbeKey(probe)] != want {
					t.Fatal("changed exact sole-choice membership", kind, probe.ID)
				}
				if want {
					n++
				}
			}
			if (kind == "order" || kind == "quality") && n == 0 {
				t.Fatal("fixture lacks signed target")
			}
		})
	}
}

func TestDNSComplementaryCoverageBalancesSignedAlternativesAroundPinnedWork(t *testing.T) {
	view, original, _, _, _ := queryExecutionFixture()
	template := view.Records[0]
	view.Records = nil
	plan := platformconfig.DNSReadinessPlan{}
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("host-%02d.example.test", i)
		rr := template
		rr.Name = name
		order := []string{"edge-a", "edge-b"}
		if i < 2 {
			order = order[:1]
		}
		rr.AnswerPolicy.PhysicalOrder = &model.DNSPhysicalOrder{OrderedEdgeIDs: order}
		view.Records = append(view.Records, rr)
		rec := original.Records[0]
		rec.Hostname, rec.MinimumHealthyEdges, rec.RequireDualStack = name, 1, false
		rec.MinDistinctCells, rec.MinDistinctDomains = 0, nil
		rec.Targets = append([]platformconfig.DNSReadinessTarget(nil), rec.Targets...)
		for j := range rec.Targets {
			tgt := &rec.Targets[j]
			id := name + ":" + tgt.EdgeID
			tgt.ProbeIDs = []string{id}
			plan.Probes = append(plan.Probes, platformconfig.DNSReadinessProbe{ID: id, Hostname: name, EdgeID: tgt.EdgeID, EdgeGroupID: tgt.EdgeGroupID, Address: tgt.Address, Path: "/"})
		}
		plan.Records = append(plan.Records, rec)
	}
	p := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view, view}}
	before := mustDNSJSON(t, p)
	prefs := dnsProbePriorities(p, view.NodeID, nil)
	counts, byHost := map[string]int{}, map[string]int{}
	for _, probe := range plan.Probes {
		key := dnsroutesource.ProbeKey(probe)
		if prefs.sole[key] || prefs.alternateCover[key] {
			counts[probe.EdgeID]++
			byHost[probe.Hostname]++
		}
	}
	if !reflect.DeepEqual(counts, map[string]int{"edge-a": 5, "edge-b": 5}) || len(byHost) != 10 || len(prefs.sole) != 2 || len(prefs.alternateCover) != 8 {
		t.Fatal("duplicate views or aligned physical queues left record coverage uneven", counts, byHost)
	}
	for _, n := range byHost {
		if n != 1 {
			t.Fatal("missing or duplicated dependency")
		}
	}
	if !reflect.DeepEqual(before, mustDNSJSON(t, p)) {
		t.Fatal("ordering mutated signed input")
	}
	if hints := dnsProbePriorities(p, "another-node", nil); len(hints.alternateCover) != 0 || len(hints.sole) != 0 {
		t.Fatal("crossed node identity")
	}
	// The fast path covers only simple one-edge quorum. Other record
	// contracts retain their ordinary priority and independent validation.
	for i := range plan.Records {
		plan.Records[i].MinimumHealthyEdges = 2
	}
	if len(dnsProbePriorities(p, view.NodeID, nil).alternateCover) != 0 {
		t.Fatal("used a single refresh to represent a multi-edge requirement")
	}
}

func TestDNSSoleChoiceServesConstrainedRecordsWithoutStarvingAlternatives(t *testing.T) {
	plan := &platformconfig.DNSReadinessPlan{}
	sole := map[string]bool{}
	var indices []int
	for i := range 80 {
		p := platformconfig.DNSReadinessProbe{ID: fmt.Sprint(i), Hostname: fmt.Sprintf("host-%d.example.test", i), Address: "192.0.2.10", Path: "/"}
		plan.Probes = append(plan.Probes, p)
		indices = append(indices, i)
		if i >= 40 {
			sole[dnsroutesource.ProbeKey(p)] = true
		}
	}
	balanceDNSCoverageQueue(indices, map[int]int{0: 1, 1: 2}, plan, sole, nil)
	want := []int{0, 1, 40, 41, 42, 2, 43, 44, 45, 3}
	if !reflect.DeepEqual(indices[:len(want)], want) {
		t.Fatal("critical or non-critical work starved", indices[:len(want)])
	}
	alternate := map[string]bool{}
	for i := 20; i < 40; i++ {
		alternate[dnsroutesource.ProbeKey(plan.Probes[i])] = true
	}
	for i := range indices {
		indices[i] = i
	}
	balanceDNSCoverageQueue(indices, map[int]int{0: 1, 1: 2}, plan, sole, alternate)
	want = []int{0, 1, 40, 41, 42, 20, 43, 44, 45, 21, 46, 47, 48, 22, 49, 50, 51, 2}
	if !reflect.DeepEqual(indices[:len(want)], want) {
		t.Fatal("complementary work displaced singleton, reserved prefix or background", indices[:len(want)])
	}
	seen := map[int]bool{}
	for _, i := range indices {
		if i < 0 || i >= 80 || seen[i] {
			t.Fatal("lost exact plan membership", i)
		}
		seen[i] = true
	}
}

func TestDNSProbePreferencesFollowLocalDeclaredCandidates(t *testing.T) {
	for _, kind := range []string{"order", "selection", "availability"} {
		t.Run(kind, func(t *testing.T) {
			view, plan, _, _, _ := queryExecutionFixture()
			wantPreferred := map[string]bool{"edge-a": true}
			wantCandidates := map[string]bool{"edge-a": true, "edge-b": true}
			if kind == "selection" {
				view.Records[0].AnswerPolicy.PhysicalOrder = nil
				view.Records[0].AnswerPolicy.PhysicalSelection = &model.DNSPhysicalSelection{PrimaryEdgeID: "edge-b", OrderedEdgeIDs: []string{"edge-b", "edge-a"}}
				wantPreferred = map[string]bool{"edge-b": true}
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
			priority := dnsProbePriorities(payload, view.NodeID, nil)
			for _, probe := range plan.Probes {
				key := dnsroutesource.ProbeKey(probe)
				if priority.preferred[key] != wantPreferred[probe.EdgeID] || priority.candidates[key] != wantCandidates[probe.EdgeID] ||
					priority.availability[key] != (kind == "availability" && wantCandidates[probe.EdgeID]) {
					t.Fatal("priority differs from local declared DNS policy", probe, priority)
				}
			}
			if other := dnsProbePriorities(payload, "absent-node", nil); len(other.preferred)+len(other.candidates)+len(other.availability) != 0 || !reflect.DeepEqual(before, mustDNSJSON(t, payload)) {
				t.Fatal("priority changed signed inputs or crossed DNS identity")
			}
		})
	}
}

func TestDNSReadinessProbesRankedFallbackDuringPartialSweep(t *testing.T) {
	view, plan, policy, _, _ := queryExecutionFixture()
	policy.MaxConcurrency = 1
	record := &view.Records[0]
	record.AnswerPolicy.PolicyKind = model.DNSAnswerPolicyKindPhysicalQuality
	record.AnswerPolicy.PhysicalOrder = nil
	digest := "sha256:" + strings.Repeat("a", 64)
	record.AnswerPolicy.PhysicalSelection = &model.DNSPhysicalSelection{
		Version: model.DNSPhysicalSelectionVersion, Scope: "global", PrimaryEdgeID: "edge-a",
		OrderedEdgeIDs: []string{"edge-a", "edge-b"}, CapturedAt: time.Now().UTC(),
		DNSReceiptID: "receipt", EvidenceDigest: digest, LoadedDigest: digest, PolicyDigest: digest,
	}
	plan.Records[0].MinimumHealthyEdges = 1
	background := platformconfig.DNSReadinessProbe{ID: "background", EdgeID: "edge-b", EdgeGroupID: "edge-group-b",
		Address: "9.9.9.9", Hostname: "other.example.test", Path: "/", RouteDigest: digest}
	plan.Probes = append([]platformconfig.DNSReadinessProbe{background}, plan.Probes...)
	priority := dnsProbePriorities(dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}}, view.NodeID, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler := (&dnsReadinessScheduler{}).prioritize(priority)
	calls := 0
	facts := scheduler.collect(ctx, &plan, &policy, func(_ context.Context, host, path, address, _ string, _ time.Duration) (routeprobe.Proof, error) {
		calls++
		if calls == 4 {
			cancel()
		}
		for _, p := range plan.Probes[1:] {
			if p.EdgeID == "edge-b" && p.Hostname == host && p.Path == path && p.Address == address {
				now := time.Now().UTC()
				return routeprobe.Proof{Version: "serving", EdgeID: p.EdgeID, GroupID: p.EdgeGroupID, Digest: p.RouteDigest,
					CheckedAt: now, ValidUntil: now.Add(30 * time.Second)}, nil
			}
		}
		return routeprobe.Proof{}, errors.New("unreachable or unrelated")
	})
	now := time.Now().UTC()
	records, err := materializeDNSQueries(view, &plan, &policy, facts, now)
	if err != nil || len(records[0].Candidates) != 1 || records[0].Candidates[0].EdgeID != "edge-b" {
		t.Fatalf("partial sweep did not collect complete fallback evidence: %+v %v", records, err)
	}
	if answers, err := executeDNSQueryRecord(records[0], dnsGeoHint{}, now); err != nil || len(answers) != 1 {
		t.Fatalf("no authorized fallback answer: %v %v", answers, err)
	}
	if priority.candidates[dnsroutesource.ProbeKey(background)] || facts[0].Ready {
		t.Fatal("partial sweep prioritized or accepted an unrelated target")
	}
}

func TestDNSProbePreferencesFollowVerifiedRuntimePathWithoutExtendingEligibility(t *testing.T) {
	view, plan, _, _, _ := queryExecutionFixture()
	runtime := &platformconfig.DNSReadinessPlan{}
	for _, baseline := range plan.Probes {
		current := baseline
		current.ID += "-runtime"
		current.Path = "/current"
		foreignAddr, foreignGroup := current, current
		foreignAddr.ID += "-foreign-address"
		foreignAddr.Address = "192.0.2.44"
		foreignGroup.ID += "-foreign-group"
		foreignGroup.EdgeGroupID += "-other"
		// When the wire key is identical, one request serves multiple variants
		// and the signed plan separately verifies the matching group and digest.
		foreignGroup.Path = "/unbound-group"
		runtime.Probes = append(runtime.Probes, current, foreignAddr, foreignGroup)
	}
	for _, availability := range []bool{false, true} {
		if availability {
			view.Records[0].AnswerPolicy.PhysicalOrder = nil
			view.Records[0].AnswerPolicy.Availability = &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
		}
		priority := dnsProbePriorities(dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}}, view.NodeID, runtime)
		for i, p := range runtime.Probes {
			key := dnsroutesource.ProbeKey(p)
			match := i%3 == 0
			if priority.candidates[key] != match || priority.preferred[key] != (match && !availability && p.EdgeID == "edge-a") || priority.availability[key] != (match && availability) {
				t.Fatal("runtime priority omitted a verified path or admitted a foreign target", p, priority)
			}
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
	priority := dnsProbePriorities(dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}}, view.NodeID, nil)
	scheduler := (&dnsReadinessScheduler{}).prioritize(priority)
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
	if priority.availability[dnsroutesource.ProbeKey(background)] {
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
	scheduler := (&dnsReadinessScheduler{}).prioritize(dnsProbePriority{availability: availability})
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

func TestDNSReadinessReservesPriorityForAvailabilityAndRankedCandidates(t *testing.T) {
	plan, policy := readinessTestPlan()
	plan.Probes = nil
	policy.MaxConcurrency = 1
	priority := dnsProbePriority{candidates: map[string]bool{}, availability: map[string]bool{}}
	add := func(class string, i int) {
		id := fmt.Sprintf("%s-%d", class, i)
		p := platformconfig.DNSReadinessProbe{ID: id, Hostname: id + ".example.test", Address: "8.8.8.8", Path: "/"}
		plan.Probes = append(plan.Probes, p)
		key := dnsroutesource.ProbeKey(p)
		if class != "background" {
			priority.candidates[key] = true
		}
		if class == "availability" {
			priority.availability[key] = true
		}
	}
	add("background", 0)
	for i := range 200 {
		add("ranked", i)
	}
	for i := range 6 {
		add("availability", i)
	}
	scheduler := (&dnsReadinessScheduler{}).prioritize(priority)
	for _, want := range [][]string{
		{"availability-0", "availability-1", "availability-2", "availability-3", "ranked-0", "ranked-1", "ranked-2", "ranked-3", "background-0"},
		{"availability-4", "availability-5", "availability-0", "availability-1", "ranked-4", "ranked-5", "ranked-6", "ranked-7", "background-0"},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		var observed []string
		scheduler.collect(ctx, &plan, &policy, func(_ context.Context, host, _, _, _ string, _ time.Duration) (routeprobe.Proof, error) {
			observed = append(observed, strings.TrimSuffix(host, ".example.test"))
			if len(observed) == len(want) {
				cancel()
			}
			return routeprobe.Proof{}, errors.New("synthetic route rejection")
		})
		cancel()
		if !reflect.DeepEqual(observed, want) {
			t.Fatalf("partial sweep lost reserved slots, rotation or background progress: got %v, want %v", observed, want)
		}
		scheduler.expires["background-0"] = time.Now().Add(time.Second)
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
			scheduler := (&dnsReadinessScheduler{}).prioritize(dnsProbePriority{preferred: map[string]bool{dnsroutesource.ProbeKey(plan.Probes[1]): true}})
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

func TestDNSReadinessPublicationBlockerIsBoundedAndAssignmentScoped(t *testing.T) {
	plan, policy := readinessTestPlan()
	plan.Probes = nil
	policy.MaxConcurrency = 1
	now := time.Now().UTC()
	policy.FactFreshnessSeconds = 120
	priority := dnsProbePriority{preferred: map[string]bool{}, candidates: map[string]bool{}, availability: map[string]bool{}}
	add := func(id, host, path string, ranked, availability, primary bool) {
		p := platformconfig.DNSReadinessProbe{ID: id, Hostname: host + ".example.test", Address: "192.0.2.1", Path: path}
		plan.Probes = append(plan.Probes, p)
		key := dnsroutesource.ProbeKey(p)
		priority.candidates[key], priority.availability[key], priority.preferred[key] = ranked, availability, primary
	}
	add("ordinary", "ordinary", "/", false, false, false)
	for i := range 9 {
		add(fmt.Sprint("availability-", i), fmt.Sprint("availability-", i), "/", true, true, false)
		add(fmt.Sprint("ranked-", i), fmt.Sprint("ranked-", i), "/", true, false, false)
	}
	add("blocked-fallback", "blocked", "/alternate", true, false, false)
	add("blocked-primary", "blocked", "/primary", true, false, true)
	add("blocked-extra", "blocked", "/extra", true, false, false)
	for _, scenario := range []string{"exact", "changed-fence", "expired", "cleared"} {
		t.Run(scenario, func(t *testing.T) {
			a := model.PlatformConsumerAssignment{ExpectedConsumerSetID: "set", ArtifactID: "dns", ReleaseChannel: "gray", FencingToken: 3}
			scheduler := (&dnsReadinessScheduler{}).prioritize(priority)
			at := now
			if scenario == "expired" {
				at = at.Add(-3 * time.Minute)
			}
			for _, name := range []string{"irrelevant-one", "irrelevant-two", "irrelevant-three", "irrelevant-four"} {
				scheduler.noteQueryBlocker(a, name+".example.test", at.Add(-time.Second))
			}
			scheduler.noteQueryBlocker(a, "blocked.example.test.", at)
			if len(scheduler.blockers) != 4 || !scheduler.blockers["blocked.example.test"].Equal(at) {
				t.Fatal("unbounded blocker history")
			}
			if scenario == "changed-fence" {
				a.FencingToken++
			}
			if scenario == "cleared" {
				scheduler.clearQueryBlockers(a)
			}
			var observation strings.Builder
			scheduler.bind(a, log.New(&observation, "", 0))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var order []string
			facts := scheduler.collect(ctx, &plan, &policy, func(_ context.Context, host, path, _, _ string, _ time.Duration) (routeprobe.Proof, error) {
				for _, p := range plan.Probes {
					if p.Hostname == host && p.Path == path {
						order = append(order, p.ID)
						break
					}
				}
				if len(order) == 9 {
					cancel()
				}
				return routeprobe.Proof{}, errors.New("test endpoint not ready")
			})
			want := []string{"availability-0", "availability-1", "availability-2", "availability-3", "ranked-0", "ranked-1", "ranked-2", "ranked-3", "blocked-primary"}
			attempts := 0
			if scenario == "exact" {
				want = []string{"blocked-primary", "blocked-fallback", "availability-0", "availability-1", "availability-2", "ranked-0", "ranked-1", "ranked-2", "ordinary"}
				attempts = 2
			}
			if !reflect.DeepEqual(order, want) {
				t.Fatalf("unexpected limited sweep order: got %v want %v", order, want)
			}
			if !strings.Contains(observation.String(), fmt.Sprintf("\"query_blocker_attempts\":%d", attempts)) {
				t.Fatal("missing bounded probe diagnostic")
			}
			for _, fact := range facts {
				if fact.Ready || len(fact.ProbeHistory) != 0 {
					t.Fatal("scheduling hint substituted for real positive evidence")
				}
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
	scheduler := (&dnsReadinessScheduler{}).bind(assignment, nil).prioritize(dnsProbePriority{preferred: preferences})
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
