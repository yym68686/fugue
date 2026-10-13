package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

func businessPublicationFixture(t *testing.T) (edgequality.Receipt, platformconfig.PhysicalQualityRoute) {
	return businessPublicationFixtureWithSelection(t, nil, false)
}

func businessPublicationFixtureWithSelection(t *testing.T, previous *model.DNSPhysicalSelection, servingLKG bool) (edgequality.Receipt, platformconfig.PhysicalQualityRoute) {
	return businessPublicationFixtureForNode(t, previous, servingLKG, "dns-a")
}

func businessPublicationFixtureForNode(t *testing.T, previous *model.DNSPhysicalSelection, servingLKG bool, nodeID string) (edgequality.Receipt, platformconfig.PhysicalQualityRoute) {
	return businessPublicationFixtureForBaselines(t, previous, servingLKG, nodeID, nil)
}

func businessPublicationFixtureForBaselines(t *testing.T, previous *model.DNSPhysicalSelection, servingLKG bool, nodeID string, baselines map[string]string) (edgequality.Receipt, platformconfig.PhysicalQualityRoute) {
	t.Helper()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	exploration := model.BusinessEdgeExplorationPolicy{Version: model.BusinessEdgeExplorationVersion, IntentID: "intent-a", Mode: "observe", BaselineEdgeID: "edge-a", CandidateEdgeIDs: []string{"edge-a", "edge-b"},
		InitialDNSPublications: map[string]model.BusinessDNSPublicationAnchor{
			"dns-a": {Digest: "sha256:" + strings.Repeat("b", 64), FencingToken: 1},
			"dns-b": {Digest: "sha256:" + strings.Repeat("e", 64), FencingToken: 37}},
		StartsAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour), ExplorationBasisPoints: 100, MaximumCandidateBasisPoints: 1000, MaximumStepBasisPoints: 50,
		BucketSeconds: 300, RequiredBuckets: 3, MinimumConnectionsPerBucket: 3, CooldownSeconds: 600, EvidenceMaxAgeSeconds: 600,
		MinimumCohortCoverageBasisPoints: 9000, RTTAdvantageRatio: .15, DeliveryAdvantageRatio: .2, MaximumRTTRegressionRatio: .1,
		MaximumDeliveryRegressionRatio: .1, MaximumRetransmissionIncrease: .01, StopRTTMS: 3000, StopRetransmissionRate: .2}
	for dnsNodeID, baseline := range baselines {
		anchor := exploration.InitialDNSPublications[dnsNodeID]
		anchor.BaselineEdgeID = baseline
		exploration.InitialDNSPublications[dnsNodeID] = anchor
	}
	route := platformconfig.PhysicalQualityRoute{Hostname: "app.example.test", PathPrefix: "/", TrafficClass: "dynamic_api", Policy: edgequality.DefaultDeliveryNetworkPolicy(), BusinessExploration: &exploration}
	plan := platformconfig.DNSReadinessPlan{Records: []platformconfig.DNSReadinessRecord{{Hostname: route.Hostname, MinimumHealthyEdges: 2}}}
	readiness := platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 1, FactFreshnessSeconds: 60, MaxConcurrency: 2, MaxProbes: 10}
	facts := []map[string]any{}
	record := model.EdgeDNSRecord{Name: route.Hostname, Type: "A", TTL: 60, AnswerPolicy: model.DNSAnswerPolicy{PolicyKind: model.DNSAnswerPolicyKindPhysicalOrder,
		PhysicalOrder: &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: exploration.CandidateEdgeIDs}, HealthRequired: true, RouteReadyRequired: true}}
	if baselines[nodeID] == "edge-b" {
		record.AnswerPolicy.PhysicalOrder.OrderedEdgeIDs = []string{"edge-b", "edge-a"}
	}
	candidates := []edgequality.Candidate{}
	for index, edgeID := range exploration.CandidateEdgeIDs {
		address, group := []string{"8.8.8.8", "9.9.9.9"}[index], "group-"+edgeID
		probe := platformconfig.DNSReadinessProbe{EdgeID: edgeID, EdgeGroupID: group, Address: address, Hostname: route.Hostname, Path: route.PathPrefix, RouteDigest: "sha256:" + strings.Repeat("a", 64)}
		probe.ID, _ = platformconfig.DNSReadinessProbeID(probe)
		plan.Probes = append(plan.Probes, probe)
		plan.Records[0].Targets = append(plan.Records[0].Targets, platformconfig.DNSReadinessTarget{EdgeID: edgeID, EdgeGroupID: group, Address: address, Family: "A", ProbeIDs: []string{probe.ID}})
		facts = append(facts, map[string]any{"probe_id": probe.ID, "ready": true, "proof": routeprobe.Proof{EdgeID: edgeID, GroupID: group, Digest: probe.RouteDigest, Version: "bundle-a", CheckedAt: now.Add(-time.Second), ValidUntil: now.Add(30 * time.Second)}})
		record.Values = append(record.Values, address)
		record.Candidates = append(record.Candidates, model.EdgeDNSAnswerCandidate{IP: address, EdgeID: edgeID, EdgeGroupID: group, Weight: 100})
		candidates = append(candidates, edgequality.Candidate{EdgeID: edgeID, EdgeGroupID: group})
	}
	if previous != nil {
		record.AnswerPolicy.PolicyKind = model.DNSAnswerPolicyKindPhysicalQuality
		record.AnswerPolicy.PhysicalOrder = nil
		record.AnswerPolicy.PhysicalSelection = previous
		exploration = previous.BusinessAllocation.Policy
	}
	query := new(dns.Msg)
	query.SetQuestion(route.Hostname+".", dns.TypeA)
	wire, _ := query.Pack()
	anchor := exploration.InitialDNSPublications[nodeID]
	publication := dnsserver.DNSDecisionPublication{Digest: anchor.Digest, PolicyDigest: "sha256:" + strings.Repeat("c", 64), FencingToken: anchor.FencingToken}
	if previous != nil {
		publication.Digest, publication.FencingToken = "sha256:"+strings.Repeat("d", 64), 2
	}
	stage := map[string]any{"publication": publication, "applied_at": now, "max_stale_seconds": 3600, "readiness": readiness,
		"authorities": []platformconfig.DNSAuthorityPolicy{{NodeID: nodeID, Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 60}},
		"zone":        "example.test", "record_name": route.Hostname, "exists": true, "query_at": now,
		"entries": []map[string]any{{"record": record, "plan": plan, "facts": facts}}, "selections": []map[string]any{{"at": now, "entropy": map[string]any{"buckets": []any{}}}}}
	if previous != nil {
		stage["selections"] = []map[string]any{{"at": now, "entropy": map[string]any{"buckets": []map[string]any{{"purpose": "business_allocation", "modulus": 10000, "value": 0}}}}}
	}
	replay, _ := json.Marshal(map[string]any{"version": dnsserver.DNSDecisionSchema, "query": wire, "stages": []any{stage}})
	actual := dnsserver.DNSDecisionReceipt{Schema: dnsserver.DNSDecisionSchema, NodeID: nodeID, Hostname: route.Hostname, QueryID: query.Id, QType: dns.TypeA, Transport: "tcp", ObservedAt: now, WriteSucceeded: true,
		DecisionID: nodeID + "-process-1", ProcessID: nodeID + "-process", ReplayInput: replay, Authority: []string{}, Additional: []string{}, AnswerPublication: &publication}
	actual.Publication.ServingLKG = servingLKG
	sign := func() {
		actual.EvidenceDigest = ""
		raw, _ := json.Marshal(actual)
		digest := sha256.Sum256(raw)
		actual.EvidenceDigest = "sha256:" + hex.EncodeToString(digest[:])
	}
	sign()
	result, _ := dnsserver.ReplayDNSDecision(actual)
	actual.RRSet, actual.Records = result.RRSet, result.Records
	sign()
	answer, err := dnsserver.QualityEvidenceFromDNSDecision(actual, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(actual)
	snapshot := edgequality.Snapshot{Schema: edgequality.Schema, Hostname: route.Hostname, PathPrefix: route.PathPrefix, TrafficClass: route.TrafficClass, Scope: "global", CapturedAt: now,
		Policy: route.Policy, Candidates: candidates, ActualDNSReceipt: raw, NetworkSamples: []model.EdgeNetworkSample{}}
	bindPhysicalQualityEvidence(&snapshot, answer)
	source, err := edgequality.Capture(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return source, route
}

func TestBusinessPublicationInitialAnchorsAreBoundToEachDNSNode(t *testing.T) {
	for _, nodeID := range []string{"dns-a", "dns-b"} {
		t.Run(nodeID, func(t *testing.T) {
			source, route := businessPublicationFixtureForNode(t, nil, false, nodeID)
			if _, err := captureBusinessPublication(source, route, source.Snapshot.CapturedAt); err != nil {
				t.Fatal("distinct fenced child artifact must initialize", err)
			}
			for _, invalid := range []string{"missing", "other_node"} {
				policy := model.CloneBusinessEdgeExplorationPolicy(*route.BusinessExploration)
				otherNodeID := "dns-a"
				if nodeID == otherNodeID {
					otherNodeID = "dns-b"
				}
				if invalid == "missing" {
					delete(policy.InitialDNSPublications, nodeID)
				} else {
					policy.InitialDNSPublications[nodeID] = policy.InitialDNSPublications[otherNodeID]
				}
				changed := route
				changed.BusinessExploration = &policy
				if _, err := captureBusinessPublication(source, changed, source.Snapshot.CapturedAt); err == nil {
					t.Fatal("accepted invalid DNS node publication anchor", invalid)
				}
			}
		})
	}
}

func TestBusinessPublicationPreservesDifferentDNSBaselines(t *testing.T) {
	baselines := map[string]string{"dns-a": "edge-a", "dns-b": "edge-b"}
	for nodeID, baseline := range baselines {
		t.Run(nodeID, func(t *testing.T) {
			source, route := businessPublicationFixtureForBaselines(t, nil, false, nodeID, baselines)
			compiled, err := captureBusinessPublication(source, route, source.Snapshot.CapturedAt)
			if err != nil {
				t.Fatal(err)
			}
			allocation := compiled.Selection.BusinessAllocation
			if compiled.Selection.PrimaryEdgeID != baseline || allocation.Policy.BaselineEdgeID != baseline || allocation.State.BasisPoints[baseline] != 9900 || route.BusinessExploration.BaselineEdgeID != "edge-a" {
				t.Fatal("initialization replaced a DNS node's serving baseline", allocation)
			}
			var evidence businessPublicationEvidence
			if err := json.Unmarshal(compiled.Evidence, &evidence); err != nil {
				t.Fatal(err)
			}
			if _, err := compileBoundBusinessSelection(evidence, route, source.Snapshot.CapturedAt); err != nil {
				t.Fatal("node-local evaluation did not replay", err)
			}
			other := "edge-b"
			if baseline == other {
				other = "edge-a"
			}
			allocation.State.Stopped[other] = "observed_transport_stop_threshold"
			allocation.State.BasisPoints = map[string]int{baseline: 10000}
			nextSource, nextRoute := businessPublicationFixtureForBaselines(t, compiled.Selection, false, nodeID, baselines)
			nextRoute.BusinessExploration.BaselineEdgeID = "edge-a"
			next, err := captureBusinessPublication(nextSource, nextRoute, nextSource.Snapshot.CapturedAt)
			if err != nil || next.Selection.BusinessAllocation.State.Stopped[other] == "" || next.Selection.BusinessAllocation.State.BasisPoints[baseline] != 10000 {
				t.Fatal("node-local renewal lost stopped predecessor", err)
			}
			anchor := route.BusinessExploration.InitialDNSPublications[nodeID]
			anchor.BaselineEdgeID = other
			route.BusinessExploration.InitialDNSPublications[nodeID] = anchor
			if _, err := captureBusinessPublication(source, route, source.Snapshot.CapturedAt); err == nil {
				t.Fatal("declared baseline mismatch authorized a bulk traffic move")
			}
		})
	}
}

func TestBusinessPublicationPreservesActualStoppedStateAcrossCapture(t *testing.T) {
	source, route := businessPublicationFixture(t)
	compiled, err := captureBusinessPublication(source, route, source.Snapshot.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	previous := compiled.Selection
	previous.BusinessAllocation.State.Stopped["edge-b"] = "observed_transport_stop_threshold"
	previous.BusinessAllocation.State.BasisPoints = map[string]int{"edge-a": 10000}
	source, route = businessPublicationFixtureWithSelection(t, previous, false)
	next, err := captureBusinessPublication(source, route, source.Snapshot.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	state := next.Selection.BusinessAllocation.State
	if state.Stopped["edge-b"] != "observed_transport_stop_threshold" || state.BasisPoints["edge-b"] != 0 || !state.LastAdjustedAt.Equal(previous.BusinessAllocation.State.LastAdjustedAt) {
		t.Fatal("actual serving predecessor stop or cooldown was reset", state)
	}
	var evidence businessPublicationEvidence
	if err := json.Unmarshal(next.Evidence, &evidence); err != nil {
		t.Fatal(err)
	}
	evidence.Evaluation.Input.Previous = nil
	evidence.Evaluation, err = edgequality.CaptureBusinessExploration(evidence.Evaluation.Input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compileBoundBusinessSelection(evidence, route, source.Snapshot.CapturedAt); err == nil {
		t.Fatal("freshly rehashed offline reset replaced stopped serving state")
	}
}

func TestBusinessPublicationCannotInitializeFromFallbackLKG(t *testing.T) {
	source, route := businessPublicationFixtureWithSelection(t, nil, true)
	if _, err := captureBusinessPublication(source, route, source.Snapshot.CapturedAt); err == nil {
		t.Fatal("fallback silently initialized a new exploration state")
	}
}

func TestBusinessPublicationRetainsCapturedPassiveSamples(t *testing.T) {
	source, route := businessPublicationFixture(t)
	now := source.Snapshot.CapturedAt
	sample, _ := networkWitnessFixture(now)
	sample.EdgeGroupID = "group-edge-a"
	sample.TrafficClass = route.TrafficClass
	sample.Source = "public_front_tcp_info_v1"
	sample.ServiceRTTMS = nil
	sample.ServiceTarget = ""
	rtt := 25.0
	sample.ClientNetwork = &model.EdgeClientNetworkSample{ConnectionID: "connection-a", Slot: "a", Scope: "tcp_peer:203.0.113.0/24", StartedAt: now.Add(-time.Minute), ObservedAt: sample.ObservedAt, TCPInfoAvailable: true, RTTMS: &rtt}
	source.Snapshot.NetworkSamples = []model.EdgeNetworkSample{sample}
	var actual dnsserver.DNSDecisionReceipt
	if err := json.Unmarshal(source.Snapshot.ActualDNSReceipt, &actual); err != nil {
		t.Fatal(err)
	}
	answer, err := dnsserver.QualityEvidenceFromDNSDecision(actual, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	source.Snapshot.Observations = nil
	bindPhysicalQualityEvidence(&source.Snapshot, answer)
	source, err = edgequality.Capture(source.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := captureBusinessPublication(source, route, now)
	if err != nil {
		t.Fatal(err)
	}
	var evidence businessPublicationEvidence
	if err := json.Unmarshal(compiled.Evidence, &evidence); err != nil {
		t.Fatal(err)
	}
	if len(evidence.Evaluation.Input.NetworkSamples) != 1 || evidence.Evaluation.Input.NetworkSamples[0].ID != sample.ID {
		t.Fatal("derived observation ID omitted the original passive TCP sample")
	}
}

func TestBusinessPublicationReconstructsExactDNSAndServingPredecessor(t *testing.T) {
	source, route := businessPublicationFixture(t)
	compiled, err := captureBusinessPublication(source, route, source.Snapshot.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	if compiled.Selection.Version != model.DNSPhysicalBusinessSelectionVersion || compiled.Selection.BusinessAllocation.State.BasisPoints["edge-b"] != 100 {
		t.Fatal("explicit bounded exploration did not compile", compiled.Selection)
	}
	var evidence businessPublicationEvidence
	if err := json.Unmarshal(compiled.Evidence, &evidence); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := compileBoundBusinessSelection(evidence, route, source.Snapshot.CapturedAt)
	if err != nil || !reflect.DeepEqual(rebuilt, compiled.Selection) {
		t.Fatal("publication did not replay", err)
	}
	for _, scenario := range []string{"path", "baseline", "candidate", "invented_previous", "omitted_candidates", "expired_dns", "unbound_route", "old_fence", "old_digest"} {
		t.Run(scenario, func(t *testing.T) {
			var changed businessPublicationEvidence
			if err := json.Unmarshal(compiled.Evidence, &changed); err != nil {
				t.Fatal(err)
			}
			changedRoute, now := route, source.Snapshot.CapturedAt
			policy := model.CloneBusinessEdgeExplorationPolicy(*route.BusinessExploration)
			changedRoute.BusinessExploration = &policy
			switch scenario {
			case "old_fence":
				anchor := policy.InitialDNSPublications["dns-a"]
				anchor.FencingToken++
				policy.InitialDNSPublications["dns-a"] = anchor
			case "old_digest":
				policy.InitialDNSPublications["dns-a"] = model.BusinessDNSPublicationAnchor{Digest: "sha256:" + strings.Repeat("a", 64), FencingToken: 1}
			case "path":
				changedRoute.PathPrefix = "/other"
			case "baseline":
				policy.BaselineEdgeID = "edge-b"
			case "candidate":
				policy.CandidateEdgeIDs = []string{"edge-a", "edge-other"}
			case "invented_previous":
				changed.Evaluation.Input.Previous = &changed.Evaluation.Decision.State
			case "omitted_candidates":
				changed.Evaluation.Input.Candidates = nil
			case "expired_dns":
				now = now.Add(time.Hour)
			case "unbound_route":
				changed.Source.Snapshot.Candidates[0].RouteGeneration = "forged"
				changed.Source, _ = edgequality.Capture(changed.Source.Snapshot)
			}
			if _, err := compileBoundBusinessSelection(changed, changedRoute, now); err == nil {
				t.Fatal("unbound business evidence obtained serving authority")
			}
		})
	}
}

func TestBusinessPublicationAppliesOnlyExactOwnedServiceAtomically(t *testing.T) {
	for _, scenario := range []string{"exact_service", "additional_path", "wrong_class", "shared_alias", "modified_allocation"} {
		t.Run(scenario, func(t *testing.T) {
			projection, nodes, policy, _ := directQueryFixture()
			source, route := businessPublicationFixture(t)
			now := source.Snapshot.CapturedAt
			projection.CapturedAt = now
			projection.RuntimeSnapshot.CapturedAt = &now
			for index := range projection.RuntimeSnapshot.Origins {
				projection.RuntimeSnapshot.Origins[index].ObservedAt = now
			}
			for index := range projection.RuntimeSnapshot.DNSEdgeEndpoints {
				projection.RuntimeSnapshot.DNSEdgeEndpoints[index].ObservedAt = now
			}
			route.Policy.WindowSeconds = 1800
			route.Policy.EvidenceMaxAgeSeconds = 600
			source.Snapshot.Policy = route.Policy
			var err error
			source, err = edgequality.Capture(source.Snapshot)
			if err != nil {
				t.Fatal(err)
			}
			compiled, err := captureBusinessPublication(source, route, now)
			if err != nil {
				t.Fatal(err)
			}
			policy.ECSEnabled, policy.ExplorationPercent = false, 0
			policy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
			policy.PhysicalRoutes = []platformconfig.PhysicalQualityRoute{route}
			if err := projectDirectDNSQueries(&projection, policy, nodes, now); err != nil {
				t.Fatal(err)
			}
			selections := map[string]compiledPhysicalDNSSelection{"dns-a\x00app.example.test": compiled}
			switch scenario {
			case "additional_path":
				additional := projection.Intent.Routes[0]
				additional.PathPrefix = "/other"
				projection.Intent.Routes = append(projection.Intent.Routes, additional)
			case "wrong_class":
				streaming := true
				projection.Intent.Routes[0].Streaming = &streaming
			case "shared_alias":
				projection.Intent.Routes[0].Hostname = "owner.example.test"
				projection.Intent.DNS[0].Route.Hostnames = []string{"owner.example.test"}
			case "modified_allocation":
				compiled.Selection.BusinessAllocation.State.BasisPoints["edge-a"]--
				compiled.Selection.BusinessAllocation.State.BasisPoints["edge-b"]++
			}
			before, _ := json.Marshal(projection)
			err = applyPhysicalDNSSelections(&projection, policy, selections, now)
			if scenario != "exact_service" {
				after, _ := json.Marshal(projection)
				if err == nil || string(before) != string(after) {
					t.Fatal("unsafe allocation did not fail atomically", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			fact := projection.RuntimeSnapshot.DNSSelections[0]
			if !reflect.DeepEqual(fact.PhysicalSelection, compiled.Selection) || len(fact.PhysicalEvidence) == 0 || projection.Policy.DNSAnswerRules[0].SelectionMode != model.DNSAnswerPolicyKindPhysicalQuality {
				t.Fatal("verified allocation and evidence were not published together")
			}
		})
	}
}
