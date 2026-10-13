package dnsserver

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDNSPhysicalAliasRankChangeDoesNotVetoSharedRouteProof(t *testing.T) {
	view, plan, readiness, _, now := queryExecutionFixture()
	record := physicalSelectionTestRecord(now)
	record.Name = plan.Records[0].Hostname
	alias := record
	alias.Name = "alias.example.test"
	view.Records = []model.EdgeDNSRecord{record, alias}
	aliasPlan := plan.Records[0]
	aliasPlan.Hostname = alias.Name
	plan.Records = append(plan.Records, aliasPlan)
	previous := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Policy: platformconfig.PolicySnapshot{MaxStaleSeconds: 3600, DNSReadiness: &readiness}}
	raw := mustDNSJSON(t, previous)
	for _, scenario := range []string{"ranking", "address", "tenant", "quorum"} {
		t.Run(scenario, func(t *testing.T) {
			var next dnsServingPayload
			json.Unmarshal(raw, &next)
			changed := &next.Queries[0].Records[1]
			changed.AnswerPolicy.PhysicalSelection.PrimaryEdgeID = "edge-a"
			changed.AnswerPolicy.PhysicalSelection.OrderedEdgeIDs = []string{"edge-a"}
			switch scenario {
			case "address":
				changed.Candidates[0].IP = "192.0.2.99"
			case "tenant":
				changed.TenantID = "other"
			case "quorum":
				next.Plan.Records[1].MinimumHealthyEdges++
			}
			old := &dnsServingState{payload: previous, record: dnsServingCheckpoint{Candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "old", ReleasedAt: now.Add(-time.Minute)}}}}
			bridge := &dnsReleaseBridge{payload: next, candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "new", ReleasedAt: now}}}
			for _, probe := range plan.Probes {
				if compatibleDNSReleaseProbes(old, bridge)[probe.ID] != (scenario == "ranking") {
					t.Fatal("shared alias proof compatibility mismatch", scenario)
				}
			}
			if dnsTransitionRecords(old, bridge)[alias.Name] {
				t.Fatal("old proof path authorized changed alias selection")
			}
			if string(mustDNSJSON(t, previous)) != string(raw) {
				t.Fatal("retained alias selection mutated")
			}
		})
	}
}

func TestDNSUnifiedWeightChangeRetainsOnlyOriginalSelectionAuthority(t *testing.T) {
	for _, scenario := range []string{"migration", "promotion", "intent", "lease", "candidate", "stopped", "execution", "scope", "address", "quorum", "route"} {
		t.Run(scenario, func(t *testing.T) {
			view, plan, readiness, _, now := queryExecutionFixture()
			record := businessSelectionRecord(t, now)
			record.Name = plan.Records[0].Hostname
			selection := record.AnswerPolicy.PhysicalSelection
			policy := selection.BusinessAllocation.Policy
			policy.Version, policy.Mode, policy.MaximumCandidateBasisPoints = model.BusinessEdgeUnifiedVersion, "adaptive", 10000
			policy.Unified = &model.BusinessUnifiedPolicy{MinimumConnectionsPerCohort: 5, RTTAdvantageMS: 20, PromotionMinimumConnections: 8, PromotionMinimumCoverageBasisPoints: 9000}
			receipt, err := edgequality.CaptureBusinessExploration(edgequality.BusinessExplorationInput{Policy: policy, CapturedAt: now, Hostname: record.Name, PathPrefix: "/", TrafficClass: "dynamic_api", Candidates: []edgequality.Candidate{
				{EdgeID: "edge-a", EdgeGroupID: "shared", RouteGeneration: selection.EvidenceDigest, RouteProofVerified: true, ProofObservedAt: &now},
				{EdgeID: "edge-b", EdgeGroupID: "shared", RouteGeneration: selection.EvidenceDigest, RouteProofVerified: true, ProofObservedAt: &now}}})
			if err != nil {
				t.Fatal(err)
			}
			selection.PrimarySince = &receipt.Decision.State.Unified.PrimarySince
			selection.BusinessAllocation = &model.DNSBusinessAllocation{ExecutionNodeID: "dns-a", Policy: policy, State: receipt.Decision.State, ValidUntil: receipt.Decision.ValidUntil, ReceiptDigest: receipt.Digest}
			if err := model.ValidateDNSPhysicalSelection(selection); err != nil {
				t.Fatal(err)
			}
			view.Records = []model.EdgeDNSRecord{record}
			previous := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Policy: platformconfig.PolicySnapshot{MaxStaleSeconds: 3600, DNSReadiness: &readiness}}
			raw := mustDNSJSON(t, previous)
			var next dnsServingPayload
			if err := json.Unmarshal(raw, &next); err != nil {
				t.Fatal(err)
			}
			changed := next.Queries[0].Records[0].AnswerPolicy.PhysicalSelection
			allocation := changed.BusinessAllocation
			allocation.State.Unified.MigrationBasisPoints = map[string]map[string]int{"dns-a": {"edge-a": 1000, "edge-b": 9000}}
			allocation.State.LastAdjustedAt = now
			allocation.State.LastEvidenceBucket = now.Add(-5 * time.Minute)
			allocation.State.Unified.Sequence++
			switch scenario {
			case "promotion":
				allocation.State.Unified.MigrationBasisPoints = nil
				allocation.State.Unified.PrimaryEdgeID, allocation.State.Unified.TargetEdgeID = "edge-a", "edge-a"
				allocation.State.Unified.QualityBasisPoints = map[string]int{"edge-a": 9900}
				allocation.State.BasisPoints = map[string]int{"edge-a": 9950, "edge-b": 50}
				changed.PrimaryEdgeID, changed.OrderedEdgeIDs = "edge-a", []string{"edge-a", "edge-b"}
			case "intent":
				allocation.Policy.IntentID += "-changed"
			case "lease":
				allocation.Policy.ExpiresAt = allocation.Policy.ExpiresAt.Add(time.Hour)
			case "candidate":
				allocation.Policy.CandidateEdgeIDs = append(allocation.Policy.CandidateEdgeIDs, "foreign")
			case "stopped":
				allocation.State.Stopped["edge-a"] = "transport_stop"
			case "execution":
				allocation.ExecutionNodeID = "dns-other"
			case "scope":
				changed.Scope = "other"
			case "address":
				next.Queries[0].Records[0].Candidates[0].IP = "192.0.2.99"
			case "quorum":
				next.Plan.Records[0].MinimumHealthyEdges++
			case "route":
				for index := range next.Plan.Probes {
					next.Plan.Probes[index].RouteDigest = "changed"
				}
			}
			old := &dnsServingState{payload: previous, record: dnsServingCheckpoint{Candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "old", ReleasedAt: now.Add(-time.Minute)}}}}
			bridge := &dnsReleaseBridge{payload: next, candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "new", ReleasedAt: now}}}
			allowed := compatibleDNSReleaseProbes(old, bridge)
			for _, probe := range plan.Probes {
				if allowed[probe.ID] != (scenario == "migration" || scenario == "promotion") {
					t.Fatal("proof compatibility changed authority", scenario, allowed)
				}
			}
			if dnsTransitionRecords(old, bridge)[record.Name] {
				t.Fatal("proof refresh authorized successor selection")
			}
			if string(mustDNSJSON(t, previous)) != string(raw) {
				t.Fatal("proof comparison altered old weights or lease")
			}
			counts := map[string]int{}
			for bucket := 0; bucket < 10000; bucket++ {
				ordered, _ := edgeDNSOrderedCandidatesWithDecision(record, businessBucketHint(bucket), now, false)
				counts[ordered[0].EdgeID]++
			}
			if counts["edge-a"] != 50 || counts["edge-b"] != 9950 {
				t.Fatal("successor changed retained execution", counts)
			}
		})
	}
}

func TestDNSBusinessObservationRenewalPreservesOriginalAuthorization(t *testing.T) {
	for _, scenario := range []string{"evidence renewal", "weights", "intent", "lease", "candidate", "stopped", "scope", "primary", "order", "execution node"} {
		t.Run(scenario, func(t *testing.T) {
			view, plan, readiness, _, now := queryExecutionFixture()
			record := businessSelectionRecord(t, now)
			record.Name = plan.Records[0].Hostname
			view.Records = []model.EdgeDNSRecord{record}
			previous := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Policy: platformconfig.PolicySnapshot{MaxStaleSeconds: 3600, DNSReadiness: &readiness}}
			raw, err := json.Marshal(previous)
			if err != nil {
				t.Fatal(err)
			}
			var next dnsServingPayload
			if err := json.Unmarshal(raw, &next); err != nil {
				t.Fatal(err)
			}
			selection := next.Queries[0].Records[0].AnswerPolicy.PhysicalSelection
			selection.DNSReceiptID += "-renewed"
			selection.CapturedAt = now.Add(time.Second)
			selection.EvidenceDigest, selection.LoadedDigest, selection.PolicyDigest = "new-evidence", "new-publication", "new-publication-policy"
			allocation := selection.BusinessAllocation
			allocation.ReceiptDigest = "new-comparison"
			allocation.State.LastEvaluatedAt = selection.CapturedAt
			allocation.ValidUntil = allocation.ValidUntil.Add(time.Second)
			allocation.State.EvidenceValidUntil = allocation.State.EvidenceValidUntil.Add(time.Second)
			switch scenario {
			case "weights":
				allocation.State.BasisPoints["edge-a"]++
				allocation.State.BasisPoints["edge-b"]--
			case "intent":
				allocation.Policy.IntentID += "-other"
			case "lease":
				allocation.Policy.ExpiresAt = allocation.Policy.ExpiresAt.Add(time.Hour)
			case "candidate":
				allocation.Policy.CandidateEdgeIDs = append(allocation.Policy.CandidateEdgeIDs, "foreign-edge")
			case "stopped":
				allocation.State.Stopped["edge-a"] = "transport_stop"
			case "scope":
				selection.Scope = "different-cohort"
			case "primary":
				selection.PrimaryEdgeID = "edge-a"
			case "order":
				selection.OrderedEdgeIDs = []string{"edge-a", "edge-b"}
			case "execution node":
				allocation.ExecutionNodeID = "dns-other"
			}
			old := &dnsServingState{payload: previous, record: dnsServingCheckpoint{Candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "old", ReleasedAt: now.Add(-time.Minute)}}}}
			bridge := &dnsReleaseBridge{payload: next, candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "new", ReleasedAt: now}}}
			allowed := compatibleDNSReleaseProbes(old, bridge)
			for _, probe := range plan.Probes {
				if allowed[probe.ID] != (scenario == "evidence renewal") {
					t.Fatalf("unexpected proof compatibility for %s: %v", scenario, allowed)
				}
			}
			if after, _ := json.Marshal(previous); string(after) != string(raw) {
				t.Fatal("comparison mutated retained authorization or evidence lease")
			}
			original := record.AnswerPolicy.PhysicalSelection.BusinessAllocation
			ordered, _ := edgeDNSOrderedCandidatesWithDecision(record, businessBucketHint(0), original.ValidUntil, false)
			if len(ordered) == 0 || ordered[0].EdgeID != original.Policy.BaselineEdgeID {
				t.Fatal("renewal prolonged expired allocation")
			}
		})
	}
}

func TestDNSBusinessBridgePrivateArtifactReplay(t *testing.T) {
	directory := os.Getenv("FUGUE_DNS_BRIDGE_PRIVATE_ARTIFACTS")
	if directory == "" {
		t.Skip("private artifact replay not requested")
	}
	payloads := make([]dnsServingPayload, 2)
	for index, name := range []string{"old", "new"} {
		raw, err := os.ReadFile(directory + "/" + name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		var artifact model.PlatformArtifact
		if json.Unmarshal(raw, &artifact) != nil {
			t.Fatal("invalid private artifact")
		}
		content, err := json.Marshal(artifact.Content)
		if err != nil || json.Unmarshal(content, &payloads[index]) != nil {
			t.Fatal("invalid private payload")
		}
	}
	stamp := time.Now()
	previous := &dnsServingState{payload: payloads[0], record: dnsServingCheckpoint{Candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "previous", ReleasedAt: stamp.Add(-time.Minute)}}}}
	next := &dnsReleaseBridge{payload: payloads[1], candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "next", ReleasedAt: stamp}}}
	_, records := dnsReleaseCompatibility(previous, next)
	probes := compatibleDNSReleaseProbes(previous, next)
	_, proofRecords := dnsReleaseCompatibilityForProof(previous, next, true)
	checked := 0
	for _, view := range payloads[0].Queries {
		for _, record := range view.Records {
			if record.AnswerPolicy.PhysicalSelection == nil || record.AnswerPolicy.PhysicalSelection.BusinessAllocation == nil {
				continue
			}
			if os.Getenv("FUGUE_DNS_BRIDGE_PRIVATE_MIGRATION") == "1" {
				if records[record.Name] {
					t.Fatal("migration authorized new selection through retained proof path")
				}
				for _, requirement := range payloads[0].Plan.Records {
					if requirement.Hostname == record.Name {
						for _, target := range requirement.Targets {
							for _, identity := range target.ProbeIDs {
								if !probes[identity] {
									blocked := []string{}
									for _, peer := range payloads[0].Plan.Records {
										for _, peerTarget := range peer.Targets {
											for _, peerID := range peerTarget.ProbeIDs {
												if peerID == identity && !proofRecords[peer.Hostname] {
													blocked = append(blocked, peer.Hostname)
												}
											}
										}
									}
									t.Fatal("unchanged route proof could not refresh retained selection", blocked)
								}
							}
						}
					}
				}
			} else if !records[record.Name] {
				t.Fatal("unchanged private business record cannot retain proof authority")
			}
			checked++
		}
	}
	if checked == 0 || reflect.DeepEqual(dnsBridgeQueries(payloads[0].Queries), dnsBridgeQueries(payloads[1].Queries)) {
		t.Fatal("private replay did not exercise record-scoped compatibility differences")
	}
}
