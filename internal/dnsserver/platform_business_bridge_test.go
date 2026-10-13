package dnsserver

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

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
	checked := 0
	for _, view := range payloads[0].Queries {
		for _, record := range view.Records {
			if record.AnswerPolicy.PhysicalSelection == nil || record.AnswerPolicy.PhysicalSelection.BusinessAllocation == nil {
				continue
			}
			if !records[record.Name] {
				t.Fatal("unchanged private business record cannot retain proof authority")
			}
			checked++
		}
	}
	if checked == 0 || reflect.DeepEqual(dnsBridgeQueries(payloads[0].Queries), dnsBridgeQueries(payloads[1].Queries)) {
		t.Fatal("private replay did not exercise record-scoped compatibility differences")
	}
}
