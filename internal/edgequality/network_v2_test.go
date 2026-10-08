package edgequality

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func networkFixture() Snapshot {
	snapshot := fixture()
	snapshot.Policy = DefaultNetworkPolicy()
	for index := range snapshot.Observations {
		observation := &snapshot.Observations[index]
		observation.ObservedAt = observation.ObservedAt.Add(4*time.Minute + 20*time.Second)
		observation.ClientCohort, observation.CapacitySource = "tcp_peer:203.0.113.0/24", "kubelet_node_allocatable_v1"
		observation.UploadBPS, observation.DownloadBPS = nil, nil
		observation.ClientFailureRate, observation.ServiceFailureRate = nil, nil
		observation.ClientNetworkMS, observation.ServiceNetworkMS = number(150), number(160)
		if observation.EdgeID == "edge-b" {
			observation.ClientNetworkMS, observation.ServiceNetworkMS = number(180), number(20)
		}
	}
	return snapshot
}

func TestNetworkV2UnknownOptionalMetricsDoNotPermanentlyExclude(t *testing.T) {
	snapshot := networkFixture()
	result := evaluateTest(t, snapshot)
	if result.Hypothesis != "switch" || result.ProposedEdgeID != "edge-b" || result.SustainedBuckets != 3 || len(result.Comparisons) != 1 {
		t.Fatal(result)
	}
	for _, candidate := range result.Candidates {
		if !candidate.Ready || len(candidate.Missing) != 4 || candidate.Metrics["download_bps"].State != "unknown" {
			t.Fatal("optional unknowns became measured or permanently excluded", candidate)
		}
		if candidate.Upper-candidate.Lower > snapshot.Policy.UnknownCostMS+4*snapshot.Policy.UncertaintyMS {
			t.Fatal("missing metrics multiplied uncertainty without bound", candidate)
		}
	}
	receipt, err := Capture(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(receipt)
	var decoded Receipt
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if replay, err := Replay(decoded); err != nil || !reflect.DeepEqual(replay, result) {
		t.Fatal("v2 offline replay differs", replay, err)
	}
}

func TestNetworkV2CannotCompareUnrelatedClientPopulations(t *testing.T) {
	snapshot := networkFixture()
	for index := range snapshot.Observations {
		if snapshot.Observations[index].EdgeID == "edge-b" {
			snapshot.Observations[index].ClientCohort = "tcp_peer:198.51.100.0/24"
		}
	}
	result := evaluateTest(t, snapshot)
	if result.Hypothesis != "hold" || len(result.Comparisons) != 0 || len(result.ProbeEdgeIDs) > snapshot.Policy.ProbeBudgetPerInterval {
		t.Fatal("unrelated client networks were averaged into a global winner", result)
	}
	for _, cohort := range []string{"", "global", "asn:64500", "tcp_peer:203.0.113.0/32", "tcp_peer:2001:db8::/128", "tcp_peer:203.0.113.2/24"} {
		snapshot = networkFixture()
		for index := range snapshot.Observations {
			snapshot.Observations[index].ClientCohort = cohort
		}
		if result := evaluateTest(t, snapshot); result.Hypothesis != "hold" {
			t.Fatal("unsupported or identifying cohort accepted", cohort, result)
		}
	}
}

func TestNetworkV2RequiresEveryCommonCohortToBenefit(t *testing.T) {
	snapshot := networkFixture()
	for _, observation := range append([]Observation(nil), snapshot.Observations...) {
		observation.ID += "-second"
		observation.ClientCohort = "tcp_peer:198.51.100.0/24"
		observation.ClientNetworkMS = number(20)
		if observation.EdgeID == "edge-b" {
			observation.ClientNetworkMS = number(250)
		}
		snapshot.Observations = append(snapshot.Observations, observation)
	}
	result := evaluateTest(t, snapshot)
	if result.Hypothesis != "hold" || len(result.Comparisons) != 2 {
		t.Fatal("a materially worse common client cohort was hidden", result)
	}
}

func TestNetworkV2PreservesCoreGatesCooldownAndFastFailure(t *testing.T) {
	for _, scenario := range []string{"missing_client", "missing_service", "missing_capacity", "untrusted_capacity", "capacity_stale", "capacity_pressure", "proof_stale", "one_bucket", "cooldown", "unknown_switch", "snapshot_blocker"} {
		t.Run(scenario, func(t *testing.T) {
			snapshot := networkFixture()
			for index := range snapshot.Observations {
				observation := &snapshot.Observations[index]
				if observation.EdgeID != "edge-b" {
					continue
				}
				switch scenario {
				case "missing_client":
					observation.ClientNetworkMS = nil
				case "missing_service":
					observation.ServiceNetworkMS = nil
				case "missing_capacity":
					observation.CapacityUtilization = nil
				case "untrusted_capacity":
					observation.CapacitySource = "active_requests"
				case "capacity_stale":
					if snapshot.CapturedAt.Sub(observation.ObservedAt) < 2*time.Minute {
						observation.CapacityUtilization = nil
					}
				case "capacity_pressure":
					observation.CapacityUtilization = number(0.9)
				case "one_bucket":
					if snapshot.CapturedAt.Sub(observation.ObservedAt) > 5*time.Minute {
						observation.ServiceNetworkMS = number(500)
					}
				}
			}
			switch scenario {
			case "proof_stale":
				stale := snapshot.CapturedAt.Add(-time.Hour)
				snapshot.Candidates[1].ProofObservedAt = &stale
			case "cooldown":
				snapshot.LastSwitchAt = &snapshot.CapturedAt
			case "unknown_switch":
				snapshot.LastSwitchAt = nil
			case "snapshot_blocker":
				snapshot.Blockers = []string{"actual_dns_receipt_not_bound"}
			}
			if result := evaluateTest(t, snapshot); result.Hypothesis != "hold" {
				t.Fatal("unsafe switch", scenario, result)
			}
		})
	}
	snapshot := networkFixture()
	snapshot.Candidates[0].HardGates = []string{"route_unavailable"}
	snapshot.LastSwitchAt = &snapshot.CapturedAt
	if result := evaluateTest(t, snapshot); result.Hypothesis != "failover" || result.ProposedEdgeID != "edge-b" {
		t.Fatal("cooldown blocked fault failover", result)
	}
}

func TestNetworkV2CapacityRecoveryAndBusinessWaitSeparation(t *testing.T) {
	snapshot := networkFixture()
	baseline := evaluateTest(t, snapshot)
	for index := range snapshot.Observations {
		snapshot.Observations[index].Diagnostics = map[string]float64{"model_wait_ms": 1e9, "total_ms": 1e10, "http_error_rate": 1}
	}
	if got := evaluateTest(t, snapshot); !reflect.DeepEqual(got, baseline) {
		t.Fatal("application wait entered the network score")
	}
	old := snapshot.Observations[len(snapshot.Observations)-1]
	old.ID, old.ObservedAt, old.CapacityUtilization = "old-capacity-pressure", snapshot.CapturedAt.Add(-20*time.Minute), number(1)
	old.ClientNetworkMS, old.ServiceNetworkMS = nil, nil
	snapshot.Observations = append(snapshot.Observations, old)
	result := evaluateTest(t, snapshot)
	if result.Hypothesis != "switch" {
		t.Fatal("recovered capacity stayed permanently excluded", result)
	}
	for _, candidate := range result.Candidates {
		if strings.Contains(strings.Join(candidate.HardGates, ","), "capacity") {
			t.Fatal("obsolete capacity pressure became a live gate", candidate)
		}
	}
}

func TestNetworkV2PreservesUnknownHealthyEdgesOnlyAsFallback(t *testing.T) {
	snapshot := networkFixture()
	snapshot.Scope = "global"
	for index := range snapshot.Observations {
		snapshot.Observations[index].Scope = snapshot.Scope
	}
	snapshot.Candidates = append(snapshot.Candidates,
		Candidate{EdgeID: "edge-c", EdgeGroupID: "remote", RouteGeneration: "route-c", RouteProofVerified: true, ProofObservedAt: &snapshot.CapturedAt},
		Candidate{EdgeID: "edge-d", EdgeGroupID: "remote", RouteGeneration: "route-d", RouteProofVerified: true, ProofObservedAt: &snapshot.CapturedAt, HardGates: []string{"tls_invalid"}})
	receipt, err := Capture(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	binding := DNSBinding{ReceiptID: "actual-a", LoadedDigest: digest, PolicyDigest: digest, Hostname: snapshot.Hostname, Scope: snapshot.Scope,
		CurrentEdgeID: snapshot.CurrentEdgeID, ObservedAt: snapshot.CapturedAt.Add(-time.Second), ReplayMatched: true, WriteSucceeded: true}
	selection, err := CompileSelection(receipt, binding, snapshot.CapturedAt)
	if err != nil || !reflect.DeepEqual(selection.OrderedEdgeIDs, []string{"edge-b", "edge-a", "edge-c"}) {
		t.Fatal("unknown fallback lost, invalid fallback admitted or normal winner overridden", selection, err)
	}
}

func TestNetworkV2ObservationOrderDoesNotChangeResult(t *testing.T) {
	snapshot := networkFixture()
	before := evaluateTest(t, snapshot)
	for left, right := 0, len(snapshot.Observations)-1; left < right; left, right = left+1, right-1 {
		snapshot.Observations[left], snapshot.Observations[right] = snapshot.Observations[right], snapshot.Observations[left]
	}
	if after := evaluateTest(t, snapshot); !reflect.DeepEqual(before, after) {
		t.Fatal("observation insertion order changed quality decision", before, after)
	}
}
