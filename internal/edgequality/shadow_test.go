package edgequality

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"
)

func number(value float64) *float64 { return &value }

func fixture() Snapshot {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	lastSwitch := now.Add(-time.Hour)
	policy := DefaultShadowPolicy()
	policy.UncertaintyMS = 1
	snapshot := Snapshot{Schema: Schema, CapturedAt: now, Hostname: "app.example.test", TrafficClass: "streaming", Scope: "asn:example", Policy: policy,
		CurrentEdgeID: "edge-a", LastSwitchAt: &lastSwitch, Blockers: []string{}, Candidates: []Candidate{
			{EdgeID: "edge-a", EdgeGroupID: "shared", RouteGeneration: "route-1", RouteProofVerified: true, ProofObservedAt: &now, HardGates: []string{}},
			{EdgeID: "edge-b", EdgeGroupID: "shared", RouteGeneration: "route-1", RouteProofVerified: true, ProofObservedAt: &now, HardGates: []string{}},
		}}
	for _, edgeID := range []string{"edge-a", "edge-b"} {
		for bucket := 1; bucket <= 3; bucket++ {
			for record := 1; record <= 3; record++ {
				client, service := 180.0, 180.0
				if edgeID == "edge-b" {
					client, service = 20, 20
				}
				snapshot.Observations = append(snapshot.Observations, Observation{ID: fmt.Sprintf("%s-%d-%d", edgeID, bucket, record), EdgeID: edgeID,
					Hostname: snapshot.Hostname, TrafficClass: snapshot.TrafficClass, Scope: snapshot.Scope, RouteGeneration: "route-1",
					ObservedAt:      now.Add(-time.Duration(bucket)*5*time.Minute + time.Duration(record)*time.Second),
					ClientNetworkMS: number(client), ServiceNetworkMS: number(service), ClientSource: "public_tcp_info", ServiceSource: "service_endpoint_tcp",
					UploadBPS: number(2 << 20), DownloadBPS: number(2 << 20), ClientFailureRate: number(0), ServiceFailureRate: number(0), CapacityUtilization: number(0.1),
					Diagnostics: map[string]float64{"model_wait_ms": 60000, "connection_lifetime_ms": 900000}})
			}
		}
	}
	return snapshot
}

func evaluateTest(t *testing.T, snapshot Snapshot) Result {
	t.Helper()
	result, err := Evaluate(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !result.DNSUnchanged || result.PromotionReady || result.ProbeExecuted || result.Mode != "shadow" {
		t.Fatal("shadow acquired serving authority", result)
	}
	return result
}

func TestPhysicalQualitySustainedPerEdgeAndReplay(t *testing.T) {
	snapshot := fixture()
	result := evaluateTest(t, snapshot)
	if result.Hypothesis != "switch" || result.ProposedEdgeID != "edge-b" || result.SustainedBuckets != 3 {
		t.Fatal(result)
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
	if replayed, err := Replay(decoded); err != nil || !reflect.DeepEqual(result, replayed) {
		t.Fatal(err, replayed)
	}
	decoded.Snapshot.Policy.AdvantageMS++
	if _, err := Replay(decoded); err == nil {
		t.Fatal("accepted altered policy")
	}
}

func TestPhysicalQualityNeverScoresApplicationWait(t *testing.T) {
	snapshot := fixture()
	before := evaluateTest(t, snapshot)
	for index := range snapshot.Observations {
		snapshot.Observations[index].Diagnostics = map[string]float64{"ttfb_ms": 1e8, "http_5xx_rate": 1, "model_wait_ms": 1e9, "app_queue_ms": 1e9, "total_ms": 1e10}
	}
	after := evaluateTest(t, snapshot)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("business wait or application failures changed network score")
	}
}

func TestPhysicalQualityUnknownBoundedAndProbesRotate(t *testing.T) {
	snapshot := fixture()
	snapshot.Observations = nil
	result := evaluateTest(t, snapshot)
	if result.Hypothesis != "hold" || len(result.ProbeEdgeIDs) != 1 {
		t.Fatal(result)
	}
	for _, candidate := range result.Candidates {
		if candidate.Ready || candidate.Score != 7*snapshot.Policy.UnknownCostMS || candidate.Metrics["client_failure_rate"].State != "unknown" {
			t.Fatal(candidate)
		}
	}
	first := result.ProbeEdgeIDs[0]
	snapshot.CapturedAt = snapshot.CapturedAt.Add(time.Duration(snapshot.Policy.ProbeIntervalSeconds) * time.Second)
	if next := evaluateTest(t, snapshot).ProbeEdgeIDs; len(next) != 1 || next[0] == first {
		t.Fatal("unknown edge permanently starved", next)
	}
	snapshot.Candidates[1].HardGates = []string{"draining"}
	if next := evaluateTest(t, snapshot).ProbeEdgeIDs; len(next) != 1 || next[0] != "edge-a" {
		t.Fatal("probed hard-gated edge", next)
	}
}

func TestPhysicalQualityPromotionGates(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Snapshot)
	}{
		{"no incumbent", func(snapshot *Snapshot) { snapshot.CurrentEdgeID = "" }},
		{"incomplete snapshot", func(snapshot *Snapshot) { snapshot.Blockers = []string{"observation_limit_reached"} }},
		{"unknown cooldown", func(snapshot *Snapshot) { snapshot.LastSwitchAt = nil }},
		{"cooldown", func(snapshot *Snapshot) { now := snapshot.CapturedAt; snapshot.LastSwitchAt = &now }},
		{"missing route proof", func(snapshot *Snapshot) { snapshot.Candidates[1].RouteProofVerified = false }},
		{"stale route proof", func(snapshot *Snapshot) {
			old := snapshot.CapturedAt.Add(-time.Hour)
			snapshot.Candidates[1].ProofObservedAt = &old
		}},
		{"different route", func(snapshot *Snapshot) { snapshot.Candidates[1].RouteGeneration = "route-2" }},
		{"incomplete evidence", func(snapshot *Snapshot) {
			for index := range snapshot.Observations {
				snapshot.Observations[index].CapacityUtilization = nil
			}
		}},
		{"unverified client hop", func(snapshot *Snapshot) {
			for index := range snapshot.Observations {
				snapshot.Observations[index].ClientSource = "caddy_loopback"
			}
		}},
		{"unverified origin hop", func(snapshot *Snapshot) {
			for index := range snapshot.Observations {
				snapshot.Observations[index].ServiceSource = "local_tunnel_listener"
			}
		}},
		{"stale measurements", func(snapshot *Snapshot) {
			for index := range snapshot.Observations {
				snapshot.Observations[index].ObservedAt = snapshot.Observations[index].ObservedAt.Add(-10 * time.Minute)
			}
		}},
		{"one bucket advantage", func(snapshot *Snapshot) {
			for index := range snapshot.Observations {
				observation := &snapshot.Observations[index]
				if observation.EdgeID == "edge-b" && observation.ObservedAt.Before(snapshot.CapturedAt.Add(-5*time.Minute)) {
					observation.ClientNetworkMS = number(180)
					observation.ServiceNetworkMS = number(180)
				}
			}
		}},
		{"cross class", func(snapshot *Snapshot) {
			for index := range snapshot.Observations {
				snapshot.Observations[index].TrafficClass = "static_cacheable"
			}
		}},
		{"cross hostname", func(snapshot *Snapshot) {
			for index := range snapshot.Observations {
				snapshot.Observations[index].Hostname = "other.example.test"
			}
		}},
		{"cross scope", func(snapshot *Snapshot) {
			for index := range snapshot.Observations {
				snapshot.Observations[index].Scope = "global"
			}
		}},
		{"future measurements", func(snapshot *Snapshot) {
			for index := range snapshot.Observations {
				snapshot.Observations[index].ObservedAt = snapshot.CapturedAt.Add(time.Second)
			}
		}},
	}
	for _, testcase := range cases {
		t.Run(testcase.name, func(t *testing.T) {
			snapshot := fixture()
			testcase.mutate(&snapshot)
			if result := evaluateTest(t, snapshot); result.Hypothesis != "hold" {
				t.Fatal(result)
			}
		})
	}
}

func TestPhysicalQualityHardFailureBypassesCooldownButNotProof(t *testing.T) {
	snapshot := fixture()
	now := snapshot.CapturedAt
	snapshot.LastSwitchAt = &now
	snapshot.Candidates[0].HardGates = []string{"unhealthy"}
	result := evaluateTest(t, snapshot)
	if result.Hypothesis != "failover" || result.ProposedEdgeID != "edge-b" {
		t.Fatal(result)
	}
	snapshot.Candidates[1].RouteProofVerified = false
	if result := evaluateTest(t, snapshot); result.Hypothesis != "hold" {
		t.Fatal("failed over to unproven hostname", result)
	}
}

func TestPhysicalQualityBadInputAndDuplicateSamples(t *testing.T) {
	snapshot := fixture()
	snapshot.Observations = append(snapshot.Observations, snapshot.Observations...)
	result := evaluateTest(t, snapshot)
	if result.RejectedRecords["duplicate_id"] != 18 || result.Candidates[0].RecordCount != 9 {
		t.Fatal(result)
	}
	for _, value := range []float64{-1, math.NaN(), math.Inf(1)} {
		snapshot = fixture()
		snapshot.Observations[0].ClientNetworkMS = &value
		if _, err := Evaluate(snapshot); err == nil {
			t.Fatal("accepted invalid numeric metric")
		}
	}
	snapshot = fixture()
	snapshot.Policy.BucketSeconds = 0
	if _, err := Evaluate(snapshot); err == nil {
		t.Fatal("accepted unbounded policy")
	}
}
