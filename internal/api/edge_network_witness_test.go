package api

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
)

func networkWitnessFixture(now time.Time) (model.EdgeNetworkSample, routeprobe.Proof) {
	value := 8.25
	sample := model.EdgeNetworkSample{ID: "sample-a", EdgeID: "edge-a", EdgeGroupID: "group-a", Hostname: "app.example.test", PathPrefix: "/",
		TrafficClass: "streaming", RouteDigest: "sha256:" + strings.Repeat("a", 64), BundleVersion: "bundle-a", ServiceTarget: "app.tenant.svc.cluster.local:3000",
		Source: "service_endpoint_tcp_info_v1", ServiceRTTMS: &value, ObservedAt: now.Add(-time.Second)}
	proof := routeprobe.Proof{EdgeID: sample.EdgeID, GroupID: sample.EdgeGroupID, Digest: sample.RouteDigest, Version: sample.BundleVersion, CheckedAt: now, ValidUntil: now.Add(time.Minute)}
	return sample, proof
}

func TestNetworkWitnessRequiresExactPublicTLSProof(t *testing.T) {
	now := time.Now().UTC()
	for _, test := range []struct {
		name string
		edit func(*model.EdgeNetworkSample, *routeprobe.Proof)
	}{
		{"physical_edge", func(sample *model.EdgeNetworkSample, proof *routeprobe.Proof) { proof.EdgeID = "edge-b" }},
		{"group", func(sample *model.EdgeNetworkSample, proof *routeprobe.Proof) { proof.GroupID = "group-b" }},
		{"bundle_missing", func(sample *model.EdgeNetworkSample, proof *routeprobe.Proof) { proof.Version = "" }},
		{"digest", func(sample *model.EdgeNetworkSample, proof *routeprobe.Proof) {
			proof.Digest = "sha256:" + strings.Repeat("b", 64)
		}},
		{"excluded", func(sample *model.EdgeNetworkSample, proof *routeprobe.Proof) { proof.State = "excluded" }},
		{"future", func(sample *model.EdgeNetworkSample, proof *routeprobe.Proof) { proof.CheckedAt = now.Add(time.Second) }},
		{"stale", func(sample *model.EdgeNetworkSample, proof *routeprobe.Proof) {
			proof.CheckedAt = now.Add(-time.Minute)
		}},
		{"expired", func(sample *model.EdgeNetworkSample, proof *routeprobe.Proof) { proof.ValidUntil = now }},
		{"old_sample", func(sample *model.EdgeNetworkSample, proof *routeprobe.Proof) {
			sample.ObservedAt = now.Add(-3 * time.Minute)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sample, proof := networkWitnessFixture(now)
			test.edit(&sample, &proof)
			if _, err := networkRouteWitnessSample(sample, "203.0.113.5", proof, now); err == nil {
				t.Fatal("invalid witness accepted")
			}
		})
	}
	sample, proof := networkWitnessFixture(now)
	witness, err := networkRouteWitnessSample(sample, "203.0.113.5", proof, now)
	if err != nil || witness.ServiceRTTMS != nil || witness.ClientNetwork != nil || !model.EdgeNetworkWitnessMatches(sample, witness) {
		t.Fatal("witness fabricated a metric or lost identity", witness, err)
	}
	active := true
	request := edgeHeartbeatRequest{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, RouteBundleVersion: sample.BundleVersion, NetworkSamples: []model.EdgeNetworkSample{witness}}
	if len(sanitizeEdgeNetworkSamples(request, &active, now)) != 0 {
		t.Fatal("heartbeat caller forged independent witness")
	}
}

func TestNetworkWitnessRenewalRecordsActualVersionWithoutRebindingOldSample(t *testing.T) {
	now := time.Now().UTC()
	sample, proof := networkWitnessFixture(now)
	proof.Version = "renewed-bundle"
	witness, err := networkRouteWitnessSample(sample, "203.0.113.5", proof, now)
	if err != nil || witness.BundleVersion != proof.Version || witness.BundleVersion == sample.BundleVersion || model.EdgeNetworkWitnessMatches(sample, witness) {
		t.Fatal("bundle renewal relabeled an old measurement", witness, err)
	}
	next := sample
	next.ID, next.BundleVersion, next.ObservedAt = "subsequent-real-sample", proof.Version, now.Add(time.Second)
	if !model.EdgeNetworkWitnessMatches(next, witness) {
		t.Fatal("actual renewed-bundle witness cannot bind a subsequent matching measurement")
	}
}

func TestHistoricalNetworkWitnessSurvivesOnlyIdenticalRouteContent(t *testing.T) {
	now := time.Now().UTC()
	sample, proof := networkWitnessFixture(now.Add(-5 * time.Minute))
	witness, err := networkRouteWitnessSample(sample, "203.0.113.5", proof, proof.CheckedAt)
	if err != nil {
		t.Fatal(err)
	}
	current := proof
	current.Version, current.CheckedAt, current.ValidUntil = "renewed-bundle", now, now.Add(time.Minute)
	evidence := dnsserver.QualityAnswerEvidence{EdgeID: sample.EdgeID, Hostname: sample.Hostname, Scope: "global", Proofs: []dnsserver.QualityRouteProof{{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, Hostname: sample.Hostname, Path: sample.PathPrefix, Proof: current}}}
	snapshot := edgequality.Snapshot{Schema: edgequality.Schema, CapturedAt: now, Hostname: sample.Hostname, TrafficClass: sample.TrafficClass,
		Scope: "global", Policy: edgequality.DefaultShadowPolicy(), Candidates: []edgequality.Candidate{{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID}},
		NetworkSamples: []model.EdgeNetworkSample{sample, witness}}
	for _, test := range []struct {
		name string
		edit func(*edgequality.Snapshot, *dnsserver.QualityAnswerEvidence)
		want int
	}{
		{"historical_exact", func(snapshot *edgequality.Snapshot, evidence *dnsserver.QualityAnswerEvidence) {}, 1},
		{"missing_witness", func(snapshot *edgequality.Snapshot, evidence *dnsserver.QualityAnswerEvidence) {
			snapshot.NetworkSamples = snapshot.NetworkSamples[:1]
		}, 0},
		{"foreign_witness", func(snapshot *edgequality.Snapshot, evidence *dnsserver.QualityAnswerEvidence) {
			snapshot.NetworkSamples[1].EdgeID = "edge-b"
		}, 0},
		{"foreign_path", func(snapshot *edgequality.Snapshot, evidence *dnsserver.QualityAnswerEvidence) {
			snapshot.NetworkSamples[1].PathPrefix = "/other"
		}, 0},
		{"no_current_proof", func(snapshot *edgequality.Snapshot, evidence *dnsserver.QualityAnswerEvidence) { evidence.Proofs = nil }, 0},
		{"changed_route", func(snapshot *edgequality.Snapshot, evidence *dnsserver.QualityAnswerEvidence) {
			evidence.Proofs[0].Proof.Digest = "sha256:" + strings.Repeat("c", 64)
		}, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			modified := snapshot
			modified.Candidates = append([]edgequality.Candidate(nil), snapshot.Candidates...)
			modified.NetworkSamples = append([]model.EdgeNetworkSample(nil), snapshot.NetworkSamples...)
			proofs := evidence
			proofs.Proofs = append([]dnsserver.QualityRouteProof(nil), evidence.Proofs...)
			test.edit(&modified, &proofs)
			bindPhysicalQualityEvidence(&modified, proofs)
			if len(modified.Observations) != test.want {
				t.Fatal(modified.Observations)
			}
			if test.want == 0 {
				return
			}
			if modified.Observations[0].RouteWitnessID != witness.ID {
				t.Fatal("historical linkage omitted")
			}
			receipt, err := edgequality.Capture(modified)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := edgequality.Replay(receipt); err != nil {
				t.Fatal(err)
			}
			modified.NetworkSamples = modified.NetworkSamples[:1]
			if _, err := edgequality.Capture(modified); err == nil {
				t.Fatal("replay evaluator trusts derived historical measurement without witness")
			}
		})
	}
}

func TestNetworkWitnessBudgetBoundedAndRecovers(t *testing.T) {
	var state networkRouteWitnessState
	now := time.Now().UTC()
	reserve := func(edgeID string, at time.Time) bool {
		sample, _ := networkWitnessFixture(at)
		sample.EdgeID = edgeID
		_, ok := state.selectSample(model.EdgeNode{ID: edgeID, EdgeGroupID: sample.EdgeGroupID}, []model.EdgeNetworkSample{sample}, at)
		return ok
	}
	for _, edgeID := range []string{"a", "b", "c", "d"} {
		if !reserve(edgeID, now) {
			t.Fatal("initial reservation rejected")
		}
	}
	if reserve("e", now) || reserve("a", now.Add(30*time.Second)) {
		t.Fatal("probe budget exceeded")
	}
	state.release()
	if !reserve("e", now) {
		t.Fatal("probe slot leaked")
	}
	state.release()
	if !reserve("a", now.Add(time.Minute)) {
		t.Fatal("probe cooldown never recovered")
	}
}

func TestNetworkWitnessSelectionKeepsFreshExactSamplesAndBounds(t *testing.T) {
	now := time.Now().UTC()
	for _, test := range []struct {
		name string
		edit func(*model.EdgeNetworkSample)
	}{
		{"foreign_node", func(sample *model.EdgeNetworkSample) { sample.EdgeID = "edge-other" }},
		{"foreign_group", func(sample *model.EdgeNetworkSample) { sample.EdgeGroupID = "group-other" }},
		{"stale", func(sample *model.EdgeNetworkSample) { sample.ObservedAt = now.Add(-91 * time.Second) }},
		{"future", func(sample *model.EdgeNetworkSample) { sample.ObservedAt = now.Add(time.Second) }},
		{"invalid", func(sample *model.EdgeNetworkSample) { sample.RouteDigest = "untrusted" }},
		{"unknown_source", func(sample *model.EdgeNetworkSample) { sample.Source = "unknown" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			var state networkRouteWitnessState
			sample, _ := networkWitnessFixture(now)
			node := model.EdgeNode{ID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID}
			test.edit(&sample)
			if _, ok := state.selectSample(node, []model.EdgeNetworkSample{sample}, now); ok || state.inFlight != 0 || len(state.last) != 0 {
				t.Fatal("invalid sample reserved observation budget")
			}
		})
	}
	var state networkRouteWitnessState
	sample, _ := networkWitnessFixture(now)
	node := model.EdgeNode{ID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID}
	older := sample
	older.ID, older.BundleVersion, older.ObservedAt = "sample-older", "old-bundle", now.Add(-time.Minute)
	selected, ok := state.selectSample(node, []model.EdgeNetworkSample{older, sample}, now)
	if !ok || selected.ID != sample.ID || selected.BundleVersion != sample.BundleVersion {
		t.Fatal("rotation replaced the newest actual route sample")
	}
	state.release()
	state.mu.Lock()
	if _, ok := state.selectSample(node, []model.EdgeNetworkSample{sample}, now); ok {
		t.Fatal("observer must not block while state is busy")
	}
	state.mu.Unlock()
	state.last = map[string]networkRouteWitnessCursor{}
	for index := 0; index < 256; index++ {
		state.last[fmt.Sprintf("edge-%d", index)] = networkRouteWitnessCursor{at: now}
	}
	if _, ok := state.selectSample(node, []model.EdgeNetworkSample{sample}, now); ok || len(state.last) != 256 {
		t.Fatal("cursor memory bound exceeded")
	}
	sample.ObservedAt = now.Add(10 * time.Minute)
	if _, ok := state.selectSample(node, []model.EdgeNetworkSample{sample}, sample.ObservedAt); !ok || len(state.last) != 1 {
		t.Fatal("idle cursors never expire")
	}
}

func TestNetworkWitnessRotationDoesNotStarveLessRecentRoutes(t *testing.T) {
	var state networkRouteWitnessState
	now := time.Now().UTC()
	counts := map[string]int{}
	for interval := 0; interval < 6; interval++ {
		at := now.Add(time.Duration(interval) * time.Minute)
		hot, _ := networkWitnessFixture(at)
		hot.Hostname = "hot.example.test"
		quiet := hot
		quiet.Hostname, quiet.ObservedAt = "quiet.example.test", at.Add(-30*time.Second)
		sample, ok := state.selectSample(model.EdgeNode{ID: hot.EdgeID, EdgeGroupID: hot.EdgeGroupID}, []model.EdgeNetworkSample{quiet, hot}, at)
		if !ok {
			t.Fatal("eligible sample not scheduled")
		}
		counts[sample.Hostname]++
		state.release()
	}
	if counts["hot.example.test"] != 3 || counts["quiet.example.test"] != 3 {
		t.Fatalf("freshest route starves an eligible lower-traffic route: %v", counts)
	}
}
