package edgequality

import (
	"fmt"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func passiveASNFixture() ([]model.EdgeNetworkSample, map[string]PassiveEdgeIdentity, time.Time) {
	now := time.Date(2026, 1, 1, 0, 20, 0, 0, time.UTC)
	identities := map[string]PassiveEdgeIdentity{
		"edge-a": {"group-a", "192.0.2.2", "2001:db8:a::1", "bundle-a"},
		"edge-b": {"group-b", "192.0.2.3", "2001:db8:b::1", "bundle-b"},
	}
	samples := []model.EdgeNetworkSample{}
	for window := 1; window <= 3; window++ {
		for i, edge := range []string{"edge-a", "edge-b"} {
			for conn := 0; conn < 3; conn++ {
				at := now.Add(-time.Duration(window)*PassiveASNInterval + time.Minute)
				id, rtt := fmt.Sprintf("%s-%d-%d", edge, window, conn), 100-float64(i*40)
				samples = append(samples, model.EdgeNetworkSample{
					ID: id, EdgeID: edge, EdgeGroupID: identities[edge].GroupID, Hostname: fmt.Sprintf("route-%d.example.test", i),
					PathPrefix: "/", TrafficClass: "streaming", RouteDigest: "sha256:" + strings.Repeat("a", 64),
					BundleVersion: identities[edge].Bundle, Source: "public_front_tcp_info_v1", ObservedAt: at,
					ClientNetwork: &model.EdgeClientNetworkSample{ConnectionID: id, Slot: "a", Scope: "tcp_peer:198.51.100.0/24",
						StartedAt: at.Add(-time.Second), ObservedAt: at, TCPInfoAvailable: true, RTTMS: &rtt},
				})
			}
		}
	}
	return samples, identities, now
}

func TestPassiveASNComparesAcrossHostsAndReplaysDeterministically(t *testing.T) {
	samples, ids, now := passiveASNFixture()
	resolve := func(netip.Prefix) uint32 { return 64496 }
	results, coverage := ComparePassiveASN(samples, ids, resolve, now)
	if len(results) != 1 || results[0].ASN != 64496 || results[0].Family != 4 || results[0].Preferred != "edge-b" ||
		results[0].RightWinWindows != 3 || !results[0].ObservedAt.Equal(now.Add(-4*time.Minute)) ||
		coverage.Connections != 18 || coverage.ClassifiedRTT != 18 {
		t.Fatalf("unexpected passive comparison: %+v %+v", results, coverage)
	}
	slices.Reverse(samples)
	replay, replayCoverage := ComparePassiveASN(samples, ids, resolve, now.Add(time.Second))
	if !reflect.DeepEqual(results, replay) || replayCoverage != coverage {
		t.Fatal("reordering input or moving capture clock changed evidence", results, replay)
	}
}

func TestPassiveASNDoesNotInferFromMissingOrNonComparableEvidence(t *testing.T) {
	for _, test := range []string{"unmapped", "different prefix same ASN", "address family", "same connection repeated", "open window", "wrong bundle", "upstream"} {
		t.Run(test, func(t *testing.T) {
			samples, ids, now := passiveASNFixture()
			resolve := func(netip.Prefix) uint32 { return 64496 }
			if test == "unmapped" {
				resolve = func(netip.Prefix) uint32 { return 0 }
			}
			for i := range samples {
				sample := &samples[i]
				if sample.EdgeID != "edge-b" {
					continue
				}
				switch test {
				case "different prefix same ASN":
					sample.ClientNetwork.Scope = "tcp_peer:203.0.113.0/24"
				case "address family":
					sample.ClientNetwork.Scope = "tcp_peer:2001:db8:1::/48"
				case "same connection repeated":
					sample.ClientNetwork.ConnectionID = "same"
					sample.ClientNetwork.StartedAt = now.Add(-20 * time.Minute)
				case "open window":
					sample.ObservedAt, sample.ClientNetwork.ObservedAt = now, now
				case "wrong bundle":
					sample.BundleVersion = "superseded"
				case "upstream":
					sample.Source = "service_endpoint_tcp_info_v1"
				}
			}
			if rows, _ := ComparePassiveASN(samples, ids, resolve, now); len(rows) != 0 {
				t.Fatalf("unexpected result: %+v", rows)
			}
		})
	}
}

func TestPassiveASNConflictingPathsDoNotElectWinner(t *testing.T) {
	samples, ids, now := passiveASNFixture()
	rtt := 180.0
	for i := range samples {
		sample := &samples[i]
		if sample.EdgeID == "edge-b" && sample.ObservedAt.Before(now.Add(-10*time.Minute)) {
			sample.ClientNetwork.RTTMS = &rtt
		}
	}
	rows, _ := ComparePassiveASN(samples, ids, func(netip.Prefix) uint32 { return 64496 }, now)
	if len(rows) != 1 || rows[0].Preferred != "" || rows[0].LeftWinWindows != 1 || rows[0].RightWinWindows != 2 {
		t.Fatalf("unexpected conflict result: %+v", rows)
	}
}
