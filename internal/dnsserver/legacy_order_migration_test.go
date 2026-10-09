package dnsserver

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/dnslegacy"
	"fugue/internal/model"
)

func TestLegacyMigrationFreezesOnlyNormalGlobalOrder(t *testing.T) {
	for _, mode := range []string{"geo", "latency_aware", "weighted", "global", "pinned"} {
		for _, selected := range []string{"", "group-b"} {
			for _, scored := range []bool{false, true} {
				record := model.EdgeDNSRecord{Name: "app.example.test", Type: "A", AnswerPolicy: model.DNSAnswerPolicy{PolicyKind: mode, SelectedEdgeGroupID: selected, ExplorationPercent: 5}, Candidates: []model.EdgeDNSAnswerCandidate{
					{IP: "8.8.8.8", EdgeID: "edge-a", EdgeGroupID: "group-a", Healthy: true, RouteReady: true, TLSReady: true, Weight: 100, Priority: 1, Reason: "SAME_REGION"},
					{IP: "9.9.9.9", EdgeID: "edge-b", EdgeGroupID: "group-b", Healthy: true, RouteReady: true, TLSReady: true, Weight: 150, Priority: 3},
					{IP: "1.1.1.1", EdgeID: "edge-c", EdgeGroupID: "group-b", Healthy: true, RouteReady: true, TLSReady: true, Weight: 120, Priority: 2},
				}}
				if scored {
					record.Candidates[1].Score, record.Candidates[2].Score = 67.125, 812.25
				}
				before := record.AnswerPolicy
				order, err := dnslegacy.GlobalOrder(record)
				if err != nil || !reflect.DeepEqual(before, record.AnswerPolicy) {
					t.Fatal("migration modified signed configuration", err)
				}
				record.AnswerPolicy.ExplorationPercent = 0
				candidates, _ := edgeDNSOrderedCandidatesWithDecision(record, dnsGeoHint{}, time.Time{}, false)
				actual := []string{}
				for _, candidate := range candidates {
					actual = append(actual, candidate.EdgeID)
				}
				if !reflect.DeepEqual(order.OrderedEdgeIDs, actual) {
					t.Fatalf("%s/%s/%v migration changed normal order: %v != %v", mode, selected, scored, order.OrderedEdgeIDs, actual)
				}
			}
		}
	}
}
