package routeobservation

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"fugue/internal/model"
)

func TestDecisionDedupSeparatesRouteAndIntentAndNormalizesRouteKeys(t *testing.T) {
	var decisions Decisions
	route := model.EdgeRouteBinding{Hostname: " EXAMPLE.TEST. ", PathPrefix: "/", RouteKind: " http ", EdgeGroupID: " edge ", DecisionID: " decision "}
	if !decisions.RouteChanged(route) {
		t.Fatal("first route decision suppressed")
	}
	route.Hostname, route.RouteKind, route.EdgeGroupID, route.DecisionID = "example.test", "http", "edge", "decision"
	if decisions.RouteChanged(route) {
		t.Fatal("equivalent route key not deduplicated")
	}
	if !decisions.IntentChanged("app-1", route.Hostname, "/", "http", route.DecisionID) ||
		decisions.IntentChanged("app-1", "EXAMPLE.TEST.", "/", "http", route.DecisionID) ||
		!decisions.IntentChanged("app-2", route.Hostname, "/", "http", route.DecisionID) {
		t.Fatal("intent identity must normalize the host without merging distinct apps or routes")
	}
	route.DecisionID = "decision-2"
	if !decisions.RouteChanged(route) || decisions.RouteChanged(route) {
		t.Fatal("changed decision must emit exactly once")
	}
}

func TestDecisionDedupRemainsBoundedAndAtomic(t *testing.T) {
	var decisions Decisions
	var wins atomic.Int32
	var group sync.WaitGroup
	for range 48 {
		group.Go(func() {
			if decisions.IntentChanged("first", "example.test", "/", "http", "same") {
				wins.Add(1)
			}
		})
	}
	group.Wait()
	if wins.Load() != 1 {
		t.Fatalf("duplicate decisions emitted %d times", wins.Load())
	}
	for i := 1; i < maxRememberedObservations; i++ {
		decisions.IntentChanged(fmt.Sprint(i), "example.test", "/", "http", "unique")
	}
	if decisions.IntentChanged("first", "example.test", "/", "http", "same") {
		t.Fatal("a duplicate at capacity must not clear the cache")
	}
	decisions.IntentChanged("overflow", "example.test", "/", "http", "next")
	if len(decisions.last) != 1 || !decisions.IntentChanged("first", "example.test", "/", "http", "same") {
		t.Fatal("new decisions beyond the limit must clear the previous batch")
	}
}

func TestMetricsContractKeepsOnlyExistingSeries(t *testing.T) {
	var metrics Metrics
	for _, source := range []string{" edge-control-group-authority/v1 ", "core-api/v1", "core-api", "core_api", "", "untrusted-input"} {
		metrics.RecordHeartbeat(source)
	}
	metrics.RecordInventoryFallback(FallbackFencingNotReady)
	metrics.RecordInventoryFallback(FallbackActiveEmpty)
	metrics.RecordInventoryFallback("untrusted-input")
	metrics.RecordPublicationTimeout()
	var output strings.Builder
	metrics.WritePrometheus(&output)
	var samples []string
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		if !strings.HasPrefix(line, "#") {
			samples = append(samples, line)
		}
	}
	want := strings.Join([]string{
		`fugue_traffic_publication_timeouts_total 1.000000`,
		`fugue_edge_route_source_heartbeats_total{source="edge_control"} 1.000000`,
		`fugue_edge_route_source_heartbeats_total{source="core_api"} 3.000000`,
		`fugue_edge_route_source_heartbeats_total{source="unknown"} 1.000000`,
		`fugue_edge_route_source_heartbeats_total{source="other"} 1.000000`,
		`fugue_edge_route_inventory_legacy_fallback_total{reason="fencing_not_ready"} 1.000000`,
		`fugue_edge_route_inventory_legacy_fallback_total{reason="active_inventory_empty"} 1.000000`,
	}, "\n")
	if got := strings.Join(samples, "\n"); got != want {
		t.Fatalf("metrics contract changed:\nwant:\n%s\ngot:\n%s", want, got)
	}
}
