package platformconfig

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"fugue/internal/model"
)

func cachedProjectionFixture(t testing.TB) (model.PlatformArtifact, model.PlatformArtifact) {
	t.Helper()
	req := declaredCellCompileFixture(t)
	for i := 0; i < 32; i++ {
		req.Policy.TrafficRolloutCohorts = append(req.Policy.TrafficRolloutCohorts, TrafficRolloutCohort{ID: fmt.Sprintf("cohort-%s%d", strings.Repeat("a", 80), i), EdgeGroupIDs: []string{"cell-a"}})
	}
	out, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	parent, child := out.ReleaseArtifact, out.RouteArtifact
	// Exercise the same generic JSON tree returned by PostgreSQL.
	raw, _ := json.Marshal(child.Content)
	if err := json.Unmarshal(raw, &child.Content); err != nil {
		t.Fatal(err)
	}
	parent.ScopeKey, child.ScopeKey = parent.Scope.Key, child.Scope.Key
	return parent, child
}

func TestTrafficProjectionCacheChecksActualPolicyAndBindings(t *testing.T) {
	for _, scenario := range []string{"child scope", "parent scope", "parent kind", "parent generation", "parent policy digest", "child policy digest", "child topology digest", "unknown policy field", "missing policy", "parent cohort", "parent membership"} {
		t.Run(scenario, func(t *testing.T) {
			parent, child := cachedProjectionFixture(t)
			if err := ValidateTrafficCohortProjection(parent, child); err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(child.Content["policy"])
			key, ok := trafficProjectionKey(parent, child, raw)
			if !ok || !trafficProjectionCached(key) {
				t.Fatal("fixture did not exercise a warm cache")
			}
			switch scenario {
			case "child scope":
				child.ScopeKey = AuthorityCellScope("cell-b")
			case "parent scope":
				parent.ScopeKey = AuthorityCellScope("cell-b")
			case "parent kind":
				parent.ArtifactKind = model.PlatformArtifactKindEdgeRouteBundle
			case "parent generation":
				parent.Generation = "changed"
			case "parent policy digest":
				parent.Metadata["policy_digest"] = "changed"
			case "child policy digest":
				child.Metadata["policy_digest"] = "changed"
			case "child topology digest":
				child.Metadata["consumer_topology_digest"] = "changed"
			case "unknown policy field":
				child.Content["policy"].(map[string]any)["unknown"] = true
			case "missing policy":
				delete(child.Content, "policy")
			case "parent cohort":
				parent.Content["traffic_rollout_cohorts"] = []any{}
			case "parent membership":
				parent.Content["consumer_topology"] = nil
			}
			if ValidateTrafficCohortProjection(parent, child) == nil {
				t.Fatal("cached projection accepted changed authority inputs")
			}
			raw, _ = json.Marshal(child.Content["policy"])
			key, ok = trafficProjectionKey(parent, child, raw)
			if ok && trafficProjectionCached(key) {
				t.Fatal("failed validation cached")
			}
		})
	}
}

func TestTrafficProjectionCacheConcurrentAndBounded(t *testing.T) {
	parent, child := cachedProjectionFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := ValidateTrafficCohortProjection(parent, child); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	for i := 0; i < 300; i++ {
		child.ID = fmt.Sprintf("artifact-%d", i)
		if err := ValidateTrafficCohortProjection(parent, child); err != nil {
			t.Fatal(err)
		}
	}
	trafficProjectionValidations.Lock()
	defer trafficProjectionValidations.Unlock()
	if len(trafficProjectionValidations.keys) > 256 || trafficProjectionValidations.count > 256 {
		t.Fatal("projection cache exceeded its bound")
	}
}

func BenchmarkTrafficProjectionMemoization(b *testing.B) {
	req := dnsQueryFixture()
	for i := 0; i < 64; i++ {
		cohort := TrafficRolloutCohort{ID: fmt.Sprintf("cohort-%d", i)}
		for j := 0; j < 64; j++ {
			cohort.EdgeGroupIDs = append(cohort.EdgeGroupIDs, fmt.Sprintf("edge-group-%s%d", strings.Repeat("a", 32), j))
		}
		req.Policy.TrafficRolloutCohorts = append(req.Policy.TrafficRolloutCohorts, cohort)
	}
	rebindPlacement(&req)
	out, err := Compile(req)
	if err != nil {
		b.Fatal(err)
	}
	parent, child := out.ReleaseArtifact, out.RouteArtifact
	for _, cached := range []bool{false, true} {
		b.Run(fmt.Sprintf("cached=%t", cached), func(b *testing.B) {
			if err := ValidateTrafficCohortProjection(parent, child); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if cached {
					err = ValidateTrafficCohortProjection(parent, child)
				} else {
					raw, _ := json.Marshal(child.Content["policy"])
					err = validateTrafficCohortPolicy(parent, child, raw)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
