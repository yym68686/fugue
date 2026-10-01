package platformconfig

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestTrafficProjectionStrictPolicyAndLegacyCompatibility(t *testing.T) {
	parent := model.PlatformArtifact{ScopeKey: "global"}
	for _, policy := range []any{nil, map[string]any{}} {
		child := model.PlatformArtifact{Content: map[string]any{"policy": policy}}
		if err := ValidateTrafficCohortProjection(parent, child); err != nil {
			t.Fatal("legacy empty policy rejected", err)
		}
	}
	for _, policy := range []any{"bad", []any{}, map[string]any{"unknown_policy_field": true}, map[string]any{"authority_cell_id": "cell-a"}} {
		child := model.PlatformArtifact{Content: map[string]any{"policy": policy}}
		if ValidateTrafficCohortProjection(parent, child) == nil {
			t.Fatal("malformed or unbound policy accepted")
		}
	}
}

func BenchmarkTrafficCohortProjection(b *testing.B) {
	r := dnsQueryFixture()
	r.Policy.TrafficRolloutCohorts = []TrafficRolloutCohort{{ID: "complete", EdgeGroupIDs: []string{"edge-group-a"}}}
	rebindPlacement(&r)
	out, err := Compile(r)
	if err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{1024, 1 << 20, 4 << 20} {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			child := out.RouteArtifact
			// Valid JSON payload represents route/certificate data outside the
			// policy. It must not make policy projection cost grow with size.
			raw, _ := json.Marshal(child.Content)
			if err := json.Unmarshal(raw, &child.Content); err != nil {
				b.Fatal(err)
			}
			child.Content["payload"] = strings.Repeat("x", size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := ValidateTrafficCohortProjection(out.ReleaseArtifact, child); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
