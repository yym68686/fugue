package platformconfig_test

import (
	"encoding/json"
	"testing"

	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

type countedDNSCellJSON struct {
	raw   json.RawMessage
	calls *int
}

func TestDNSCellPlanDoesNotIgnoreEarlierPolicyAlias(t *testing.T) {
	a := celldns.Compile(t, celldns.Request(t)).DNSArtifact
	a.Content["Policy"] = map[string]any{"canary_weights": []any{float64(101)}}
	if err := platformconfig.ValidateDNSCellPlan(a); err == nil {
		t.Fatal("invalid earlier policy alias was silently ignored")
	}
}

func (c countedDNSCellJSON) MarshalJSON() ([]byte, error) { (*c.calls)++; return c.raw, nil }
func TestDNSCellAndTransitionValidationSerializeOnce(t *testing.T) {
	for _, transition := range []bool{false, true} {
		name := "cell"
		if transition {
			name = "transition"
		}
		t.Run(name, func(t *testing.T) {
			req := celldns.Request(t)
			if transition {
				req = celldns.TransitionRequest(t)
			}
			a := celldns.Compile(t, req).DNSArtifact
			raw, err := json.Marshal(a.Content["readiness_plan"])
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			a.Content["readiness_plan"] = countedDNSCellJSON{raw: raw, calls: &calls}
			if err := platformconfig.ValidateDNSCellPlan(a); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("serialized identical artifact %d times; want 1", calls)
			}
		})
	}
}
