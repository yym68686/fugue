package model

import "testing"

func TestEdgeRouteDecisionCorrelationKeyIsCanonicalAndComplete(t *testing.T) {
	t.Parallel()
	if got := EdgeRouteDecisionCorrelationKey(" decision_1 ", " bundle_1 ", " route_1 "); got != `["decision_1","bundle_1","route_1"]` {
		t.Fatalf("unexpected canonical key %q", got)
	}
	for _, values := range [][3]string{{"", "bundle", "route"}, {"decision", "", "route"}, {"decision", "bundle", ""}} {
		if got := EdgeRouteDecisionCorrelationKey(values[0], values[1], values[2]); got != "" {
			t.Fatalf("incomplete correlation produced key %q for %+v", got, values)
		}
	}
	if EdgeRouteDecisionCorrelationKey("a", "bc", "d") == EdgeRouteDecisionCorrelationKey("ab", "c", "d") {
		t.Fatal("canonical framing is ambiguous")
	}
}
