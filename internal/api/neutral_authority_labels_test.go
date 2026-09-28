package api

import (
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func TestExplicitNeutralRuntimeAuthorityPreservedByRouteDerivation(t *testing.T) {
	for _, authority := range []string{"cell-a", "edge-group-a"} {
		for _, key := range []string{runtimepkg.EdgeGroupIDLabelKey, "edge_group_id", "edgeGroupID"} {
			labels := map[string]string{key: authority, runtimepkg.LocationCountryCodeLabelKey: "us"}
			if got := derivedEdgeGroupIDForLabels(labels); got != authority {
				t.Fatalf("label %s authority=%s got=%s", key, authority, got)
			}
			if got := derivedEdgeGroupIDForRuntime(model.Runtime{Labels: labels}, true, map[string]string{key: "cell-other"}); got != authority {
				t.Fatalf("explicit runtime authority was replaced by node placement: %s", got)
			}
		}
		if got := edgeGroupIDFromEdgeID(authority); got != authority {
			t.Fatalf("explicit authority selector was discarded: %s", got)
		}
	}
	for _, invalid := range []string{"cell-", "cell-a/path", "cell-A", "edge-group-", "edge-node"} {
		if got := derivedEdgeGroupIDForLabels(map[string]string{runtimepkg.EdgeGroupIDLabelKey: invalid}); got != defaultEdgeGroupID {
			t.Fatalf("invalid authority %q selected %q", invalid, got)
		}
		if edgeGroupIDFromEdgeID(invalid) != "" {
			t.Fatalf("invalid authority selector %q accepted", invalid)
		}
	}
	if got := derivedEdgeGroupIDForLabels(map[string]string{runtimepkg.LocationCountryCodeLabelKey: "us"}); got != defaultEdgeGroupID {
		t.Fatalf("country alone manufactured authority: %s", got)
	}
}
