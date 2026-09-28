package store

import (
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func TestNeutralAuthorityLabelsSurviveManagedAndSSHProjection(t *testing.T) {
	for _, authority := range []string{"cell-a", "edge-group-a"} {
		labels := map[string]string{runtimepkg.EdgeGroupIDLabelKey: authority, runtimepkg.LocationCountryCodeLabelKey: "US"}
		retained := normalizeManagedSharedLocationLabels(labels)
		if retained[runtimepkg.EdgeGroupIDLabelKey] != authority || retained[runtimepkg.LocationCountryCodeLabelKey] != "us" {
			t.Fatalf("managed runtime lost authority or locality: %+v", retained)
		}
		if got := edgeGroupIDForRuntime(model.Runtime{Labels: retained}); got != authority {
			t.Fatalf("SSH projection lost managed authority: %q", got)
		}
		for _, key := range []string{"edge_group_id", "edgeGroupID"} {
			if got := edgeGroupIDForRuntime(model.Runtime{Labels: map[string]string{key: authority}}); got != authority {
				t.Fatalf("SSH explicit authority alias %q was discarded", key)
			}
		}
	}
	for _, invalid := range []string{"cell-", "cell-a/other", "cell-A", "edge-group-"} {
		labels := map[string]string{runtimepkg.EdgeGroupIDLabelKey: invalid}
		if got := normalizeManagedSharedLocationLabels(labels); len(got) != 0 {
			t.Fatalf("invalid managed authority retained: %+v", got)
		}
		if got := edgeGroupIDForRuntime(model.Runtime{Labels: labels}); got != "" {
			t.Fatalf("invalid SSH authority retained: %q", got)
		}
	}
	if got := edgeGroupIDForRuntime(model.Runtime{Labels: map[string]string{runtimepkg.LocationCountryCodeLabelKey: "us"}}); got != "" {
		t.Fatalf("country alone manufactured SSH authority: %q", got)
	}
}
