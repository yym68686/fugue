package trafficbinding

import (
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestTrafficBindingRejectsMalformedAndAmbiguousScope(t *testing.T) {
	d := "sha256:" + strings.Repeat("a", 64)
	valid := model.TrafficReleaseBinding{Schema: Schema, ReleaseSetID: "parent", ReleaseSetDigest: d, ReleaseSetGeneration: "parent-gen", RouteArtifactID: "child", RouteArtifactDigest: d, RouteArtifactGeneration: "child-gen", RouteArtifactSequence: 1, ReleaseID: "release", ReleaseChannel: "gray", FencingToken: 1, ScopeKey: "global", IntentDigest: d, PolicyDigest: d, InputSnapshotDigest: d, CompilerVersion: "compiler", ProjectionDigest: d, CanaryRuleRef: "cohort=first", EdgeGroupIDs: []string{"edge-group-a", "edge-group-b"}}
	for _, mode := range []string{"valid", "unknown-schema", "zero-fence", "bad-digest", "noncanonical", "duplicate", "invalid-group", "missing-groups", "free-selector", "shadow-with-canary", "full-with-canary"} {
		t.Run(mode, func(t *testing.T) {
			b := Clone(&valid)
			switch mode {
			case "unknown-schema":
				b.Schema = "future"
			case "zero-fence":
				b.FencingToken = 0
			case "bad-digest":
				b.PolicyDigest = "sha256:invalid"
			case "noncanonical":
				b.EdgeGroupIDs = []string{"edge-group-b", "edge-group-a"}
			case "duplicate":
				b.EdgeGroupIDs = []string{"edge-group-a", "edge-group-a"}
			case "invalid-group":
				b.EdgeGroupIDs = []string{"anything"}
			case "missing-groups":
				b.EdgeGroupIDs = nil
			case "free-selector":
				b.CanaryRuleRef = "node=any"
			case "shadow-with-canary":
				b.ReleaseChannel = "shadow"
			case "full-with-canary":
				b.ReleaseChannel = "full"
			}
			if err := Validate(b); (err == nil) != (mode == "valid") {
				t.Fatalf("unexpected validation: %v", err)
			}
		})
	}
	if err := ValidateGroup(nil, "legacy", true); err != nil {
		t.Fatal("legacy compatibility lost")
	}
}
