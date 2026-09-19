package routeartifact

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/edgecontrol"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/trafficbinding"
)

func releaseFixture(t *testing.T) (model.PlatformArtifact, model.PlatformArtifact, model.PlatformConsumerAssignment, model.PlatformArtifactRelease, bundleauth.Keyring) {
	t.Helper()
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: "global", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: true}}},
		Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: "global", TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "first", EdgeGroupIDs: []string{"edge-group-test-a"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	keys := bundleauth.NewKeyring("synthetic-release-key", "key", "", "", nil)
	sign := func(a model.PlatformArtifact, id string) model.PlatformArtifact {
		a.ID, a.Status, a.GenerationSequence = id, model.PlatformArtifactStatusValidated, 1
		a.ScopeKey = a.Scope.Key
		a.ContentHash, err = platformconfig.Digest(a.Content)
		if err != nil {
			t.Fatal(err)
		}
		a, err = platformsafety.SignPlatformArtifact(a, keys)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	child := sign(compiled.RouteArtifact, "route")
	parent := sign(platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, []string{"route", "dns", "tls"}, time.Now().UTC()), "parent")
	a := model.PlatformConsumerAssignment{ExpectedConsumerSetID: "expected", ArtifactReleaseID: "release", ReleaseSetID: parent.ID, ArtifactID: child.ID, ArtifactKind: child.ArtifactKind, ScopeKey: "global", Revision: 1, ExpectedGeneration: child.Generation, ContentHash: child.ContentHash, GenerationSequence: 1, FencingToken: 3, ReleaseChannel: "gray"}
	r := model.PlatformArtifactRelease{ID: a.ArtifactReleaseID, ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind, Generation: parent.Generation, ScopeKey: "global", ReleaseChannel: "gray", Status: model.PlatformArtifactReleaseStatusActive, FencingToken: 3, CanaryRuleRef: "cohort=first"}
	return parent, child, a, r, keys
}

func TestReleaseProjectionRequiresSignedParentAndExactBinding(t *testing.T) {
	for _, mode := range []string{"valid", "parent-tampered", "child-tampered", "wrong-parent", "wrong-child", "wrong-release", "wrong-fence", "wrong-channel", "wrong-generation", "unknown-cohort", "ambiguous-full", "wrong-key"} {
		t.Run(mode, func(t *testing.T) {
			p, c, a, r, keys := releaseFixture(t)
			switch mode {
			case "parent-tampered":
				p.Content["scope"] = "foreign"
			case "child-tampered":
				c.Content["routes"] = []any{}
			case "wrong-parent":
				a.ReleaseSetID = "other"
			case "wrong-child":
				a.ArtifactID = "other"
			case "wrong-release":
				r.ID = "other"
			case "wrong-fence":
				r.FencingToken++
			case "wrong-channel":
				r.ReleaseChannel = "full"
			case "wrong-generation":
				a.ExpectedGeneration = "other"
			case "unknown-cohort":
				r.CanaryRuleRef = "cohort=unknown"
			case "ambiguous-full":
				r.ReleaseChannel, a.ReleaseChannel = "full", "full"
			case "wrong-key":
				keys = bundleauth.NewKeyring("wrong-secret", "key", "", "", nil)
			}
			before, _ := json.Marshal([]any{p, c, a, r})
			snapshot, err := ProjectRelease(p, c, a, r, keys)
			if mode != "valid" {
				if err == nil {
					t.Fatal("unbound projection accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			b := snapshot.TrafficRelease
			if b.ReleaseSetDigest != p.ContentHash || b.RouteArtifactDigest != c.ContentHash || b.FencingToken != r.FencingToken || b.CanaryRuleRef != r.CanaryRuleRef {
				t.Fatal("provenance lost")
			}
			if _, err := MaterializeSnapshotForGroup(snapshot, "edge-group-test-a"); err != nil {
				t.Fatal(err)
			}
			if _, err := MaterializeSnapshotForGroup(snapshot, "edge-group-test-b"); err == nil {
				t.Fatal("canary escaped group")
			}
			after, _ := json.Marshal([]any{p, c, a, r})
			if string(before) != string(after) {
				t.Fatal("projection changed immutable inputs")
			}
			snapshot.Routes[0].Hostname = "other.example.test"
			if _, err := MaterializeSnapshotForGroup(snapshot, "edge-group-test-a"); err == nil {
				t.Fatal("mutated projection accepted")
			}
		})
	}
}

func TestGroupCompilerPreservesReleaseProvenanceAndSeparatesRollouts(t *testing.T) {
	p, c, a, r, keys := releaseFixture(t)
	now := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	ledger := edgecontrol.NewMemoryGroupShadowLedger()
	compiler := edgecontrol.GroupShadowCompiler{Inventory: candidateInventory{now}, Ledger: ledger, Now: func() time.Time { return now }}
	var previousGeneration string
	for _, channel := range []string{"shadow", "gray", "full"} {
		a.ReleaseChannel, r.ReleaseChannel = channel, channel
		r.CanaryRuleRef = ""
		if channel == "gray" {
			r.CanaryRuleRef = "cohort=first"
		}
		snapshot, err := ProjectRelease(p, c, a, r, keys)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := compiler.Reconcile(context.Background(), snapshot, []string{"edge-group-test-a"})
		if err != nil || batch.Succeeded != 1 {
			t.Fatalf("compile failed: %+v %v", batch, err)
		}
		head, found, err := ledger.Head(context.Background(), "edge-group-test-a")
		if err != nil || !found || head.Bundle == nil {
			t.Fatal("missing bundle", err)
		}
		if !reflect.DeepEqual(head.Bundle.TrafficRelease, snapshot.TrafficRelease) {
			t.Fatal("group compiler lost provenance")
		}
		if head.Bundle.Generation == previousGeneration {
			t.Fatal("channel change reused previous bundle generation")
		}
		previousGeneration = head.Bundle.Generation
		if err := trafficbinding.ValidateGroup(head.Bundle.TrafficRelease, "edge-group-test-a", true); (err != nil) != (channel == "shadow") {
			t.Fatalf("serving authorization mismatch: %s %v", channel, err)
		}
		signed := bundleauth.SignEdgeRouteBundleWithKeyring(*head.Bundle, keys, time.Hour)
		if err := bundleauth.VerifyEdgeRouteBundleWithKeyring(signed, keys, now); (err != nil) != (channel == "shadow") {
			t.Fatalf("signed serving authorization mismatch: %s %v", channel, err)
		}
		if channel != "shadow" {
			signed.TrafficRelease = nil
			if err := bundleauth.VerifyEdgeRouteBundleWithKeyring(signed, keys, now); err == nil {
				t.Fatal("signed provenance stripped into legacy interpretation")
			}
		}
	}
}
