package routeartifact

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

func requestBindingFixture(t testing.TB, count int) (model.PlatformArtifact, model.PlatformArtifact, model.PlatformConsumerAssignment, model.PlatformArtifactRelease, bundleauth.Keyring) {
	t.Helper()
	routes := make([]platformconfig.RouteIntent, count)
	for i := range routes {
		routes[i] = platformconfig.RouteIntent{Hostname: fmt.Sprintf("route-%d.example.test", i), UpstreamURL: "http://origin:8080", Enabled: true}
	}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: "global", Routes: routes},
		Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: "global", TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "first", EdgeGroupIDs: []string{"edge-group-test-a"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	keys := bundleauth.NewKeyring("synthetic-binding-key", "key", "", "", nil)
	sign := func(artifact model.PlatformArtifact, id string) model.PlatformArtifact {
		artifact.ID, artifact.Status, artifact.GenerationSequence = id, model.PlatformArtifactStatusValidated, 1
		artifact.ScopeKey = artifact.Scope.Key
		artifact.ContentHash, err = platformconfig.Digest(artifact.Content)
		if err != nil {
			t.Fatal(err)
		}
		artifact, err = platformsafety.SignPlatformArtifact(artifact, keys)
		if err != nil {
			t.Fatal(err)
		}
		return artifact
	}
	child := sign(compiled.RouteArtifact, "route")
	parent := sign(platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, []string{"route", "dns", "tls"}, time.Now().UTC()), "parent")
	assignment := model.PlatformConsumerAssignment{ExpectedConsumerSetID: "expected", ArtifactReleaseID: "release", ReleaseSetID: parent.ID, ArtifactID: child.ID, ArtifactKind: child.ArtifactKind, ScopeKey: "global", Revision: 1, ExpectedGeneration: child.Generation, ContentHash: child.ContentHash, GenerationSequence: 1, FencingToken: 3, ReleaseChannel: "gray"}
	release := model.PlatformArtifactRelease{ID: assignment.ArtifactReleaseID, ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind, Generation: parent.Generation, ScopeKey: "global", ReleaseChannel: "gray", Status: model.PlatformArtifactReleaseStatusActive, FencingToken: 3, CanaryRuleRef: "cohort=first"}
	return parent, child, assignment, release, keys
}

func TestRequestBindingReaderReusesIdenticalProjectionAndOwnsResult(t *testing.T) {
	p, c, a, r, keys := requestBindingFixture(t, 16)
	calls := 0
	read := newRequestBindingReader(func(parent, child model.PlatformArtifact, assignment model.PlatformConsumerAssignment, release model.PlatformArtifactRelease, keys bundleauth.Keyring) (model.EdgeRouteIntentSnapshot, error) {
		calls++
		return ProjectRelease(parent, child, assignment, release, keys)
	})
	want, err := ProjectRelease(p, c, a, r, keys)
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		binding, err := read(p, c, a, r, keys)
		if err != nil || !reflect.DeepEqual(binding, want.TrafficRelease) {
			t.Fatal("binding changed", binding, err)
		}
		binding.EdgeGroupIDs[0] = "changed-group"
		binding.ReleaseID = "changed-release"
	}
	if calls != 1 {
		t.Fatalf("identical request projection executed %d times, want 1", calls)
	}
}

func TestRequestBindingReaderRevalidatesChangedInputs(t *testing.T) {
	for _, mode := range []string{"parent-body", "child-body", "parent-metadata", "child-metadata", "signature", "child-status", "assignment", "fence", "lane", "cohort", "key", "revocation", "schema-type"} {
		t.Run(mode, func(t *testing.T) {
			p, c, a, r, keys := requestBindingFixture(t, 2)
			// Mutate this map in place after success, so pointer identity cannot
			// conceal a newly revoked signing key.
			keys.RevokedKeyIDs = map[string]struct{}{}
			read := NewRequestBindingReader()
			if _, err := read(p, c, a, r, keys); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "parent-body":
				p.Content["scope"] = "foreign"
			case "child-body":
				c.Content["routes"] = []any{}
			case "parent-metadata":
				p.Metadata["intent_digest"] = "changed"
			case "child-metadata":
				c.Metadata["release_set_generation"] = "changed"
			case "signature":
				c.Provenance.Signature = "changed"
			case "child-status":
				c.Status = model.PlatformArtifactStatusDraft
			case "assignment":
				a.ExpectedGeneration = "changed"
			case "fence":
				r.FencingToken++
			case "lane":
				r.ReleaseChannel = "full"
			case "cohort":
				r.CanaryRuleRef = "cohort=missing"
			case "key":
				keys.PrimaryKey = "changed"
			case "revocation":
				keys.RevokedKeyIDs["key"] = struct{}{}
			case "schema-type":
				// Same JSON, different Go validation semantics: custom values must
				// bypass reuse instead of colliding with the earlier valid input.
				type schemaString string
				c.Content["schema_version"] = schemaString(platformconfig.SchemaVersion)
			}
			_, originalErr := ProjectRelease(p, c, a, r, keys)
			_, cachedErr := read(p, c, a, r, keys)
			if originalErr == nil || cachedErr == nil || originalErr.Error() != cachedErr.Error() {
				t.Fatal("changed input bypassed validation", originalErr, cachedErr)
			}
		})
	}
}

func TestRequestBindingReaderDoesNotReuseErrorsOrOtherRequests(t *testing.T) {
	p, c, a, r, keys := requestBindingFixture(t, 2)
	calls := 0
	failed := errors.New("projection unavailable")
	project := func(parent, child model.PlatformArtifact, assignment model.PlatformConsumerAssignment, release model.PlatformArtifactRelease, keys bundleauth.Keyring) (model.EdgeRouteIntentSnapshot, error) {
		calls++
		if calls <= 2 {
			return model.EdgeRouteIntentSnapshot{}, failed
		}
		return ProjectRelease(parent, child, assignment, release, keys)
	}
	read := newRequestBindingReader(project)
	for range 2 {
		if _, err := read(p, c, a, r, keys); !errors.Is(err, failed) {
			t.Fatal(err)
		}
	}
	for range 2 {
		if _, err := read(p, c, a, r, keys); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 3 {
		t.Fatal("failure retained or valid result not reused", calls)
	}
	// A new topology revision and a subsequent request must independently
	// perform the full validation, even when the resulting binding is equal.
	a.Revision++
	if _, err := read(p, c, a, r, keys); err != nil {
		t.Fatal(err)
	}
	if _, err := newRequestBindingReader(project)(p, c, a, r, keys); err != nil || calls != 5 {
		t.Fatal("revision or request boundary reused old validation", calls, err)
	}
}

func BenchmarkRequestBindingSelection(b *testing.B) {
	for _, count := range []int{16, 256} {
		p, c, a, r, keys := requestBindingFixture(b, count)
		for _, reuse := range []bool{false, true} {
			b.Run(fmt.Sprintf("routes=%d/reuse=%t", count, reuse), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					read := NewRequestBindingReader()
					for range 4 { // Two kinds across full and gray candidates.
						if reuse {
							if _, err := read(p, c, a, r, keys); err != nil {
								b.Fatal(err)
							}
						} else if _, err := ProjectRelease(p, c, a, r, keys); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
