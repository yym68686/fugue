package store

import (
	"errors"
	"fugue/internal/model"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestTrafficCanaryPublicationRejectsUnboundSelectorsAtomically(t *testing.T) {
	testTrafficCanaryPublication(t, "")
}
func TestTrafficCanaryPostgresPublication(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set disposable test database")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testTrafficCanaryPublication(t, address)
}
func testTrafficCanaryPublication(t *testing.T, address string) {
	for _, ref := range []string{"edge=edge-a", "cohort=missing", "cohort=test"} {
		t.Run(ref, func(t *testing.T) {
			s := New(t.TempDir()+"/state.json", address)
			if address != "" {
				t.Cleanup(func() { s.db.Close() })
			}
			configureTestPlatformArtifactSigning(s)
			if err := s.Init(); err != nil {
				t.Fatal(err)
			}
			scope := "canary-" + model.NewID("test")
			f := prepareTrafficLKGFixture(t, s, scope, "shadow", false)
			for _, override := range []bool{false, true} {
				principal := testPlatformPrincipal()
				if override {
					principal = testPlatformSoftOverridePrincipal()
				}
				_, release, _, _, err := s.ReleasePlatformArtifact(f.parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: ref, SoftOverride: override, Reason: "test bounded rollout"}, principal)
				if ref == "cohort=test" {
					if err != nil {
						t.Fatal(err)
					}
					if release.CanaryRuleRef != ref {
						t.Fatal("cohort identity changed")
					}
					for _, badRef := range []string{"edge=edge-a", "cohort=missing"} {
						_, _, _, _, rollbackErr := s.RollbackPlatformArtifact(f.parent.ID, model.PlatformArtifactRollbackRequest{ReleaseChannel: "gray", ToGeneration: f.parent.Generation, CanaryRuleRef: badRef, Reason: "test rollback target cohort"}, testPlatformPrincipal())
						if !errors.Is(rollbackErr, ErrConflict) {
							t.Fatal("rollback accepted unbound selector", rollbackErr)
						}
						_, active, found, readErr := s.GetActivePlatformArtifact(f.parent.ArtifactKind, scope, "gray")
						if readErr != nil || !found || active.ID != release.ID || active.FencingToken != release.FencingToken {
							t.Fatal("rejected rollback changed authority", readErr)
						}
					}
					break
				}
				if !errors.Is(err, ErrConflict) {
					t.Fatal("unbound canary accepted", err)
				}
				if _, _, found, err := s.GetActivePlatformArtifact(f.parent.ArtifactKind, scope, "gray"); err != nil || found {
					t.Fatal("failed publication changed lane", err)
				}
			}
			sets, err := s.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{ReleaseSetID: f.parent.ID})
			if err != nil || len(sets) != len(f.sets) {
				t.Fatal("publication changed expected sets", err)
			}
			original := map[string]model.PlatformExpectedConsumerSet{}
			for _, set := range f.sets {
				original[set.ID] = set
			}
			for _, set := range sets {
				if !reflect.DeepEqual(set, original[set.ID]) {
					t.Fatal("immutable expectation changed")
				}
			}
		})
	}
}
