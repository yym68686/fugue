package store

import (
	"context"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

func activationFixture(t *testing.T, address string) reconfigurationFixture {
	t.Helper()
	f := newReconfigurationFixture(t, address)
	r, err := f.release()
	if err != nil {
		t.Fatal(err)
	}
	f.old = f.next
	f.request.ProducerReconfiguration.PreviousPolicy = model.PlatformPublicationPrecondition{ArtifactID: f.old.ID, ContentHash: f.old.ContentHash, ReleaseID: r.ID, FencingToken: r.FencingToken}
	f.request.ProducerReconfiguration.Operation = "activate_serving"
	f.policy.Generation, f.policy.Mode = "serving-once", "serving"
	f.policy.Serving = &platformproducer.ServingPolicy{SinglePublication: true, CanaryRuleRef: "cohort=complete", GrayMinSeconds: 1, FullMinSeconds: 1, RolloutTimeoutSeconds: 60}
	f.next = f.save(t, f.policy)
	f.bindKey(t)
	f.setBaselineVerification(t, true)
	lkg, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, f.policy.TargetScope)
	if err != nil || lkg == nil {
		t.Fatal("stored baseline", err)
	}
	f.lkg = *lkg
	return f
}

// Seed only the local test ledger; actual probe verification is exercised in API tests.
func (f reconfigurationFixture) setBaselineVerification(t *testing.T, verified bool) {
	t.Helper()
	r, err := f.s.GetPlatformArtifactRelease(f.lkg.VerifiedByReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	r.VerificationState, r.VerifiedLKGGeneration = "", ""
	if verified {
		r.VerificationState, r.VerifiedLKGGeneration = model.PlatformArtifactVerificationStateVerified, r.Generation
	}
	if f.s.db != nil {
		_, err = pgUpdatePlatformArtifactReleaseVerification(context.Background(), f.s.db, r)
	} else {
		err = f.s.withLockedState(true, func(st *model.State) error {
			st.PlatformArtifactReleases[platformArtifactReleaseIndex(st.PlatformArtifactReleases, r.ID)] = r
			return nil
		})
	}
	if err != nil {
		t.Fatal(err)
	}
}

func testProducerServingActivation(t *testing.T, address string) {
	for _, scenario := range []string{"success", "implicit", "unknown", "continuous", "constraint", "source", "schedule", "cohort", "unverified", "frozen", "stale"} {
		t.Run(scenario, func(t *testing.T) {
			f := activationFixture(t, address)
			p := f.policy
			switch scenario {
			case "implicit":
				f.request.ProducerReconfiguration.Operation = ""
			case "unknown":
				f.request.ProducerReconfiguration.Operation = "arbitrary"
			case "continuous":
				p.Serving.SinglePublication = false
			case "constraint":
				p.RoutePlacementTransition.Constraints[0].Source.MinHealthyEdgeNodes++
				p.RoutePlacementTransition.Constraints[0].SourceDigest, _ = platformproducer.RouteConstraintDigest(p.RoutePlacementTransition.Constraints[0].Source)
			case "source":
				p.StaticIntentArtifactID = "another-static"
			case "schedule":
				p.RefreshSeconds = 300
			case "cohort":
				p.Serving.CanaryRuleRef = "cohort=missing"
			case "unverified":
				f.setBaselineVerification(t, false)
			case "frozen":
				f.freeze(t, p.TargetScope)
			case "stale":
				f.request.ProducerReconfiguration.PreviousPolicy.FencingToken++
			}
			p.Generation = "activation-" + scenario
			f.next = f.save(t, p)
			f.bindKey(t)
			r, err := f.release()
			if scenario == "success" {
				if err != nil {
					t.Fatal(err)
				}
				again, err := f.release()
				if err != nil || r.ID != again.ID {
					t.Fatal("activation retry", err)
				}
			} else if err == nil {
				t.Fatal("unsafe activation admitted")
			}
			current, _, found, err := f.s.GetActivePlatformArtifact(f.old.ArtifactKind, f.old.ScopeKey, "shadow")
			wanted := f.old.ID
			if scenario == "success" {
				wanted = f.next.ID
			}
			if err != nil || !found || current.ID != wanted {
				t.Fatal("policy transaction differs", err)
			}
			lkg, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, p.TargetScope)
			if err != nil || !reflect.DeepEqual(lkg, &f.lkg) {
				t.Fatal("activation changed LKG", err)
			}
		})
	}
}

func TestProducerServingActivation(t *testing.T) { testProducerServingActivation(t, "") }
func TestProducerServingActivationPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testProducerServingActivation(t, address)
}
