package store

import (
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

func continuousFixture(t *testing.T, address string) reconfigurationFixture {
	t.Helper()
	f := refreshFixture(t, address)
	f.request.ProducerReconfiguration.Operation = "continuous_serving"
	f.policy.Generation = "producer-continuous"
	settings := *f.policy.Serving
	settings.SinglePublication = false
	f.policy.Serving = &settings
	f.next = f.save(t, f.policy)
	f.bindKey(t)
	return f
}

func testProducerContinuousServing(t *testing.T, address string) {
	t.Run("simultaneous successors", func(t *testing.T) { testSimultaneousProducerReconfiguration(t, address, continuousFixture) })
	for _, scenario := range []string{"success", "implicit", "wrong operation", "still once", "foreign policy", "operator full", "unverified", "bounds", "source", "schedule", "constraint", "stale", "frozen", "superseded retry", "continuous predecessor"} {
		t.Run(scenario, func(t *testing.T) {
			f := continuousFixture(t, address)
			p := f.policy
			settings := *p.Serving
			p.Serving = &settings
			switch scenario {
			case "implicit":
				f.request.ProducerReconfiguration.Operation = ""
			case "wrong operation":
				f.request.ProducerReconfiguration.Operation = "refresh_serving"
			case "still once":
				p.Serving.SinglePublication = true
			case "foreign policy":
				f.seedCompletedPublication(t, "other-policy", platformproducer.Actor)
			case "operator full":
				f.seedCompletedPublication(t, f.request.ProducerReconfiguration.PreviousPolicy.ReleaseID, "operator")
			case "unverified":
				f.setBaselineVerification(t, false)
			case "bounds":
				p.Serving.FullMinSeconds++
			case "source":
				p.StaticIntentArtifactID = "other-input"
			case "schedule":
				p.RefreshSeconds += 60
			case "constraint":
				p.RoutePlacementTransition.Constraints[0].Source.MinHealthyEdgeNodes++
				p.RoutePlacementTransition.Constraints[0].SourceDigest, _ = platformproducer.RouteConstraintDigest(p.RoutePlacementTransition.Constraints[0].Source)
			case "stale":
				f.request.ProducerReconfiguration.PreviousPolicy.FencingToken++
			case "frozen":
				f.freeze(t, p.TargetScope)
			}
			p.Generation = "continuous-" + strings.ReplaceAll(scenario, " ", "-")
			f.next = f.save(t, p)
			f.bindKey(t)
			r, err := f.release()
			accepted := scenario == "success" || scenario == "superseded retry" || scenario == "continuous predecessor"
			if accepted {
				if err != nil {
					t.Fatal(err)
				}
				if again, err := f.release(); err != nil || again.ID != r.ID {
					t.Fatal("retry changed continuous authority", err)
				}
				if scenario == "superseded retry" {
					other := p
					other.Generation = "other-producer"
					a := f.save(t, other)
					if _, _, _, _, err := f.s.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal()); err != nil {
						t.Fatal(err)
					}
					if _, err := f.release(); err == nil {
						t.Fatal("superseded continuous request replayed")
					}
				}
				if scenario == "continuous predecessor" {
					f.old = f.next
					f.request.ProducerReconfiguration.PreviousPolicy = model.PlatformPublicationPrecondition{ArtifactID: f.old.ID, ContentHash: f.old.ContentHash, ReleaseID: r.ID, FencingToken: r.FencingToken}
					f.seedCompletedPublication(t, r.ID, platformproducer.Actor)
					other := p
					other.Generation = "another-continuous"
					f.next = f.save(t, other)
					f.bindKey(t)
					if _, err := f.release(); err == nil {
						t.Fatal("operation accepted an already continuous predecessor")
					}
				}
			} else if err == nil {
				t.Fatal("continuous operation changed unrelated serving policy")
			}
			lkg, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, p.TargetScope)
			if err != nil || !reflect.DeepEqual(lkg, &f.lkg) {
				t.Fatal("configuration changed positive LKG", err)
			}
			if !accepted {
				a, _, found, err := f.s.GetActivePlatformArtifact(f.old.ArtifactKind, f.old.ScopeKey, "shadow")
				if err != nil || !found || a.ID != f.old.ID {
					t.Fatal("rejected request changed producer", err)
				}
			}
		})
	}
}

func TestProducerContinuousServing(t *testing.T) { testProducerContinuousServing(t, "") }
func TestProducerContinuousServingPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testProducerContinuousServing(t, address)
}
