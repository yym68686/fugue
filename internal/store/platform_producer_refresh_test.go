package store

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/testfixture/celldns"
)

func refreshFixture(t *testing.T, address string) reconfigurationFixture {
	t.Helper()
	f := activationFixture(t, address)
	r, err := f.release()
	if err != nil {
		t.Fatal(err)
	}
	f.old = f.next
	f.request.ProducerReconfiguration.PreviousPolicy = model.PlatformPublicationPrecondition{ArtifactID: f.old.ID, ContentHash: f.old.ContentHash, ReleaseID: r.ID, FencingToken: r.FencingToken}
	f.request.ProducerReconfiguration.Operation = "refresh_serving"
	f.policy.Generation = "producer-refreshed"
	f.next = f.save(t, f.policy)
	f.bindKey(t)
	f.seedCompletedPublication(t, r.ID, platformproducer.Actor)
	return f
}

// The real gray/full/probe flow is covered by API tests. This local fixture
// isolates the subsequent configuration transaction and signs its checkpoint.
func (f *reconfigurationFixture) seedCompletedPublication(t *testing.T, policy, actor string) {
	t.Helper()
	parent, err := f.s.GetPlatformArtifact(f.lkg.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	if parent.Metadata == nil {
		parent.Metadata = map[string]string{}
	}
	parent.Metadata[platformproducer.PolicyReleaseMetadata] = policy
	parent.Metadata[platformproducer.StaticIntentIDMetadata] = f.policy.StaticIntentArtifactID
	parent.Metadata[platformproducer.StaticIntentDigestMetadata] = f.policy.StaticIntentDigest
	parent.Metadata[platformproducer.DNSPolicyIDMetadata] = f.policy.DNSPolicyArtifactID
	parent.Metadata[platformproducer.DNSPolicyDigestMetadata] = f.policy.DNSPolicyDigest
	parent, err = platformsafety.SignPlatformArtifact(parent, celldns.Keys())
	if err != nil {
		t.Fatal(err)
	}
	full, err := f.s.GetPlatformArtifactRelease(f.lkg.VerifiedByReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	full.ReleasedByType, full.ReleasedByID = model.ActorTypeBootstrap, actor
	lkg := f.lkg
	lkg.ArtifactProvenance = parent.Provenance
	lkg, err = platformsafety.SignPlatformLKGSnapshot(lkg, celldns.Keys())
	if err != nil {
		t.Fatal(err)
	}
	if f.s.db != nil {
		metadata, _ := json.Marshal(parent.Metadata)
		provenance, _ := json.Marshal(parent.Provenance)
		_, err = f.s.db.Exec(`UPDATE fugue_platform_artifacts SET metadata_json=$2::jsonb,provenance_json=$3::jsonb WHERE id=$1`, parent.ID, metadata, provenance)
		if err == nil {
			_, err = f.s.db.Exec(`UPDATE fugue_platform_artifact_releases SET released_by_type=$2,released_by_id=$3 WHERE id=$1`, full.ID, full.ReleasedByType, full.ReleasedByID)
		}
		if err == nil {
			_, err = pgUpsertPlatformLKGSnapshot(context.Background(), f.s.db, lkg)
		}
	} else {
		err = f.s.withLockedState(true, func(state *model.State) error {
			state.PlatformArtifacts[platformArtifactIndex(state.PlatformArtifacts, parent.ID)] = parent
			state.PlatformArtifactReleases[platformArtifactReleaseIndex(state.PlatformArtifactReleases, full.ID)] = full
			state.PlatformLKGSnapshots = upsertPlatformLKGSnapshot(state.PlatformLKGSnapshots, lkg)
			return nil
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	stored, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, f.policy.TargetScope)
	if err != nil || stored == nil {
		t.Fatal("seeded LKG", err)
	}
	f.lkg = *stored
}

func testProducerServingRefresh(t *testing.T, address string) {
	t.Run("simultaneous successors", func(t *testing.T) { testSimultaneousProducerReconfiguration(t, address, refreshFixture) })
	for _, scenario := range []string{"success", "implicit", "activation", "foreign policy", "operator full", "unverified", "bounds", "source", "schedule", "constraint", "stale", "frozen", "superseded retry"} {
		t.Run(scenario, func(t *testing.T) {
			f := refreshFixture(t, address)
			p := f.policy
			switch scenario {
			case "implicit":
				f.request.ProducerReconfiguration.Operation = ""
			case "activation":
				f.request.ProducerReconfiguration.Operation = "activate_serving"
			case "foreign policy":
				f.seedCompletedPublication(t, "other-policy", platformproducer.Actor)
			case "operator full":
				f.seedCompletedPublication(t, f.request.ProducerReconfiguration.PreviousPolicy.ReleaseID, "operator")
			case "unverified":
				f.setBaselineVerification(t, false)
			case "bounds":
				p.Serving.FullMinSeconds++
			case "source":
				p.StaticIntentArtifactID = "another-input"
			case "schedule":
				p.RefreshSeconds = 300
			case "constraint":
				p.RoutePlacementTransition.Constraints[0].Source.MinHealthyEdgeNodes++
				p.RoutePlacementTransition.Constraints[0].SourceDigest, _ = platformproducer.RouteConstraintDigest(p.RoutePlacementTransition.Constraints[0].Source)
			case "stale":
				f.request.ProducerReconfiguration.PreviousPolicy.FencingToken++
			case "frozen":
				f.freeze(t, p.TargetScope)
			}
			p.Generation = "refresh-" + strings.ReplaceAll(scenario, " ", "-")
			f.next = f.save(t, p)
			f.bindKey(t)
			r, err := f.release()
			if scenario == "success" || scenario == "superseded retry" {
				if err != nil {
					t.Fatal(err)
				}
				if again, err := f.release(); err != nil || again.ID != r.ID {
					t.Fatal("refresh retry", err)
				}
				if scenario == "superseded retry" {
					other := p
					other.Generation = "operator-replaced-policy"
					a := f.save(t, other)
					if _, _, _, _, err := f.s.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal()); err != nil {
						t.Fatal(err)
					}
					if _, err := f.release(); err == nil {
						t.Fatal("stale successful refresh replayed")
					}
				}
			} else if err == nil {
				t.Fatal("unsafe refresh admitted")
			}
			lkg, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, p.TargetScope)
			if err != nil || !reflect.DeepEqual(lkg, &f.lkg) {
				t.Fatal("refresh altered LKG", err)
			}
			if scenario != "success" && scenario != "superseded retry" {
				a, _, found, err := f.s.GetActivePlatformArtifact(f.old.ArtifactKind, f.old.ScopeKey, "shadow")
				if err != nil || !found || a.ID != f.old.ID {
					t.Fatal("rejected refresh changed policy", err)
				}
			}
		})
	}
}

func TestProducerServingRefresh(t *testing.T) { testProducerServingRefresh(t, "") }
func TestProducerServingRefreshPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testProducerServingRefresh(t, address)
}
