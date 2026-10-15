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
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/testfixture/celldns"
)

func shrinkFixture(t *testing.T, address string) reconfigurationFixture {
	t.Helper()
	f := membershipFixture(t, address)
	p := f.policy
	p.Generation, p.Mode = "shrink-predecessor", "serving"
	p.Serving.SinglePublication = false
	f.policy = p
	f.old = f.save(t, p)
	_, r, _, _, err := f.s.ReleasePlatformArtifact(f.old.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	f.request.ProducerReconfiguration.PreviousPolicy = model.PlatformPublicationPrecondition{ArtifactID: f.old.ID, ContentHash: f.old.ContentHash, ReleaseID: r.ID, FencingToken: r.FencingToken}
	f.seedCompletedPublication(t, r.ID, platformproducer.Actor)
	a, _ := f.s.GetPlatformArtifact(p.StaticIntentArtifactID)
	var intent platformconfig.PlatformIntent
	raw, _ := json.Marshal(a.Content)
	json.Unmarshal(raw, &intent)
	topology, err := platformconfig.TrafficConsumerTopologyFromIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	// Seed the two-member signed checkpoint. This fixture tests configuration
	// transactions; end-to-end candidate evidence has separate admission tests.
	parent, _ := f.s.GetPlatformArtifact(f.lkg.ArtifactID)
	raw, _ = json.Marshal(topology)
	var object map[string]any
	json.Unmarshal(raw, &object)
	parent.Content["consumer_topology"] = object
	parent.ContentHash, _ = platformconfig.Digest(parent.Content)
	parent.Metadata["consumer_topology_digest"], _ = platformconfig.Digest(topology)
	parent, err = platformsafety.SignPlatformArtifact(parent, celldns.Keys())
	if err != nil {
		t.Fatal(err)
	}
	f.lkg.ContentHash, f.lkg.ArtifactProvenance = parent.ContentHash, parent.Provenance
	f.lkg, err = platformsafety.SignPlatformLKGSnapshot(f.lkg, celldns.Keys())
	if err != nil {
		t.Fatal(err)
	}
	if f.s.db != nil {
		content, _ := json.Marshal(parent.Content)
		metadata, _ := json.Marshal(parent.Metadata)
		provenance, _ := json.Marshal(parent.Provenance)
		_, err = f.s.db.Exec(`UPDATE fugue_platform_artifacts SET content_json=$2::jsonb,content_hash=$3,metadata_json=$4::jsonb,provenance_json=$5::jsonb WHERE id=$1`, parent.ID, content, parent.ContentHash, metadata, provenance)
		if err == nil {
			_, err = pgUpsertPlatformLKGSnapshot(context.Background(), f.s.db, f.lkg)
		}
	} else {
		err = f.s.withLockedState(true, func(state *model.State) error {
			state.PlatformArtifacts[platformArtifactIndex(state.PlatformArtifacts, parent.ID)] = parent
			state.PlatformLKGSnapshots = upsertPlatformLKGSnapshot(state.PlatformLKGSnapshots, f.lkg)
			return nil
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	stored, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, p.TargetScope)
	if err != nil || stored == nil {
		t.Fatal("persisted checkpoint", err)
	}
	f.lkg = *stored
	f.request.ProducerReconfiguration.ServingFull.ContentHash = parent.ContentHash
	intent.Generation = "shrink-input"
	intent.EdgeTopology.Edges = intent.EdgeTopology.Edges[:1]
	a = membershipInput(t, f.s, model.PlatformArtifactKindPlatformIntent, p.TargetScope, intent)
	f.policy.StaticIntentArtifactID, f.policy.StaticIntentDigest = a.ID, a.ContentHash
	projection, _ := f.s.GetPlatformArtifact(p.DNSPolicyArtifactID)
	topology, _ = platformconfig.TrafficConsumerTopologyFromIntent(intent)
	projection.Content["generation"] = "shrink-projection"
	projection.Content["consumer_topology_digest"], _ = platformconfig.Digest(topology)
	a = membershipInput(t, f.s, model.PlatformArtifactKindPolicySnapshot, p.TargetScope, projection.Content)
	f.policy.DNSPolicyArtifactID, f.policy.DNSPolicyDigest = a.ID, a.ContentHash
	f.policy.Generation, f.policy.Mode = "shrink-successor", "shadow"
	f.next = f.save(t, f.policy)
	f.request.ProducerReconfiguration.Operation = "shrink_membership"
	f.bindKey(t)
	return f
}

func testProducerMembershipShrink(t *testing.T, address string) {
	t.Run("simultaneous successors", func(t *testing.T) { testSimultaneousProducerReconfiguration(t, address, shrinkFixture) })
	for _, scenario := range []string{"success", "failed gray", "pending gray", "frozen gray", "stale predecessor", "changed settings", "wrong operation", "wrong digest", "changed retained member", "removed every member", "floor", "changed route", "wrong lkg evidence"} {
		t.Run(scenario, func(t *testing.T) {
			f := shrinkFixture(t, address)
			switch scenario {
			case "failed gray", "pending gray", "frozen gray":
				seedMembershipGray(t, f, scenario == "failed gray", scenario == "frozen gray")
			case "stale predecessor":
				f.request.ProducerReconfiguration.PreviousPolicy.FencingToken++
			case "changed settings":
				f.policy.Serving.FullMinSeconds++
			case "wrong operation":
				f.request.ProducerReconfiguration.Operation = "expand_membership"
			case "wrong digest":
				f.policy.StaticIntentDigest = "sha256:" + strings.Repeat("b", 64)
			case "floor":
				a, _ := f.s.GetPlatformArtifact(f.policy.DNSPolicyArtifactID)
				a.Content["generation"] = "changed-floor"
				a.Content["minimum_healthy_edges"] = 2
				a = membershipInput(t, f.s, a.ArtifactKind, a.ScopeKey, a.Content)
				f.policy.DNSPolicyArtifactID, f.policy.DNSPolicyDigest = a.ID, a.ContentHash
			case "changed retained member", "removed every member", "changed route":
				a, _ := f.s.GetPlatformArtifact(f.policy.StaticIntentArtifactID)
				var intent platformconfig.PlatformIntent
				raw, _ := json.Marshal(a.Content)
				json.Unmarshal(raw, &intent)
				intent.Generation = "invalid-input"
				if scenario == "changed retained member" {
					intent.EdgeTopology.Edges[0].Labels = map[string]string{"changed": "true"}
				}
				if scenario == "removed every member" {
					intent.EdgeTopology.Edges = nil
				}
				if scenario == "changed route" {
					intent.Routes = []platformconfig.RouteIntent{{Hostname: "changed.example.test", Enabled: true}}
				}
				a = membershipInput(t, f.s, a.ArtifactKind, a.ScopeKey, intent)
				f.policy.StaticIntentArtifactID, f.policy.StaticIntentDigest = a.ID, a.ContentHash
			case "wrong lkg evidence":
				f.request.ProducerReconfiguration.VerificationEvidenceHash = "sha256:" + strings.Repeat("c", 64)
			}
			f.policy.Generation = "shrink-" + strings.ReplaceAll(scenario, " ", "-")
			f.next = f.save(t, f.policy)
			f.bindKey(t)
			r, err := f.release()
			success := scenario == "success" || scenario == "failed gray"
			if success {
				if err != nil {
					t.Fatal(err)
				}
				if retry, err := f.release(); err != nil || retry.ID != r.ID {
					t.Fatal("idempotency", err)
				}
			} else if err == nil {
				t.Fatal("unsafe shrink admitted")
			}
			lkg, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, f.policy.TargetScope)
			if err != nil || !reflect.DeepEqual(lkg, &f.lkg) {
				t.Fatal("positive checkpoint changed", err)
			}
			_, full, _, err := f.s.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, f.policy.TargetScope, "full")
			if err != nil || full.ID != f.lkg.VerifiedByReleaseID {
				t.Fatal("serving release changed", err)
			}
		})
	}
}
func TestProducerMembershipShrink(t *testing.T) { testProducerMembershipShrink(t, "") }
func TestProducerMembershipShrinkPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testProducerMembershipShrink(t, address)
}
