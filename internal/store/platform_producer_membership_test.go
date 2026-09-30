package store

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func membershipInput(t *testing.T, s *Store, kind, scope string, value any) model.PlatformArtifact {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]any
	if err := json.Unmarshal(raw, &content); err != nil {
		t.Fatal(err)
	}
	a, err := s.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: kind, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: scope}, Generation: content["generation"].(string), Content: content})
	if err != nil {
		t.Fatal(err)
	}
	a, err = s.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func seedMembershipGray(t *testing.T, f reconfigurationFixture, failed, frozen bool) {
	t.Helper()
	r, err := f.s.GetPlatformArtifactRelease(f.lkg.VerifiedByReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	r.ID = model.NewID("operator-gray")
	r.ReleaseChannel = "gray"
	r.LaneKey = platformsafety.ReleaseLaneKey(r.ArtifactKind, r.ScopeKey, "gray")
	r.ReleasedAt = time.Now().UTC()
	r.VerificationState = model.PlatformArtifactVerificationStateServingUnverified
	if failed {
		r.VerificationState = model.PlatformArtifactVerificationStateFailed
	}
	r.FencingToken = 1
	if f.s.db == nil {
		err = f.s.withLockedState(true, func(state *model.State) error {
			state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, r)
			state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{LaneKey: r.LaneKey, ArtifactKind: r.ArtifactKind, ScopeKey: r.ScopeKey, ReleaseChannel: "gray", FencingToken: 1, Version: 1, ActiveReleaseID: r.ID, Frozen: frozen, UpdatedAt: r.ReleasedAt})
			return nil
		})
	} else {
		ctx := context.Background()
		tx, e := f.s.db.BeginTx(ctx, nil)
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback()
		if _, err = pgNextPlatformReleaseLane(ctx, tx, r.ArtifactKind, r.ScopeKey, "gray", r.ReleasedAt); err == nil {
			_, err = pgInsertPlatformArtifactRelease(ctx, tx, r)
		}
		if err == nil {
			err = pgSetPlatformReleaseLaneActive(ctx, tx, r.LaneKey, r.ID, r.FencingToken, r.ReleasedAt)
		}
		if err == nil && frozen {
			_, err = tx.Exec(`UPDATE fugue_platform_release_lanes SET frozen=true WHERE lane_key=$1`, r.LaneKey)
		}
		if err == nil {
			err = tx.Commit()
		}
	}
	if err != nil {
		t.Fatal(err)
	}
}

func membershipFixture(t *testing.T, address string) reconfigurationFixture {
	t.Helper()
	f := refreshFixture(t, address)
	p := f.policy
	p.Generation = "membership-predecessor"
	edge := p.RoutePlacementTransition.NextTopology.Edges[0]
	// Clone map/slice fields before declaring another physical member.
	raw, _ := json.Marshal(edge)
	var added edgetopology.Edge
	json.Unmarshal(raw, &added)
	added.ID, added.FailureDomains["host"] = "edge-added", "edge-added"
	p.RoutePlacementTransition.PreviousTopology.Edges = append(p.RoutePlacementTransition.PreviousTopology.Edges, added)
	p.RoutePlacementTransition.NextTopology.Edges = append(p.RoutePlacementTransition.NextTopology.Edges, added)
	for _, topology := range []*edgetopology.Intent{&p.RoutePlacementTransition.PreviousTopology, &p.RoutePlacementTransition.NextTopology} {
		sort.Slice(topology.Edges, func(i, j int) bool { return topology.Edges[i].ID < topology.Edges[j].ID })
	}
	intent := platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Generation: "members-before", Scope: p.TargetScope, PublicationRole: p.PublicationRole, AuthorityCellID: p.AuthorityCellID, ApplicationDomains: &platformconfig.ApplicationDomainsIntent{AppBaseDomain: "example.test", ReservedHostnames: []string{}, DefaultDNSTTL: 60}, EdgeTopology: &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Cells: []edgetopology.AuthorityCell{{ID: p.AuthorityCellID}}, Pools: p.RoutePlacementTransition.NextTopology.Pools, Edges: []edgetopology.Edge{edge}}}
	oldStatic := membershipInput(t, f.s, model.PlatformArtifactKindPlatformIntent, p.TargetScope, intent)
	topology, err := platformconfig.TrafficConsumerTopologyFromIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := platformconfig.Digest(topology)
	minimum, stale := 1, 3600
	rules := []platformconfig.RoutePolicyConstraint{}
	states := []platformconfig.DNSRouteStateConstraint{}
	projection := platformproducer.ProjectionPolicyInput{SchemaVersion: platformconfig.SchemaVersion, Generation: "projection-before", Scope: p.TargetScope, PublicationRole: p.PublicationRole, AuthorityCellID: p.AuthorityCellID, ConsumerTopologyDigest: digest, MinimumHealthyEdges: &minimum, MaxStaleSeconds: &stale, RouteConstraints: &rules, DNSRouteStateConstraints: &states, TLSReadiness: &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 10, ProbeTimeoutSeconds: 2, FactFreshnessSeconds: 60, MaxConcurrency: 4, MaxProbes: 1024}, Cohorts: []platformconfig.TrafficRolloutCohort{{ID: "complete", EdgeGroupIDs: []string{p.AuthorityCellID}}}}
	oldProjection := membershipInput(t, f.s, model.PlatformArtifactKindPolicySnapshot, p.TargetScope, projection)
	p.StaticIntentArtifactID, p.StaticIntentDigest = oldStatic.ID, oldStatic.ContentHash
	p.DNSPolicyArtifactID, p.DNSPolicyDigest = oldProjection.ID, oldProjection.ContentHash
	f.policy = p
	f.old = f.save(t, p)
	if _, err := platformproducer.Decode(f.old); err != nil {
		t.Fatal("membership fixture predecessor", err)
	}
	_, r, _, _, err := f.s.ReleasePlatformArtifact(f.old.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	f.request.ProducerReconfiguration.PreviousPolicy = model.PlatformPublicationPrecondition{ArtifactID: f.old.ID, ContentHash: f.old.ContentHash, ReleaseID: r.ID, FencingToken: r.FencingToken}
	f.seedCompletedPublication(t, r.ID, platformproducer.Actor)
	intent.Generation = "members-after"
	intent.EdgeTopology.Edges = append(intent.EdgeTopology.Edges, added)
	newStatic := membershipInput(t, f.s, model.PlatformArtifactKindPlatformIntent, p.TargetScope, intent)
	topology, err = platformconfig.TrafficConsumerTopologyFromIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	projection.Generation = "projection-after"
	projection.ConsumerTopologyDigest, _ = platformconfig.Digest(topology)
	newProjection := membershipInput(t, f.s, model.PlatformArtifactKindPolicySnapshot, p.TargetScope, projection)
	f.policy.Generation, f.policy.Mode = "membership-expanded", "shadow"
	f.policy.StaticIntentArtifactID, f.policy.StaticIntentDigest = newStatic.ID, newStatic.ContentHash
	f.policy.DNSPolicyArtifactID, f.policy.DNSPolicyDigest = newProjection.ID, newProjection.ContentHash
	f.next = f.save(t, f.policy)
	for _, id := range []string{p.StaticIntentArtifactID, f.policy.StaticIntentArtifactID} {
		a, err := f.s.GetPlatformArtifact(id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := platformproducer.DecodeStaticIntent(a); err != nil {
			t.Fatalf("fixture input %s: %v; kind=%s content=%v", id, err, a.ArtifactKind, a.Content)
		}
	}
	f.request.ProducerReconfiguration.Operation = "expand_membership"
	f.bindKey(t)
	return f
}

func testProducerMembershipExpansion(t *testing.T, address string) {
	t.Run("simultaneous successors", func(t *testing.T) { testSimultaneousProducerReconfiguration(t, address, membershipFixture) })
	for _, scenario := range []string{"success", "failed gray", "pending gray", "frozen gray", "implicit", "serving", "settings", "schedule", "wrong digest", "changed old member", "unknown added member", "two added members", "removed member", "route intent", "projection floor", "source invalid", "stale predecessor", "generation alias", "frozen baseline", "foreign baseline"} {
		t.Run(scenario, func(t *testing.T) {
			f := membershipFixture(t, address)
			p := f.policy
			oldPolicy, err := platformproducer.Decode(f.old)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "failed gray", "pending gray", "frozen gray":
				seedMembershipGray(t, f, scenario == "failed gray", scenario == "frozen gray")
			case "implicit":
				f.request.ProducerReconfiguration.Operation = ""
			case "serving":
				p.Mode = "serving"
			case "settings":
				p.Serving.FullMinSeconds++
			case "schedule":
				p.RefreshSeconds = 300
			case "wrong digest":
				p.StaticIntentDigest = "sha256:" + strings.Repeat("f", 64)
			case "source invalid":
				if _, err := f.s.ValidatePlatformArtifact(oldPolicy.StaticIntentArtifactID, []model.PlatformArtifactValidationResult{{Name: "invalid-source", Pass: false}}); err != nil {
					t.Fatal(err)
				}
			case "stale predecessor":
				f.request.ProducerReconfiguration.PreviousPolicy.FencingToken++
			case "generation alias":
				f.request.ProducerReconfiguration.PreviousPolicy.ArtifactID = f.old.Generation
			case "frozen baseline":
				f.freeze(t, p.TargetScope)
			case "foreign baseline":
				// Signed metadata naming the new inputs cannot stand in for the
				// predecessor's actual full input references.
				f.seedCompletedPublication(t, f.request.ProducerReconfiguration.PreviousPolicy.ReleaseID, platformproducer.Actor)
			case "projection floor":
				a, err := f.s.GetPlatformArtifact(p.DNSPolicyArtifactID)
				if err != nil {
					t.Fatal(err)
				}
				var value platformproducer.ProjectionPolicyInput
				raw, _ := json.Marshal(a.Content)
				if err := json.Unmarshal(raw, &value); err != nil {
					t.Fatal(err)
				}
				value.Generation = "changed-floor"
				*value.MinimumHealthyEdges = 2
				a = membershipInput(t, f.s, a.ArtifactKind, a.ScopeKey, value)
				p.DNSPolicyArtifactID, p.DNSPolicyDigest = a.ID, a.ContentHash
			case "changed old member", "unknown added member", "two added members", "removed member", "route intent":
				a, err := f.s.GetPlatformArtifact(p.StaticIntentArtifactID)
				if err != nil {
					t.Fatal(err)
				}
				var intent platformconfig.PlatformIntent
				raw, _ := json.Marshal(a.Content)
				if err := json.Unmarshal(raw, &intent); err != nil {
					t.Fatal(err)
				}
				intent.Generation = "changed-members"
				switch scenario {
				case "changed old member":
					intent.EdgeTopology.Edges[0].FailureDomains["provider"] = "different-provider"
				case "unknown added member":
					intent.EdgeTopology.Edges[1].ID = "undeclared"
				case "two added members":
					extra := intent.EdgeTopology.Edges[1]
					extra.ID = "extra"
					intent.EdgeTopology.Edges = append(intent.EdgeTopology.Edges, extra)
				case "removed member":
					intent.EdgeTopology.Edges = intent.EdgeTopology.Edges[1:]
				case "route intent":
					intent.Routes = []platformconfig.RouteIntent{{Hostname: "api.example.test", Kind: model.EdgeRouteKindControlPlaneAPI, UpstreamURL: "http://changed:8080", Enabled: true}}
				}
				a = membershipInput(t, f.s, a.ArtifactKind, a.ScopeKey, intent)
				p.StaticIntentArtifactID, p.StaticIntentDigest = a.ID, a.ContentHash
			}
			p.Generation = "membership-" + strings.ReplaceAll(scenario, " ", "-")
			f.next = f.save(t, p)
			f.bindKey(t)
			r, err := f.release()
			wanted := f.old.ID
			if scenario == "success" || scenario == "failed gray" {
				if err != nil {
					t.Fatal("membership expansion", err)
				}
				wanted = f.next.ID
				if again, err := f.release(); err != nil || again.ID != r.ID {
					t.Fatal("idempotent membership expansion", err)
				}
			} else if err == nil {
				t.Fatal("unsafe membership expansion admitted")
			}
			a, _, found, err := f.s.GetActivePlatformArtifact(f.old.ArtifactKind, f.old.ScopeKey, "shadow")
			if err != nil || !found || a.ID != wanted {
				t.Fatal("unexpected producer authority", err)
			}
			lkg, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, p.TargetScope)
			if err != nil || !reflect.DeepEqual(lkg, &f.lkg) {
				t.Fatal("membership changed positive LKG", err)
			}
			_, full, found, err := f.s.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "full")
			if err != nil || !found || full.ID != f.lkg.VerifiedByReleaseID {
				t.Fatal("membership changed full publication", err)
			}
		})
	}
}

func TestProducerMembershipExpansion(t *testing.T) { testProducerMembershipExpansion(t, "") }
func TestProducerMembershipExpansionPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testProducerMembershipExpansion(t, address)
}
