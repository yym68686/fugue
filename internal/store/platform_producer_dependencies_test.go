package store

import (
	"context"
	"errors"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/testfixture/celldns"
)

func dependencyFixture(t *testing.T, address string) reconfigurationFixture {
	t.Helper()
	f := continuousFixture(t, address)
	r, err := f.release()
	if err != nil {
		t.Fatal(err)
	}
	f.old = f.next
	f.request.ProducerReconfiguration.PreviousPolicy = model.PlatformPublicationPrecondition{ArtifactID: f.old.ID, ContentHash: f.old.ContentHash, ReleaseID: r.ID, FencingToken: r.FencingToken}
	f.seedCompletedPublication(t, r.ID, platformproducer.Actor)
	f.request.ProducerReconfiguration.Operation = "placement_constraints"
	f.policy.Generation = "explicit-constraint-refresh"
	// A newly observed source is authorized by the new signed producer; the
	// previous topology and every unrelated source setting remain exact.
	pin := f.policy.RoutePlacementTransition.Constraints[0]
	pin.Source.Hostname = "second.example.test"
	pin.Source.ID = "constraint-second"
	pin.SourceDigest, _ = platformproducer.RouteConstraintDigest(pin.Source)
	f.policy.RoutePlacementTransition.Constraints = append(f.policy.RoutePlacementTransition.Constraints, pin)
	f.next = f.save(t, f.policy)
	f.bindKey(t)
	return f
}

func testProducerDependencyRefresh(t *testing.T, address string) {
	t.Run("concurrent successors", func(t *testing.T) { testSimultaneousProducerReconfiguration(t, address, dependencyFixture) })
	for _, scenario := range []string{"success", "topology", "schedule", "source", "serving", "stale", "wrong evidence", "unverified", "frozen"} {
		t.Run(scenario, func(t *testing.T) {
			f := dependencyFixture(t, address)
			p := f.policy
			switch scenario {
			case "topology":
				p.RoutePlacementTransition.NextTopology.Edges[0].Labels = map[string]string{"region": "changed"}
			case "schedule":
				p.RefreshSeconds++
			case "source":
				p.StaticIntentArtifactID = "foreign"
			case "serving":
				p.Serving.FullMinSeconds++
			case "stale":
				f.request.ProducerReconfiguration.PreviousPolicy.FencingToken++
			case "wrong evidence":
				f.request.ProducerReconfiguration.VerificationEvidenceHash = "sha256:" + strings.Repeat("f", 64)
			case "unverified":
				f.setBaselineVerification(t, false)
			case "frozen":
				f.freeze(t, p.TargetScope)
			}
			p.Generation += "-" + strings.ReplaceAll(scenario, " ", "-")
			f.next = f.save(t, p)
			f.bindKey(t)
			r, err := f.release()
			if scenario == "success" {
				if err != nil {
					t.Fatal(err)
				}
				if again, err := f.release(); err != nil || again.ID != r.ID {
					t.Fatal("retry changed successor", err)
				}
			} else if err == nil {
				t.Fatal("unrelated mutation accepted")
			}
			lkg, err := f.s.GetPlatformLKG(f.lkg.ArtifactKind, f.lkg.ScopeKey)
			if err != nil || !reflect.DeepEqual(lkg, &f.lkg) {
				t.Fatal("dependency repair rewrote LKG", err)
			}
		})
	}
	t.Run("route writes fail before persistence", func(t *testing.T) {
		f := dependencyFixture(t, address)
		if _, err := f.release(); err != nil {
			t.Fatal(err)
		}
		rule := f.policy.RoutePlacementTransition.Constraints[0].Source
		policy := model.EdgeRoutePolicy{ID: rule.ID, Hostname: rule.Hostname, AppID: rule.AppID, TenantID: rule.TenantID, EdgeGroupID: rule.EdgeGroupID, RoutePolicy: rule.RoutePolicy, Enabled: rule.Enabled}
		// The fixture pin is app-scoped; a normal tenant route projects explicit
		// tenant_hostname and must not silently change that pinned scope either.
		if _, err := f.s.PutEdgeRoutePolicy(policy); !errors.Is(err, ErrConflict) {
			t.Fatal("pinned source changed", err)
		}
		policy.Hostname = "new.example.test"
		if _, err := f.s.PutEdgeRoutePolicy(policy); !errors.Is(err, ErrConflict) {
			t.Fatal("unlisted alias accepted", err)
		}
		if _, err := f.s.GetEdgeRoutePolicy(policy.Hostname); !errors.Is(err, ErrNotFound) {
			t.Fatal("rejected write persisted", err)
		}
		policy.EdgeGroupID = "cell-a"
		if f.s.db != nil {
			tenant, err := f.s.CreateTenant("route mutation fixture")
			if err != nil {
				t.Fatal(err)
			}
			project, err := f.s.CreateProject(tenant.ID, "route mutation fixture", "")
			if err != nil {
				t.Fatal(err)
			}
			app, err := f.s.CreateAppWithRoute(tenant.ID, project.ID, "route mutation fixture", "", model.AppSpec{Image: "registry.example.test/app:latest", Ports: []int{8080}, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID}, model.AppRoute{Hostname: policy.Hostname, BaseDomain: "example.test", ServicePort: 8080})
			if err != nil {
				t.Fatal(err)
			}
			policy.AppID, policy.TenantID = app.ID, tenant.ID
		}
		if _, err := f.s.PutEdgeRoutePolicy(policy); err != nil {
			t.Fatal("unrelated neutral constraint blocked", err)
		}
	})
}

func TestProducerDependencyRefresh(t *testing.T) { testProducerDependencyRefresh(t, "") }
func TestProducerDependencyRefreshPostgres(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable PostgreSQL required")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback PostgreSQL")
	}
	testProducerDependencyRefresh(t, dsn)
	testDNSDependencyRefresh(t, dsn)
}

func dnsDependencyFixture(t *testing.T, address string) reconfigurationFixture {
	t.Helper()
	f := newReconfigurationFixture(t, address)
	req, policies, releases := celldns.SourceAuthorizedRequest(t, true)
	celldns.RetainGlobalOnly(&req)
	pin := *req.PreviousTrafficPublication.ProducerPolicy
	for i, a := range policies {
		if a.ID == pin.ArtifactID {
			if _, _, err := f.s.EnsurePlatformArtifact(a); err != nil {
				t.Fatal(err)
			}
			seedDNSRouteRelease(t, f.s, releases[i])
		}
	}
	seedCellDNSRoutePublications(t, f.s, &req)
	p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "dns-before", Mode: "serving", InputSource: platformproducer.DNSProducerInputSource, PublicationRole: platformconfig.PublicationRoleCellDNS, AuthorityCellID: "cell-dns", TargetScope: "authority-cell:cell-dns", IntervalSeconds: 30, RefreshSeconds: 120, Serving: &platformproducer.ServingPolicy{CanaryRuleRef: "cohort=complete", GrayMinSeconds: 1, FullMinSeconds: 1, RolloutTimeoutSeconds: 60}, DNSSource: &platformproducer.DNSProducerSource{ProducerPolicy: pin, PreviousTopology: req.Intent.RouteAuthorityTransition.PreviousTopology, DNSNodeIDs: []string{"dns-a"}}}
	p.DNSSource.ProducerPolicy.ArtifactID = "previous-approved-policy"
	f.old = f.save(t, p)
	_, oldRelease, _, _, err := f.s.ReleasePlatformArtifact(f.old.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	compiled := celldns.Compile(t, req)
	child, _, err := f.s.EnsurePlatformArtifact(compiled.DNSArtifact)
	if err != nil {
		t.Fatal(err)
	}
	parent := platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, []string{child.ID}, time.Now().UTC())
	parent.Metadata[platformproducer.PolicyReleaseMetadata] = oldRelease.ID
	parent, err = f.s.CreatePlatformArtifact(parent)
	if err != nil {
		t.Fatal(err)
	}
	parent, err = f.s.ValidatePlatformArtifact(parent.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	full := model.PlatformArtifactRelease{ID: "dns-full", ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind, Scope: parent.Scope, ScopeKey: parent.ScopeKey, Generation: parent.Generation, ReleaseChannel: "full", Status: model.PlatformArtifactReleaseStatusActive, LaneKey: platformsafety.ReleaseLaneKey(parent.ArtifactKind, parent.ScopeKey, "full"), FencingToken: 1, Version: 1, VerificationState: model.PlatformArtifactVerificationStateVerified, VerifiedLKGGeneration: parent.Generation, VerifiedAt: &now, ReleasedByType: model.ActorTypeBootstrap, ReleasedByID: platformproducer.Actor, ReleasedAt: now}
	seedDNSRouteRelease(t, f.s, full)
	lkg, err := buildPlatformLKGSnapshot(parent, full.ID, "sha256:"+strings.Repeat("a", 64), now, celldns.Keys())
	if err != nil {
		t.Fatal(err)
	}
	if f.s.db != nil {
		_, err = pgUpsertPlatformLKGSnapshot(context.Background(), f.s.db, lkg)
	} else {
		err = f.s.withLockedState(true, func(st *model.State) error {
			st.PlatformLKGSnapshots = append(st.PlatformLKGSnapshots, lkg)
			return nil
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	storedLKG, err := f.s.GetPlatformLKG(lkg.ArtifactKind, lkg.ScopeKey)
	if err != nil || storedLKG == nil {
		t.Fatal("stored DNS baseline unavailable", err)
	}
	f.lkg = *storedLKG
	p.Generation = "dns-approved-successor"
	p.DNSSource.ProducerPolicy = pin
	f.policy = p
	f.next = f.save(t, p)
	f.request = model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", ProducerReconfiguration: &model.PlatformProducerReconfiguration{Operation: "dns_source", PreviousPolicy: model.PlatformPublicationPrecondition{ArtifactID: f.old.ID, ContentHash: f.old.ContentHash, ReleaseID: oldRelease.ID, FencingToken: oldRelease.FencingToken}, ServingFull: model.PlatformPublicationPrecondition{ArtifactID: parent.ID, ContentHash: parent.ContentHash, ReleaseID: full.ID, FencingToken: full.FencingToken}, VerificationEvidenceHash: lkg.VerificationEvidenceHash}}
	f.bindKey(t)
	return f
}

func testDNSDependencyRefresh(t *testing.T, address string) {
	for _, scenario := range []string{"success", "source fence", "membership", "topology", "serving", "stale"} {
		t.Run("dns "+scenario, func(t *testing.T) {
			f := dnsDependencyFixture(t, address)
			p := f.policy
			switch scenario {
			case "source fence":
				p.DNSSource.ProducerPolicy.FencingToken++
			case "membership":
				p.DNSSource.DNSNodeIDs = []string{"dns-b"}
			case "topology":
				p.DNSSource.PreviousTopology.Edges[0].Labels = map[string]string{"region": "changed"}
			case "serving":
				p.Serving.FullMinSeconds++
			case "stale":
				f.request.ProducerReconfiguration.PreviousPolicy.FencingToken++
			}
			p.Generation += "-" + strings.ReplaceAll(scenario, " ", "-")
			f.next = f.save(t, p)
			f.bindKey(t)
			r, err := f.release()
			if scenario == "success" {
				if err != nil {
					t.Fatal(err)
				}
				if again, err := f.release(); err != nil || again.ID != r.ID {
					t.Fatal("DNS retry", err)
				}
			} else if err == nil {
				t.Fatal("unsafe DNS handoff accepted")
			}
			lkg, err := f.s.GetPlatformLKG(f.lkg.ArtifactKind, f.lkg.ScopeKey)
			if err != nil || !reflect.DeepEqual(lkg, &f.lkg) {
				t.Fatal("DNS handoff rewrote positive LKG", err)
			}
		})
	}
}

func TestDNSDependencyRefresh(t *testing.T) { testDNSDependencyRefresh(t, "") }
