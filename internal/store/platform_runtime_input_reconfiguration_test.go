package store

import (
	"encoding/json"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/testfixture/celldns"
)

func runtimeInputFixture(t *testing.T, address string) reconfigurationFixture {
	t.Helper()
	f := physicalDNSReconfigurationFixture(t, address)
	return runtimeInputSuccessor(t, f)
}

func cellRuntimeInputFixture(t *testing.T, address string) reconfigurationFixture {
	t.Helper()
	f := newReconfigurationFixture(t, address)
	previous, err := platformproducer.Decode(f.old)
	if err != nil {
		t.Fatal(err)
	}
	request, _, _, inputs, _ := celldns.CompletePublication(t, false, false)
	for _, input := range inputs {
		if input.PolicyArtifact.Scope.Key != request.CellRoutePublications[0].Parent.ScopeKey {
			continue
		}
		// This fixture supplies explicit supported business inputs. A compiled
		// intent also contains generated TLS and application execution fields,
		// which are deliberately not accepted as producer static intent.
		var intent platformconfig.PlatformIntent
		raw, _ := json.Marshal(input.IntentArtifact.Content)
		if err := json.Unmarshal(raw, &intent); err != nil {
			t.Fatal(err)
		}
		intent.Generation, intent.TLS, intent.ACMEChallenges, intent.CachePolicies = "cell-static", nil, nil, nil
		for i := range intent.Routes {
			intent.Routes[i].AppID, intent.Routes[i].TenantID = "", ""
		}
		var policy platformconfig.PolicySnapshot
		raw, _ = json.Marshal(input.PolicyArtifact.Content)
		if err := json.Unmarshal(raw, &policy); err != nil {
			t.Fatal(err)
		}
		rules := []platformconfig.RoutePolicyConstraint{}
		states := []platformconfig.DNSRouteStateConstraint{}
		projectionInput := platformproducer.ProjectionPolicyInput{SchemaVersion: platformconfig.SchemaVersion, Generation: "cell-projection", Scope: intent.Scope, PublicationRole: intent.PublicationRole, AuthorityCellID: intent.AuthorityCellID, ConsumerTopologyDigest: policy.ConsumerTopologyDigest, MinimumHealthyEdges: &policy.MinimumHealthyEdges, MaxStaleSeconds: &policy.MaxStaleSeconds, RouteConstraints: &rules, DNSRouteStateConstraints: &states, TLSReadiness: policy.TLSReadiness, Cohorts: policy.TrafficRolloutCohorts}
		static := savePhysicalDNSTestArtifact(t, f.s, model.PlatformArtifactKindPlatformIntent, intent.Scope, intent.Generation, intent)
		projection := savePhysicalDNSTestArtifact(t, f.s, model.PlatformArtifactKindPolicySnapshot, intent.Scope, projectionInput.Generation, projectionInput)
		previous.StaticIntentArtifactID, previous.StaticIntentDigest = static.ID, static.ContentHash
		previous.DNSPolicyArtifactID, previous.DNSPolicyDigest = projection.ID, projection.ContentHash
	}
	previous.Generation, previous.Mode = "cell-continuous", "serving"
	previous.Serving = &platformproducer.ServingPolicy{CanaryRuleRef: "cohort=complete", GrayMinSeconds: 1, FullMinSeconds: 1, RolloutTimeoutSeconds: 60}
	f.policy = previous
	f.old = f.save(t, previous)
	_, authority, _, _, err := f.s.ReleasePlatformArtifact(f.old.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	f.request.ProducerReconfiguration.PreviousPolicy = model.PlatformPublicationPrecondition{ArtifactID: f.old.ID, ContentHash: f.old.ContentHash, ReleaseID: authority.ID, FencingToken: authority.FencingToken}
	f.seedCompletedPublication(t, authority.ID, platformproducer.Actor)
	f.setBaselineVerification(t, true)
	return runtimeInputSuccessor(t, f)
}

func runtimeInputSuccessor(t *testing.T, f reconfigurationFixture) reconfigurationFixture {
	t.Helper()
	baseline, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, f.policy.TargetScope)
	if err != nil || baseline == nil {
		t.Fatal("stored baseline unavailable", err)
	}
	// Compare persisted timestamps (PostgreSQL microsecond precision), rather
	// than the nanosecond-valued object originally constructed by the fixture.
	f.lkg = *baseline
	previous, err := platformproducer.Decode(f.old)
	if err != nil {
		t.Fatal(err)
	}
	source, err := f.s.GetPlatformArtifact(previous.DNSPolicyArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	source.Content["generation"] = "runtime-projection"
	source.Content["runtime_input_mode"] = platformconfig.RuntimeInputConfigurationV1
	source = savePhysicalDNSTestArtifact(t, f.s, source.ArtifactKind, source.ScopeKey, "runtime-projection", source.Content)
	f.policy = previous
	f.policy.Generation = "runtime-producer"
	f.policy.DNSPolicyArtifactID, f.policy.DNSPolicyDigest = source.ID, source.ContentHash
	f.next = savePhysicalDNSTestArtifact(t, f.s, model.PlatformArtifactKindPolicySnapshot, f.old.ScopeKey, f.policy.Generation, f.policy)
	static, err := f.s.GetPlatformArtifact(previous.StaticIntentArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	oldSource, err := f.s.GetPlatformArtifact(previous.DNSPolicyArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	if err := platformproducer.ValidateRuntimeInputUpdate(previous, f.policy, static, oldSource, source); err != nil {
		t.Fatal("fixture requires valid exact signed sources", err)
	}
	f.request.ProducerReconfiguration.Operation = "runtime_input"
	f.bindKey(t)
	return f
}

func TestRuntimeInputReconfigurationPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	for _, fixture := range []struct {
		name string
		make func(*testing.T, string) reconfigurationFixture
	}{{"global", runtimeInputFixture}, {"cell", cellRuntimeInputFixture}} {
		t.Run(fixture.name, func(t *testing.T) {
			t.Run("simultaneous", func(t *testing.T) { testSimultaneousProducerReconfiguration(t, address, fixture.make) })
			for _, scenario := range []string{"success", "stale-full", "unverified"} {
				t.Run(scenario, func(t *testing.T) {
					f := fixture.make(t, address)
					if scenario == "stale-full" {
						f.request.ProducerReconfiguration.ServingFull.FencingToken++
					} else if scenario == "unverified" {
						f.setBaselineVerification(t, false)
					}
					f.bindKey(t)
					_, err := f.release()
					if (scenario == "success") != (err == nil) {
						t.Fatal(scenario, err)
					}
					selected, _, found, err := f.s.GetActivePlatformArtifact(f.old.ArtifactKind, f.old.ScopeKey, "shadow")
					want := f.old.ID
					if scenario == "success" {
						want = f.next.ID
					}
					if err != nil || !found || selected.ID != want {
						t.Fatal("transaction selected incorrect successor", err)
					}
					lkg, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, f.policy.TargetScope)
					if err != nil || !reflect.DeepEqual(lkg, &f.lkg) {
						t.Fatal("transaction changed serving LKG", err)
					}
				})
			}
		})
	}
}
