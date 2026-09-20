package store

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
)

type servingFixture struct {
	s         *Store
	policy    platformproducer.Policy
	authority model.PlatformArtifactRelease
	input     platformconfig.PolicySnapshot
	base      model.PlatformArtifact
}

func newServingFixture(t *testing.T, dsn string) servingFixture {
	t.Helper()
	s := New(t.TempDir()+"/state.json", dsn)
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	if dsn != "" {
		t.Cleanup(func() { s.db.Close() })
	}
	save := func(kind, scope, generation string, content any) model.PlatformArtifact {
		t.Helper()
		raw, err := json.Marshal(content)
		if err != nil {
			t.Fatal(err)
		}
		var value map[string]any
		json.Unmarshal(raw, &value)
		a, err := s.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: kind, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: scope}, Generation: generation, Content: value})
		if err != nil {
			t.Fatal(err)
		}
		a, err = s.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	generation := model.NewID("base-input")
	intent := platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Generation: generation, Scope: "global", ApplicationDomains: &platformconfig.ApplicationDomainsIntent{AppBaseDomain: "example.test", ReservedHostnames: []string{}, DefaultDNSTTL: 60}, DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}}
	base := save(model.PlatformArtifactKindPlatformIntent, "global", generation, intent)
	probe := &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 1, FactFreshnessSeconds: 60, MaxConcurrency: 2, MaxProbes: 10}
	input := platformconfig.PolicySnapshot{SchemaVersion: platformconfig.SchemaVersion, Generation: model.NewID("input"), Scope: "global", MinimumHealthyEdges: 1, MaxStaleSeconds: 120, DNSPlacementMode: platformconfig.DNSPlacementConsumerReadiness, DNSQueryPolicy: &platformconfig.DNSQueryPolicy{RankingMode: "disabled", PreferenceMode: "runtime_locality", MinimumTTLSeconds: 60, MaximumTTLSeconds: 120}, DNSReadiness: probe, TLSReadiness: probe, DNSAuthorities: []platformconfig.DNSAuthorityPolicy{{NodeID: "dns-a", Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}}, DNSClientPolicies: []platformconfig.DNSClientPolicy{{NodeID: "dns-a"}}, TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "test", EdgeGroupIDs: []string{"edge-group-a"}}}}
	rules := []platformconfig.RoutePolicyConstraint{}
	states := []platformconfig.DNSRouteStateConstraint{}
	source := platformproducer.ProjectionPolicyInput{SchemaVersion: input.SchemaVersion, Generation: input.Generation, Scope: "global", MinimumHealthyEdges: &input.MinimumHealthyEdges, MaxStaleSeconds: &input.MaxStaleSeconds, RouteConstraints: &rules, DNSRouteStateConstraints: &states, DNSPlacementMode: input.DNSPlacementMode, DNSQueryPolicy: input.DNSQueryPolicy, DNSReadiness: probe, TLSReadiness: probe, Authorities: input.DNSAuthorities, Clients: input.DNSClientPolicies, Cohorts: input.TrafficRolloutCohorts}
	sourceArtifact := save(model.PlatformArtifactKindPolicySnapshot, "global", source.Generation, source)
	p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: model.NewID("producer"), Mode: "serving", InputSource: "business-static-intent", TargetScope: "global", IntervalSeconds: 30, RefreshSeconds: 300, RequireApplicationDomains: true, RequireDNSQueryPolicy: true, RequireRouteDefaults: true, StaticIntentArtifactID: base.ID, StaticIntentDigest: base.ContentHash, DNSPolicyArtifactID: sourceArtifact.ID, DNSPolicyDigest: sourceArtifact.ContentHash, Serving: &platformproducer.ServingPolicy{CanaryRuleRef: "cohort=test", GrayMinSeconds: 1, FullMinSeconds: 1, RolloutTimeoutSeconds: 60}}
	owner := save(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, p.Generation, p)
	_, authority, _, _, err := s.ReleasePlatformArtifact(owner.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	return servingFixture{s: s, policy: p, authority: authority, input: input, base: base}
}

func (f servingFixture) candidate(t *testing.T) model.PlatformArtifact {
	t.Helper()
	now := time.Now().UTC()
	generation := model.NewID("candidate")
	request := platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: generation, Scope: "global", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: true}}, DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}}, Policy: f.input, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, DNSConsumers: []platformconfig.DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", ObservedAt: now, A: []string{"8.8.8.8"}}}}}
	request.Policy.Generation = generation
	compiled, err := platformconfig.Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	save := func(a model.PlatformArtifact) model.PlatformArtifact {
		t.Helper()
		a, err := f.s.CreatePlatformArtifact(a)
		if err != nil {
			t.Fatal(err)
		}
		a, err = f.s.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	save(compiled.PolicyArtifact)
	ids := []string{}
	for _, a := range []model.PlatformArtifact{compiled.RouteArtifact, compiled.DNSArtifact, compiled.TLSArtifact} {
		ids = append(ids, save(a).ID)
	}
	parent := platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, ids, now)
	parent.Metadata[platformproducer.PolicyReleaseMetadata] = f.authority.ID
	parent.Metadata[platformproducer.SourceDigestMetadata] = "source-" + generation
	parent.Metadata[platformproducer.StaticIntentIDMetadata] = f.policy.StaticIntentArtifactID
	parent.Metadata[platformproducer.StaticIntentDigestMetadata] = f.policy.StaticIntentDigest
	parent.Metadata[platformproducer.DNSPolicyIDMetadata] = f.policy.DNSPolicyArtifactID
	parent.Metadata[platformproducer.DNSPolicyDigestMetadata] = f.policy.DNSPolicyDigest
	return save(parent)
}

func producerTestPrincipal() model.Principal {
	return model.Principal{ActorType: model.ActorTypeBootstrap, ActorID: platformproducer.Actor, Scopes: map[string]struct{}{"platform.admin": {}}}
}

func reportProducedPublication(t *testing.T, s *Store, parent model.PlatformArtifact, r model.PlatformArtifactRelease, positive bool) {
	t.Helper()
	prepareProducedPublication(t, s, parent, r, positive, true)
}
func prepareProducedPublication(t *testing.T, s *Store, parent model.PlatformArtifact, r model.PlatformArtifactRelease, positive, report bool) {

	t.Helper()
	now := time.Now().UTC()
	topology := platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a"}}, DNSNodes: []model.DNSNode{{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test"}}}
	for n, id := range parent.Content["artifact_ids"].([]any) {
		child, err := s.GetPlatformArtifact(id.(string))
		if err != nil {
			t.Fatal(err)
		}
		set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: parent.ID, ArtifactReleaseID: r.ID, ArtifactKind: child.ArtifactKind, Scope: child.Scope, ScopeKey: "global", Generation: child.Generation, Revision: now.UnixNano() + int64(n), PreparedAt: now, Topology: topology})
		if err != nil {
			t.Fatal(err)
		}
		set, err = s.CreatePlatformExpectedConsumerSet(set)
		if err != nil {
			t.Fatal(err)
		}
		if !report {
			continue
		}
		previous, err := s.ListPlatformConsumers(child.ArtifactKind, "global")
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
			seq := int64(1)
			for _, old := range previous {
				if old.ConsumerID == c.ConsumerID {
					seq = max(seq, old.Sequence+1)
				}
			}
			keys := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "key", Keys: map[string]string{"key": "synthetic-serving-producer-identity"}}
			claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "credential", TokenID: "token", Component: c.Component, NodeID: c.NodeID, ScopeKey: "global", ArtifactKinds: []string{child.ArtifactKind}}
			token, err := platformcontrol.IssuePlatformComponentIdentity(keys, claims, now, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			claims, err = platformcontrol.ParsePlatformComponentIdentity(keys, token, now)
			if err != nil {
				t.Fatal(err)
			}
			h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: c.ConsumerID, Component: c.Component, NodeID: c.NodeID, ArtifactKind: child.ArtifactKind, ScopeKey: "global", ReleaseSetID: parent.ID, ExpectedConsumerSetID: set.ID, FencingToken: r.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: seq, IssuedAt: time.Now().UTC(), Nonce: model.NewID("nonce"), GenerationSequence: child.GenerationSequence, DesiredGeneration: child.Generation, ApplyStatus: "applied", ProbeStatus: "passed", ActualGeneration: child.Generation, LKGGeneration: child.Generation, CompatibilityCapabilities: []string{platformcontrol.TrafficReleaseCapabilityV1}}
			if r.ReleaseChannel == "shadow" {
				h.ActualGeneration = ""
				h.LKGGeneration = ""
				h.CandidateGeneration = child.Generation
				h.ApplyStatus = "staged"
				h.ProbeStatus = "shadow_validated"
			} else if !positive {
				h.ProbeStatus = "failed"
			}
			h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, h, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func ageProducerPublication(t *testing.T, s *Store, id string, age time.Duration) {
	t.Helper()
	when := time.Now().Add(-age)
	if s.usingDatabase() {
		if _, err := s.db.Exec(`UPDATE fugue_platform_artifact_releases SET released_at=$2 WHERE id=$1`, id, when); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err := s.withLockedState(true, func(st *model.State) error {
		st.PlatformArtifactReleases[platformArtifactReleaseIndex(st.PlatformArtifactReleases, id)].ReleasedAt = when
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestProducedTrafficLifecycle(t *testing.T) { testProducedTrafficLifecycle(t, "") }
func TestProducedTrafficLifecyclePostgres(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires disposable PostgreSQL")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != "/fugue_test_serving" {
		t.Fatal("requires isolated loopback fugue_test_serving database")
	}
	testProducedTrafficLifecycle(t, dsn)
}
func testProducedTrafficLifecycle(t *testing.T, dsn string) {
	f := newServingFixture(t, dsn)
	s := f.s
	p := producerTestPrincipal()
	candidate := f.candidate(t)
	_, shadow, _, _, err := s.ReleaseProducedPlatformArtifact(candidate.ID, f.authority.ID, "", p)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err = s.ReleaseProducedTrafficArtifact(candidate.ID, f.authority.ID, "gray", "", "", "missing", "cohort=test", p); !errors.Is(err, ErrConflict) {
		t.Fatal("automatic bootstrap accepted", err)
	}
	// Seed the explicit operator baseline through real gray/full verification.
	baseline := prepareTrafficLKGFixture(t, s, "global", "gray", true)
	_, full, _, _, err := s.ReleasePlatformArtifact(baseline.parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "full"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	reportProducedPublication(t, s, baseline.parent, full, true)
	_, full, _, lkg, err := s.VerifyPlatformArtifactReleaseLKG(full.ID, completePlatformVerificationRequest(full.FencingToken, false), testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	candidate = f.candidate(t)
	_, shadow, _, _, err = s.ReleaseProducedPlatformArtifact(candidate.ID, f.authority.ID, shadow.ID, p)
	if err != nil {
		t.Fatal(err)
	}
	reportProducedPublication(t, s, candidate, shadow, true)
	_, gray, _, _, err := s.ReleaseProducedTrafficArtifact(candidate.ID, f.authority.ID, "gray", baseline.release.ID, full.ID, lkg.ArtifactID, "cohort=test", p)
	if err != nil {
		t.Fatal("gray", err)
	}
	if _, _, _, _, err = s.ReleaseProducedTrafficArtifact(candidate.ID, f.authority.ID, "full", full.ID, full.ID, lkg.ArtifactID, "", p); !errors.Is(err, ErrConflict) {
		t.Fatal("gray age or missing applied facts accepted", err)
	}
	reportProducedPublication(t, s, candidate, gray, true)
	// Keep ordering relative to the baseline full publication while aging gray.
	ageProducerPublication(t, s, full.ID, 10*time.Second)
	ageProducerPublication(t, s, shadow.ID, 5*time.Second)
	ageProducerPublication(t, s, gray.ID, 2*time.Second)
	_, next, _, _, err := s.ReleaseProducedTrafficArtifact(candidate.ID, f.authority.ID, "full", full.ID, full.ID, lkg.ArtifactID, "", p)
	if err != nil {
		t.Fatal("full", err)
	}
	request := completePlatformVerificationRequest(next.FencingToken, false)
	if _, _, _, _, err = s.VerifyProducedTrafficLKG(next.ID, f.authority.ID, lkg.ArtifactID, request, p); !errors.Is(err, ErrConflict) {
		t.Fatal("old gray facts verified full", err)
	}
	reportProducedPublication(t, s, candidate, next, true)
	ageProducerPublication(t, s, next.ID, 2*time.Second)
	if dsn != "" {
		// The policy is revoked while a verification transaction waits for its
		// scope lock: the guard must read the committed revocation, not a
		// preflight snapshot. No serving or recovery pointer may change.
		tx, err := s.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if err = pgLockPromotionScope(context.Background(), tx, platformproducer.Scope, true); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, _, _, _, err := s.VerifyProducedTrafficLKG(next.ID, f.authority.ID, lkg.ArtifactID, completePlatformVerificationRequest(next.FencingToken, false), p)
			done <- err
		}()
		deadline := time.Now().Add(5 * time.Second)
		for {
			var n int
			if err = s.db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND wait_event='advisory'`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n > 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("verification did not acquire policy scope before mutable reads")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if _, err = tx.Exec(`UPDATE fugue_platform_release_lanes SET frozen=true WHERE scope_key=$1`, platformproducer.Scope); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if !errors.Is(err, ErrConflict) {
				t.Fatal("queued verification ignored revocation", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("verification deadlocked")
		}
		if _, err = s.db.Exec(`UPDATE fugue_platform_release_lanes SET frozen=false WHERE scope_key=$1`, platformproducer.Scope); err != nil {
			t.Fatal(err)
		}
	}
	_, next, _, positive, err := s.VerifyProducedTrafficLKG(next.ID, f.authority.ID, lkg.ArtifactID, request, p)
	if err != nil {
		t.Fatal("verify", err)
	}
	if positive.ArtifactID != candidate.ID {
		t.Fatal("wrong LKG")
	}
	// Both gray and full failures recover without revalidating their revoked
	// inputs, while preserving all five positive recovery pointers.
	for _, phase := range []string{"gray", "full", "policy revision"} {
		if _, err = s.ValidatePlatformArtifact(f.policy.DNSPolicyArtifactID, []model.PlatformArtifactValidationResult{{Name: "restored input", Pass: true}}); err != nil {
			t.Fatal(err)
		}
		failed := f.candidate(t)
		_, freshShadow, _, _, err := s.ReleaseProducedPlatformArtifact(failed.ID, f.authority.ID, shadow.ID, p)
		if err != nil {
			t.Fatal(err)
		}
		shadow = freshShadow
		prepareProducedPublication(t, s, failed, freshShadow, false, false)
		for _, bad := range []struct{ previous, baseline, cohort string }{
			{"stale-full", positive.ArtifactID, "cohort=test"},
			{next.ID, "wrong-baseline", "cohort=test"},
			{next.ID, positive.ArtifactID, "cohort=absent"},
		} {
			if _, _, _, _, err := s.ReleaseProducedTrafficArtifact(failed.ID, f.authority.ID, "gray", gray.ID, bad.previous, bad.baseline, bad.cohort, p); !errors.Is(err, ErrConflict) {
				t.Fatal("stale authority or invalid cohort accepted", err)
			}
		}
		_, badGray, _, _, err := s.ReleaseProducedTrafficArtifact(failed.ID, f.authority.ID, "gray", gray.ID, next.ID, positive.ArtifactID, "cohort=test", p)
		if err != nil {
			t.Fatal("next gray", phase, err)
		}
		gray = badGray
		badRelease := badGray
		if phase == "full" {
			reportProducedPublication(t, s, failed, badGray, true)
			ageProducerPublication(t, s, next.ID, 120*time.Second)
			ageProducerPublication(t, s, freshShadow.ID, 5*time.Second)
			ageProducerPublication(t, s, badGray.ID, 2*time.Second)
			_, badRelease, _, _, err = s.ReleaseProducedTrafficArtifact(failed.ID, f.authority.ID, "full", next.ID, next.ID, positive.ArtifactID, "", p)
			if err != nil {
				t.Fatal("failing full", err)
			}
			next = badRelease
		}
		reportProducedPublication(t, s, failed, badRelease, false)
		rollback := func(authority string) (model.PlatformArtifactRelease, error) {
			_, restored, _, _, err := s.RollbackProducedTraffic(failed.ID, badRelease.ID, authority, next.ID, positive.ArtifactID, model.PlatformArtifactRollbackRequest{ReleaseChannel: "full", ToGeneration: positive.Generation, Reason: "recover candidate"}, p)
			return restored, err
		}
		if _, err := rollback(f.authority.ID); !errors.Is(err, ErrConflict) {
			t.Fatal("premature automatic rollback", err)
		}
		if phase == "policy revision" {
			oldAuthority := f.authority
			f.policy.Generation = model.NewID("revised-producer")
			raw, _ := json.Marshal(f.policy)
			var content map[string]any
			json.Unmarshal(raw, &content)
			a, err := s.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: platformproducer.Scope}, Generation: f.policy.Generation, Content: content})
			if err != nil {
				t.Fatal(err)
			}
			a, err = s.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "policy", Pass: true}})
			if err != nil {
				t.Fatal(err)
			}
			_, f.authority, _, _, err = s.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := rollback(oldAuthority.ID); !errors.Is(err, ErrConflict) {
				t.Fatal("superseded authority mutated serving", err)
			}
		} else {
			if phase == "gray" {
				ageProducerPublication(t, s, next.ID, 120*time.Second)
			}
			if phase == "full" {
				ageProducerPublication(t, s, badGray.ID, 120*time.Second)
			}
			ageProducerPublication(t, s, badRelease.ID, 61*time.Second)
		}
		if _, err = s.ValidatePlatformArtifact(f.policy.DNSPolicyArtifactID, []model.PlatformArtifactValidationResult{{Name: "revoked input", Pass: false}}); err != nil {
			t.Fatal(err)
		}
		before := trafficLKGState(t, s, "global")
		restored, err := rollback(f.authority.ID)
		if err != nil {
			t.Fatal("rollback of invalid source", phase, err)
		}
		if restored.ArtifactID != positive.ArtifactID || !reflect.DeepEqual(before, trafficLKGState(t, s, "global")) {
			t.Fatal("rollback changed verified recovery pointers")
		}
		for _, id := range []string{badGray.ID, badRelease.ID} {
			rejected, err := s.GetPlatformArtifactRelease(id)
			if err != nil || rejected.VerificationState != "failed" || rejected.VerificationEvidence["failed_source_digest"] != failed.Metadata[platformproducer.SourceDigestMetadata] || rejected.VerificationEvidence["recovered_by_release_id"] != restored.ID {
				t.Fatal("failed candidate not recorded", phase, err)
			}
		}
		if _, err = rollback(f.authority.ID); !errors.Is(err, ErrConflict) {
			t.Fatal("stale rollback duplicated mutation", err)
		}
		reportProducedPublication(t, s, candidate, restored, true)
		next = restored
	}
}
