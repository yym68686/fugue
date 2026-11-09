package store

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/testfixture/celldns"
)

func TestCellDNSPublicationAndLKG(t *testing.T) { testCellDNSPublication(t, "", false) }
func TestCellDNSPublicationAndLKGPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set disposable test database")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testCellDNSPublication(t, address, false)
	t.Run("authorized sources", func(t *testing.T) { testCellDNSPublication(t, address, false, true) })
	t.Run("authorized transition", func(t *testing.T) { testCellDNSPublication(t, address, true, true) })
	t.Run("retained routing only", func(t *testing.T) { testCellDNSPublication(t, address, true, true, true) })
	t.Run("verified source renewal", func(t *testing.T) { testCellDNSPublication(t, address, true, true, true, true) })
	t.Run("advancing source", func(t *testing.T) { testCellDNSPublication(t, address, true, true, true, true, true) })
}

func seedCellDNSRoutePublications(t *testing.T, s *Store, req *platformconfig.CompileRequest) {
	t.Helper()
	storeArtifacts := func(artifacts ...*model.PlatformArtifact) {
		for _, a := range artifacts {
			stored, _, err := s.EnsurePlatformArtifact(*a)
			if err != nil {
				t.Fatal(err)
			}
			*a = stored
		}
	}
	for i := range req.CellRoutePublications {
		p := &req.CellRoutePublications[i]
		storeArtifacts(&p.Parent, &p.Route, &p.TLS)
		seedDNSRouteRelease(t, s, celldns.Publication(*p))
	}
	if p := req.PreviousTrafficPublication; p != nil {
		storeArtifacts(&p.Parent, &p.Route, &p.TLS, &p.DNS)
		seedDNSRouteRelease(t, s, celldns.PreviousPublication(*p))
	}
}

func seedDNSRouteRelease(t *testing.T, s *Store, r model.PlatformArtifactRelease) {
	t.Helper()
	r.Version = 1
	r.CreatedAt, r.UpdatedAt = r.ReleasedAt, r.ReleasedAt
	r.OverrideMode = model.PlatformArtifactOverrideModeNone
	if s.db == nil {
		if err := s.withLockedState(true, func(state *model.State) error {
			state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, r)
			state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{LaneKey: r.LaneKey, ArtifactKind: r.ArtifactKind, ScopeKey: r.ScopeKey, ReleaseChannel: r.ReleaseChannel, FencingToken: r.FencingToken, Version: 1, ActiveReleaseID: r.ID, UpdatedAt: r.ReleasedAt})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := pgNextPlatformReleaseLane(ctx, tx, r.ArtifactKind, r.ScopeKey, r.ReleaseChannel, r.ReleasedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pgInsertPlatformArtifactRelease(ctx, tx, r); err != nil {
		t.Fatal(err)
	}
	if err := pgSetPlatformReleaseLaneActive(ctx, tx, r.LaneKey, r.ID, r.FencingToken, r.ReleasedAt); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestCellDNSPublicationWithSourceAuthorization(t *testing.T) {
	t.Run("neutral", func(t *testing.T) { testCellDNSPublication(t, "", false, true) })
	t.Run("transition", func(t *testing.T) { testCellDNSPublication(t, "", true, true) })
	t.Run("retained routing only", func(t *testing.T) { testCellDNSPublication(t, "", true, true, true) })
	t.Run("verified source renewal", func(t *testing.T) { testCellDNSPublication(t, "", true, true, true, true) })
	t.Run("advancing source", func(t *testing.T) { testCellDNSPublication(t, "", true, true, true, true, true) })
}

func testCellDNSPublication(t *testing.T, address string, transition bool, sources ...bool) {
	s := New(t.TempDir()+"/state.json", address)
	if address != "" {
		db, isolated, _ := hotInventoryTestDatabase(t)
		s = &Store{db: db, databaseURL: isolated}
	}
	s.ConfigurePlatformArtifactSigning(celldns.Keys())
	if s.db == nil {
		if err := s.Init(); err != nil {
			t.Fatal(err)
		}
	}
	req := celldns.Request(t)
	if transition {
		req = celldns.TransitionRequest(t)
	}
	withSources := len(sources) > 0 && sources[0]
	retained := len(sources) > 1 && sources[1]
	var policies []model.PlatformArtifact
	var releases []model.PlatformArtifactRelease
	if s.db != nil {
		var inputs []platformconfig.CompileResult
		var originals []model.PlatformArtifact
		req, policies, releases, inputs, originals = celldns.CompletePublication(t, transition, withSources)
		for _, c := range inputs {
			prepareCompiledInputPin(t, s, c)
			if _, _, err := s.EnsurePlatformArtifact(c.PolicyArtifact); err != nil {
				t.Fatal("persist exact scoped publication policy", err)
			}
		}
		for _, a := range originals {
			if _, _, err := s.EnsurePlatformArtifact(a); err != nil {
				t.Fatal("persist exact producer source", err)
			}
		}
	}
	if withSources {
		if s.db == nil {
			req, policies, releases = celldns.SourceAuthorizedRequest(t, transition)
		}
		for _, policy := range policies {
			if _, _, err := s.EnsurePlatformArtifact(policy); err != nil {
				t.Fatal(err)
			}
		}
		for _, release := range releases {
			seedDNSRouteRelease(t, s, release)
		}
	}
	if retained {
		celldns.RetainGlobalOnly(&req)
	}
	seedCellDNSRoutePublications(t, s, &req)
	compiled, err := platformconfig.Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	prepareCompiledInputPin(t, s, compiled)
	if err := s.ValidateDNSPublicationReferences(compiled.DNSArtifact); err != nil {
		t.Fatal("coherent compilation reference snapshot rejected", err)
	}
	if s.db != nil && retained {
		testDNSReferencesIndependentOfSourceHeartbeats(t, s, compiled.DNSArtifact)
	}

	save := func(a model.PlatformArtifact) model.PlatformArtifact {
		t.Helper()
		a, err := s.CreatePlatformArtifact(a)
		if err != nil {
			t.Fatal(err)
		}
		a, err = s.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "compiler", Pass: true}})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	_ = save(compiled.PolicyArtifact)
	child := save(compiled.DNSArtifact)
	parent := save(platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, []string{child.ID}, time.Now().UTC()))
	_, shadow, _, _, err := s.ReleasePlatformArtifact(parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	report := func(release model.PlatformArtifactRelease, seq int64, caps []string, positive bool) {
		t.Helper()
		now := time.Now().UTC()
		topology, _, err := platformcontrol.DeclaredTrafficConsumerTopology(parent)
		if err != nil {
			t.Fatal(err)
		}
		set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: parent.ID, ArtifactReleaseID: release.ID, ArtifactKind: child.ArtifactKind, Scope: parent.Scope, ScopeKey: parent.ScopeKey, Generation: child.Generation, Revision: seq, PreparedAt: now, Topology: topology})
		if err != nil {
			t.Fatal(err)
		}
		set, err = s.CreatePlatformExpectedConsumerSet(set)
		if err != nil {
			t.Fatal(err)
		}
		if set.RequiredCardinality != 1 || len(set.Consumers) != 1 || set.Consumers[0].Component != model.PlatformConsumerComponentDNSServer {
			t.Fatal("DNS enrolled routing receipts")
		}
		member := set.Consumers[0]
		claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "kubernetes:test-system:dns-account:pod-a", Component: member.Component, NodeID: member.NodeID, AuthorityID: member.AuthorityID, ScopeKey: parent.ScopeKey, ArtifactKinds: []string{child.ArtifactKind}}
		keys := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "identity", Keys: map[string]string{"identity": "synthetic-component-identity"}}
		token, err := platformcontrol.IssuePlatformComponentIdentity(keys, claims, now.Add(-time.Second), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		claims, err = platformcontrol.ParsePlatformComponentIdentity(keys, token, now)
		if err != nil {
			t.Fatal(err)
		}
		h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: member.ConsumerID, Component: member.Component, NodeID: member.NodeID, ArtifactKind: child.ArtifactKind, ScopeKey: parent.ScopeKey, ReleaseSetID: parent.ID, ExpectedConsumerSetID: set.ID, FencingToken: release.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: seq, IssuedAt: now, Nonce: model.NewID("nonce"), GenerationSequence: child.GenerationSequence, DesiredGeneration: child.Generation, CandidateGeneration: child.Generation, CompatibilityCapabilities: caps, ApplyStatus: "staged", ProbeStatus: "shadow_validated"}
		if positive {
			h.ApplyStatus, h.ProbeStatus, h.ActualGeneration, h.LKGGeneration = "applied", "passed", child.Generation, child.Generation
		}
		h.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
		if _, err = s.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, h, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
			// Preserve the replay inputs when this multi-stage fixture fails;
			// do not turn an unexplained clock/sequence failure into a retry.
			previous, readErr := s.ListPlatformConsumers(child.ArtifactKind, parent.ScopeKey)
			for _, consumer := range previous {
				if consumer.ConsumerID == h.ConsumerID {
					t.Logf("heartbeat rejected: sequence=%d previous=%d issued=%s previous_issued=%v nonce_reused=%t read_error=%v", h.Sequence, consumer.Sequence, h.IssuedAt.Format(time.RFC3339Nano), consumer.IssuedAt, h.Nonce == consumer.Nonce, readErr)
				}
			}
			t.Fatal(err)
		}
	}
	legacyCaps := []string{platformcontrol.TrafficReleaseCapabilityV1}
	fullCaps := []string{platformcontrol.TrafficReleaseCapabilityV1, platformcontrol.CellDNSCapabilityV1, platformcontrol.PhysicalOrderCapabilityV1}
	if transition {
		legacyCaps = append([]string(nil), fullCaps...)
		fullCaps = append(fullCaps, platformcontrol.DNSAuthorityTransitionCapabilityV1)
	}
	if withSources {
		legacyCaps = append([]string(nil), fullCaps...)
		fullCaps = append(fullCaps, platformcontrol.DNSRouteSourcesCapabilityV1)
	}
	if retained {
		legacyCaps = append([]string(nil), fullCaps...)
		fullCaps = append(fullCaps, platformcontrol.DNSRetainedRoutesCapabilityV1)
	}
	report(shadow, 1, legacyCaps, false)
	grayRequest := model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=complete"}
	if _, _, _, _, err = s.ReleasePlatformArtifact(parent.ID, grayRequest, testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
		t.Fatal("DNS without explicit capability admitted", err)
	}
	report(shadow, 2, fullCaps, false)
	if s.db != nil {
		// A competing routing publication cannot deadlock this DNS transaction.
		tx, err := s.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		scope := "global"
		if !transition {
			scope = req.CellRoutePublications[0].Parent.ScopeKey
		}
		if err = pgLockPromotionScope(context.Background(), tx, scope, true); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		started := time.Now()
		if err := s.ValidateDNSPublicationReferences(compiled.DNSArtifact); !errors.Is(err, ErrConflict) || time.Since(started) > 2*time.Second {
			t.Fatal("compilation waited on changing authority", err)
		}
		started = time.Now()
		_, _, _, _, err = s.ReleasePlatformArtifact(parent.ID, grayRequest, testPlatformPrincipal())
		tx.Rollback()
		if !errors.Is(err, ErrConflict) || time.Since(started) > 2*time.Second {
			t.Fatal("DNS did not fail promptly during a Cell mutation", err)
		}
		if withSources {
			producerScope := "platform-config-producer"
			if !retained {
				producerScope += ":" + req.CellRoutePublications[0].Reference.AuthorityCellID
			}
			tx, err := s.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := pgLockPromotionScope(context.Background(), tx, producerScope, true); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			started := time.Now()
			err = s.ValidateDNSPublicationReferences(compiled.DNSArtifact)
			tx.Rollback()
			if !errors.Is(err, ErrConflict) || time.Since(started) > 2*time.Second {
				t.Fatal("DNS did not fail promptly during a producer policy mutation", err)
			}
		}
	}
	_, gray, _, _, err := s.ReleasePlatformArtifact(parent.ID, grayRequest, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	report(gray, 3, fullCaps, true)
	if len(sources) > 2 && sources[2] {
		renewed := celldns.PreviousPublication(*req.PreviousTrafficPublication)
		originalID := renewed.ID
		renewed.ID += "-renewed"
		renewed.FencingToken++
		renewed.ReleasedAt = renewed.ReleasedAt.Add(time.Minute)
		renewed.VerificationState = model.PlatformArtifactVerificationStateServingUnverified
		renewed.VerifiedLKGGeneration = ""
		if len(sources) > 3 && sources[3] {
			artifacts, advanced := advancedDNSRouteSource(t, *req.PreviousTrafficPublication)
			for _, a := range artifacts {
				if _, _, err := s.EnsurePlatformArtifact(a); err != nil {
					t.Fatal(err)
				}
			}
			renewed = advanced
		}
		if s.db != nil {
			if _, err := s.db.Exec("UPDATE fugue_platform_artifact_releases SET status=$1 WHERE id=$2", model.PlatformArtifactReleaseStatusSuperseded, originalID); err != nil {
				t.Fatal(err)
			}
			seedDNSRouteRelease(t, s, renewed)
		} else {
			if err := s.withLockedState(true, func(state *model.State) error {
				index := platformArtifactReleaseIndex(state.PlatformArtifactReleases, originalID)
				state.PlatformArtifactReleases[index].Status = model.PlatformArtifactReleaseStatusSuperseded
				state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, renewed)
				for i := range state.PlatformReleaseLanes {
					lane := &state.PlatformReleaseLanes[i]
					if lane.LaneKey == renewed.LaneKey {
						lane.ActiveReleaseID = renewed.ID
						lane.FencingToken = renewed.FencingToken
						lane.Version++
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if s.db != nil && len(sources) > 3 && sources[3] {
		if err := s.ValidateDNSPublicationReferences(child); err != nil {
			t.Fatal("current source read set was not loaded", err)
		}
		originalID := req.PreviousTrafficPublication.Reference.ReleaseID
		for _, id := range []string{originalID, originalID + "-next"} {
			restore := model.PlatformArtifactVerificationStateVerified
			if id != originalID {
				restore = model.PlatformArtifactVerificationStateServingUnverified
			}
			if _, err := s.db.Exec("UPDATE fugue_platform_artifact_releases SET verification_state=$1 WHERE id=$2", model.PlatformArtifactVerificationStateFailed, id); err != nil {
				t.Fatal(err)
			}
			rejected := s.ValidateDNSPublicationReferences(child)
			if _, err := s.db.Exec("UPDATE fugue_platform_artifact_releases SET verification_state=$1 WHERE id=$2", restore, id); err != nil {
				t.Fatal(err)
			}
			if !errors.Is(rejected, ErrConflict) {
				t.Fatal("PostgreSQL accepted failed historical or current authority", id, rejected)
			}
		}
	}

	if len(sources) > 2 && sources[2] {
		report(gray, 4, fullCaps, false)
		if _, _, _, _, err = s.VerifyPlatformArtifactReleaseLKG(gray.ID, completePlatformVerificationRequest(gray.FencingToken, true), testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
			t.Fatal("historical route verification bypassed current DNS failure", err)
		}
		report(gray, 5, fullCaps, true)
	}
	if _, _, _, _, err = s.VerifyPlatformArtifactReleaseLKG(gray.ID, completePlatformVerificationRequest(gray.FencingToken, true), testPlatformPrincipal()); err != nil {
		t.Fatal(err)
	}
	_, full, _, _, err := s.ReleasePlatformArtifact(parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "full"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err = s.VerifyPlatformArtifactReleaseLKG(full.ID, completePlatformVerificationRequest(full.FencingToken, false), testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
		t.Fatal("gray facts verified full LKG", err)
	}
	report(full, 6, fullCaps, true)
	if _, _, _, _, err = s.VerifyPlatformArtifactReleaseLKG(full.ID, completePlatformVerificationRequest(full.FencingToken, false), testPlatformPrincipal()); err != nil {
		t.Fatal(err)
	}
	for _, kind := range trafficLKGKinds() {
		lkg, err := s.GetPlatformLKG(kind, parent.ScopeKey)
		if err != nil {
			t.Fatal(err)
		}
		if kind == model.PlatformArtifactKindEdgeRouteBundle || kind == model.PlatformArtifactKindCaddyRouteConfig {
			if lkg != nil {
				t.Fatal("DNS wrote a route/TLS LKG")
			}
		} else if lkg == nil || lkg.VerifiedByReleaseID != full.ID {
			t.Fatal("DNS recovery members incomplete", kind)
		}
	}
	if withSources {
		snapshot, err := s.ObserveDNSRouteSources(context.Background(), child)
		if err != nil || len(snapshot.Scopes) != len(req.Intent.DNSRouteSources) || snapshot.SelectionDigest == "" {
			t.Fatal("coherent source observation failed", err)
		}
		for _, scope := range snapshot.Scopes {
			if len(scope.Publications) != 1 || scope.Publications[0].Selections[0] != "full" {
				t.Fatal("source observation lost selected full publication")
			}
		}
		if s.db != nil {
			tx, err := s.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			parentID := req.PreviousTrafficPublication
			lockedID := ""
			if retained {
				lockedID = parentID.Parent.ID
			} else {
				lockedID = req.CellRoutePublications[0].Parent.ID
			}
			if _, err := pgGetPlatformArtifactForUpdate(context.Background(), tx, lockedID, true); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			started := time.Now()
			_, err = s.ObserveDNSRouteSources(context.Background(), child)
			tx.Rollback()
			if err != nil || time.Since(started) > 2*time.Second {
				t.Fatal("read-only source observation waited on a publication row lock", err)
			}
		}
	}
	testDNSPublicationSuccessorGuard(t, s, req, parent, shadow, gray, full)
	// Complete the winning successor through the same real capability,
	// convergence and LKG gates. The guard must not only reject unsafe changes.
	parent, shadow, _, err = s.GetActivePlatformArtifact(parent.ArtifactKind, parent.ScopeKey, "shadow")
	if err != nil {
		t.Fatal(err)
	}
	child, err = s.GetPlatformArtifact(parent.Content["artifact_ids"].([]any)[0].(string))
	if err != nil {
		t.Fatal(err)
	}
	report(shadow, 17, fullCaps, false)
	guarded := func(channel string) model.PlatformArtifactReleaseRequest {
		t.Helper()
		lkg, err := s.GetPlatformLKG(parent.ArtifactKind, parent.ScopeKey)
		if err != nil || lkg == nil {
			t.Fatal("missing guarded promotion baseline", err)
		}
		guard := &model.PlatformDNSPublicationPrecondition{LKGID: lkg.ID, VerificationEvidenceHash: lkg.VerificationEvidenceHash, Lanes: map[string]*model.PlatformPublicationPrecondition{}}
		for _, lane := range []string{"shadow", "gray", "full"} {
			a, r, found, err := s.GetActivePlatformArtifact(parent.ArtifactKind, parent.ScopeKey, lane)
			if err != nil || !found {
				t.Fatal("missing exact DNS lane", err)
			}
			ref := &model.PlatformPublicationPrecondition{ArtifactID: a.ID, ContentHash: a.ContentHash, ReleaseID: r.ID, FencingToken: r.FencingToken}
			guard.Lanes[lane] = ref
			if r.ID == lkg.VerifiedByReleaseID {
				guard.LKGPublication = *ref
			}
		}
		r := model.PlatformArtifactReleaseRequest{ReleaseChannel: channel, DNSPublicationPrecondition: guard}
		if channel == "gray" {
			r.CanaryRuleRef = "cohort=complete"
		}
		r.IdempotencyKey, err = dnsPublicationKey(parent, r)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	_, gray, _, _, err = s.ReleasePlatformArtifact(parent.ID, guarded("gray"), testPlatformPrincipal())
	if err != nil {
		t.Fatal("guarded gray rejected despite valid capabilities", err)
	}
	report(gray, 18, fullCaps, true)
	if _, _, _, _, err = s.VerifyPlatformArtifactReleaseLKG(gray.ID, completePlatformVerificationRequest(gray.FencingToken, false), testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
		t.Fatal("successor gray replaced existing positive LKG", err)
	}
	_, full, _, _, err = s.ReleasePlatformArtifact(parent.ID, guarded("full"), testPlatformPrincipal())
	if err != nil {
		t.Fatal("guarded full rejected despite observed gray", err)
	}
	previousLKG, err := s.GetPlatformLKG(parent.ArtifactKind, parent.ScopeKey)
	if err != nil || previousLKG == nil || previousLKG.ArtifactID == parent.ID {
		t.Fatal("publishing full replaced LKG before successor verification", err)
	}
	report(full, 19, fullCaps, false)
	if _, _, _, _, err = s.VerifyPlatformArtifactReleaseLKG(full.ID, completePlatformVerificationRequest(full.FencingToken, false), testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
		t.Fatal("negative full observations replaced positive LKG", err)
	}
	stillRetained, err := s.GetPlatformLKG(parent.ArtifactKind, parent.ScopeKey)
	if err != nil || stillRetained == nil || stillRetained.ID != previousLKG.ID {
		t.Fatal("failed verification lost previous positive LKG", err)
	}
	report(full, 20, fullCaps, true)
	if _, _, _, _, err = s.VerifyPlatformArtifactReleaseLKG(full.ID, completePlatformVerificationRequest(full.FencingToken, false), testPlatformPrincipal()); err != nil {
		t.Fatal("observed successor full LKG failed", err)
	}
	if withSources && retained {
		if s.db != nil && len(sources) == 2 {
			testDNSPublicationAdmissionRefusesLongContention(t, s, parent)
		}
		testDNSPositiveRecoveryAfterSourceRevocation(t, s, parent)
	}
}

func testDNSPublicationAdmissionRefusesLongContention(t *testing.T, s *Store, parent model.PlatformArtifact) {
	t.Helper()
	before, err := s.GetPlatformLKG(parent.ArtifactKind, parent.ScopeKey)
	if err != nil || before == nil {
		t.Fatal("verified baseline missing", err)
	}
	// Use a neutral policy in the same scope so the delay cannot alter the DNS
	// publication or borrow its existing LKG as a new positive checkpoint.
	successor, err := s.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy, Scope: parent.Scope, Generation: "bounded-budget-successor", Content: map[string]any{"weights": map[string]any{"latency": 1}}})
	if err != nil {
		t.Fatal(err)
	}
	successor, err = s.ValidatePlatformArtifact(successor.ID, []model.PlatformArtifactValidationResult{{Name: "compiler", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if err := pgLockPromotionScope(context.Background(), blocker, parent.ScopeKey, true); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	started := time.Now()
	go func() {
		_, _, _, _, err := s.ReleasePlatformArtifact(successor.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: "bounded-publication-delay"}, testPlatformPrincipal())
		done <- err
	}()
	// Input admission adds a strict 250ms lock bound. A queued publication
	// must refuse without changing its lane; a later explicit request prepares
	// a new proof instead of continuing the old transaction or retrying itself.
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("publication bypassed the held scope")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("publication exceeded bounded admission lock wait")
	}
	if _, _, found, err := s.GetActivePlatformArtifact(successor.ArtifactKind, successor.ScopeKey, "shadow"); err != nil || found {
		t.Fatal("refused publication changed lane", err)
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := s.ReleasePlatformArtifact(successor.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: "bounded-publication-delay"}, testPlatformPrincipal()); err != nil {
		t.Fatal("fresh publication failed after contention cleared", err)
	}
	after, err := s.GetPlatformLKG(parent.ArtifactKind, parent.ScopeKey)
	if err != nil || after == nil || after.ID != before.ID || after.VerifiedByReleaseID != before.VerifiedByReleaseID || !after.ExpiresAt.Equal(before.ExpiresAt) {
		t.Fatal("publication wait renewed or replaced verified LKG", err)
	}
	t.Logf("bounded refusal and fresh publication completed in %s", time.Since(started))
}

// Source consumer state does not authorize a DNS routing reference. Exercise
// the real loader while those unrelated tables are locked by another writer;
// the old full snapshot incorrectly waited on them until its deadline.
func testDNSReferencesIndependentOfSourceHeartbeats(t *testing.T, s *Store, child model.PlatformArtifact) {
	t.Helper()
	blocker, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.Exec(`LOCK TABLE fugue_platform_consumer_instances, fugue_platform_expected_consumer_sets IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	state := &model.State{PlatformArtifacts: []model.PlatformArtifact{child}}
	parent := model.PlatformArtifact{ScopeKey: child.ScopeKey, Content: map[string]any{"publication_role": platformconfig.PublicationRoleCellDNS}}
	started := time.Now()
	if err := s.pgLoadCellDNSReferences(ctx, tx, parent, state); err != nil {
		t.Fatal("DNS reference read waited on unrelated source consumers", err)
	}
	if len(state.ExpectedConsumerSets) != 0 || len(state.PlatformConsumerInstances) != 0 || len(state.PlatformShadowConsumers) != 0 {
		t.Fatal("DNS routing dependency loaded unrelated source consumer state")
	}
	if err := validateCellDNSReferencesInState(state, child, celldns.Keys()); err != nil {
		t.Fatal("narrow routing snapshot lost source authorization", err)
	}
	t.Logf("DNS reference authorization completed with source consumer tables exclusively locked: %s", time.Since(started))
}

func TestCellDNSAdmissionRejectsChangedReferencedAuthority(t *testing.T) {
	for _, scenario := range []string{"fence", "new full", "new gray", "frozen", "missing TLS"} {
		t.Run(scenario, func(t *testing.T) {
			r := celldns.Request(t)
			c := celldns.Compile(t, r)
			state := &model.State{}
			for _, p := range r.CellRoutePublications {
				state.PlatformArtifacts = append(state.PlatformArtifacts, p.Parent, p.Route, p.TLS)
				state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, celldns.Publication(p))
				state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{LaneKey: celldns.Publication(p).LaneKey, ActiveReleaseID: celldns.Publication(p).ID, FencingToken: celldns.Publication(p).FencingToken})
			}
			if err := validateCellDNSReferencesInState(state, c.DNSArtifact, celldns.Keys()); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "fence":
				state.PlatformReleaseLanes[0].FencingToken++
			case "frozen":
				state.PlatformReleaseLanes[0].Frozen = true
			case "missing TLS":
				state.PlatformArtifacts = append(state.PlatformArtifacts[:2], state.PlatformArtifacts[3:]...)
			case "new full":
				state.PlatformArtifactReleases[0].ID = "successor"
				state.PlatformReleaseLanes[0].ActiveReleaseID = "successor"
			case "new gray":
				v := state.PlatformArtifactReleases[0]
				v.ID = "successor-gray"
				v.ReleaseChannel = "gray"
				v.CanaryRuleRef = "cohort=complete"
				v.LaneKey = "gray-lane"
				v.ReleasedAt = v.ReleasedAt.Add(time.Second)
				state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, v)
				state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{LaneKey: v.LaneKey, ActiveReleaseID: v.ID, FencingToken: v.FencingToken})
			}
			err := validateCellDNSReferencesInState(state, c.DNSArtifact, celldns.Keys())
			if !errors.Is(err, ErrConflict) {
				t.Fatal("stale dependency admitted")
			}
			if PlatformReleaseFailureStage(err) != "routing_source_selection" {
				t.Fatal("source failure lost diagnostic stage", err)
			}
		})
	}
}

func TestDNSAuthorityTransitionPublicationAndLKG(t *testing.T) { testCellDNSPublication(t, "", true) }
func TestDNSAuthorityTransitionPublicationAndLKGPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set disposable test database")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testCellDNSPublication(t, address, true)
}
