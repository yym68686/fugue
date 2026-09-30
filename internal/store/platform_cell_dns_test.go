package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/schemamigrate"
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

func testCellDNSPublication(t *testing.T, address string, transition bool) {
	s := New(t.TempDir()+"/state.json", address)
	if address != "" {
		base, err := sql.Open("pgx", address)
		if err != nil {
			t.Fatal(err)
		}
		schema := fmt.Sprintf("cell_dns_%d", time.Now().UnixNano())
		if _, err := base.Exec("CREATE SCHEMA " + schema); err != nil {
			base.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := base.Exec("DROP SCHEMA " + schema + " CASCADE"); err != nil {
				t.Error(err)
			}
			base.Close()
		})
		address = postgresIntegrationURLWithSearchPath(t, address, schema)
		db, err := sql.Open("pgx", address)
		if err != nil {
			t.Fatal(err)
		}
		s = &Store{db: db, databaseURL: address}
		ctx := context.Background()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.applyPostgresSchemaTx(ctx, tx); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		for _, migrate := range []func(context.Context, string) error{schemamigrate.MigratePlatformState, schemamigrate.MigrateImageCacheManifestGraph, schemamigrate.MigrateEdgeInstanceFencing, schemamigrate.MigrateSourceUploadSessions} {
			if err := migrate(ctx, address); err != nil {
				t.Fatal(err)
			}
		}
	}
	s.ConfigurePlatformArtifactSigning(celldns.Keys())
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	if s.db != nil {
		t.Cleanup(func() { s.db.Close() })
	}
	req := celldns.Request(t)
	if transition {
		req = celldns.TransitionRequest(t)
	}
	seedCellDNSRoutePublications(t, s, &req)
	compiled, err := platformconfig.Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ValidateDNSPublicationReferences(compiled.DNSArtifact); err != nil {
		t.Fatal("coherent compilation reference snapshot rejected", err)
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
			t.Fatal(err)
		}
	}
	legacyCaps := []string{platformcontrol.TrafficReleaseCapabilityV1}
	fullCaps := []string{platformcontrol.TrafficReleaseCapabilityV1, platformcontrol.CellDNSCapabilityV1}
	if transition {
		legacyCaps = append([]string(nil), fullCaps...)
		fullCaps = append(fullCaps, platformcontrol.DNSAuthorityTransitionCapabilityV1)
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
		scope := req.CellRoutePublications[0].Parent.ScopeKey
		if transition {
			scope = "global"
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
	}
	_, gray, _, _, err := s.ReleasePlatformArtifact(parent.ID, grayRequest, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	report(gray, 3, fullCaps, true)
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
	report(full, 4, fullCaps, true)
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
			if !errors.Is(validateCellDNSReferencesInState(state, c.DNSArtifact, celldns.Keys()), ErrConflict) {
				t.Fatal("stale dependency admitted")
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
