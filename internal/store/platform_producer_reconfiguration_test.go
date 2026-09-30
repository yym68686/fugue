package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/schemamigrate"
	"fugue/internal/testfixture/celldns"
)

type reconfigurationFixture struct {
	s         *Store
	old, next model.PlatformArtifact
	policy    platformproducer.Policy
	request   model.PlatformArtifactReleaseRequest
	lkg       model.PlatformLKGSnapshot
}

func newReconfigurationFixture(t *testing.T, address string) reconfigurationFixture {
	t.Helper()
	if address != "" {
		base, err := sql.Open("pgx", address)
		if err != nil {
			t.Fatal(err)
		}
		schema := model.NewID("reconfiguration")
		quoted := quotePostgresIntegrationIdentifier(t, schema)
		if _, err = base.Exec("CREATE SCHEMA " + quoted); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := base.Exec("DROP SCHEMA " + quoted + " CASCADE"); err != nil {
				t.Error(err)
			}
			base.Close()
		})
		address = postgresIntegrationURLWithSearchPath(t, address, schema)
	}
	s := New(t.TempDir()+"/state.json", address)
	if address != "" {
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
		if _, err = s.applyPostgresSchemaTx(ctx, tx); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		for _, migrate := range []func(context.Context, string) error{schemamigrate.MigratePlatformState, schemamigrate.MigrateImageCacheManifestGraph, schemamigrate.MigrateEdgeInstanceFencing, schemamigrate.MigrateSourceUploadSessions} {
			if err = migrate(ctx, address); err != nil {
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
	compiled := celldns.Request(t)
	seedCellDNSRoutePublications(t, s, &compiled)
	pub := compiled.CellRoutePublications[0]
	full := celldns.Publication(pub)
	parent := pub.Parent
	now := time.Now().UTC()
	// Seed a cryptographically verified recovery checkpoint. These tests
	// isolate configuration CAS, not the independently tested probe verifier.
	lkg, err := platformsafety.SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{ID: "lkg-route", ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind, Scope: parent.Scope, ScopeKey: parent.ScopeKey, SchemaVersion: parent.SchemaVersion, Generation: parent.Generation, GenerationSequence: parent.GenerationSequence, ContentHash: parent.ContentHash, ArtifactProvenance: parent.Provenance, VerifiedByReleaseID: full.ID, VerificationEvidenceHash: "sha256:" + strings.Repeat("a", 64), ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}, celldns.Keys())
	if err != nil {
		t.Fatal(err)
	}
	if s.db != nil {
		_, err = pgUpsertPlatformLKGSnapshot(context.Background(), s.db, lkg)
	} else {
		err = s.withLockedState(true, func(st *model.State) error {
			st.PlatformLKGSnapshots = append(st.PlatformLKGSnapshots, lkg)
			return nil
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "producer-old", Mode: "shadow", PublicationRole: platformconfig.PublicationRoleCellRoutes, AuthorityCellID: "cell-a", InputSource: "business-static-intent", TargetScope: parent.ScopeKey, IntervalSeconds: 30, RefreshSeconds: 120, RequireApplicationDomains: true, RequireRouteDefaults: true, StaticIntentArtifactID: "static", StaticIntentDigest: "sha256:" + strings.Repeat("b", 64), DNSPolicyArtifactID: "projection", DNSPolicyDigest: "sha256:" + strings.Repeat("c", 64)}
	f := reconfigurationFixture{s: s, policy: p, lkg: lkg}
	f.old = f.save(t, p)
	_, previous, _, _, err := s.ReleasePlatformArtifact(f.old.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	next := compiled.Intent.EdgeTopology.Clone()
	before := next.Clone()
	for i := range before.Cells {
		before.Cells[i].LegacyGroupID = "edge-group-old-" + strings.TrimPrefix(before.Cells[i].ID, "cell-")
	}
	rule := platformconfig.RoutePolicyConstraint{ID: "constraint", Hostname: "app.example.test", TenantID: "tenant-a", AppID: "app-a", EdgeGroupID: "edge-group-old-a", RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true}
	digest, err := platformproducer.RouteConstraintDigest(rule)
	if err != nil {
		t.Fatal(err)
	}
	f.policy.Generation = "producer-next"
	f.policy.RoutePlacementTransition = &platformproducer.RoutePlacementTransition{PreviousTopology: before, NextTopology: next, Constraints: []platformproducer.RouteConstraintTransition{{Source: rule, SourceDigest: digest}}}
	f.next = f.save(t, f.policy)
	ref := func(a model.PlatformArtifact, r model.PlatformArtifactRelease) model.PlatformPublicationPrecondition {
		return model.PlatformPublicationPrecondition{ArtifactID: a.ID, ContentHash: a.ContentHash, ReleaseID: r.ID, FencingToken: r.FencingToken}
	}
	f.request = model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", ProducerReconfiguration: &model.PlatformProducerReconfiguration{PreviousPolicy: ref(f.old, previous), ServingFull: ref(parent, full), VerificationEvidenceHash: lkg.VerificationEvidenceHash}, Reason: "exact source-bound configuration transition"}
	f.bindKey(t)
	return f
}

func (f reconfigurationFixture) save(t *testing.T, p platformproducer.Policy) model.PlatformArtifact {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]any
	if err = json.Unmarshal(raw, &content); err != nil {
		t.Fatal(err)
	}
	a, err := f.s.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "platform-config-producer:cell-a"}, Generation: p.Generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	a, err = f.s.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func (f *reconfigurationFixture) bindKey(t *testing.T) {
	t.Helper()
	key, err := producerReconfigurationKey(f.next, *f.request.ProducerReconfiguration)
	if err != nil {
		t.Fatal(err)
	}
	f.request.IdempotencyKey = key
}
func (f reconfigurationFixture) release() (model.PlatformArtifactRelease, error) {
	_, r, _, _, err := f.s.ReleasePlatformArtifact(f.next.ID, f.request, testPlatformPrincipal())
	return r, err
}

func (f reconfigurationFixture) freeze(t *testing.T, scope string) {
	t.Helper()
	kind, channel := model.PlatformArtifactKindReleaseSet, "full"
	if strings.HasPrefix(scope, "platform-config-producer:") {
		kind, channel = model.PlatformArtifactKindPolicySnapshot, "shadow"
	}
	key := platformsafety.ReleaseLaneKey(kind, scope, channel)
	var err error
	if f.s.db != nil {
		_, err = f.s.db.Exec(`UPDATE fugue_platform_release_lanes SET frozen=true WHERE lane_key=$1`, key)
	} else {
		err = f.s.withLockedState(true, func(s *model.State) error {
			for i := range s.PlatformReleaseLanes {
				if s.PlatformReleaseLanes[i].LaneKey == key {
					s.PlatformReleaseLanes[i].Frozen = true
				}
			}
			return nil
		})
	}
	if err != nil {
		t.Fatal(err)
	}
}

func testProducerReconfiguration(t *testing.T, address string) {
	for _, scenario := range []string{"success and replay", "stale policy", "full fence", "wrong evidence", "frozen producer", "frozen target", "expired LKG", "tampered LKG", "inputs changed", "schedule changed", "key mismatch", "canary", "override", "no initial write", "superseded retry", "concurrent successor"} {
		t.Run(scenario, func(t *testing.T) {
			f := newReconfigurationFixture(t, address)
			switch scenario {
			case "stale policy":
				f.request.ProducerReconfiguration.PreviousPolicy.FencingToken++
				f.bindKey(t)
			case "full fence":
				f.request.ProducerReconfiguration.ServingFull.FencingToken++
				f.bindKey(t)
			case "wrong evidence":
				f.request.ProducerReconfiguration.VerificationEvidenceHash = "sha256:" + strings.Repeat("d", 64)
				f.bindKey(t)
			case "frozen producer":
				f.freeze(t, f.old.ScopeKey)
			case "frozen target":
				f.freeze(t, f.policy.TargetScope)
			case "inputs changed", "schedule changed":
				p := f.policy
				p.Generation = "producer-invalid"
				if scenario == "inputs changed" {
					p.StaticIntentArtifactID = "other"
				} else {
					p.IntervalSeconds = 60
				}
				f.next = f.save(t, p)
				f.bindKey(t)
			case "key mismatch":
				f.request.IdempotencyKey = "unbound"
			case "canary":
				f.request.CanaryRuleRef = "cohort=other"
			case "override":
				f.request.SoftOverride = true
			case "no initial write":
				f.request.ProducerReconfiguration.PreviousPolicy.ReleaseID = "missing"
				f.bindKey(t)
			case "expired LKG", "tampered LKG":
				lkg := f.lkg
				if scenario == "expired LKG" {
					lkg.ExpiresAt = time.Now().Add(-time.Second)
					var err error
					lkg, err = platformsafety.SignPlatformLKGSnapshot(lkg, celldns.Keys())
					if err != nil {
						t.Fatal(err)
					}
				} else {
					lkg.SnapshotProvenance.Signature = "wrong"
				}
				var err error
				if f.s.db != nil {
					_, err = pgUpsertPlatformLKGSnapshot(context.Background(), f.s.db, lkg)
				} else {
					err = f.s.withLockedState(true, func(s *model.State) error {
						s.PlatformLKGSnapshots = upsertPlatformLKGSnapshot(s.PlatformLKGSnapshots, lkg)
						return nil
					})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			before, err := f.s.ListPlatformReleaseMessages(model.PlatformArtifactKindPolicySnapshot, f.old.ScopeKey, time.Time{}, 100)
			if err != nil {
				t.Fatal(err)
			}
			r, err := f.release()
			positive := scenario == "success and replay" || scenario == "superseded retry" || scenario == "concurrent successor"
			if (err == nil) != positive {
				t.Fatalf("release: %v", err)
			}
			if !positive {
				after, e := f.s.ListPlatformReleaseMessages(model.PlatformArtifactKindPolicySnapshot, f.old.ScopeKey, time.Time{}, 100)
				if e != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("failed precondition mutated release ledger", e)
				}
				return
			}
			if scenario == "success and replay" {
				replay, err := f.release()
				if err != nil || replay.ID != r.ID {
					t.Fatal("lost-response retry did not reuse exact release", err)
				}
				lkg, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, f.policy.TargetScope)
				if err != nil || lkg == nil || lkg.SnapshotProvenance != f.lkg.SnapshotProvenance {
					t.Fatal("configuration changed LKG", err)
				}
			}
			if scenario == "superseded retry" {
				p := f.policy
				p.Generation = "producer-later"
				later := f.save(t, p)
				if _, _, _, _, err := f.s.ReleasePlatformArtifact(later.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal()); err != nil {
					t.Fatal(err)
				}
				if _, err := f.release(); !errors.Is(err, ErrConflict) {
					t.Fatal("superseded success replay accepted", err)
				}
			}
			if scenario == "concurrent successor" {
				// Replaying the committed request races a distinct successor of
				// the same original predecessor. Only the original can succeed.
				other := f
				p := f.policy
				p.Generation = "producer-competitor"
				other.next = f.save(t, p)
				other.bindKey(t)
				var wg sync.WaitGroup
				results := make(chan error, 2)
				for _, candidate := range []reconfigurationFixture{f, other} {
					wg.Add(1)
					go func(c reconfigurationFixture) { defer wg.Done(); _, err := c.release(); results <- err }(candidate)
				}
				wg.Wait()
				close(results)
				success, conflicts := 0, 0
				for err := range results {
					if err == nil {
						success++
					} else if errors.Is(err, ErrConflict) {
						conflicts++
					} else {
						t.Fatal(err)
					}
				}
				if success != 1 || conflicts != 1 {
					t.Fatal("competing predecessor CAS admitted two successors")
				}
			}
		})
	}
}

func TestProducerReconfiguration(t *testing.T) { testProducerReconfiguration(t, "") }
func TestProducerReconfigurationPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testProducerReconfiguration(t, address)
}

func TestProducerReconfigurationQueuedPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	f := newReconfigurationFixture(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	tx, err := f.s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := pgLockPromotionScope(ctx, tx, f.policy.TargetScope, true); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := f.release(); done <- err }()
	deadline := time.Now().Add(4 * time.Second)
	for {
		var waiting int
		if err := f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND wait_event='advisory'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("release did not wait for target scope before reading preconditions")
		}
		time.Sleep(10 * time.Millisecond)
	}
	key := platformsafety.ReleaseLaneKey(model.PlatformArtifactKindReleaseSet, f.policy.TargetScope, "full")
	if _, err := tx.ExecContext(ctx, `UPDATE fugue_platform_release_lanes SET frozen=true WHERE lane_key=$1`, key); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrConflict) {
			t.Fatal("queued release ignored changed target baseline", err)
		}
	case <-ctx.Done():
		t.Fatal("queued release did not complete")
	}
	a, _, found, err := f.s.GetActivePlatformArtifact(f.old.ArtifactKind, f.old.ScopeKey, "shadow")
	if err != nil || !found || a.ID != f.old.ID {
		t.Fatal("queued stale request replaced previous policy", err)
	}
}

func testSimultaneousProducerReconfiguration(t *testing.T, address string) {
	f := newReconfigurationFixture(t, address)
	other := f
	policy := f.policy
	policy.Generation = "simultaneous-successor"
	other.next = f.save(t, policy)
	other.bindKey(t)
	start := make(chan struct{})
	done := make(chan error, 2)
	for _, candidate := range []reconfigurationFixture{f, other} {
		go func(c reconfigurationFixture) { <-start; _, err := c.release(); done <- err }(candidate)
	}
	close(start)
	success, conflict := 0, 0
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err == nil {
				success++
			} else if errors.Is(err, ErrConflict) {
				conflict++
			} else {
				t.Fatal(err)
			}
		case <-time.After(12 * time.Second):
			t.Fatal("competing publication did not complete")
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("success=%d conflicts=%d", success, conflict)
	}
	current, _, found, err := f.s.GetActivePlatformArtifact(f.old.ArtifactKind, f.old.ScopeKey, "shadow")
	if err != nil || !found || (current.ID != f.next.ID && current.ID != other.next.ID) {
		t.Fatal("winning successor unavailable", err)
	}
}

func TestProducerReconfigurationSimultaneous(t *testing.T) {
	testSimultaneousProducerReconfiguration(t, "")
}
func TestProducerReconfigurationSimultaneousPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testSimultaneousProducerReconfiguration(t, address)
}
