package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/schemamigrate"
)

func savePhysicalDNSTestArtifact(t *testing.T, state *Store, kind, scope, generation string, value any) model.PlatformArtifact {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]any
	if err := json.Unmarshal(raw, &content); err != nil {
		t.Fatal(err)
	}
	artifact, err := state.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: kind, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: scope}, Generation: generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	artifact, err = state.ValidatePlatformArtifact(artifact.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}

func physicalDNSReconfigurationFixture(t *testing.T, database string) reconfigurationFixture {
	return physicalDNSFixtureWithQuery(t, database, nil)
}

func physicalDNSFixtureWithQuery(t *testing.T, database string, edit func(map[string]any)) reconfigurationFixture {
	t.Helper()
	serving := newServingFixture(t, database)
	source, err := serving.s.GetPlatformArtifact(serving.policy.DNSPolicyArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	source.Content["generation"] = "dns-active"
	source.Content["dns_query_policy"].(map[string]any)["ranking_mode"] = "active"
	if edit != nil {
		edit(source.Content["dns_query_policy"].(map[string]any))
	}
	source = savePhysicalDNSTestArtifact(t, serving.s, source.ArtifactKind, source.ScopeKey, "dns-active", source.Content)
	serving.input.DNSQueryPolicy.RankingMode = "active"
	if edit != nil {
		raw, _ := json.Marshal(source.Content["dns_query_policy"])
		if err := json.Unmarshal(raw, serving.input.DNSQueryPolicy); err != nil {
			t.Fatal(err)
		}
	}
	serving.policy.Generation = "producer-active"
	serving.policy.DNSPolicyArtifactID, serving.policy.DNSPolicyDigest = source.ID, source.ContentHash
	old := savePhysicalDNSTestArtifact(t, serving.s, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, serving.policy.Generation, serving.policy)
	_, serving.authority, _, _, err = serving.s.ReleasePlatformArtifact(old.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	parent := serving.candidate(t)
	now := time.Now().UTC()
	full := model.PlatformArtifactRelease{ID: "physical-baseline-full", ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind, Scope: parent.Scope, ScopeKey: parent.ScopeKey,
		Generation: parent.Generation, ReleaseChannel: "full", Status: model.PlatformArtifactReleaseStatusActive, FencingToken: 1,
		LaneKey: platformsafety.ReleaseLaneKey(parent.ArtifactKind, parent.ScopeKey, "full"), ReleasedAt: now,
		ReleasedByType: model.ActorTypeBootstrap, ReleasedByID: platformproducer.Actor, VerificationState: model.PlatformArtifactVerificationStateVerified, VerifiedLKGGeneration: parent.Generation}
	lkg, err := platformsafety.SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{ID: "physical-baseline-lkg", ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind, Scope: parent.Scope, ScopeKey: parent.ScopeKey,
		SchemaVersion: parent.SchemaVersion, Generation: parent.Generation, GenerationSequence: parent.GenerationSequence, ContentHash: parent.ContentHash, ArtifactProvenance: parent.Provenance,
		VerifiedByReleaseID: full.ID, VerificationEvidenceHash: "sha256:" + strings.Repeat("a", 64), ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}, serving.s.platformArtifactSigningKeyring())
	if err != nil {
		t.Fatal(err)
	}
	{
		ctx := context.Background()
		tx, beginErr := serving.s.db.BeginTx(ctx, nil)
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		defer tx.Rollback()
		_, err = pgNextPlatformReleaseLane(ctx, tx, full.ArtifactKind, full.ScopeKey, "full", now)
		if err == nil {
			_, err = pgInsertPlatformArtifactRelease(ctx, tx, full)
		}
		if err == nil {
			err = pgSetPlatformReleaseLaneActive(ctx, tx, full.LaneKey, full.ID, full.FencingToken, now)
		}
		if err == nil {
			lkg, err = pgUpsertPlatformLKGSnapshot(ctx, tx, lkg)
		}
		if err == nil {
			err = tx.Commit()
		}
	}

	if err != nil {
		t.Fatal(err)
	}
	source.Content["generation"] = "dns-physical"
	source.Content["dns_query_policy"].(map[string]any)["physical_routes"] = []platformconfig.PhysicalQualityRoute{{Hostname: "app.example.test", TrafficClass: "streaming", Policy: edgequality.DefaultNetworkPolicy()}}
	nextSource := savePhysicalDNSTestArtifact(t, serving.s, source.ArtifactKind, source.ScopeKey, "dns-physical", source.Content)
	nextPolicy := serving.policy
	nextPolicy.Generation = "producer-physical"
	nextPolicy.DNSPolicyArtifactID, nextPolicy.DNSPolicyDigest = nextSource.ID, nextSource.ContentHash
	f := reconfigurationFixture{s: serving.s, old: old, policy: nextPolicy, lkg: lkg}
	f.next = savePhysicalDNSTestArtifact(t, serving.s, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, nextPolicy.Generation, nextPolicy)
	f.request = model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", ProducerReconfiguration: &model.PlatformProducerReconfiguration{Operation: "physical_dns",
		PreviousPolicy: model.PlatformPublicationPrecondition{ArtifactID: old.ID, ContentHash: old.ContentHash, ReleaseID: serving.authority.ID, FencingToken: serving.authority.FencingToken},
		ServingFull:    model.PlatformPublicationPrecondition{ArtifactID: parent.ID, ContentHash: parent.ContentHash, ReleaseID: full.ID, FencingToken: full.FencingToken}, VerificationEvidenceHash: lkg.VerificationEvidenceHash}}
	f.bindKey(t)
	return f
}

func disposablePhysicalDNSDatabase(t *testing.T) string {
	t.Helper()
	database := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if database == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	parsed, err := url.Parse(database)
	if err != nil || parsed.Hostname() != "127.0.0.1" || !strings.Contains(parsed.Path, "fugue_test") {
		t.Fatal("requires disposable loopback PostgreSQL")
	}
	base, err := sql.Open("pgx", database)
	if err != nil {
		t.Fatal(err)
	}
	schema := model.NewID("physical_transition")
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
	database = postgresIntegrationURLWithSearchPath(t, database, schema)
	bootstrap, err := sql.Open("pgx", database)
	if err != nil {
		t.Fatal(err)
	}
	defer bootstrap.Close()
	ctx := context.Background()
	tx, err := bootstrap.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	state := &Store{db: bootstrap, databaseURL: database}
	if _, err = state.applyPostgresSchemaTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, migrate := range []func(context.Context, string) error{schemamigrate.MigratePlatformState, schemamigrate.MigrateImageCacheManifestGraph, schemamigrate.MigrateEdgeInstanceFencing, schemamigrate.MigrateSourceUploadSessions} {
		if err := migrate(ctx, database); err != nil {
			t.Fatal(err)
		}
	}
	return database
}

func TestPhysicalDNSProducerReconfigurationPostgres(t *testing.T) {
	fixture := physicalDNSReconfigurationFixture(t, disposablePhysicalDNSDatabase(t))
	before, err := fixture.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil {
		t.Fatal(err)
	}
	stale := fixture.request
	precondition := *stale.ProducerReconfiguration
	precondition.PreviousPolicy.FencingToken++
	stale.ProducerReconfiguration = &precondition
	stale.IdempotencyKey, err = producerReconfigurationKey(fixture.next, precondition)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := fixture.s.ReleasePlatformArtifact(fixture.next.ID, stale, testPlatformPrincipal()); err == nil {
		t.Fatal("PostgreSQL admitted stale producer fence")
	}
	first, err := fixture.release()
	if err != nil {
		t.Fatal(err)
	}
	again, err := fixture.release()
	if err != nil || first.ID != again.ID {
		t.Fatal("PostgreSQL idempotent retry changed authority", err)
	}
	after, err := fixture.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("PostgreSQL configuration transition changed positive LKG", err)
	}
}

// The freeze is checked by PostgreSQL itself, so the legacy generic release
// and rollback APIs cannot escape the global producer's shared boundary.
func TestGlobalProducerFreezeRejectsLegacyStoreMutationsPostgres(t *testing.T) {
	f := physicalDNSReconfigurationFixture(t, disposablePhysicalDNSDatabase(t))
	kind, scope := model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope
	current, release, ok, err := f.s.GetActivePlatformArtifact(kind, scope, "shadow")
	if err != nil || !ok || current.ID != f.old.ID {
		t.Fatalf("missing producer baseline: %t %v", ok, err)
	}
	lkg, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.s.db.Exec(`UPDATE fugue_native_global_producer_gate SET frozen=TRUE,
baseline_release_id=$1,baseline_fence=$2,baseline_digest=$3,evidence_digest=$4 WHERE singleton`,
		release.ID, release.FencingToken, current.ContentHash, "sha256:"+strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.release(); err == nil {
		t.Fatal("exact legacy reconfiguration escaped the provider freeze")
	}
	if _, _, _, _, err := f.s.ReleasePlatformArtifact(f.next.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal()); err == nil {
		t.Fatal("generic legacy release escaped the provider freeze")
	}
	if _, _, _, _, err := f.s.RollbackPlatformArtifact(f.old.ID, model.PlatformArtifactRollbackRequest{ReleaseChannel: "shadow", ToGeneration: f.next.Generation, Reason: "legacy fixture"}, testPlatformPrincipal()); err == nil {
		t.Fatal("generic legacy rollback escaped the provider freeze")
	}
	after, selected, ok, err := f.s.GetActivePlatformArtifact(kind, scope, "shadow")
	if err != nil || !ok || !reflect.DeepEqual(release, selected) || !reflect.DeepEqual(current, after) {
		t.Fatalf("freeze changed selected producer: %t %v", ok, err)
	}
	still, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || !reflect.DeepEqual(lkg, still) {
		t.Fatal("freeze changed serving LKG", err)
	}
}
