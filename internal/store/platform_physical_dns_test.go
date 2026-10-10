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
	if serving.s.usingDatabase() {
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
			_, err = pgUpsertPlatformLKGSnapshot(ctx, tx, lkg)
		}
		if err == nil {
			err = tx.Commit()
		}
	} else {
		err = serving.s.withLockedState(true, func(state *model.State) error {
			state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, full)
			state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{LaneKey: full.LaneKey, ArtifactKind: full.ArtifactKind, ScopeKey: full.ScopeKey, ReleaseChannel: "full", FencingToken: full.FencingToken, ActiveReleaseID: full.ID})
			state.PlatformLKGSnapshots = append(state.PlatformLKGSnapshots, lkg)
			return nil
		})
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

func TestPhysicalDNSProducerReconfigurationPreservesPositivePublication(t *testing.T) {
	for _, scenario := range []string{"success", "stale_policy", "stale_full", "evidence_mismatch", "implicit_operation", "schedule_changed", "static_changed", "source_tampered", "unrelated_dns_change", "two_hostnames", "frozen_target", "unverified_baseline", "pending_gray"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := physicalDNSReconfigurationFixture(t, "")
			switch scenario {
			case "stale_policy":
				fixture.request.ProducerReconfiguration.PreviousPolicy.FencingToken++
			case "stale_full":
				fixture.request.ProducerReconfiguration.ServingFull.FencingToken++
			case "evidence_mismatch":
				fixture.request.ProducerReconfiguration.VerificationEvidenceHash = "sha256:" + strings.Repeat("b", 64)
			case "implicit_operation":
				fixture.request.ProducerReconfiguration.Operation = ""
			case "schedule_changed", "static_changed":
				policy := fixture.policy
				if scenario == "schedule_changed" {
					policy.RefreshSeconds++
				} else {
					policy.StaticIntentArtifactID = "foreign"
				}
				policy.Generation += "-changed"
				fixture.next = savePhysicalDNSTestArtifact(t, fixture.s, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, policy.Generation, policy)
			case "unrelated_dns_change", "two_hostnames":
				source, err := fixture.s.GetPlatformArtifact(fixture.policy.DNSPolicyArtifactID)
				if err != nil {
					t.Fatal(err)
				}
				source.Content["generation"] = "dns-changed"
				query := source.Content["dns_query_policy"].(map[string]any)
				if scenario == "unrelated_dns_change" {
					query["exploration_percent"] = 4
				} else {
					query["physical_routes"] = []platformconfig.PhysicalQualityRoute{{Hostname: "app.example.test", TrafficClass: "streaming", Policy: edgequality.DefaultNetworkPolicy()}, {Hostname: "other.example.test", TrafficClass: "streaming", Policy: edgequality.DefaultNetworkPolicy()}}
				}
				source = savePhysicalDNSTestArtifact(t, fixture.s, source.ArtifactKind, source.ScopeKey, "dns-changed", source.Content)
				policy := fixture.policy
				policy.Generation += "-changed"
				policy.DNSPolicyArtifactID, policy.DNSPolicyDigest = source.ID, source.ContentHash
				fixture.next = savePhysicalDNSTestArtifact(t, fixture.s, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, policy.Generation, policy)
			case "frozen_target":
				fixture.freeze(t, "global")
			case "unverified_baseline":
				fixture.setBaselineVerification(t, false)
			case "source_tampered", "pending_gray":
				if err := fixture.s.withLockedState(true, func(state *model.State) error {
					if scenario == "source_tampered" {
						index := platformArtifactIndex(state.PlatformArtifacts, fixture.policy.DNSPolicyArtifactID)
						state.PlatformArtifacts[index].Content["generation"] = "untrusted"
					} else {
						gray := state.PlatformArtifactReleases[platformArtifactReleaseIndex(state.PlatformArtifactReleases, fixture.request.ProducerReconfiguration.ServingFull.ReleaseID)]
						gray.ID, gray.ReleaseChannel, gray.ReleasedAt = "pending-gray", "gray", time.Now().UTC().Add(time.Second)
						gray.LaneKey = platformsafety.ReleaseLaneKey(gray.ArtifactKind, "global", "gray")
						gray.VerificationState = ""
						state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, gray)
						state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{LaneKey: gray.LaneKey, ArtifactKind: gray.ArtifactKind, ScopeKey: "global", ReleaseChannel: "gray", FencingToken: gray.FencingToken, ActiveReleaseID: gray.ID})
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			fixture.bindKey(t)
			release, err := fixture.release()
			if (scenario == "success") != (err == nil) {
				t.Fatalf("%s: unexpected transition result: %v", scenario, err)
			}
			if scenario == "success" {
				again, err := fixture.release()
				if err != nil || release.ID != again.ID {
					t.Fatal("exact retry changed publication", err)
				}
			}
			artifact, _, found, err := fixture.s.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
			wanted := fixture.old.ID
			if scenario == "success" {
				wanted = fixture.next.ID
			}
			if err != nil || !found || artifact.ID != wanted {
				t.Fatal("failed configuration replaced producer authority", err)
			}
			lkg, err := fixture.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
			if err != nil || !reflect.DeepEqual(lkg, &fixture.lkg) {
				t.Fatal("configuration transaction changed positive LKG", err)
			}
			_, full, found, err := fixture.s.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "full")
			if err != nil || !found || full.ID != fixture.request.ProducerReconfiguration.ServingFull.ReleaseID {
				t.Fatal("configuration transaction changed serving full", err)
			}
		})
	}
}

func TestPhysicalDNSProducerReconfigurationPostgres(t *testing.T) {
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
	fixture := physicalDNSReconfigurationFixture(t, database)
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
