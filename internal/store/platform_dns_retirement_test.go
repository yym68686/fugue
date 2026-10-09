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

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/schemamigrate"
)

func sealDNSRetirementTestArtifact(artifact model.PlatformArtifact, state *Store) (model.PlatformArtifact, error) {
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return artifact, err
	}
	if err = json.Unmarshal(raw, &artifact.Content); err != nil {
		return artifact, err
	}
	artifact.ContentHash, err = platformconfig.Digest(artifact.Content)
	if err != nil {
		return artifact, err
	}
	return platformsafety.SignPlatformArtifact(artifact, state.platformArtifactSigningKeyring())
}

func TestDNSRetirementTransactionPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Hostname() != "127.0.0.1" || !strings.Contains(parsed.Path, "fugue_test") {
		t.Fatal("requires disposable loopback PostgreSQL")
	}
	base, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	schema := model.NewID("dns_retirement")
	quoted := quotePostgresIntegrationIdentifier(t, schema)
	if _, err = base.Exec("CREATE SCHEMA " + quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { base.Exec("DROP SCHEMA " + quoted + " CASCADE"); base.Close() })
	address = postgresIntegrationURLWithSearchPath(t, address, schema)
	database, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	fixture := dnsRetirementFixture(t)
	target := &Store{db: database, databaseURL: address}
	target.ConfigurePlatformArtifactSigning(fixture.s.platformArtifactSigningKeyring())
	ctx := context.Background()
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = target.applyPostgresSchemaTx(ctx, tx); err != nil {
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
	tx, err = database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	err = fixture.s.withLockedState(false, func(state *model.State) error {
		for _, artifact := range state.PlatformArtifacts {
			if _, err := pgInsertPlatformArtifact(ctx, tx, artifact); err != nil {
				return err
			}
		}
		for _, release := range state.PlatformArtifactReleases {
			if _, err := pgInsertPlatformArtifactRelease(ctx, tx, release); err != nil {
				return err
			}
		}
		for _, lane := range state.PlatformReleaseLanes {
			for sequence := int64(0); sequence < lane.FencingToken; sequence++ {
				if _, err := pgNextPlatformReleaseLane(ctx, tx, lane.ArtifactKind, lane.ScopeKey, lane.ReleaseChannel, time.Now().UTC()); err != nil {
					return err
				}
			}
			if err := pgSetPlatformReleaseLaneActive(ctx, tx, lane.LaneKey, lane.ActiveReleaseID, lane.FencingToken, time.Now().UTC()); err != nil {
				return err
			}
		}
		for _, lkg := range state.PlatformLKGSnapshots {
			if _, err := pgUpsertPlatformLKGSnapshot(ctx, tx, lkg); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	fixture.s = target
	before, err := target.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil {
		t.Fatal(err)
	}
	stale := fixture
	precondition := *fixture.request.ProducerReconfiguration
	precondition.ServingFull.FencingToken++
	stale.request.ProducerReconfiguration = &precondition
	stale.bindKey(t)
	if _, err := stale.release(); err == nil {
		t.Fatal("stale PostgreSQL baseline accepted")
	}
	first, err := fixture.release()
	if err != nil {
		t.Fatal(err)
	}
	again, err := fixture.release()
	if err != nil || first.ID != again.ID {
		t.Fatal("PostgreSQL retry changed authority", err)
	}
	after, err := target.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("PostgreSQL retirement changed LKG", err)
	}
	t.Run("capabilities", func(t *testing.T) { testTrafficExecutionAdmission(t, address, true) })
}

func dnsRetirementFixture(t *testing.T) reconfigurationFixture {
	t.Helper()
	fixture := physicalDNSReconfigurationFixture(t, "")
	previous, err := platformproducer.Decode(fixture.old)
	if err != nil {
		t.Fatal(err)
	}
	source, err := fixture.s.GetPlatformArtifact(previous.DNSPolicyArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	source.Content["generation"] = "dns-ordered"
	query := source.Content["dns_query_policy"].(map[string]any)
	query["ecs_enabled"], query["exploration_percent"] = false, 0
	order := model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a"}}
	query["ordered_projection"] = platformconfig.DNSOrderedProjection{DefaultOrder: order, Overrides: []platformconfig.DNSOrderOverride{{NodeID: "dns-a", Hostname: "app.example.test", Type: "A", Order: order}}}
	source = savePhysicalDNSTestArtifact(t, fixture.s, source.ArtifactKind, source.ScopeKey, "dns-ordered", source.Content)
	fixture.policy = previous
	fixture.policy.Generation = "producer-ordered"
	fixture.policy.DNSPolicyArtifactID, fixture.policy.DNSPolicyDigest = source.ID, source.ContentHash
	fixture.next = savePhysicalDNSTestArtifact(t, fixture.s, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, fixture.policy.Generation, fixture.policy)
	fixture.request.ProducerReconfiguration.Operation = "retire_dns_selector"
	fixture.bindKey(t)
	if err := fixture.s.withLockedState(true, func(state *model.State) error {
		parent := state.PlatformArtifacts[platformArtifactIndex(state.PlatformArtifacts, fixture.request.ProducerReconfiguration.ServingFull.ArtifactID)]
		id, err := dnsRetirementMember(parent)
		if err != nil {
			return err
		}
		index := platformArtifactIndex(state.PlatformArtifacts, id)
		child := state.PlatformArtifacts[index]
		child.Content["query_views"] = []platformconfig.DNSQueryView{{NodeID: "dns-a", Zone: "example.test", Records: []model.EdgeDNSRecord{{Name: "app.example.test", Type: "A", AnswerPolicy: model.DNSAnswerPolicy{PolicyKind: "geo"}, Candidates: []model.EdgeDNSAnswerCandidate{{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", IP: "8.8.8.8"}}}}}}
		child, err = sealDNSRetirementTestArtifact(child, fixture.s)
		state.PlatformArtifacts[index] = child
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestDNSRetirementTransactionPreservesPositiveLKG(t *testing.T) {
	for _, scenario := range []string{"success", "stale_policy", "stale_full", "implicit", "wrong_operation", "baseline_tampered", "source_tampered", "baseline_missing", "candidate_changed", "static_changed", "frozen", "unverified"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := dnsRetirementFixture(t)
			switch scenario {
			case "stale_policy":
				fixture.request.ProducerReconfiguration.PreviousPolicy.FencingToken++
			case "stale_full":
				fixture.request.ProducerReconfiguration.ServingFull.FencingToken++
			case "implicit":
				fixture.request.ProducerReconfiguration.Operation = ""
			case "wrong_operation":
				fixture.request.ProducerReconfiguration.Operation = "physical_dns"
			case "frozen":
				fixture.freeze(t, "global")
			case "unverified":
				fixture.setBaselineVerification(t, false)
			case "static_changed":
				fixture.policy.StaticIntentArtifactID = "foreign"
				fixture.policy.Generation += "-changed"
				fixture.next = savePhysicalDNSTestArtifact(t, fixture.s, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, fixture.policy.Generation, fixture.policy)
			case "baseline_tampered", "source_tampered", "baseline_missing", "candidate_changed":
				if err := fixture.s.withLockedState(true, func(state *model.State) error {
					parent := state.PlatformArtifacts[platformArtifactIndex(state.PlatformArtifacts, fixture.request.ProducerReconfiguration.ServingFull.ArtifactID)]
					id, err := dnsRetirementMember(parent)
					if err != nil {
						return err
					}
					if scenario == "source_tampered" {
						id = fixture.policy.DNSPolicyArtifactID
					}
					index := platformArtifactIndex(state.PlatformArtifacts, id)
					if scenario == "baseline_missing" {
						state.PlatformArtifacts = append(state.PlatformArtifacts[:index], state.PlatformArtifacts[index+1:]...)
					} else if scenario == "candidate_changed" {
						child := state.PlatformArtifacts[index]
						child.Content["query_views"] = []platformconfig.DNSQueryView{{NodeID: "dns-a", Zone: "example.test", Records: []model.EdgeDNSRecord{{Name: "app.example.test", Type: "A", AnswerPolicy: model.DNSAnswerPolicy{PolicyKind: "geo"}, Candidates: []model.EdgeDNSAnswerCandidate{{EdgeID: "edge-b", IP: "9.9.9.9"}}}}}}
						child, err = sealDNSRetirementTestArtifact(child, fixture.s)
						if err != nil {
							return err
						}
						state.PlatformArtifacts[index] = child
					} else {
						state.PlatformArtifacts[index].Content["untrusted"] = true
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			fixture.bindKey(t)
			before, err := fixture.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
			if err != nil {
				t.Fatal(err)
			}
			first, err := fixture.release()
			if (scenario == "success") != (err == nil) {
				t.Fatalf("%s transaction result: %v", scenario, err)
			}
			if scenario == "success" {
				again, err := fixture.release()
				if err != nil || first.ID != again.ID {
					t.Fatal("idempotent retry changed authority", err)
				}
			}
			after, err := fixture.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("retirement altered positive LKG", err)
			}
		})
	}
}
