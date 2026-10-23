package store

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func availabilityRecoveryFixture(t *testing.T, database, scenario string) reconfigurationFixture {
	t.Helper()
	f := physicalDNSReconfigurationFixture(t, database)
	previous, err := platformproducer.Decode(f.old)
	if err != nil {
		t.Fatal(err)
	}
	source, err := f.s.GetPlatformArtifact(previous.DNSPolicyArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	source.Content["generation"] = "recovery-source"
	source.Content["dns_query_policy"].(map[string]any)["availability_routes"] = []platformconfig.DNSAvailabilityRoute{{Hostname: "app.example.test", Policy: model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}}}
	source = savePhysicalDNSTestArtifact(t, f.s, source.ArtifactKind, source.ScopeKey, "recovery-source", source.Content)
	next := previous
	next.Generation, next.DNSPolicyArtifactID, next.DNSPolicyDigest = "recovery-producer", source.ID, source.ContentHash
	f.next = savePhysicalDNSTestArtifact(t, f.s, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, next.Generation, next)
	f.request.ProducerReconfiguration.Operation = "dns_availability_recovery"
	parent, original, found, err := f.s.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "full")
	if err != nil || !found {
		t.Fatal("baseline missing", err)
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	lane := model.PlatformReleaseLane{LaneKey: original.LaneKey, ArtifactKind: parent.ArtifactKind, ScopeKey: parent.ScopeKey, ReleaseChannel: "full", FencingToken: original.FencingToken + 1}
	principal := model.Principal{ActorType: model.ActorTypeBootstrap, ActorID: platformproducer.Actor}
	entry := buildPlatformArtifactReleaseLedgerEntry(parent, "full", parent.Generation, "", "verified rollback fixture", "", model.PlatformReleaseMessageTypeRollback, "", nil, nil, principal, lane, at)
	current, message := entry.Release, entry.Message
	switch scenario {
	case "ordinary_unverified":
		current.PinnedRollbackGeneration, current.RollbackTargetGeneration = "", ""
	case "no_message":
		message.ReleaseID = "other-release"
	case "wrong_message":
		message.ArtifactID = f.next.ID
	case "wrong_message_time":
		message.CreatedAt = message.CreatedAt.Add(-time.Second)
	case "unverified_original":
		original.VerificationState = model.PlatformArtifactVerificationStateServingUnverified
	case "wrong_original_artifact":
		original.ArtifactID = "other-artifact"
	case "original_nonproducer":
		original.ReleasedByID = "unrelated-actor"
	case "override":
		current.OverrideMode = "soft"
	case "bypassed":
		current.BypassedInvariants = []string{"synthetic-invariant"}
	case "wrong_rollback_target":
		current.RollbackTargetGeneration = "other-generation"
	case "nonproducer":
		current.ReleasedByID = "unrelated-actor"
	case "future_rollback":
		current.ReleasedAt = at.Add(time.Hour)
		message.CreatedAt = current.ReleasedAt
	case "expired_lkg", "tampered_lkg":
		if scenario == "expired_lkg" {
			f.lkg.ExpiresAt = at.Add(-time.Second)
			f.lkg, err = platformsafety.SignPlatformLKGSnapshot(f.lkg, f.s.platformArtifactSigningKeyring())
			if err != nil {
				t.Fatal(err)
			}
		} else {
			f.lkg.VerificationEvidenceHash = "sha256:" + strings.Repeat("b", 64)
		}
	}
	lane.ActiveReleaseID = current.ID
	if f.s.usingDatabase() {
		ctx := context.Background()
		tx, e := f.s.db.BeginTx(ctx, nil)
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback()
		if e = pgSupersedePlatformReleases(ctx, tx, parent.ArtifactKind, parent.ScopeKey, "full", at); e != nil {
			t.Fatal(e)
		}
		if _, e = pgNextPlatformReleaseLane(ctx, tx, parent.ArtifactKind, parent.ScopeKey, "full", at); e != nil {
			t.Fatal(e)
		}
		if _, e = pgInsertPlatformArtifactRelease(ctx, tx, current); e != nil {
			t.Fatal(e)
		}
		if e = pgSetPlatformReleaseLaneActive(ctx, tx, lane.LaneKey, current.ID, current.FencingToken, at); e != nil {
			t.Fatal(e)
		}
		if scenario != "no_message" {
			if _, e = pgInsertPlatformReleaseMessage(ctx, tx, message); e != nil {
				t.Fatal(e)
			}
		}
		if e = tx.Commit(); e != nil {
			t.Fatal(e)
		}
	} else {
		err = f.s.withLockedState(true, func(state *model.State) error {
			state.PlatformArtifactReleases[platformArtifactReleaseIndex(state.PlatformArtifactReleases, original.ID)] = original
			state.PlatformArtifactReleases = supersedePlatformReleases(state.PlatformArtifactReleases, parent.ArtifactKind, parent.ScopeKey, "full", current.ID, at)
			state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, current)
			state.PlatformReleaseLanes = upsertPlatformReleaseLane(state.PlatformReleaseLanes, lane)
			state.PlatformReleaseMessages = append(state.PlatformReleaseMessages, message)
			state.PlatformLKGSnapshots = []model.PlatformLKGSnapshot{f.lkg}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	f.request.ProducerReconfiguration.ServingFull = model.PlatformPublicationPrecondition{ArtifactID: parent.ID, ContentHash: parent.ContentHash, ReleaseID: current.ID, FencingToken: current.FencingToken}
	switch scenario {
	case "normal_operation":
		f.request.ProducerReconfiguration.Operation = "dns_availability"
	case "stale_full":
		f.request.ProducerReconfiguration.ServingFull.FencingToken++
	case "stale_policy":
		f.request.ProducerReconfiguration.PreviousPolicy.FencingToken++
	case "wrong_evidence":
		f.request.ProducerReconfiguration.VerificationEvidenceHash = "sha256:" + strings.Repeat("c", 64)
	}
	f.bindKey(t)
	return f
}

func testAvailabilityRecoveryTransaction(t *testing.T, database, scenario string) {
	t.Helper()
	f := availabilityRecoveryFixture(t, database, scenario)
	_, fullBefore, _, err := f.s.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "full")
	if err != nil {
		t.Fatal(err)
	}
	lkgBefore, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil {
		t.Fatal(err)
	}
	released, err := f.release()
	if (scenario == "success") != (err == nil) {
		t.Fatalf("%s: unexpected result %v", scenario, err)
	}
	if scenario == "success" {
		again, err := f.release()
		if err != nil || again.ID != released.ID {
			t.Fatal("non-idempotent recovery", err)
		}
	}
	authority, _, found, err := f.s.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	wanted := f.old.ID
	if scenario == "success" {
		wanted = f.next.ID
	}
	if err != nil || !found || authority.ID != wanted {
		t.Fatal("producer authority differs", err)
	}
	_, fullAfter, _, err := f.s.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "full")
	if err != nil || !reflect.DeepEqual(fullBefore, fullAfter) {
		t.Fatal("configuration changed/verified serving release", err)
	}
	lkgAfter, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || !reflect.DeepEqual(lkgBefore, lkgAfter) {
		t.Fatal("configuration overwrote positive LKG", err)
	}
}

func TestAvailabilityRecoveryRequiresExactVerifiedRollback(t *testing.T) {
	for _, scenario := range []string{"success", "ordinary_unverified", "no_message", "wrong_message", "wrong_message_time", "unverified_original", "wrong_original_artifact", "original_nonproducer", "override", "bypassed", "wrong_rollback_target", "nonproducer", "future_rollback", "expired_lkg", "tampered_lkg", "normal_operation", "stale_full", "stale_policy", "wrong_evidence"} {
		t.Run(scenario, func(t *testing.T) { testAvailabilityRecoveryTransaction(t, "", scenario) })
	}
}

func TestAvailabilityRecoveryPostgres(t *testing.T) {
	for _, scenario := range []string{"success", "no_message", "wrong_message", "normal_operation", "stale_full", "stale_policy", "wrong_evidence"} {
		t.Run(scenario, func(t *testing.T) { testAvailabilityRecoveryTransaction(t, disposablePhysicalDNSDatabase(t), scenario) })
	}
}
