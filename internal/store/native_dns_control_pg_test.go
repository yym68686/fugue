package store

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian/dnscontrol"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type nativeDNSFixtureEvidence struct{}

func (nativeDNSFixtureEvidence) VerifyBaseline(context.Context, productioncontrol.Baseline) error {
	return nil
}
func (nativeDNSFixtureEvidence) VerifyTerminal(context.Context, productioncontrol.Claim, productioncontrol.Intent, productioncontrol.ExecutionResult) error {
	return errors.New("fixture has no terminal serving evidence")
}

// This test uses signed *real Store artifacts* and the existing PostgreSQL
// publication transaction. Only the external shadow and enrollment evidence
// are fixture values. This is not a production traffic or rollback test.
func TestNativeDNSDispatchStorePostgres(t *testing.T) {
	if os.Getenv("FUGUE_TEST_DATABASE_URL") == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	f := physicalDNSFixtureWithQuery(t, disposablePhysicalDNSDatabase(t), func(map[string]any) {})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	previous, err := platformproducer.Decode(f.old)
	if err != nil {
		t.Fatal(err)
	}
	get := func(id string) model.PlatformArtifact {
		t.Helper()
		a, err := f.s.GetPlatformArtifact(id)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	oldDNS := get(previous.DNSPolicyArtifactID)
	newDNS := get(previous.DNSPolicyArtifactID)
	newDNS.Content["generation"] = "native-dns-fixture"
	newDNS.Content["offline_edges"] = []platformproducer.OfflineEdge{{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Reason: "quota_exhausted", ExpectedRecoveryDate: "2020-01-01"}}
	newDNS = savePhysicalDNSTestArtifact(t, f.s, oldDNS.ArtifactKind, oldDNS.ScopeKey, "native-dns-fixture", newDNS.Content)
	next := previous
	next.Generation = "native-producer-fixture"
	next.DNSPolicyArtifactID, next.DNSPolicyDigest = newDNS.ID, newDNS.ContentHash
	newProducer := savePhysicalDNSTestArtifact(t, f.s, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, next.Generation, next)
	base, previousRelease, found, err := f.s.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil || !found || base.ID != f.old.ID {
		t.Fatal("no selected predecessor", err)
	}
	full, fullRelease, found, err := f.s.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "full")
	if err != nil || !found {
		t.Fatal("no selected full", err)
	}
	lkg, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || lkg == nil {
		t.Fatal("no verified LKG", err)
	}
	sources := dnscontrol.Sources{Producer: dnscontrol.Publication{Artifact: base, Release: previousRelease},
		Full: dnscontrol.Publication{Artifact: full, Release: fullRelease}, LKG: *lkg,
		Static: get(previous.StaticIntentArtifactID), PreviousDNS: oldDNS, NextDNS: newDNS, NextProducer: newProducer}
	payload, _, err := dnscontrol.ValidateSources(sources, time.Now().UTC())
	if err != nil {
		t.Fatal("real producer source validation", err)
	}
	payload.Origin, payload.ShadowDigest = "https://fixture.example.test", "sha256:"+strings.Repeat("a", 64)
	const cluster = "dns-fixture"
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := productioncontrol.NewIntent(cluster, dnscontrol.Resource, "fixture-offline", productioncontrol.DNSMembership,
		previousRelease.FencingToken, base.ContentHash, newProducer.ContentHash, base.ContentHash, raw)
	if err != nil {
		t.Fatal(err)
	}
	sub := productioncontrol.Submission{Intent: intent, Payload: raw}
	if _, err := dnscontrol.Inspect(sub); err != nil {
		t.Fatal(err)
	}
	var schemaExists bool
	if err := f.s.db.QueryRowContext(ctx, `SELECT to_regnamespace('fugue_control') IS NOT NULL`).Scan(&schemaExists); err != nil || schemaExists {
		t.Fatal("fixture requires disposable database without a control journal", err)
	}
	if err := productioncontrol.ApplySchema(ctx, f.s.db); err != nil {
		t.Fatal(err)
	}
	// Keep role and journal clean-up scoped to this newly-created fixture.
	roleCreated := false
	const writerRole = "fugue_native_dns_store_fixture"
	t.Cleanup(func() {
		// The schema fixture's later cleanup owns the platform schema. Drop
		// the grants only for this exact temporary role before removing it.
		if roleCreated {
			if _, err := f.s.db.Exec(`DROP OWNED BY fugue_native_dns_store_fixture`); err != nil {
				t.Error(err)
			}
		}
		if _, err := f.s.db.Exec(`DROP SCHEMA fugue_control CASCADE`); err != nil {
			t.Error(err)
		}
		if roleCreated {
			if _, err := f.s.db.Exec(`DROP ROLE fugue_native_dns_store_fixture`); err != nil {
				t.Error(err)
			}
		}
	})
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	owners := productioncontrol.Approvers{"owner": {ClusterID: cluster, PublicKey: public}}
	ownerJournal, err := productioncontrol.NewJournal(f.s.db, owners)
	if err != nil {
		t.Fatal(err)
	}
	observer, err := productioncontrol.NewExecutionJournal(ownerJournal, nativeDNSFixtureEvidence{})
	if err != nil {
		t.Fatal(err)
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := f.s.db.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	freezeProof := "sha256:" + strings.Repeat("b", 64)
	baselineProof := "sha256:" + strings.Repeat("c", 64)
	activationProof := "sha256:" + strings.Repeat("d", 64)
	exec(`UPDATE fugue_native_global_producer_gate SET frozen=TRUE,baseline_release_id=$1,baseline_fence=$2,baseline_digest=$3,evidence_digest=$4 WHERE singleton`,
		previousRelease.ID, previousRelease.FencingToken, base.ContentHash, freezeProof)
	if inserted, err := observer.AnchorResource(ctx, "fixture-observer", productioncontrol.Baseline{Scope: dnscontrol.Scope(cluster),
		Verified: productioncontrol.Version{Generation: previousRelease.FencingToken, Digest: base.ContentHash}, EvidenceDigest: baselineProof}); err != nil || !inserted {
		t.Fatal("baseline", inserted, err)
	}
	exec(`CREATE ROLE fugue_native_dns_store_fixture LOGIN NOINHERIT`)
	roleCreated = true
	var schemaName string
	if err := f.s.db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schemaName); err != nil {
		t.Fatal(err)
	}
	schema := quotePostgresIntegrationIdentifier(t, schemaName)
	exec(`GRANT USAGE ON SCHEMA ` + schema + ` TO fugue_native_dns_store_fixture;
GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA ` + schema + ` TO fugue_native_dns_store_fixture;
REVOKE INSERT,UPDATE ON fugue_native_global_producer_gate,fugue_native_global_producer_authority,fugue_native_global_producer_receipts FROM fugue_native_dns_store_fixture;
GRANT INSERT ON fugue_native_global_producer_receipts TO fugue_native_dns_store_fixture;
GRANT USAGE ON SCHEMA fugue_control TO fugue_native_dns_store_fixture;
GRANT SELECT ON ALL TABLES IN SCHEMA fugue_control TO fugue_native_dns_store_fixture;
GRANT INSERT ON fugue_control.execution_claims TO fugue_native_dns_store_fixture;`)
	exec(`INSERT INTO fugue_native_global_producer_authority(cluster_id,writer_role,baseline_evidence_digest,activation_evidence_digest) VALUES ($1,$2,$3,$4)`, cluster, writerRole, baselineProof, activationProof)
	row, err := ownerJournal.RecordIntent(ctx, "fixture-owner", intent, raw)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := productioncontrol.SignApproval(intent, "owner", private, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	approved, err := ownerJournal.RecordApproval(ctx, approval)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(f.s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	nativeDB := stdlib.OpenDB(*cfg, stdlib.OptionAfterConnect(func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `SET SESSION AUTHORIZATION fugue_native_dns_store_fixture`)
		return err
	}))
	t.Cleanup(func() { _ = nativeDB.Close() })
	nativeStore := &Store{db: nativeDB, dbReady: true}
	nativeStore.ConfigurePlatformArtifactSigning(f.s.platformArtifactSigningKeyring())
	nativeJournal, err := productioncontrol.NewJournal(nativeDB, owners)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := dnscontrol.NewReader(payload.Origin, "fixture-read-token")
	if err != nil {
		t.Fatal(err)
	}
	control := &NativeDNSControl{store: nativeStore, journal: nativeJournal, cluster: cluster, reader: reader}
	exec(`GRANT UPDATE (evidence_digest) ON fugue_control.execution_results TO fugue_native_dns_store_fixture`)
	if _, err := control.Dispatch(ctx, row.Digest, approved.Digest); err == nil || !strings.Contains(err.Error(), "conflicting journal or provider privileges") {
		t.Fatalf("did not refuse dispatch with conflicting privileges: %v", err)
	}
	if head, err := nativeJournal.ReadResource(ctx, dnscontrol.Scope(cluster)); err != nil || head.LastFence != 0 || head.Pending != "" {
		t.Fatalf("privilege preflight reserved an execution: %+v %v", head, err)
	}
	exec(`REVOKE UPDATE (evidence_digest) ON fugue_control.execution_results FROM fugue_native_dns_store_fixture`)
	if err := control.isActive(ctx); err != nil {
		t.Fatal(err)
	}
	live := payload
	live.ShadowDigest = "sha256:" + strings.Repeat("e", 64)
	result, err := control.dispatchPrepared(ctx, row.Digest, approved.Digest, sub, payload, live)
	if err != nil || result.Replayed || result.SelectedFence != previousRelease.FencingToken+1 || result.SelectedReleaseID == "" || result.ExecutionEvidence {
		t.Fatalf("native exact store transaction: %+v %v", result, err)
	}
	selected, release, found, err := f.s.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil || !found || selected.ID != newProducer.ID || release.ID != result.SelectedReleaseID || release.ReleasedByID != nativeDNSActor || release.FencingToken != result.SelectedFence {
		t.Fatal("unexpected provider readback", err)
	}
	var count int
	if err := f.s.db.QueryRowContext(ctx, `SELECT count(*) FROM fugue_native_global_producer_receipts WHERE intent_digest=$1 AND approval_digest=$2 AND approved_shadow_digest=$3 AND live_shadow_digest=$4`,
		row.Digest, approved.Digest, payload.ShadowDigest, live.ShadowDigest).Scan(&count); err != nil || count != 1 {
		t.Fatal("native transaction lacks exact receipt", count, err)
	}
	if replay, err := control.Dispatch(ctx, row.Digest, approved.Digest); err != nil || !replay.Replayed || replay.Claim != result.Claim || replay.SelectedReleaseID != "" {
		t.Fatalf("restart/replay issued another dispatch: %+v %v", replay, err)
	}
	if _, _, _, _, err := f.s.RollbackPlatformArtifact(newProducer.ID, model.PlatformArtifactRollbackRequest{ReleaseChannel: "shadow", ToGeneration: base.Generation, Reason: "old runner"}, testPlatformPrincipal()); err == nil {
		t.Fatal("native activation reopened the legacy rollback")
	}
	still, err := f.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || !reflect.DeepEqual(lkg, still) {
		t.Fatal("native policy publication changed the previously verified LKG", err)
	}
	head, err := nativeJournal.ReadResource(ctx, dnscontrol.Scope(cluster))
	if err != nil || head.Pending != row.Digest || head.LastVerified.Digest != base.ContentHash {
		t.Fatal("policy selection was falsely settled as serving", head, err)
	}
}
