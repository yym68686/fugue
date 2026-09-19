package edgecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/trafficbinding"
)

func TestGroupCodeRecoveryCannotRollBackTrafficAuthority(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	group := "edge-group-country-de"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = store.StoreGroupInventoryCAS(ctx, group, 0, groupInventoryFixture(group, "a", "epoch", "inventory", false)); err != nil {
		t.Fatal(err)
	}
	signer := &fixtureGroupSigner{keys: map[string][]byte{group: bytes.Repeat([]byte{0x73}, 32)}, validFor: time.Hour}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}
	publisher := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	publish := func(snapshot model.EdgeRouteIntentSnapshot) GroupAuthorityState {
		t.Helper()
		batch, err := compiler.Reconcile(ctx, snapshot, []string{group})
		if err != nil || batch.Succeeded != 1 {
			t.Fatal("compile traffic", err)
		}
		result, err := publisher.Publish(ctx, batch)
		if err != nil || result.Published != 1 {
			t.Fatal("publish traffic", result, err)
		}
		state, err := store.ReadGroupAuthority(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
		return state
	}
	legacy := publish(routeIntentFixture())
	snapshot := routeIntentFixture()
	digest := "sha256:" + strings.Repeat("a", 64)
	snapshot.TrafficRelease = &model.TrafficReleaseBinding{Schema: trafficbinding.Schema, ReleaseSetID: "parent-first", ReleaseSetDigest: digest, ReleaseSetGeneration: "parent-first-generation", RouteArtifactID: "route-first", RouteArtifactDigest: digest, RouteArtifactGeneration: snapshot.Generation, RouteArtifactSequence: 1, ReleaseID: "release-gray", ReleaseChannel: "gray", FencingToken: 1, ScopeKey: "global", IntentDigest: digest, PolicyDigest: digest, InputSnapshotDigest: digest, CompilerVersion: "compiler", ProjectionDigest: trafficbinding.ProjectionDigest(snapshot), CanaryRuleRef: "cohort=first", EdgeGroupIDs: []string{group}}
	gray := publish(snapshot)
	fullSnapshot := routeIntentFixture()
	fullSnapshot.Generation = "route-newer"
	fullSnapshot.Routes[0].UpstreamURL = "http://new-origin.mesh:8080"
	fullSnapshot.TrafficRelease = trafficbinding.Clone(snapshot.TrafficRelease)
	fullSnapshot.TrafficRelease.ReleaseSetID = "parent-newer"
	fullSnapshot.TrafficRelease.ReleaseSetGeneration = "parent-newer-generation"
	fullSnapshot.TrafficRelease.RouteArtifactID = "route-newer"
	fullSnapshot.TrafficRelease.RouteArtifactGeneration = fullSnapshot.Generation
	fullSnapshot.TrafficRelease.RouteArtifactSequence = 2
	fullSnapshot.TrafficRelease.ProjectionDigest = trafficbinding.ProjectionDigest(fullSnapshot)
	fullSnapshot.TrafficRelease.ReleaseID = "release-full"
	fullSnapshot.TrafficRelease.ReleaseChannel = "full"
	fullSnapshot.TrafficRelease.CanaryRuleRef = ""
	fullSnapshot.TrafficRelease.EdgeGroupIDs = nil
	full := publish(fullSnapshot)
	dir := privateFixtureDir(t)
	secret := bytes.Repeat([]byte{0x51}, 32)
	writeGroupRecoveryFixture(t, dir, group, secret, now)
	handler, err := NewGroupRecoveryHandler(GroupRecoveryHandlerConfig{Store: store, Signer: signer, GroupIDs: []string{group}, KeyringDir: dir, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	request := func(target string, current GroupAuthorityState) *httptest.ResponseRecorder {
		t.Helper()
		r := GroupRecoveryRequest{Schema: GroupRecoveryRequestSchemaV1, KeyID: "recovery-de-1", GroupID: group, ExpectedPublicationSequence: current.Published.PublicationSequence, ExpectedRecoveryEpoch: current.Published.RecoveryEpoch, TargetBundleGeneration: target, IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix(), Nonce: strings.Repeat("n", 24), Reason: "recover code without selecting traffic configuration"}
		if err := SignGroupRecoveryRequest(&r, secret); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(r)
		httpRequest := httptest.NewRequest(http.MethodPost, GroupRecoveryPathV1, bytes.NewReader(raw))
		httpRequest.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httpRequest)
		return recorder
	}
	for _, old := range []GroupAuthorityState{legacy, gray} {
		before, err := os.ReadFile(store.groupStatePath(group))
		if err != nil {
			t.Fatal(err)
		}
		r := request(old.Published.Bundle.Generation, full)
		if r.Code != http.StatusConflict || !strings.Contains(r.Body.String(), "traffic_release_conflict") {
			t.Fatal("historical recovery changed traffic authority", r.Code, r.Body.String())
		}
		after, err := os.ReadFile(store.groupStatePath(group))
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("rejected recovery changed durable state", err)
		}
	}
	// Exercise the store boundary directly with a previously published signed
	// gray bundle. The HTTP layer must not be the sole protection.
	_, candidate, epoch, err := store.ReadGroupRecoveryTarget(ctx, group, gray.Published.Bundle.Generation)
	if err != nil {
		t.Fatal(err)
	}
	bundle := cloneEdgeRouteBundle(*candidate.Bundle)
	bundle.Issuer = groupAuthorityIssuer
	bundle.GeneratedAt = now
	bundle.ValidUntil = time.Time{}
	bundle.KeyID = ""
	bundle.Signature = ""
	bundle.Signatures = nil
	bundle.PreviousGeneration = full.Published.Bundle.Generation
	bundle.Version = groupPublicationVersion(bundle.Generation, full.LedgerHead.Sequence+1, epoch+1)
	signed, err := signer.SignGroupBundle(ctx, group, bundle)
	if err != nil {
		t.Fatal(err)
	}
	entry := GroupAuthorityLedgerEntry{Schema: GroupAuthorityLedgerSchemaV1, GroupID: group, Status: GroupAuthorityStatusPublished, CandidateLedgerSequence: candidate.Sequence, RouteIntentGeneration: candidate.RouteIntentGeneration, InventoryGeneration: candidate.InventoryGeneration, BundleGeneration: signed.Generation, LastPublishedBundleGeneration: signed.Generation, PublishedBundleDigest: signedGroupBundleDigest(signed), SigningKeyID: signed.KeyID, RecoveryEpoch: epoch + 1, RecoveryReason: "historical cross-channel recovery", Authority: "edge-control", PublicationEnabled: true, RecordedAt: now}
	if _, err = store.RecoverGroupAuthorityCAS(ctx, group, full.Published.PublicationSequence, epoch, entry, signed); !errors.Is(err, ErrGroupAuthorityTrafficRecovery) {
		t.Fatal("store accepted independent historical traffic rollback", err)
	}
	// Renewing the current configuration is available, including after restart.
	store, err = OpenPersistentGroupStore(store.root)
	if err != nil {
		t.Fatal(err)
	}
	handler, err = NewGroupRecoveryHandler(GroupRecoveryHandlerConfig{Store: store, Signer: signer, GroupIDs: []string{group}, KeyringDir: dir, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	current, err := store.ReadGroupAuthority(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if r := request(current.Published.Bundle.Generation, current); r.Code != http.StatusOK {
		t.Fatal("current traffic renewal rejected", r.Code, r.Body.String())
	}
	renewed, err := store.ReadGroupAuthority(ctx, group)
	if err != nil || !reflect.DeepEqual(renewed.Published.Bundle.TrafficRelease, full.Published.Bundle.TrafficRelease) || renewed.Published.RecoveryEpoch != epoch+1 {
		t.Fatal("renewal changed traffic binding", err)
	}
	// A parent-authorized rollback is a normal new fenced publication, so the
	// protection does not reject an older artifact under fresh authority.
	rollback := snapshot
	rollback.TrafficRelease = trafficbinding.Clone(snapshot.TrafficRelease)
	rollback.TrafficRelease.ReleaseChannel = "full"
	rollback.TrafficRelease.CanaryRuleRef = ""
	rollback.TrafficRelease.EdgeGroupIDs = nil
	rollback.TrafficRelease.ReleaseID = "release-explicit-rollback"
	rollback.TrafficRelease.FencingToken++
	rolled := publish(rollback)
	if rolled.Published.Bundle.TrafficRelease.ReleaseID != "release-explicit-rollback" || rolled.Published.Bundle.TrafficRelease.RouteArtifactSequence != 1 {
		t.Fatal("explicit traffic rollback did not publish")
	}
}
