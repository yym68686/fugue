package edgecontrol

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/trafficbinding"
)

type changingTrafficSource struct {
	snapshots []model.EdgeRouteIntentSnapshot
	calls     int
}

func (s *changingTrafficSource) FetchRouteIntents(context.Context) (model.EdgeRouteIntentSnapshot, error) {
	i := min(s.calls, len(s.snapshots)-1)
	s.calls++
	return s.snapshots[i], nil
}

func TestAuthorityRuntimeRechecksTrafficReleaseAndNeverDowngradesToLegacy(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	group := "edge-group-country-de"
	state, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = state.StoreGroupInventoryCAS(ctx, group, 0, groupInventoryFixture(group, "a", "epoch", "inventory", false)); err != nil {
		t.Fatal(err)
	}
	dir := privateFixtureDir(t)
	writeGroupSigningFixture(t, dir, group, bytes.Repeat([]byte{0x33}, 32), now)
	signer, err := NewProjectedGroupBundleSigner(dir, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := routeIntentFixture()
	source := &changingTrafficSource{snapshots: []model.EdgeRouteIntentSnapshot{snapshot}}
	runtime := &AuthorityRuntime{RouteIntents: source, Compiler: GroupShadowCompiler{Inventory: state, Ledger: state, Now: func() time.Time { return now }}, Publisher: GroupAuthorityPublisher{Store: state, Signer: signer, Now: func() time.Time { return now }}, GroupIDs: []string{group}}
	first, _, err := runtime.RunOnce(ctx)
	if err != nil || first.Published.Published != 1 {
		t.Fatalf("baseline: %+v %v", first, err)
	}
	before, err := state.ReadGroupAuthority(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	d := "sha256:" + strings.Repeat("a", 64)
	b := &model.TrafficReleaseBinding{Schema: trafficbinding.Schema, ReleaseSetID: "parent", ReleaseSetDigest: d, ReleaseSetGeneration: "parent-gen", RouteArtifactID: "route", RouteArtifactDigest: d, RouteArtifactGeneration: snapshot.Generation, RouteArtifactSequence: 1, ReleaseID: "release", ReleaseChannel: "gray", FencingToken: 4, ScopeKey: "global", IntentDigest: d, PolicyDigest: d, InputSnapshotDigest: d, CompilerVersion: "compiler", ProjectionDigest: trafficbinding.ProjectionDigest(snapshot), CanaryRuleRef: "cohort=first", EdgeGroupIDs: []string{group}}
	snapshot.TrafficRelease = b
	superseded := snapshot
	superseded.TrafficRelease = trafficbinding.Clone(b)
	superseded.TrafficRelease.FencingToken++
	source.snapshots, source.calls = []model.EdgeRouteIntentSnapshot{snapshot, superseded}, 0
	if _, _, err = runtime.RunOnce(ctx); err == nil || !strings.Contains(err.Error(), "changed before") {
		t.Fatal("superseded release published", err)
	}
	after, err := state.ReadGroupAuthority(ctx, group)
	if err != nil || after.Published.Bundle.Version != before.Published.Bundle.Version {
		t.Fatal("failed confirmation changed serving")
	}
	source.snapshots, source.calls = []model.EdgeRouteIntentSnapshot{snapshot}, 0
	active, _, err := runtime.RunOnce(ctx)
	if err != nil || active.Published.Published != 1 {
		t.Fatalf("bound release: %+v %v", active, err)
	}
	before, err = state.ReadGroupAuthority(ctx, group)
	if err != nil || before.Published.Bundle.TrafficRelease == nil {
		t.Fatal("missing serving provenance")
	}
	legacy := snapshot
	legacy.TrafficRelease = nil
	source.snapshots, source.calls = []model.EdgeRouteIntentSnapshot{legacy}, 0
	failed, _, err := runtime.RunOnce(ctx)
	if err != nil || failed.Published.Failed != 1 {
		t.Fatalf("legacy downgrade: %+v %v", failed, err)
	}
	after, err = state.ReadGroupAuthority(ctx, group)
	if err != nil || after.Published.Bundle.Version != before.Published.Bundle.Version {
		t.Fatal("legacy source replaced bound serving artifact")
	}
}

func TestAuthorityRuntimeSkipsStableInputsUntilRefresh(t *testing.T) {
	runtime := &AuthorityRuntime{
		lastRouteIntentGen:  "route-1",
		lastInventoryDigest: "inventory-1",
		lastInventoryKnown:  true,
		lastReconcileAt:     time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC),
		hasBatch:            true,
	}

	if runtime.cachedBatchValid("route-1", "inventory-1", true, runtime.lastReconcileAt.Add(4*time.Minute+59*time.Second)) != true {
		t.Fatal("stable inputs should use the cached batch before the refresh interval")
	}
	if runtime.cachedBatchValid("route-1", "inventory-1", true, runtime.lastReconcileAt.Add(authorityRuntimeRefreshInterval)) {
		t.Fatal("cached batch must refresh at the bounded interval")
	}
	if runtime.cachedBatchValid("route-2", "inventory-1", true, runtime.lastReconcileAt.Add(time.Minute)) {
		t.Fatal("route intent changes must force reconciliation")
	}
	if runtime.cachedBatchValid("route-1", "inventory-2", true, runtime.lastReconcileAt.Add(time.Minute)) {
		t.Fatal("inventory changes must force reconciliation")
	}
	if runtime.cachedBatchValid("route-1", "inventory-1", false, runtime.lastReconcileAt.Add(time.Minute)) {
		t.Fatal("unknown inventory must never use a cached batch")
	}
}
