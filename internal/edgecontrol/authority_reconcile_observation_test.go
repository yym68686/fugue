package edgecontrol

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func reconcileSnapshot(t *testing.T, runtime *AuthorityRuntime) map[string]any {
	t.Helper()
	v, err := runtime.ReconcileObservationSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return v.(map[string]any)
}

func TestAuthorityReconcileObservationBoundsPrivacyAndCopies(t *testing.T) {
	runtime := &AuthorityRuntime{}
	source := model.EdgeRouteIntentSnapshot{Generation: "private-generation", TrafficRelease: &model.TrafficReleaseBinding{
		ReleaseSetID: "private-parent", RouteArtifactID: "private-route", ReleaseID: "private-release", ReleaseChannel: "private-channel", FencingToken: 7}}
	for range authorityReconcileLimit + 5 {
		trace := runtime.observations.begin()
		trace.phase("private-phase")
		trace.phase("route_fetch")
		trace.source(source, false)
		trace.phase("route_recheck")
		trace.source(source, true)
		trace.phaseError(errors.New("private-error"))
		trace.finish(AuthorityRuntimeBatch{}, false, errors.New("private-error"))
	}
	runtime.observations.begin().finish(AuthorityRuntimeBatch{Published: GroupAuthorityBatch{Published: 1}}, false, nil)
	got := reconcileSnapshot(t, runtime)
	if got["completed"] != uint64(authorityReconcileLimit+6) || got["failed"] != uint64(authorityReconcileLimit+5) || got["evicted"] != uint64(6) || got["failures_evicted"] != uint64(5) {
		t.Fatal(got)
	}
	rows := got["failures"].([]authorityReconcileReceipt)
	if len(rows) != authorityReconcileLimit || len(rows[0].Phases) != 2 || rows[0].Source.Parent != authorityReconcileHash("private-parent") || rows[0].Source.Channel != "unrecognized" || rows[0].Source.Fence != 7 {
		t.Fatal(rows)
	}
	raw, _ := json.Marshal(got)
	if len(raw) > 128<<10 || strings.Contains(string(raw), "private-") {
		t.Fatal("unbounded or private observation", len(raw))
	}
	rows[0].Phases[0].Name = "mutated-snapshot"
	if reconcileSnapshot(t, runtime)["failures"].([]authorityReconcileReceipt)[0].Phases[0].Name != "route_fetch" {
		t.Fatal("snapshot aliases retained phases")
	}
	if authorityReconcileHash(strings.Repeat("x", 4097)) != "identity_exceeds_budget" {
		t.Fatal("unbounded identity hashing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runtime.ReconcileObservationSnapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestAuthorityReconcileObservationActiveBudgetAndRace(t *testing.T) {
	runtime := &AuthorityRuntime{}
	traces := make([]*authorityReconcileTrace, authorityReconcileActiveLimit)
	for i := range traces {
		traces[i] = runtime.observations.begin()
		traces[i].phase("route_fetch")
	}
	if dropped := runtime.observations.begin(); dropped != nil {
		t.Fatal("active budget exceeded")
	}
	// A dropped trace is deliberately a no-op and cannot alter a decision.
	var dropped *authorityReconcileTrace
	dropped.phase("compile")
	dropped.source(model.EdgeRouteIntentSnapshot{}, false)
	dropped.phaseError(context.Canceled)
	dropped.inputChanged()
	dropped.finish(AuthorityRuntimeBatch{}, false, nil)
	got := reconcileSnapshot(t, runtime)
	if got["dropped"] != uint64(1) || len(got["active"].([]authorityReconcileReceipt)) != authorityReconcileActiveLimit {
		t.Fatal(got)
	}
	var wg sync.WaitGroup
	for _, trace := range traces {
		wg.Go(func() {
			for _, phase := range []string{"inventory_read", "cache_check", "compile", "route_recheck", "publish", "cache_store"} {
				trace.phase(phase)
				trace.source(model.EdgeRouteIntentSnapshot{Generation: "example"}, false)
				trace.phaseError(context.DeadlineExceeded)
			}
			trace.finish(AuthorityRuntimeBatch{}, false, context.Canceled)
		})
	}
	wg.Go(func() {
		for range 100 {
			_, _ = runtime.ReconcileObservationSnapshot(context.Background())
		}
	})
	wg.Wait()
	got = reconcileSnapshot(t, runtime)
	if got["completed"] != uint64(authorityReconcileActiveLimit) || len(got["active"].([]authorityReconcileReceipt)) != 0 {
		t.Fatal(got)
	}
	for _, r := range got["receipts"].([]authorityReconcileReceipt) {
		if len(r.Phases) != 7 || r.Outcome != "failed" || r.ErrorClass != "canceled" || r.FinishedAt.Before(r.StartedAt) {
			t.Fatal(r)
		}
	}
}

type reconcileObservationSource func(context.Context) (model.EdgeRouteIntentSnapshot, error)

func (f reconcileObservationSource) FetchRouteIntents(ctx context.Context) (model.EdgeRouteIntentSnapshot, error) {
	return f(ctx)
}

func TestAuthorityReconcileObservationOriginalErrorsAndCache(t *testing.T) {
	wantErr := errors.New("private-source-error")
	runtime := &AuthorityRuntime{RouteIntents: reconcileObservationSource(func(context.Context) (model.EdgeRouteIntentSnapshot, error) {
		return model.EdgeRouteIntentSnapshot{}, wantErr
	})}
	if _, skipped, err := runtime.RunOnce(context.Background()); err != wantErr || skipped {
		t.Fatal("observation changed original failure", err, skipped)
	}
	r := reconcileSnapshot(t, runtime)["receipts"].([]authorityReconcileReceipt)[0]
	if r.Outcome != "failed" || len(r.Phases) != 1 || r.Phases[0].Name != "route_fetch" || r.ErrorClass != "runtime_failed" {
		t.Fatal(r)
	}
	now := time.Now().UTC()
	state, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	const group = "cell-observation"
	inventory := groupInventoryFixture(group, "a", "epoch", "inventory", false)
	if err := state.StoreGroupInventoryCAS(context.Background(), group, 0, inventory); err != nil {
		t.Fatal(err)
	}
	stored, err := state.ReadGroupInventory(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := routeIntentFixture()
	wantBatch := AuthorityRuntimeBatch{Published: GroupAuthorityBatch{Published: 2, RouteIntentGeneration: "example-generation"}}
	runtime = &AuthorityRuntime{RouteIntents: &changingTrafficSource{snapshots: []model.EdgeRouteIntentSnapshot{snapshot}},
		Compiler: GroupShadowCompiler{Inventory: state}, GroupIDs: []string{group}, hasBatch: true, lastBatch: wantBatch,
		lastRouteIntentGen: routeIntentSemanticDigest(snapshot), lastInventoryDigest: groupInventorySemanticDigest(stored), lastInventoryKnown: true, lastReconcileAt: now}
	if got, skipped, err := runtime.RunOnce(context.Background()); err != nil || !skipped || !reflect.DeepEqual(got, wantBatch) {
		t.Fatal("cache decision changed", got, skipped, err)
	}
	r = reconcileSnapshot(t, runtime)["receipts"].([]authorityReconcileReceipt)[0]
	if r.Outcome != "cached" || len(r.Phases) != 3 || r.Published != 2 {
		t.Fatal(r)
	}
}
