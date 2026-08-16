package edgecontrol

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestGroupShadowCompilerIsolatesMixedGroupFailures(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 4, 9, 30, 0, 0, time.UTC)
	reader := &shadowInventoryReader{
		snapshots: map[string]GroupInventorySnapshot{
			"edge-group-country-us": groupInventoryFixture("edge-group-country-us", "b", "epoch-us-b", "inventory-us-7", true),
			"edge-group-country-de": groupInventoryFixture("edge-group-country-de", "b", "epoch-de-b", "inventory-de-9", false),
			"edge-group-country-hk": {
				Schema:     GroupInventorySchemaV1,
				GroupID:    "edge-group-country-hk",
				Sequence:   1,
				Generation: "inventory-hk-legacy",
			},
		},
		errors: map[string]error{
			"edge-group-country-jp": errors.New("simulated isolated inventory store failure"),
		},
	}
	ledger := NewMemoryGroupShadowLedger()
	compiler := GroupShadowCompiler{Inventory: reader, Ledger: ledger, Now: func() time.Time { return now }}

	batch, err := compiler.Reconcile(context.Background(), routeIntentFixture(), []string{
		"edge-group-country-jp",
		"edge-group-country-us",
		"edge-group-country-hk",
		"edge-group-country-de",
		"edge-group-country-us",
	})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if batch.Schema != GroupShadowBatchSchemaV1 || batch.RouteIntentGeneration != "route-intents-42" || batch.Succeeded != 2 || batch.Failed != 2 {
		t.Fatalf("unexpected batch: %+v", batch)
	}
	if got, want := shadowResultGroups(batch.Results), []string{
		"edge-group-country-de",
		"edge-group-country-hk",
		"edge-group-country-jp",
		"edge-group-country-us",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("result groups = %v, want %v", got, want)
	}

	byGroup := shadowResultsByGroup(batch.Results)
	for _, groupID := range []string{"edge-group-country-us", "edge-group-country-de"} {
		result := byGroup[groupID]
		if result.Status != GroupShadowStatusCompiled || result.FailureCode != "" || result.BundleGeneration == "" || result.LedgerSequence != 1 {
			t.Fatalf("group %s did not compile independently: %+v", groupID, result)
		}
		history := ledger.History(groupID)
		if len(history) != 1 || history[0].Bundle == nil || history[0].Status != GroupShadowStatusCompiled {
			t.Fatalf("group %s ledger = %+v", groupID, history)
		}
		bundle := *history[0].Bundle
		if bundle.EdgeGroupID != groupID || bundle.Version != result.BundleGeneration || bundle.Generation != result.BundleGeneration {
			t.Fatalf("group %s bundle identity = %+v", groupID, bundle)
		}
		if history[0].Authority != "none" || history[0].PublicationEnabled || bundle.Signature != "" || len(bundle.Signatures) != 0 {
			t.Fatalf("group %s shadow candidate gained authority: entry=%+v bundle=%+v", groupID, history[0], bundle)
		}
		wantRoutes := 1
		if groupID == "edge-group-country-de" {
			wantRoutes = 2
		}
		if len(bundle.Routes) != wantRoutes {
			t.Fatalf("group %s routes = %d, want %d: %+v", groupID, len(bundle.Routes), wantRoutes, bundle.Routes)
		}
		for _, route := range bundle.Routes {
			if route.EdgeGroupID != groupID || route.SelectedEdgeGroup != groupID || route.Status == model.EdgeRouteStatusUnavailable || route.HealthyEdgeNodeCount != 1 {
				t.Fatalf("group %s route crossed inventory boundary: %+v", groupID, route)
			}
		}
	}

	if got := byGroup["edge-group-country-hk"].FailureCode; got != GroupShadowFailureInventoryInvalid {
		t.Fatalf("HK failure code = %q, want %q", got, GroupShadowFailureInventoryInvalid)
	}
	if got := byGroup["edge-group-country-jp"].FailureCode; got != GroupShadowFailureInventoryRead {
		t.Fatalf("JP failure code = %q, want %q", got, GroupShadowFailureInventoryRead)
	}
	for _, groupID := range []string{"edge-group-country-hk", "edge-group-country-jp"} {
		history := ledger.History(groupID)
		if len(history) != 1 || history[0].Status != GroupShadowStatusFailed || history[0].Bundle != nil || history[0].FailureCode == "" {
			t.Fatalf("failed group %s ledger = %+v", groupID, history)
		}
	}
	if history := ledger.History("edge-group-country-us"); len(history) != 1 || history[0].ActiveHealthyInstances != 1 {
		t.Fatalf("inactive A overrode active B identity: %+v", history)
	}
}

func TestGroupShadowCompilerKeepsLastSuccessAfterGroupFailure(t *testing.T) {
	t.Parallel()

	groupID := "edge-group-country-us"
	reader := &shadowInventoryReader{snapshots: map[string]GroupInventorySnapshot{
		groupID: groupInventoryFixture(groupID, "b", "epoch-us-b", "inventory-us-1", false),
	}}
	ledger := NewMemoryGroupShadowLedger()
	compiler := GroupShadowCompiler{Inventory: reader, Ledger: ledger, Now: func() time.Time {
		return time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC)
	}}

	first, err := compiler.Reconcile(context.Background(), routeIntentFixture(), []string{groupID})
	if err != nil || first.Succeeded != 1 || len(first.Results) != 1 {
		t.Fatalf("initial Reconcile() = %+v, %v", first, err)
	}
	firstGeneration := first.Results[0].BundleGeneration
	reader.errors = map[string]error{groupID: errors.New("inventory unavailable")}

	second, err := compiler.Reconcile(context.Background(), routeIntentFixture(), []string{groupID})
	if err != nil {
		t.Fatalf("failed Reconcile() error = %v", err)
	}
	if second.Failed != 1 || second.Results[0].FailureCode != GroupShadowFailureInventoryRead || second.Results[0].LastSuccessfulBundleGeneration != firstGeneration {
		t.Fatalf("failed reconcile lost LKG evidence: %+v", second)
	}
	history := ledger.History(groupID)
	if len(history) != 2 || history[0].Bundle == nil || history[1].Bundle != nil || history[1].LastSuccessfulBundleGeneration != firstGeneration {
		t.Fatalf("group ledger did not preserve last success: %+v", history)
	}
}

func TestGroupShadowCompilerLedgerCASFailureIsGroupScoped(t *testing.T) {
	t.Parallel()

	reader := &shadowInventoryReader{snapshots: map[string]GroupInventorySnapshot{
		"edge-group-country-us": groupInventoryFixture("edge-group-country-us", "b", "epoch-us-b", "inventory-us-1", false),
		"edge-group-country-de": groupInventoryFixture("edge-group-country-de", "b", "epoch-de-b", "inventory-de-1", false),
	}}
	inner := NewMemoryGroupShadowLedger()
	ledger := &rejectingShadowLedger{GroupShadowLedger: inner, rejectGroup: "edge-group-country-de"}
	compiler := GroupShadowCompiler{Inventory: reader, Ledger: ledger}

	batch, err := compiler.Reconcile(context.Background(), routeIntentFixture(), []string{"edge-group-country-us", "edge-group-country-de"})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	byGroup := shadowResultsByGroup(batch.Results)
	if byGroup["edge-group-country-us"].Status != GroupShadowStatusCompiled || len(inner.History("edge-group-country-us")) != 1 {
		t.Fatalf("US was contaminated by DE CAS failure: result=%+v history=%+v", byGroup["edge-group-country-us"], inner.History("edge-group-country-us"))
	}
	if byGroup["edge-group-country-de"].FailureCode != GroupShadowFailureLedgerCAS || len(inner.History("edge-group-country-de")) != 0 {
		t.Fatalf("DE CAS failure was not isolated: result=%+v history=%+v", byGroup["edge-group-country-de"], inner.History("edge-group-country-de"))
	}
}

func TestGroupShadowCompilerRefreshesOnlyConflictedGroupAfterInputCAS(t *testing.T) {
	t.Parallel()

	const (
		usGroup    = "edge-group-country-us"
		deGroup    = "edge-group-country-de"
		thirdGroup = "edge-group-region-test"
	)
	usInitial := groupInventoryFixture(usGroup, "b", "epoch-us-b", "inventory-us-1", false)
	usFresh := groupInventoryFixture(usGroup, "b", "epoch-us-b", "inventory-us-2", false)
	usFresh.Sequence = 2
	reader := &sequencedShadowInventoryReader{snapshots: map[string][]GroupInventorySnapshot{
		usGroup:    {usInitial, usFresh},
		deGroup:    {groupInventoryFixture(deGroup, "b", "epoch-de-b", "inventory-de-1", false)},
		thirdGroup: {groupInventoryFixture(thirdGroup, "b", "epoch-third-b", "inventory-third-1", false)},
	}}
	inner := NewMemoryGroupShadowLedger()
	ledger := &scriptedShadowLedger{
		GroupShadowLedger: inner,
		errors: map[string][]error{
			usGroup: {ErrGroupShadowInputCAS},
		},
	}
	compiler := GroupShadowCompiler{Inventory: reader, Ledger: ledger}

	batch, err := compiler.Reconcile(context.Background(), routeIntentFixture(), []string{usGroup, deGroup, thirdGroup})
	if err != nil || batch.Succeeded != 3 || batch.Failed != 0 {
		t.Fatalf("Reconcile() = %+v, %v", batch, err)
	}
	if got := reader.ReadCalls(usGroup); got != 2 {
		t.Fatalf("US inventory reads = %d, want 2", got)
	}
	if got := ledger.AppendCalls(usGroup); got != 2 {
		t.Fatalf("US append calls = %d, want 2", got)
	}
	for _, groupID := range []string{deGroup, thirdGroup} {
		if got := reader.ReadCalls(groupID); got != 1 {
			t.Fatalf("%s inventory reads = %d, want 1", groupID, got)
		}
		if got := ledger.AppendCalls(groupID); got != 1 {
			t.Fatalf("%s append calls = %d, want 1", groupID, got)
		}
	}
	history := inner.History(usGroup)
	if len(history) != 1 || history[0].InventoryGeneration != usFresh.Generation || history[0].InventoryDigest != groupInventorySemanticDigest(usFresh) {
		t.Fatalf("US retry did not bind the fresh exact inventory: %+v", history)
	}
	for _, groupID := range []string{deGroup, thirdGroup} {
		if history := inner.History(groupID); len(history) != 1 || history[0].Sequence != 1 {
			t.Fatalf("%s ledger was contaminated by US retry: %+v", groupID, history)
		}
	}
}

func TestGroupShadowCompilerCASRefreshIsBoundedAndExact(t *testing.T) {
	t.Parallel()

	const groupID = "edge-group-region-test"
	newCompiler := func(script []error) (GroupShadowCompiler, *sequencedShadowInventoryReader, *scriptedShadowLedger) {
		reader := &sequencedShadowInventoryReader{snapshots: map[string][]GroupInventorySnapshot{
			groupID: {
				groupInventoryFixture(groupID, "b", "epoch-test-b", "inventory-test-1", false),
				groupInventoryFixture(groupID, "b", "epoch-test-b", "inventory-test-2", false),
			},
		}}
		ledger := &scriptedShadowLedger{GroupShadowLedger: NewMemoryGroupShadowLedger(), errors: map[string][]error{groupID: script}}
		return GroupShadowCompiler{Inventory: reader, Ledger: ledger}, reader, ledger
	}

	t.Run("persistent exact CAS conflict stops after one refresh", func(t *testing.T) {
		compiler, reader, ledger := newCompiler([]error{ErrGroupShadowInputCAS, ErrGroupShadowLedgerConflict})
		batch, err := compiler.Reconcile(context.Background(), routeIntentFixture(), []string{groupID})
		if err != nil || batch.Failed != 1 || batch.Results[0].FailureCode != GroupShadowFailureLedgerCAS {
			t.Fatalf("Reconcile() = %+v, %v", batch, err)
		}
		if got := reader.ReadCalls(groupID); got != groupShadowCASAttempts {
			t.Fatalf("inventory reads = %d, want %d", got, groupShadowCASAttempts)
		}
		if got := ledger.AppendCalls(groupID); got != groupShadowCASAttempts {
			t.Fatalf("append calls = %d, want %d", got, groupShadowCASAttempts)
		}
	})

	t.Run("non CAS failure is not retried", func(t *testing.T) {
		compiler, reader, ledger := newCompiler([]error{errors.New("durable ledger unavailable")})
		batch, err := compiler.Reconcile(context.Background(), routeIntentFixture(), []string{groupID})
		if err != nil || batch.Failed != 1 || batch.Results[0].FailureCode != GroupShadowFailureLedgerCAS {
			t.Fatalf("Reconcile() = %+v, %v", batch, err)
		}
		if got := reader.ReadCalls(groupID); got != 1 {
			t.Fatalf("inventory reads = %d, want 1", got)
		}
		if got := ledger.AppendCalls(groupID); got != 1 {
			t.Fatalf("append calls = %d, want 1", got)
		}
	})
}

func TestGroupShadowCompilerCandidateGenerationIsCanonical(t *testing.T) {
	t.Parallel()

	groupID := "edge-group-country-de"
	firstIntent := routeIntentFixture()
	secondIntent := routeIntentFixture()
	secondIntent.GeneratedAt = secondIntent.GeneratedAt.Add(12 * time.Hour)
	sort.Slice(secondIntent.Routes, func(i, j int) bool { return secondIntent.Routes[i].Hostname > secondIntent.Routes[j].Hostname })
	sort.Slice(secondIntent.TLSAllowlist, func(i, j int) bool {
		return secondIntent.TLSAllowlist[i].Hostname > secondIntent.TLSAllowlist[j].Hostname
	})
	firstInventory := groupInventoryFixture(groupID, "b", "epoch-de-b", "inventory-de-1", true)
	secondInventory := groupInventoryFixture(groupID, "b", "epoch-de-b", "inventory-de-1", true)
	secondInventory.ObservedAt = secondInventory.ObservedAt.Add(24 * time.Hour)
	sort.Slice(secondInventory.Instances, func(i, j int) bool { return secondInventory.Instances[i].Slot > secondInventory.Instances[j].Slot })

	compile := func(intent model.EdgeRouteIntentSnapshot, inventory GroupInventorySnapshot, now time.Time) GroupShadowResult {
		t.Helper()
		ledger := NewMemoryGroupShadowLedger()
		compiler := GroupShadowCompiler{
			Inventory: &shadowInventoryReader{snapshots: map[string]GroupInventorySnapshot{groupID: inventory}},
			Ledger:    ledger,
			Now:       func() time.Time { return now },
		}
		batch, err := compiler.Reconcile(context.Background(), intent, []string{groupID})
		if err != nil || len(batch.Results) != 1 || batch.Results[0].Status != GroupShadowStatusCompiled {
			t.Fatalf("Reconcile() = %+v, %v", batch, err)
		}
		return batch.Results[0]
	}

	first := compile(firstIntent, firstInventory, time.Date(2026, 8, 4, 11, 0, 0, 0, time.UTC))
	second := compile(secondIntent, secondInventory, time.Date(2026, 8, 5, 11, 0, 0, 0, time.UTC))
	if first.BundleGeneration != second.BundleGeneration || first.InputDigest != second.InputDigest {
		t.Fatalf("transport order/time changed candidate identity: first=%+v second=%+v", first, second)
	}
}

func TestGroupShadowCompilerRetainsRoutesDuringTrafficExclusion(t *testing.T) {
	t.Parallel()

	const groupID = "edge-group-country-us"
	intent := routeIntentFixture()
	intent.Routes[0].ExcludedEdgeIDs = []string{"edge-" + groupID}
	intent.Routes[0].ExcludedEdgeGroupIDs = []string{groupID}
	intent.Routes[0].ExclusionReason = "operator traffic drain"

	ledger := NewMemoryGroupShadowLedger()
	compiler := GroupShadowCompiler{
		Inventory: &shadowInventoryReader{snapshots: map[string]GroupInventorySnapshot{
			groupID: groupInventoryFixture(groupID, "b", "epoch-us-b", "inventory-us-1", false),
		}},
		Ledger: ledger,
	}
	batch, err := compiler.Reconcile(context.Background(), intent, []string{groupID})
	if err != nil || batch.Succeeded != 1 || len(batch.Results) != 1 {
		t.Fatalf("Reconcile() = %+v, %v", batch, err)
	}
	history := ledger.History(groupID)
	if len(history) != 1 || history[0].Bundle == nil {
		t.Fatalf("expected compiled route bundle, got %+v", history)
	}
	bundle := history[0].Bundle
	if len(bundle.Routes) != 1 {
		t.Fatalf("traffic exclusion removed the active route: %+v", bundle.Routes)
	}
	route := bundle.Routes[0]
	if route.Hostname != "all.example.test" || route.Status != model.EdgeRouteStatusActive || !routeHasUpstream(route) {
		t.Fatalf("traffic exclusion changed serving route material: %+v", route)
	}
	if route.HealthyEdgeNodeCount != 1 || len(route.ExcludedEdgeIDs) != 1 || len(route.ExcludedEdgeGroupIDs) != 1 {
		t.Fatalf("traffic drain metadata or healthy inventory was lost: %+v", route)
	}
	if len(bundle.TLSAllowlist) != 1 || bundle.TLSAllowlist[0].Hostname != route.Hostname {
		t.Fatalf("traffic exclusion removed TLS readiness for retained route: %+v", bundle.TLSAllowlist)
	}
}

func TestMemoryGroupShadowLedgerRejectsCASAndLKGForgery(t *testing.T) {
	t.Parallel()

	groupID := "edge-group-country-us"
	ledger := NewMemoryGroupShadowLedger()
	compiler := GroupShadowCompiler{
		Inventory: &shadowInventoryReader{snapshots: map[string]GroupInventorySnapshot{
			groupID: groupInventoryFixture(groupID, "b", "epoch-us-b", "inventory-us-1", false),
		}},
		Ledger: ledger,
		Now: func() time.Time {
			return time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
		},
	}
	batch, err := compiler.Reconcile(context.Background(), routeIntentFixture(), []string{groupID})
	if err != nil || batch.Succeeded != 1 {
		t.Fatalf("initial Reconcile() = %+v, %v", batch, err)
	}
	head, exists, err := ledger.Head(context.Background(), groupID)
	if err != nil || !exists || head.Sequence != 1 || head.Bundle == nil {
		t.Fatalf("initial ledger head = %+v, exists=%v, err=%v", head, exists, err)
	}
	lastSuccess := head.BundleGeneration

	stale := GroupShadowLedgerEntry{
		Schema: GroupShadowLedgerSchemaV1, GroupID: groupID, Status: GroupShadowStatusFailed,
		RouteIntentGeneration: head.RouteIntentGeneration, InputDigest: head.InputDigest,
		LastSuccessfulBundleGeneration: lastSuccess, FailureCode: GroupShadowFailureCompile,
		Authority: "none", RecordedAt: head.RecordedAt.Add(time.Minute),
	}
	if _, err := ledger.AppendCAS(context.Background(), groupID, 0, stale); !errors.Is(err, ErrGroupShadowLedgerConflict) {
		t.Fatalf("stale group CAS error = %v, want %v", err, ErrGroupShadowLedgerConflict)
	}

	rollback := stale
	rollback.LastSuccessfulBundleGeneration = ""
	if _, err := ledger.AppendCAS(context.Background(), groupID, 1, rollback); err == nil || errors.Is(err, ErrGroupShadowLedgerConflict) {
		t.Fatalf("forged LKG rollback error = %v", err)
	}
	after, exists, err := ledger.Head(context.Background(), groupID)
	if err != nil || !exists || after.Sequence != 1 || after.BundleGeneration != lastSuccess || after.LastSuccessfulBundleGeneration != lastSuccess {
		t.Fatalf("rejected writes changed ledger head: %+v, exists=%v, err=%v", after, exists, err)
	}

	head.Bundle.Routes[0].Hostname = "forged.example.test"
	defensive, _, _ := ledger.Head(context.Background(), groupID)
	if defensive.Bundle == nil || defensive.Bundle.Routes[0].Hostname == "forged.example.test" {
		t.Fatalf("ledger head exposed mutable bundle alias: %+v", defensive)
	}
}

func TestGroupShadowCompilerRejectsInvalidGlobalIntentWithoutLedgerWrites(t *testing.T) {
	t.Parallel()

	ledger := NewMemoryGroupShadowLedger()
	compiler := GroupShadowCompiler{
		Inventory: &shadowInventoryReader{snapshots: map[string]GroupInventorySnapshot{
			"edge-group-country-us": groupInventoryFixture("edge-group-country-us", "b", "epoch-us-b", "inventory-us-1", false),
		}},
		Ledger: ledger,
	}
	intent := routeIntentFixture()
	intent.SchemaVersion = "edge-route-intent/v0"

	if _, err := compiler.Reconcile(context.Background(), intent, []string{"edge-group-country-us"}); err == nil {
		t.Fatal("invalid global RouteIntent unexpectedly reconciled")
	}
	if history := ledger.History("edge-group-country-us"); len(history) != 0 {
		t.Fatalf("invalid global RouteIntent wrote ledger: %+v", history)
	}
}

type shadowInventoryReader struct {
	snapshots map[string]GroupInventorySnapshot
	errors    map[string]error
}

func (r *shadowInventoryReader) ReadGroupInventory(_ context.Context, groupID string) (GroupInventorySnapshot, error) {
	if err := r.errors[groupID]; err != nil {
		return GroupInventorySnapshot{}, err
	}
	return r.snapshots[groupID], nil
}

type rejectingShadowLedger struct {
	GroupShadowLedger
	rejectGroup string
}

type sequencedShadowInventoryReader struct {
	mu        sync.Mutex
	snapshots map[string][]GroupInventorySnapshot
	readCalls map[string]int
}

func (r *sequencedShadowInventoryReader) ReadGroupInventory(_ context.Context, groupID string) (GroupInventorySnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readCalls == nil {
		r.readCalls = make(map[string]int)
	}
	index := r.readCalls[groupID]
	r.readCalls[groupID]++
	snapshots := r.snapshots[groupID]
	if len(snapshots) == 0 {
		return GroupInventorySnapshot{}, errors.New("inventory fixture is missing")
	}
	if index >= len(snapshots) {
		index = len(snapshots) - 1
	}
	return snapshots[index], nil
}

func (r *sequencedShadowInventoryReader) ReadCalls(groupID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readCalls[groupID]
}

type scriptedShadowLedger struct {
	GroupShadowLedger
	mu          sync.Mutex
	errors      map[string][]error
	appendCalls map[string]int
}

func (l *scriptedShadowLedger) AppendCAS(ctx context.Context, groupID string, expectedSequence uint64, entry GroupShadowLedgerEntry) (GroupShadowLedgerEntry, error) {
	l.mu.Lock()
	if l.appendCalls == nil {
		l.appendCalls = make(map[string]int)
	}
	call := l.appendCalls[groupID]
	l.appendCalls[groupID]++
	var scripted error
	if call < len(l.errors[groupID]) {
		scripted = l.errors[groupID][call]
	}
	l.mu.Unlock()
	if scripted != nil {
		return GroupShadowLedgerEntry{}, scripted
	}
	return l.GroupShadowLedger.AppendCAS(ctx, groupID, expectedSequence, entry)
}

func (l *scriptedShadowLedger) AppendCalls(groupID string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendCalls[groupID]
}

func (l *rejectingShadowLedger) AppendCAS(ctx context.Context, groupID string, expectedSequence uint64, entry GroupShadowLedgerEntry) (GroupShadowLedgerEntry, error) {
	if groupID == l.rejectGroup {
		return GroupShadowLedgerEntry{}, ErrGroupShadowLedgerConflict
	}
	return l.GroupShadowLedger.AppendCAS(ctx, groupID, expectedSequence, entry)
}

func groupInventoryFixture(groupID, slot, releaseEpoch, generation string, includeInactiveSameEdgeID bool) GroupInventorySnapshot {
	now := time.Date(2026, 8, 4, 9, 0, 0, 0, time.UTC)
	active := GroupInstance{
		EdgeID:           "edge-" + groupID,
		GroupID:          groupID,
		Slot:             slot,
		InstanceUID:      "uid-" + groupID + "-" + slot,
		ReleaseEpoch:     releaseEpoch,
		EffectiveHealthy: true,
		NodeHealthy:      true,
		NodeStatus:       model.EdgeHealthHealthy,
	}
	instances := []GroupInstance{active}
	if includeInactiveSameEdgeID {
		inactive := active
		inactive.Slot = "a"
		inactive.InstanceUID = ""
		inactive.ReleaseEpoch = ""
		inactive.FailureClass = model.EdgeInstanceFailureSignatureInvalid
		instances = append([]GroupInstance{inactive}, instances...)
	}
	return GroupInventorySnapshot{
		Schema:     GroupInventorySchemaV1,
		GroupID:    groupID,
		Sequence:   1,
		Generation: generation,
		ActiveEpoch: GroupActiveEpoch{
			GroupID: groupID, Slot: slot, ReleaseEpoch: releaseEpoch, FenceSequence: 7, MinHealthyInstances: 1,
		},
		Instances:  instances,
		ObservedAt: now,
	}
}

func routeIntentFixture() model.EdgeRouteIntentSnapshot {
	now := time.Date(2026, 8, 4, 8, 0, 0, 0, time.UTC)
	return model.EdgeRouteIntentSnapshot{
		SchemaVersion: model.EdgeRouteIntentSchemaVersionV1,
		Generation:    "route-intents-42",
		GeneratedAt:   now,
		Routes: []model.EdgeRouteIntent{
			{
				Generation: "route-all-1", Hostname: "all.example.test", PathPrefix: "/", RouteKind: model.EdgeRouteKindPlatform,
				TargetGroupMode: model.EdgeRouteIntentGroupModeAllGroups, MinHealthyEdgeNodes: 1, RoutePolicy: model.EdgeRoutePolicyEnabled,
				UpstreamKind: model.EdgeRouteUpstreamKindMesh, UpstreamScope: model.EdgeRouteUpstreamScopeMesh,
				UpstreamURL: "http://runtime.mesh:8080", ServicePort: 8080, TLSPolicy: model.EdgeRouteTLSPolicyPlatform,
				Streaming: true, OriginStatus: model.EdgeRouteStatusActive, CreatedAt: now, UpdatedAt: now,
			},
			{
				Generation: "route-de-1", Hostname: "de.example.test", PathPrefix: "/v1", RouteKind: model.EdgeRouteKindPlatformRoute,
				TargetGroupMode: model.EdgeRouteIntentGroupModePinnedGroup, PinnedEdgeGroupID: "edge-group-country-de",
				MinHealthyEdgeNodes: 1, RoutePolicy: model.EdgeRoutePolicyEnabled,
				UpstreamKind: model.EdgeRouteUpstreamKindMesh, UpstreamScope: model.EdgeRouteUpstreamScopeMesh,
				UpstreamURL: "http://runtime-de.mesh:8080", ServicePort: 8080, TLSPolicy: model.EdgeRouteTLSPolicyPlatform,
				Streaming: true, OriginStatus: model.EdgeRouteStatusActive, CreatedAt: now, UpdatedAt: now,
			},
		},
		TLSAllowlist: []model.EdgeTLSAllowlistEntry{
			{Hostname: "de.example.test", Status: model.EdgeRouteStatusActive, TLSStatus: model.EdgeTLSStatusReady},
			{Hostname: "all.example.test", Status: model.EdgeRouteStatusActive, TLSStatus: model.EdgeTLSStatusReady},
		},
	}
}

func shadowResultsByGroup(results []GroupShadowResult) map[string]GroupShadowResult {
	out := make(map[string]GroupShadowResult, len(results))
	for _, result := range results {
		out[result.GroupID] = result
	}
	return out
}

func shadowResultGroups(results []GroupShadowResult) []string {
	out := make([]string, 0, len(results))
	for _, result := range results {
		out = append(out, result.GroupID)
	}
	return out
}
