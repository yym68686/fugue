package edgecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func compileInputFixture(t *testing.T, retain bool) (*PersistentGroupStore, string, time.Time, model.EdgeRouteIntentSnapshot) {
	t.Helper()
	group := "edge-group-neutral"
	now := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	s, err := OpenPersistentGroupStoreWithOptions(privateStateDir(t), PersistentGroupStoreOptions{RetainCompileInputs: retain, CompileInputKeyringDir: compileInputTestKeys(t, group)})
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(group, "a", "epoch-a", "inventory-original", false)
	inventory.ObservedAt = now
	if err := s.StoreGroupInventoryCAS(t.Context(), group, 0, inventory); err != nil {
		t.Fatal(err)
	}
	return s, group, now, routeIntentFixture()
}

func compileInputTestKeys(t *testing.T, group string) string {
	t.Helper()
	return originalExportKeys(t, group, "signing-"+strings.TrimPrefix(group, "edge-group-country-")+"-1", bytes.Repeat([]byte{0x73}, 32))
}

func compileInputRun(t *testing.T, s *PersistentGroupStore, group string, now time.Time, intent model.EdgeRouteIntentSnapshot) GroupShadowBatch {
	t.Helper()
	batch, err := (GroupShadowCompiler{Inventory: s, Ledger: s, Now: func() time.Time { return now }, InventoryMaxAge: time.Minute}).Reconcile(t.Context(), intent, []string{group})
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func TestGroupCompileInputOriginalCASRestartAndCompaction(t *testing.T) {
	s, group, now, intent := compileInputFixture(t, true)
	if b := compileInputRun(t, s, group, now, intent); b.Succeeded != 1 {
		t.Fatal(b)
	}
	path := s.groupStatePath(group)
	state, err := s.readGroupState(path, group)
	if err != nil || !state.CompileInputsRequired || len(state.RetentionInputs) != 1 {
		t.Fatal("missing original source", err)
	}
	original := cloneGroupCompileInput(state.RetentionInputs[0])
	if !sameGroupCompileValue(original.RouteIntent, intent) || original.InventoryMaxAgeNanoseconds != int64(time.Minute) || !original.AllowBootstrap || original.Entry.Sequence != 1 {
		t.Fatal("original conditions changed")
	}
	// New heartbeats and intent must not rewrite the first source record.
	inventory := *state.Inventory
	inventory.Sequence++
	inventory.Generation = "inventory-next"
	inventory.ObservedAt = now.Add(time.Hour)
	if err := s.StoreGroupInventoryCAS(t.Context(), group, inventory.Sequence-1, inventory); err != nil {
		t.Fatal(err)
	}
	intent.Generation = "intent-next"
	intent.Routes[0].Generation = "route-next"
	if b := compileInputRun(t, s, group, now.Add(time.Hour), intent); b.Succeeded != 1 {
		t.Fatal(b)
	}
	state, err = s.readGroupState(path, group)
	if err != nil || len(state.RetentionInputs) != 2 || !sameGroupCompileValue(original, state.RetentionInputs[0]) || state.RetentionInputs[1].AllowBootstrap {
		t.Fatal("heartbeat replaced original or changed bootstrap", err)
	}
	// Capture survives archival of the shadow payload, cache loss and reopen
	// with the process option off. Validation replays at the original clock.
	state.Ledger[0].Bundle = nil
	state.Ledger[0].BundleArchived = true
	state.Revision++
	if err := s.writeGroupState(path, state); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenPersistentGroupStore(s.root)
	if err != nil {
		t.Fatal(err)
	}
	state, err = restarted.readGroupState(path, group)
	if err != nil || !sameGroupCompileValue(state.RetentionInputs[0], original) || validateGroupCompileInputs(state) != nil {
		t.Fatal("restart lost source or replay used current clock", err)
	}
	// Mutating a returned clone cannot change the cached original.
	copy := clonePersistentGroupState(state)
	copy.RetentionInputs[0].RouteIntent.Routes[0].Hostname = "mutated.example.invalid"
	copy.RetentionInputs[0].Entry.Bundle.Routes[0].Hostname = "mutated.example.invalid"
	if !sameGroupCompileValue(state.RetentionInputs[0], original) {
		t.Fatal("source clone aliases original")
	}
	before, _ := os.ReadFile(path)
	next := cloneGroupShadowLedgerEntry(state.Ledger[1])
	next.Sequence = 0
	next.compileInput = nil
	if _, err := restarted.AppendCAS(t.Context(), group, 2, next); !errors.Is(err, ErrGroupCompileInput) {
		t.Fatal("old caller bypassed durable capture requirement", err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("refused append changed durable state")
	}
}

func TestGroupCompileInputNoLegacyBackfillOrJSONWitness(t *testing.T) {
	s, group, now, intent := compileInputFixture(t, false)
	compileInputRun(t, s, group, now, intent)
	state, _ := s.readGroupState(s.groupStatePath(group), group)
	if state.CompileInputsRequired || len(state.RetentionInputs) != 0 {
		t.Fatal("default writer changed durable format")
	}
	upgraded, err := OpenPersistentGroupStoreWithOptions(s.root, PersistentGroupStoreOptions{RetainCompileInputs: true, CompileInputKeyringDir: s.compileInputKeyringDir})
	if err != nil {
		t.Fatal(err)
	}
	// An unchanged successful head is returned without inventing old input.
	compileInputRun(t, upgraded, group, now, intent)
	state, _ = upgraded.readGroupState(s.groupStatePath(group), group)
	if len(state.RetentionInputs) != 0 {
		t.Fatal("unchanged legacy head was backfilled")
	}
	intent.Generation = "intent-after-reader-upgrade"
	compileInputRun(t, upgraded, group, now, intent)
	state, err = upgraded.readGroupState(s.groupStatePath(group), group)
	if err != nil || len(state.RetentionInputs) != 1 || state.RetentionInputs[0].Entry.Sequence != 2 {
		t.Fatal("new sequence did not preserve legacy gap", err)
	}
	entry := state.Ledger[1]
	entry.Sequence = 0
	raw, _ := json.Marshal(entry)
	var decoded GroupShadowLedgerEntry
	if json.Unmarshal(raw, &decoded) != nil || decoded.compileInput != nil {
		t.Fatal("JSON supplied process witness")
	}
	if _, err := upgraded.AppendCAS(t.Context(), group, 2, decoded); !errors.Is(err, ErrGroupCompileInput) {
		t.Fatal("decoded ledger entry admitted as original", err)
	}
}

func TestGroupCompileInputRefusesReboundOrIncompleteOriginal(t *testing.T) {
	s, group, now, intent := compileInputFixture(t, true)
	compileInputRun(t, s, group, now, intent)
	state, _ := s.readGroupState(s.groupStatePath(group), group)
	for _, mode := range []string{"route-body", "route-generation", "inventory-body", "entry", "stale-at-original-time", "previous", "bootstrap", "missing", "flag", "duplicate", "unrecorded-new-entry"} {
		t.Run(mode, func(t *testing.T) {
			bad := clonePersistentGroupState(state)
			x := &bad.RetentionInputs[0]
			switch mode {
			case "route-body":
				x.RouteIntent.Routes[0].Hostname = "wrong.example.invalid"
			case "route-generation":
				x.RouteIntent.Generation = "wrong-original"
			case "inventory-body":
				x.Inventory.ActiveEpoch.FenceSequence++
			case "entry":
				x.Entry.ActiveHealthyInstances++
			case "stale-at-original-time":
				x.Inventory.ObservedAt = now.Add(-time.Hour)
			case "previous":
				x.PreviousSuccessfulGeneration = x.Entry.BundleGeneration
			case "bootstrap":
				x.AllowBootstrap = false
			case "missing":
				bad.RetentionInputs = nil
			case "flag":
				bad.CompileInputsRequired = false
			case "duplicate":
				bad.RetentionInputs = append(bad.RetentionInputs, *x)
			case "unrecorded-new-entry":
				e := bad.Ledger[0]
				e.Sequence++
				bad.Ledger = append(bad.Ledger, e)
			}
			bad.Digest = persistentGroupStateDigest(bad)
			if validatePersistentGroupState(bad, group) == nil {
				t.Fatal("rechecksummed altered source admitted")
			}
		})
	}
}

func TestGroupCompileInputCapacityRefusalKeepsAllOriginals(t *testing.T) {
	s, group, now, intent := compileInputFixture(t, true)
	for i := 0; i < 32; i++ {
		intent.Generation = fmt.Sprintf("intent-%02d", i)
		if batch := compileInputRun(t, s, group, now, intent); batch.Succeeded != 1 {
			t.Fatal("original capture failed", i, batch)
		}
	}
	before, _ := os.ReadFile(s.groupStatePath(group))
	intent.Generation = "intent-over-limit"
	if batch := compileInputRun(t, s, group, now, intent); batch.Succeeded != 0 || batch.Failed != 1 {
		t.Fatal("capture pruned originals to admit new generation", batch)
	}
	after, _ := os.ReadFile(s.groupStatePath(group))
	if !bytes.Equal(before, after) {
		t.Fatal("capacity refusal changed original file")
	}
	state, err := s.readGroupState(s.groupStatePath(group), group)
	if err != nil || len(state.RetentionInputs) != 32 || len(state.Ledger) != 32 {
		t.Fatal("originals lost", err)
	}
}

func TestGroupCompileInputSelectiveOriginalExport(t *testing.T) {
	s, group, now, intent := compileInputFixture(t, true)
	batch := compileInputRun(t, s, group, now, intent)
	signer := &fixtureGroupSigner{keys: map[string][]byte{group: bytes.Repeat([]byte{0x73}, 32)}, validFor: 30 * time.Minute}
	if out, err := (GroupAuthorityPublisher{Store: s, Signer: signer, Now: func() time.Time { return now }}).Publish(t.Context(), batch); err != nil || out.Published != 1 {
		t.Fatal("publish fixture", err)
	}
	state, _ := s.readGroupState(s.groupStatePath(group), group)
	keys := originalExportKeys(t, group, state.Published.Bundle.KeyID, signer.keys[group])
	out, err := ExportGroupOriginalEvidence(t.Context(), s.groupStatePath(group), group, keys, []string{state.Published.Bundle.Generation})
	if err != nil || len(out.Generations[0].CompileInputs) != 1 || out.AdmissionAuthenticated || out.DependencyClosure || out.StateContentsValidated {
		t.Fatal("export lost exact source or promoted diagnostic to admission", err)
	}
	var input GroupCompileInput
	if json.Unmarshal(out.Generations[0].CompileInputs[0], &input) != nil || !reflect.DeepEqual(input, state.RetentionInputs[0]) {
		t.Fatal("export changed original source")
	}
}

type compileInputCaptureLedger struct {
	*PersistentGroupStore
	captured GroupShadowLedgerEntry
}

func (l *compileInputCaptureLedger) AppendCAS(_ context.Context, _ string, _ uint64, entry GroupShadowLedgerEntry) (GroupShadowLedgerEntry, error) {
	l.captured = entry
	return GroupShadowLedgerEntry{}, ErrGroupCompileInput
}

func TestGroupCompileInputCASBindsOriginalHeartbeatNotOnlySemanticHash(t *testing.T) {
	s, group, now, intent := compileInputFixture(t, true)
	ledger := &compileInputCaptureLedger{PersistentGroupStore: s}
	_, err := (GroupShadowCompiler{Inventory: s, Ledger: ledger, Now: func() time.Time { return now }, InventoryMaxAge: time.Minute}).Reconcile(t.Context(), intent, []string{group})
	if err != nil || ledger.captured.compileInput == nil {
		t.Fatal("native compiler did not provide witness", err)
	}
	inventory, _ := s.ReadGroupInventory(t.Context(), group)
	digest := groupInventorySemanticDigest(inventory)
	inventory.Sequence++
	inventory.ObservedAt = now.Add(time.Second)
	if err := s.StoreGroupInventoryCAS(t.Context(), group, inventory.Sequence-1, inventory); err != nil {
		t.Fatal(err)
	}
	if groupInventorySemanticDigest(inventory) != digest {
		t.Fatal("fixture must change only nonsemantic heartbeat fields")
	}
	if _, err := s.AppendCAS(t.Context(), group, 0, ledger.captured); !errors.Is(err, ErrGroupShadowInputCAS) {
		t.Fatal("new heartbeat substituted for original compiler observation", err)
	}
	state, _ := s.readGroupState(s.groupStatePath(group), group)
	if len(state.Ledger) != 0 || len(state.RetentionInputs) != 0 || state.CompileInputsRequired {
		t.Fatal("source drift partially committed")
	}
}

func TestGroupCompileInputReplaysAuthenticatedBootstrapProducer(t *testing.T) {
	group := "cell-original-test"
	now := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	s, err := OpenPersistentGroupStoreWithOptions(privateStateDir(t), PersistentGroupStoreOptions{RetainCompileInputs: true, CompileInputKeyringDir: compileInputTestKeys(t, group)})
	if err != nil {
		t.Fatal(err)
	}
	h := authorityInventoryHeartbeatFixture(group, "node-neutral", 0, 1, now, "compile-original-bootstrap-01")
	serving := false
	instance := &h.Inventory.Instances[0]
	instance.EffectiveHealthy, instance.ServingHealthy = false, &serving
	instance.BootstrapEligibility = &GroupBootstrapEligibility{GroupID: group, ReleaseEpoch: instance.ReleaseEpoch, ProducerGeneration: 1, ValidUntil: now.Add(time.Minute)}
	id := GroupInventoryProducerIdentity{CredentialID: "credential-neutral", TokenID: "token-neutral", NodeID: instance.EdgeID, GroupID: group}
	if _, err := s.StoreGroupInventoryProducerHeartbeat(t.Context(), id, h, now); err != nil {
		t.Fatal(err)
	}
	if batch := compileInputRun(t, s, group, now.Add(time.Second), routeIntentFixture()); batch.Succeeded != 1 {
		t.Fatal(batch)
	}
	state, err := s.readGroupState(s.groupStatePath(group), group)
	if err != nil || len(state.RetentionInputs) != 1 || state.RetentionInputs[0].InventoryProducer == nil || state.RetentionInputs[0].Entry.ActiveBootstrapInstances != 1 {
		t.Fatal("authenticated producer not retained", err)
	}
	input := cloneGroupCompileInput(state.RetentionInputs[0])
	if validateGroupCompileInput(input, group) != nil {
		t.Fatal("original bootstrap could not replay after wire roundtrip")
	}
	for _, mode := range []string{"producer-missing", "producer-instance", "epoch", "receipt-time", "original-bootstrap-deadline"} {
		t.Run(mode, func(t *testing.T) {
			bad := cloneGroupCompileInput(input)
			switch mode {
			case "producer-missing":
				bad.InventoryProducer = nil
			case "producer-instance":
				bad.InventoryProducer.Observations[0].Instance.InstanceUID = "replacement"
			case "epoch":
				bad.InventoryProducer.Observations[0].ServingEpoch.FenceSequence++
			case "receipt-time":
				bad.InventoryProducer.Observations[0].ObservedAt = now.Add(time.Second)
			case "original-bootstrap-deadline":
				bad.Entry.RecordedAt = now.Add(time.Minute)
			}
			if validateGroupCompileInput(bad, group) == nil {
				t.Fatal("changed original producer accepted")
			}
		})
	}
}

func TestGroupCompileInputRecordBoundRefusesWholeCAS(t *testing.T) {
	s, group, now, intent := compileInputFixture(t, true)
	intent.Routes[0].OriginStatusReason = strings.Repeat("x", groupOriginalRecordBytes)
	before, _ := os.ReadFile(s.groupStatePath(group))
	if batch := compileInputRun(t, s, group, now, intent); batch.Succeeded != 0 {
		t.Fatal("oversized input accepted", batch)
	}
	after, _ := os.ReadFile(s.groupStatePath(group))
	if !bytes.Equal(before, after) {
		t.Fatal("oversized input partially committed")
	}
}
