package edgecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"fugue/internal/model"
)

const groupCompileInputSchema = "fugue.group-compile-input.v1"

var ErrGroupCompileInput = errors.New("edge-control original compile input unavailable")

// GroupCompileInput is the exact input to one successful native CAS, including
// original time, freshness policy and authenticated producer projection.
// Optional native attestation authenticates the complete input under current
// group trust. Custody and the complete external registry must still be
// independently admitted before a dependency census can accept it.
// New capture is opt-in, bounded and append-only. Historical entries are never
// backfilled from a newer heartbeat, current route intent or renewed signature.
type GroupCompileInput struct {
	Schema                       string                        `json:"schema"`
	Entry                        GroupShadowLedgerEntry        `json:"entry"`
	RouteIntent                  model.EdgeRouteIntentSnapshot `json:"route_intent"`
	Inventory                    GroupInventorySnapshot        `json:"inventory"`
	InventoryProducer            *GroupInventoryProducerState  `json:"inventory_producer,omitempty"`
	AllowLegacySingleProducer    bool                          `json:"allow_legacy_single_producer"`
	InventoryMaxAgeNanoseconds   int64                         `json:"inventory_max_age_nanoseconds"`
	AllowBootstrap               bool                          `json:"allow_bootstrap"`
	PreviousSuccessfulGeneration string                        `json:"previous_successful_generation"`
	Attestation                  *GroupCompileInputAttestation `json:"attestation,omitempty"`
}

// Only the native compiler can supply this process-local witness. In
// particular AppendCAS with a decoded/reconstructed ledger entry cannot turn
// an unsigned operator declaration into original compiler provenance.
type groupCompileInputWitness struct {
	routeIntent     model.EdgeRouteIntentSnapshot
	inventory       GroupInventorySnapshot
	inventoryMaxAge time.Duration
	allowBootstrap  bool
}

func retainGroupCompileInput(ctx context.Context, state *persistentGroupState, entry GroupShadowLedgerEntry, witness *groupCompileInputWitness, allowLegacy bool, keyringDir string) error {
	if witness == nil || state.Inventory == nil || len(state.RetentionInputs) >= 32 {
		return ErrGroupCompileInput
	}
	current := inventoryWithProducer(*state.Inventory, state.InventoryProducer, allowLegacy)
	if current.allowLegacySingleProducer != witness.inventory.allowLegacySingleProducer || !sameGroupCompileValue(current, witness.inventory) || !sameGroupCompileValue(current.verifiedProducer, witness.inventory.verifiedProducer) {
		return ErrGroupShadowInputCAS
	}
	previous := ""
	if len(state.Ledger) > 0 {
		previous = state.Ledger[len(state.Ledger)-1].LastSuccessfulBundleGeneration
	}
	if witness.allowBootstrap != (previous == "" && state.Published == nil) {
		return ErrGroupShadowInputCAS
	}
	entry.compileInput = nil
	input := cloneGroupCompileInput(GroupCompileInput{Schema: groupCompileInputSchema, Entry: entry,
		RouteIntent: witness.routeIntent, Inventory: witness.inventory, InventoryProducer: current.verifiedProducer,
		AllowLegacySingleProducer:  witness.inventory.allowLegacySingleProducer,
		InventoryMaxAgeNanoseconds: int64(witness.inventoryMaxAge), AllowBootstrap: witness.allowBootstrap,
		PreviousSuccessfulGeneration: previous})
	if validateGroupCompileInput(input, state.GroupID) != nil {
		return ErrGroupCompileInput
	}
	// Only this native successful CAS can seal a new source. In particular,
	// loading an old unsigned record never re-signs or backfills that record.
	if err := sealGroupCompileInput(ctx, &input, keyringDir); err != nil {
		return err
	}
	// The exporter and census retain at most 32 records/32MiB, one record at
	// most 8MiB. Refuse the entire CAS before touching durable state. Never
	// prune a protected original to admit another generation.
	total := 0
	for _, original := range append(state.RetentionInputs, input) {
		raw, err := json.Marshal(original)
		if err != nil || len(raw) > groupOriginalRecordBytes || len(raw) > groupOriginalBatchBytes-total {
			return ErrGroupCompileInput
		}
		total += len(raw)
	}
	state.CompileInputsRequired = true
	state.CompileInputAttestationsRequired = true
	state.RetentionInputs = append(state.RetentionInputs, input)
	if validateGroupPublicationOriginals(*state) != nil {
		return ErrGroupCompileInput
	}
	return nil
}

// Replay uses ORIGINAL observed time/bootstrap/freshness and full native
// compilation, not merely equality of the semantic hashes. Replay success is
// a consistency check, not source authentication or permission to reactivate.
func validateGroupCompileInput(input GroupCompileInput, groupID string) error {
	e := input.Entry
	if input.Schema != groupCompileInputSchema || e.GroupID != groupID || e.Schema != GroupShadowLedgerSchemaV1 || e.Sequence == 0 ||
		e.Status != GroupShadowStatusCompiled || e.Authority != "none" || e.PublicationEnabled || e.BundleArchived || e.Bundle == nil || e.FailureCode != "" ||
		e.RecordedAt.IsZero() || input.InventoryMaxAgeNanoseconds < 0 || validateRouteIntentSnapshot(input.RouteIntent) != nil ||
		e.RouteIntentGeneration != strings.TrimSpace(input.RouteIntent.Generation) || e.InventoryGeneration != strings.TrimSpace(input.Inventory.Generation) ||
		input.AllowBootstrap != (input.PreviousSuccessfulGeneration == "") ||
		e.BundleGeneration == "" || e.LastSuccessfulBundleGeneration != e.BundleGeneration {
		return ErrGroupCompileInput
	}
	i := inventoryWithProducer(input.Inventory, input.InventoryProducer, input.AllowLegacySingleProducer)
	if input.InventoryProducer != nil && (validateGroupInventoryProducerState(*input.InventoryProducer, groupID) != nil ||
		i.Generation != groupInventoryProducerGeneration(input.InventoryProducer.Generation, i)) {
		return ErrGroupCompileInput
	}
	maxAge := time.Duration(input.InventoryMaxAgeNanoseconds)
	if maxAge > 0 && (i.ObservedAt.IsZero() || i.ObservedAt.After(e.RecordedAt.Add(maxInventoryHeartbeatClockSkew)) || e.RecordedAt.Sub(i.ObservedAt) > maxAge) {
		return ErrGroupCompileInput
	}
	digest := groupInventorySemanticDigest(i)
	if e.InventoryDigest != digest || e.InputDigest != groupShadowInputDigest(routeIntentSemanticDigest(input.RouteIntent), groupID, digest) {
		return ErrGroupCompileInput
	}
	view, err := validateGroupInventory(groupID, i, input.AllowBootstrap, e.RecordedAt)
	if err != nil || e.ActiveSlot != view.activeEpoch.Slot || e.ActiveReleaseEpoch != view.activeEpoch.ReleaseEpoch ||
		e.ActiveFenceSequence != view.activeEpoch.FenceSequence || e.ActiveHealthyInstances != len(view.servingEdgeIDs) || e.ActiveBootstrapInstances != len(view.bootstrapEdgeIDs) {
		return ErrGroupCompileInput
	}
	bundle, err := compileGroupShadowCandidate(input.RouteIntent, i, digest, view, input.PreviousSuccessfulGeneration, e.RecordedAt)
	if err != nil || e.BundleGeneration != bundle.Generation || !sameGroupCompileValue(e.Bundle, &bundle) {
		return ErrGroupCompileInput
	}
	if input.Attestation != nil && !validGroupCompileInputAttestation(input) {
		return ErrGroupCompileInput
	}
	return nil
}

func validateGroupCompileInputs(state persistentGroupState) error {
	if len(state.RetentionInputs) == 0 {
		if state.CompileInputsRequired || state.CompileInputAttestationsRequired {
			return ErrGroupCompileInput
		}
		return nil // Legacy absence stays absence, never an empty proof.
	}
	if !state.CompileInputsRequired || len(state.RetentionInputs) > 32 {
		return ErrGroupCompileInput
	}
	previous, total := uint64(0), 0
	seen := map[uint64]bool{}
	sealed := false
	for _, input := range state.RetentionInputs {
		seq := input.Entry.Sequence
		raw, err := json.Marshal(input)
		if err != nil || len(raw) > groupOriginalRecordBytes || len(raw) > groupOriginalBatchBytes-total || seq <= previous || seq > uint64(len(state.Ledger)) || validateGroupCompileInput(input, state.GroupID) != nil {
			return ErrGroupCompileInput
		}
		total += len(raw)
		if input.Attestation != nil {
			sealed = true
		} else if sealed {
			return ErrGroupCompileInput
		}
		stored := state.Ledger[seq-1]
		stored.compileInput = nil
		if stored.BundleArchived {
			stored.BundleArchived = false
			stored.Bundle = input.Entry.Bundle
		}
		if !sameGroupCompileValue(stored, input.Entry) || seq == 1 && input.PreviousSuccessfulGeneration != "" || seq > 1 && input.PreviousSuccessfulGeneration != state.Ledger[seq-2].LastSuccessfulBundleGeneration {
			return ErrGroupCompileInput
		}
		seen[seq], previous = true, seq
	}
	if sealed != state.CompileInputAttestationsRequired {
		return ErrGroupCompileInput
	}
	// Once enabled, old append callers and a subsequent reopen with the
	// capture option off cannot create an unrecorded successful generation.
	for _, entry := range state.Ledger[state.RetentionInputs[0].Entry.Sequence-1:] {
		if entry.Status == GroupShadowStatusCompiled && !seen[entry.Sequence] {
			return ErrGroupCompileInput
		}
	}
	return nil
}

func sameGroupCompileValue(a, b any) bool {
	x, ex := json.Marshal(a)
	y, ey := json.Marshal(b)
	return ex == nil && ey == nil && bytes.Equal(x, y)
}

func cloneGroupCompileInput(input GroupCompileInput) GroupCompileInput {
	// Preserve nil/empty and nested pointer semantics through the native typed
	// encoding. This copy includes all fields; no allowlisted subset replaces
	// an original route projection or authenticated producer observation.
	raw, _ := json.Marshal(input)
	var copy GroupCompileInput
	_ = json.Unmarshal(raw, &copy)
	return copy
}
