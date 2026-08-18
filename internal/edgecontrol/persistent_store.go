package edgecontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"fugue/internal/edgeauthority"
	"fugue/internal/model"
)

const (
	persistentGroupStateSchemaV1          = "edge-control-persistent-group-state/v1"
	maxPersistentGroupStateBytes          = 64 << 20
	targetPersistentGroupStateBytes       = 48 << 20
	retainedGroupCandidateBundles         = 8
	persistentGroupStateDigestPlaceholder = "sha256:" + "0000000000000000000000000000000000000000000000000000000000000000"
)

var (
	ErrGroupInventoryCASConflict = errors.New("edge-control group inventory CAS conflict")
	ErrGroupInventorySequence    = errors.New("edge-control group inventory sequence must advance exactly once")
	ErrGroupInventoryNotFound    = errors.New("edge-control group inventory not found")
	ErrGroupShadowInputCAS       = errors.New("edge-control group shadow input CAS conflict")
)

type persistentGroupState struct {
	Schema            string                       `json:"schema"`
	GroupID           string                       `json:"edge_group_id"`
	Revision          uint64                       `json:"revision"`
	Inventory         *GroupInventorySnapshot      `json:"inventory,omitempty"`
	InventoryProducer *GroupInventoryProducerState `json:"inventory_producer,omitempty"`
	Ledger            []GroupShadowLedgerEntry     `json:"ledger,omitempty"`
	AuthorityLedger   []GroupAuthorityLedgerEntry  `json:"authority_ledger,omitempty"`
	Published         *GroupPublishedBundle        `json:"published_bundle,omitempty"`
	Candidate         *GroupCandidateBundle        `json:"candidate_bundle,omitempty"`
	CandidateHistory  []GroupCandidateBundle       `json:"candidate_history,omitempty"`
	Digest            string                       `json:"digest"`
}

// legacyGroupCandidateBundle is the exact durable shape written before
// candidates were bound to the authority-ledger head. It exists only so the
// state reader can verify the old file digest before migrating that one
// missing witness in memory. No writer or HTTP path emits this shape.
type legacyGroupCandidateBundle struct {
	Schema                  string                           `json:"schema"`
	GroupID                 string                           `json:"edge_group_id"`
	Epoch                   uint64                           `json:"epoch"`
	CandidateLedgerSequence uint64                           `json:"candidate_ledger_sequence"`
	RouteIntentGeneration   string                           `json:"route_intent_generation"`
	InventoryGeneration     string                           `json:"inventory_generation"`
	ReleaseRecordDigest     string                           `json:"release_record_digest"`
	WorkerSlot              string                           `json:"worker_slot"`
	PublishedAt             time.Time                        `json:"published_at"`
	CurrentRecord           *edgeauthority.RouteBundleRecord `json:"current_record,omitempty"`
	CurrentBundle           *model.EdgeRouteBundle           `json:"current_bundle,omitempty"`
	CurrentWorkerSlot       string                           `json:"current_worker_slot,omitempty"`
	Record                  edgeauthority.RouteBundleRecord  `json:"record"`
	Bundle                  model.EdgeRouteBundle            `json:"bundle"`
}

type legacyCandidatePersistentGroupState struct {
	Schema            string                       `json:"schema"`
	GroupID           string                       `json:"edge_group_id"`
	Revision          uint64                       `json:"revision"`
	Inventory         *GroupInventorySnapshot      `json:"inventory,omitempty"`
	InventoryProducer *GroupInventoryProducerState `json:"inventory_producer,omitempty"`
	Ledger            []GroupShadowLedgerEntry     `json:"ledger,omitempty"`
	AuthorityLedger   []GroupAuthorityLedgerEntry  `json:"authority_ledger,omitempty"`
	Published         *GroupPublishedBundle        `json:"published_bundle,omitempty"`
	Candidate         *legacyGroupCandidateBundle  `json:"candidate_bundle,omitempty"`
	Digest            string                       `json:"digest"`
}

// PersistentGroupStore owns one checksummed, atomically replaced state file
// and one CAS sequence per edge group. A corrupt group file cannot prevent any
// other group from being read or advanced.
type PersistentGroupStore struct {
	root      string
	summaryMu sync.RWMutex
	summaries map[string]persistentGroupSummary
}

type persistentGroupSummary struct {
	status AuthorityGroupStoreSnapshot
	stage  GroupCandidateStageSnapshot
}

func OpenPersistentGroupStore(root string) (*PersistentGroupStore, error) {
	root = strings.TrimSpace(root)
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, errors.New("edge-control persistent state directory must be an absolute normalized path")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create edge-control persistent state directory: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("inspect edge-control persistent state directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o007 != 0 {
		return nil, errors.New("edge-control persistent state directory must be a non-world-accessible real directory")
	}
	return &PersistentGroupStore{root: root, summaries: make(map[string]persistentGroupSummary)}, nil
}

func (store *PersistentGroupStore) StoreGroupInventoryCAS(ctx context.Context, groupID string, expectedSequence uint64, snapshot GroupInventorySnapshot) error {
	groupID = normalizeGroupID(groupID)
	if groupID == "" || snapshot.Schema != GroupInventorySchemaV1 || normalizeGroupID(snapshot.GroupID) != groupID || snapshot.Sequence == 0 || strings.TrimSpace(snapshot.Generation) == "" {
		return errGroupInventoryInvalid
	}
	snapshot = cloneGroupInventorySnapshot(snapshot)
	return store.withGroupState(ctx, groupID, true, func(state *persistentGroupState) error {
		if state.InventoryProducer != nil {
			return ErrGroupInventoryProducerIdentity
		}
		currentSequence := uint64(0)
		if state.Inventory != nil {
			currentSequence = state.Inventory.Sequence
		}
		if currentSequence != expectedSequence {
			return ErrGroupInventoryCASConflict
		}
		if snapshot.Sequence != currentSequence+1 {
			return ErrGroupInventorySequence
		}
		state.Inventory = &snapshot
		return nil
	})
}

func (store *PersistentGroupStore) ReadGroupInventory(ctx context.Context, groupID string) (GroupInventorySnapshot, error) {
	var inventory GroupInventorySnapshot
	err := store.withGroupState(ctx, groupID, false, func(state *persistentGroupState) error {
		if state.Inventory == nil {
			return ErrGroupInventoryNotFound
		}
		inventory = cloneGroupInventorySnapshot(*state.Inventory)
		return nil
	})
	return inventory, err
}

func (store *PersistentGroupStore) Head(ctx context.Context, groupID string) (GroupShadowLedgerEntry, bool, error) {
	var head GroupShadowLedgerEntry
	exists := false
	err := store.withGroupState(ctx, groupID, false, func(state *persistentGroupState) error {
		if len(state.Ledger) == 0 {
			return nil
		}
		head = cloneGroupShadowLedgerEntry(state.Ledger[len(state.Ledger)-1])
		exists = true
		return nil
	})
	return head, exists, err
}

func (store *PersistentGroupStore) AppendCAS(ctx context.Context, groupID string, expectedSequence uint64, entry GroupShadowLedgerEntry) (GroupShadowLedgerEntry, error) {
	var appended GroupShadowLedgerEntry
	err := store.withGroupState(ctx, groupID, true, func(state *persistentGroupState) error {
		if state.Inventory == nil {
			if entry.InventoryGeneration != "" || entry.InventoryDigest != "" {
				return ErrGroupShadowInputCAS
			}
		} else if entry.InventoryGeneration != strings.TrimSpace(state.Inventory.Generation) || entry.InventoryDigest != groupInventorySemanticDigest(*state.Inventory) {
			return ErrGroupShadowInputCAS
		}
		var err error
		appended, err = prepareGroupShadowLedgerAppend(state.GroupID, expectedSequence, state.Ledger, entry)
		if err != nil {
			return err
		}
		state.Ledger = append(state.Ledger, appended)
		return nil
	})
	return cloneGroupShadowLedgerEntry(appended), err
}

func (store *PersistentGroupStore) History(ctx context.Context, groupID string) ([]GroupShadowLedgerEntry, error) {
	var history []GroupShadowLedgerEntry
	err := store.withGroupState(ctx, groupID, false, func(state *persistentGroupState) error {
		history = make([]GroupShadowLedgerEntry, len(state.Ledger))
		for index := range state.Ledger {
			history[index] = cloneGroupShadowLedgerEntry(state.Ledger[index])
		}
		return nil
	})
	return history, err
}

func (store *PersistentGroupStore) ReadGroupAuthority(ctx context.Context, groupID string) (GroupAuthorityState, error) {
	var snapshot GroupAuthorityState
	err := store.withGroupState(ctx, groupID, false, func(state *persistentGroupState) error {
		if len(state.AuthorityLedger) > 0 {
			snapshot.LedgerHead = state.AuthorityLedger[len(state.AuthorityLedger)-1]
			snapshot.LedgerExists = true
		}
		if state.Published != nil {
			snapshot.Published = cloneGroupPublishedBundle(*state.Published)
			snapshot.PublishedExists = true
		}
		return nil
	})
	return snapshot, err
}

func (store *PersistentGroupStore) ReadGroupCandidate(ctx context.Context, groupID string) (GroupCandidateBundle, bool, error) {
	var candidate GroupCandidateBundle
	exists := false
	err := store.withGroupState(ctx, groupID, false, func(state *persistentGroupState) error {
		if state.Candidate != nil {
			candidate = cloneGroupCandidateBundle(*state.Candidate)
			exists = true
		}
		return nil
	})
	return candidate, exists, err
}

func (store *PersistentGroupStore) ReadGroupCandidateStage(ctx context.Context, groupID, servingBundleVersion string) (GroupCandidateStageSnapshot, error) {
	servingBundleVersion = strings.TrimSpace(servingBundleVersion)
	if servingBundleVersion != "" {
		var snapshot GroupCandidateStageSnapshot
		err := store.withGroupState(ctx, groupID, false, func(state *persistentGroupState) error {
			snapshot = summarizePersistentGroupState(*state).stage
			authority, candidate, exists := persistentPublishedCandidateByVersion(state, servingBundleVersion)
			if exists {
				snapshot.ServingAuthority = authority
				snapshot.ServingCandidate = candidate
				snapshot.ServingExists = true
			}
			return nil
		})
		return cloneGroupCandidateStageSnapshot(snapshot), err
	}
	summary, err := store.readGroupSummary(ctx, groupID)
	return cloneGroupCandidateStageSnapshot(summary.stage), err
}

func (store *PersistentGroupStore) ReadGroupCandidateByEpoch(ctx context.Context, groupID string, epoch uint64) (GroupCandidateBundle, bool, error) {
	if epoch == 0 {
		return GroupCandidateBundle{}, false, errors.New("edge-control candidate epoch is required")
	}
	var candidate GroupCandidateBundle
	found := false
	err := store.withGroupState(ctx, groupID, false, func(state *persistentGroupState) error {
		if state.Candidate != nil && state.Candidate.Epoch == epoch {
			candidate, found = cloneGroupCandidateBundle(*state.Candidate), true
			return nil
		}
		for index := len(state.CandidateHistory) - 1; index >= 0; index-- {
			if state.CandidateHistory[index].Epoch == epoch {
				candidate, found = cloneGroupCandidateBundle(state.CandidateHistory[index]), true
				return nil
			}
		}
		return nil
	})
	return candidate, found, err
}

func (store *PersistentGroupStore) PutGroupCandidateCAS(ctx context.Context, groupID string, expectedEpoch, expectedCandidateSequence uint64, candidate GroupCandidateBundle) (GroupCandidateBundle, error) {
	var stored GroupCandidateBundle
	err := store.withGroupState(ctx, groupID, true, func(state *persistentGroupState) error {
		currentEpoch := uint64(0)
		if state.Candidate != nil {
			currentEpoch = state.Candidate.Epoch
		}
		currentPublicationSequence := uint64(0)
		if state.Published != nil {
			currentPublicationSequence = state.Published.PublicationSequence
		}
		if currentEpoch != expectedEpoch || candidate.Epoch <= expectedEpoch || candidate.Epoch <= currentPublicationSequence || len(state.Ledger) == 0 || len(state.AuthorityLedger) == 0 ||
			state.AuthorityLedger[len(state.AuthorityLedger)-1].Sequence != candidate.AuthorityLedgerSequence ||
			state.Ledger[len(state.Ledger)-1].Sequence != expectedCandidateSequence || candidate.CandidateLedgerSequence != expectedCandidateSequence {
			return ErrGroupAuthorityCandidateCAS
		}
		if err := validateGroupCandidateBundle(state.GroupID, candidate); err != nil {
			return err
		}
		head := state.Ledger[len(state.Ledger)-1]
		if head.Status != GroupShadowStatusCompiled || head.Bundle == nil || head.BundleArchived ||
			head.BundleGeneration != candidate.Bundle.Generation || head.RouteIntentGeneration != candidate.RouteIntentGeneration ||
			head.InventoryGeneration != candidate.InventoryGeneration || head.InventoryDigest != candidate.Record.InventoryDigest ||
			head.ActiveSlot == candidate.WorkerSlot || (head.ActiveSlot != "a" && head.ActiveSlot != "b") ||
			groupAuthorityCandidateDigest(*head.Bundle) != groupAuthorityCandidateDigest(candidate.Bundle) {
			return ErrGroupAuthorityCandidateCAS
		}
		stored = cloneGroupCandidateBundle(candidate)
		retainReplacedCandidate(state, stored.Epoch)
		state.Candidate = &stored
		return nil
	})
	return cloneGroupCandidateBundle(stored), err
}

// PutGroupCurrentLKGCandidateCAS replaces only the inactive candidate pointer
// from the exact immutable ledger entry backing CurrentAuthority. It is used
// when the newest shadow head is a no-healthy-active observation and therefore
// cannot safely produce a new route bundle. The current publication itself is
// byte-bound and is never modified by this transaction.
func (store *PersistentGroupStore) PutGroupCurrentLKGCandidateCAS(ctx context.Context, groupID string, expectedEpoch, expectedCandidateSequence, expectedPublicationSequence uint64, expectedPublishedDigest string, candidate GroupCandidateBundle) (GroupCandidateBundle, error) {
	var stored GroupCandidateBundle
	err := store.withGroupState(ctx, groupID, true, func(state *persistentGroupState) error {
		currentEpoch := uint64(0)
		if state.Candidate != nil {
			currentEpoch = state.Candidate.Epoch
		}
		if currentEpoch != expectedEpoch || state.Published == nil || len(state.AuthorityLedger) == 0 ||
			state.AuthorityLedger[len(state.AuthorityLedger)-1].Sequence != candidate.AuthorityLedgerSequence || state.Published.PublicationSequence != expectedPublicationSequence ||
			state.Published.Digest != expectedPublishedDigest || state.Published.CandidateLedgerSequence != expectedCandidateSequence ||
			expectedCandidateSequence == 0 || expectedCandidateSequence > uint64(len(state.Ledger)) || len(state.Ledger) == 0 {
			return ErrGroupAuthorityCandidateCAS
		}
		head := state.Ledger[expectedCandidateSequence-1]
		latest := state.Ledger[len(state.Ledger)-1]
		if state.Inventory == nil || latest.Status != GroupShadowStatusFailed || latest.FailureCode != GroupShadowFailureNoHealthyActive ||
			head.Sequence != expectedCandidateSequence || head.Status != GroupShadowStatusCompiled || head.Bundle == nil || head.BundleArchived ||
			head.BundleGeneration != state.Published.Bundle.Generation || state.Inventory.ActiveEpoch.Slot != candidate.CurrentWorkerSlot ||
			(state.Inventory.ActiveEpoch.Slot != "a" && state.Inventory.ActiveEpoch.Slot != "b") || candidate.CandidateLedgerSequence != expectedCandidateSequence ||
			candidate.Epoch <= currentEpoch || candidate.Epoch <= expectedPublicationSequence || candidate.CurrentRecord == nil ||
			candidate.CurrentRecord.BundleDigest != expectedPublishedDigest || candidate.CurrentRecord.Epoch != int64(expectedPublicationSequence) ||
			groupAuthorityCandidateDigest(*head.Bundle) != groupAuthorityCandidateDigest(candidate.Bundle) || candidate.WorkerSlot == candidate.CurrentWorkerSlot {
			return ErrGroupAuthorityCandidateCAS
		}
		if err := validateGroupCandidateBundle(state.GroupID, candidate); err != nil {
			return err
		}
		stored = cloneGroupCandidateBundle(candidate)
		retainReplacedCandidate(state, stored.Epoch)
		state.Candidate = &stored
		return nil
	})
	return cloneGroupCandidateBundle(stored), err
}

// PutGroupStagedCurrentLKGCandidateCAS binds an exact Worker release to an
// inactive slot while preserving the currently published route LKG. It cannot
// advance publication or select ordinary traffic; those are separate Guardian
// CAS transactions after a candidate-bound route canary succeeds.
func (store *PersistentGroupStore) PutGroupStagedCurrentLKGCandidateCAS(ctx context.Context, groupID string, expectedEpoch, expectedAuthoritySequence, expectedPublicationSequence, expectedRecoveryEpoch uint64, expectedPublishedDigest string, serving *GroupServingAuthorityWitness, candidate GroupCandidateBundle) (GroupCandidateBundle, error) {
	var stored GroupCandidateBundle
	err := store.withGroupState(ctx, groupID, true, func(state *persistentGroupState) error {
		currentEpoch := uint64(0)
		if state.Candidate != nil {
			currentEpoch = state.Candidate.Epoch
		}
		if currentEpoch != expectedEpoch {
			return groupCandidateCASConflict(fmt.Sprintf("store_candidate_epoch_mismatch expected=%d actual=%d", expectedEpoch, currentEpoch))
		}
		if state.Published == nil || len(state.AuthorityLedger) == 0 {
			return groupCandidateCASConflict("store_published_authority_unavailable")
		}
		actualAuthoritySequence := state.AuthorityLedger[len(state.AuthorityLedger)-1].Sequence
		if actualAuthoritySequence < expectedAuthoritySequence || state.Published.PublicationSequence != expectedPublicationSequence ||
			state.Published.RecoveryEpoch != expectedRecoveryEpoch || state.Published.Digest != expectedPublishedDigest {
			return groupCandidateCASConflict(fmt.Sprintf("store_published_authority_mismatch expected_ledger=%d actual_ledger=%d expected_publication=%d actual_publication=%d expected_recovery=%d actual_recovery=%d expected_digest=%s actual_digest=%s",
				expectedAuthoritySequence, actualAuthoritySequence, expectedPublicationSequence, state.Published.PublicationSequence,
				expectedRecoveryEpoch, state.Published.RecoveryEpoch, expectedPublishedDigest, state.Published.Digest))
		}
		for _, audit := range state.AuthorityLedger[expectedAuthoritySequence:] {
			if audit.Status != GroupAuthorityStatusFailed || audit.RecoveryEpoch != 0 ||
				audit.LastPublishedBundleGeneration != state.Published.Bundle.Generation {
				return groupCandidateCASConflict(fmt.Sprintf("store_authority_audit_tail_changed_publication sequence=%d status=%s recovery=%d expected_generation=%s actual_generation=%s",
					audit.Sequence, audit.Status, audit.RecoveryEpoch, state.Published.Bundle.Generation, audit.LastPublishedBundleGeneration))
			}
		}
		if state.Published.CandidateLedgerSequence == 0 || state.Published.CandidateLedgerSequence > uint64(len(state.Ledger)) ||
			candidate.AuthorityLedgerSequence != expectedAuthoritySequence || candidate.CandidateLedgerSequence == 0 ||
			candidate.CandidateLedgerSequence > uint64(len(state.Ledger)) {
			return groupCandidateCASConflict(fmt.Sprintf("store_candidate_sequence_invalid published=%d candidate=%d ledger_length=%d candidate_authority=%d expected_authority=%d",
				state.Published.CandidateLedgerSequence, candidate.CandidateLedgerSequence, len(state.Ledger), candidate.AuthorityLedgerSequence, expectedAuthoritySequence))
		}
		if candidate.Epoch <= currentEpoch || candidate.Epoch <= expectedPublicationSequence || candidate.CurrentRecord == nil || candidate.CurrentBundle == nil ||
			candidate.CurrentRecord.BundleDigest != expectedPublishedDigest || candidate.CurrentRecord.Epoch != int64(expectedPublicationSequence) ||
			candidate.CurrentWorkerSlot == candidate.WorkerSlot || !candidateHasStagedWorkerIdentity(candidate) {
			return groupCandidateCASConflict(fmt.Sprintf("store_candidate_identity_invalid candidate_epoch=%d current_epoch=%d publication=%d record_present=%t bundle_present=%t record_digest=%s expected_digest=%s record_epoch=%d expected_record_epoch=%d current_slot=%s worker_slot=%s",
				candidate.Epoch, currentEpoch, expectedPublicationSequence, candidate.CurrentRecord != nil, candidate.CurrentBundle != nil,
				candidateCurrentRecordDigest(candidate), expectedPublishedDigest, candidateCurrentRecordEpoch(candidate), expectedPublicationSequence,
				candidate.CurrentWorkerSlot, candidate.WorkerSlot))
		}
		head := state.Ledger[state.Published.CandidateLedgerSequence-1]
		if serving != nil {
			if serving.Validate() != nil || !servingAuthorityWitnessesEqual(serving, candidate.ServingAuthority) {
				return groupCandidateCASConflict("store_serving_authority_witness_invalid")
			}
			_, servingHead, exists := persistentPublishedCandidateByVersion(state, serving.BundleVersion)
			fallback := !exists &&
				servingAuthorityCanUseCurrentPublishedFallback(serving.BundleVersion, state.Published.Bundle.Generation,
					state.Published.PublicationSequence, state.Published.RecoveryEpoch,
					candidate.AllowDegradedPrevious && !candidate.StandbyOnly) &&
				candidate.CandidateLedgerSequence == state.Published.CandidateLedgerSequence
			if fallback {
				head = state.Ledger[state.Published.CandidateLedgerSequence-1]
			} else if !exists || servingHead.ActiveSlot != serving.WorkerSlot {
				return groupCandidateCASConflict(fmt.Sprintf("store_serving_authority_history_mismatch version=%s exists=%t expected_slot=%s actual_slot=%s",
					serving.BundleVersion, exists, serving.WorkerSlot, servingHead.ActiveSlot))
			} else {
				head = servingHead
			}
		} else if candidate.ServingAuthority != nil || candidate.CandidateLedgerSequence != state.Published.CandidateLedgerSequence {
			return groupCandidateCASConflict("store_bootstrap_candidate_binding_invalid")
		}
		if head.Sequence != candidate.CandidateLedgerSequence || head.Status != GroupShadowStatusCompiled || head.Bundle == nil || head.BundleArchived ||
			head.BundleGeneration != candidate.Bundle.Generation || head.RouteIntentGeneration != candidate.RouteIntentGeneration ||
			head.InventoryGeneration != candidate.InventoryGeneration || head.InventoryDigest != candidate.Record.InventoryDigest ||
			groupAuthorityCandidateDigest(*head.Bundle) != groupAuthorityCandidateDigest(candidate.Bundle) {
			return groupCandidateCASConflict(fmt.Sprintf("store_candidate_head_mismatch expected_sequence=%d actual_sequence=%d status=%s bundle_present=%t archived=%t expected_generation=%s actual_generation=%s expected_route_intent=%s actual_route_intent=%s expected_inventory_generation=%s actual_inventory_generation=%s expected_inventory_digest=%s actual_inventory_digest=%s",
				candidate.CandidateLedgerSequence, head.Sequence, head.Status, head.Bundle != nil, head.BundleArchived, candidate.Bundle.Generation,
				head.BundleGeneration, candidate.RouteIntentGeneration, head.RouteIntentGeneration, candidate.InventoryGeneration,
				head.InventoryGeneration, candidate.Record.InventoryDigest, head.InventoryDigest))
		}
		if signedGroupBundleDigest(*candidate.CurrentBundle) != expectedPublishedDigest {
			return groupCandidateCASConflict(fmt.Sprintf("store_current_bundle_digest_mismatch expected=%s actual=%s",
				expectedPublishedDigest, signedGroupBundleDigest(*candidate.CurrentBundle)))
		}
		if err := validateGroupCandidateBundle(state.GroupID, candidate); err != nil {
			return err
		}
		stored = cloneGroupCandidateBundle(candidate)
		retainReplacedCandidate(state, stored.Epoch)
		state.Candidate = &stored
		return nil
	})
	return cloneGroupCandidateBundle(stored), err
}

func candidateCurrentRecordDigest(candidate GroupCandidateBundle) string {
	if candidate.CurrentRecord == nil {
		return ""
	}
	return candidate.CurrentRecord.BundleDigest
}

func candidateCurrentRecordEpoch(candidate GroupCandidateBundle) int64 {
	if candidate.CurrentRecord == nil {
		return 0
	}
	return candidate.CurrentRecord.Epoch
}

func persistentPublishedCandidateByVersion(state *persistentGroupState, version string) (GroupAuthorityLedgerEntry, GroupShadowLedgerEntry, bool) {
	if state == nil {
		return GroupAuthorityLedgerEntry{}, GroupShadowLedgerEntry{}, false
	}
	for index := len(state.AuthorityLedger) - 1; index >= 0; index-- {
		authority := state.AuthorityLedger[index]
		if authority.Status != GroupAuthorityStatusPublished ||
			groupPublicationVersion(authority.BundleGeneration, authority.Sequence, authority.RecoveryEpoch) != version ||
			authority.CandidateLedgerSequence == 0 || authority.CandidateLedgerSequence > uint64(len(state.Ledger)) {
			continue
		}
		candidate := state.Ledger[authority.CandidateLedgerSequence-1]
		if candidate.Sequence != authority.CandidateLedgerSequence || candidate.Status != GroupShadowStatusCompiled ||
			candidate.Bundle == nil || candidate.BundleArchived || candidate.BundleGeneration != authority.BundleGeneration {
			continue
		}
		return authority, cloneGroupShadowLedgerEntry(candidate), true
	}
	return GroupAuthorityLedgerEntry{}, GroupShadowLedgerEntry{}, false
}

func retainReplacedCandidate(state *persistentGroupState, nextEpoch uint64) {
	if state == nil || state.Candidate == nil || state.Candidate.Epoch == nextEpoch {
		return
	}
	state.CandidateHistory = append(state.CandidateHistory, cloneGroupCandidateBundle(*state.Candidate))
	if len(state.CandidateHistory) > retainedGroupCandidateBundles {
		state.CandidateHistory = append([]GroupCandidateBundle(nil), state.CandidateHistory[len(state.CandidateHistory)-retainedGroupCandidateBundles:]...)
	}
}

func persistentCandidateByEpoch(state *persistentGroupState, epoch uint64) (*GroupCandidateBundle, bool) {
	if state == nil || epoch == 0 {
		return nil, false
	}
	if state.Candidate != nil && state.Candidate.Epoch == epoch {
		return state.Candidate, true
	}
	for index := len(state.CandidateHistory) - 1; index >= 0; index-- {
		if state.CandidateHistory[index].Epoch == epoch {
			return &state.CandidateHistory[index], true
		}
	}
	return nil, false
}

func (store *PersistentGroupStore) ReadGroupAuthorityStatus(ctx context.Context, groupID string) (AuthorityGroupStoreSnapshot, error) {
	summary, err := store.readGroupSummary(ctx, groupID)
	return cloneAuthorityGroupStoreSnapshot(summary.status), err
}

func (store *PersistentGroupStore) AppendGroupAuthorityCAS(ctx context.Context, groupID string, expectedSequence, expectedCandidateSequence uint64,
	entry GroupAuthorityLedgerEntry, signed *model.EdgeRouteBundle) (GroupAuthorityLedgerEntry, error) {
	if entry.RecoveryEpoch != 0 || entry.RecoveryReason != "" {
		return GroupAuthorityLedgerEntry{}, errors.New("edge-control normal publication cannot carry recovery authority")
	}
	var appended GroupAuthorityLedgerEntry
	err := store.withGroupState(ctx, groupID, true, func(state *persistentGroupState) error {
		var candidate *GroupShadowLedgerEntry
		if expectedCandidateSequence > 0 {
			if len(state.Ledger) == 0 || state.Ledger[len(state.Ledger)-1].Sequence != expectedCandidateSequence {
				return ErrGroupAuthorityCandidateCAS
			}
			value := cloneGroupShadowLedgerEntry(state.Ledger[len(state.Ledger)-1])
			candidate = &value
		}
		var current *GroupPublishedBundle
		if state.Published != nil {
			value := cloneGroupPublishedBundle(*state.Published)
			current = &value
		}
		var next *GroupPublishedBundle
		var err error
		appended, next, err = prepareGroupAuthorityAppend(state.GroupID, expectedSequence, state.AuthorityLedger, current, candidate, entry, signed)
		if err != nil {
			return err
		}
		state.AuthorityLedger = append(state.AuthorityLedger, appended)
		if next != nil {
			state.Published = next
		}
		return nil
	})
	return appended, err
}

func (store *PersistentGroupStore) PromoteGroupCandidateCAS(ctx context.Context, groupID string, request GroupPromotionRequest,
	entry GroupAuthorityLedgerEntry, signed model.EdgeRouteBundle) (GroupAuthorityLedgerEntry, error) {
	var appended GroupAuthorityLedgerEntry
	err := store.withGroupState(ctx, groupID, true, func(state *persistentGroupState) error {
		candidateBundle, candidateExists := persistentCandidateByEpoch(state, request.ExpectedCandidateEpoch)
		if state.Published == nil || !candidateExists || len(state.AuthorityLedger) == 0 ||
			uint64(len(state.AuthorityLedger)) < request.ExpectedAuthoritySequence ||
			state.Published.PublicationSequence != request.ExpectedPublicationSequence || state.Published.RecoveryEpoch != request.ExpectedRecoveryEpoch ||
			state.Published.Digest != request.ExpectedPublishedBundleDigest || candidateBundle.Record.RecordDigest != request.CandidateRecordDigest ||
			candidateBundle.WorkerSlot != request.CandidateWorkerSlot || candidateBundle.Bundle.Generation != request.CandidateBundleGeneration ||
			candidateBundle.AuthorityLedgerSequence != request.ExpectedAuthoritySequence {
			return ErrGroupAuthorityCASConflict
		}
		for _, audit := range state.AuthorityLedger[request.ExpectedAuthoritySequence:] {
			if audit.Status != GroupAuthorityStatusFailed || audit.RecoveryEpoch != 0 ||
				audit.LastPublishedBundleGeneration != state.Published.Bundle.Generation {
				return ErrGroupAuthorityCASConflict
			}
		}
		candidateSequence := candidateBundle.CandidateLedgerSequence
		if candidateSequence == 0 || candidateSequence > uint64(len(state.Ledger)) {
			return ErrGroupAuthorityCandidateCAS
		}
		candidate := cloneGroupShadowLedgerEntry(state.Ledger[candidateSequence-1])
		if candidate.Sequence != candidateSequence || candidate.Bundle == nil || candidate.BundleArchived ||
			groupAuthorityCandidateDigest(*candidate.Bundle) != groupAuthorityCandidateDigest(candidateBundle.Bundle) {
			return ErrGroupAuthorityCandidateCAS
		}
		current := cloneGroupPublishedBundle(*state.Published)
		var next *GroupPublishedBundle
		var err error
		currentAuthoritySequence := uint64(len(state.AuthorityLedger))
		appended, next, err = prepareGroupAuthorityAppend(state.GroupID, currentAuthoritySequence, state.AuthorityLedger, &current, &candidate, entry, &signed)
		if err != nil {
			return err
		}
		state.AuthorityLedger = append(state.AuthorityLedger, appended)
		state.Published = next
		return nil
	})
	return appended, err
}

func (store *PersistentGroupStore) ReadGroupRecoveryTarget(ctx context.Context, groupID, generation string) (GroupAuthorityState, GroupShadowLedgerEntry, uint64, error) {
	generation = strings.TrimSpace(generation)
	if generation == "" {
		return GroupAuthorityState{}, GroupShadowLedgerEntry{}, 0, errors.New("edge-control recovery target generation is required")
	}
	var authority GroupAuthorityState
	var candidate GroupShadowLedgerEntry
	var recoveryEpoch uint64
	err := store.withGroupState(ctx, groupID, false, func(state *persistentGroupState) error {
		if len(state.AuthorityLedger) == 0 || state.Published == nil {
			return errors.New("edge-control recovery authority is unavailable")
		}
		authority.LedgerHead = state.AuthorityLedger[len(state.AuthorityLedger)-1]
		authority.LedgerExists = true
		authority.Published = cloneGroupPublishedBundle(*state.Published)
		authority.PublishedExists = true
		targetCandidateSequence := uint64(0)
		for index := len(state.AuthorityLedger) - 1; index >= 0; index-- {
			entry := state.AuthorityLedger[index]
			if entry.RecoveryEpoch > recoveryEpoch {
				recoveryEpoch = entry.RecoveryEpoch
			}
			if targetCandidateSequence == 0 && entry.Status == GroupAuthorityStatusPublished && entry.BundleGeneration == generation {
				targetCandidateSequence = entry.CandidateLedgerSequence
			}
		}
		if targetCandidateSequence == 0 || targetCandidateSequence > uint64(len(state.Ledger)) {
			return errors.New("edge-control recovery target was never published")
		}
		candidate = cloneGroupShadowLedgerEntry(state.Ledger[targetCandidateSequence-1])
		if candidate.Status != GroupShadowStatusCompiled || candidate.BundleGeneration != generation {
			return errors.New("edge-control recovery target candidate is invalid")
		}
		// Candidate bundles are compacted independently of the authority ledger.
		// A previously published bundle remains a valid recovery source even when
		// its shadow candidate payload has been archived. Rehydrate only the exact
		// published generation; never use a different candidate or current intent.
		if candidate.Bundle == nil {
			if !candidate.BundleArchived || state.Published.Bundle.Generation != generation {
				return errors.New("edge-control recovery target candidate is unavailable")
			}
			bundle := cloneEdgeRouteBundle(state.Published.Bundle)
			candidate.Bundle = &bundle
		}
		return nil
	})
	return authority, candidate, recoveryEpoch, err
}

// RecoverPublishedLKG renews only the exact bundle already referenced by the
// durable published pointer. It is independent of the shadow candidate payload
// so candidate compaction or a failed compiler audit cannot strand serving
// traffic behind an expired validity window.
func (store *PersistentGroupStore) RecoverPublishedLKG(ctx context.Context, groupID string, expectedSequence, expectedRecoveryEpoch uint64,
	generation string, signed model.EdgeRouteBundle, reason string, recordedAt time.Time) (GroupAuthorityLedgerEntry, error) {
	groupID = normalizeGroupID(groupID)
	generation = strings.TrimSpace(generation)
	reason = strings.TrimSpace(reason)
	if groupID == "" || generation == "" || signed.Generation != generation || reason == "" || recordedAt.IsZero() {
		return GroupAuthorityLedgerEntry{}, errors.New("edge-control published LKG recovery input is invalid")
	}
	var appended GroupAuthorityLedgerEntry
	err := store.withGroupState(ctx, groupID, true, func(state *persistentGroupState) error {
		if state.Published == nil || state.Published.PublicationSequence != expectedSequence ||
			state.Published.RecoveryEpoch != expectedRecoveryEpoch || state.Published.Bundle.Generation != generation {
			return ErrGroupAuthorityCASConflict
		}
		currentRecoveryEpoch := uint64(0)
		for _, previous := range state.AuthorityLedger {
			if previous.RecoveryEpoch > currentRecoveryEpoch {
				currentRecoveryEpoch = previous.RecoveryEpoch
			}
		}
		if currentRecoveryEpoch != expectedRecoveryEpoch {
			return ErrGroupAuthorityCASConflict
		}
		for _, audit := range state.AuthorityLedger[expectedSequence:] {
			if audit.Status != GroupAuthorityStatusFailed || audit.RecoveryEpoch != 0 ||
				audit.LastPublishedBundleGeneration != state.Published.Bundle.Generation {
				return ErrGroupAuthorityCASConflict
			}
		}
		candidateSequence := state.Published.CandidateLedgerSequence
		if candidateSequence == 0 || candidateSequence > uint64(len(state.Ledger)) {
			return ErrGroupAuthorityCandidateCAS
		}
		candidate := cloneGroupShadowLedgerEntry(state.Ledger[candidateSequence-1])
		if candidate.Status != GroupShadowStatusCompiled || candidate.BundleGeneration != generation {
			return ErrGroupAuthorityCandidateCAS
		}
		if candidate.Bundle == nil {
			if !candidate.BundleArchived {
				return ErrGroupAuthorityCandidateCAS
			}
			bundle := cloneEdgeRouteBundle(state.Published.Bundle)
			candidate.Bundle = &bundle
		}
		previousPublished := GroupAuthorityLedgerEntry{}
		foundPublished := false
		for index := len(state.AuthorityLedger) - 1; index >= 0; index-- {
			entry := state.AuthorityLedger[index]
			if entry.Status == GroupAuthorityStatusPublished && entry.BundleGeneration == generation {
				previousPublished = entry
				foundPublished = true
				break
			}
		}
		if !foundPublished {
			return ErrGroupAuthorityCandidateCAS
		}
		entry := GroupAuthorityLedgerEntry{
			Schema: GroupAuthorityLedgerSchemaV1, GroupID: groupID, Status: GroupAuthorityStatusPublished,
			CandidateLedgerSequence: candidateSequence, RouteIntentGeneration: previousPublished.RouteIntentGeneration,
			InventoryGeneration: previousPublished.InventoryGeneration, BundleGeneration: signed.Generation,
			LastPublishedBundleGeneration: signed.Generation, PublishedBundleDigest: signedGroupBundleDigest(signed),
			SigningKeyID: signed.KeyID, RecoveryEpoch: expectedRecoveryEpoch + 1, RecoveryReason: reason,
			Authority: "edge-control", PublicationEnabled: true, RecordedAt: recordedAt.UTC(),
		}
		current := cloneGroupPublishedBundle(*state.Published)
		var next *GroupPublishedBundle
		var err error
		appended, next, err = prepareGroupAuthorityAppend(state.GroupID, uint64(len(state.AuthorityLedger)), state.AuthorityLedger, &current, &candidate, entry, &signed)
		if err != nil {
			return err
		}
		state.AuthorityLedger = append(state.AuthorityLedger, appended)
		state.Published = next
		return nil
	})
	return appended, err
}

func (store *PersistentGroupStore) RecoverGroupAuthorityCAS(ctx context.Context, groupID string, expectedSequence, expectedRecoveryEpoch uint64,
	entry GroupAuthorityLedgerEntry, signed model.EdgeRouteBundle) (GroupAuthorityLedgerEntry, error) {
	if entry.RecoveryEpoch != expectedRecoveryEpoch+1 || strings.TrimSpace(entry.RecoveryReason) == "" {
		return GroupAuthorityLedgerEntry{}, errors.New("edge-control recovery entry is invalid")
	}
	var appended GroupAuthorityLedgerEntry
	err := store.withGroupState(ctx, groupID, true, func(state *persistentGroupState) error {
		currentRecoveryEpoch := uint64(0)
		for _, previous := range state.AuthorityLedger {
			if previous.RecoveryEpoch > currentRecoveryEpoch {
				currentRecoveryEpoch = previous.RecoveryEpoch
			}
		}
		if currentRecoveryEpoch != expectedRecoveryEpoch {
			return ErrGroupAuthorityCASConflict
		}
		if state.Published == nil || state.Published.PublicationSequence != expectedSequence ||
			state.Published.RecoveryEpoch != expectedRecoveryEpoch {
			return ErrGroupAuthorityCASConflict
		}
		for _, audit := range state.AuthorityLedger[expectedSequence:] {
			if audit.Status != GroupAuthorityStatusFailed || audit.RecoveryEpoch != 0 ||
				audit.LastPublishedBundleGeneration != state.Published.Bundle.Generation {
				return ErrGroupAuthorityCASConflict
			}
		}
		candidateSequence := entry.CandidateLedgerSequence
		if candidateSequence == 0 || candidateSequence > uint64(len(state.Ledger)) {
			return ErrGroupAuthorityCandidateCAS
		}
		previouslyPublished := false
		for _, previous := range state.AuthorityLedger {
			if previous.Status == GroupAuthorityStatusPublished && previous.CandidateLedgerSequence == candidateSequence && previous.BundleGeneration == signed.Generation {
				previouslyPublished = true
				break
			}
		}
		if !previouslyPublished {
			return ErrGroupAuthorityCandidateCAS
		}
		candidate := cloneGroupShadowLedgerEntry(state.Ledger[candidateSequence-1])
		if candidate.Bundle == nil {
			if !candidate.BundleArchived || state.Published == nil ||
				candidate.BundleGeneration != state.Published.Bundle.Generation || signed.Generation != state.Published.Bundle.Generation {
				return ErrGroupAuthorityCandidateCAS
			}
			bundle := cloneEdgeRouteBundle(state.Published.Bundle)
			candidate.Bundle = &bundle
		}
		var current *GroupPublishedBundle
		if state.Published != nil {
			value := cloneGroupPublishedBundle(*state.Published)
			current = &value
		}
		var next *GroupPublishedBundle
		var err error
		currentAuthoritySequence := uint64(len(state.AuthorityLedger))
		appended, next, err = prepareGroupAuthorityAppend(state.GroupID, currentAuthoritySequence, state.AuthorityLedger, current, &candidate, entry, &signed)
		if err != nil {
			return err
		}
		state.AuthorityLedger = append(state.AuthorityLedger, appended)
		state.Published = next
		return nil
	})
	return appended, err
}

func (store *PersistentGroupStore) AuthorityHistory(ctx context.Context, groupID string) ([]GroupAuthorityLedgerEntry, error) {
	var history []GroupAuthorityLedgerEntry
	err := store.withGroupState(ctx, groupID, false, func(state *persistentGroupState) error {
		history = append([]GroupAuthorityLedgerEntry(nil), state.AuthorityLedger...)
		return nil
	})
	return history, err
}

func (store *PersistentGroupStore) groupStatePath(groupID string) string {
	digest := sha256.Sum256([]byte(normalizeGroupID(groupID)))
	return filepath.Join(store.root, "group-"+hex.EncodeToString(digest[:])+".json")
}

func (store *PersistentGroupStore) withGroupState(ctx context.Context, groupID string, write bool, fn func(*persistentGroupState) error) error {
	if store == nil || strings.TrimSpace(store.root) == "" {
		return errors.New("edge-control persistent group store is nil")
	}
	if ctx == nil {
		return errors.New("edge-control persistent group store context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	groupID = normalizeGroupID(groupID)
	if groupID == "" {
		return errors.New("edge-control persistent group id is required")
	}
	if fn == nil {
		return errors.New("edge-control persistent group transaction is nil")
	}
	statePath := store.groupStatePath(groupID)
	lockPath := statePath + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open edge-control group lock: %w", err)
	}
	defer lockFile.Close()
	if info, err := lockFile.Stat(); err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o007 != 0 {
		return errors.New("edge-control group lock must be a private regular file")
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock edge-control group state: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN) }()
	if err := ctx.Err(); err != nil {
		return err
	}

	state, err := store.readGroupState(statePath, groupID)
	if err != nil {
		return err
	}
	if err := fn(&state); err != nil {
		return err
	}
	if !write {
		return nil
	}
	compactPersistentGroupState(&state)
	state.Revision++
	if err := compactPersistentGroupStateForSize(&state); err != nil {
		return err
	}
	if err := store.writeGroupState(statePath, state); err != nil {
		return err
	}
	store.cacheGroupSummary(groupID, state)
	return nil
}

func (store *PersistentGroupStore) readGroupSummary(ctx context.Context, groupID string) (persistentGroupSummary, error) {
	if ctx == nil {
		return persistentGroupSummary{}, errors.New("edge-control group summary context is nil")
	}
	if err := ctx.Err(); err != nil {
		return persistentGroupSummary{}, err
	}
	groupID = normalizeGroupID(groupID)
	if cached, found := store.cachedGroupSummary(groupID); found {
		return cached, nil
	}
	var summary persistentGroupSummary
	err := store.withGroupState(ctx, groupID, false, func(state *persistentGroupState) error {
		summary = summarizePersistentGroupState(*state)
		return nil
	})
	if err != nil {
		return persistentGroupSummary{}, err
	}
	store.summaryMu.Lock()
	store.summaries[groupID] = summary
	store.summaryMu.Unlock()
	return summary, nil
}

func (store *PersistentGroupStore) cachedGroupSummary(groupID string) (persistentGroupSummary, bool) {
	store.summaryMu.RLock()
	summary, found := store.summaries[groupID]
	store.summaryMu.RUnlock()
	return summary, found
}

func (store *PersistentGroupStore) cacheGroupSummary(groupID string, state persistentGroupState) {
	summary := summarizePersistentGroupState(state)
	store.summaryMu.Lock()
	store.summaries[groupID] = summary
	store.summaryMu.Unlock()
}

func summarizePersistentGroupState(state persistentGroupState) persistentGroupSummary {
	var out persistentGroupSummary
	if state.Inventory != nil {
		out.status.Inventory = cloneGroupInventorySnapshot(*state.Inventory)
		out.status.InventoryExists = true
		out.stage.Inventory = cloneGroupInventorySnapshot(*state.Inventory)
		out.stage.InventoryExists = true
	}
	if state.InventoryProducer != nil {
		out.status.Producer = cloneGroupInventoryProducerState(*state.InventoryProducer)
		out.status.ProducerExists = true
	}
	if len(state.AuthorityLedger) > 0 {
		out.status.Authority.LedgerHead = state.AuthorityLedger[len(state.AuthorityLedger)-1]
		out.status.Authority.LedgerExists = true
		out.stage.Authority.LedgerHead = out.status.Authority.LedgerHead
		out.stage.Authority.LedgerExists = true
	}
	if state.Published != nil {
		published := cloneGroupPublishedBundle(*state.Published)
		out.status.Authority.Published, out.status.Authority.PublishedExists = published, true
		out.stage.Authority.Published, out.stage.Authority.PublishedExists = cloneGroupPublishedBundle(published), true
		sequence := published.CandidateLedgerSequence
		if sequence > 0 && sequence <= uint64(len(state.Ledger)) {
			out.stage.PublishedCandidate = cloneGroupShadowLedgerEntry(state.Ledger[sequence-1])
		}
	}
	if state.Candidate != nil {
		out.status.Candidate, out.status.CandidateExists = cloneGroupCandidateBundle(*state.Candidate), true
		out.stage.Candidate, out.stage.CandidateExists = cloneGroupCandidateBundle(*state.Candidate), true
	}
	return out
}

func cloneAuthorityGroupStoreSnapshot(value AuthorityGroupStoreSnapshot) AuthorityGroupStoreSnapshot {
	if value.InventoryExists {
		value.Inventory = cloneGroupInventorySnapshot(value.Inventory)
	}
	if value.ProducerExists {
		value.Producer = cloneGroupInventoryProducerState(value.Producer)
	}
	if value.Authority.PublishedExists {
		value.Authority.Published = cloneGroupPublishedBundle(value.Authority.Published)
	}
	if value.CandidateExists {
		value.Candidate = cloneGroupCandidateBundle(value.Candidate)
	}
	return value
}

func cloneGroupCandidateStageSnapshot(value GroupCandidateStageSnapshot) GroupCandidateStageSnapshot {
	if value.Authority.PublishedExists {
		value.Authority.Published = cloneGroupPublishedBundle(value.Authority.Published)
	}
	if value.CandidateExists {
		value.Candidate = cloneGroupCandidateBundle(value.Candidate)
	}
	if value.InventoryExists {
		value.Inventory = cloneGroupInventorySnapshot(value.Inventory)
	}
	value.PublishedCandidate = cloneGroupShadowLedgerEntry(value.PublishedCandidate)
	if value.ServingExists {
		value.ServingCandidate = cloneGroupShadowLedgerEntry(value.ServingCandidate)
	}
	return value
}

func (store *PersistentGroupStore) readGroupState(path, groupID string) (persistentGroupState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return persistentGroupState{Schema: persistentGroupStateSchemaV1, GroupID: groupID}, nil
		}
		return persistentGroupState{}, fmt.Errorf("read edge-control group state: %w", err)
	}
	if len(data) == 0 || len(data) > maxPersistentGroupStateBytes {
		return persistentGroupState{}, errors.New("edge-control group state size is invalid")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o007 != 0 {
		return persistentGroupState{}, errors.New("edge-control group state must be a private regular file")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state persistentGroupState
	if err := decoder.Decode(&state); err != nil {
		return persistentGroupState{}, fmt.Errorf("decode edge-control group state: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return persistentGroupState{}, errors.New("edge-control group state contains trailing data")
	}
	if err := validatePersistentGroupState(state, groupID); err != nil {
		migrated, ok := migrateLegacyCandidateAuthoritySequence(state, groupID)
		if !ok {
			return persistentGroupState{}, err
		}
		state = migrated
	}
	return state, nil
}

// compactPersistentGroupState keeps the current published LKG and a bounded
// recent recovery window. Older decisions retain their checksummed identity
// and sequence but not another full copy of the route bundle.
func compactPersistentGroupState(state *persistentGroupState) {
	if state == nil || len(state.Ledger) == 0 {
		return
	}
	keep := make(map[uint64]struct{}, retainedGroupCandidateBundles+1)
	if state.Published != nil && state.Published.CandidateLedgerSequence > 0 {
		keep[state.Published.CandidateLedgerSequence] = struct{}{}
	}
	if state.Candidate != nil && state.Candidate.CandidateLedgerSequence > 0 {
		keep[state.Candidate.CandidateLedgerSequence] = struct{}{}
	}
	for index := range state.CandidateHistory {
		if state.CandidateHistory[index].CandidateLedgerSequence > 0 {
			keep[state.CandidateHistory[index].CandidateLedgerSequence] = struct{}{}
		}
	}
	remaining := retainedGroupCandidateBundles
	for index := len(state.Ledger) - 1; index >= 0 && remaining > 0; index-- {
		entry := state.Ledger[index]
		if entry.Status != GroupShadowStatusCompiled || entry.Bundle == nil {
			continue
		}
		keep[entry.Sequence] = struct{}{}
		remaining--
	}
	for index := range state.Ledger {
		entry := &state.Ledger[index]
		if entry.Status != GroupShadowStatusCompiled || entry.Bundle == nil {
			continue
		}
		if _, retained := keep[entry.Sequence]; retained {
			entry.BundleArchived = false
			continue
		}
		entry.Bundle = nil
		entry.BundleArchived = true
	}
}

// compactPersistentGroupStateForSize preserves the serving and newest
// candidates while shrinking older recovery copies before the hard durable
// state limit can reject a write.
func compactPersistentGroupStateForSize(state *persistentGroupState) error {
	if state == nil {
		return nil
	}
	encodedSize := func() (int, error) {
		copy := *state
		copy.Digest = persistentGroupStateDigestPlaceholder
		data, err := json.Marshal(copy)
		if err != nil {
			return 0, fmt.Errorf("encode edge-control group state for compaction: %w", err)
		}
		return len(data), nil
	}
	size, err := encodedSize()
	if err != nil || size <= targetPersistentGroupStateBytes {
		return err
	}
	for size > targetPersistentGroupStateBytes && len(state.CandidateHistory) > 0 {
		state.CandidateHistory = append([]GroupCandidateBundle(nil), state.CandidateHistory[1:]...)
		compactPersistentGroupState(state)
		size, err = encodedSize()
		if err != nil {
			return err
		}
	}
	mandatory := make(map[uint64]struct{}, 3+len(state.CandidateHistory))
	if state.Published != nil && state.Published.CandidateLedgerSequence > 0 {
		mandatory[state.Published.CandidateLedgerSequence] = struct{}{}
	}
	if state.Candidate != nil && state.Candidate.CandidateLedgerSequence > 0 {
		mandatory[state.Candidate.CandidateLedgerSequence] = struct{}{}
	}
	for index := range state.CandidateHistory {
		if state.CandidateHistory[index].CandidateLedgerSequence > 0 {
			mandatory[state.CandidateHistory[index].CandidateLedgerSequence] = struct{}{}
		}
	}
	for index := len(state.Ledger) - 1; index >= 0; index-- {
		entry := state.Ledger[index]
		if entry.Status == GroupShadowStatusCompiled && entry.Bundle != nil {
			mandatory[entry.Sequence] = struct{}{}
			break
		}
	}
	for index := range state.Ledger {
		if size <= targetPersistentGroupStateBytes {
			break
		}
		entry := &state.Ledger[index]
		if entry.Status != GroupShadowStatusCompiled || entry.Bundle == nil {
			continue
		}
		if _, retained := mandatory[entry.Sequence]; retained {
			continue
		}
		entry.Bundle = nil
		entry.BundleArchived = true
		size, err = encodedSize()
		if err != nil {
			return err
		}
	}
	if size > maxPersistentGroupStateBytes {
		return errors.New("edge-control group state exceeds durable size limit after compaction")
	}
	return nil
}

func (store *PersistentGroupStore) writeGroupState(path string, state persistentGroupState) error {
	state.Schema = persistentGroupStateSchemaV1
	state.GroupID = normalizeGroupID(state.GroupID)
	state.Digest = ""
	state.Digest = persistentGroupStateDigest(state)
	if err := validatePersistentGroupState(state, state.GroupID); err != nil {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode edge-control group state: %w", err)
	}
	if len(data) > maxPersistentGroupStateBytes {
		return errors.New("edge-control group state exceeds durable size limit")
	}
	temporary, err := os.CreateTemp(store.root, ".group-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create edge-control group state temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace edge-control group state: %w", err)
	}
	directory, err := os.Open(store.root)
	if err != nil {
		return fmt.Errorf("open edge-control state directory for sync: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync edge-control state directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close edge-control state directory: %w", err)
	}
	return nil
}

func validatePersistentGroupState(state persistentGroupState, groupID string) error {
	groupID = normalizeGroupID(groupID)
	if state.Schema != persistentGroupStateSchemaV1 || normalizeGroupID(state.GroupID) != groupID || state.Revision == 0 || state.Digest != persistentGroupStateDigest(state) {
		return errors.New("edge-control persistent group state identity or digest is invalid")
	}
	if state.Inventory != nil {
		if state.Inventory.Schema != GroupInventorySchemaV1 || normalizeGroupID(state.Inventory.GroupID) != groupID || state.Inventory.Sequence == 0 || strings.TrimSpace(state.Inventory.Generation) == "" {
			return errors.New("edge-control persistent group inventory is invalid")
		}
	}
	if state.InventoryProducer != nil {
		if err := validateGroupInventoryProducerState(*state.InventoryProducer, groupID); err != nil {
			return err
		}
		if state.Inventory == nil || state.Inventory.ObservedAt.IsZero() ||
			state.Inventory.Generation != groupInventoryProducerGeneration(state.InventoryProducer.Generation, *state.Inventory) {
			return errors.New("edge-control persistent inventory is not bound to its producer cursor")
		}
	}
	validated := make([]GroupShadowLedgerEntry, 0, len(state.Ledger))
	for index, persisted := range state.Ledger {
		if persisted.BundleArchived {
			if persisted.Schema != GroupShadowLedgerSchemaV1 || normalizeGroupID(persisted.GroupID) != groupID ||
				persisted.Sequence != uint64(index+1) || persisted.Status != GroupShadowStatusCompiled || persisted.Bundle != nil ||
				persisted.Authority != "none" || persisted.PublicationEnabled || strings.TrimSpace(persisted.RouteIntentGeneration) == "" ||
				!strings.HasPrefix(persisted.InputDigest, "sha256:") || len(persisted.InputDigest) != len("sha256:")+sha256.Size*2 ||
				persisted.RecordedAt.IsZero() || persisted.BundleGeneration == "" || persisted.FailureCode != "" ||
				persisted.LastSuccessfulBundleGeneration != persisted.BundleGeneration {
				return errors.New("edge-control archived group candidate is invalid")
			}
			validated = append(validated, persisted)
			continue
		}
		candidate := cloneGroupShadowLedgerEntry(persisted)
		if candidate.Sequence != uint64(index+1) {
			return errors.New("edge-control persistent group ledger sequence is invalid")
		}
		candidate.Sequence = 0
		appended, err := prepareGroupShadowLedgerAppend(groupID, uint64(index), validated, candidate)
		if err != nil || appended.Sequence != persisted.Sequence {
			return errors.New("edge-control persistent group ledger transition is invalid")
		}
		validated = append(validated, persisted)
	}
	lastPublishedGeneration := ""
	lastPublishedSequence := uint64(0)
	lastPublishedDigest := ""
	lastPublishedCandidate := uint64(0)
	lastRecoveryEpoch := uint64(0)
	for index, entry := range state.AuthorityLedger {
		if entry.Schema != GroupAuthorityLedgerSchemaV1 || normalizeGroupID(entry.GroupID) != groupID || entry.Sequence != uint64(index+1) ||
			entry.Authority != "edge-control" || !entry.PublicationEnabled || entry.RecordedAt.IsZero() ||
			strings.TrimSpace(entry.RouteIntentGeneration) == "" || entry.RouteIntentGeneration != strings.TrimSpace(entry.RouteIntentGeneration) ||
			(entry.Status != GroupAuthorityStatusPublished && entry.Status != GroupAuthorityStatusFailed) {
			return errors.New("edge-control persistent group authority ledger is invalid")
		}
		if entry.CandidateLedgerSequence > uint64(len(state.Ledger)) {
			return errors.New("edge-control persistent group authority candidate sequence is invalid")
		}
		if entry.RecoveryEpoch == 0 {
			if entry.RecoveryReason != "" {
				return errors.New("edge-control persistent group recovery reason has no epoch")
			}
		} else {
			if entry.Status != GroupAuthorityStatusPublished || entry.RecoveryEpoch != lastRecoveryEpoch+1 || strings.TrimSpace(entry.RecoveryReason) == "" || len(entry.RecoveryReason) > 256 {
				return errors.New("edge-control persistent group recovery epoch is invalid")
			}
			lastRecoveryEpoch = entry.RecoveryEpoch
		}
		if entry.Status == GroupAuthorityStatusPublished {
			if entry.CandidateLedgerSequence == 0 || entry.BundleGeneration == "" || entry.LastPublishedBundleGeneration != entry.BundleGeneration ||
				!groupAuthorityDigestPattern.MatchString(entry.PublishedBundleDigest) || !groupAuthorityKeyIDPattern.MatchString(entry.SigningKeyID) || entry.FailureCode != "" {
				return errors.New("edge-control persistent group published authority entry is invalid")
			}
			candidate := state.Ledger[entry.CandidateLedgerSequence-1]
			if candidate.Status != GroupShadowStatusCompiled || candidate.BundleGeneration != entry.BundleGeneration ||
				(candidate.Bundle == nil && !candidate.BundleArchived) || (candidate.Bundle != nil && candidate.Bundle.Generation != entry.BundleGeneration) {
				return errors.New("edge-control persistent group authority entry lost candidate binding")
			}
			lastPublishedGeneration = entry.BundleGeneration
			lastPublishedSequence = entry.Sequence
			lastPublishedDigest = entry.PublishedBundleDigest
			lastPublishedCandidate = entry.CandidateLedgerSequence
		} else if entry.FailureCode == "" || entry.BundleGeneration != "" || entry.PublishedBundleDigest != "" || entry.SigningKeyID != "" ||
			entry.LastPublishedBundleGeneration != lastPublishedGeneration {
			return errors.New("edge-control persistent group failed authority entry changed LKG")
		}
	}
	if state.Published == nil {
		if lastPublishedGeneration != "" {
			return errors.New("edge-control persistent group published head is missing")
		}
	} else {
		if err := validateGroupPublishedBundle(groupID, *state.Published); err != nil {
			return err
		}
		candidateMatchesPublished := false
		if lastPublishedCandidate > 0 && lastPublishedCandidate <= uint64(len(state.Ledger)) {
			candidate := state.Ledger[lastPublishedCandidate-1]
			switch {
			case candidate.Bundle != nil && !candidate.BundleArchived:
				candidateMatchesPublished = groupAuthorityCandidateDigest(*candidate.Bundle) == groupAuthorityCandidateDigest(state.Published.Bundle)
			case candidate.Bundle == nil && candidate.BundleArchived:
				// Compaction may archive the shadow payload after publication. The
				// published bundle is the durable recovery source; retain the
				// binding through its immutable generation instead of rejecting the
				// whole group state as corrupt.
				candidateMatchesPublished = candidate.BundleGeneration == state.Published.Bundle.Generation &&
					candidate.LastSuccessfulBundleGeneration == state.Published.Bundle.Generation
			}
		}
		if state.Published.PublicationSequence != lastPublishedSequence || state.Published.CandidateLedgerSequence != lastPublishedCandidate ||
			state.Published.Bundle.Generation != lastPublishedGeneration || state.Published.Digest != lastPublishedDigest || state.Published.RecoveryEpoch != lastRecoveryEpoch ||
			!candidateMatchesPublished {
			return errors.New("edge-control persistent group published head does not match authority ledger")
		}
	}
	if state.Candidate != nil {
		if err := validatePersistentCandidateBinding(state, groupID, *state.Candidate); err != nil {
			return err
		}
	}
	if len(state.CandidateHistory) > retainedGroupCandidateBundles {
		return errors.New("edge-control persistent candidate history exceeds its retention bound")
	}
	previousEpoch := uint64(0)
	for index := range state.CandidateHistory {
		candidate := state.CandidateHistory[index]
		if candidate.Epoch <= previousEpoch || (state.Candidate != nil && candidate.Epoch >= state.Candidate.Epoch) {
			return errors.New("edge-control persistent candidate history order is invalid")
		}
		if err := validatePersistentCandidateBinding(state, groupID, candidate); err != nil {
			return err
		}
		previousEpoch = candidate.Epoch
	}
	return nil
}

func validatePersistentCandidateBinding(state persistentGroupState, groupID string, candidate GroupCandidateBundle) error {
	if err := validateGroupCandidateBundle(groupID, candidate); err != nil {
		return err
	}
	if candidate.CandidateLedgerSequence == 0 || candidate.CandidateLedgerSequence > uint64(len(state.Ledger)) ||
		state.Ledger[candidate.CandidateLedgerSequence-1].Bundle == nil || state.Ledger[candidate.CandidateLedgerSequence-1].BundleArchived ||
		state.Ledger[candidate.CandidateLedgerSequence-1].RouteIntentGeneration != candidate.RouteIntentGeneration ||
		state.Ledger[candidate.CandidateLedgerSequence-1].InventoryGeneration != candidate.InventoryGeneration ||
		state.Ledger[candidate.CandidateLedgerSequence-1].InventoryDigest != candidate.Record.InventoryDigest ||
		(state.Ledger[candidate.CandidateLedgerSequence-1].ActiveSlot != "a" && state.Ledger[candidate.CandidateLedgerSequence-1].ActiveSlot != "b") ||
		groupAuthorityCandidateDigest(*state.Ledger[candidate.CandidateLedgerSequence-1].Bundle) != groupAuthorityCandidateDigest(candidate.Bundle) {
		return errors.New("edge-control persistent candidate is not bound to the group shadow ledger")
	}
	if candidate.ServingAuthority != nil {
		authority, servingCandidate, exists := persistentPublishedCandidateByVersion(&state, candidate.ServingAuthority.BundleVersion)
		fallback := !exists && state.Published != nil &&
			servingAuthorityCanUseCurrentPublishedFallback(candidate.ServingAuthority.BundleVersion, state.Published.Bundle.Generation,
				state.Published.PublicationSequence, state.Published.RecoveryEpoch,
				candidate.AllowDegradedPrevious && !candidate.StandbyOnly) &&
			state.Published.CandidateLedgerSequence == candidate.CandidateLedgerSequence
		if !fallback && (!exists || authority.CandidateLedgerSequence != candidate.CandidateLedgerSequence ||
			servingCandidate.ActiveSlot != candidate.ServingAuthority.WorkerSlot) {
			return errors.New("edge-control persistent candidate lost its serving authority witness")
		}
	}
	return nil
}

func persistentGroupStateDigest(state persistentGroupState) string {
	state.Digest = ""
	payload, err := json.Marshal(state)
	if err != nil {
		panic(fmt.Sprintf("encode edge-control persistent group digest: %v", err))
	}
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func migrateLegacyCandidateAuthoritySequence(state persistentGroupState, groupID string) (persistentGroupState, bool) {
	if state.Candidate == nil || state.Candidate.AuthorityLedgerSequence != 0 || len(state.AuthorityLedger) == 0 || state.Published == nil ||
		state.AuthorityLedger[len(state.AuthorityLedger)-1].Sequence == 0 ||
		state.AuthorityLedger[len(state.AuthorityLedger)-1].Sequence != state.Published.PublicationSequence ||
		state.Digest != legacyCandidatePersistentGroupStateDigest(state) {
		return persistentGroupState{}, false
	}
	migrated := clonePersistentGroupState(state)
	migrated.Candidate.AuthorityLedgerSequence = migrated.AuthorityLedger[len(migrated.AuthorityLedger)-1].Sequence
	migrated.Digest = persistentGroupStateDigest(migrated)
	if validatePersistentGroupState(migrated, groupID) != nil {
		return persistentGroupState{}, false
	}
	return migrated, true
}

func legacyCandidatePersistentGroupStateDigest(state persistentGroupState) string {
	legacy := legacyCandidatePersistentGroupState{
		Schema: state.Schema, GroupID: state.GroupID, Revision: state.Revision, Inventory: state.Inventory,
		InventoryProducer: state.InventoryProducer, Ledger: state.Ledger, AuthorityLedger: state.AuthorityLedger,
		Published: state.Published,
	}
	if state.Candidate != nil {
		candidate := state.Candidate
		legacy.Candidate = &legacyGroupCandidateBundle{
			Schema: candidate.Schema, GroupID: candidate.GroupID, Epoch: candidate.Epoch,
			CandidateLedgerSequence: candidate.CandidateLedgerSequence, RouteIntentGeneration: candidate.RouteIntentGeneration,
			InventoryGeneration: candidate.InventoryGeneration, ReleaseRecordDigest: candidate.ReleaseRecordDigest,
			WorkerSlot: candidate.WorkerSlot, PublishedAt: candidate.PublishedAt, CurrentRecord: candidate.CurrentRecord,
			CurrentBundle: candidate.CurrentBundle, CurrentWorkerSlot: candidate.CurrentWorkerSlot, Record: candidate.Record, Bundle: candidate.Bundle,
		}
	}
	payload, err := json.Marshal(legacy)
	if err != nil {
		panic(fmt.Sprintf("encode legacy edge-control persistent group digest: %v", err))
	}
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func clonePersistentGroupState(state persistentGroupState) persistentGroupState {
	if state.Inventory != nil {
		inventory := cloneGroupInventorySnapshot(*state.Inventory)
		state.Inventory = &inventory
	}
	if state.InventoryProducer != nil {
		producer := cloneGroupInventoryProducerState(*state.InventoryProducer)
		state.InventoryProducer = &producer
	}
	originalLedger := state.Ledger
	state.Ledger = make([]GroupShadowLedgerEntry, len(originalLedger))
	for index := range originalLedger {
		state.Ledger[index] = cloneGroupShadowLedgerEntry(originalLedger[index])
	}
	state.AuthorityLedger = append([]GroupAuthorityLedgerEntry(nil), state.AuthorityLedger...)
	if state.Published != nil {
		published := cloneGroupPublishedBundle(*state.Published)
		state.Published = &published
	}
	if state.Candidate != nil {
		candidate := cloneGroupCandidateBundle(*state.Candidate)
		state.Candidate = &candidate
	}
	if len(state.CandidateHistory) > 0 {
		history := make([]GroupCandidateBundle, len(state.CandidateHistory))
		for index := range state.CandidateHistory {
			history[index] = cloneGroupCandidateBundle(state.CandidateHistory[index])
		}
		state.CandidateHistory = history
	}
	return state
}

func cloneGroupInventorySnapshot(snapshot GroupInventorySnapshot) GroupInventorySnapshot {
	snapshot.Instances = append([]GroupInstance(nil), snapshot.Instances...)
	return snapshot
}
