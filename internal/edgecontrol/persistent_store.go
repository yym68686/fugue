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
	"syscall"

	"fugue/internal/model"
)

const (
	persistentGroupStateSchemaV1 = "edge-control-persistent-group-state/v1"
	maxPersistentGroupStateBytes = 32 << 20
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
	Digest            string                       `json:"digest"`
}

// PersistentGroupStore owns one checksummed, atomically replaced state file
// and one CAS sequence per edge group. A corrupt group file cannot prevent any
// other group from being read or advanced.
type PersistentGroupStore struct {
	root string
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
	return &PersistentGroupStore{root: root}, nil
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
		if candidate.Status != GroupShadowStatusCompiled || candidate.Bundle == nil || candidate.BundleGeneration != generation {
			return errors.New("edge-control recovery target candidate is invalid")
		}
		return nil
	})
	return authority, candidate, recoveryEpoch, err
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
		var current *GroupPublishedBundle
		if state.Published != nil {
			value := cloneGroupPublishedBundle(*state.Published)
			current = &value
		}
		var next *GroupPublishedBundle
		var err error
		appended, next, err = prepareGroupAuthorityAppend(state.GroupID, expectedSequence, state.AuthorityLedger, current, &candidate, entry, &signed)
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
	state.Revision++
	return store.writeGroupState(statePath, state)
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
		return persistentGroupState{}, err
	}
	return clonePersistentGroupState(state), nil
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
		candidate := cloneGroupShadowLedgerEntry(persisted)
		if candidate.Sequence != uint64(index+1) {
			return errors.New("edge-control persistent group ledger sequence is invalid")
		}
		candidate.Sequence = 0
		appended, err := prepareGroupShadowLedgerAppend(groupID, uint64(index), validated, candidate)
		if err != nil || appended.Sequence != persisted.Sequence {
			return errors.New("edge-control persistent group ledger transition is invalid")
		}
		validated = append(validated, appended)
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
			if candidate.Status != GroupShadowStatusCompiled || candidate.Bundle == nil || candidate.BundleGeneration != entry.BundleGeneration {
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
		if state.Published.PublicationSequence != lastPublishedSequence || state.Published.CandidateLedgerSequence != lastPublishedCandidate ||
			state.Published.Bundle.Generation != lastPublishedGeneration || state.Published.Digest != lastPublishedDigest || state.Published.RecoveryEpoch != lastRecoveryEpoch ||
			lastPublishedCandidate == 0 || lastPublishedCandidate > uint64(len(state.Ledger)) ||
			state.Ledger[lastPublishedCandidate-1].Bundle == nil ||
			groupAuthorityCandidateDigest(*state.Ledger[lastPublishedCandidate-1].Bundle) != groupAuthorityCandidateDigest(state.Published.Bundle) {
			return errors.New("edge-control persistent group published head does not match authority ledger")
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
	return state
}

func cloneGroupInventorySnapshot(snapshot GroupInventorySnapshot) GroupInventorySnapshot {
	snapshot.Instances = append([]GroupInstance(nil), snapshot.Instances...)
	return snapshot
}
