package edgecontrol

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"fugue/internal/inputarchive"
)

var ErrGroupPendingRoot = errors.New("edge-control pending retention registration unavailable")

// Implementations must authenticate and bind this exact owner/source through
// the independent registry transport. A receipt is retention acknowledgement,
// never complete source custody, activation, pin or retirement authority.
// No command/production transport opts into this capability implicitly.
type GroupPendingRootRegistrar interface {
	RetainPendingCompilerInputExternalRoot(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootReceipt, error)
}

type GroupPendingRootRecord struct {
	Generation   string                          `json:"generation"`
	SourceDigest string                          `json:"source_digest"`
	Receipt      inputarchive.PendingRootReceipt `json:"receipt"`
}

func pendingGroupRootRegistration(group string, binding GroupOriginalPublicationBinding) (inputarchive.PendingRootRegistration, error) {
	if group == "" || binding.GroupID != group || !binding.DependencyBindingKnown || len(binding.References) != 4 {
		return inputarchive.PendingRootRegistration{}, ErrGroupPendingRoot
	}
	r := inputarchive.PendingRootRegistration{Schema: inputarchive.PendingRootSchema, Owner: "edge-control/" + group,
		Generation: binding.Generation, SourceDigest: binding.OriginalInputDigest, References: append([]inputarchive.HotReference(nil), binding.References...)}
	sort.Slice(r.References, func(i, j int) bool {
		a, b := r.References[i], r.References[j]
		return a.Kind < b.Kind || a.Kind == b.Kind && a.ID < b.ID
	})
	if _, _, err := r.Canonical(); err != nil {
		return inputarchive.PendingRootRegistration{}, ErrGroupPendingRoot
	}
	return r, nil
}

// Runs inside the native local writer CAS, after candidate/publication checks
// but BEFORE its atomic file replacement. Covers normal publication, candidate
// promotion and recovery uniformly. Failure leaves the old LKG and all audit
// roots intact; a remote commit followed by local failure leaves a protected
// orphan. There is deliberately no compensating delete or automatic expiry.
func (store *PersistentGroupStore) retainPendingGroupPublication(ctx context.Context, before *GroupPublishedBundle, state *persistentGroupState) error {
	if state.Published == nil || before != nil && sameGroupCompileValue(before, state.Published) {
		return nil
	}
	if !state.PendingRootsRequired && store.pendingRootRegistrar == nil {
		return nil // existing serving policy, capability remains OFF
	}
	if store.pendingRootRegistrar == nil || !state.CompileInputAttestationsRequired || len(state.PendingRootRecords) > 32 {
		return ErrGroupPendingRoot
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	pub := state.Published
	if pub.PublicationSequence == 0 || pub.PublicationSequence > uint64(len(state.AuthorityLedger)) {
		return ErrGroupPendingRoot
	}
	var input *GroupCompileInput
	for i := range state.RetentionInputs {
		if state.RetentionInputs[i].Entry.Sequence == pub.CandidateLedgerSequence {
			input = &state.RetentionInputs[i]
			break
		}
	}
	if input == nil {
		return ErrGroupPendingRoot // old missing source is never backfilled
	}
	path := filepath.Join(store.compileInputKeyringDir, state.GroupID+".json")
	keys, err := loadGroupBundleSigningKeyring(path, state.GroupID)
	if err != nil {
		return ErrGroupPendingRoot
	}
	binding, err := bindGroupOriginalPublication(state.GroupID, *input, state.AuthorityLedger[pub.PublicationSequence-1], pub.Bundle, pub.RecoveryEpoch, keys)
	if err != nil {
		return ErrGroupPendingRoot
	}
	registration, err := pendingGroupRootRegistration(state.GroupID, binding)
	if err != nil {
		return err
	}
	var receipt inputarchive.PendingRootReceipt
	found := false
	for _, record := range state.PendingRootRecords {
		if record.Generation == registration.Generation {
			if record.SourceDigest != registration.SourceDigest || !record.Receipt.Matches(registration) {
				return ErrGroupPendingRoot
			}
			receipt, found = record.Receipt, true
			break
		}
	}
	if !found {
		if len(state.PendingRootRecords) >= 32 {
			return ErrGroupPendingRoot
		}
		request, requestErr := pendingGroupRootRequest(state.GroupID, *input, state.AuthorityLedger[pub.PublicationSequence-1], pub.Bundle, pub.RecoveryEpoch, binding)
		if requestErr != nil {
			return ErrGroupPendingRoot
		}
		receipt, err = store.pendingRootRegistrar.RetainPendingCompilerInputExternalRoot(ctx, request)
		if err != nil || !receipt.Matches(registration) {
			return ErrGroupPendingRoot
		}
	}
	// Revocation/rotation while the remote retention operation runs refuses
	// local publication. The already-persisted remote root remains protected.
	after, err := loadGroupBundleSigningKeyring(path, state.GroupID)
	if err != nil || ctx.Err() != nil || !reflect.DeepEqual(keys, after) {
		return ErrGroupPendingRoot
	}
	if !found {
		state.PendingRootRecords = append(state.PendingRootRecords, GroupPendingRootRecord{registration.Generation, registration.SourceDigest, receipt})
	}
	state.PendingRootsRequired = true
	return nil
}

func validateGroupPendingRoots(state persistentGroupState) error {
	if len(state.PendingRootRecords) == 0 {
		if state.PendingRootsRequired {
			return ErrGroupPendingRoot
		}
		return nil
	}
	if !state.PendingRootsRequired || !state.CompileInputAttestationsRequired || len(state.PendingRootRecords) > 32 {
		return ErrGroupPendingRoot
	}
	seen := map[string]bool{}
	for _, record := range state.PendingRootRecords {
		if seen[record.Generation] || record.Receipt.RegistryRevision <= 0 || record.Receipt.SourceEpoch < 0 {
			return ErrGroupPendingRoot
		}
		seen[record.Generation] = true
		matched := false
		for _, input := range state.RetentionInputs {
			if input.Entry.BundleGeneration != record.Generation || digestJSON(input) != record.SourceDigest || input.RouteIntent.TrafficRelease == nil {
				continue
			}
			b := input.RouteIntent.TrafficRelease
			binding := GroupOriginalPublicationBinding{GroupID: state.GroupID, Generation: record.Generation, OriginalInputDigest: record.SourceDigest, DependencyBindingKnown: true,
				References: []inputarchive.HotReference{{Kind: "artifact", ID: b.ReleaseSetID, ExpectedHash: b.ReleaseSetDigest}, {Kind: "artifact", ID: b.RouteArtifactID, ExpectedHash: b.RouteArtifactDigest},
					{Kind: "release", ID: b.ReleaseID}, {Kind: "input", ID: b.InputSnapshotDigest, ExpectedHash: b.InputSnapshotDigest}}}
			r, err := pendingGroupRootRegistration(state.GroupID, binding)
			if err != nil || !record.Receipt.Matches(r) {
				return ErrGroupPendingRoot
			}
			matched = true
			break
		}
		if !matched {
			return ErrGroupPendingRoot
		}
	}
	if state.Published == nil || !seen[state.Published.Bundle.Generation] {
		return ErrGroupPendingRoot
	}
	return nil
}

// Retention refusal must not depend on that unavailable registry to retain its
// mandatory failure audit. The native CAS checks the observed head and keeps
// the exact published pointer. Unchanged refusal is deduplicated as elsewhere.
func (publisher GroupAuthorityPublisher) recordPendingRootRefreshFailure(ctx context.Context, group string, observed GroupAuthorityState, now time.Time) GroupAuthorityResult {
	if observed.LedgerHead.Status == GroupAuthorityStatusFailed && observed.LedgerHead.FailureCode == GroupAuthorityFailurePendingRetention &&
		observed.LedgerHead.CandidateLedgerSequence == observed.Published.CandidateLedgerSequence && observed.LedgerHead.LastPublishedBundleGeneration == observed.Published.Bundle.Generation {
		return authorityResultFromEntry(observed.LedgerHead)
	}
	entry := GroupAuthorityLedgerEntry{Schema: GroupAuthorityLedgerSchemaV1, GroupID: group, Status: GroupAuthorityStatusFailed,
		CandidateLedgerSequence: observed.Published.CandidateLedgerSequence, RouteIntentGeneration: observed.LedgerHead.RouteIntentGeneration,
		LastPublishedBundleGeneration: observed.Published.Bundle.Generation, FailureCode: GroupAuthorityFailurePendingRetention,
		Authority: "edge-control", PublicationEnabled: true, RecordedAt: now}
	// A newer shadow candidate may exist; this audit is bound to the current
	// authority head and published source, never an assertion about that head.
	appended, err := publisher.Store.AppendGroupAuthorityCAS(ctx, group, observed.LedgerHead.Sequence, 0, entry, nil)
	if err != nil {
		return GroupAuthorityResult{GroupID: group, Status: GroupAuthorityStatusFailed, PublicationSequence: observed.LedgerHead.Sequence,
			CandidateLedgerSequence: observed.Published.CandidateLedgerSequence, LastPublishedBundleGeneration: observed.Published.Bundle.Generation, FailureCode: GroupAuthorityFailurePublicationCAS}
	}
	return authorityResultFromEntry(appended)
}
