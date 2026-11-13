package edgecontrol

import (
	"context"
	"errors"
	"fugue/internal/inputarchive"
	"sort"
	"time"
)

// Compatibility for persisted opt-in states. New remote registrations are
// retired. Required legacy states stay fail-closed rather than silently
// discarding their retention contract.
var ErrGroupPendingRoot = errors.New("edge-control legacy retention registration unavailable")

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
