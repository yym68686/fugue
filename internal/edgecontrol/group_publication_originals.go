package edgecontrol

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
)

// Use the existing retention failure path so rejected refreshes still retain
// their mandatory, deduplicated failure audit independently of the registry.
var ErrGroupPublicationOriginal = fmt.Errorf("%w: original publication custody unavailable", ErrGroupPendingRoot)

// The current published pointer is renewable and candidate bodies can be
// compacted. Neither is durable custody of an earlier exact signed issuance.
// Capture only the NEW native publication, before the same atomic replacement
// as its authority ledger. Never seal, reconstruct or backfill a legacy body.
// The existing opt-in input capture remains OFF by default.
func (store *PersistentGroupStore) retainGroupPublicationOriginal(ctx context.Context, before *GroupPublishedBundle, state *persistentGroupState) error {
	if state.Published == nil || before != nil && sameGroupCompileValue(before, state.Published) {
		return nil
	}
	if !state.CompileInputsRequired && state.PublicationOriginalsFromSequence == 0 {
		return nil
	}
	if ctx.Err() != nil || store.compileInputKeyringDir == "" || !state.CompileInputAttestationsRequired || len(state.PublicationOriginals) >= 32 {
		return ErrGroupPublicationOriginal
	}
	pub := state.Published
	if pub.PublicationSequence == 0 || pub.PublicationSequence > uint64(len(state.AuthorityLedger)) {
		return ErrGroupPublicationOriginal
	}
	path := filepath.Join(store.compileInputKeyringDir, state.GroupID+".json")
	keys, err := loadGroupBundleSigningKeyring(path, state.GroupID)
	if err != nil {
		return ErrGroupPublicationOriginal
	}
	matched := false
	for _, input := range state.RetentionInputs {
		if input.Entry.Sequence != pub.CandidateLedgerSequence {
			continue
		}
		if _, err := bindGroupOriginalPublication(state.GroupID, input, state.AuthorityLedger[pub.PublicationSequence-1], pub.Bundle, pub.RecoveryEpoch, keys); err != nil {
			return ErrGroupPublicationOriginal
		}
		matched = true
		break
	}
	if !matched {
		return ErrGroupPublicationOriginal
	}
	state.PublicationOriginals = append(state.PublicationOriginals, cloneGroupPublishedBundle(*pub))
	if state.PublicationOriginalsFromSequence == 0 {
		state.PublicationOriginalsFromSequence = pub.PublicationSequence
	}
	if validateGroupPublicationOriginals(*state) != nil {
		return ErrGroupPublicationOriginal
	}
	after, err := loadGroupBundleSigningKeyring(path, state.GroupID)
	if err != nil || ctx.Err() != nil || !reflect.DeepEqual(keys, after) {
		return ErrGroupPublicationOriginal
	}
	return nil
}

// Read-time integrity is independent of current key revocation. Original
// signature authentication is repeated under current trust when exporting or
// admitting a root; a file checksum alone never grants that authority.
func validateGroupPublicationOriginals(state persistentGroupState) error {
	if len(state.PublicationOriginals) == 0 {
		if state.PublicationOriginalsFromSequence != 0 {
			return ErrGroupPublicationOriginal
		}
		return nil
	}
	if state.PublicationOriginalsFromSequence == 0 || state.PublicationOriginals[0].PublicationSequence != state.PublicationOriginalsFromSequence || !state.CompileInputAttestationsRequired || len(state.PublicationOriginals) > 32 {
		return ErrGroupPublicationOriginal
	}
	// Inputs and signed outputs share the existing custody batch budget.
	total := 0
	for _, input := range state.RetentionInputs {
		raw, err := json.Marshal(input)
		if err != nil || len(raw) > groupOriginalRecordBytes || len(raw) > groupOriginalBatchBytes-total {
			return ErrGroupPublicationOriginal
		}
		total += len(raw)
	}
	previous, epoch, next := uint64(0), uint64(0), 0
	for i, entry := range state.AuthorityLedger {
		if entry.RecoveryEpoch != 0 {
			epoch = entry.RecoveryEpoch
		}
		sequence := uint64(i + 1)
		if next == len(state.PublicationOriginals) {
			if entry.Status == GroupAuthorityStatusPublished && sequence > previous {
				return ErrGroupPublicationOriginal // no later unrecorded issuance
			}
			continue
		}
		original := state.PublicationOriginals[next]
		if original.PublicationSequence > sequence {
			if next > 0 && entry.Status == GroupAuthorityStatusPublished {
				return ErrGroupPublicationOriginal
			}
			continue // legacy prefix stays missing
		}
		if original.PublicationSequence != sequence || sequence <= previous || validateGroupPublishedBundle(state.GroupID, original) != nil ||
			original.RecoveryEpoch != epoch || original.CandidateLedgerSequence != entry.CandidateLedgerSequence ||
			entry.Status != GroupAuthorityStatusPublished || original.Bundle.Generation != entry.BundleGeneration ||
			original.Digest != entry.PublishedBundleDigest || original.Bundle.KeyID != entry.SigningKeyID ||
			!original.PublishedAt.Equal(entry.RecordedAt) || !original.Bundle.GeneratedAt.Equal(entry.RecordedAt) {
			return ErrGroupPublicationOriginal
		}
		matched := false
		for _, input := range state.RetentionInputs {
			if input.Entry.Sequence == original.CandidateLedgerSequence && input.Entry.Bundle != nil && sameGroupBundleGeneration(*input.Entry.Bundle, original.Bundle) {
				matched = true
				break
			}
		}
		raw, err := json.Marshal(original)
		if !matched || err != nil || len(raw) > groupOriginalRecordBytes || len(raw) > groupOriginalBatchBytes-total {
			return ErrGroupPublicationOriginal
		}
		total += len(raw)
		previous, next = sequence, next+1
	}
	if next != len(state.PublicationOriginals) || state.Published == nil || !sameGroupCompileValue(state.PublicationOriginals[next-1], *state.Published) {
		return ErrGroupPublicationOriginal
	}
	return nil
}
