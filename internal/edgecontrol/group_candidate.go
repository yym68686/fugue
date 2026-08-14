package edgecontrol

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"fugue/internal/edgeauthority"
	"fugue/internal/model"
)

const (
	GroupCandidateBundleSchemaV1 = "edge-control-group-candidate-bundle/v1"
	GroupCandidateBatchSchemaV1  = "edge-control-group-candidate-batch/v1"

	GroupCandidateStatusPublished = "candidate_published"
	GroupCandidateStatusFailed    = "candidate_failed"
)

var groupCandidateSourcePattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

// CandidateReleaseIdentity is immutable provenance injected by the
// declarative renderer. It lets Edge Control bind a signed route candidate to
// the exact source, OCI, manifest and component artifact that produced it.
type CandidateReleaseIdentity struct {
	SourceSHA            string
	ControlImageDigest   string
	ManifestDigest       string
	HealthContractDigest string
	ReleaseRecordDigest  string
}

func (identity CandidateReleaseIdentity) Validate() error {
	if !groupCandidateSourcePattern.MatchString(identity.SourceSHA) ||
		!groupAuthorityDigestPattern.MatchString(identity.ControlImageDigest) ||
		!groupAuthorityDigestPattern.MatchString(identity.ManifestDigest) ||
		!groupAuthorityDigestPattern.MatchString(identity.HealthContractDigest) ||
		!groupAuthorityDigestPattern.MatchString(identity.ReleaseRecordDigest) {
		return errors.New("edge-control candidate release identity is invalid")
	}
	return nil
}

// GroupCandidateBundle is the durable inactive publication for one group. It
// can be loaded by an inactive Worker but cannot grant ordinary traffic.
type GroupCandidateBundle struct {
	Schema                  string                           `json:"schema"`
	GroupID                 string                           `json:"edge_group_id"`
	Epoch                   uint64                           `json:"epoch"`
	AuthorityLedgerSequence uint64                           `json:"authority_ledger_sequence"`
	CandidateLedgerSequence uint64                           `json:"candidate_ledger_sequence"`
	RouteIntentGeneration   string                           `json:"route_intent_generation"`
	InventoryGeneration     string                           `json:"inventory_generation"`
	ReleaseRecordDigest     string                           `json:"release_record_digest"`
	WorkerSourceSHA         string                           `json:"worker_source_sha,omitempty"`
	WorkerImageDigest       string                           `json:"worker_image_digest,omitempty"`
	WorkerSlot              string                           `json:"worker_slot"`
	PublishedAt             time.Time                        `json:"published_at"`
	CurrentRecord           *edgeauthority.RouteBundleRecord `json:"current_record,omitempty"`
	CurrentBundle           *model.EdgeRouteBundle           `json:"current_bundle,omitempty"`
	CurrentWorkerSlot       string                           `json:"current_worker_slot,omitempty"`
	ServingAuthority        *GroupServingAuthorityWitness    `json:"serving_authority,omitempty"`
	Record                  edgeauthority.RouteBundleRecord  `json:"record"`
	Bundle                  model.EdgeRouteBundle            `json:"bundle"`
}

type GroupCandidateStore interface {
	Head(context.Context, string) (GroupShadowLedgerEntry, bool, error)
	History(context.Context, string) ([]GroupShadowLedgerEntry, error)
	ReadGroupInventory(context.Context, string) (GroupInventorySnapshot, error)
	ReadGroupAuthority(context.Context, string) (GroupAuthorityState, error)
	ReadGroupCandidate(context.Context, string) (GroupCandidateBundle, bool, error)
	ReadGroupCandidateStage(context.Context, string, string) (GroupCandidateStageSnapshot, error)
	PutGroupCandidateCAS(context.Context, string, uint64, uint64, GroupCandidateBundle) (GroupCandidateBundle, error)
	PutGroupCurrentLKGCandidateCAS(context.Context, string, uint64, uint64, uint64, string, GroupCandidateBundle) (GroupCandidateBundle, error)
	PutGroupStagedCurrentLKGCandidateCAS(context.Context, string, uint64, uint64, uint64, uint64, string, *GroupServingAuthorityWitness, GroupCandidateBundle) (GroupCandidateBundle, error)
}

// GroupCandidateStageSnapshot is the exact small projection required to bind
// an inactive Worker candidate. Persistent stores derive it from one fully
// validated group-state revision so staging cannot combine separate reads.
type GroupCandidateStageSnapshot struct {
	Authority          GroupAuthorityState
	Candidate          GroupCandidateBundle
	CandidateExists    bool
	Inventory          GroupInventorySnapshot
	InventoryExists    bool
	PublishedCandidate GroupShadowLedgerEntry
	ServingAuthority   GroupAuthorityLedgerEntry
	ServingCandidate   GroupShadowLedgerEntry
	ServingExists      bool
}

type GroupCandidatePublisher struct {
	Store      GroupCandidateStore
	Signer     GroupBundleSigner
	CurrentLKG *GroupAuthorityPublisher
	Identity   CandidateReleaseIdentity
	Now        func() time.Time
}

type GroupCandidateResult struct {
	GroupID                 string `json:"edge_group_id"`
	Status                  string `json:"status"`
	Epoch                   uint64 `json:"epoch,omitempty"`
	CandidateLedgerSequence uint64 `json:"candidate_ledger_sequence,omitempty"`
	RecordDigest            string `json:"record_digest,omitempty"`
	FailureCode             string `json:"failure_code,omitempty"`
}

type GroupCandidateBatch struct {
	Schema                string                 `json:"schema"`
	RouteIntentGeneration string                 `json:"route_intent_generation"`
	Results               []GroupCandidateResult `json:"results"`
	Published             int                    `json:"published"`
	Failed                int                    `json:"failed"`
}

func (publisher GroupCandidatePublisher) Publish(ctx context.Context, compiled GroupShadowBatch) (GroupCandidateBatch, error) {
	if publisher.Store == nil || publisher.Signer == nil || publisher.CurrentLKG == nil || publisher.Identity.Validate() != nil {
		return GroupCandidateBatch{}, errors.New("edge-control candidate publisher configuration is invalid")
	}
	if compiled.Schema != GroupShadowBatchSchemaV1 || strings.TrimSpace(compiled.RouteIntentGeneration) == "" || len(compiled.Results) == 0 {
		return GroupCandidateBatch{}, errors.New("edge-control candidate compiler batch is invalid")
	}
	results := append([]GroupShadowResult(nil), compiled.Results...)
	sort.Slice(results, func(i, j int) bool { return results[i].GroupID < results[j].GroupID })
	batch := GroupCandidateBatch{Schema: GroupCandidateBatchSchemaV1, RouteIntentGeneration: compiled.RouteIntentGeneration, Results: make([]GroupCandidateResult, len(results))}
	now := time.Now().UTC()
	if publisher.Now != nil {
		now = publisher.Now().UTC()
	}
	var wait sync.WaitGroup
	for index, result := range results {
		wait.Add(1)
		go func() {
			defer wait.Done()
			batch.Results[index] = publisher.publishGroup(ctx, result, now)
		}()
	}
	wait.Wait()
	for _, result := range batch.Results {
		if result.Status == GroupCandidateStatusPublished {
			batch.Published++
		} else {
			batch.Failed++
		}
	}
	for index, result := range results {
		published := batch.Results[index].Status == GroupCandidateStatusPublished
		if (result.Status == GroupShadowStatusCompiled && !published) ||
			(result.Status == GroupShadowStatusFailed && result.FailureCode != GroupShadowFailureNoHealthyActive && published) {
			return GroupCandidateBatch{}, errors.New("edge-control candidate batch result is invalid")
		}
	}
	if batch.Published+batch.Failed != len(results) {
		return GroupCandidateBatch{}, errors.New("edge-control candidate batch counts are invalid")
	}
	return batch, nil
}

func (publisher GroupCandidatePublisher) publishGroup(ctx context.Context, compiled GroupShadowResult, now time.Time) GroupCandidateResult {
	groupID := normalizeGroupID(compiled.GroupID)
	failed := func(code string) GroupCandidateResult {
		return GroupCandidateResult{GroupID: groupID, Status: GroupCandidateStatusFailed, FailureCode: code}
	}
	if groupID == "" || compiled.Status != GroupShadowStatusCompiled || compiled.LedgerSequence == 0 || strings.TrimSpace(compiled.BundleGeneration) == "" {
		// A candidate publisher may restart while workers are deliberately
		// serving the signed group LKG and the newest shadow compilation is
		// therefore unhealthy. Rebuild the inactive candidate from the exact
		// immutable ledger entry that backs the current publication. This never
		// advances CurrentAuthority: it only replaces the candidate pointer with
		// a release-bound envelope that can be independently loaded and probed.
		if strings.TrimSpace(compiled.FailureCode) == GroupShadowFailureNoHealthyActive {
			if _, err := publisher.CurrentLKG.RefreshPublishedLKG(ctx, groupID, now); err != nil {
				return failed(GroupAuthorityFailurePublicationCAS)
			}
			if rebuilt, ok := publisher.publishCurrentLKGCandidate(ctx, groupID, now); ok {
				return rebuilt
			}
		}
		code := strings.TrimSpace(compiled.FailureCode)
		if code == "" {
			code = GroupAuthorityFailureCandidateRead
		}
		return failed(code)
	}
	if _, err := publisher.CurrentLKG.RefreshPublishedLKG(ctx, groupID, now); err != nil {
		return failed(GroupAuthorityFailurePublicationCAS)
	}
	head, exists, err := publisher.Store.Head(ctx, groupID)
	if err != nil || !exists || head.Status != GroupShadowStatusCompiled || head.Sequence != compiled.LedgerSequence ||
		head.Bundle == nil || head.BundleGeneration != compiled.BundleGeneration || !groupAuthorityDigestPattern.MatchString(head.InventoryDigest) {
		return failed(GroupAuthorityFailureCandidateCAS)
	}
	previous, previousExists, err := publisher.Store.ReadGroupCandidate(ctx, groupID)
	if err != nil {
		return failed(GroupAuthorityFailureCandidateRead)
	}
	authority, err := publisher.Store.ReadGroupAuthority(ctx, groupID)
	if err != nil || !authority.PublishedExists || validateGroupPublishedBundle(groupID, authority.Published) != nil {
		return failed(GroupAuthorityFailureCandidateRead)
	}
	inventory, err := publisher.Store.ReadGroupInventory(ctx, groupID)
	if err != nil || inventory.GroupID != groupID || inventory.Sequence == 0 || strings.TrimSpace(inventory.Generation) == "" ||
		(inventory.ActiveEpoch.Slot != "a" && inventory.ActiveEpoch.Slot != "b") || inventory.ObservedAt.IsZero() ||
		inventory.ObservedAt.After(now.Add(maxInventoryHeartbeatClockSkew)) || now.Sub(inventory.ObservedAt) > GroupInventoryHeartbeatMaxAge {
		return failed(GroupAuthorityFailureCandidateRead)
	}
	if previousExists && previous.CandidateLedgerSequence == head.Sequence && previous.ReleaseRecordDigest == publisher.Identity.ReleaseRecordDigest &&
		previous.CurrentWorkerSlot == inventory.ActiveEpoch.Slot && previous.WorkerSlot != inventory.ActiveEpoch.Slot &&
		candidateRecordMatchesIdentity(previous.Record, publisher.Identity) && candidateBindsCurrentAuthority(previous, authority) {
		lifetime := previous.Bundle.ValidUntil.Sub(previous.Bundle.GeneratedAt)
		if lifetime > 0 && previous.Bundle.ValidUntil.Sub(now) > lifetime/3 {
			return candidateResult(previous)
		}
	}
	if previousExists && candidateHasStagedWorkerIdentity(previous) && candidateBindsCurrentAuthority(previous, authority) &&
		previous.CurrentWorkerSlot == inventory.ActiveEpoch.Slot && previous.WorkerSlot != inventory.ActiveEpoch.Slot {
		lifetime := previous.Bundle.ValidUntil.Sub(previous.Bundle.GeneratedAt)
		if lifetime > 0 && previous.Bundle.ValidUntil.Sub(now) > lifetime/3 {
			return candidateResult(previous)
		}
	}
	history, err := publisher.Store.History(ctx, groupID)
	if err != nil || authority.Published.CandidateLedgerSequence == 0 || authority.Published.CandidateLedgerSequence > uint64(len(history)) {
		return failed(GroupAuthorityFailureCandidateRead)
	}
	currentLedger := history[authority.Published.CandidateLedgerSequence-1]
	if currentLedger.Sequence != authority.Published.CandidateLedgerSequence ||
		!groupAuthorityDigestPattern.MatchString(currentLedger.InventoryDigest) ||
		(currentLedger.ActiveSlot != "a" && currentLedger.ActiveSlot != "b") {
		return failed(GroupAuthorityFailureCandidateRead)
	}
	workerSlot := "a"
	if inventory.ActiveEpoch.Slot == "a" {
		workerSlot = "b"
	}
	return publisher.publishLedgerCandidate(ctx, groupID, head, authority, previous, previousExists, workerSlot, inventory.ActiveEpoch.Slot, now, false)
}

func (publisher GroupCandidatePublisher) publishLedgerCandidate(ctx context.Context, groupID string, head GroupShadowLedgerEntry, authority GroupAuthorityState, previous GroupCandidateBundle, previousExists bool, workerSlot, currentWorkerSlot string, now time.Time, fromCurrentLKG bool) GroupCandidateResult {
	failed := func(code string) GroupCandidateResult {
		return GroupCandidateResult{GroupID: groupID, Status: GroupCandidateStatusFailed, FailureCode: code}
	}
	epoch := authority.Published.PublicationSequence + 1
	if previousExists && previous.Epoch >= epoch {
		epoch = previous.Epoch + 1
	}
	bundle := cloneEdgeRouteBundle(*head.Bundle)
	history, err := publisher.Store.History(ctx, groupID)
	if err != nil || authority.Published.CandidateLedgerSequence == 0 || authority.Published.CandidateLedgerSequence > uint64(len(history)) {
		return failed(GroupAuthorityFailureCandidateRead)
	}
	currentLedger := history[authority.Published.CandidateLedgerSequence-1]
	if currentLedger.Sequence != authority.Published.CandidateLedgerSequence || !groupAuthorityDigestPattern.MatchString(currentLedger.InventoryDigest) ||
		(currentLedger.ActiveSlot != "a" && currentLedger.ActiveSlot != "b") {
		return failed(GroupAuthorityFailureCandidateRead)
	}
	if currentWorkerSlot == "" {
		currentWorkerSlot = currentLedger.ActiveSlot
	}
	if (currentWorkerSlot != "a" && currentWorkerSlot != "b") || currentWorkerSlot == workerSlot {
		return failed(GroupAuthorityFailureCandidateRead)
	}
	bundle.Issuer = groupAuthorityIssuer
	bundle.GeneratedAt = now
	bundle.ValidUntil = time.Time{}
	bundle.KeyID = ""
	bundle.Signature = ""
	bundle.Signatures = nil
	bundle.PreviousGeneration = authority.Published.Bundle.Generation
	bundle.Version = groupPublicationVersion(bundle.Generation, epoch, 0)
	signed, err := publisher.Signer.SignGroupBundle(ctx, groupID, bundle)
	if err != nil {
		return failed(GroupAuthorityFailureSigning)
	}
	record, err := (edgeauthority.RouteBundleRecord{
		GroupID: groupID, Epoch: int64(epoch), BundleDigest: signedGroupBundleDigest(signed),
		SourceSHA: publisher.Identity.SourceSHA, ControlImageDigest: publisher.Identity.ControlImageDigest,
		InventoryDigest: head.InventoryDigest, ManifestDigest: publisher.Identity.ManifestDigest,
		HealthContractDigest: publisher.Identity.HealthContractDigest, IssuedAt: now.Format(time.RFC3339Nano),
		KeyID: signed.KeyID, Signature: signed.Signature,
	}).Seal()
	if err != nil {
		return failed(GroupAuthorityFailureSigning)
	}
	currentRecord, err := (edgeauthority.RouteBundleRecord{
		GroupID: groupID, Epoch: int64(authority.Published.PublicationSequence), BundleDigest: authority.Published.Digest,
		SourceSHA: publisher.Identity.SourceSHA, ControlImageDigest: publisher.Identity.ControlImageDigest,
		InventoryDigest: currentLedger.InventoryDigest, ManifestDigest: publisher.Identity.ManifestDigest,
		HealthContractDigest: publisher.Identity.HealthContractDigest, IssuedAt: now.Format(time.RFC3339Nano),
		KeyID: authority.Published.Bundle.KeyID, Signature: authority.Published.Bundle.Signature,
	}).Seal()
	if err != nil {
		return failed(GroupAuthorityFailureSigning)
	}
	candidate := GroupCandidateBundle{
		Schema: GroupCandidateBundleSchemaV1, GroupID: groupID, Epoch: epoch,
		AuthorityLedgerSequence: authority.LedgerHead.Sequence,
		CandidateLedgerSequence: head.Sequence, RouteIntentGeneration: head.RouteIntentGeneration,
		InventoryGeneration: head.InventoryGeneration, ReleaseRecordDigest: publisher.Identity.ReleaseRecordDigest, WorkerSlot: workerSlot,
		PublishedAt: now, CurrentRecord: &currentRecord, CurrentBundle: &authority.Published.Bundle, CurrentWorkerSlot: currentWorkerSlot, Record: record, Bundle: signed,
	}
	expectedEpoch := uint64(0)
	if previousExists {
		expectedEpoch = previous.Epoch
	}
	var stored GroupCandidateBundle
	if fromCurrentLKG {
		stored, err = publisher.Store.PutGroupCurrentLKGCandidateCAS(ctx, groupID, expectedEpoch, head.Sequence,
			authority.Published.PublicationSequence, authority.Published.Digest, candidate)
	} else {
		stored, err = publisher.Store.PutGroupCandidateCAS(ctx, groupID, expectedEpoch, head.Sequence, candidate)
	}
	if err != nil {
		return failed(GroupAuthorityFailureCandidateCAS)
	}
	return candidateResult(stored)
}

func (publisher GroupCandidatePublisher) publishCurrentLKGCandidate(ctx context.Context, groupID string, now time.Time) (GroupCandidateResult, bool) {
	authority, err := publisher.Store.ReadGroupAuthority(ctx, groupID)
	if err != nil || !authority.PublishedExists || validateGroupPublishedBundle(groupID, authority.Published) != nil {
		return GroupCandidateResult{}, false
	}
	history, err := publisher.Store.History(ctx, groupID)
	sequence := authority.Published.CandidateLedgerSequence
	if err != nil || sequence == 0 || sequence > uint64(len(history)) {
		return GroupCandidateResult{}, false
	}
	head := history[sequence-1]
	if head.Sequence != sequence || head.Status != GroupShadowStatusCompiled || head.Bundle == nil || head.BundleArchived ||
		head.BundleGeneration != authority.Published.Bundle.Generation || !groupAuthorityDigestPattern.MatchString(head.InventoryDigest) ||
		(head.ActiveSlot != "a" && head.ActiveSlot != "b") {
		return GroupCandidateResult{}, false
	}
	inventory, err := publisher.Store.ReadGroupInventory(ctx, groupID)
	if err != nil || inventory.GroupID != groupID || inventory.Sequence == 0 || strings.TrimSpace(inventory.Generation) == "" ||
		(inventory.ActiveEpoch.Slot != "a" && inventory.ActiveEpoch.Slot != "b") || inventory.ObservedAt.IsZero() ||
		inventory.ObservedAt.After(now.Add(maxInventoryHeartbeatClockSkew)) || now.Sub(inventory.ObservedAt) > GroupInventoryHeartbeatMaxAge {
		return GroupCandidateResult{}, false
	}
	previous, previousExists, err := publisher.Store.ReadGroupCandidate(ctx, groupID)
	if err != nil {
		return GroupCandidateResult{}, false
	}
	if previousExists && previous.CandidateLedgerSequence == head.Sequence && previous.ReleaseRecordDigest == publisher.Identity.ReleaseRecordDigest &&
		previous.CurrentWorkerSlot == inventory.ActiveEpoch.Slot && previous.WorkerSlot != inventory.ActiveEpoch.Slot &&
		candidateRecordMatchesIdentity(previous.Record, publisher.Identity) && candidateBindsCurrentAuthority(previous, authority) {
		lifetime := previous.Bundle.ValidUntil.Sub(previous.Bundle.GeneratedAt)
		if lifetime > 0 && previous.Bundle.ValidUntil.Sub(now) > lifetime/3 {
			return candidateResult(previous), true
		}
	}
	if previousExists && candidateHasStagedWorkerIdentity(previous) && candidateBindsCurrentAuthority(previous, authority) &&
		previous.CurrentWorkerSlot == inventory.ActiveEpoch.Slot && previous.WorkerSlot != inventory.ActiveEpoch.Slot {
		lifetime := previous.Bundle.ValidUntil.Sub(previous.Bundle.GeneratedAt)
		if lifetime > 0 && previous.Bundle.ValidUntil.Sub(now) > lifetime/3 {
			return candidateResult(previous), true
		}
	}
	workerSlot := "a"
	if inventory.ActiveEpoch.Slot == "a" {
		workerSlot = "b"
	}
	// The immutable bundle LKG is backed by head, while the ordinary traffic
	// slot is backed by the latest signed inventory/Front activation epoch.
	// Keeping those witnesses separate prevents a historical bundle ledger
	// slot from being mistaken for live CurrentAuthority.
	return publisher.publishLedgerCandidate(ctx, groupID, head, authority, previous, previousExists, workerSlot, inventory.ActiveEpoch.Slot, now, true), true
}

func candidateBindsCurrentAuthority(candidate GroupCandidateBundle, authority GroupAuthorityState) bool {
	return authority.LedgerExists && authority.PublishedExists && candidate.CurrentRecord != nil && candidate.CurrentBundle != nil &&
		candidate.AuthorityLedgerSequence == authority.LedgerHead.Sequence &&
		candidate.Epoch > authority.Published.PublicationSequence && candidate.CurrentRecord.Epoch == int64(authority.Published.PublicationSequence) &&
		candidate.CurrentRecord.BundleDigest == authority.Published.Digest && candidate.CurrentBundle.Generation == authority.Published.Bundle.Generation &&
		signedGroupBundleDigest(*candidate.CurrentBundle) == authority.Published.Digest
}

func candidateRecordMatchesIdentity(record edgeauthority.RouteBundleRecord, identity CandidateReleaseIdentity) bool {
	return record.SourceSHA == identity.SourceSHA && record.ControlImageDigest == identity.ControlImageDigest &&
		record.ManifestDigest == identity.ManifestDigest && record.HealthContractDigest == identity.HealthContractDigest
}

func candidateHasStagedWorkerIdentity(candidate GroupCandidateBundle) bool {
	return groupCandidateSourcePattern.MatchString(candidate.WorkerSourceSHA) &&
		groupAuthorityDigestPattern.MatchString(candidate.WorkerImageDigest)
}

func candidateResult(candidate GroupCandidateBundle) GroupCandidateResult {
	return GroupCandidateResult{GroupID: candidate.GroupID, Status: GroupCandidateStatusPublished, Epoch: candidate.Epoch,
		CandidateLedgerSequence: candidate.CandidateLedgerSequence, RecordDigest: candidate.Record.RecordDigest}
}

func validateGroupCandidateBundle(groupID string, candidate GroupCandidateBundle) error {
	groupID = normalizeGroupID(groupID)
	if candidate.Schema != GroupCandidateBundleSchemaV1 || candidate.GroupID != groupID || candidate.Epoch == 0 ||
		candidate.AuthorityLedgerSequence == 0 || candidate.CandidateLedgerSequence == 0 || strings.TrimSpace(candidate.RouteIntentGeneration) == "" ||
		strings.TrimSpace(candidate.InventoryGeneration) == "" || !groupAuthorityDigestPattern.MatchString(candidate.ReleaseRecordDigest) ||
		candidate.PublishedAt.IsZero() || candidate.Record.Validate() != nil || candidate.Record.GroupID != groupID ||
		candidate.Record.Epoch != int64(candidate.Epoch) || candidate.Record.BundleDigest != signedGroupBundleDigest(candidate.Bundle) ||
		(candidate.WorkerSlot != "a" && candidate.WorkerSlot != "b") || candidate.Bundle.EdgeGroupID != groupID ||
		candidate.Bundle.Version != groupPublicationVersion(candidate.Bundle.Generation, candidate.Epoch, 0) ||
		candidate.Bundle.Issuer != groupAuthorityIssuer || strings.TrimSpace(candidate.Bundle.KeyID) == "" || strings.TrimSpace(candidate.Bundle.Signature) == "" ||
		candidate.Bundle.GeneratedAt.IsZero() || !candidate.Bundle.ValidUntil.After(candidate.Bundle.GeneratedAt) {
		return errors.New("edge-control group candidate bundle is invalid")
	}
	if (candidate.WorkerSourceSHA == "") != (candidate.WorkerImageDigest == "") ||
		(candidate.WorkerSourceSHA != "" && !candidateHasStagedWorkerIdentity(candidate)) {
		return errors.New("edge-control group candidate worker release identity is invalid")
	}
	if candidate.ServingAuthority != nil {
		if candidate.ServingAuthority.Validate() != nil || candidate.ServingAuthority.WorkerSlot != candidate.CurrentWorkerSlot ||
			candidate.ServingAuthority.WorkerSlot == candidate.WorkerSlot {
			return errors.New("edge-control group candidate serving authority witness is invalid")
		}
		generation, _, _, ok := parseGroupPublicationVersion(candidate.ServingAuthority.BundleVersion)
		if !ok || generation != candidate.Bundle.Generation {
			return errors.New("edge-control group candidate serving publication is invalid")
		}
	}
	if candidate.CurrentRecord == nil && candidate.CurrentBundle == nil && candidate.CurrentWorkerSlot == "" {
		return nil
	}
	if candidate.CurrentRecord == nil || candidate.CurrentBundle == nil || candidate.CurrentRecord.Validate() != nil || candidate.CurrentRecord.GroupID != groupID ||
		candidate.CurrentRecord.RecordDigest == candidate.Record.RecordDigest ||
		candidate.CurrentRecord.BundleDigest == candidate.Record.BundleDigest || candidate.CurrentRecord.Epoch >= candidate.Record.Epoch ||
		candidate.CurrentRecord.BundleDigest != signedGroupBundleDigest(*candidate.CurrentBundle) ||
		candidate.CurrentBundle.EdgeGroupID != groupID || candidate.CurrentBundle.Issuer != groupAuthorityIssuer ||
		strings.TrimSpace(candidate.CurrentBundle.KeyID) == "" || strings.TrimSpace(candidate.CurrentBundle.Signature) == "" ||
		candidate.CurrentBundle.GeneratedAt.IsZero() || !candidate.CurrentBundle.ValidUntil.After(candidate.CurrentBundle.GeneratedAt) ||
		candidate.CurrentRecord.SourceSHA != candidate.Record.SourceSHA ||
		candidate.CurrentRecord.ControlImageDigest != candidate.Record.ControlImageDigest ||
		candidate.CurrentRecord.ManifestDigest != candidate.Record.ManifestDigest ||
		candidate.CurrentRecord.HealthContractDigest != candidate.Record.HealthContractDigest ||
		(candidate.CurrentWorkerSlot != "a" && candidate.CurrentWorkerSlot != "b") || candidate.CurrentWorkerSlot == candidate.WorkerSlot {
		return errors.New("edge-control group candidate current authority binding is invalid")
	}
	return nil
}

func cloneGroupCandidateBundle(value GroupCandidateBundle) GroupCandidateBundle {
	if value.CurrentRecord != nil {
		current := *value.CurrentRecord
		value.CurrentRecord = &current
	}
	if value.CurrentBundle != nil {
		current := cloneEdgeRouteBundle(*value.CurrentBundle)
		value.CurrentBundle = &current
	}
	if value.ServingAuthority != nil {
		serving := *value.ServingAuthority
		value.ServingAuthority = &serving
	}
	value.Bundle = cloneEdgeRouteBundle(value.Bundle)
	return value
}
