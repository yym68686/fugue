package edgecontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

const (
	GroupAuthorityLedgerSchemaV1 = "edge-control-group-authority-ledger/v1"
	GroupAuthorityBatchSchemaV1  = "edge-control-group-authority-batch/v1"
	GroupPublishedBundleSchemaV1 = "edge-control-group-published-bundle/v1"

	GroupAuthorityStatusPublished = "published"
	GroupAuthorityStatusFailed    = "failed"

	GroupAuthorityFailureCandidateRead  = "candidate_read_failed"
	GroupAuthorityFailureCandidateCAS   = "candidate_cas_failed"
	GroupAuthorityFailureSigning        = "signing_failed"
	GroupAuthorityFailurePublicationCAS = "publication_cas_failed"

	GroupBundleSigningKeyringSchemaV1 = "edge-control-group-bundle-signing-keyring/v1"

	groupAuthorityIssuer       = "fugue-edge-control"
	maxGroupSigningKeyringSize = 128 << 10
	minGroupBundleValidity     = 5 * time.Minute
	maxGroupBundleValidity     = 24 * time.Hour
)

var (
	ErrGroupAuthorityCASConflict  = errors.New("edge-control group authority ledger CAS conflict")
	ErrGroupAuthorityCandidateCAS = errors.New("edge-control group authority candidate CAS conflict")
	groupAuthorityKeyIDPattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{2,63}$`)
	groupAuthorityDigestPattern   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// GroupAuthorityLedgerEntry is an append-only decision in one group's
// publication transaction. A failed entry advances only that group's audit
// sequence and cannot replace its last published bundle.
type GroupAuthorityLedgerEntry struct {
	Schema                        string    `json:"schema"`
	GroupID                       string    `json:"edge_group_id"`
	Sequence                      uint64    `json:"sequence"`
	Status                        string    `json:"status"`
	CandidateLedgerSequence       uint64    `json:"candidate_ledger_sequence,omitempty"`
	RouteIntentGeneration         string    `json:"route_intent_generation,omitempty"`
	InventoryGeneration           string    `json:"inventory_generation,omitempty"`
	BundleGeneration              string    `json:"bundle_generation,omitempty"`
	LastPublishedBundleGeneration string    `json:"last_published_bundle_generation,omitempty"`
	PublishedBundleDigest         string    `json:"published_bundle_digest,omitempty"`
	SigningKeyID                  string    `json:"signing_key_id,omitempty"`
	FailureCode                   string    `json:"failure_code,omitempty"`
	RecoveryEpoch                 uint64    `json:"recovery_epoch,omitempty"`
	RecoveryReason                string    `json:"recovery_reason,omitempty"`
	Authority                     string    `json:"authority"`
	PublicationEnabled            bool      `json:"publication_enabled"`
	RecordedAt                    time.Time `json:"recorded_at"`
}

// GroupPublishedBundle is the durable, independently readable LKG for one
// edge group. It never contains another group's route or signing state.
type GroupPublishedBundle struct {
	Schema                  string                `json:"schema"`
	GroupID                 string                `json:"edge_group_id"`
	PublicationSequence     uint64                `json:"publication_sequence"`
	CandidateLedgerSequence uint64                `json:"candidate_ledger_sequence"`
	Digest                  string                `json:"digest"`
	PublishedAt             time.Time             `json:"published_at"`
	RecoveryEpoch           uint64                `json:"recovery_epoch"`
	Bundle                  model.EdgeRouteBundle `json:"bundle"`
}

type GroupAuthorityState struct {
	LedgerHead      GroupAuthorityLedgerEntry `json:"ledger_head,omitempty"`
	LedgerExists    bool                      `json:"ledger_exists"`
	Published       GroupPublishedBundle      `json:"published,omitempty"`
	PublishedExists bool                      `json:"published_exists"`
}

// GroupAuthorityStore has no multi-group method. Every read and CAS names one
// group, which prevents a signer or storage failure in one group from rolling
// back or clearing another group's LKG. Implementations must permit concurrent
// transactions for different groups.
type GroupAuthorityStore interface {
	Head(context.Context, string) (GroupShadowLedgerEntry, bool, error)
	ReadGroupAuthority(context.Context, string) (GroupAuthorityState, error)
	AppendGroupAuthorityCAS(context.Context, string, uint64, uint64, GroupAuthorityLedgerEntry, *model.EdgeRouteBundle) (GroupAuthorityLedgerEntry, error)
}

type GroupBundleSigner interface {
	// SignGroupBundle may be invoked concurrently for different groups and
	// must never load another group's private signing material.
	SignGroupBundle(context.Context, string, model.EdgeRouteBundle) (model.EdgeRouteBundle, error)
}

// PublishedLKGRecoveryStore is an optional persistent-store capability for
// renewing the exact durable published bundle after expiry. It deliberately
// does not accept a RouteIntent or an arbitrary candidate bundle.
type PublishedLKGRecoveryStore interface {
	RecoverPublishedLKG(context.Context, string, uint64, uint64, string, model.EdgeRouteBundle, string, time.Time) (GroupAuthorityLedgerEntry, error)
}

type GroupAuthorityResult struct {
	GroupID                       string `json:"edge_group_id"`
	Status                        string `json:"status"`
	PublicationSequence           uint64 `json:"publication_sequence,omitempty"`
	CandidateLedgerSequence       uint64 `json:"candidate_ledger_sequence,omitempty"`
	BundleGeneration              string `json:"bundle_generation,omitempty"`
	LastPublishedBundleGeneration string `json:"last_published_bundle_generation,omitempty"`
	PublishedBundleDigest         string `json:"published_bundle_digest,omitempty"`
	FailureCode                   string `json:"failure_code,omitempty"`
}

type GroupAuthorityBatch struct {
	Schema                string                 `json:"schema"`
	RouteIntentGeneration string                 `json:"route_intent_generation"`
	Results               []GroupAuthorityResult `json:"results"`
	Published             int                    `json:"published"`
	Failed                int                    `json:"failed"`
}

// GroupAuthorityPublisher converts group-local compiler decisions into signed
// group-local publication transactions. There is deliberately no global
// signer call, global publication head, or cross-group rollback.
type GroupAuthorityPublisher struct {
	Store  GroupAuthorityStore
	Signer GroupBundleSigner
	Now    func() time.Time
}

func (publisher GroupAuthorityPublisher) Publish(ctx context.Context, compiled GroupShadowBatch) (GroupAuthorityBatch, error) {
	if publisher.Store == nil {
		return GroupAuthorityBatch{}, errors.New("edge-control group authority store is nil")
	}
	if publisher.Signer == nil {
		return GroupAuthorityBatch{}, errors.New("edge-control group bundle signer is nil")
	}
	if compiled.Schema != GroupShadowBatchSchemaV1 || strings.TrimSpace(compiled.RouteIntentGeneration) == "" {
		return GroupAuthorityBatch{}, errors.New("edge-control group authority compiler batch is invalid")
	}
	results := append([]GroupShadowResult(nil), compiled.Results...)
	if len(results) == 0 {
		return GroupAuthorityBatch{}, errors.New("edge-control group authority compiler batch is empty")
	}
	seenGroups := make(map[string]struct{}, len(results))
	succeeded, failed := 0, 0
	for _, result := range results {
		groupID := normalizeGroupID(result.GroupID)
		if groupID == "" || result.GroupID != groupID || !edgeGroupIDPattern.MatchString(groupID) {
			return GroupAuthorityBatch{}, errors.New("edge-control group authority result group is invalid")
		}
		if _, duplicate := seenGroups[groupID]; duplicate {
			return GroupAuthorityBatch{}, errors.New("edge-control group authority compiler batch contains a duplicate group")
		}
		seenGroups[groupID] = struct{}{}
		switch result.Status {
		case GroupShadowStatusCompiled:
			succeeded++
		case GroupShadowStatusFailed:
			failed++
		default:
			return GroupAuthorityBatch{}, errors.New("edge-control group authority compiler result status is invalid")
		}
	}
	if succeeded != compiled.Succeeded || failed != compiled.Failed {
		return GroupAuthorityBatch{}, errors.New("edge-control group authority compiler batch counts are invalid")
	}
	sort.Slice(results, func(i, j int) bool {
		return normalizeGroupID(results[i].GroupID) < normalizeGroupID(results[j].GroupID)
	})
	now := time.Now().UTC()
	if publisher.Now != nil {
		now = publisher.Now().UTC()
	}
	batch := GroupAuthorityBatch{Schema: GroupAuthorityBatchSchemaV1, RouteIntentGeneration: strings.TrimSpace(compiled.RouteIntentGeneration)}
	batch.Results = make([]GroupAuthorityResult, len(results))
	var wait sync.WaitGroup
	for index, compiledResult := range results {
		wait.Add(1)
		go func() {
			defer wait.Done()
			batch.Results[index] = publisher.publishGroup(ctx, compiled.RouteIntentGeneration, compiledResult, now)
		}()
	}
	wait.Wait()
	for _, result := range batch.Results {
		if result.Status == GroupAuthorityStatusPublished {
			batch.Published++
		} else {
			batch.Failed++
		}
	}
	return batch, nil
}

func (publisher GroupAuthorityPublisher) publishGroup(ctx context.Context, routeIntentGeneration string, compiled GroupShadowResult, now time.Time) GroupAuthorityResult {
	groupID := normalizeGroupID(compiled.GroupID)
	state, err := publisher.Store.ReadGroupAuthority(ctx, groupID)
	if err != nil {
		return GroupAuthorityResult{GroupID: groupID, Status: GroupAuthorityStatusFailed, FailureCode: GroupAuthorityFailurePublicationCAS}
	}
	expectedAuthoritySequence := uint64(0)
	lastPublished := ""
	if state.LedgerExists {
		expectedAuthoritySequence = state.LedgerHead.Sequence
	}
	if state.PublishedExists {
		lastPublished = strings.TrimSpace(state.Published.Bundle.Generation)
	}

	fail := func(code string, candidateSequence uint64) GroupAuthorityResult {
		if state.LedgerExists && state.LedgerHead.Status == GroupAuthorityStatusFailed && state.LedgerHead.FailureCode == code &&
			state.LedgerHead.CandidateLedgerSequence == candidateSequence && state.LedgerHead.RouteIntentGeneration == strings.TrimSpace(routeIntentGeneration) &&
			state.LedgerHead.LastPublishedBundleGeneration == lastPublished {
			return authorityResultFromEntry(state.LedgerHead)
		}
		entry := GroupAuthorityLedgerEntry{
			Schema: GroupAuthorityLedgerSchemaV1, GroupID: groupID, Status: GroupAuthorityStatusFailed,
			CandidateLedgerSequence: candidateSequence, RouteIntentGeneration: strings.TrimSpace(routeIntentGeneration),
			LastPublishedBundleGeneration: lastPublished, FailureCode: code, Authority: "edge-control",
			PublicationEnabled: true, RecordedAt: now,
		}
		appended, appendErr := publisher.Store.AppendGroupAuthorityCAS(ctx, groupID, expectedAuthoritySequence, candidateSequence, entry, nil)
		if appendErr != nil {
			return GroupAuthorityResult{GroupID: groupID, Status: GroupAuthorityStatusFailed, PublicationSequence: expectedAuthoritySequence,
				CandidateLedgerSequence: candidateSequence, LastPublishedBundleGeneration: lastPublished, FailureCode: GroupAuthorityFailurePublicationCAS}
		}
		return authorityResultFromEntry(appended)
	}

	if groupID == "" || compiled.Status != GroupShadowStatusCompiled || compiled.BundleGeneration == "" || compiled.LedgerSequence == 0 {
		failureCode := strings.TrimSpace(compiled.FailureCode)
		if failureCode == "" {
			failureCode = GroupAuthorityFailureCandidateRead
		}
		// A worker whose signed bundle expired is deliberately not serving
		// healthy. That must not deadlock a restarted Control process: refresh
		// only the exact durable LKG that this authority previously published.
		// This is not bootstrap eligibility (which remains first-publication
		// only), and it never compiles current route intent into a new bundle.
		if failureCode == GroupShadowFailureNoHealthyActive {
			if refreshed, ok := publisher.refreshExpiredPublishedLKG(ctx, groupID, state, now); ok {
				return refreshed
			}
		}
		candidateSequence := compiled.LedgerSequence
		if failureCode == GroupShadowFailureLedgerCAS {
			candidateSequence = 0
		}
		return fail(failureCode, candidateSequence)
	}
	candidate, exists, err := publisher.Store.Head(ctx, groupID)
	if err != nil || !exists {
		return fail(GroupAuthorityFailureCandidateRead, compiled.LedgerSequence)
	}
	if candidate.Sequence != compiled.LedgerSequence || candidate.Status != GroupShadowStatusCompiled || candidate.Bundle == nil || candidate.BundleGeneration != compiled.BundleGeneration {
		return fail(GroupAuthorityFailureCandidateCAS, compiled.LedgerSequence)
	}
	if state.LedgerExists && state.LedgerHead.Status == GroupAuthorityStatusPublished && state.PublishedExists &&
		state.Published.CandidateLedgerSequence == candidate.Sequence && state.Published.Bundle.Generation == candidate.BundleGeneration {
		lifetime := state.Published.Bundle.ValidUntil.Sub(state.Published.Bundle.GeneratedAt)
		if lifetime > 0 && state.Published.Bundle.ValidUntil.Sub(now) > lifetime/3 {
			return authorityResultFromEntry(state.LedgerHead)
		}
	}

	bundle := cloneEdgeRouteBundle(*candidate.Bundle)
	bundle.Issuer = groupAuthorityIssuer
	bundle.GeneratedAt = now
	bundle.ValidUntil = time.Time{}
	bundle.KeyID = ""
	bundle.Signature = ""
	bundle.Signatures = nil
	bundle.PreviousGeneration = ""
	if state.PublishedExists && state.Published.Bundle.Generation != bundle.Generation {
		bundle.PreviousGeneration = state.Published.Bundle.Generation
	}
	publicationRecoveryEpoch := uint64(0)
	if state.PublishedExists {
		publicationRecoveryEpoch = state.Published.RecoveryEpoch
	}
	bundle.Version = groupPublicationVersion(bundle.Generation, expectedAuthoritySequence+1, publicationRecoveryEpoch)
	signed, signErr := publisher.Signer.SignGroupBundle(ctx, groupID, bundle)
	if signErr != nil {
		return fail(GroupAuthorityFailureSigning, candidate.Sequence)
	}
	entry := GroupAuthorityLedgerEntry{
		Schema: GroupAuthorityLedgerSchemaV1, GroupID: groupID, Status: GroupAuthorityStatusPublished,
		CandidateLedgerSequence: candidate.Sequence, RouteIntentGeneration: candidate.RouteIntentGeneration,
		InventoryGeneration: candidate.InventoryGeneration, BundleGeneration: signed.Generation,
		LastPublishedBundleGeneration: signed.Generation, PublishedBundleDigest: signedGroupBundleDigest(signed),
		SigningKeyID: signed.KeyID, Authority: "edge-control", PublicationEnabled: true, RecordedAt: now,
	}
	appended, appendErr := publisher.Store.AppendGroupAuthorityCAS(ctx, groupID, expectedAuthoritySequence, candidate.Sequence, entry, &signed)
	if appendErr != nil {
		return GroupAuthorityResult{GroupID: groupID, Status: GroupAuthorityStatusFailed, PublicationSequence: expectedAuthoritySequence,
			CandidateLedgerSequence: candidate.Sequence, LastPublishedBundleGeneration: lastPublished, FailureCode: GroupAuthorityFailurePublicationCAS}
	}
	return authorityResultFromEntry(appended)
}

func (publisher GroupAuthorityPublisher) refreshExpiredPublishedLKG(ctx context.Context, groupID string, observed GroupAuthorityState, now time.Time) (GroupAuthorityResult, bool) {
	if !observed.LedgerExists || !observed.PublishedExists || observed.Published.Bundle.ValidUntil.After(now) {
		return GroupAuthorityResult{}, false
	}
	return publisher.refreshPublishedLKG(ctx, groupID, observed, now, "automatic persisted group LKG refresh after expiry")
}

// RefreshPublishedLKG renews only the exact durable current bundle when half
// its validity has elapsed. It never reads current RouteIntent and therefore
// cannot activate a newly compiled candidate.
func (publisher GroupAuthorityPublisher) RefreshPublishedLKG(ctx context.Context, groupID string, now time.Time) (GroupAuthorityResult, error) {
	if publisher.Store == nil || publisher.Signer == nil {
		return GroupAuthorityResult{}, errors.New("edge-control current LKG maintainer is invalid")
	}
	observed, err := publisher.Store.ReadGroupAuthority(ctx, groupID)
	if err != nil || !observed.LedgerExists || !observed.PublishedExists || validateGroupPublishedBundle(groupID, observed.Published) != nil {
		return GroupAuthorityResult{}, errors.New("edge-control current LKG is unavailable")
	}
	lifetime := observed.Published.Bundle.ValidUntil.Sub(observed.Published.Bundle.GeneratedAt)
	if lifetime <= 0 {
		return GroupAuthorityResult{}, errors.New("edge-control current LKG validity is invalid")
	}
	if observed.Published.Bundle.ValidUntil.Sub(now.UTC()) > lifetime/2 {
		return authorityResultFromEntry(observed.LedgerHead), nil
	}
	result, ok := publisher.refreshPublishedLKG(ctx, groupID, observed, now.UTC(), "automatic exact LKG validity refresh during candidate publication")
	if !ok {
		return GroupAuthorityResult{}, errors.New("edge-control current LKG refresh failed")
	}
	return result, nil
}

func (publisher GroupAuthorityPublisher) refreshPublishedLKG(ctx context.Context, groupID string, observed GroupAuthorityState, now time.Time, reason string) (GroupAuthorityResult, bool) {
	if recovery, ok := publisher.Store.(PublishedLKGRecoveryStore); ok {
		failed := func(code string) (GroupAuthorityResult, bool) {
			return GroupAuthorityResult{
				GroupID: groupID, Status: GroupAuthorityStatusFailed, PublicationSequence: observed.LedgerHead.Sequence,
				CandidateLedgerSequence:       observed.Published.CandidateLedgerSequence,
				LastPublishedBundleGeneration: observed.Published.Bundle.Generation, FailureCode: code,
			}, true
		}
		if !observed.LedgerExists || !observed.PublishedExists || validateGroupPublishedBundle(groupID, observed.Published) != nil {
			return failed(GroupAuthorityFailureCandidateRead)
		}
		bundle := cloneEdgeRouteBundle(observed.Published.Bundle)
		bundle.Issuer = groupAuthorityIssuer
		bundle.GeneratedAt = now
		bundle.ValidUntil = time.Time{}
		bundle.KeyID = ""
		bundle.Signature = ""
		bundle.Signatures = nil
		bundle.PreviousGeneration = ""
		bundle.Version = groupPublicationVersion(bundle.Generation, observed.LedgerHead.Sequence+1, observed.Published.RecoveryEpoch+1)
		signed, err := publisher.Signer.SignGroupBundle(ctx, groupID, bundle)
		if err != nil {
			return failed(GroupAuthorityFailureSigning)
		}
		appended, err := recovery.RecoverPublishedLKG(ctx, groupID, observed.Published.PublicationSequence,
			observed.Published.RecoveryEpoch, observed.Published.Bundle.Generation, signed, reason, now)
		if err != nil {
			if errors.Is(err, ErrGroupAuthorityCandidateCAS) {
				return failed(GroupAuthorityFailureCandidateCAS)
			}
			return failed(GroupAuthorityFailurePublicationCAS)
		}
		return authorityResultFromEntry(appended), true
	}
	store, ok := publisher.Store.(GroupRecoveryStore)
	if !ok {
		return GroupAuthorityResult{}, false
	}
	authority, candidate, recoveryEpoch, err := store.ReadGroupRecoveryTarget(ctx, groupID, observed.Published.Bundle.Generation)
	if err != nil || !authority.LedgerExists || !authority.PublishedExists ||
		authority.LedgerHead.Sequence != observed.LedgerHead.Sequence || authority.Published.Digest != observed.Published.Digest ||
		authority.Published.Bundle.Generation != observed.Published.Bundle.Generation || candidate.Bundle == nil {
		return GroupAuthorityResult{}, false
	}
	bundle := cloneEdgeRouteBundle(*candidate.Bundle)
	bundle.Issuer = groupAuthorityIssuer
	bundle.GeneratedAt = now
	bundle.ValidUntil = time.Time{}
	bundle.KeyID = ""
	bundle.Signature = ""
	bundle.Signatures = nil
	bundle.PreviousGeneration = ""
	bundle.Version = groupPublicationVersion(bundle.Generation, authority.LedgerHead.Sequence+1, recoveryEpoch+1)
	signed, err := publisher.Signer.SignGroupBundle(ctx, groupID, bundle)
	if err != nil {
		return GroupAuthorityResult{}, false
	}
	entry := GroupAuthorityLedgerEntry{
		Schema: GroupAuthorityLedgerSchemaV1, GroupID: groupID, Status: GroupAuthorityStatusPublished,
		CandidateLedgerSequence: candidate.Sequence, RouteIntentGeneration: candidate.RouteIntentGeneration,
		InventoryGeneration: candidate.InventoryGeneration, BundleGeneration: signed.Generation,
		LastPublishedBundleGeneration: signed.Generation, PublishedBundleDigest: signedGroupBundleDigest(signed),
		SigningKeyID: signed.KeyID, RecoveryEpoch: recoveryEpoch + 1,
		RecoveryReason: reason,
		Authority:      "edge-control", PublicationEnabled: true, RecordedAt: now,
	}
	appended, err := store.RecoverGroupAuthorityCAS(ctx, groupID, authority.Published.PublicationSequence, recoveryEpoch, entry, signed)
	if err != nil {
		return GroupAuthorityResult{}, false
	}
	return authorityResultFromEntry(appended), true
}

func authorityResultFromEntry(entry GroupAuthorityLedgerEntry) GroupAuthorityResult {
	return GroupAuthorityResult{
		GroupID: entry.GroupID, Status: entry.Status, PublicationSequence: entry.Sequence,
		CandidateLedgerSequence: entry.CandidateLedgerSequence, BundleGeneration: entry.BundleGeneration,
		LastPublishedBundleGeneration: entry.LastPublishedBundleGeneration,
		PublishedBundleDigest:         entry.PublishedBundleDigest, FailureCode: entry.FailureCode,
	}
}

type projectedGroupBundleSigner struct {
	keyringDir string
	validFor   time.Duration
}

func NewProjectedGroupBundleSigner(keyringDir string, validFor time.Duration) (GroupBundleSigner, error) {
	keyringDir = strings.TrimSpace(keyringDir)
	if keyringDir == "" || !filepath.IsAbs(keyringDir) || filepath.Clean(keyringDir) != keyringDir {
		return nil, errors.New("edge-control group signing keyring directory must be an absolute normalized path")
	}
	if validFor < minGroupBundleValidity || validFor > maxGroupBundleValidity {
		return nil, errors.New("edge-control group bundle validity must be between 5m and 24h")
	}
	return &projectedGroupBundleSigner{keyringDir: keyringDir, validFor: validFor}, nil
}

func (signer *projectedGroupBundleSigner) SignGroupBundle(ctx context.Context, groupID string, bundle model.EdgeRouteBundle) (model.EdgeRouteBundle, error) {
	if signer == nil || ctx == nil {
		return model.EdgeRouteBundle{}, errors.New("edge-control group signer is nil")
	}
	if err := ctx.Err(); err != nil {
		return model.EdgeRouteBundle{}, err
	}
	groupID = normalizeGroupID(groupID)
	if groupID == "" || normalizeGroupID(bundle.EdgeGroupID) != groupID || strings.TrimSpace(bundle.Generation) == "" || bundle.GeneratedAt.IsZero() {
		return model.EdgeRouteBundle{}, errors.New("edge-control group signing input is invalid")
	}
	group, err := loadGroupBundleSigningKeyring(filepath.Join(signer.keyringDir, groupID+".json"), groupID)
	if err != nil {
		return model.EdgeRouteBundle{}, err
	}
	primary, err := base64.RawURLEncoding.DecodeString(group.PrimaryKey)
	if err != nil {
		return model.EdgeRouteBundle{}, errors.New("edge-control group signing key is invalid")
	}
	previous := []byte(nil)
	if group.PreviousKey != "" {
		previous, err = base64.RawURLEncoding.DecodeString(group.PreviousKey)
		if err != nil {
			zeroBytes(primary)
			return model.EdgeRouteBundle{}, errors.New("edge-control group previous signing key is invalid")
		}
	}
	signed := bundleauth.SignEdgeRouteBundleWithKeyring(bundle, bundleauth.NewKeyring(
		string(primary), group.PrimaryKeyID, string(previous), group.PreviousKeyID, group.RevokedKeyIDs,
	), signer.validFor)
	verifyErr := bundleauth.VerifyEdgeRouteBundleWithKeyring(signed, bundleauth.NewKeyring(
		string(primary), group.PrimaryKeyID, string(previous), group.PreviousKeyID, group.RevokedKeyIDs,
	), bundle.GeneratedAt)
	zeroBytes(primary)
	zeroBytes(previous)
	if verifyErr != nil || signed.KeyID != group.PrimaryKeyID || strings.TrimSpace(signed.Signature) == "" {
		return model.EdgeRouteBundle{}, errors.New("edge-control group bundle signature self-check failed")
	}
	return signed, nil
}

type groupBundleSigningKeyringFile struct {
	Schema     string                `json:"schema"`
	Generation uint64                `json:"generation"`
	Group      groupBundleSigningKey `json:"group"`
}

type groupBundleSigningKey struct {
	GroupID       string   `json:"edge_group_id"`
	PrimaryKeyID  string   `json:"primary_key_id"`
	PrimaryKey    string   `json:"primary_key"`
	PreviousKeyID string   `json:"previous_key_id,omitempty"`
	PreviousKey   string   `json:"previous_key,omitempty"`
	RevokedKeyIDs []string `json:"revoked_key_ids,omitempty"`
}

func loadGroupBundleSigningKeyring(path, expectedGroupID string) (groupBundleSigningKey, error) {
	raw, err := readPrivateProjectedFile(path, maxGroupSigningKeyringSize)
	if err != nil {
		return groupBundleSigningKey{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var file groupBundleSigningKeyringFile
	if err := decoder.Decode(&file); err != nil {
		return groupBundleSigningKey{}, errors.New("edge-control group signing keyring is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || file.Schema != GroupBundleSigningKeyringSchemaV1 || file.Generation == 0 {
		return groupBundleSigningKey{}, errors.New("edge-control group signing keyring is invalid")
	}
	group := file.Group
	group.GroupID = normalizeGroupID(group.GroupID)
	if group.GroupID == "" || group.GroupID != normalizeGroupID(expectedGroupID) || !groupAuthorityKeyIDPattern.MatchString(group.PrimaryKeyID) ||
		(group.PreviousKeyID == "") != (group.PreviousKey == "") {
		return groupBundleSigningKey{}, errors.New("edge-control group signing keyring identity is invalid")
	}
	keyIDs := make(map[string]struct{}, 2)
	keyDigests := make(map[string]struct{}, 2)
	for _, material := range []struct{ id, encoded string }{{group.PrimaryKeyID, group.PrimaryKey}, {group.PreviousKeyID, group.PreviousKey}} {
		if material.id == "" {
			continue
		}
		if !groupAuthorityKeyIDPattern.MatchString(material.id) {
			return groupBundleSigningKey{}, errors.New("edge-control group signing key id is invalid")
		}
		decoded, decodeErr := base64.RawURLEncoding.DecodeString(material.encoded)
		if decodeErr != nil || len(decoded) < 32 || len(decoded) > 64 {
			zeroBytes(decoded)
			return groupBundleSigningKey{}, errors.New("edge-control group signing key material is invalid")
		}
		digest := sha256.Sum256(decoded)
		zeroBytes(decoded)
		digestString := hex.EncodeToString(digest[:])
		if _, duplicate := keyIDs[material.id]; duplicate {
			return groupBundleSigningKey{}, errors.New("edge-control group signing key id is duplicated")
		}
		if _, duplicate := keyDigests[digestString]; duplicate {
			return groupBundleSigningKey{}, errors.New("edge-control group signing key material is duplicated")
		}
		keyIDs[material.id] = struct{}{}
		keyDigests[digestString] = struct{}{}
	}
	revoked := make(map[string]struct{}, len(group.RevokedKeyIDs))
	for _, id := range group.RevokedKeyIDs {
		id = strings.TrimSpace(id)
		if !groupAuthorityKeyIDPattern.MatchString(id) || id == group.PrimaryKeyID {
			return groupBundleSigningKey{}, errors.New("edge-control revoked signing key id is invalid")
		}
		if _, duplicate := revoked[id]; duplicate {
			return groupBundleSigningKey{}, errors.New("edge-control revoked signing key id is duplicated")
		}
		revoked[id] = struct{}{}
	}
	return group, nil
}

func signedGroupBundleDigest(bundle model.EdgeRouteBundle) string {
	return digestJSON(bundle)
}

func groupAuthorityCandidateDigest(bundle model.EdgeRouteBundle) string {
	bundle.Version = bundle.Generation
	bundle.GeneratedAt = time.Time{}
	bundle.ValidUntil = time.Time{}
	bundle.Issuer = ""
	bundle.KeyID = ""
	bundle.Signature = ""
	bundle.Signatures = nil
	bundle.PreviousGeneration = ""
	return digestJSON(bundle)
}

func zeroBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func validateGroupPublishedBundle(groupID string, published GroupPublishedBundle) error {
	groupID = normalizeGroupID(groupID)
	bundle := published.Bundle
	if published.Schema != GroupPublishedBundleSchemaV1 || normalizeGroupID(published.GroupID) != groupID || published.PublicationSequence == 0 ||
		published.CandidateLedgerSequence == 0 || published.PublishedAt.IsZero() || published.Digest != signedGroupBundleDigest(bundle) ||
		normalizeGroupID(bundle.EdgeGroupID) != groupID || bundle.SchemaVersion != model.BundleSchemaVersionV1 || bundle.Issuer != groupAuthorityIssuer ||
		strings.TrimSpace(bundle.Generation) == "" || bundle.Version != groupPublicationVersion(bundle.Generation, published.PublicationSequence, published.RecoveryEpoch) ||
		strings.TrimSpace(bundle.KeyID) == "" || strings.TrimSpace(bundle.Signature) == "" ||
		bundle.GeneratedAt.IsZero() || !bundle.ValidUntil.After(bundle.GeneratedAt) {
		return errors.New("edge-control group published bundle is invalid")
	}
	return nil
}

func prepareGroupAuthorityAppend(groupID string, expectedSequence uint64, entries []GroupAuthorityLedgerEntry, current *GroupPublishedBundle,
	candidate *GroupShadowLedgerEntry, entry GroupAuthorityLedgerEntry, signed *model.EdgeRouteBundle) (GroupAuthorityLedgerEntry, *GroupPublishedBundle, error) {
	groupID = normalizeGroupID(groupID)
	if groupID == "" || entry.Schema != GroupAuthorityLedgerSchemaV1 || normalizeGroupID(entry.GroupID) != groupID || entry.Sequence != 0 ||
		entry.Authority != "edge-control" || !entry.PublicationEnabled || entry.RecordedAt.IsZero() ||
		(entry.Status != GroupAuthorityStatusPublished && entry.Status != GroupAuthorityStatusFailed) {
		return GroupAuthorityLedgerEntry{}, nil, errors.New("invalid edge-control group authority entry")
	}
	if strings.TrimSpace(entry.RouteIntentGeneration) == "" || entry.RouteIntentGeneration != strings.TrimSpace(entry.RouteIntentGeneration) {
		return GroupAuthorityLedgerEntry{}, nil, errors.New("edge-control group authority RouteIntent generation is invalid")
	}
	if uint64(len(entries)) != expectedSequence {
		return GroupAuthorityLedgerEntry{}, nil, ErrGroupAuthorityCASConflict
	}
	lastRecoveryEpoch := uint64(0)
	for _, previous := range entries {
		if previous.RecoveryEpoch > lastRecoveryEpoch {
			lastRecoveryEpoch = previous.RecoveryEpoch
		}
	}
	if entry.RecoveryEpoch == 0 {
		if entry.RecoveryReason != "" {
			return GroupAuthorityLedgerEntry{}, nil, errors.New("edge-control group authority recovery reason has no epoch")
		}
	} else if entry.Status != GroupAuthorityStatusPublished || entry.RecoveryEpoch != lastRecoveryEpoch+1 || strings.TrimSpace(entry.RecoveryReason) == "" || len(entry.RecoveryReason) > 256 {
		return GroupAuthorityLedgerEntry{}, nil, errors.New("edge-control group authority recovery epoch is not monotonic")
	}
	publicationRecoveryEpoch := lastRecoveryEpoch
	if entry.RecoveryEpoch > publicationRecoveryEpoch {
		publicationRecoveryEpoch = entry.RecoveryEpoch
	}
	lastPublished := ""
	if current != nil {
		if err := validateGroupPublishedBundle(groupID, *current); err != nil {
			return GroupAuthorityLedgerEntry{}, nil, err
		}
		lastPublished = current.Bundle.Generation
	}
	if entry.Status == GroupAuthorityStatusFailed {
		if signed != nil || strings.TrimSpace(entry.FailureCode) == "" || entry.BundleGeneration != "" || entry.PublishedBundleDigest != "" || entry.SigningKeyID != "" ||
			entry.LastPublishedBundleGeneration != lastPublished {
			return GroupAuthorityLedgerEntry{}, nil, errors.New("failed edge-control group authority entry changed published LKG")
		}
	} else {
		if signed == nil || candidate == nil || candidate.Sequence == 0 || candidate.Sequence != entry.CandidateLedgerSequence ||
			candidate.Status != GroupShadowStatusCompiled || candidate.Bundle == nil || candidate.BundleGeneration != signed.Generation ||
			candidate.RouteIntentGeneration != entry.RouteIntentGeneration || candidate.InventoryGeneration != entry.InventoryGeneration ||
			groupAuthorityCandidateDigest(*candidate.Bundle) != groupAuthorityCandidateDigest(*signed) ||
			entry.BundleGeneration != signed.Generation || entry.LastPublishedBundleGeneration != signed.Generation ||
			entry.PublishedBundleDigest != signedGroupBundleDigest(*signed) || !groupAuthorityDigestPattern.MatchString(entry.PublishedBundleDigest) ||
			entry.SigningKeyID != signed.KeyID || !groupAuthorityKeyIDPattern.MatchString(entry.SigningKeyID) || entry.FailureCode != "" {
			return GroupAuthorityLedgerEntry{}, nil, errors.New("published edge-control group authority entry is not bound to its candidate")
		}
		if signed.Version != groupPublicationVersion(signed.Generation, expectedSequence+1, publicationRecoveryEpoch) {
			return GroupAuthorityLedgerEntry{}, nil, errors.New("edge-control group publication version is not bound to its CAS")
		}
		if signed.PreviousGeneration != "" && signed.PreviousGeneration != lastPublished {
			return GroupAuthorityLedgerEntry{}, nil, errors.New("edge-control group publication previous generation is not monotonic")
		}
		if lastPublished != "" && lastPublished != signed.Generation && signed.PreviousGeneration != lastPublished {
			return GroupAuthorityLedgerEntry{}, nil, errors.New("edge-control group publication skipped its previous LKG")
		}
	}
	entry.GroupID = groupID
	entry.Sequence = expectedSequence + 1
	if entry.Status == GroupAuthorityStatusFailed {
		return entry, current, nil
	}
	published := &GroupPublishedBundle{
		Schema: GroupPublishedBundleSchemaV1, GroupID: groupID, PublicationSequence: entry.Sequence,
		CandidateLedgerSequence: entry.CandidateLedgerSequence, Digest: entry.PublishedBundleDigest,
		PublishedAt: entry.RecordedAt, RecoveryEpoch: publicationRecoveryEpoch, Bundle: cloneEdgeRouteBundle(*signed),
	}
	if err := validateGroupPublishedBundle(groupID, *published); err != nil {
		return GroupAuthorityLedgerEntry{}, nil, err
	}
	return entry, published, nil
}

func groupPublicationVersion(generation string, sequence, recoveryEpoch uint64) string {
	return strings.TrimSpace(generation) + ".p" + strconv.FormatUint(sequence, 10) + ".r" + strconv.FormatUint(recoveryEpoch, 10)
}

func cloneGroupPublishedBundle(value GroupPublishedBundle) GroupPublishedBundle {
	value.Bundle = cloneEdgeRouteBundle(value.Bundle)
	return value
}
