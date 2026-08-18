package edgecontrol

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"fugue/internal/edgeauthority"
)

const (
	GroupCandidateStagePathV1          = "/v1/authority/group-worker-candidates"
	GroupCandidateStageRequestSchemaV1 = "edge-control-group-worker-candidate-request/v1"
	GroupCandidateStageReceiptSchemaV1 = "edge-control-group-worker-candidate-receipt/v1"
	maxGroupCandidateStageBodyBytes    = 64 << 10
)

var groupServingAuthorityTokenPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{1,255}$`)

// GroupServingAuthorityWitness binds a candidate-stage request to the exact
// CurrentAuthority object and Front/Worker activation that are serving users.
// It is optional for backward compatibility; when present, Edge Control may
// select only the exact immutable historical publication named here.
type GroupServingAuthorityWitness struct {
	CurrentRecordDigest string `json:"current_record_digest"`
	AuthorityEpoch      int64  `json:"authority_epoch"`
	CurrentAuthorityUID string `json:"current_authority_uid"`
	CurrentAuthorityRV  string `json:"current_authority_resource_version"`
	FrontGeneration     uint64 `json:"front_generation"`
	BundleVersion       string `json:"bundle_version"`
	WorkerSlot          string `json:"worker_slot"`
	WorkerSourceSHA     string `json:"worker_source_sha"`
	WorkerImageDigest   string `json:"worker_image_digest"`
}

func (witness GroupServingAuthorityWitness) Validate() error {
	if !groupAuthorityDigestPattern.MatchString(witness.CurrentRecordDigest) || witness.AuthorityEpoch < 1 ||
		!groupServingAuthorityTokenPattern.MatchString(witness.CurrentAuthorityUID) ||
		!groupServingAuthorityTokenPattern.MatchString(witness.CurrentAuthorityRV) || witness.FrontGeneration == 0 ||
		(witness.WorkerSlot != "a" && witness.WorkerSlot != "b") || !groupCandidateSourcePattern.MatchString(witness.WorkerSourceSHA) ||
		!groupAuthorityDigestPattern.MatchString(witness.WorkerImageDigest) {
		return errors.New("edge-control serving authority witness is invalid")
	}
	if _, publicationSequence, _, ok := parseGroupPublicationVersion(witness.BundleVersion); !ok || publicationSequence == 0 {
		return errors.New("edge-control serving authority publication is invalid")
	}
	return nil
}

// GroupCandidateStageRequest can only attach an immutable Worker release to
// the inactive slot and the exact current signed LKG. It carries no route,
// manifest, publication, or ordinary-traffic mutation.
type GroupCandidateStageRequest struct {
	Schema                        string                        `json:"schema"`
	KeyID                         string                        `json:"key_id"`
	GroupID                       string                        `json:"edge_group_id"`
	ExpectedAuthoritySequence     uint64                        `json:"expected_authority_sequence"`
	ExpectedPublicationSequence   uint64                        `json:"expected_publication_sequence"`
	ExpectedRecoveryEpoch         uint64                        `json:"expected_recovery_epoch"`
	ExpectedPublishedBundleDigest string                        `json:"expected_published_bundle_digest"`
	ExpectedCandidateEpoch        uint64                        `json:"expected_candidate_epoch"`
	ExpectedCurrentWorkerSlot     string                        `json:"expected_current_worker_slot"`
	TargetWorkerSlot              string                        `json:"target_worker_slot"`
	ServingAuthority              *GroupServingAuthorityWitness `json:"serving_authority,omitempty"`
	AllowDegradedPrevious         bool                          `json:"allow_degraded_previous,omitempty"`
	StandbyOnly                   bool                          `json:"standby_only,omitempty"`
	WorkerSourceSHA               string                        `json:"worker_source_sha"`
	WorkerImageDigest             string                        `json:"worker_image_digest"`
	ReleaseRecordDigest           string                        `json:"release_record_digest"`
	IssuedAtUnix                  int64                         `json:"issued_at_unix"`
	ExpiresAtUnix                 int64                         `json:"expires_at_unix"`
	Nonce                         string                        `json:"nonce"`
	Reason                        string                        `json:"reason"`
	Signature                     string                        `json:"signature"`
}

type GroupCandidateStageReceipt struct {
	Schema                       string `json:"schema"`
	GroupID                      string `json:"edge_group_id"`
	CandidateEpoch               uint64 `json:"candidate_epoch"`
	CandidateRecordDigest        string `json:"candidate_record_digest"`
	ReleaseRecordDigest          string `json:"release_record_digest"`
	WorkerSourceSHA              string `json:"worker_source_sha"`
	WorkerImageDigest            string `json:"worker_image_digest"`
	WorkerSlot                   string `json:"worker_slot"`
	CurrentWorkerSlot            string `json:"current_worker_slot"`
	CurrentPublishedBundleDigest string `json:"current_published_bundle_digest"`
	CurrentPublicationSequence   uint64 `json:"current_publication_sequence"`
	CurrentRecoveryEpoch         uint64 `json:"current_recovery_epoch"`
	AllowDegradedPrevious        bool   `json:"allow_degraded_previous,omitempty"`
	StandbyOnly                  bool   `json:"standby_only,omitempty"`
	OrdinaryTrafficMutation      bool   `json:"ordinary_traffic_mutation"`
}

type GroupCandidateStageHandlerConfig struct {
	Publisher  GroupCandidatePublisher
	GroupIDs   []string
	KeyringDir string
	Now        func() time.Time
}

type groupCandidateStageHandler struct {
	publisher  GroupCandidatePublisher
	groups     map[string]struct{}
	keyringDir string
	now        func() time.Time
}

func NewGroupCandidateStageHandler(config GroupCandidateStageHandlerConfig) (http.Handler, error) {
	if config.Publisher.Store == nil || config.Publisher.Signer == nil || config.Publisher.Identity.Validate() != nil {
		return nil, errors.New("edge-control worker candidate staging dependency is invalid")
	}
	groups, err := normalizeGroupIDs(config.GroupIDs)
	if err != nil {
		return nil, err
	}
	keyringDir := strings.TrimSpace(config.KeyringDir)
	if keyringDir == "" || !filepath.IsAbs(keyringDir) || filepath.Clean(keyringDir) != keyringDir {
		return nil, errors.New("edge-control worker candidate staging keyring directory must be absolute")
	}
	allowed := make(map[string]struct{}, len(groups))
	for _, groupID := range groups {
		allowed[groupID] = struct{}{}
	}
	now := func() time.Time { return time.Now().UTC() }
	if config.Now != nil {
		now = func() time.Time { return config.Now().UTC() }
	}
	return &groupCandidateStageHandler{publisher: config.Publisher, groups: allowed, keyringDir: keyringDir, now: now}, nil
}

func (handler *groupCandidateStageHandler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != GroupCandidateStagePathV1 || request.URL.RawQuery != "" {
		http.NotFound(w, request)
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeGroupBundleError(w, http.StatusUnsupportedMediaType, "content_type_rejected")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, maxGroupCandidateStageBodyBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxGroupCandidateStageBodyBytes {
		writeGroupBundleError(w, http.StatusBadRequest, "request_rejected")
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value GroupCandidateStageRequest
	if decoder.Decode(&value) != nil || !decodeEOF(decoder) || validateGroupCandidateStageRequest(value) != nil {
		writeGroupBundleError(w, http.StatusBadRequest, "request_rejected")
		return
	}
	if _, allowed := handler.groups[value.GroupID]; !allowed {
		writeGroupBundleError(w, http.StatusForbidden, "group_rejected")
		return
	}
	now := handler.now()
	if authenticateGroupCandidateStage(filepath.Join(handler.keyringDir, value.GroupID+".json"), value, now) != nil {
		writeGroupBundleError(w, http.StatusUnauthorized, "credential_rejected")
		return
	}
	candidate, err := handler.publisher.stageWorkerCurrentLKG(request.Context(), value, now)
	if err != nil {
		if errors.Is(err, ErrGroupAuthorityCandidateCAS) {
			log.Printf("edge-control worker candidate stage conflict group=%s detail=%v", value.GroupID, err)
			writeGroupBundleError(w, http.StatusConflict, "sequence_conflict")
			return
		}
		log.Printf("edge-control worker candidate stage unavailable group=%s detail=%v", value.GroupID, err)
		writeGroupBundleError(w, http.StatusServiceUnavailable, "candidate_unavailable")
		return
	}
	writeJSON(w, http.StatusOK, groupCandidateStageReceipt(candidate, value))
}

func (publisher GroupCandidatePublisher) stageWorkerCurrentLKG(ctx context.Context, request GroupCandidateStageRequest, now time.Time) (GroupCandidateBundle, error) {
	servingBundleVersion := ""
	if request.ServingAuthority != nil {
		servingBundleVersion = request.ServingAuthority.BundleVersion
	}
	snapshot, err := publisher.Store.ReadGroupCandidateStage(ctx, request.GroupID, servingBundleVersion)
	authority := snapshot.Authority
	if err != nil {
		return GroupCandidateBundle{}, fmt.Errorf("read candidate stage snapshot: %w", err)
	}
	if !authority.LedgerExists || !authority.PublishedExists || validateGroupPublishedBundle(request.GroupID, authority.Published) != nil {
		return GroupCandidateBundle{}, groupCandidateCASConflict("published_authority_unavailable")
	}
	if !authorityHeadPreservesPublishedAuthority(authority, request.ExpectedAuthoritySequence) ||
		!stagePublicationMatchesAuthority(authority.Published, request, request.ServingAuthority) {
		return GroupCandidateBundle{}, groupCandidateCASConflict(fmt.Sprintf("published_authority_mismatch expected_ledger=%d actual_ledger=%d expected_publication=%d actual_publication=%d expected_recovery=%d actual_recovery=%d expected_digest=%s actual_digest=%s",
			request.ExpectedAuthoritySequence, authority.LedgerHead.Sequence, request.ExpectedPublicationSequence, authority.Published.PublicationSequence,
			request.ExpectedRecoveryEpoch, authority.Published.RecoveryEpoch, request.ExpectedPublishedBundleDigest, authority.Published.Digest))
	}
	if request.ServingAuthority == nil {
		bootstrapEligible, _ := groupPublishedBootstrapEligibility(authority.Published, now)
		if !bootstrapEligible {
			return GroupCandidateBundle{}, groupCandidateCASConflict("bootstrap_publication_ineligible")
		}
	}
	currentCandidate, exists := snapshot.Candidate, snapshot.CandidateExists
	currentEpoch := uint64(0)
	if exists {
		currentEpoch = currentCandidate.Epoch
		if currentCandidate.Epoch == request.ExpectedCandidateEpoch && stagedCandidateMatchesRequest(currentCandidate, request, authority) {
			return currentCandidate, nil
		}
	}
	if currentEpoch != request.ExpectedCandidateEpoch {
		return GroupCandidateBundle{}, groupCandidateCASConflict(fmt.Sprintf("candidate_epoch_mismatch expected=%d actual=%d", request.ExpectedCandidateEpoch, currentEpoch))
	}
	currentHead := snapshot.PublishedCandidate
	if currentHead.Sequence != authority.Published.CandidateLedgerSequence || currentHead.Status != GroupShadowStatusCompiled ||
		currentHead.Bundle == nil || currentHead.BundleArchived || currentHead.BundleGeneration != authority.Published.Bundle.Generation ||
		!groupAuthorityDigestPattern.MatchString(currentHead.InventoryDigest) {
		return GroupCandidateBundle{}, groupCandidateCASConflict(fmt.Sprintf("published_candidate_head_invalid expected_sequence=%d actual_sequence=%d status=%s bundle_present=%t archived=%t expected_generation=%s actual_generation=%s inventory_digest=%s",
			authority.Published.CandidateLedgerSequence, currentHead.Sequence, currentHead.Status, currentHead.Bundle != nil, currentHead.BundleArchived,
			authority.Published.Bundle.Generation, currentHead.BundleGeneration, currentHead.InventoryDigest))
	}
	head := currentHead
	if request.ServingAuthority == nil {
		if head.ActiveSlot != request.ExpectedCurrentWorkerSlot {
			return GroupCandidateBundle{}, groupCandidateCASConflict(fmt.Sprintf("bootstrap_active_slot_mismatch expected=%s actual=%s", request.ExpectedCurrentWorkerSlot, head.ActiveSlot))
		}
	} else {
		servingVersion := groupPublicationVersion(snapshot.ServingAuthority.BundleGeneration, snapshot.ServingAuthority.Sequence, snapshot.ServingAuthority.RecoveryEpoch)
		fallback := !snapshot.ServingExists &&
			(servingAuthorityCanUsePrunedCurrentGeneration(request.ServingAuthority.BundleVersion, authority.Published.Bundle.Generation,
				authority.Published.PublicationSequence, authority.Published.RecoveryEpoch) ||
				servingAuthorityCanUseCurrentPublishedFallback(request.ServingAuthority.BundleVersion, authority.Published.Bundle.Generation,
					authority.Published.PublicationSequence, authority.Published.RecoveryEpoch,
					request.AllowDegradedPrevious && !request.StandbyOnly))
		if fallback {
			head = currentHead
		} else if !snapshot.ServingExists || snapshot.ServingCandidate.Bundle == nil || snapshot.ServingCandidate.BundleArchived ||
			snapshot.ServingCandidate.Status != GroupShadowStatusCompiled || snapshot.ServingCandidate.ActiveSlot != request.ExpectedCurrentWorkerSlot ||
			request.ServingAuthority.WorkerSlot != request.ExpectedCurrentWorkerSlot || servingVersion != request.ServingAuthority.BundleVersion {
			_, witnessPublication, witnessRecovery, witnessParsed := parseGroupPublicationVersion(request.ServingAuthority.BundleVersion)
			return GroupCandidateBundle{}, groupCandidateCASConflict(fmt.Sprintf("serving_authority_history_mismatch exists=%t bundle_present=%t archived=%t status=%s expected_slot=%s candidate_slot=%s witness_slot=%s expected_version=%s actual_version=%s witness_parsed=%t witness_publication=%d current_publication=%d witness_recovery=%d current_recovery=%d",
				snapshot.ServingExists, snapshot.ServingCandidate.Bundle != nil, snapshot.ServingCandidate.BundleArchived, snapshot.ServingCandidate.Status,
				request.ExpectedCurrentWorkerSlot, snapshot.ServingCandidate.ActiveSlot, request.ServingAuthority.WorkerSlot, request.ServingAuthority.BundleVersion,
				servingVersion, witnessParsed, witnessPublication, authority.Published.PublicationSequence, witnessRecovery, authority.Published.RecoveryEpoch))
		} else {
			head = snapshot.ServingCandidate
		}
	}
	sequence := head.Sequence
	epoch := authority.Published.PublicationSequence + 1
	if currentEpoch >= epoch {
		epoch = currentEpoch + 1
	}
	bundle := cloneEdgeRouteBundle(*head.Bundle)
	bundle.Issuer = groupAuthorityIssuer
	bundle.GeneratedAt, bundle.ValidUntil = now, time.Time{}
	bundle.KeyID, bundle.Signature, bundle.Signatures = "", "", nil
	bundle.PreviousGeneration = authority.Published.Bundle.Generation
	bundle.Version = groupPublicationVersion(bundle.Generation, epoch, 0)
	signed, err := publisher.Signer.SignGroupBundle(ctx, request.GroupID, bundle)
	if err != nil {
		return GroupCandidateBundle{}, err
	}
	record, err := (edgeauthority.RouteBundleRecord{GroupID: request.GroupID, Epoch: int64(epoch), BundleDigest: signedGroupBundleDigest(signed),
		SourceSHA: publisher.Identity.SourceSHA, ControlImageDigest: publisher.Identity.ControlImageDigest, InventoryDigest: head.InventoryDigest,
		ManifestDigest: publisher.Identity.ManifestDigest, HealthContractDigest: publisher.Identity.HealthContractDigest,
		IssuedAt: now.Format(time.RFC3339Nano), KeyID: signed.KeyID, Signature: signed.Signature}).Seal()
	if err != nil {
		return GroupCandidateBundle{}, err
	}
	currentRecord, err := (edgeauthority.RouteBundleRecord{GroupID: request.GroupID, Epoch: int64(authority.Published.PublicationSequence),
		BundleDigest: authority.Published.Digest, SourceSHA: publisher.Identity.SourceSHA, ControlImageDigest: publisher.Identity.ControlImageDigest,
		InventoryDigest: currentHead.InventoryDigest, ManifestDigest: publisher.Identity.ManifestDigest, HealthContractDigest: publisher.Identity.HealthContractDigest,
		IssuedAt: now.Format(time.RFC3339Nano), KeyID: authority.Published.Bundle.KeyID, Signature: authority.Published.Bundle.Signature}).Seal()
	if err != nil {
		return GroupCandidateBundle{}, err
	}
	candidate := GroupCandidateBundle{Schema: GroupCandidateBundleSchemaV1, GroupID: request.GroupID, Epoch: epoch,
		AuthorityLedgerSequence: authority.LedgerHead.Sequence, CandidateLedgerSequence: sequence,
		RouteIntentGeneration: head.RouteIntentGeneration, InventoryGeneration: head.InventoryGeneration,
		ReleaseRecordDigest: request.ReleaseRecordDigest, WorkerSourceSHA: request.WorkerSourceSHA, WorkerImageDigest: request.WorkerImageDigest,
		WorkerSlot: request.TargetWorkerSlot, PublishedAt: now, CurrentRecord: &currentRecord,
		CurrentBundle: &authority.Published.Bundle, CurrentWorkerSlot: request.ExpectedCurrentWorkerSlot,
		ServingAuthority: request.ServingAuthority, AllowDegradedPrevious: request.AllowDegradedPrevious, StandbyOnly: request.StandbyOnly,
		Record: record, Bundle: signed}
	return publisher.Store.PutGroupStagedCurrentLKGCandidateCAS(ctx, request.GroupID, currentEpoch, authority.LedgerHead.Sequence,
		request.ExpectedPublicationSequence, request.ExpectedRecoveryEpoch, request.ExpectedPublishedBundleDigest, request.ServingAuthority, candidate)
}

// stagePublicationMatchesAuthority permits a Worker that observed an older
// publication to stage against an exact, monotonic validity refresh of the
// same immutable route generation. Bootstrap staging remains exact-CAS only.
func stagePublicationMatchesAuthority(published GroupPublishedBundle, request GroupCandidateStageRequest, serving *GroupServingAuthorityWitness) bool {
	if request.ExpectedPublicationSequence == published.PublicationSequence && request.ExpectedRecoveryEpoch == published.RecoveryEpoch &&
		request.ExpectedPublishedBundleDigest == published.Digest {
		return true
	}
	if serving == nil {
		return false
	}
	generation, publicationSequence, recoveryEpoch, ok := parseGroupPublicationVersion(serving.BundleVersion)
	if request.AllowDegradedPrevious && ok && generation == published.Bundle.Generation &&
		publicationSequence <= published.PublicationSequence && recoveryEpoch <= published.RecoveryEpoch {
		// A publication refresh may advance the authority sequence while the
		// serving Front still presents an older version of the same immutable
		// bundle. This is safe only for an explicitly authorized degraded
		// recovery; ordinary transitions remain exact-CAS bound below.
		return published.PublicationSequence >= request.ExpectedPublicationSequence &&
			published.RecoveryEpoch >= request.ExpectedRecoveryEpoch
	}
	return ok && publicationSequence == request.ExpectedPublicationSequence && recoveryEpoch == request.ExpectedRecoveryEpoch &&
		generation == published.Bundle.Generation && published.PublicationSequence >= request.ExpectedPublicationSequence &&
		published.RecoveryEpoch >= request.ExpectedRecoveryEpoch &&
		(published.PublicationSequence > request.ExpectedPublicationSequence || published.RecoveryEpoch > request.ExpectedRecoveryEpoch)
}

func authorityAuditTailPreservesPublishedAuthority(entry GroupAuthorityLedgerEntry, generation string) bool {
	if entry.Status == GroupAuthorityStatusFailed {
		return entry.RecoveryEpoch == 0 && entry.LastPublishedBundleGeneration == generation
	}
	return entry.Status == GroupAuthorityStatusPublished && entry.RecoveryEpoch > 0 &&
		entry.BundleGeneration == generation && entry.LastPublishedBundleGeneration == generation &&
		entry.RecoveryReason != "" && groupAuthorityDigestPattern.MatchString(entry.PublishedBundleDigest)
}

func groupCandidateCASConflict(reason string) error {
	return fmt.Errorf("%s: %w", reason, ErrGroupAuthorityCandidateCAS)
}

func servingAuthorityCanUseCurrentPublishedFallback(version, currentGeneration string, currentPublicationSequence, currentRecoveryEpoch uint64, allowFallback bool) bool {
	generation, publicationSequence, recoveryEpoch, ok := parseGroupPublicationVersion(version)
	if !ok || strings.TrimSpace(currentGeneration) == "" {
		return false
	}
	if publicationSequence == currentPublicationSequence && recoveryEpoch == currentRecoveryEpoch {
		return generation == currentGeneration
	}
	if !allowFallback {
		return false
	}
	// Republishing an exact bundle generation only advances its authority
	// sequence, recovery epoch, and validity window. Candidates already bound
	// to that immutable generation retain their serving witness; requiring
	// every retained candidate to be rewritten would make LKG renewal depend
	// on candidate-history mutation.
	if generation == currentGeneration {
		return true
	}
	return (recoveryEpoch == currentRecoveryEpoch && publicationSequence < currentPublicationSequence) ||
		(currentRecoveryEpoch != 0 && recoveryEpoch == 0 && publicationSequence > currentPublicationSequence)
}

func servingAuthorityCanUsePrunedCurrentGeneration(version, currentGeneration string, currentPublicationSequence, currentRecoveryEpoch uint64) bool {
	generation, publicationSequence, recoveryEpoch, ok := parseGroupPublicationVersion(version)
	return ok && generation == strings.TrimSpace(currentGeneration) && publicationSequence <= currentPublicationSequence &&
		recoveryEpoch <= currentRecoveryEpoch
}

func stagedCandidateMatchesRequest(candidate GroupCandidateBundle, request GroupCandidateStageRequest, authority GroupAuthorityState) bool {
	return candidateHasStagedWorkerIdentity(candidate) && candidateBindsCurrentAuthority(candidate, authority) &&
		candidate.ReleaseRecordDigest == request.ReleaseRecordDigest && candidate.WorkerSourceSHA == request.WorkerSourceSHA &&
		candidate.WorkerImageDigest == request.WorkerImageDigest && candidate.WorkerSlot == request.TargetWorkerSlot &&
		candidate.CurrentWorkerSlot == request.ExpectedCurrentWorkerSlot && candidate.AllowDegradedPrevious == request.AllowDegradedPrevious &&
		candidate.StandbyOnly == request.StandbyOnly &&
		servingAuthorityWitnessesEqual(candidate.ServingAuthority, request.ServingAuthority)
}

func groupCandidateStageReceipt(candidate GroupCandidateBundle, request GroupCandidateStageRequest) GroupCandidateStageReceipt {
	return GroupCandidateStageReceipt{Schema: GroupCandidateStageReceiptSchemaV1, GroupID: candidate.GroupID,
		CandidateEpoch: candidate.Epoch, CandidateRecordDigest: candidate.Record.RecordDigest, ReleaseRecordDigest: candidate.ReleaseRecordDigest,
		WorkerSourceSHA: candidate.WorkerSourceSHA, WorkerImageDigest: candidate.WorkerImageDigest, WorkerSlot: candidate.WorkerSlot,
		CurrentWorkerSlot: candidate.CurrentWorkerSlot, CurrentPublishedBundleDigest: candidate.CurrentRecord.BundleDigest,
		CurrentPublicationSequence: uint64(candidate.CurrentRecord.Epoch), CurrentRecoveryEpoch: request.ExpectedRecoveryEpoch,
		AllowDegradedPrevious: candidate.AllowDegradedPrevious, StandbyOnly: candidate.StandbyOnly, OrdinaryTrafficMutation: false}
}

func validateGroupCandidateStageRequest(request GroupCandidateStageRequest) error {
	if request.Schema != GroupCandidateStageRequestSchemaV1 || request.GroupID != normalizeGroupID(request.GroupID) ||
		request.ExpectedAuthoritySequence == 0 || request.ExpectedPublicationSequence == 0 ||
		request.ExpectedPublicationSequence > request.ExpectedAuthoritySequence || !groupAuthorityDigestPattern.MatchString(request.ExpectedPublishedBundleDigest) ||
		(request.ExpectedCurrentWorkerSlot != "a" && request.ExpectedCurrentWorkerSlot != "b") ||
		(request.TargetWorkerSlot != "a" && request.TargetWorkerSlot != "b") || request.ExpectedCurrentWorkerSlot == request.TargetWorkerSlot ||
		!groupCandidateSourcePattern.MatchString(request.WorkerSourceSHA) || !groupAuthorityDigestPattern.MatchString(request.WorkerImageDigest) ||
		!groupAuthorityDigestPattern.MatchString(request.ReleaseRecordDigest) || request.Reason != strings.TrimSpace(request.Reason) ||
		len(request.Reason) < 8 || len(request.Reason) > 256 {
		return errors.New("edge-control worker candidate staging request is invalid")
	}
	if request.ServingAuthority != nil && (request.ServingAuthority.Validate() != nil ||
		request.ServingAuthority.WorkerSlot != request.ExpectedCurrentWorkerSlot) {
		return errors.New("edge-control worker candidate serving authority is invalid")
	}
	if request.AllowDegradedPrevious && request.ServingAuthority == nil {
		return errors.New("edge-control degraded previous authorization requires serving authority")
	}
	if request.StandbyOnly && (request.ServingAuthority == nil || request.AllowDegradedPrevious) {
		return errors.New("edge-control standby candidate requires serving authority without degraded recovery authorization")
	}
	return nil
}

func servingAuthorityWitnessesEqual(left, right *GroupServingAuthorityWitness) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func parseGroupPublicationVersion(version string) (string, uint64, uint64, bool) {
	version = strings.TrimSpace(version)
	recoveryIndex := strings.LastIndex(version, ".r")
	if recoveryIndex <= 0 || recoveryIndex+2 >= len(version) {
		return "", 0, 0, false
	}
	recoveryEpoch, err := strconv.ParseUint(version[recoveryIndex+2:], 10, 64)
	if err != nil {
		return "", 0, 0, false
	}
	publicationPart := version[:recoveryIndex]
	publicationIndex := strings.LastIndex(publicationPart, ".p")
	if publicationIndex <= 0 || publicationIndex+2 >= len(publicationPart) {
		return "", 0, 0, false
	}
	publicationSequence, err := strconv.ParseUint(publicationPart[publicationIndex+2:], 10, 64)
	generation := strings.TrimSpace(publicationPart[:publicationIndex])
	if err != nil || publicationSequence == 0 || generation == "" || groupPublicationVersion(generation, publicationSequence, recoveryEpoch) != version {
		return "", 0, 0, false
	}
	return generation, publicationSequence, recoveryEpoch, true
}

func SignGroupCandidateStageRequest(request *GroupCandidateStageRequest, secret []byte) error {
	if request == nil || len(secret) < 32 || len(secret) > 64 {
		return errors.New("edge-control worker candidate staging signing input is invalid")
	}
	request.Signature = ""
	raw, err := json.Marshal(request)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(raw)
	request.Signature = base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return nil
}

func authenticateGroupCandidateStage(path string, request GroupCandidateStageRequest, now time.Time) error {
	if !groupAuthorityKeyIDPattern.MatchString(request.KeyID) || !inventoryNoncePattern.MatchString(request.Nonce) ||
		request.IssuedAtUnix <= 0 || request.ExpiresAtUnix <= 0 {
		return errors.New("edge-control worker candidate staging credential is invalid")
	}
	issuedAt, expiresAt := time.Unix(request.IssuedAtUnix, 0).UTC(), time.Unix(request.ExpiresAtUnix, 0).UTC()
	if !expiresAt.After(now) || !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > maxGroupRecoveryRequestTTL ||
		issuedAt.After(now.Add(maxGroupRecoveryClockSkew)) {
		return errors.New("edge-control worker candidate staging lifetime is invalid")
	}
	keyring, err := loadGroupRecoveryKeyring(path, request.GroupID)
	if err != nil {
		return err
	}
	for _, key := range keyring.Keys {
		if key.KeyID != request.KeyID {
			continue
		}
		notBefore, notAfter := time.Unix(key.NotBeforeUnix, 0).UTC(), time.Unix(key.NotAfterUnix, 0).UTC()
		if key.Revoked || now.Before(notBefore) || !now.Before(notAfter) || issuedAt.Before(notBefore) || expiresAt.After(notAfter) {
			return errors.New("edge-control worker candidate staging key is inactive")
		}
		secret, err := base64.RawURLEncoding.DecodeString(key.Secret)
		if err != nil || len(secret) < 32 || len(secret) > 64 {
			zeroBytes(secret)
			return errors.New("edge-control worker candidate staging key is invalid")
		}
		provided, decodeErr := base64.RawURLEncoding.DecodeString(request.Signature)
		request.Signature = ""
		raw, encodeErr := json.Marshal(request)
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(raw)
		valid := decodeErr == nil && encodeErr == nil && hmac.Equal(provided, mac.Sum(nil))
		zeroBytes(secret)
		if !valid {
			return errors.New("edge-control worker candidate staging signature is invalid")
		}
		return nil
	}
	return errors.New("edge-control worker candidate staging key is unknown")
}
