package edgecontrol

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"path/filepath"
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

// GroupCandidateStageRequest can only attach an immutable Worker release to
// the inactive slot and the exact current signed LKG. It carries no route,
// manifest, publication, or ordinary-traffic mutation.
type GroupCandidateStageRequest struct {
	Schema                        string `json:"schema"`
	KeyID                         string `json:"key_id"`
	GroupID                       string `json:"edge_group_id"`
	ExpectedAuthoritySequence     uint64 `json:"expected_authority_sequence"`
	ExpectedPublicationSequence   uint64 `json:"expected_publication_sequence"`
	ExpectedRecoveryEpoch         uint64 `json:"expected_recovery_epoch"`
	ExpectedPublishedBundleDigest string `json:"expected_published_bundle_digest"`
	ExpectedCandidateEpoch        uint64 `json:"expected_candidate_epoch"`
	ExpectedCurrentWorkerSlot     string `json:"expected_current_worker_slot"`
	TargetWorkerSlot              string `json:"target_worker_slot"`
	WorkerSourceSHA               string `json:"worker_source_sha"`
	WorkerImageDigest             string `json:"worker_image_digest"`
	ReleaseRecordDigest           string `json:"release_record_digest"`
	IssuedAtUnix                  int64  `json:"issued_at_unix"`
	ExpiresAtUnix                 int64  `json:"expires_at_unix"`
	Nonce                         string `json:"nonce"`
	Reason                        string `json:"reason"`
	Signature                     string `json:"signature"`
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
			writeGroupBundleError(w, http.StatusConflict, "sequence_conflict")
			return
		}
		writeGroupBundleError(w, http.StatusServiceUnavailable, "candidate_unavailable")
		return
	}
	writeJSON(w, http.StatusOK, groupCandidateStageReceipt(candidate, value))
}

func (publisher GroupCandidatePublisher) stageWorkerCurrentLKG(ctx context.Context, request GroupCandidateStageRequest, now time.Time) (GroupCandidateBundle, error) {
	snapshot, err := publisher.Store.ReadGroupCandidateStage(ctx, request.GroupID)
	authority := snapshot.Authority
	if err != nil || !authority.LedgerExists || !authority.PublishedExists || validateGroupPublishedBundle(request.GroupID, authority.Published) != nil ||
		authority.LedgerHead.Sequence != request.ExpectedAuthoritySequence || authority.Published.PublicationSequence != request.ExpectedPublicationSequence ||
		authority.Published.RecoveryEpoch != request.ExpectedRecoveryEpoch || authority.Published.Digest != request.ExpectedPublishedBundleDigest {
		return GroupCandidateBundle{}, ErrGroupAuthorityCandidateCAS
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
		return GroupCandidateBundle{}, ErrGroupAuthorityCandidateCAS
	}
	inventory := snapshot.Inventory
	if !snapshot.InventoryExists || inventory.ActiveEpoch.Slot != request.ExpectedCurrentWorkerSlot ||
		inventory.ObservedAt.IsZero() || inventory.ObservedAt.After(now.Add(maxInventoryHeartbeatClockSkew)) ||
		now.Sub(inventory.ObservedAt) > GroupInventoryHeartbeatMaxAge {
		return GroupCandidateBundle{}, ErrGroupAuthorityCandidateCAS
	}
	sequence := authority.Published.CandidateLedgerSequence
	if sequence == 0 {
		return GroupCandidateBundle{}, ErrGroupAuthorityCandidateCAS
	}
	head := snapshot.PublishedCandidate
	if head.Sequence != sequence || head.Status != GroupShadowStatusCompiled || head.Bundle == nil || head.BundleArchived ||
		head.BundleGeneration != authority.Published.Bundle.Generation || !groupAuthorityDigestPattern.MatchString(head.InventoryDigest) {
		return GroupCandidateBundle{}, ErrGroupAuthorityCandidateCAS
	}
	epoch := authority.Published.PublicationSequence + 1
	if currentEpoch >= epoch {
		epoch = currentEpoch + 1
	}
	bundle := cloneEdgeRouteBundle(authority.Published.Bundle)
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
		InventoryDigest: head.InventoryDigest, ManifestDigest: publisher.Identity.ManifestDigest, HealthContractDigest: publisher.Identity.HealthContractDigest,
		IssuedAt: now.Format(time.RFC3339Nano), KeyID: authority.Published.Bundle.KeyID, Signature: authority.Published.Bundle.Signature}).Seal()
	if err != nil {
		return GroupCandidateBundle{}, err
	}
	candidate := GroupCandidateBundle{Schema: GroupCandidateBundleSchemaV1, GroupID: request.GroupID, Epoch: epoch,
		AuthorityLedgerSequence: authority.LedgerHead.Sequence, CandidateLedgerSequence: sequence,
		RouteIntentGeneration: head.RouteIntentGeneration, InventoryGeneration: head.InventoryGeneration,
		ReleaseRecordDigest: request.ReleaseRecordDigest, WorkerSourceSHA: request.WorkerSourceSHA, WorkerImageDigest: request.WorkerImageDigest,
		WorkerSlot: request.TargetWorkerSlot, PublishedAt: now, CurrentRecord: &currentRecord,
		CurrentBundle: &authority.Published.Bundle, CurrentWorkerSlot: request.ExpectedCurrentWorkerSlot, Record: record, Bundle: signed}
	return publisher.Store.PutGroupStagedCurrentLKGCandidateCAS(ctx, request.GroupID, currentEpoch, request.ExpectedAuthoritySequence,
		request.ExpectedPublicationSequence, request.ExpectedRecoveryEpoch, request.ExpectedPublishedBundleDigest, candidate)
}

func stagedCandidateMatchesRequest(candidate GroupCandidateBundle, request GroupCandidateStageRequest, authority GroupAuthorityState) bool {
	return candidateHasStagedWorkerIdentity(candidate) && candidateBindsCurrentAuthority(candidate, authority) &&
		candidate.ReleaseRecordDigest == request.ReleaseRecordDigest && candidate.WorkerSourceSHA == request.WorkerSourceSHA &&
		candidate.WorkerImageDigest == request.WorkerImageDigest && candidate.WorkerSlot == request.TargetWorkerSlot &&
		candidate.CurrentWorkerSlot == request.ExpectedCurrentWorkerSlot
}

func groupCandidateStageReceipt(candidate GroupCandidateBundle, request GroupCandidateStageRequest) GroupCandidateStageReceipt {
	return GroupCandidateStageReceipt{Schema: GroupCandidateStageReceiptSchemaV1, GroupID: candidate.GroupID,
		CandidateEpoch: candidate.Epoch, CandidateRecordDigest: candidate.Record.RecordDigest, ReleaseRecordDigest: candidate.ReleaseRecordDigest,
		WorkerSourceSHA: candidate.WorkerSourceSHA, WorkerImageDigest: candidate.WorkerImageDigest, WorkerSlot: candidate.WorkerSlot,
		CurrentWorkerSlot: candidate.CurrentWorkerSlot, CurrentPublishedBundleDigest: candidate.CurrentRecord.BundleDigest,
		CurrentPublicationSequence: uint64(candidate.CurrentRecord.Epoch), CurrentRecoveryEpoch: request.ExpectedRecoveryEpoch, OrdinaryTrafficMutation: false}
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
	return nil
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
