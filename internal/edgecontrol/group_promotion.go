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

	"fugue/internal/model"
)

const (
	GroupPromotionPathV1          = "/v1/authority/group-candidate-promotions"
	GroupPromotionRequestSchemaV1 = "edge-control-group-promotion-request/v1"
	GroupPromotionReceiptSchemaV1 = "edge-control-group-promotion-receipt/v1"
	maxGroupPromotionBodyBytes    = 64 << 10
)

// GroupPromotionRequest can only promote the exact durable candidate that is
// still bound to the exact current publication. It cannot carry a manifest,
// bundle, image, or arbitrary mutation.
type GroupPromotionRequest struct {
	Schema                        string `json:"schema"`
	KeyID                         string `json:"key_id"`
	GroupID                       string `json:"edge_group_id"`
	ExpectedAuthoritySequence     uint64 `json:"expected_authority_sequence"`
	ExpectedPublicationSequence   uint64 `json:"expected_publication_sequence"`
	ExpectedRecoveryEpoch         uint64 `json:"expected_recovery_epoch"`
	ExpectedPublishedBundleDigest string `json:"expected_published_bundle_digest"`
	ExpectedCandidateEpoch        uint64 `json:"expected_candidate_epoch"`
	CandidateRecordDigest         string `json:"candidate_record_digest"`
	CandidateWorkerSlot           string `json:"candidate_worker_slot"`
	CandidateBundleGeneration     string `json:"candidate_bundle_generation"`
	IssuedAtUnix                  int64  `json:"issued_at_unix"`
	ExpiresAtUnix                 int64  `json:"expires_at_unix"`
	Nonce                         string `json:"nonce"`
	Reason                        string `json:"reason"`
	Signature                     string `json:"signature"`
}

type GroupPromotionReceipt struct {
	Schema                        string `json:"schema"`
	GroupID                       string `json:"edge_group_id"`
	PreviousAuthoritySequence     uint64 `json:"previous_authority_sequence"`
	PreviousPublicationSequence   uint64 `json:"previous_publication_sequence"`
	PreviousRecoveryEpoch         uint64 `json:"previous_recovery_epoch"`
	PreviousBundleGeneration      string `json:"previous_bundle_generation"`
	PreviousPublishedBundleDigest string `json:"previous_published_bundle_digest"`
	PublicationSequence           uint64 `json:"publication_sequence"`
	RecoveryEpoch                 uint64 `json:"recovery_epoch"`
	BundleGeneration              string `json:"bundle_generation"`
	PublishedBundleDigest         string `json:"published_bundle_digest"`
	CandidateRecordDigest         string `json:"candidate_record_digest"`
	WorkerSlot                    string `json:"worker_slot"`
	Authority                     string `json:"authority"`
}

type GroupPromotionStore interface {
	ReadGroupAuthority(context.Context, string) (GroupAuthorityState, error)
	ReadGroupCandidate(context.Context, string) (GroupCandidateBundle, bool, error)
	PromoteGroupCandidateCAS(context.Context, string, GroupPromotionRequest, GroupAuthorityLedgerEntry, model.EdgeRouteBundle) (GroupAuthorityLedgerEntry, error)
}

type GroupPromotionHandlerConfig struct {
	Store      GroupPromotionStore
	Signer     GroupBundleSigner
	GroupIDs   []string
	KeyringDir string
	Now        func() time.Time
}

type groupPromotionHandler struct {
	store      GroupPromotionStore
	signer     GroupBundleSigner
	groups     map[string]struct{}
	keyringDir string
	now        func() time.Time
}

func NewGroupPromotionHandler(config GroupPromotionHandlerConfig) (http.Handler, error) {
	if config.Store == nil || config.Signer == nil {
		return nil, errors.New("edge-control group promotion dependency is nil")
	}
	groups, err := normalizeGroupIDs(config.GroupIDs)
	if err != nil {
		return nil, err
	}
	keyringDir := strings.TrimSpace(config.KeyringDir)
	if keyringDir == "" || !filepath.IsAbs(keyringDir) || filepath.Clean(keyringDir) != keyringDir {
		return nil, errors.New("edge-control group promotion keyring directory must be an absolute normalized path")
	}
	allowed := make(map[string]struct{}, len(groups))
	for _, groupID := range groups {
		allowed[groupID] = struct{}{}
	}
	now := func() time.Time { return time.Now().UTC() }
	if config.Now != nil {
		now = func() time.Time { return config.Now().UTC() }
	}
	return &groupPromotionHandler{store: config.Store, signer: config.Signer, groups: allowed, keyringDir: keyringDir, now: now}, nil
}

func (handler *groupPromotionHandler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != GroupPromotionPathV1 || request.URL.RawQuery != "" {
		http.NotFound(w, request)
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeGroupBundleError(w, http.StatusUnsupportedMediaType, "content_type_rejected")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, maxGroupPromotionBodyBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxGroupPromotionBodyBytes {
		writeGroupBundleError(w, http.StatusBadRequest, "request_rejected")
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var promotion GroupPromotionRequest
	if decoder.Decode(&promotion) != nil || !decodeEOF(decoder) || validateGroupPromotionRequest(promotion) != nil {
		writeGroupBundleError(w, http.StatusBadRequest, "request_rejected")
		return
	}
	if _, allowed := handler.groups[promotion.GroupID]; !allowed {
		writeGroupBundleError(w, http.StatusForbidden, "group_rejected")
		return
	}
	now := handler.now()
	if authenticateGroupPromotion(filepath.Join(handler.keyringDir, promotion.GroupID+".json"), promotion, now) != nil {
		writeGroupBundleError(w, http.StatusUnauthorized, "credential_rejected")
		return
	}
	authority, err := handler.store.ReadGroupAuthority(request.Context(), promotion.GroupID)
	if err != nil || !authority.PublishedExists || !authority.LedgerExists {
		writeGroupBundleError(w, http.StatusConflict, "sequence_conflict")
		return
	}
	candidate, exists, err := handler.store.ReadGroupCandidate(request.Context(), promotion.GroupID)
	if err != nil || !exists || validateGroupCandidateBundle(promotion.GroupID, candidate) != nil ||
		candidate.Epoch != promotion.ExpectedCandidateEpoch || candidate.Record.RecordDigest != promotion.CandidateRecordDigest ||
		candidate.WorkerSlot != promotion.CandidateWorkerSlot || candidate.Bundle.Generation != promotion.CandidateBundleGeneration ||
		candidate.AuthorityLedgerSequence != promotion.ExpectedAuthoritySequence ||
		candidate.CurrentRecord == nil || candidate.CurrentRecord.BundleDigest != promotion.ExpectedPublishedBundleDigest ||
		candidate.CurrentBundle == nil || signedGroupBundleDigest(*candidate.CurrentBundle) != promotion.ExpectedPublishedBundleDigest {
		writeGroupBundleError(w, http.StatusConflict, "candidate_conflict")
		return
	}
	if receipt, replayed := groupPromotionReplayReceipt(authority, candidate, promotion); replayed {
		writeJSON(w, http.StatusOK, receipt)
		return
	}
	if authority.LedgerHead.Sequence != promotion.ExpectedAuthoritySequence ||
		authority.Published.PublicationSequence != promotion.ExpectedPublicationSequence ||
		authority.Published.RecoveryEpoch != promotion.ExpectedRecoveryEpoch ||
		authority.Published.Digest != promotion.ExpectedPublishedBundleDigest {
		writeGroupBundleError(w, http.StatusConflict, "sequence_conflict")
		return
	}
	bundle := cloneEdgeRouteBundle(candidate.Bundle)
	bundle.Issuer = groupAuthorityIssuer
	bundle.GeneratedAt = now
	bundle.ValidUntil = time.Time{}
	bundle.KeyID, bundle.Signature, bundle.Signatures = "", "", nil
	bundle.PreviousGeneration = authority.Published.Bundle.Generation
	bundle.Version = groupPublicationVersion(bundle.Generation, promotion.ExpectedAuthoritySequence+1, promotion.ExpectedRecoveryEpoch)
	signed, err := handler.signer.SignGroupBundle(request.Context(), promotion.GroupID, bundle)
	if err != nil {
		writeGroupBundleError(w, http.StatusServiceUnavailable, "signing_unavailable")
		return
	}
	entry := GroupAuthorityLedgerEntry{Schema: GroupAuthorityLedgerSchemaV1, GroupID: promotion.GroupID, Status: GroupAuthorityStatusPublished,
		CandidateLedgerSequence: candidate.CandidateLedgerSequence, RouteIntentGeneration: candidate.RouteIntentGeneration,
		InventoryGeneration: candidate.InventoryGeneration, BundleGeneration: signed.Generation,
		LastPublishedBundleGeneration: signed.Generation, PublishedBundleDigest: signedGroupBundleDigest(signed), SigningKeyID: signed.KeyID,
		Authority: "edge-control", PublicationEnabled: true, RecordedAt: now}
	appended, err := handler.store.PromoteGroupCandidateCAS(request.Context(), promotion.GroupID, promotion, entry, signed)
	if err != nil {
		if errors.Is(err, ErrGroupAuthorityCASConflict) || errors.Is(err, ErrGroupAuthorityCandidateCAS) {
			writeGroupBundleError(w, http.StatusConflict, "sequence_conflict")
			return
		}
		writeGroupBundleError(w, http.StatusServiceUnavailable, "promotion_unavailable")
		return
	}
	writeJSON(w, http.StatusOK, GroupPromotionReceipt{Schema: GroupPromotionReceiptSchemaV1, GroupID: promotion.GroupID,
		PreviousAuthoritySequence:   promotion.ExpectedAuthoritySequence,
		PreviousPublicationSequence: promotion.ExpectedPublicationSequence, PreviousRecoveryEpoch: promotion.ExpectedRecoveryEpoch,
		PreviousBundleGeneration: authority.Published.Bundle.Generation, PreviousPublishedBundleDigest: promotion.ExpectedPublishedBundleDigest,
		PublicationSequence: appended.Sequence, RecoveryEpoch: promotion.ExpectedRecoveryEpoch, BundleGeneration: appended.BundleGeneration,
		PublishedBundleDigest: appended.PublishedBundleDigest, CandidateRecordDigest: promotion.CandidateRecordDigest,
		WorkerSlot: promotion.CandidateWorkerSlot, Authority: "edge-control"})
}

func groupPromotionReplayReceipt(authority GroupAuthorityState, candidate GroupCandidateBundle, promotion GroupPromotionRequest) (GroupPromotionReceipt, bool) {
	if authority.LedgerHead.Sequence != promotion.ExpectedAuthoritySequence+1 || authority.LedgerHead.Status != GroupAuthorityStatusPublished ||
		authority.LedgerHead.CandidateLedgerSequence != candidate.CandidateLedgerSequence ||
		authority.LedgerHead.BundleGeneration != promotion.CandidateBundleGeneration ||
		authority.LedgerHead.PublishedBundleDigest != authority.Published.Digest ||
		authority.Published.PublicationSequence != promotion.ExpectedAuthoritySequence+1 ||
		authority.Published.RecoveryEpoch != promotion.ExpectedRecoveryEpoch ||
		authority.Published.Bundle.Generation != promotion.CandidateBundleGeneration ||
		authority.Published.Bundle.PreviousGeneration != candidate.CurrentBundle.Generation ||
		candidate.CurrentRecord == nil || candidate.CurrentBundle == nil ||
		candidate.CurrentRecord.BundleDigest != promotion.ExpectedPublishedBundleDigest ||
		signedGroupBundleDigest(*candidate.CurrentBundle) != promotion.ExpectedPublishedBundleDigest {
		return GroupPromotionReceipt{}, false
	}
	return GroupPromotionReceipt{Schema: GroupPromotionReceiptSchemaV1, GroupID: promotion.GroupID,
		PreviousAuthoritySequence: promotion.ExpectedAuthoritySequence, PreviousPublicationSequence: promotion.ExpectedPublicationSequence,
		PreviousRecoveryEpoch: promotion.ExpectedRecoveryEpoch, PreviousBundleGeneration: candidate.CurrentBundle.Generation,
		PreviousPublishedBundleDigest: promotion.ExpectedPublishedBundleDigest, PublicationSequence: authority.Published.PublicationSequence,
		RecoveryEpoch: authority.Published.RecoveryEpoch, BundleGeneration: authority.Published.Bundle.Generation,
		PublishedBundleDigest: authority.Published.Digest, CandidateRecordDigest: promotion.CandidateRecordDigest,
		WorkerSlot: promotion.CandidateWorkerSlot, Authority: "edge-control"}, true
}

func validateGroupPromotionRequest(request GroupPromotionRequest) error {
	if request.Schema != GroupPromotionRequestSchemaV1 || request.GroupID != normalizeGroupID(request.GroupID) ||
		request.ExpectedAuthoritySequence == 0 || request.ExpectedPublicationSequence == 0 ||
		request.ExpectedPublicationSequence > request.ExpectedAuthoritySequence || !groupAuthorityDigestPattern.MatchString(request.ExpectedPublishedBundleDigest) ||
		request.ExpectedCandidateEpoch == 0 || !groupAuthorityDigestPattern.MatchString(request.CandidateRecordDigest) ||
		(request.CandidateWorkerSlot != "a" && request.CandidateWorkerSlot != "b") || strings.TrimSpace(request.CandidateBundleGeneration) == "" ||
		request.Reason != strings.TrimSpace(request.Reason) || len(request.Reason) < 8 || len(request.Reason) > 256 {
		return errors.New("edge-control group promotion request is invalid")
	}
	return nil
}

func SignGroupPromotionRequest(request *GroupPromotionRequest, secret []byte) error {
	if request == nil || len(secret) < 32 || len(secret) > 64 {
		return errors.New("edge-control group promotion signing input is invalid")
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

func authenticateGroupPromotion(path string, request GroupPromotionRequest, now time.Time) error {
	if !groupAuthorityKeyIDPattern.MatchString(request.KeyID) || !inventoryNoncePattern.MatchString(request.Nonce) || request.IssuedAtUnix <= 0 || request.ExpiresAtUnix <= 0 {
		return errors.New("edge-control group promotion credential envelope is invalid")
	}
	issuedAt, expiresAt := time.Unix(request.IssuedAtUnix, 0).UTC(), time.Unix(request.ExpiresAtUnix, 0).UTC()
	if !expiresAt.After(now) || !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > maxGroupRecoveryRequestTTL || issuedAt.After(now.Add(maxGroupRecoveryClockSkew)) {
		return errors.New("edge-control group promotion request lifetime is invalid")
	}
	keyring, err := loadGroupRecoveryKeyring(path, request.GroupID)
	if err != nil {
		return err
	}
	for _, key := range keyring.Keys {
		if key.KeyID != request.KeyID {
			continue
		}
		secret, err := base64.RawURLEncoding.DecodeString(key.Secret)
		if err != nil || len(secret) < 32 || len(secret) > 64 {
			zeroBytes(secret)
			return errors.New("edge-control group promotion key is invalid")
		}
		active := !key.Revoked && !now.Before(time.Unix(key.NotBeforeUnix, 0).UTC()) && now.Before(time.Unix(key.NotAfterUnix, 0).UTC()) &&
			!issuedAt.Before(time.Unix(key.NotBeforeUnix, 0).UTC()) && !expiresAt.After(time.Unix(key.NotAfterUnix, 0).UTC())
		copy := request
		provided, decodeErr := base64.RawURLEncoding.DecodeString(copy.Signature)
		copy.Signature = ""
		raw, marshalErr := json.Marshal(copy)
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(raw)
		valid := active && decodeErr == nil && marshalErr == nil && hmac.Equal(provided, mac.Sum(nil))
		zeroBytes(secret)
		if !valid {
			return errors.New("edge-control group promotion signature is invalid")
		}
		return nil
	}
	return errors.New("edge-control group promotion key is unknown")
}

func decodeEOF(decoder *json.Decoder) bool {
	return errors.Is(decoder.Decode(&struct{}{}), io.EOF)
}
