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
)

const (
	GroupCandidateRecoveryPathV1          = "/v1/recovery/group-worker-candidates"
	GroupCandidateRecoveryRequestSchemaV1 = "edge-control-group-candidate-recovery-request/v1"
	GroupCandidateRecoveryReceiptSchemaV1 = "edge-control-group-candidate-recovery-receipt/v1"
)

type GroupCandidateRecoveryRequest struct {
	Schema                        string `json:"schema"`
	KeyID                         string `json:"key_id"`
	GroupID                       string `json:"edge_group_id"`
	ExpectedAuthoritySequence     uint64 `json:"expected_authority_sequence"`
	ExpectedPublicationSequence   uint64 `json:"expected_publication_sequence"`
	ExpectedRecoveryEpoch         uint64 `json:"expected_recovery_epoch"`
	ExpectedPublishedBundleDigest string `json:"expected_published_bundle_digest"`
	ExpectedCandidateEpoch        uint64 `json:"expected_candidate_epoch"`
	ExpectedWorkerSourceSHA       string `json:"expected_worker_source_sha"`
	IssuedAtUnix                  int64  `json:"issued_at_unix"`
	ExpiresAtUnix                 int64  `json:"expires_at_unix"`
	Nonce                         string `json:"nonce"`
	Reason                        string `json:"reason"`
	Signature                     string `json:"signature"`
}

type GroupCandidateRecoveryReceipt struct {
	Schema                     string `json:"schema"`
	GroupID                    string `json:"edge_group_id"`
	FencedCandidateEpoch       uint64 `json:"fenced_candidate_epoch"`
	FencedWorkerSourceSHA      string `json:"fenced_worker_source_sha"`
	CurrentPublicationSequence uint64 `json:"current_publication_sequence"`
	CurrentRecoveryEpoch       uint64 `json:"current_recovery_epoch"`
	PublishedBundleDigest      string `json:"published_bundle_digest"`
	CandidateCleared           bool   `json:"candidate_cleared"`
}

type GroupCandidateRecoveryStore interface {
	FenceGroupCandidateCAS(context.Context, string, uint64, uint64, uint64, string, uint64, string) (GroupCandidateRecoveryReceipt, error)
}

type GroupCandidateRecoveryHandlerConfig struct {
	Store      GroupCandidateRecoveryStore
	GroupIDs   []string
	KeyringDir string
	Now        func() time.Time
}

type groupCandidateRecoveryHandler struct {
	store      GroupCandidateRecoveryStore
	groups     map[string]struct{}
	keyringDir string
	now        func() time.Time
}

func NewGroupCandidateRecoveryHandler(config GroupCandidateRecoveryHandlerConfig) (http.Handler, error) {
	if config.Store == nil {
		return nil, errors.New("edge-control candidate recovery store is nil")
	}
	groups, err := normalizeGroupIDs(config.GroupIDs)
	if err != nil {
		return nil, err
	}
	keyringDir := strings.TrimSpace(config.KeyringDir)
	if keyringDir == "" || !filepath.IsAbs(keyringDir) || filepath.Clean(keyringDir) != keyringDir {
		return nil, errors.New("edge-control candidate recovery keyring directory must be an absolute normalized path")
	}
	now := func() time.Time { return time.Now().UTC() }
	if config.Now != nil {
		now = func() time.Time { return config.Now().UTC() }
	}
	allowed := make(map[string]struct{}, len(groups))
	for _, groupID := range groups {
		allowed[groupID] = struct{}{}
	}
	return &groupCandidateRecoveryHandler{store: config.Store, groups: allowed, keyringDir: keyringDir, now: now}, nil
}

func (handler *groupCandidateRecoveryHandler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != GroupCandidateRecoveryPathV1 || request.URL.RawQuery != "" {
		http.NotFound(w, request)
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeGroupBundleError(w, http.StatusUnsupportedMediaType, "content_type_rejected")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, maxGroupRecoveryBodyBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxGroupRecoveryBodyBytes {
		writeGroupBundleError(w, http.StatusBadRequest, "request_rejected")
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value GroupCandidateRecoveryRequest
	if decoder.Decode(&value) != nil || !decodeEOF(decoder) || validateGroupCandidateRecoveryRequest(value) != nil {
		writeGroupBundleError(w, http.StatusBadRequest, "request_rejected")
		return
	}
	if _, allowed := handler.groups[value.GroupID]; !allowed {
		writeGroupBundleError(w, http.StatusForbidden, "group_rejected")
		return
	}
	if authenticateGroupCandidateRecovery(filepath.Join(handler.keyringDir, value.GroupID+".json"), value, handler.now()) != nil {
		writeGroupBundleError(w, http.StatusUnauthorized, "credential_rejected")
		return
	}
	receipt, err := handler.store.FenceGroupCandidateCAS(request.Context(), value.GroupID, value.ExpectedAuthoritySequence,
		value.ExpectedPublicationSequence, value.ExpectedRecoveryEpoch, value.ExpectedPublishedBundleDigest,
		value.ExpectedCandidateEpoch, value.ExpectedWorkerSourceSHA)
	if err != nil {
		if errors.Is(err, ErrGroupAuthorityCandidateCAS) || errors.Is(err, ErrGroupAuthorityCASConflict) {
			writeGroupBundleError(w, http.StatusConflict, "sequence_conflict")
			return
		}
		writeGroupBundleError(w, http.StatusServiceUnavailable, "candidate_recovery_unavailable")
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}

func validateGroupCandidateRecoveryRequest(request GroupCandidateRecoveryRequest) error {
	if request.Schema != GroupCandidateRecoveryRequestSchemaV1 || request.GroupID != normalizeGroupID(request.GroupID) ||
		request.ExpectedAuthoritySequence == 0 || request.ExpectedPublicationSequence == 0 ||
		request.ExpectedPublicationSequence > request.ExpectedAuthoritySequence || request.ExpectedCandidateEpoch == 0 ||
		!groupAuthorityDigestPattern.MatchString(request.ExpectedPublishedBundleDigest) ||
		!groupCandidateSourcePattern.MatchString(request.ExpectedWorkerSourceSHA) ||
		request.Reason != strings.TrimSpace(request.Reason) || len(request.Reason) < 8 || len(request.Reason) > 256 {
		return errors.New("edge-control candidate recovery request is invalid")
	}
	return nil
}

func SignGroupCandidateRecoveryRequest(request *GroupCandidateRecoveryRequest, secret []byte) error {
	if request == nil || len(secret) < 32 || len(secret) > 64 {
		return errors.New("edge-control candidate recovery signing input is invalid")
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

func authenticateGroupCandidateRecovery(path string, request GroupCandidateRecoveryRequest, now time.Time) error {
	if !groupAuthorityKeyIDPattern.MatchString(request.KeyID) || !inventoryNoncePattern.MatchString(request.Nonce) ||
		request.IssuedAtUnix <= 0 || request.ExpiresAtUnix <= 0 {
		return errors.New("edge-control candidate recovery credential is invalid")
	}
	issuedAt, expiresAt := time.Unix(request.IssuedAtUnix, 0).UTC(), time.Unix(request.ExpiresAtUnix, 0).UTC()
	if !expiresAt.After(now) || !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > maxGroupRecoveryRequestTTL ||
		issuedAt.After(now.Add(maxGroupRecoveryClockSkew)) {
		return errors.New("edge-control candidate recovery lifetime is invalid")
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
			return errors.New("edge-control candidate recovery key is inactive")
		}
		secret, err := base64.RawURLEncoding.DecodeString(key.Secret)
		if err != nil || len(secret) < 32 || len(secret) > 64 {
			zeroBytes(secret)
			return errors.New("edge-control candidate recovery key is invalid")
		}
		provided, decodeErr := base64.RawURLEncoding.DecodeString(request.Signature)
		request.Signature = ""
		raw, encodeErr := json.Marshal(request)
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(raw)
		valid := decodeErr == nil && encodeErr == nil && hmac.Equal(provided, mac.Sum(nil))
		zeroBytes(secret)
		if valid {
			return nil
		}
		return errors.New("edge-control candidate recovery signature is invalid")
	}
	return errors.New("edge-control candidate recovery key is unknown")
}
