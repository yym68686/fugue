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
	"sync/atomic"
	"time"

	"fugue/internal/model"
)

const (
	GroupRecoveryPathV1          = "/v1/recovery/group-publications"
	GroupRecoveryRequestSchemaV1 = "edge-control-group-recovery-request/v1"
	GroupRecoveryReceiptSchemaV1 = "edge-control-group-recovery-receipt/v1"
	GroupRecoveryKeyringSchemaV1 = "edge-control-group-recovery-keyring/v1"
	maxGroupRecoveryBodyBytes    = 64 << 10
	maxGroupRecoveryKeyringBytes = 64 << 10
	maxGroupRecoveryRequestTTL   = 2 * time.Minute
	maxGroupRecoveryClockSkew    = 30 * time.Second
)

type GroupRecoveryRequest struct {
	Schema                      string `json:"schema"`
	KeyID                       string `json:"key_id"`
	GroupID                     string `json:"edge_group_id"`
	ExpectedPublicationSequence uint64 `json:"expected_publication_sequence"`
	ExpectedRecoveryEpoch       uint64 `json:"expected_recovery_epoch"`
	TargetBundleGeneration      string `json:"target_bundle_generation"`
	IssuedAtUnix                int64  `json:"issued_at_unix"`
	ExpiresAtUnix               int64  `json:"expires_at_unix"`
	Nonce                       string `json:"nonce"`
	Reason                      string `json:"reason"`
	Signature                   string `json:"signature"`
}

type GroupRecoveryReceipt struct {
	Schema                string `json:"schema"`
	GroupID               string `json:"edge_group_id"`
	PublicationSequence   uint64 `json:"publication_sequence"`
	RecoveryEpoch         uint64 `json:"recovery_epoch"`
	BundleGeneration      string `json:"bundle_generation"`
	PublishedBundleDigest string `json:"published_bundle_digest"`
	Authority             string `json:"authority"`
	PublicationEnabled    bool   `json:"publication_enabled"`
}

type GroupRecoveryStore interface {
	ReadGroupRecoveryTarget(context.Context, string, string) (GroupAuthorityState, GroupShadowLedgerEntry, uint64, error)
	RecoverGroupAuthorityCAS(context.Context, string, uint64, uint64, GroupAuthorityLedgerEntry, model.EdgeRouteBundle) (GroupAuthorityLedgerEntry, error)
}

type currentPublishedLKGRecoveryStore interface {
	ReadGroupAuthority(context.Context, string) (GroupAuthorityState, error)
	PublishedLKGRecoveryStore
}

type GroupRecoveryHandlerConfig struct {
	Store      GroupRecoveryStore
	Signer     GroupBundleSigner
	GroupIDs   []string
	KeyringDir string
	Metrics    *GroupRecoveryMetrics
	Now        func() time.Time
}

type GroupRecoveryMetrics struct {
	accepted atomic.Uint64
	rejected atomic.Uint64
}

type GroupRecoveryMetricsSnapshot struct {
	Accepted uint64
	Rejected uint64
}

func NewGroupRecoveryMetrics() *GroupRecoveryMetrics {
	return &GroupRecoveryMetrics{}
}

func (metrics *GroupRecoveryMetrics) Snapshot() GroupRecoveryMetricsSnapshot {
	if metrics == nil {
		return GroupRecoveryMetricsSnapshot{}
	}
	return GroupRecoveryMetricsSnapshot{Accepted: metrics.accepted.Load(), Rejected: metrics.rejected.Load()}
}

func (metrics *GroupRecoveryMetrics) observe(accepted bool) {
	if metrics == nil {
		return
	}
	if accepted {
		metrics.accepted.Add(1)
		return
	}
	metrics.rejected.Add(1)
}

type groupRecoveryKeyring struct {
	Schema     string             `json:"schema"`
	Generation uint64             `json:"generation"`
	GroupID    string             `json:"edge_group_id"`
	Keys       []groupRecoveryKey `json:"keys"`
}

type groupRecoveryKey struct {
	KeyID         string `json:"key_id"`
	Secret        string `json:"secret"`
	NotBeforeUnix int64  `json:"not_before_unix"`
	NotAfterUnix  int64  `json:"not_after_unix"`
	Revoked       bool   `json:"revoked"`
}

type groupRecoveryHandler struct {
	store      GroupRecoveryStore
	signer     GroupBundleSigner
	groups     map[string]struct{}
	keyringDir string
	metrics    *GroupRecoveryMetrics
	now        func() time.Time
}

func NewGroupRecoveryHandler(config GroupRecoveryHandlerConfig) (http.Handler, error) {
	if config.Store == nil || config.Signer == nil {
		return nil, errors.New("edge-control group recovery dependency is nil")
	}
	groups, err := normalizeGroupIDs(config.GroupIDs)
	if err != nil {
		return nil, err
	}
	keyringDir := strings.TrimSpace(config.KeyringDir)
	if keyringDir == "" || !filepath.IsAbs(keyringDir) || filepath.Clean(keyringDir) != keyringDir {
		return nil, errors.New("edge-control group recovery keyring directory must be an absolute normalized path")
	}
	allowed := make(map[string]struct{}, len(groups))
	for _, groupID := range groups {
		allowed[groupID] = struct{}{}
	}
	now := func() time.Time { return time.Now().UTC() }
	if config.Now != nil {
		now = func() time.Time { return config.Now().UTC() }
	}
	return &groupRecoveryHandler{store: config.Store, signer: config.Signer, groups: allowed, keyringDir: keyringDir, metrics: config.Metrics, now: now}, nil
}

func (handler *groupRecoveryHandler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != GroupRecoveryPathV1 || request.URL.RawQuery != "" {
		http.NotFound(w, request)
		return
	}
	accepted := false
	defer func() { handler.metrics.observe(accepted) }()
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
	var recovery GroupRecoveryRequest
	if err := decoder.Decode(&recovery); err != nil {
		writeGroupBundleError(w, http.StatusBadRequest, "request_rejected")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeGroupBundleError(w, http.StatusBadRequest, "request_rejected")
		return
	}
	groupID := normalizeGroupID(recovery.GroupID)
	if recovery.Schema != GroupRecoveryRequestSchemaV1 || recovery.GroupID != groupID || recovery.ExpectedPublicationSequence == 0 ||
		strings.TrimSpace(recovery.TargetBundleGeneration) == "" || recovery.TargetBundleGeneration != strings.TrimSpace(recovery.TargetBundleGeneration) ||
		recovery.Reason != strings.TrimSpace(recovery.Reason) || len(recovery.Reason) < 8 || len(recovery.Reason) > 256 {
		writeGroupBundleError(w, http.StatusBadRequest, "request_rejected")
		return
	}
	if _, allowed := handler.groups[groupID]; !allowed {
		writeGroupBundleError(w, http.StatusForbidden, "group_rejected")
		return
	}
	now := handler.now()
	if err := authenticateGroupRecovery(filepath.Join(handler.keyringDir, groupID+".json"), recovery, now); err != nil {
		writeGroupBundleError(w, http.StatusUnauthorized, "credential_rejected")
		return
	}
	if store, ok := handler.store.(currentPublishedLKGRecoveryStore); ok {
		authority, readErr := store.ReadGroupAuthority(request.Context(), groupID)
		targetGeneration := recovery.TargetBundleGeneration
		if generation, _, _, parsed := parseGroupPublicationVersion(targetGeneration); parsed {
			targetGeneration = generation
		}
		if readErr == nil && authority.LedgerExists && authority.PublishedExists && authority.Published.Bundle.Generation == targetGeneration {
			accepted = handler.recoverCurrentPublishedLKG(w, request, store, authority, recovery, now)
			return
		}
	}
	authority, candidate, recoveryEpoch, err := handler.store.ReadGroupRecoveryTarget(request.Context(), groupID, recovery.TargetBundleGeneration)
	if err != nil {
		writeGroupBundleError(w, http.StatusBadRequest, "target_rejected")
		return
	}
	if !authority.LedgerExists || !authority.PublishedExists ||
		authority.Published.PublicationSequence != recovery.ExpectedPublicationSequence || recoveryEpoch != recovery.ExpectedRecoveryEpoch {
		writeGroupBundleError(w, http.StatusConflict, "sequence_conflict")
		return
	}
	bundle := cloneEdgeRouteBundle(*candidate.Bundle)
	bundle.Issuer = groupAuthorityIssuer
	bundle.GeneratedAt = now
	bundle.ValidUntil = time.Time{}
	bundle.KeyID = ""
	bundle.Signature = ""
	bundle.Signatures = nil
	bundle.PreviousGeneration = ""
	if authority.Published.Bundle.Generation != bundle.Generation {
		bundle.PreviousGeneration = authority.Published.Bundle.Generation
	}
	bundle.Version = groupPublicationVersion(bundle.Generation, authority.LedgerHead.Sequence+1, recovery.ExpectedRecoveryEpoch+1)
	signed, err := handler.signer.SignGroupBundle(request.Context(), groupID, bundle)
	if err != nil {
		writeGroupBundleError(w, http.StatusServiceUnavailable, "signing_unavailable")
		return
	}
	entry := GroupAuthorityLedgerEntry{
		Schema: GroupAuthorityLedgerSchemaV1, GroupID: groupID, Status: GroupAuthorityStatusPublished,
		CandidateLedgerSequence: candidate.Sequence, RouteIntentGeneration: candidate.RouteIntentGeneration,
		InventoryGeneration: candidate.InventoryGeneration, BundleGeneration: signed.Generation,
		LastPublishedBundleGeneration: signed.Generation, PublishedBundleDigest: signedGroupBundleDigest(signed),
		SigningKeyID: signed.KeyID, RecoveryEpoch: recovery.ExpectedRecoveryEpoch + 1, RecoveryReason: recovery.Reason,
		Authority: "edge-control", PublicationEnabled: true, RecordedAt: now,
	}
	appended, err := handler.store.RecoverGroupAuthorityCAS(request.Context(), groupID, recovery.ExpectedPublicationSequence, recovery.ExpectedRecoveryEpoch, entry, signed)
	if err != nil {
		if errors.Is(err, ErrGroupAuthorityCASConflict) || errors.Is(err, ErrGroupAuthorityCandidateCAS) {
			writeGroupBundleError(w, http.StatusConflict, "sequence_conflict")
			return
		}
		writeGroupBundleError(w, http.StatusServiceUnavailable, "recovery_unavailable")
		return
	}
	accepted = true
	writeJSON(w, http.StatusOK, GroupRecoveryReceipt{
		Schema: GroupRecoveryReceiptSchemaV1, GroupID: groupID, PublicationSequence: appended.Sequence,
		RecoveryEpoch: appended.RecoveryEpoch, BundleGeneration: appended.BundleGeneration,
		PublishedBundleDigest: appended.PublishedBundleDigest, Authority: "edge-control", PublicationEnabled: true,
	})
}

func (handler *groupRecoveryHandler) recoverCurrentPublishedLKG(w http.ResponseWriter, request *http.Request, store currentPublishedLKGRecoveryStore,
	authority GroupAuthorityState, recovery GroupRecoveryRequest, now time.Time) bool {
	if validateGroupPublishedBundle(recovery.GroupID, authority.Published) != nil {
		writeGroupBundleError(w, http.StatusBadRequest, "target_rejected")
		return false
	}
	if authority.Published.PublicationSequence != recovery.ExpectedPublicationSequence ||
		authority.Published.RecoveryEpoch != recovery.ExpectedRecoveryEpoch {
		writeGroupBundleError(w, http.StatusConflict, "sequence_conflict")
		return false
	}
	bundle := cloneEdgeRouteBundle(authority.Published.Bundle)
	bundle.Issuer = groupAuthorityIssuer
	bundle.GeneratedAt = now
	bundle.ValidUntil = time.Time{}
	bundle.KeyID = ""
	bundle.Signature = ""
	bundle.Signatures = nil
	bundle.PreviousGeneration = ""
	bundle.Version = groupPublicationVersion(bundle.Generation, authority.LedgerHead.Sequence+1, recovery.ExpectedRecoveryEpoch+1)
	signed, err := handler.signer.SignGroupBundle(request.Context(), recovery.GroupID, bundle)
	if err != nil {
		writeGroupBundleError(w, http.StatusServiceUnavailable, "signing_unavailable")
		return false
	}
	appended, err := store.RecoverPublishedLKG(request.Context(), recovery.GroupID, recovery.ExpectedPublicationSequence,
		recovery.ExpectedRecoveryEpoch, bundle.Generation, signed, recovery.Reason, now)
	if err != nil {
		if errors.Is(err, ErrGroupAuthorityCandidateCAS) || errors.Is(err, ErrGroupAuthorityPublishedPointerCAS) ||
			errors.Is(err, ErrGroupAuthorityRecoveryEpochCAS) || errors.Is(err, ErrGroupAuthorityAuditTailCAS) ||
			errors.Is(err, ErrGroupAuthorityCASConflict) {
			writeGroupBundleError(w, http.StatusConflict, "sequence_conflict")
			return false
		}
		writeGroupBundleError(w, http.StatusServiceUnavailable, "recovery_unavailable")
		return false
	}
	writeJSON(w, http.StatusOK, GroupRecoveryReceipt{
		Schema: GroupRecoveryReceiptSchemaV1, GroupID: recovery.GroupID, PublicationSequence: appended.Sequence,
		RecoveryEpoch: appended.RecoveryEpoch, BundleGeneration: appended.BundleGeneration,
		PublishedBundleDigest: appended.PublishedBundleDigest, Authority: "edge-control", PublicationEnabled: true,
	})
	return true
}

func authenticateGroupRecovery(path string, request GroupRecoveryRequest, now time.Time) error {
	if !groupAuthorityKeyIDPattern.MatchString(request.KeyID) || !inventoryNoncePattern.MatchString(request.Nonce) || request.IssuedAtUnix <= 0 || request.ExpiresAtUnix <= 0 {
		return errors.New("edge-control recovery credential envelope is invalid")
	}
	issuedAt := time.Unix(request.IssuedAtUnix, 0).UTC()
	expiresAt := time.Unix(request.ExpiresAtUnix, 0).UTC()
	now = now.UTC()
	if !expiresAt.After(now) || !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > maxGroupRecoveryRequestTTL || issuedAt.After(now.Add(maxGroupRecoveryClockSkew)) {
		return errors.New("edge-control recovery request lifetime is invalid")
	}
	keyring, err := loadGroupRecoveryKeyring(path, request.GroupID)
	if err != nil {
		return err
	}
	for _, key := range keyring.Keys {
		if key.KeyID != request.KeyID {
			continue
		}
		notBefore := time.Unix(key.NotBeforeUnix, 0).UTC()
		notAfter := time.Unix(key.NotAfterUnix, 0).UTC()
		if key.Revoked || now.Before(notBefore) || !now.Before(notAfter) || issuedAt.Before(notBefore) || expiresAt.After(notAfter) {
			return errors.New("edge-control recovery key is inactive")
		}
		secret, err := base64.RawURLEncoding.DecodeString(key.Secret)
		if err != nil || len(secret) < 32 || len(secret) > 64 {
			zeroBytes(secret)
			return errors.New("edge-control recovery key is invalid")
		}
		verified := verifyGroupRecoveryRequest(request, secret)
		zeroBytes(secret)
		if !verified {
			return errors.New("edge-control recovery signature is invalid")
		}
		return nil
	}
	return errors.New("edge-control recovery key is unknown")
}

func loadGroupRecoveryKeyring(path, expectedGroupID string) (groupRecoveryKeyring, error) {
	raw, err := readPrivateProjectedFile(path, maxGroupRecoveryKeyringBytes)
	if err != nil {
		return groupRecoveryKeyring{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var keyring groupRecoveryKeyring
	if err := decoder.Decode(&keyring); err != nil {
		return groupRecoveryKeyring{}, errors.New("edge-control group recovery keyring is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || keyring.Schema != GroupRecoveryKeyringSchemaV1 || keyring.Generation == 0 ||
		keyring.GroupID != normalizeGroupID(expectedGroupID) || len(keyring.Keys) == 0 || len(keyring.Keys) > 8 {
		return groupRecoveryKeyring{}, errors.New("edge-control group recovery keyring is invalid")
	}
	seen := make(map[string]struct{}, len(keyring.Keys))
	for _, key := range keyring.Keys {
		if !groupAuthorityKeyIDPattern.MatchString(key.KeyID) || key.NotBeforeUnix <= 0 || key.NotAfterUnix <= key.NotBeforeUnix {
			return groupRecoveryKeyring{}, errors.New("edge-control group recovery key is invalid")
		}
		secret, decodeErr := base64.RawURLEncoding.DecodeString(key.Secret)
		if decodeErr != nil || len(secret) < 32 || len(secret) > 64 {
			zeroBytes(secret)
			return groupRecoveryKeyring{}, errors.New("edge-control group recovery key is invalid")
		}
		zeroBytes(secret)
		if _, duplicate := seen[key.KeyID]; duplicate {
			return groupRecoveryKeyring{}, errors.New("edge-control group recovery key id is duplicated")
		}
		seen[key.KeyID] = struct{}{}
	}
	return keyring, nil
}

func SignGroupRecoveryRequest(request *GroupRecoveryRequest, secret []byte) error {
	if request == nil || len(secret) < 32 || len(secret) > 64 {
		return errors.New("edge-control group recovery signing input is invalid")
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

func verifyGroupRecoveryRequest(request GroupRecoveryRequest, secret []byte) bool {
	provided, err := base64.RawURLEncoding.DecodeString(request.Signature)
	if err != nil || len(provided) != sha256.Size {
		return false
	}
	request.Signature = ""
	raw, err := json.Marshal(request)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(raw)
	return hmac.Equal(provided, mac.Sum(nil))
}
