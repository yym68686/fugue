package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/edgecontrol"
	"fugue/internal/releaseguardian"
)

const authorityRequestTTL = 45 * time.Second

var errAuthorityMutationUnknown = errors.New("Edge Control mutation result is unknown")

type groupAuthorityConfig struct {
	GroupID     string
	Endpoint    string
	KeyringFile string
}

type authorityRecoveryKeyring struct {
	Schema     string                 `json:"schema"`
	Generation uint64                 `json:"generation"`
	GroupID    string                 `json:"edge_group_id"`
	Keys       []authorityRecoveryKey `json:"keys"`
}

type authorityRecoveryKey struct {
	KeyID         string `json:"key_id"`
	Secret        string `json:"secret"`
	NotBeforeUnix int64  `json:"not_before_unix"`
	NotAfterUnix  int64  `json:"not_after_unix"`
	Revoked       bool   `json:"revoked"`
}

type groupAuthorityActivator struct {
	front  *frontAuthorityActivator
	config groupAuthorityConfig
	client *http.Client
	now    func() time.Time
}

type groupAuthorityTransaction struct {
	activator *groupAuthorityActivator
	front     *frontAuthorityTransaction
	promotion edgecontrol.GroupPromotionReceipt
	closed    bool
}

func newGroupAuthorityActivator(front *frontAuthorityActivator, config groupAuthorityConfig) (*groupAuthorityActivator, error) {
	endpoint, err := url.Parse(strings.TrimSpace(config.Endpoint))
	if front == nil || err != nil || endpoint.Scheme != "http" || endpoint.Host == "" || endpoint.User != nil || endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		config.GroupID != front.config.GroupID || !filepath.IsAbs(config.KeyringFile) || filepath.Clean(config.KeyringFile) != config.KeyringFile {
		return nil, errors.New("group authority activator configuration is invalid")
	}
	return &groupAuthorityActivator{front: front, config: config, client: &http.Client{Timeout: 8 * time.Second}, now: time.Now}, nil
}

func (activator *groupAuthorityActivator) BeginPromote(ctx context.Context, target releaseguardian.FrontAuthorityTarget) (releaseguardian.FrontAuthorityTransaction, error) {
	if target.GroupID != activator.config.GroupID || target.AuthoritySequence == 0 || target.PublicationSequence == 0 ||
		target.PublicationSequence > target.AuthoritySequence || target.CandidateEpoch <= target.PublicationSequence ||
		!exactSHA256Digest(target.PublishedBundleDigest) || strings.TrimSpace(target.PreviousServingGeneration) == "" {
		return nil, errors.New("group authority promotion target is invalid")
	}
	target.FrontBundleGeneration = promotedBundleVersion(target.ServingGeneration, target.AuthoritySequence+1, target.RecoveryEpoch)
	lease, err := activator.front.acquireLease(ctx)
	if err != nil {
		return nil, err
	}
	releaseOnError := true
	defer func() {
		if releaseOnError {
			_ = lease.release(context.WithoutCancel(ctx))
		}
	}()
	preflight, err := activator.front.preflight(ctx, target)
	if err != nil {
		return nil, err
	}
	promotion, err := activator.promoteControl(ctx, target)
	if err != nil {
		return nil, err
	}
	if target.FrontBundleGeneration != promotedBundleVersion(target.ServingGeneration, promotion.PublicationSequence, promotion.RecoveryEpoch) {
		return nil, errors.New("Edge Control promotion version is not target-bound")
	}
	frontTx, err := activator.front.promoteWithLease(ctx, target, lease, preflight)
	if err != nil {
		recoveryErr := activator.recoverControl(context.WithoutCancel(ctx), promotion, target.PreviousServingGeneration)
		if recoveryErr != nil {
			releaseOnError = false
			return nil, errors.Join(err, fmt.Errorf("Edge Control compensation is unknown: %w", recoveryErr))
		}
		if errors.Is(err, errFrontCompensationUnknown) {
			releaseOnError = false
		}
		return nil, err
	}
	releaseOnError = false
	return &groupAuthorityTransaction{activator: activator, front: frontTx.(*frontAuthorityTransaction), promotion: promotion}, nil
}

func (activator *groupAuthorityActivator) BeginRestore(ctx context.Context, current releaseguardian.CurrentAuthority) (releaseguardian.FrontAuthorityTransaction, error) {
	return nil, errors.New("continuous group authority restore is not enabled until a durable transition receipt is installed")
}

func (activator *groupAuthorityActivator) Finalize(ctx context.Context) error {
	lease, err := activator.front.acquireLease(ctx)
	if err != nil {
		return err
	}
	return lease.release(ctx)
}

func (transaction *groupAuthorityTransaction) Receipt() releaseguardian.FrontAuthorityReceipt {
	return transaction.front.Receipt()
}

func (transaction *groupAuthorityTransaction) Commit(ctx context.Context) error {
	if transaction == nil || transaction.closed {
		return errors.New("group authority transaction is already finalized")
	}
	if err := transaction.front.Commit(ctx); err != nil {
		return err
	}
	transaction.closed = true
	return nil
}

func (transaction *groupAuthorityTransaction) Rollback(ctx context.Context) error {
	if transaction == nil || transaction.closed {
		return errors.New("group authority transaction is already finalized")
	}
	if err := transaction.front.rollbackFront(ctx); err != nil {
		return err
	}
	if err := transaction.activator.recoverControl(ctx, transaction.promotion, transaction.promotion.PreviousBundleGeneration); err != nil {
		return err
	}
	if err := transaction.front.lease.release(context.WithoutCancel(ctx)); err != nil {
		return err
	}
	transaction.front.closed, transaction.closed = true, true
	return nil
}

func (activator *groupAuthorityActivator) promoteControl(ctx context.Context, target releaseguardian.FrontAuthorityTarget) (edgecontrol.GroupPromotionReceipt, error) {
	keyID, secret, err := activator.activeKey(activator.now().UTC())
	if err != nil {
		return edgecontrol.GroupPromotionReceipt{}, err
	}
	defer zeroSecret(secret)
	now := activator.now().UTC().Truncate(time.Second)
	request := edgecontrol.GroupPromotionRequest{Schema: edgecontrol.GroupPromotionRequestSchemaV1, KeyID: keyID, GroupID: target.GroupID,
		ExpectedAuthoritySequence: target.AuthoritySequence, ExpectedPublicationSequence: target.PublicationSequence,
		ExpectedRecoveryEpoch: target.RecoveryEpoch, ExpectedPublishedBundleDigest: target.PublishedBundleDigest,
		ExpectedCandidateEpoch: target.CandidateEpoch, CandidateRecordDigest: target.CandidateRecordDigest,
		CandidateWorkerSlot: string(target.TargetSlot), CandidateBundleGeneration: target.ServingGeneration,
		IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(authorityRequestTTL).Unix(),
		Reason: "promote independently canaried Guardian group authority"}
	request.Nonce, err = randomAuthorityNonce()
	if err != nil {
		return edgecontrol.GroupPromotionReceipt{}, err
	}
	if err := edgecontrol.SignGroupPromotionRequest(&request, secret); err != nil {
		return edgecontrol.GroupPromotionReceipt{}, err
	}
	var receipt edgecontrol.GroupPromotionReceipt
	if err := activator.post(ctx, edgecontrol.GroupPromotionPathV1, request, &receipt); err != nil {
		if !errors.Is(err, errAuthorityMutationUnknown) {
			return receipt, err
		}
		// Promotion is request-idempotent in Edge Control. Replaying these exact
		// signed bytes can only return the prior receipt or fail closed.
		if replayErr := activator.post(ctx, edgecontrol.GroupPromotionPathV1, request, &receipt); replayErr != nil {
			return receipt, errors.Join(err, replayErr)
		}
	}
	if receipt.Schema != edgecontrol.GroupPromotionReceiptSchemaV1 || receipt.GroupID != target.GroupID ||
		receipt.PreviousAuthoritySequence != target.AuthoritySequence || receipt.PreviousPublicationSequence != target.PublicationSequence ||
		receipt.PreviousRecoveryEpoch != target.RecoveryEpoch || receipt.PreviousBundleGeneration != target.PreviousServingGeneration ||
		receipt.PreviousPublishedBundleDigest != target.PublishedBundleDigest || receipt.PublicationSequence != target.AuthoritySequence+1 ||
		receipt.RecoveryEpoch != target.RecoveryEpoch || receipt.BundleGeneration != target.ServingGeneration ||
		receipt.CandidateRecordDigest != target.CandidateRecordDigest || receipt.WorkerSlot != string(target.TargetSlot) || receipt.Authority != "edge-control" ||
		!exactSHA256Digest(receipt.PublishedBundleDigest) {
		return receipt, errors.New("Edge Control promotion receipt is invalid")
	}
	return receipt, nil
}

func (activator *groupAuthorityActivator) recoverControl(ctx context.Context, promotion edgecontrol.GroupPromotionReceipt, targetGeneration string) error {
	keyID, secret, err := activator.activeKey(activator.now().UTC())
	if err != nil {
		return err
	}
	defer zeroSecret(secret)
	now := activator.now().UTC().Truncate(time.Second)
	request := edgecontrol.GroupRecoveryRequest{Schema: edgecontrol.GroupRecoveryRequestSchemaV1, KeyID: keyID, GroupID: promotion.GroupID,
		ExpectedPublicationSequence: promotion.PublicationSequence, ExpectedRecoveryEpoch: promotion.RecoveryEpoch,
		TargetBundleGeneration: targetGeneration, IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(authorityRequestTTL).Unix(),
		Reason: "rollback uncommitted Guardian group authority"}
	request.Nonce, err = randomAuthorityNonce()
	if err != nil {
		return err
	}
	if err := edgecontrol.SignGroupRecoveryRequest(&request, secret); err != nil {
		return err
	}
	var receipt edgecontrol.GroupRecoveryReceipt
	if err := activator.post(ctx, edgecontrol.GroupRecoveryPathV1, request, &receipt); err != nil {
		if errors.Is(err, errAuthorityMutationUnknown) {
			return activator.reconcileRecovery(ctx, promotion, targetGeneration)
		}
		return err
	}
	if receipt.Schema != edgecontrol.GroupRecoveryReceiptSchemaV1 || receipt.GroupID != promotion.GroupID ||
		receipt.PublicationSequence != promotion.PublicationSequence+1 || receipt.RecoveryEpoch != promotion.RecoveryEpoch+1 ||
		receipt.BundleGeneration != targetGeneration || !exactSHA256Digest(receipt.PublishedBundleDigest) ||
		receipt.Authority != "edge-control" || !receipt.PublicationEnabled {
		return errors.New("Edge Control recovery receipt is invalid")
	}
	return nil
}

func (activator *groupAuthorityActivator) reconcileRecovery(ctx context.Context, promotion edgecontrol.GroupPromotionReceipt, targetGeneration string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(activator.config.Endpoint, "/")+
		edgecontrol.AuthorityGroupReadyPrefixV1+url.PathEscape(promotion.GroupID)+"/readyz", nil)
	if err != nil {
		return err
	}
	response, err := activator.client.Do(request)
	if err != nil {
		return errAuthorityMutationUnknown
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusServiceUnavailable {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		return errAuthorityMutationUnknown
	}
	var status edgecontrol.AuthorityGroupStatus
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&status) != nil || !decodeEOF(decoder) || status.GroupID != promotion.GroupID ||
		status.PublicationSequence != promotion.PublicationSequence+1 || status.RecoveryEpoch != promotion.RecoveryEpoch+1 ||
		status.BundleGeneration != targetGeneration || !exactSHA256Digest(status.PublishedBundleDigest) {
		return errAuthorityMutationUnknown
	}
	return nil
}

func (activator *groupAuthorityActivator) post(ctx context.Context, path string, value, destination any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(activator.config.Endpoint, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := activator.client.Do(request)
	if err != nil {
		return errAuthorityMutationUnknown
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		return fmt.Errorf("Edge Control mutation was rejected: status=%d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if decoder.Decode(destination) != nil || !decodeEOF(decoder) {
		return errors.New("Edge Control mutation receipt is invalid")
	}
	return nil
}

func (activator *groupAuthorityActivator) activeKey(now time.Time) (string, []byte, error) {
	raw, err := os.ReadFile(activator.config.KeyringFile)
	if err != nil || len(raw) == 0 || len(raw) > 64<<10 {
		return "", nil, errors.New("group authority credential is unavailable")
	}
	var keyring authorityRecoveryKeyring
	if decodeStrictJSON(raw, &keyring) != nil || keyring.Schema != edgecontrol.GroupRecoveryKeyringSchemaV1 || keyring.Generation == 0 ||
		keyring.GroupID != activator.config.GroupID || len(keyring.Keys) == 0 || len(keyring.Keys) > 8 {
		return "", nil, errors.New("group authority credential is invalid")
	}
	for _, key := range keyring.Keys {
		secret, decodeErr := base64.RawURLEncoding.DecodeString(key.Secret)
		active := decodeErr == nil && len(secret) >= 32 && len(secret) <= 64 && !key.Revoked &&
			!now.Before(time.Unix(key.NotBeforeUnix, 0).UTC()) && now.Add(authorityRequestTTL).Before(time.Unix(key.NotAfterUnix, 0).UTC())
		if active && strings.TrimSpace(key.KeyID) == key.KeyID && key.KeyID != "" {
			return key.KeyID, secret, nil
		}
		zeroSecret(secret)
	}
	return "", nil, errors.New("group authority credential has no active key")
}

func randomAuthorityNonce() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

func promotedBundleVersion(generation string, sequence, recoveryEpoch uint64) string {
	return strings.TrimSpace(generation) + ".p" + fmt.Sprint(sequence) + ".r" + fmt.Sprint(recoveryEpoch)
}

func zeroSecret(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func decodeEOF(decoder *json.Decoder) bool {
	return errors.Is(decoder.Decode(&struct{}{}), io.EOF)
}
