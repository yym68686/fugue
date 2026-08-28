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
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/edgecontrol"
	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	authorityRequestTTL       = 2 * time.Minute
	authorityMutationTimeout  = 60 * time.Second
	authorityReconcileTimeout = 10 * time.Second
)

var errAuthorityMutationUnknown = errors.New("Edge Control mutation result is unknown")
var errAuthorityPrewriteCASChanged = errors.New("Edge Control prewrite CAS changed")

type groupAuthorityConfig struct {
	GroupID     string
	Endpoint    string
	KeyringFile string
	SlotA       string
	SlotB       string
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
		config.GroupID != front.config.GroupID || !filepath.IsAbs(config.KeyringFile) || filepath.Clean(config.KeyringFile) != config.KeyringFile ||
		!validAuthoritySlotAddress(config.SlotA) || !validAuthoritySlotAddress(config.SlotB) || config.SlotA == config.SlotB {
		return nil, errors.New("group authority activator configuration is invalid")
	}
	return &groupAuthorityActivator{front: front, config: config, client: &http.Client{Timeout: authorityMutationTimeout}, now: time.Now}, nil
}

func validAuthoritySlotAddress(value string) bool {
	host, port, err := net.SplitHostPort(strings.TrimSpace(value))
	parsed, portErr := strconv.Atoi(port)
	return err == nil && portErr == nil && strings.TrimSpace(host) != "" && parsed > 0 && parsed <= 65535
}

func (activator *groupAuthorityActivator) ObserveCurrentAndLKG(ctx context.Context, current releaseguardian.CurrentAuthority) (bool, bool, string, error) {
	if activator == nil || current.Validate() != nil || current.GroupID != activator.config.GroupID || current.PreviousRecordDigest == "" ||
		current.PreviousWorkerSlot.Validate() != nil || current.CurrentWorkerSlot == current.PreviousWorkerSlot {
		return false, false, "", errors.New("group authority health observation is invalid")
	}
	probe := canaryProbe{Address: activator.front.config.RouteAddress, Host: activator.front.config.RouteHost, Path: activator.front.config.RoutePath}
	status, body, headers, currentErr := requestPublicRouteWithHeaders(ctx, probe)
	currentHealthy := authorityCurrentRouteMatches(status, body, headers, currentErr, activator.front.config.RouteBodyDigest, current.CurrentWorkerSlot)
	currentRuntimeHealthy, currentRuntimeErr := activator.front.observeAuthorityRuntime(ctx, current.CurrentWorkerSlot,
		current.CurrentWorkerSourceSHA, current.CurrentWorkerImageDigest, current.CurrentFrontGeneration, current.CurrentBundleGeneration, true)
	currentHealthy = currentHealthy && currentRuntimeHealthy
	address := activator.config.SlotA
	if current.PreviousWorkerSlot == releaseguardian.AuthoritySlotB {
		address = activator.config.SlotB
	}
	status, body, headers, lkgErr := requestCandidateRoute(ctx, address, activator.front.config.RouteHost, activator.front.config.RoutePath)
	lkgRuntimeHealthy, lkgRuntimeErr := activator.front.observeAuthorityRuntime(ctx, current.PreviousWorkerSlot,
		current.PreviousWorkerSourceSHA, current.PreviousWorkerImageDigest, 0, current.PreviousBundleGeneration, false)
	lkgHealthy := lkgRuntimeHealthy && authorityRouteMatches(status, body, headers, lkgErr, activator.front.config.RouteBodyDigest,
		current.PreviousRecordDigest, current.PreviousWorkerSlot, true)
	if !lkgHealthy && lkgRuntimeHealthy && lkgErr == nil && status == http.StatusOK && shaDigest(body) == activator.front.config.RouteBodyDigest {
		if groupStatus, statusErr := activator.groupStatus(ctx, current.GroupID); statusErr == nil {
			lkgHealthy = recoverableLKGWitness(groupStatus, current.PreviousBundleGeneration, headers, current.PreviousWorkerSlot, activator.now().UTC())
		}
	}
	evidence, err := declarativerelease.CanonicalJSON(map[string]any{
		"groupId": current.GroupID, "currentRecordDigest": current.CurrentRecordDigest, "currentSlot": current.CurrentWorkerSlot,
		"currentHealthy": currentHealthy, "currentError": errorClass(currentErr), "currentRuntimeError": errorClass(currentRuntimeErr),
		"lkgRecordDigest": current.PreviousRecordDigest, "lkgSlot": current.PreviousWorkerSlot, "lkgHealthy": lkgHealthy,
		"lkgError": errorClass(lkgErr), "lkgRuntimeError": errorClass(lkgRuntimeErr),
	})
	if err != nil {
		return false, false, "", err
	}
	return currentHealthy, lkgHealthy, shaDigest(evidence), nil
}

// authorityCurrentRouteMatches checks code routing identity only. The record
// digest names configuration and may advance independently after code commit.
func authorityCurrentRouteMatches(status int, body []byte, headers http.Header, requestErr error, expectedBodyDigest string,
	slot releaseguardian.AuthoritySlot) bool {
	return requestErr == nil && status == http.StatusOK && shaDigest(body) == expectedBodyDigest &&
		exactSHA256Digest(strings.TrimSpace(headers.Get("X-Fugue-Candidate-Record-Digest"))) &&
		releaseguardian.AuthoritySlot(strings.TrimSpace(headers.Get("X-Fugue-Candidate-Worker-Slot"))) == slot
}

// recoverableLKGWitness authorizes the existing restore transaction when an
// inactive Worker is healthy but still serves a stale candidate envelope. The
// witness is deliberately weaker than settled LKG health: it requires a
// live, valid Edge Control publication for the same immutable generation and
// leaves the final activation and public-route checks mandatory.
func recoverableLKGWitness(status edgecontrol.AuthorityGroupStatus, expectedBundle string, headers http.Header, slot releaseguardian.AuthoritySlot, now time.Time) bool {
	if status.GroupID == "" || !status.Ready || !status.ServingHealthy ||
		(status.LKGState != edgecontrol.GroupAuthorityLKGCurrent && status.LKGState != edgecontrol.GroupAuthorityLKGPreserved) ||
		status.CurrentPublicationSequence == 0 || !exactSHA256Digest(status.PublishedBundleDigest) || status.BundleValidUntil == nil || !status.BundleValidUntil.After(now) ||
		authorityGenerationBase(status.BundleGeneration) != authorityGenerationBase(expectedBundle) {
		return false
	}
	observedSlot := strings.TrimSpace(headers.Get("X-Fugue-Candidate-Worker-Slot"))
	observedRecord := strings.TrimSpace(headers.Get("X-Fugue-Candidate-Record-Digest"))
	if observedSlot == "" && observedRecord == "" {
		return true
	}
	return observedSlot == string(slot) && exactSHA256Digest(observedRecord)
}

// authorityRouteMatches permits an unattested route only for a previous LKG.
// Old LKG workers predate candidate headers; their record binding is instead
// supplied by observeAuthorityRuntime's exact source/image and bundle witness.
// A partially populated or wrong attestation always fails closed.
func authorityRouteMatches(status int, body []byte, headers http.Header, requestErr error, expectedBodyDigest, recordDigest string,
	slot releaseguardian.AuthoritySlot, allowUnattestedLKG bool) bool {
	if requestErr != nil || status != http.StatusOK || shaDigest(body) != expectedBodyDigest {
		return false
	}
	record := strings.TrimSpace(headers.Get("X-Fugue-Candidate-Record-Digest"))
	observedSlot := strings.TrimSpace(headers.Get("X-Fugue-Candidate-Worker-Slot"))
	if record == "" && observedSlot == "" {
		return allowUnattestedLKG
	}
	return record == recordDigest && releaseguardian.AuthoritySlot(observedSlot) == slot
}

func (activator *groupAuthorityActivator) BeginPromote(ctx context.Context, target releaseguardian.FrontAuthorityTarget) (releaseguardian.FrontAuthorityTransaction, error) {
	if target.GroupID != activator.config.GroupID || target.AuthoritySequence == 0 || target.PublicationSequence == 0 ||
		target.PublicationSequence > target.AuthoritySequence || target.CandidateEpoch <= target.PublicationSequence ||
		!exactSHA256Digest(target.PublishedBundleDigest) || strings.TrimSpace(target.PreviousServingGeneration) == "" {
		return nil, errors.New("group authority promotion target is invalid")
	}
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
	promotion, err := activator.promoteControl(ctx, target)
	if err != nil {
		return nil, err
	}
	frontBundleGeneration := promotedBundleVersion(target.ServingGeneration, promotion.PublicationSequence, promotion.RecoveryEpoch)
	if target.FrontBundleGeneration != "" && !sameAuthorityBundleGeneration(target.FrontBundleGeneration, frontBundleGeneration) {
		if recoveryErr := activator.recoverControl(context.WithoutCancel(ctx), promotion, target.PreviousServingGeneration); recoveryErr != nil {
			releaseOnError = false
			return nil, errors.Join(errors.New("Edge Control promotion generation is not target-bound"),
				fmt.Errorf("Edge Control compensation is unknown: %w", recoveryErr))
		}
		return nil, errors.New("Edge Control promotion generation is not target-bound")
	}
	target.FrontBundleGeneration = frontBundleGeneration
	preflight, err := activator.front.preflight(ctx, target)
	if err != nil {
		if recoveryErr := activator.recoverControl(context.WithoutCancel(ctx), promotion, target.PreviousServingGeneration); recoveryErr != nil {
			releaseOnError = false
			return nil, errors.Join(err, fmt.Errorf("Edge Control compensation is unknown: %w", recoveryErr))
		}
		return nil, err
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
	if activator == nil || activator.front == nil || current.Validate() != nil || current.GroupID != activator.config.GroupID || current.PreviousRecordDigest == "" ||
		current.PreviousWorkerSlot.Validate() != nil || current.CurrentWorkerSlot == current.PreviousWorkerSlot {
		return nil, errors.New("group authority restore target is invalid")
	}
	// CurrentAuthority owns code and Front activation identity, not the mutable
	// Edge Control publication pointer. The previous Worker was already proven
	// healthy by ObserveCurrentAndLKG, so a code rollback only changes Front.
	return activator.front.BeginRestore(ctx, current)
}

func (activator *groupAuthorityActivator) Finalize(ctx context.Context) error {
	lease, err := activator.front.acquireLease(ctx)
	if err != nil {
		return err
	}
	return lease.release(ctx)
}

func (*groupAuthorityActivator) IsPrewriteCASChanged(err error) bool {
	return errors.Is(err, errAuthorityPrewriteCASChanged)
}

// PreparedSettled proves that a historical Control self-candidate never
// became authority (or was exactly compensated) before its immutable prepared
// journal is retired. It performs no mutation and cannot bless a staged
// Worker candidate.
func (activator *groupAuthorityActivator) PreparedSettled(ctx context.Context, journal releaseguardian.AuthorityTransitionJournal) (bool, error) {
	if activator == nil || journal.Validate() != nil || journal.Phase != releaseguardian.AuthorityTransitionPrepared ||
		journal.GroupID != activator.config.GroupID || journal.Candidate.HasWorkerReleaseIdentity() {
		return false, errors.New("prepared authority settlement input is invalid")
	}
	target := releaseguardian.FrontAuthorityTarget{GroupID: journal.GroupID, TargetSlot: journal.Candidate.WorkerSlot,
		CandidateBundleGeneration: journal.Candidate.BundleGeneration, ServingGeneration: journal.Candidate.ServingGeneration,
		FrontBundleGeneration: journal.Candidate.BundleGeneration, WorkerSourceSHA: journal.Before.CurrentWorkerSourceSHA,
		WorkerImageDigest: journal.Before.CurrentWorkerImageDigest, CandidateRecordDigest: journal.Candidate.RecordDigest,
		PreviousSlot:            journal.Before.CurrentWorkerSlot,
		PreviousFrontGeneration: journal.Before.CurrentFrontGeneration, PreviousBundleGeneration: journal.Before.CurrentBundleGeneration,
		PreviousWorkerSourceSHA: journal.Before.CurrentWorkerSourceSHA, PreviousWorkerImageDigest: journal.Before.CurrentWorkerImageDigest}
	preflight, err := activator.front.preflightForOperation(ctx, target, edgegroupfront.ActivationOperationPromote)
	if err != nil {
		return false, err
	}
	if preflight.alreadyAtNew {
		return false, nil
	}
	lease, err := activator.front.client.CoordinationV1().Leases(activator.front.config.Namespace).
		Get(ctx, "fugue-authority-"+journal.GroupID, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil || lease.UID == "" || lease.ResourceVersion == "" {
		return false, errors.New("prepared authority Lease observation is unavailable")
	}
	holder := strings.TrimSpace(valueOrEmpty(lease.Spec.HolderIdentity))
	if holder == "" {
		return true, nil
	}
	if lease.Spec.RenewTime == nil || lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds <= 0 {
		return false, nil
	}
	return !activator.now().UTC().Before(lease.Spec.RenewTime.Time.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second)), nil
}

// CompensationSettled proves both sides of an activated-but-uncommitted
// transaction are back at their exact LKG. It performs no mutation: the
// Edge Control status endpoint and Front activation files are read-only CAS
// witnesses. A mismatch leaves the immutable journal in place.
func (activator *groupAuthorityActivator) CompensationSettled(ctx context.Context, journal releaseguardian.AuthorityTransitionJournal) (bool, error) {
	if activator == nil || journal.Validate() != nil || journal.Phase != releaseguardian.AuthorityTransitionActivated ||
		journal.GroupID != activator.config.GroupID || journal.Activation == nil {
		return false, errors.New("authority compensation settlement input is invalid")
	}
	activation := *journal.Activation
	target := releaseguardian.FrontAuthorityTarget{
		GroupID: journal.GroupID, TargetSlot: activation.PreviousSlot,
		CandidateBundleGeneration: activation.PreviousBundleGeneration,
		ServingGeneration:         activation.PreviousBundleGeneration,
		FrontBundleGeneration:     activation.PreviousBundleGeneration,
		WorkerSourceSHA:           activation.PreviousWorkerSourceSHA,
		WorkerImageDigest:         activation.PreviousWorkerImageDigest,
		PreviousSlot:              activation.TargetSlot,
		PreviousFrontGeneration:   activation.TargetGeneration,
		PreviousBundleGeneration:  activation.TargetBundleGeneration,
		PreviousWorkerSourceSHA:   activation.TargetWorkerSourceSHA,
		PreviousWorkerImageDigest: activation.TargetWorkerImageDigest,
	}
	preflight, err := activator.front.preflightForOperation(ctx, target, edgegroupfront.ActivationOperationRollback)
	if err != nil || !preflight.alreadyAtNew {
		return false, err
	}
	status, err := activator.groupStatus(ctx, journal.GroupID)
	if err != nil || !edgeControlCompensationSettled(status, journal) {
		if err == nil {
			err = errors.New("Edge Control LKG recovery witness is invalid")
		}
		return false, err
	}
	return true, nil
}

func edgeControlCompensationSettled(status edgecontrol.AuthorityGroupStatus, journal releaseguardian.AuthorityTransitionJournal) bool {
	return journal.Validate() == nil && journal.Phase == releaseguardian.AuthorityTransitionActivated &&
		status.GroupID == journal.GroupID && status.PublicationSequence >= journal.Candidate.AuthoritySequence+2 &&
		status.RecoveryEpoch >= journal.Candidate.CurrentRecoveryEpoch+1 &&
		status.BundleGeneration == journal.Candidate.CurrentServingGeneration &&
		(status.LKGState == edgecontrol.GroupAuthorityLKGCurrent || status.LKGState == edgecontrol.GroupAuthorityLKGPreserved) &&
		exactSHA256Digest(status.PublishedBundleDigest)
}

func (activator *groupAuthorityActivator) groupStatus(ctx context.Context, groupID string) (edgecontrol.AuthorityGroupStatus, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(activator.config.Endpoint, "/")+
		edgecontrol.AuthorityGroupReadyPrefixV1+url.PathEscape(groupID)+"/readyz", nil)
	if err != nil {
		return edgecontrol.AuthorityGroupStatus{}, err
	}
	response, err := activator.client.Do(request)
	if err != nil {
		return edgecontrol.AuthorityGroupStatus{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusServiceUnavailable {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		return edgecontrol.AuthorityGroupStatus{}, errors.New("Edge Control group status is unavailable")
	}
	var status edgecontrol.AuthorityGroupStatus
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&status) != nil || !decodeEOF(decoder) || status.GroupID != groupID {
		return edgecontrol.AuthorityGroupStatus{}, errors.New("Edge Control group status is invalid")
	}
	return status, nil
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
		// A committed promotion can lose its response while Edge Control is
		// serializing the large signed bundle. Reconcile the compact authority
		// status first; only an explicit non-commit falls back to replaying the
		// exact signed request.
		var reconcileErr error
		// During a same-generation recovery, an ordinary validity refresh is
		// indistinguishable from promotion in the compact status projection.
		// Replay the exact signed request instead; Edge Control reconciles a
		// committed request by its retained candidate epoch.
		if target.ServingGeneration != target.PreviousServingGeneration {
			reconcileCtx, cancel := context.WithTimeout(ctx, authorityReconcileTimeout)
			receipt, reconcileErr = activator.reconcilePromotionReceipt(reconcileCtx, target)
			cancel()
		} else {
			reconcileErr = errAuthorityMutationUnknown
		}
		if reconcileErr != nil {
			if replayErr := activator.post(ctx, edgecontrol.GroupPromotionPathV1, request, &receipt); replayErr != nil {
				return receipt, errors.Join(err, reconcileErr, replayErr)
			}
		}
	}
	if receipt.Schema != edgecontrol.GroupPromotionReceiptSchemaV1 || receipt.GroupID != target.GroupID ||
		receipt.PreviousAuthoritySequence < target.AuthoritySequence || receipt.PreviousPublicationSequence < target.PublicationSequence ||
		receipt.PreviousRecoveryEpoch < target.RecoveryEpoch || receipt.PreviousBundleGeneration != target.PreviousServingGeneration ||
		!exactSHA256Digest(receipt.PreviousPublishedBundleDigest) || receipt.PublicationSequence != receipt.PreviousAuthoritySequence+1 ||
		receipt.RecoveryEpoch != target.RecoveryEpoch || receipt.BundleGeneration != target.ServingGeneration ||
		receipt.CandidateRecordDigest != target.CandidateRecordDigest || receipt.WorkerSlot != string(target.TargetSlot) || receipt.Authority != "edge-control" ||
		!exactSHA256Digest(receipt.PublishedBundleDigest) {
		return receipt, errors.New("Edge Control promotion receipt is invalid")
	}
	return receipt, nil
}

func sameAuthorityBundleGeneration(left, right string) bool {
	leftGeneration, _, _, leftErr := splitPromotedBundleVersion(left)
	rightGeneration, _, _, rightErr := splitPromotedBundleVersion(right)
	return leftErr == nil && rightErr == nil && leftGeneration == rightGeneration
}

func (activator *groupAuthorityActivator) reconcilePromotionReceipt(ctx context.Context, target releaseguardian.FrontAuthorityTarget) (edgecontrol.GroupPromotionReceipt, error) {
	status, err := activator.groupStatus(ctx, target.GroupID)
	if err != nil {
		return edgecontrol.GroupPromotionReceipt{}, errAuthorityMutationUnknown
	}
	if status.GroupID != target.GroupID || status.AuthoritySequence <= target.AuthoritySequence ||
		status.CurrentPublicationSequence <= target.PublicationSequence ||
		status.RecoveryEpoch != target.RecoveryEpoch || status.BundleGeneration != target.ServingGeneration ||
		status.CandidateEpoch < target.CandidateEpoch || status.CandidateWorkerSourceSHA != target.WorkerSourceSHA ||
		!exactSHA256Digest(status.PublishedBundleDigest) {
		return edgecontrol.GroupPromotionReceipt{}, errAuthorityMutationUnknown
	}
	return edgecontrol.GroupPromotionReceipt{Schema: edgecontrol.GroupPromotionReceiptSchemaV1, GroupID: target.GroupID,
		PreviousAuthoritySequence: status.AuthoritySequence - 1, PreviousPublicationSequence: target.PublicationSequence,
		PreviousRecoveryEpoch: target.RecoveryEpoch, PreviousBundleGeneration: target.PreviousServingGeneration,
		PreviousPublishedBundleDigest: target.PublishedBundleDigest, PublicationSequence: status.CurrentPublicationSequence,
		RecoveryEpoch: status.RecoveryEpoch, BundleGeneration: target.ServingGeneration, PublishedBundleDigest: status.PublishedBundleDigest,
		CandidateRecordDigest: target.CandidateRecordDigest, WorkerSlot: string(target.TargetSlot), Authority: "edge-control"}, nil
}

func (activator *groupAuthorityActivator) recoverControl(ctx context.Context, promotion edgecontrol.GroupPromotionReceipt, targetGeneration string) error {
	_, err := activator.recoverControlReceipt(ctx, promotion, targetGeneration)
	return err
}

func (activator *groupAuthorityActivator) recoverControlReceipt(ctx context.Context, promotion edgecontrol.GroupPromotionReceipt, targetGeneration string) (edgecontrol.GroupRecoveryReceipt, error) {
	if receipt, err := activator.reconcileRecoveryReceipt(ctx, promotion, targetGeneration); err == nil {
		return receipt, nil
	}
	// A prior recovery may have committed and then expired before Front could
	// finish its CAS. Refresh that exact published LKG rather than replaying the
	// stale pre-recovery sequence. PublicationSequence is the audit-ledger head;
	// CurrentPublicationSequence is the only CAS value for the live bundle.
	if status, statusErr := activator.groupStatus(ctx, promotion.GroupID); statusErr == nil &&
		status.BundleGeneration == targetGeneration && status.CurrentPublicationSequence > promotion.PublicationSequence &&
		status.RecoveryEpoch > promotion.RecoveryEpoch && exactSHA256Digest(status.PublishedBundleDigest) {
		promotion.PublicationSequence = status.CurrentPublicationSequence
		promotion.RecoveryEpoch = status.RecoveryEpoch
	}
	keyID, secret, err := activator.activeKey(activator.now().UTC())
	if err != nil {
		return edgecontrol.GroupRecoveryReceipt{}, err
	}
	defer zeroSecret(secret)
	now := activator.now().UTC().Truncate(time.Second)
	request := edgecontrol.GroupRecoveryRequest{Schema: edgecontrol.GroupRecoveryRequestSchemaV1, KeyID: keyID, GroupID: promotion.GroupID,
		ExpectedPublicationSequence: promotion.PublicationSequence, ExpectedRecoveryEpoch: promotion.RecoveryEpoch,
		TargetBundleGeneration: targetGeneration, IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(authorityRequestTTL).Unix(),
		Reason: "rollback uncommitted Guardian group authority"}
	request.Nonce, err = randomAuthorityNonce()
	if err != nil {
		return edgecontrol.GroupRecoveryReceipt{}, err
	}
	if err := edgecontrol.SignGroupRecoveryRequest(&request, secret); err != nil {
		return edgecontrol.GroupRecoveryReceipt{}, err
	}
	var receipt edgecontrol.GroupRecoveryReceipt
	if err := activator.post(ctx, edgecontrol.GroupRecoveryPathV1, request, &receipt); err != nil {
		if errors.Is(err, errAuthorityMutationUnknown) {
			return activator.reconcileRecoveryReceipt(ctx, promotion, targetGeneration)
		}
		return edgecontrol.GroupRecoveryReceipt{}, err
	}
	if receipt.Schema != edgecontrol.GroupRecoveryReceiptSchemaV1 || receipt.GroupID != promotion.GroupID ||
		receipt.PublicationSequence <= promotion.PublicationSequence || receipt.RecoveryEpoch != promotion.RecoveryEpoch+1 ||
		receipt.BundleGeneration != targetGeneration || !exactSHA256Digest(receipt.PublishedBundleDigest) ||
		receipt.Authority != "edge-control" || !receipt.PublicationEnabled {
		return edgecontrol.GroupRecoveryReceipt{}, errors.New("Edge Control recovery receipt is invalid")
	}
	return receipt, nil
}

func (activator *groupAuthorityActivator) reconcileRecovery(ctx context.Context, promotion edgecontrol.GroupPromotionReceipt, targetGeneration string) error {
	_, err := activator.reconcileRecoveryReceipt(ctx, promotion, targetGeneration)
	return err
}

func (activator *groupAuthorityActivator) reconcileRecoveryReceipt(ctx context.Context, promotion edgecontrol.GroupPromotionReceipt, targetGeneration string) (edgecontrol.GroupRecoveryReceipt, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(activator.config.Endpoint, "/")+
		edgecontrol.AuthorityGroupReadyPrefixV1+url.PathEscape(promotion.GroupID)+"/readyz", nil)
	if err != nil {
		return edgecontrol.GroupRecoveryReceipt{}, err
	}
	response, err := activator.client.Do(request)
	if err != nil {
		return edgecontrol.GroupRecoveryReceipt{}, errAuthorityMutationUnknown
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusServiceUnavailable {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		return edgecontrol.GroupRecoveryReceipt{}, errAuthorityMutationUnknown
	}
	var status edgecontrol.AuthorityGroupStatus
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&status) != nil || !decodeEOF(decoder) || status.GroupID != promotion.GroupID ||
		status.CurrentPublicationSequence <= promotion.PublicationSequence || status.RecoveryEpoch <= promotion.RecoveryEpoch ||
		status.BundleGeneration != targetGeneration || !exactSHA256Digest(status.PublishedBundleDigest) ||
		status.BundleValidUntil == nil || !status.BundleValidUntil.After(activator.now().UTC()) {
		return edgecontrol.GroupRecoveryReceipt{}, errAuthorityMutationUnknown
	}
	return edgecontrol.GroupRecoveryReceipt{Schema: edgecontrol.GroupRecoveryReceiptSchemaV1, GroupID: status.GroupID,
		PublicationSequence: status.CurrentPublicationSequence, RecoveryEpoch: status.RecoveryEpoch,
		BundleGeneration: status.BundleGeneration, PublishedBundleDigest: status.PublishedBundleDigest,
		Authority: "edge-control", PublicationEnabled: true}, nil
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
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		if response.StatusCode == http.StatusConflict && (bytes.Contains(raw, []byte(`"error":"sequence_conflict"`)) || bytes.Contains(raw, []byte(`"error":"candidate_conflict"`))) {
			return errAuthorityPrewriteCASChanged
		}
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

func splitPromotedBundleVersion(version string) (string, uint64, uint64, error) {
	separator := strings.LastIndex(strings.TrimSpace(version), ".p")
	if separator < 1 {
		return "", 0, 0, errors.New("promoted bundle version is invalid")
	}
	generation := strings.TrimSpace(version[:separator])
	sequence, recovery, err := parseAuthorityBundleVersion(generation, strings.TrimSpace(version))
	if err != nil {
		return "", 0, 0, err
	}
	return generation, sequence, recovery, nil
}

func zeroSecret(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func decodeEOF(decoder *json.Decoder) bool {
	return errors.Is(decoder.Decode(&struct{}{}), io.EOF)
}
