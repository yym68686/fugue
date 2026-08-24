package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

const (
	edgeActivationStateSchema          = "edge-front-group-activation/v1"
	edgeActivationReceiptSchema        = "edge-front-group-activation-receipt/v1"
	edgeActivationAuthority            = "edge-control"
	edgeActivationInitialize           = "initialize"
	edgeActivationPromote              = "promote"
	edgeActivationRollback             = "rollback"
	edgeGroupAuthoritySource           = "edge-control-group-authority/v1"
	edgeCandidateStagePath             = "/v1/authority/group-worker-candidates"
	edgeGroupRecoveryPath              = "/v1/recovery/group-publications"
	edgeCandidateRecoveryPath          = "/v1/recovery/group-worker-candidates"
	edgeCandidateStageSchema           = "edge-control-group-worker-candidate-request/v1"
	edgeCandidateReceiptSchema         = "edge-control-group-worker-candidate-receipt/v1"
	edgeGroupRecoverySchema            = "edge-control-group-recovery-request/v1"
	edgeGroupRecoveryReceiptSchema     = "edge-control-group-recovery-receipt/v1"
	edgeCandidateRecoverySchema        = "edge-control-group-candidate-recovery-request/v1"
	edgeCandidateRecoveryReceiptSchema = "edge-control-group-candidate-recovery-receipt/v1"
	edgeCandidateStageAttempts         = 4
	edgeCandidateStageRetryBase        = 200 * time.Millisecond
	edgeGroupRecoveryHTTPTimeout       = 60 * time.Second
)

var (
	edgeServingAuthorityTokenPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{1,255}$`)
	errEdgeCandidateStageSequenceConflict = errors.New("stage edge Worker candidate: HTTP 409 (sequence_conflict)")
	errEdgeCandidateStageTransient        = errors.New("stage edge Worker candidate: transient transport failure")
)

type edgeServingAuthorityWitness struct {
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

type edgeCandidateStageRequest struct {
	Schema                        string                       `json:"schema"`
	KeyID                         string                       `json:"key_id"`
	GroupID                       string                       `json:"edge_group_id"`
	ExpectedAuthoritySequence     uint64                       `json:"expected_authority_sequence"`
	ExpectedPublicationSequence   uint64                       `json:"expected_publication_sequence"`
	ExpectedRecoveryEpoch         uint64                       `json:"expected_recovery_epoch"`
	ExpectedPublishedBundleDigest string                       `json:"expected_published_bundle_digest"`
	ExpectedCandidateEpoch        uint64                       `json:"expected_candidate_epoch"`
	ExpectedCurrentWorkerSlot     string                       `json:"expected_current_worker_slot"`
	TargetWorkerSlot              string                       `json:"target_worker_slot"`
	ServingAuthority              *edgeServingAuthorityWitness `json:"serving_authority,omitempty"`
	AllowDegradedPrevious         bool                         `json:"allow_degraded_previous,omitempty"`
	StandbyOnly                   bool                         `json:"standby_only,omitempty"`
	WorkerSourceSHA               string                       `json:"worker_source_sha"`
	WorkerImageDigest             string                       `json:"worker_image_digest"`
	ReleaseRecordDigest           string                       `json:"release_record_digest"`
	IssuedAtUnix                  int64                        `json:"issued_at_unix"`
	ExpiresAtUnix                 int64                        `json:"expires_at_unix"`
	Nonce                         string                       `json:"nonce"`
	Reason                        string                       `json:"reason"`
	Signature                     string                       `json:"signature"`
}

type edgeCandidateStageReceipt struct {
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

type edgeControlError struct {
	Schema string `json:"schema"`
	Error  string `json:"error"`
}

type edgeCandidateStageStatus struct {
	GroupID                    string `json:"edge_group_id"`
	AuthoritySequence          uint64 `json:"authority_sequence"`
	CurrentPublicationSequence uint64 `json:"current_publication_sequence"`
	CandidateEpoch             uint64 `json:"candidate_epoch"`
	CandidateWorkerSourceSHA   string `json:"candidate_worker_source_sha"`
	PublishedBundleDigest      string `json:"published_bundle_digest"`
	BundleGeneration           string `json:"bundle_generation"`
	RecoveryEpoch              uint64 `json:"recovery_epoch"`
}

type edgeGroupRecoveryRequest struct {
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

type edgeGroupRecoveryReceipt struct {
	Schema                string `json:"schema"`
	GroupID               string `json:"edge_group_id"`
	PublicationSequence   uint64 `json:"publication_sequence"`
	RecoveryEpoch         uint64 `json:"recovery_epoch"`
	BundleGeneration      string `json:"bundle_generation"`
	PublishedBundleDigest string `json:"published_bundle_digest"`
	Authority             string `json:"authority"`
	PublicationEnabled    bool   `json:"publication_enabled"`
}

type edgeCandidateRecoveryRequest struct {
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

type edgeCandidateRecoveryReceipt struct {
	Schema                     string `json:"schema"`
	GroupID                    string `json:"edge_group_id"`
	FencedCandidateEpoch       uint64 `json:"fenced_candidate_epoch"`
	FencedWorkerSourceSHA      string `json:"fenced_worker_source_sha"`
	CurrentPublicationSequence uint64 `json:"current_publication_sequence"`
	CurrentRecoveryEpoch       uint64 `json:"current_recovery_epoch"`
	PublishedBundleDigest      string `json:"published_bundle_digest"`
	CandidateCleared           bool   `json:"candidate_cleared"`
}

type edgeCandidateKeyring struct {
	Schema     string             `json:"schema"`
	Generation uint64             `json:"generation"`
	GroupID    string             `json:"edge_group_id"`
	Keys       []edgeCandidateKey `json:"keys"`
}

type edgeCandidateKey struct {
	KeyID         string `json:"key_id"`
	Secret        string `json:"secret"`
	NotBeforeUnix int64  `json:"not_before_unix"`
	NotAfterUnix  int64  `json:"not_after_unix"`
	Revoked       bool   `json:"revoked"`
}

type edgeActivationRequest struct {
	GroupID              string
	ExpectedGeneration   uint64
	ExpectedSlot         string
	TargetSlot           string
	BundleGeneration     string
	WorkerSourceCommit   string
	WorkerImageDigest    string
	Operation            string
	RollbackOfGeneration uint64
	Reason               string
}

type edgeActivationState struct {
	Schema               string    `json:"schema"`
	GroupID              string    `json:"edge_group_id"`
	Generation           uint64    `json:"generation"`
	ActiveSlot           string    `json:"active_slot"`
	PreviousSlot         string    `json:"previous_slot,omitempty"`
	BundleGeneration     string    `json:"bundle_generation"`
	WorkerSourceCommit   string    `json:"worker_source_commit"`
	WorkerImageDigest    string    `json:"worker_image_digest"`
	Authority            string    `json:"authority"`
	Operation            string    `json:"operation"`
	RollbackOfGeneration uint64    `json:"rollback_of_generation,omitempty"`
	Reason               string    `json:"reason"`
	UpdatedAt            time.Time `json:"updated_at"`
}

type edgeActivationReceipt struct {
	Schema         string               `json:"schema"`
	GroupID        string               `json:"edge_group_id"`
	PreviousExists bool                 `json:"previous_exists"`
	Previous       *edgeActivationState `json:"previous,omitempty"`
	Current        edgeActivationState  `json:"current"`
	StateDigest    string               `json:"state_digest"`
}

type edgeGroupPod struct {
	Name                         string
	UID                          string
	ResourceVersion              string
	NodeName                     string
	PodIP                        string
	HealthPort                   int
	SourceCommit                 string
	ImageRef                     string
	ImageID                      string
	RestartCount                 int64
	BundleGeneration             string
	RouteBundleSource            string
	PublicationSequence          uint64
	ServingGeneration            string
	InventoryProducerActive      bool
	InventoryHeartbeatGeneration uint64
	InventoryHeartbeatAt         time.Time
	InventoryHeartbeatError      string
	LegacyIdentity               bool
	Ready                        bool
}

type edgeWorkerHealth struct {
	BundleGeneration             string
	RouteBundleSource            string
	PublicationSequence          uint64
	ServingGeneration            string
	InventoryProducerActive      bool
	InventoryHeartbeatGeneration uint64
	InventoryHeartbeatAt         time.Time
	InventoryHeartbeatError      string
}

type edgeFrontHealth struct {
	ActiveSlot         string
	ActivationPresent  bool
	Generation         uint64
	BundleGeneration   string
	WorkerSourceCommit string
	WorkerImageDigest  string
	RouteAuthority     string
}

type edgeGroupState struct {
	Front           map[string]edgeGroupPod
	FrontHealth     map[string]edgeFrontHealth
	FrontActivation *edgeActivationState
	WorkerA         map[string]edgeGroupPod
	WorkerB         map[string]edgeGroupPod
	ActiveSlot      string
}

type edgeGroupTransitionRuntime interface {
	Snapshot(context.Context) (edgeGroupState, error)
	ApplyCandidateResources(context.Context, string) error
	StageCandidate(context.Context, edgeGroupState, string, declarativerelease.TargetIdentity) (edgeCandidateStageReceipt, error)
	StageStandby(context.Context, edgeGroupState, string, declarativerelease.TargetIdentity) (edgeCandidateStageReceipt, error)
	DeclaredTarget(string) (declarativerelease.TargetIdentity, error)
	Roll(context.Context, string, declarativerelease.TargetIdentity, bool, bool) (map[string]edgeGroupPod, error)
	SelectCASExecutor(context.Context, ...edgeGroupPod) (edgeGroupPod, error)
	ReadActivation(context.Context, edgeGroupPod) (edgeActivationState, bool, error)
	WaitFront(context.Context, string, string, string) (map[string]edgeFrontHealth, error)
	WaitActiveWorkerAuthority(context.Context, string, declarativerelease.TargetIdentity) error
	ActivationCAS(context.Context, edgeGroupPod, edgeActivationRequest) (edgeActivationReceipt, error)
}

type kubectlEdgeGroupRuntime struct {
	cluster    *kubectlCluster
	client     dynamic.Interface
	release    declarativerelease.PlanRelease
	transition declarativerelease.EdgeGroupABTransition
	manifest   []byte
}

func (cluster *kubectlCluster) applyEdgeGroupAB(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte) error {
	if release.Transition == nil || release.Transition.EdgeGroupAB == nil || !target.Present {
		return errors.New("edge group transition is not fully bound")
	}
	transition := *release.Transition.EdgeGroupAB
	config, err := loadComponentLeaseClientConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes client config: %w", err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes dynamic client: %w", err)
	}
	runtime := &kubectlEdgeGroupRuntime{cluster: cluster, client: client, release: release, transition: transition, manifest: manifest}
	return executeEdgeGroupAB(ctx, runtime, release, transition, target)
}

func declaredEdgeDaemonSetTarget(manifest []byte, release declarativerelease.PlanRelease, name, container string) (declarativerelease.TargetIdentity, error) {
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: release.Workload.Namespace, Name: name}
	desired, err := declarativerelease.ResourceSetItem(manifest, identity)
	if err != nil {
		return declarativerelease.TargetIdentity{}, err
	}
	workload, err := workloadFromDeclaredResource(desired, identity, container, release.Workload.FieldManager)
	if err != nil {
		return declarativerelease.TargetIdentity{}, err
	}
	return targetIdentityFromDeclaredWorkload(desired, workload)
}

func executeEdgeGroupAB(ctx context.Context, runtime edgeGroupTransitionRuntime, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, target declarativerelease.TargetIdentity) error {
	before, err := runtime.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("capture edge group prewrite state: %w", err)
	}
	activeSlot := before.ActiveSlot
	inactiveSlot := otherEdgeSlot(activeSlot)
	inactiveName := edgeWorkerName(transition, inactiveSlot)
	desiredDigest, err := immutableDigestFromRef(target.ImageRef)
	if err != nil {
		return err
	}
	var compensationCandidates []edgeGroupPod
	compensate := func(cause error) error {
		if compensationErr := compensateEdgeActivation(ctx, runtime, before, transition, compensationCandidates...); compensationErr != nil {
			return errors.Join(cause, fmt.Errorf("edge activation compensation is unknown: %w", compensationErr))
		}
		return errors.Join(cause, errors.New("edge activation compensated"))
	}

	if target.ConfigSHA == release.ExpectedPreviousConfigSHA {
		if err := runtime.ApplyCandidateResources(ctx, ""); err != nil {
			return err
		}
		for _, name := range []string{inactiveName, edgeWorkerName(transition, activeSlot), transition.FrontName} {
			declared, targetErr := runtime.DeclaredTarget(name)
			if targetErr != nil {
				return targetErr
			}
			if _, err := runtime.Roll(ctx, name, declared, name != transition.FrontName, true); err != nil {
				return fmt.Errorf("restore exact edge LKG workload %s: %w", name, err)
			}
		}
		return nil
	}
	stage, err := runtime.StageCandidate(ctx, before, inactiveSlot, target)
	if err != nil {
		return fmt.Errorf("stage inactive Worker candidate: %w", err)
	}
	if stage.WorkerSlot != inactiveSlot || stage.CurrentWorkerSlot != activeSlot || stage.WorkerSourceSHA != target.ConfigSHA ||
		stage.WorkerImageDigest != desiredDigest || stage.AllowDegradedPrevious != (release.SupersedesFailedConfigSHA != "") || stage.StandbyOnly || stage.OrdinaryTrafficMutation {
		return errors.New("inactive Worker candidate receipt is invalid")
	}
	if err := runtime.ApplyCandidateResources(ctx, inactiveSlot); err != nil {
		return compensate(err)
	}
	candidatePods, err := runtime.Roll(ctx, inactiveName, target, true, release.SupersedesFailedConfigSHA != "")
	if err != nil {
		return compensate(fmt.Errorf("roll inactive edge slot %s: %w", inactiveSlot, err))
	}
	for _, pod := range candidatePods {
		compensationCandidates = append(compensationCandidates, pod)
	}
	// A failed successor can leave the serving slot on an older worker binary
	// that rejects the renewed publication version for its exact LKG bundle.
	// Restore code execution on that slot before changing traffic authority;
	// the activation pointer and route artifact remain the existing LKG.
	if release.SupersedesFailedConfigSHA != "" && edgeActiveWorkerNeedsCodeRecovery(before, target) {
		if err := runtime.ApplyCandidateResources(ctx, activeSlot); err != nil {
			return compensate(fmt.Errorf("apply active Worker recovery code: %w", err))
		}
		if _, err := runtime.Roll(ctx, edgeWorkerName(transition, activeSlot), target, true, true); err != nil {
			return compensate(fmt.Errorf("roll active Worker recovery code: %w", err))
		}
	}
	if err := promoteEdgeActivation(ctx, runtime, before, transition, inactiveSlot, candidatePods, target, desiredDigest); err != nil {
		return compensate(fmt.Errorf("promote edge activation after candidate health: %w", err))
	}
	frontHealth, err := runtime.WaitFront(ctx, inactiveSlot, target.ConfigSHA, desiredDigest)
	if err != nil {
		return compensate(fmt.Errorf("wait Guardian authority switch: %w", err))
	}
	if err := runtime.ApplyCandidateResources(ctx, transition.FrontName); err != nil {
		return compensate(fmt.Errorf("apply Front candidate after Guardian authority switch: %w", err))
	}
	// A failed successor may leave the old Front process running but unready
	// after compensation.  In that explicitly superseding flow, allow the
	// rollout helper to use immutable snapshot identity and replace that pod;
	// ordinary promotions retain the strict ready-only read.
	frontPods, err := runtime.Roll(ctx, transition.FrontName, target, true, release.SupersedesFailedConfigSHA != "")
	if err != nil {
		return compensate(fmt.Errorf("roll edge front after Guardian authority switch: %w", err))
	}
	if err := runtime.WaitActiveWorkerAuthority(ctx, inactiveName, target); err != nil {
		return compensate(fmt.Errorf("verify active edge worker authority: %w", err))
	}
	previousName := edgeWorkerName(transition, activeSlot)
	previousTarget, err := runtime.DeclaredTarget(previousName)
	if err != nil {
		return err
	}
	serving := edgeGroupState{Front: frontPods, FrontHealth: frontHealth, ActiveSlot: inactiveSlot}
	if inactiveSlot == "a" {
		serving.WorkerA, serving.WorkerB = candidatePods, before.WorkerB
	} else {
		serving.WorkerA, serving.WorkerB = before.WorkerA, candidatePods
	}
	// Front and the active Worker are already committed to the new authority at
	// this point.  Standby preparation is maintenance-only: a failed sequence,
	// receipt, or inactive-slot roll must not send the generic executor down its
	// workload LKG rollback path and split traffic authority from serving code.
	standby, standbyErr := runtime.StageStandby(ctx, serving, activeSlot, previousTarget)
	standbyConverged := false
	if standbyErr == nil {
		previousDigest, digestErr := immutableDigestFromRef(previousTarget.ImageRef)
		receiptValid := digestErr == nil && standby.WorkerSlot == activeSlot && standby.CurrentWorkerSlot == inactiveSlot &&
			standby.WorkerSourceSHA == previousTarget.ConfigSHA && standby.WorkerImageDigest == previousDigest &&
			!standby.AllowDegradedPrevious && standby.StandbyOnly && !standby.OrdinaryTrafficMutation
		if receiptValid && runtime.ApplyCandidateResources(ctx, previousName) == nil {
			_, standbyErr = runtime.Roll(ctx, previousName, previousTarget, true, true)
			standbyConverged = standbyErr == nil
		}
	}
	final, err := runtime.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("capture final edge group state: %w", err)
	}
	if !edgePodsMatchTarget(final.Front, target) || !edgePodsMatchTarget(edgeWorkerPods(final, inactiveSlot), target) {
		return errors.New("edge group did not converge candidate current authority")
	}
	if standbyConverged && !edgePodsMatchTarget(edgeWorkerPods(final, activeSlot), previousTarget) {
		return errors.New("edge group did not converge candidate current and previous LKG")
	}
	if standbyConverged {
		if err := validateEdgeGroupAuthority(final, transition); err != nil {
			return err
		}
	} else if err := validateActiveEdgeGroupAuthority(final, transition); err != nil {
		return err
	}
	if final.ActiveSlot != inactiveSlot {
		return errors.New("edge group activation did not converge to the promoted slot")
	}
	return nil
}

func edgeActiveWorkerNeedsCodeRecovery(state edgeGroupState, target declarativerelease.TargetIdentity) bool {
	for _, pod := range edgeWorkerPods(state, state.ActiveSlot) {
		if !pod.Ready || !edgePodHasGroupAuthority(pod) || pod.SourceCommit != target.ConfigSHA || pod.ImageRef != target.ImageRef {
			return true
		}
	}
	return false
}

// promoteEdgeActivation is the only forward traffic-authority mutation in the
// Edge group transition. The candidate Worker has already loaded and exposed
// its signed bundle before this CAS, so the activation record can name the
// exact immutable generation that Front must serve. Groups without an
// activation witness are legacy/bootstrap layouts; they retain their prior
// externally managed handoff until initialized by the control plane.
func promoteEdgeActivation(ctx context.Context, runtime edgeGroupTransitionRuntime, before edgeGroupState, transition declarativerelease.EdgeGroupABTransition, targetSlot string, candidates map[string]edgeGroupPod, target declarativerelease.TargetIdentity, imageDigest string) error {
	if before.FrontActivation == nil {
		return nil
	}
	if before.FrontActivation.GroupID != transition.GroupID || before.FrontActivation.Generation == 0 ||
		before.FrontActivation.ActiveSlot != before.ActiveSlot || before.FrontActivation.BundleGeneration == "" {
		return errors.New("pre-transition activation witness is invalid")
	}
	if targetSlot != "a" && targetSlot != "b" {
		return errors.New("candidate activation target slot is invalid")
	}
	var bundleGeneration string
	for _, node := range sortedEdgeNodes(candidates) {
		pod := candidates[node]
		if !pod.Ready || pod.BundleGeneration == "" || pod.SourceCommit != target.ConfigSHA || pod.ImageRef == "" {
			return errors.New("candidate Worker activation evidence is incomplete")
		}
		if bundleGeneration == "" {
			bundleGeneration = pod.BundleGeneration
		} else if bundleGeneration != pod.BundleGeneration {
			return errors.New("candidate Worker bundle generations disagree")
		}
	}
	if bundleGeneration == "" {
		return errors.New("candidate Worker activation bundle generation is missing")
	}
	executor, err := runtime.SelectCASExecutor(ctx, candidatesInNodeOrder(candidates)...)
	if err != nil {
		return fmt.Errorf("select candidate activation CAS executor: %w", err)
	}
	request := edgeActivationRequest{
		GroupID: transition.GroupID, ExpectedGeneration: before.FrontActivation.Generation,
		ExpectedSlot: before.ActiveSlot, TargetSlot: targetSlot, BundleGeneration: bundleGeneration,
		WorkerSourceCommit: target.ConfigSHA, WorkerImageDigest: imageDigest,
		Operation: edgeActivationPromote, Reason: "promote verified edge group candidate",
	}
	if _, err := runtime.ActivationCAS(ctx, executor, request); err != nil {
		return fmt.Errorf("candidate activation CAS: %w", err)
	}
	return nil
}

func candidatesInNodeOrder(candidates map[string]edgeGroupPod) []edgeGroupPod {
	ordered := make([]edgeGroupPod, 0, len(candidates))
	for _, node := range sortedEdgeNodes(candidates) {
		ordered = append(ordered, candidates[node])
	}
	return ordered
}

// compensateEdgeActivation restores the activation pointer to the exact
// Front evidence captured before the transition. This is intentionally
// limited to failures before the active Worker authority is committed: once
// that boundary is crossed, rolling the pointer back here would split serving
// authority from the already-promoted Worker and the Guardian transaction
// owns compensation instead.
func compensateEdgeActivation(ctx context.Context, runtime edgeGroupTransitionRuntime, before edgeGroupState, transition declarativerelease.EdgeGroupABTransition, extraCandidates ...edgeGroupPod) error {
	if runtime == nil || (before.ActiveSlot != "a" && before.ActiveSlot != "b") || len(before.FrontHealth) == 0 {
		return errors.New("pre-transition activation evidence is unavailable")
	}
	var beforeHealth edgeFrontHealth
	for _, health := range before.FrontHealth {
		beforeHealth = health
		break
	}
	if !beforeHealth.ActivationPresent || beforeHealth.Generation == 0 || beforeHealth.BundleGeneration == "" ||
		beforeHealth.WorkerSourceCommit == "" || beforeHealth.WorkerImageDigest == "" {
		return errors.New("pre-transition activation evidence is incomplete")
	}
	selectorCandidates := make([]edgeGroupPod, 0, len(before.WorkerA)+len(before.WorkerB))
	for _, pods := range []map[string]edgeGroupPod{before.WorkerA, before.WorkerB} {
		for _, pod := range pods {
			selectorCandidates = append(selectorCandidates, pod)
		}
	}
	selectorCandidates = append(selectorCandidates, extraCandidates...)
	executor, err := runtime.SelectCASExecutor(ctx, selectorCandidates...)
	if err != nil {
		return err
	}
	current, exists, err := runtime.ReadActivation(ctx, executor)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("current activation evidence is unavailable")
	}
	if current.GroupID != transition.GroupID || current.Authority != edgeActivationAuthority || current.ActiveSlot == "" {
		return errors.New("current activation identity is invalid")
	}
	if current.Generation == beforeHealth.Generation && current.ActiveSlot == before.ActiveSlot &&
		current.BundleGeneration == beforeHealth.BundleGeneration && current.WorkerSourceCommit == beforeHealth.WorkerSourceCommit &&
		current.WorkerImageDigest == beforeHealth.WorkerImageDigest {
		return nil
	}
	if current.Generation <= beforeHealth.Generation {
		return errors.New("current activation generation is not newer than the pre-transition witness")
	}
	request := edgeActivationRequest{GroupID: transition.GroupID, ExpectedGeneration: current.Generation,
		ExpectedSlot: current.ActiveSlot, TargetSlot: before.ActiveSlot, BundleGeneration: beforeHealth.BundleGeneration,
		WorkerSourceCommit: beforeHealth.WorkerSourceCommit, WorkerImageDigest: beforeHealth.WorkerImageDigest,
		Operation: edgeActivationRollback, RollbackOfGeneration: current.Generation,
		Reason: "compensate pre-commit edge group transition failure"}
	if _, err := runtime.ActivationCAS(ctx, executor, request); err != nil {
		return err
	}
	settled, exists, err := runtime.ReadActivation(ctx, executor)
	if err != nil || !exists || settled.Generation != current.Generation+1 || settled.ActiveSlot != before.ActiveSlot ||
		settled.BundleGeneration != beforeHealth.BundleGeneration || settled.WorkerSourceCommit != beforeHealth.WorkerSourceCommit ||
		settled.WorkerImageDigest != beforeHealth.WorkerImageDigest || settled.Operation != edgeActivationRollback ||
		settled.RollbackOfGeneration != current.Generation {
		if err != nil {
			return errors.Join(errors.New("read compensated activation"), err)
		}
		return errors.New("compensated activation is not request-bound")
	}
	return nil
}

func (runtime *kubectlEdgeGroupRuntime) Snapshot(ctx context.Context) (edgeGroupState, error) {
	return runtime.cluster.readEdgeGroupState(ctx, runtime.release, runtime.transition)
}

func (runtime *kubectlEdgeGroupRuntime) ApplyCandidateResources(ctx context.Context, inactiveSlot string) error {
	return runtime.cluster.applyEdgeCandidateResources(ctx, runtime.release, runtime.transition, runtime.manifest, inactiveSlot)
}

func (runtime *kubectlEdgeGroupRuntime) DeclaredTarget(name string) (declarativerelease.TargetIdentity, error) {
	container := runtime.transition.WorkerContainer
	if name == runtime.transition.FrontName {
		container = "edge-front"
	}
	return declaredEdgeDaemonSetTarget(runtime.manifest, runtime.release, name, container)
}

func (runtime *kubectlEdgeGroupRuntime) StageCandidate(ctx context.Context, before edgeGroupState, inactiveSlot string, target declarativerelease.TargetIdentity) (edgeCandidateStageReceipt, error) {
	return runtime.stageCandidate(ctx, before, inactiveSlot, target, false)
}

func (runtime *kubectlEdgeGroupRuntime) StageStandby(ctx context.Context, before edgeGroupState, inactiveSlot string, target declarativerelease.TargetIdentity) (edgeCandidateStageReceipt, error) {
	return runtime.stageCandidate(ctx, before, inactiveSlot, target, true)
}

func (runtime *kubectlEdgeGroupRuntime) stageCandidate(ctx context.Context, before edgeGroupState, inactiveSlot string, target declarativerelease.TargetIdentity, standbyOnly bool) (edgeCandidateStageReceipt, error) {
	var lastErr error
	recoveryAttempts := 0
	for attempt := 0; attempt < edgeCandidateStageAttempts; attempt++ {
		// A failed Guardian atom can leave the exact target candidate durable
		// even though Front still serves the previous activation. Reusing that
		// candidate makes the next import carry an expired/stale current bundle
		// and prevents the sequence-conflict recovery branch from running. Clear
		// the candidate and renew the exact Front publication before the first
		// retry, using the same signed CAS pair as the conflict path below.
		if !standbyOnly && before.FrontActivation != nil && recoveryAttempts < 2 && runtime.release.SupersedesFailedConfigSHA != "" {
			status, statusErr := readEdgeCandidateStageStatus(ctx, runtime.transition.CandidateStageURL, runtime.transition.GroupID)
			if statusErr != nil {
				return edgeCandidateStageReceipt{}, fmt.Errorf("read Edge Control status before failed candidate recovery: %w", statusErr)
			}
			servingLKGBundle, servingLKG, servingErr := runtime.servingLKGRecoveryTarget(ctx, before, status)
			if servingErr != nil {
				return edgeCandidateStageReceipt{}, fmt.Errorf("read exact Front LKG recovery witness before candidate staging: %w", servingErr)
			}
			if servingLKG && status.CandidateEpoch != 0 && status.CandidateWorkerSourceSHA != "" {
				if fenceErr := runtime.fenceFailedCandidate(ctx, status); fenceErr != nil {
					return edgeCandidateStageReceipt{}, fmt.Errorf("fence failed Edge Control candidate before candidate staging: %w", fenceErr)
				}
				if recoveryErr := runtime.recoverPublishedLKG(ctx, status, servingLKGBundle, "restore exact Front activation LKG before failed candidate retry"); recoveryErr != nil {
					return edgeCandidateStageReceipt{}, fmt.Errorf("restore exact Front activation LKG before candidate staging: %w", recoveryErr)
				}
				recoveryAttempts++
				continue
			}
		}
		receipt, err := runtime.stageCandidateOnce(ctx, before, inactiveSlot, target, standbyOnly)
		if err == nil {
			return receipt, nil
		}
		if !errors.Is(err, errEdgeCandidateStageSequenceConflict) && !errors.Is(err, errEdgeCandidateStageTransient) {
			return edgeCandidateStageReceipt{}, err
		}
		lastErr = err
		if errors.Is(err, errEdgeCandidateStageSequenceConflict) && recoveryAttempts < 2 && !standbyOnly && runtime.release.SupersedesFailedConfigSHA != "" {
			status, statusErr := readEdgeCandidateStageStatus(ctx, runtime.transition.CandidateStageURL, runtime.transition.GroupID)
			if statusErr != nil {
				return edgeCandidateStageReceipt{}, errors.Join(lastErr, fmt.Errorf("read Edge Control status before published LKG recovery: %w", statusErr))
			}
			servingLKGBundle, servingLKG, servingErr := runtime.servingLKGRecoveryTarget(ctx, before, status)
			if servingErr != nil {
				return edgeCandidateStageReceipt{}, errors.Join(lastErr, fmt.Errorf("read exact Front LKG recovery witness after candidate sequence conflict: %w", servingErr))
			}
			if servingLKG {
				// The durable candidate is fenced before the exact publication
				// named by Front activation is restored. Both writes remain
				// signed and CAS-bound to the same Edge Control status snapshot.
				if fenceErr := runtime.fenceFailedCandidate(ctx, status); fenceErr != nil {
					return edgeCandidateStageReceipt{}, errors.Join(lastErr, fmt.Errorf("fence failed Edge Control candidate before Front LKG recovery: %w", fenceErr))
				}
				if recoveryErr := runtime.recoverPublishedLKG(ctx, status, servingLKGBundle, "restore exact Front activation LKG before candidate retry"); recoveryErr != nil {
					return edgeCandidateStageReceipt{}, errors.Join(lastErr, fmt.Errorf("restore exact Front activation LKG after candidate sequence conflict: %w", recoveryErr))
				}
			} else if status.CandidateEpoch != 0 && status.CandidateWorkerSourceSHA != "" && status.CandidateWorkerSourceSHA != target.ConfigSHA {
				if fenceErr := runtime.fenceFailedCandidate(ctx, status); fenceErr != nil {
					return edgeCandidateStageReceipt{}, errors.Join(lastErr, fmt.Errorf("fence failed Edge Control candidate after sequence conflict: %w", fenceErr))
				}
				if recoveryErr := runtime.refreshPublishedLKG(ctx, status); recoveryErr != nil {
					return edgeCandidateStageReceipt{}, errors.Join(lastErr, fmt.Errorf("refresh published Edge Control LKG after candidate sequence conflict: %w", recoveryErr))
				}
			} else if recoveryErr := runtime.refreshPublishedLKG(ctx, status); recoveryErr != nil {
				return edgeCandidateStageReceipt{}, errors.Join(lastErr, fmt.Errorf("refresh published Edge Control LKG after candidate sequence conflict: %w", recoveryErr))
			}
			recoveryAttempts++
		}
		if attempt+1 == edgeCandidateStageAttempts {
			break
		}
		timer := time.NewTimer(edgeCandidateStageRetryBase * time.Duration(attempt+1))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return edgeCandidateStageReceipt{}, errors.Join(lastErr, ctx.Err())
		case <-timer.C:
		}
	}
	return edgeCandidateStageReceipt{}, lastErr
}

func (runtime *kubectlEdgeGroupRuntime) fenceFailedCandidate(ctx context.Context, status edgeCandidateStageStatus) error {
	endpoint, err := edgeCandidateRecoveryURL(runtime.transition.CandidateStageURL)
	if err != nil {
		return err
	}
	nonceRaw := make([]byte, 24)
	if _, err := rand.Read(nonceRaw); err != nil {
		return errors.New("generate Edge Control candidate recovery nonce")
	}
	now := time.Now().UTC().Truncate(time.Second)
	request := edgeCandidateRecoveryRequest{
		Schema: edgeCandidateRecoverySchema, GroupID: runtime.transition.GroupID,
		ExpectedAuthoritySequence: status.AuthoritySequence, ExpectedPublicationSequence: status.CurrentPublicationSequence,
		ExpectedRecoveryEpoch: status.RecoveryEpoch, ExpectedPublishedBundleDigest: status.PublishedBundleDigest,
		ExpectedCandidateEpoch: status.CandidateEpoch, ExpectedWorkerSourceSHA: status.CandidateWorkerSourceSHA,
		IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(2 * time.Minute).Unix(),
		Nonce: base64.RawURLEncoding.EncodeToString(nonceRaw), Reason: "fence failed Worker candidate before controlled LKG recovery",
	}
	if err := signEdgeCandidateRecoveryRequest(runtime.transition.CandidateKeyring, &request, now); err != nil {
		return err
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 15 * time.Second}).Do(httpRequest)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil {
		return err
	}
	if response.StatusCode == http.StatusConflict {
		latest, latestErr := readEdgeCandidateStageStatus(ctx, runtime.transition.CandidateStageURL, runtime.transition.GroupID)
		if latestErr == nil && latest.CandidateEpoch == 0 {
			return nil
		}
		return errors.New("Edge Control candidate recovery CAS conflict")
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Edge Control candidate recovery HTTP %d", response.StatusCode)
	}
	var receipt edgeCandidateRecoveryReceipt
	if err := decodeEdgeCandidateStageResponse(body, &receipt); err != nil {
		return fmt.Errorf("decode Edge Control candidate recovery receipt: %w", err)
	}
	if receipt.Schema != edgeCandidateRecoveryReceiptSchema || receipt.GroupID != status.GroupID ||
		receipt.FencedCandidateEpoch != status.CandidateEpoch || receipt.FencedWorkerSourceSHA != status.CandidateWorkerSourceSHA ||
		receipt.CurrentPublicationSequence != status.CurrentPublicationSequence || receipt.CurrentRecoveryEpoch != status.RecoveryEpoch ||
		receipt.PublishedBundleDigest != status.PublishedBundleDigest || !receipt.CandidateCleared {
		return errors.New("Edge Control candidate recovery receipt is not bound to the failed candidate")
	}
	return nil
}

func (runtime *kubectlEdgeGroupRuntime) refreshPublishedLKG(ctx context.Context, status edgeCandidateStageStatus) error {
	return runtime.recoverPublishedLKG(ctx, status, status.BundleGeneration, "refresh exact published LKG before degraded Worker candidate staging")
}

func (runtime *kubectlEdgeGroupRuntime) recoverPublishedLKG(ctx context.Context, status edgeCandidateStageStatus, targetGeneration, reason string) error {
	if status.GroupID != runtime.transition.GroupID || status.CurrentPublicationSequence == 0 || strings.TrimSpace(targetGeneration) == "" || status.PublishedBundleDigest == "" || strings.TrimSpace(reason) == "" {
		return errors.New("Edge Control published LKG status is incomplete")
	}
	endpoint, err := edgeGroupRecoveryURL(runtime.transition.CandidateStageURL)
	if err != nil {
		return err
	}
	nonceRaw := make([]byte, 24)
	if _, err := rand.Read(nonceRaw); err != nil {
		return errors.New("generate Edge Control recovery nonce")
	}
	now := time.Now().UTC().Truncate(time.Second)
	request := edgeGroupRecoveryRequest{
		Schema: edgeGroupRecoverySchema, GroupID: runtime.transition.GroupID,
		ExpectedPublicationSequence: status.CurrentPublicationSequence, ExpectedRecoveryEpoch: status.RecoveryEpoch,
		TargetBundleGeneration: targetGeneration, IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(2 * time.Minute).Unix(),
		Nonce: base64.RawURLEncoding.EncodeToString(nonceRaw), Reason: reason,
	}
	if err := signEdgeGroupRecoveryRequest(runtime.transition.CandidateKeyring, &request, now); err != nil {
		return err
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: edgeGroupRecoveryHTTPTimeout}).Do(httpRequest)
	if err != nil {
		if runtime.publishedLKGRefreshCommitted(ctx, status) {
			return nil
		}
		return err
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	response.Body.Close()
	if err != nil {
		if runtime.publishedLKGRefreshCommitted(ctx, status) {
			return nil
		}
		return err
	}
	if response.StatusCode == http.StatusConflict {
		if runtime.publishedLKGRefreshCommitted(ctx, status) {
			return nil
		}
		return errors.New("Edge Control recovery CAS conflict")
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Edge Control recovery HTTP %d", response.StatusCode)
	}
	var receipt edgeGroupRecoveryReceipt
	if err := decodeEdgeCandidateStageResponse(body, &receipt); err != nil {
		return fmt.Errorf("decode Edge Control recovery receipt: %w", err)
	}
	targetBase, _, _, targetVersionOK := parseEdgePublicationVersion(targetGeneration)
	receiptGeneration := targetGeneration
	if targetVersionOK {
		receiptGeneration = targetBase
	}
	if receipt.Schema != edgeGroupRecoveryReceiptSchema || receipt.GroupID != status.GroupID || receipt.BundleGeneration != receiptGeneration ||
		receipt.PublicationSequence <= status.CurrentPublicationSequence || receipt.RecoveryEpoch != status.RecoveryEpoch+1 ||
		receipt.PublishedBundleDigest == "" || receipt.Authority != edgeActivationAuthority || !receipt.PublicationEnabled {
		return errors.New("Edge Control recovery receipt is not bound to the published LKG")
	}
	return nil
}

// servingLKGRecoveryTarget returns the exact publication version named by the
// immutable Front activation when CurrentAuthority has advanced to a failed
// candidate. A base bundle generation is insufficient here because multiple
// authority publications intentionally share it.
func (runtime *kubectlEdgeGroupRuntime) servingLKGRecoveryTarget(ctx context.Context, before edgeGroupState, status edgeCandidateStageStatus) (string, bool, error) {
	if runtime == nil || runtime.client == nil || before.FrontActivation == nil ||
		before.ActiveSlot != "a" && before.ActiveSlot != "b" ||
		runtime.release.ExpectedPreviousConfigSHA == "" || runtime.release.ExpectedPreviousImageDigest == "" ||
		status.CandidateEpoch == 0 || status.CandidateWorkerSourceSHA == "" {
		return "", false, nil
	}
	current, object, err := runtime.readCurrentAuthority(ctx)
	if err != nil {
		return "", false, err
	}
	if object == nil {
		return "", false, nil
	}
	if current.Validate() != nil || current.GroupID != runtime.transition.GroupID {
		return "", false, errors.New("Guardian current authority recovery witness is invalid")
	}
	if current.CurrentWorkerSlot == current.PreviousWorkerSlot || current.PreviousWorkerSlot.Validate() != nil ||
		string(current.PreviousWorkerSlot) != before.ActiveSlot || current.PreviousBundleGeneration != before.FrontActivation.BundleGeneration ||
		current.PreviousWorkerSourceSHA != runtime.release.ExpectedPreviousConfigSHA || current.PreviousWorkerImageDigest != runtime.release.ExpectedPreviousImageDigest {
		return "", false, nil
	}
	targetBase, targetSequence, targetRecovery, targetVersionOK := parseEdgePublicationVersion(before.FrontActivation.BundleGeneration)
	if !targetVersionOK {
		return "", false, errors.New("Front activation LKG publication version is invalid")
	}
	if status.BundleGeneration == targetBase {
		// Edge Control already points at a newer publication of the exact
		// immutable LKG family. Re-signing it again only extends the durable
		// ledger write and can exhaust the Guardian recovery deadline.
		return "", false, nil
	}
	if status.CurrentPublicationSequence < targetSequence ||
		(status.CurrentPublicationSequence == targetSequence && status.RecoveryEpoch <= targetRecovery) {
		return "", false, nil
	}
	return before.FrontActivation.BundleGeneration, true, nil
}

func (runtime *kubectlEdgeGroupRuntime) publishedLKGRefreshCommitted(ctx context.Context, previous edgeCandidateStageStatus) bool {
	latest, err := readEdgeCandidateStageStatus(ctx, runtime.transition.CandidateStageURL, runtime.transition.GroupID)
	return err == nil && latest.BundleGeneration == previous.BundleGeneration &&
		latest.CurrentPublicationSequence > previous.CurrentPublicationSequence && latest.RecoveryEpoch > previous.RecoveryEpoch
}

func (runtime *kubectlEdgeGroupRuntime) stageCandidateOnce(ctx context.Context, before edgeGroupState, inactiveSlot string, target declarativerelease.TargetIdentity, standbyOnly bool) (edgeCandidateStageReceipt, error) {
	status, err := readEdgeCandidateStageStatus(ctx, runtime.transition.CandidateStageURL, runtime.transition.GroupID)
	if err != nil {
		return edgeCandidateStageReceipt{}, err
	}
	if status.AuthoritySequence == 0 || status.CurrentPublicationSequence == 0 || status.PublishedBundleDigest == "" {
		return edgeCandidateStageReceipt{}, errors.New("edge-control candidate staging status is incomplete")
	}
	digest, err := immutableDigestFromRef(target.ImageRef)
	if err != nil {
		return edgeCandidateStageReceipt{}, err
	}
	recordDigest := strings.TrimSpace(os.Getenv("FUGUE_RELEASE_GUARDIAN_RECORD_DIGEST"))
	if !strings.HasPrefix(recordDigest, "sha256:") || len(recordDigest) != 71 {
		return edgeCandidateStageReceipt{}, errors.New("Guardian release record digest is invalid")
	}
	servingAuthority, err := runtime.readServingAuthorityWitness(ctx, before)
	if err != nil {
		return edgeCandidateStageReceipt{}, err
	}
	nonceRaw := make([]byte, 24)
	if _, err := rand.Read(nonceRaw); err != nil {
		return edgeCandidateStageReceipt{}, errors.New("generate candidate staging nonce")
	}
	now := time.Now().UTC().Truncate(time.Second)
	request := edgeCandidateStageRequest{Schema: edgeCandidateStageSchema,
		GroupID: runtime.transition.GroupID, ExpectedAuthoritySequence: status.AuthoritySequence,
		ExpectedPublicationSequence: status.CurrentPublicationSequence, ExpectedRecoveryEpoch: status.RecoveryEpoch,
		ExpectedPublishedBundleDigest: status.PublishedBundleDigest, ExpectedCandidateEpoch: status.CandidateEpoch,
		ExpectedCurrentWorkerSlot: before.ActiveSlot, TargetWorkerSlot: inactiveSlot, ServingAuthority: servingAuthority,
		AllowDegradedPrevious: !standbyOnly && runtime.release.SupersedesFailedConfigSHA != "", StandbyOnly: standbyOnly, WorkerSourceSHA: target.ConfigSHA,
		WorkerImageDigest: digest, ReleaseRecordDigest: recordDigest, IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix(),
		Nonce: base64.RawURLEncoding.EncodeToString(nonceRaw), Reason: "stage immutable Worker candidate for independent route canary"}
	if standbyOnly {
		request.Reason = "restore previous Worker LKG as non-promotable standby"
	}
	if err := signEdgeCandidateStageRequest(runtime.transition.CandidateKeyring, &request, now); err != nil {
		return edgeCandidateStageReceipt{}, err
	}
	return postEdgeCandidateStage(ctx, runtime.transition.CandidateStageURL, request)
}

func (runtime *kubectlEdgeGroupRuntime) readServingAuthorityWitness(ctx context.Context, before edgeGroupState) (*edgeServingAuthorityWitness, error) {
	current, object, err := runtime.readCurrentAuthority(ctx)
	if err != nil || object == nil {
		return nil, err
	}
	return edgeServingAuthorityWitnessFromCurrentWithExpectedLKG(before, current, runtime.transition.GroupID, string(object.GetUID()), object.GetResourceVersion(), runtime.release.SupersedesFailedConfigSHA != "", runtime.release.ExpectedPreviousConfigSHA, runtime.release.ExpectedPreviousImageDigest)
}

func (runtime *kubectlEdgeGroupRuntime) readCurrentAuthority(ctx context.Context) (releaseguardian.CurrentAuthority, *unstructured.Unstructured, error) {
	if runtime == nil || runtime.client == nil {
		return releaseguardian.CurrentAuthority{}, nil, nil
	}
	resource := runtime.client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace(runtime.release.Workload.Namespace)
	object, err := resource.Get(ctx, "fugue-current-authority-"+runtime.transition.GroupID, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return releaseguardian.CurrentAuthority{}, nil, nil
	}
	if err != nil {
		return releaseguardian.CurrentAuthority{}, nil, fmt.Errorf("read Guardian current authority: %w", err)
	}
	data, found, err := unstructured.NestedStringMap(object.Object, "data")
	if err != nil || !found || strings.TrimSpace(data["authority.json"]) == "" {
		return releaseguardian.CurrentAuthority{}, nil, errors.New("Guardian current authority payload is unavailable")
	}
	var current releaseguardian.CurrentAuthority
	if err := decodeStrictJSON([]byte(data["authority.json"]), &current); err != nil {
		return releaseguardian.CurrentAuthority{}, nil, errors.New("Guardian current authority payload is invalid")
	}
	return current, object, nil
}

func edgeServingAuthorityWitnessFromCurrent(before edgeGroupState, current releaseguardian.CurrentAuthority, groupID, uid, resourceVersion string) (*edgeServingAuthorityWitness, error) {
	return edgeServingAuthorityWitnessFromCurrentWithDegradedRecovery(before, current, groupID, uid, resourceVersion, false)
}

func edgeServingAuthorityWitnessFromCurrentWithDegradedRecovery(before edgeGroupState, current releaseguardian.CurrentAuthority, groupID, uid, resourceVersion string, allowDegradedRecovery bool) (*edgeServingAuthorityWitness, error) {
	return edgeServingAuthorityWitnessFromCurrentWithExpectedLKG(before, current, groupID, uid, resourceVersion, allowDegradedRecovery, "", "")
}

// edgeServingAuthorityWitnessFromCurrentWithExpectedLKG admits the narrow
// recovery state left by a committed-but-unserved authority promotion: the
// Front still serves the exact declared LKG while CurrentAuthority points at
// the failed candidate. The LKG source/image and bundle family are explicit
// inputs, so this cannot turn arbitrary Front drift into a serving witness.
func edgeServingAuthorityWitnessFromCurrentWithExpectedLKG(before edgeGroupState, current releaseguardian.CurrentAuthority, groupID, uid, resourceVersion string, allowDegradedRecovery bool, expectedLKGSourceSHA, expectedLKGImageDigest string) (*edgeServingAuthorityWitness, error) {
	if current.Validate() != nil || current.GroupID != groupID {
		return nil, errors.New("Guardian current authority payload is invalid")
	}
	if current.CurrentFrontGeneration == 0 {
		return nil, nil
	}
	if !edgeServingAuthorityTokenPattern.MatchString(uid) || !edgeServingAuthorityTokenPattern.MatchString(resourceVersion) {
		return nil, errors.New("Guardian current authority CAS identity is invalid")
	}
	if before.FrontActivation != nil {
		health := edgeFrontHealthFromActivation(*before.FrontActivation)
		if before.ActiveSlot != health.ActiveSlot {
			return nil, errors.New("edge activation evidence disagrees with serving slot")
		}
		if before.ActiveSlot != string(current.CurrentWorkerSlot) {
			if allowDegradedRecovery && edgeFrontHealthMatchesExpectedLKG(health, current, expectedLKGSourceSHA, expectedLKGImageDigest) {
				return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
			}
			if !allowDegradedRecovery || !edgeFrontHealthMatchesDegradedServingAuthority(health, current) {
				return nil, fmt.Errorf("Guardian current authority does not match the serving Front slot: %s expected_lkg=%s",
					edgeDegradedServingAuthorityMismatch(health, current, allowDegradedRecovery), edgeExpectedLKGMismatch(health, current, expectedLKGSourceSHA, expectedLKGImageDigest))
			}
			return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
		}
		if !edgeFrontHealthMatchesServingAuthority(health, current) {
			if allowDegradedRecovery && edgeFrontHealthMatchesExpectedLKG(health, current, expectedLKGSourceSHA, expectedLKGImageDigest) {
				return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
			}
			return nil, errors.New("Guardian current authority does not match serving Front activation evidence")
		}
		return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
	}
	if len(before.FrontHealth) == 0 {
		return nil, fmt.Errorf("Guardian current authority does not match the serving Front slot: front_health_empty activation_present=%t active_slot=%s guardian_slot=%s",
			before.FrontActivation != nil, before.ActiveSlot, current.CurrentWorkerSlot)
	}
	if before.ActiveSlot != string(current.CurrentWorkerSlot) {
		for _, health := range before.FrontHealth {
			if allowDegradedRecovery && edgeFrontHealthMatchesExpectedLKG(health, current, expectedLKGSourceSHA, expectedLKGImageDigest) {
				return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
			}
		}
		if !allowDegradedRecovery || current.CurrentWorkerSourceSHA == "" || current.CurrentWorkerImageDigest == "" ||
			current.CurrentBundleGeneration == "" {
			var health edgeFrontHealth
			for _, observed := range before.FrontHealth {
				health = observed
				break
			}
			return nil, fmt.Errorf("Guardian current authority does not match the serving Front slot: %s expected_lkg=%s",
				edgeDegradedServingAuthorityMismatch(health, current, allowDegradedRecovery), edgeExpectedLKGMismatch(health, current, expectedLKGSourceSHA, expectedLKGImageDigest))
		}
		for _, health := range before.FrontHealth {
			if !edgeFrontHealthMatchesDegradedServingAuthority(health, current) {
				return nil, fmt.Errorf("Guardian current authority does not match degraded serving Front evidence: %s",
					edgeDegradedServingAuthorityMismatch(health, current, allowDegradedRecovery))
			}
		}
		var health edgeFrontHealth
		for _, observed := range before.FrontHealth {
			health = observed
			break
		}
		return &edgeServingAuthorityWitness{
			CurrentRecordDigest: current.CurrentRecordDigest, AuthorityEpoch: current.AuthorityEpoch,
			CurrentAuthorityUID: uid, CurrentAuthorityRV: resourceVersion, FrontGeneration: health.Generation,
			BundleVersion: health.BundleGeneration, WorkerSlot: before.ActiveSlot,
			WorkerSourceSHA: health.WorkerSourceCommit, WorkerImageDigest: health.WorkerImageDigest,
		}, nil
	}
	for _, health := range before.FrontHealth {
		if !edgeFrontHealthMatchesServingAuthority(health, current) {
			if allowDegradedRecovery && edgeFrontHealthMatchesExpectedLKG(health, current, expectedLKGSourceSHA, expectedLKGImageDigest) {
				return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
			}
			return nil, errors.New("Guardian current authority does not match serving Front evidence")
		}
	}
	return &edgeServingAuthorityWitness{
		CurrentRecordDigest: current.CurrentRecordDigest, AuthorityEpoch: current.AuthorityEpoch,
		CurrentAuthorityUID: uid, CurrentAuthorityRV: resourceVersion, FrontGeneration: current.CurrentFrontGeneration,
		BundleVersion: current.CurrentBundleGeneration, WorkerSlot: string(current.CurrentWorkerSlot),
		WorkerSourceSHA: current.CurrentWorkerSourceSHA, WorkerImageDigest: current.CurrentWorkerImageDigest,
	}, nil
}

func edgeFrontHealthMatchesExpectedLKG(health edgeFrontHealth, current releaseguardian.CurrentAuthority, expectedSourceSHA, expectedImageDigest string) bool {
	if strings.TrimSpace(expectedSourceSHA) == "" || strings.TrimSpace(expectedImageDigest) == "" ||
		current.CurrentWorkerSourceSHA == expectedSourceSHA && current.CurrentWorkerImageDigest == expectedImageDigest {
		return false
	}
	if !health.ActivationPresent || (health.ActiveSlot != string(current.CurrentWorkerSlot) && health.ActiveSlot != string(current.PreviousWorkerSlot)) ||
		health.RouteAuthority != edgeActivationAuthority || health.Generation < current.CurrentFrontGeneration ||
		health.WorkerSourceCommit != expectedSourceSHA || health.WorkerImageDigest != expectedImageDigest {
		return false
	}
	currentGeneration, _, _, currentOK := parseEdgePublicationVersion(current.CurrentBundleGeneration)
	frontGeneration, _, _, frontOK := parseEdgePublicationVersion(health.BundleGeneration)
	return currentOK && frontOK && currentGeneration == frontGeneration
}

func edgeExpectedLKGMismatch(health edgeFrontHealth, current releaseguardian.CurrentAuthority, expectedSourceSHA, expectedImageDigest string) string {
	currentGeneration, _, _, currentOK := parseEdgePublicationVersion(current.CurrentBundleGeneration)
	frontGeneration, _, _, frontOK := parseEdgePublicationVersion(health.BundleGeneration)
	return fmt.Sprintf("slot_ok=%t source_ok=%t image_ok=%t activation_ok=%t route_ok=%t generation_ok=%t bundle_family_ok=%t expected_source=%s actual_source=%s expected_image=%s actual_image=%s",
		health.ActiveSlot == string(current.CurrentWorkerSlot) || health.ActiveSlot == string(current.PreviousWorkerSlot),
		health.WorkerSourceCommit == expectedSourceSHA, health.WorkerImageDigest == expectedImageDigest,
		health.ActivationPresent, health.RouteAuthority == edgeActivationAuthority, health.Generation >= current.CurrentFrontGeneration,
		currentOK && frontOK && currentGeneration == frontGeneration, expectedSourceSHA, health.WorkerSourceCommit, expectedImageDigest, health.WorkerImageDigest)
}

func edgeDegradedServingAuthorityMismatch(health edgeFrontHealth, current releaseguardian.CurrentAuthority, allowed bool) string {
	currentGeneration, _, _, currentOK := parseEdgePublicationVersion(current.CurrentBundleGeneration)
	frontGeneration, _, _, frontOK := parseEdgePublicationVersion(health.BundleGeneration)
	return fmt.Sprintf("allowed=%t activation_present=%t route_authority_ok=%t slot_ok=%t generation_ok=%t source_ok=%t image_ok=%t bundle_family_ok=%t",
		allowed, health.ActivationPresent, health.RouteAuthority == edgeActivationAuthority,
		health.ActiveSlot == "a" || health.ActiveSlot == "b", health.Generation >= current.CurrentFrontGeneration,
		health.WorkerSourceCommit == current.CurrentWorkerSourceSHA, health.WorkerImageDigest == current.CurrentWorkerImageDigest,
		currentOK && frontOK && currentGeneration == frontGeneration)
}

func edgeFrontHealthFromActivation(activation edgeActivationState) edgeFrontHealth {
	return edgeFrontHealth{ActiveSlot: activation.ActiveSlot, ActivationPresent: true, Generation: activation.Generation,
		BundleGeneration: activation.BundleGeneration, WorkerSourceCommit: activation.WorkerSourceCommit,
		WorkerImageDigest: activation.WorkerImageDigest, RouteAuthority: activation.Authority}
}

func edgeServingAuthorityWitnessFromFrontHealth(current releaseguardian.CurrentAuthority, uid, resourceVersion string, health edgeFrontHealth) *edgeServingAuthorityWitness {
	return &edgeServingAuthorityWitness{
		CurrentRecordDigest: current.CurrentRecordDigest, AuthorityEpoch: current.AuthorityEpoch,
		CurrentAuthorityUID: uid, CurrentAuthorityRV: resourceVersion, FrontGeneration: health.Generation,
		BundleVersion: health.BundleGeneration, WorkerSlot: health.ActiveSlot,
		WorkerSourceSHA: health.WorkerSourceCommit, WorkerImageDigest: health.WorkerImageDigest,
	}
}

func edgeFrontHealthMatchesDegradedServingAuthority(health edgeFrontHealth, current releaseguardian.CurrentAuthority) bool {
	if !health.ActivationPresent || health.RouteAuthority != edgeActivationAuthority ||
		(health.ActiveSlot != "a" && health.ActiveSlot != "b") || health.Generation < current.CurrentFrontGeneration ||
		health.WorkerSourceCommit != current.CurrentWorkerSourceSHA || health.WorkerImageDigest != current.CurrentWorkerImageDigest {
		return false
	}
	currentGeneration, _, _, currentOK := parseEdgePublicationVersion(current.CurrentBundleGeneration)
	frontGeneration, _, _, frontOK := parseEdgePublicationVersion(health.BundleGeneration)
	return currentOK && frontOK && currentGeneration == frontGeneration
}

func parseEdgePublicationVersion(version string) (string, uint64, uint64, bool) {
	version = strings.TrimSpace(version)
	pivot := strings.LastIndex(version, ".p")
	if pivot <= 0 {
		return "", 0, 0, false
	}
	recoveryPivot := strings.LastIndex(version[pivot+2:], ".r")
	if recoveryPivot <= 0 {
		return "", 0, 0, false
	}
	recoveryPivot += pivot + 2
	publication, publicationErr := strconv.ParseUint(version[pivot+2:recoveryPivot], 10, 64)
	recovery, recoveryErr := strconv.ParseUint(version[recoveryPivot+2:], 10, 64)
	if publicationErr != nil || recoveryErr != nil || publication == 0 {
		return "", 0, 0, false
	}
	return version[:pivot], publication, recovery, true
}

func edgeFrontHealthMatchesServingAuthority(health edgeFrontHealth, current releaseguardian.CurrentAuthority) bool {
	return health.ActivationPresent && health.ActiveSlot == string(current.CurrentWorkerSlot) &&
		health.Generation >= current.CurrentFrontGeneration && (health.Generation-current.CurrentFrontGeneration)%2 == 0 &&
		health.BundleGeneration == current.CurrentBundleGeneration && health.WorkerSourceCommit == current.CurrentWorkerSourceSHA &&
		health.WorkerImageDigest == current.CurrentWorkerImageDigest && health.RouteAuthority == edgeActivationAuthority
}

func edgeCandidateStatusURL(stageURL, groupID string) (string, error) {
	parsed, err := url.Parse(stageURL)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" ||
		parsed.Path != edgeCandidateStagePath {
		return "", errors.New("edge candidate staging URL is invalid")
	}
	parsed.Path = "/v1/authority/groups/" + groupID + "/readyz"
	return parsed.String(), nil
}

func (cluster *kubectlCluster) applyEdgeCandidateResources(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, manifest []byte, selector string) error {
	if selector == "" {
		return cluster.applyResourceSet(ctx, release, manifest, false)
	}
	name := selector
	if selector == "a" || selector == "b" {
		name = edgeWorkerName(transition, selector)
	}
	if name != transition.FrontName && name != transition.WorkerAName && name != transition.WorkerBName {
		return errors.New("edge candidate resource selector is invalid")
	}
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: release.Workload.Namespace, Name: name}
	desired, err := declarativerelease.ResourceSetItem(manifest, identity)
	if err != nil {
		return err
	}
	encoded, err := declarativerelease.CanonicalJSON(desired)
	if err != nil {
		return err
	}
	if err := cluster.applyResourceWithOwnershipConvergence(ctx, release, identity, desired, encoded, false); err != nil {
		return fmt.Errorf("apply candidate %s/%s: %w", identity.Kind, identity.Name, err)
	}
	return nil
}

func readEdgeCandidateStageStatus(ctx context.Context, stageURL, groupID string) (edgeCandidateStageStatus, error) {
	statusURL, err := edgeCandidateStatusURL(stageURL, groupID)
	if err != nil {
		return edgeCandidateStageStatus{}, err
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return edgeCandidateStageStatus{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusServiceUnavailable {
		return edgeCandidateStageStatus{}, fmt.Errorf("read edge-control candidate status: HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	var status edgeCandidateStageStatus
	if decoder.Decode(&status) != nil || decoder.Decode(&struct{}{}) != io.EOF || status.GroupID != groupID {
		return edgeCandidateStageStatus{}, errors.New("edge-control candidate status is invalid")
	}
	return status, nil
}

func postEdgeCandidateStage(ctx context.Context, endpoint string, value edgeCandidateStageRequest) (edgeCandidateStageReceipt, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return edgeCandidateStageReceipt{}, err
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
		request.Header.Set("Content-Type", "application/json")
		response, requestErr := (&http.Client{Timeout: 15 * time.Second}).Do(request)
		if requestErr != nil {
			lastErr = fmt.Errorf("%w: %v", errEdgeCandidateStageTransient, requestErr)
			continue
		}
		rawResponse, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		response.Body.Close()
		var receipt edgeCandidateStageReceipt
		decodeErr := decodeEdgeCandidateStageResponse(rawResponse, &receipt)
		if response.StatusCode == http.StatusOK && readErr == nil && decodeErr == nil &&
			receipt.Schema == edgeCandidateReceiptSchema {
			return receipt, nil
		}
		lastErr = fmt.Errorf("stage edge Worker candidate: HTTP %d", response.StatusCode)
		if readErr == nil {
			var failure edgeControlError
			if decodeEdgeCandidateStageResponse(rawResponse, &failure) == nil && failure.Schema == "edge-control-error/v1" && validEdgeControlErrorCode(failure.Error) {
				if response.StatusCode == http.StatusConflict && failure.Error == "sequence_conflict" {
					lastErr = errEdgeCandidateStageSequenceConflict
				} else {
					lastErr = fmt.Errorf("stage edge Worker candidate: HTTP %d (%s)", response.StatusCode, failure.Error)
				}
			}
		}
		if response.StatusCode != http.StatusServiceUnavailable {
			break
		}
	}
	return edgeCandidateStageReceipt{}, lastErr
}

func decodeEdgeCandidateStageResponse(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("edge-control candidate response has trailing data")
	}
	return nil
}

func edgeGroupRecoveryURL(stageURL string) (string, error) {
	parsed, err := url.Parse(stageURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("edge-control candidate stage URL is invalid")
	}
	parsed.Path = edgeGroupRecoveryPath
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func edgeCandidateRecoveryURL(stageURL string) (string, error) {
	parsed, err := url.Parse(stageURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("edge-control candidate stage URL is invalid")
	}
	parsed.Path = edgeCandidateRecoveryPath
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func validEdgeControlErrorCode(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && character != '_' {
			return false
		}
	}
	return true
}

func signEdgeCandidateStageRequest(filename string, request *edgeCandidateStageRequest, now time.Time) error {
	if request == nil {
		return errors.New("edge candidate staging request is nil")
	}
	raw, err := os.ReadFile(filename)
	if err != nil || len(raw) == 0 || len(raw) > 64<<10 {
		return errors.New("read edge candidate staging keyring")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var keyring edgeCandidateKeyring
	if decoder.Decode(&keyring) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		keyring.Schema != "edge-control-group-recovery-keyring/v1" || keyring.Generation == 0 || keyring.GroupID != request.GroupID {
		return errors.New("edge candidate staging keyring is invalid")
	}
	for _, key := range keyring.Keys {
		if key.Revoked || now.Before(time.Unix(key.NotBeforeUnix, 0)) || !now.Before(time.Unix(key.NotAfterUnix, 0)) {
			continue
		}
		secret, decodeErr := base64.RawURLEncoding.DecodeString(key.Secret)
		if decodeErr != nil || len(secret) < 32 || len(secret) > 64 {
			zeroEdgeCandidateSecret(secret)
			return errors.New("edge candidate staging key is invalid")
		}
		request.KeyID, request.Signature = key.KeyID, ""
		signingRaw, encodeErr := json.Marshal(request)
		if encodeErr != nil {
			zeroEdgeCandidateSecret(secret)
			return encodeErr
		}
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(signingRaw)
		request.Signature = base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
		zeroEdgeCandidateSecret(secret)
		return nil
	}
	return errors.New("edge candidate staging key is inactive")
}

func signEdgeGroupRecoveryRequest(filename string, request *edgeGroupRecoveryRequest, now time.Time) error {
	if request == nil {
		return errors.New("edge-control recovery request is nil")
	}
	raw, err := os.ReadFile(filename)
	if err != nil || len(raw) == 0 || len(raw) > 64<<10 {
		return errors.New("read edge-control recovery keyring")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var keyring edgeCandidateKeyring
	if decoder.Decode(&keyring) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		keyring.Schema != "edge-control-group-recovery-keyring/v1" || keyring.Generation == 0 || keyring.GroupID != request.GroupID {
		return errors.New("edge-control recovery keyring is invalid")
	}
	for _, key := range keyring.Keys {
		if key.Revoked || now.Before(time.Unix(key.NotBeforeUnix, 0)) || !now.Before(time.Unix(key.NotAfterUnix, 0)) {
			continue
		}
		secret, decodeErr := base64.RawURLEncoding.DecodeString(key.Secret)
		if decodeErr != nil || len(secret) < 32 || len(secret) > 64 {
			zeroEdgeCandidateSecret(secret)
			return errors.New("edge-control recovery key is invalid")
		}
		request.KeyID, request.Signature = key.KeyID, ""
		signingRaw, encodeErr := json.Marshal(request)
		if encodeErr != nil {
			zeroEdgeCandidateSecret(secret)
			return encodeErr
		}
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(signingRaw)
		request.Signature = base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
		zeroEdgeCandidateSecret(secret)
		return nil
	}
	return errors.New("edge-control recovery key is inactive")
}

func signEdgeCandidateRecoveryRequest(filename string, request *edgeCandidateRecoveryRequest, now time.Time) error {
	if request == nil {
		return errors.New("edge-control candidate recovery request is nil")
	}
	raw, err := os.ReadFile(filename)
	if err != nil || len(raw) == 0 || len(raw) > 64<<10 {
		return errors.New("read edge-control candidate recovery keyring")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var keyring edgeCandidateKeyring
	if decoder.Decode(&keyring) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		keyring.Schema != "edge-control-group-recovery-keyring/v1" || keyring.Generation == 0 || keyring.GroupID != request.GroupID {
		return errors.New("edge-control candidate recovery keyring is invalid")
	}
	for _, key := range keyring.Keys {
		if key.Revoked || now.Before(time.Unix(key.NotBeforeUnix, 0)) || !now.Before(time.Unix(key.NotAfterUnix, 0)) {
			continue
		}
		secret, decodeErr := base64.RawURLEncoding.DecodeString(key.Secret)
		if decodeErr != nil || len(secret) < 32 || len(secret) > 64 {
			zeroEdgeCandidateSecret(secret)
			return errors.New("edge-control candidate recovery key is invalid")
		}
		request.KeyID, request.Signature = key.KeyID, ""
		signingRaw, encodeErr := json.Marshal(request)
		if encodeErr != nil {
			zeroEdgeCandidateSecret(secret)
			return encodeErr
		}
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(signingRaw)
		request.Signature = base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
		zeroEdgeCandidateSecret(secret)
		return nil
	}
	return errors.New("edge-control candidate recovery key is inactive")
}

func zeroEdgeCandidateSecret(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func (runtime *kubectlEdgeGroupRuntime) Roll(ctx context.Context, name string, target declarativerelease.TargetIdentity, requireGroupAuthority, replaceUnready bool) (map[string]edgeGroupPod, error) {
	return runtime.cluster.rollEdgeDaemonSetTarget(ctx, runtime.client, runtime.release, runtime.transition, name, target, requireGroupAuthority, replaceUnready)
}

func (runtime *kubectlEdgeGroupRuntime) SelectCASExecutor(ctx context.Context, candidates ...edgeGroupPod) (edgeGroupPod, error) {
	return runtime.cluster.selectEdgeCASExecutor(ctx, runtime.release.Workload.Namespace, runtime.transition, candidates...)
}

func (runtime *kubectlEdgeGroupRuntime) ReadActivation(ctx context.Context, pod edgeGroupPod) (edgeActivationState, bool, error) {
	return runtime.cluster.readEdgeActivationState(ctx, runtime.release, runtime.transition, pod)
}

func (runtime *kubectlEdgeGroupRuntime) ActivationCAS(ctx context.Context, pod edgeGroupPod, request edgeActivationRequest) (edgeActivationReceipt, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		receipt, err := runtime.cluster.runEdgeActivationCAS(ctx, runtime.release, runtime.transition, pod, request)
		if err == nil {
			return receipt, nil
		}
		lastErr = err
		state, exists, readErr := runtime.cluster.readEdgeActivationState(ctx, runtime.release, runtime.transition, pod)
		if readErr != nil {
			return edgeActivationReceipt{}, errors.Join(lastErr, readErr)
		}
		if exists && edgeActivationStateMatchesRequest(state, request) {
			return edgeActivationReceipt{Schema: edgeActivationReceiptSchema, GroupID: request.GroupID, Current: state}, nil
		}
		if attempt != 0 || !edgeActivationStateMatchesPrecondition(state, exists, request) {
			break
		}
	}
	return edgeActivationReceipt{}, lastErr
}

func edgeActivationStateMatchesRequest(state edgeActivationState, request edgeActivationRequest) bool {
	return state.Schema == edgeActivationStateSchema && state.GroupID == request.GroupID && state.Generation == request.ExpectedGeneration+1 &&
		state.ActiveSlot == request.TargetSlot && state.BundleGeneration == request.BundleGeneration && state.WorkerSourceCommit == request.WorkerSourceCommit &&
		state.WorkerImageDigest == request.WorkerImageDigest && state.Authority == edgeActivationAuthority && state.Operation == request.Operation &&
		state.RollbackOfGeneration == request.RollbackOfGeneration
}

func edgeActivationStateMatchesPrecondition(state edgeActivationState, exists bool, request edgeActivationRequest) bool {
	if !exists {
		return request.Operation == edgeActivationInitialize && request.ExpectedGeneration == 0 && request.ExpectedSlot == request.TargetSlot
	}
	return state.Schema == edgeActivationStateSchema && state.GroupID == request.GroupID && state.Generation == request.ExpectedGeneration &&
		state.ActiveSlot == request.ExpectedSlot && state.Authority == edgeActivationAuthority
}

func (runtime *kubectlEdgeGroupRuntime) WaitFront(ctx context.Context, slot, source, digest string) (map[string]edgeFrontHealth, error) {
	return runtime.cluster.waitFrontActivation(ctx, runtime.release, runtime.transition, slot, source, digest)
}

func (runtime *kubectlEdgeGroupRuntime) WaitActiveWorkerAuthority(ctx context.Context, name string, target declarativerelease.TargetIdentity) error {
	return runtime.cluster.waitActiveEdgeWorkerAuthority(ctx, runtime.release, runtime.transition, name, target)
}

func (cluster *kubectlCluster) readEdgeGroupState(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition) (edgeGroupState, error) {
	// A broken active Front is exactly the state this transition is meant to
	// recover. Keep its immutable pod identity even when readiness is false so
	// the caller can use the group-local activation CAS as the serving witness.
	front, err := cluster.readEdgeDaemonSetPodsForSnapshot(ctx, release, transition.FrontName, "edge-front", transition.ExpectedNodes, transition.GroupID, false)
	if err != nil {
		return edgeGroupState{}, err
	}
	workerA, err := cluster.readEdgeDaemonSetPodsForSnapshot(ctx, release, transition.WorkerAName, transition.WorkerContainer, transition.ExpectedNodes, transition.GroupID, true)
	if err != nil {
		return edgeGroupState{}, err
	}
	workerB, err := cluster.readEdgeDaemonSetPodsForSnapshot(ctx, release, transition.WorkerBName, transition.WorkerContainer, transition.ExpectedNodes, transition.GroupID, true)
	if err != nil {
		return edgeGroupState{}, err
	}
	if !sameEdgeNodes(front, workerA) || !sameEdgeNodes(front, workerB) {
		return edgeGroupState{}, errors.New("edge group workloads do not share one exact node cohort")
	}
	// The Front /readyz endpoint is useful liveness evidence, but its metadata
	// can lag an activation CAS performed by a Worker after the Front process
	// started. Read the same CAS state from a group-local Worker mount so the
	// transition uses an authoritative slot and generation before staging a
	// candidate. This is independent of DNS and never fabricates an ACK.
	activation, activationExists, activationErr := cluster.readEdgeGroupActivation(ctx, release, transition, front, workerA, workerB)
	if activationErr != nil {
		return edgeGroupState{}, activationErr
	}
	frontHealth := make(map[string]edgeFrontHealth, len(front))
	activeSlot := ""
	for node, pod := range front {
		health, healthErr := cluster.readEdgeFrontHealth(ctx, pod)
		if healthErr != nil {
			if !activationExists {
				return edgeGroupState{}, healthErr
			}
			// /readyz depends on the active worker and can be unavailable during
			// recovery. The activation file is a group-local CAS witness; it does
			// not claim that traffic is healthy and is only used to plan repair.
			health = edgeFrontHealthFromActivation(*activation)
		}
		if activationExists && health.ActiveSlot != activation.ActiveSlot {
			return edgeGroupState{}, errors.New("edge front health disagrees with activation evidence")
		}
		if activeSlot == "" && activation == nil {
			activeSlot = health.ActiveSlot
		} else if activation == nil && activeSlot != health.ActiveSlot {
			return edgeGroupState{}, errors.New("edge group front nodes disagree on active slot")
		}
		frontHealth[node] = health
	}
	if activationExists {
		activeSlot = activation.ActiveSlot
	}
	if activeSlot != "a" && activeSlot != "b" {
		return edgeGroupState{}, errors.New("edge group active slot is invalid")
	}
	return edgeGroupState{Front: front, FrontHealth: frontHealth, FrontActivation: activation, WorkerA: workerA, WorkerB: workerB, ActiveSlot: activeSlot}, nil
}

func (cluster *kubectlCluster) readEdgeGroupActivation(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, front, workerA, workerB map[string]edgeGroupPod) (*edgeActivationState, bool, error) {
	// Front and Worker mount the same activation state, but the Front is the
	// process that serves it. Read the file from that container first so a
	// stale Front health cache cannot hide a valid CAS record before candidate
	// staging. Fall back to a Worker mount for older layouts.
	for _, node := range sortedEdgeNodes(front) {
		state, exists, err := cluster.readEdgeActivationStateFromPod(ctx, release, transition, front[node], "edge-front")
		if err != nil {
			return nil, false, fmt.Errorf("read Front activation evidence: %w", err)
		}
		if exists {
			return &state, true, nil
		}
	}
	candidates := make([]edgeGroupPod, 0, len(workerA)+len(workerB))
	for _, pods := range []map[string]edgeGroupPod{workerA, workerB} {
		for _, pod := range pods {
			candidates = append(candidates, pod)
		}
	}
	executor, err := cluster.selectEdgeCASExecutor(ctx, release.Workload.Namespace, transition, candidates...)
	if err != nil {
		return nil, false, fmt.Errorf("select edge activation evidence reader: %w", err)
	}
	state, exists, err := cluster.readEdgeActivationState(ctx, release, transition, executor)
	if err != nil {
		return nil, false, fmt.Errorf("read edge activation evidence: %w", err)
	}
	if !exists {
		return nil, false, nil
	}
	return &state, true, nil
}

// An active Worker or Front can be unready during an outage. Preserve its
// immutable pod identity so the transition planner can recover that slot
// instead of failing before it can publish the signed LKG candidate.
func (cluster *kubectlCluster) readEdgeDaemonSetPodsForSnapshot(ctx context.Context, release declarativerelease.PlanRelease, name, container string, expectedNodes int, groupID string, includeWorkerHealth bool) (map[string]edgeGroupPod, error) {
	pods, err := cluster.readEdgeDaemonSetPodsWithReadiness(ctx, release, name, container, expectedNodes, groupID, false, false)
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		if !pod.Ready {
			return pods, nil
		}
	}
	return cluster.readEdgeDaemonSetPods(ctx, release, name, container, expectedNodes, groupID, includeWorkerHealth)
}

func (cluster *kubectlCluster) readEdgeDaemonSetPods(ctx context.Context, release declarativerelease.PlanRelease, name, container string, expectedNodes int, groupID string, includeWorkerHealth bool) (map[string]edgeGroupPod, error) {
	return cluster.readEdgeDaemonSetPodsWithReadiness(ctx, release, name, container, expectedNodes, groupID, includeWorkerHealth, true)
}

func (cluster *kubectlCluster) readEdgeDaemonSetPodsWithReadiness(ctx context.Context, release declarativerelease.PlanRelease, name, container string, expectedNodes int, groupID string, includeWorkerHealth, requireReady bool) (map[string]edgeGroupPod, error) {
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: release.Workload.Namespace, Name: name}
	workloadRaw, err := cluster.getResource(ctx, identity)
	if err != nil || len(bytes.TrimSpace(workloadRaw)) == 0 {
		return nil, fmt.Errorf("read DaemonSet/%s: %w", name, err)
	}
	selector, err := selectorFromWorkload(workloadRaw)
	if err != nil {
		return nil, err
	}
	podsRaw, err := cluster.kubectlRun(ctx, nil, "get", "pods", "--namespace", release.Workload.Namespace, "--selector", selector, "--output", "json")
	if err != nil {
		return nil, err
	}
	pods, err := parseEdgeGroupPodsWithReadiness(podsRaw, container, expectedNodes, groupID, false, "", requireReady)
	if err != nil {
		return nil, fmt.Errorf("parse DaemonSet/%s pods: %w", name, err)
	}
	// Both the front and worker edge containers expose their readiness endpoint
	// on the canonical named port "health".  The worker resources have always
	// declared that name; using "http" here made prewrite adoption reject the
	// healthy live bootstrap cohort before any production write.
	var endpoints []podHTTPEndpoint
	var endpointErr error
	for _, portName := range edgeHealthPortNames() {
		endpoints, endpointErr = podHTTPEndpointsFromJSONWithReadiness(podsRaw, container, portName, requireReady)
		if endpointErr == nil {
			break
		}
	}
	if !requireReady && endpointErr != nil {
		// Snapshot mode only needs immutable pod identity. An unready pod may
		// not have a routable PodIP yet; activation CAS evidence is used by the
		// recovery planner instead of probing this endpoint.
		return pods, nil
	}
	if endpointErr != nil {
		err = endpointErr
	}
	if err != nil {
		return nil, fmt.Errorf("read DaemonSet/%s health endpoints: %w", name, err)
	}
	byName := make(map[string]podHTTPEndpoint, len(endpoints))
	for _, endpoint := range endpoints {
		byName[endpoint.Name] = endpoint
	}
	for node, pod := range pods {
		endpoint, exists := byName[pod.Name]
		if !exists {
			return nil, fmt.Errorf("DaemonSet/%s pod health endpoint is absent", name)
		}
		pod.PodIP, pod.HealthPort = endpoint.IP, endpoint.Port
		pods[node] = pod
	}
	if !requireReady {
		return pods, nil
	}
	if includeWorkerHealth {
		for node, pod := range pods {
			health, healthErr := cluster.readEdgeWorkerHealth(ctx, pod, groupID)
			if healthErr != nil {
				return nil, healthErr
			}
			pod.BundleGeneration = health.BundleGeneration
			pod.RouteBundleSource = health.RouteBundleSource
			pod.PublicationSequence = health.PublicationSequence
			pod.ServingGeneration = health.ServingGeneration
			pod.InventoryProducerActive = health.InventoryProducerActive
			pod.InventoryHeartbeatGeneration = health.InventoryHeartbeatGeneration
			pod.InventoryHeartbeatAt = health.InventoryHeartbeatAt
			pod.InventoryHeartbeatError = health.InventoryHeartbeatError
			pods[node] = pod
		}
	}
	return pods, nil
}

func edgeHealthPortNames() []string {
	return []string{"health"}
}

func parseEdgeGroupPods(raw []byte, container string, expectedNodes int, groupID string, allowLegacy bool, legacySource string) (map[string]edgeGroupPod, error) {
	return parseEdgeGroupPodsWithReadiness(raw, container, expectedNodes, groupID, allowLegacy, legacySource, true)
}

func parseEdgeGroupPodsWithReadiness(raw []byte, container string, expectedNodes int, groupID string, allowLegacy bool, legacySource string, requireReady bool) (map[string]edgeGroupPod, error) {
	value, err := decodeJSONObject(raw)
	if err != nil {
		return nil, err
	}
	items, ok := value["items"].([]any)
	if !ok {
		return nil, errors.New("pod list is invalid")
	}
	pods := make(map[string]edgeGroupPod, len(items))
	for _, rawItem := range items {
		item, ok := rawItem.(map[string]any)
		if !ok {
			return nil, errors.New("pod item is invalid")
		}
		metadata := mapField(item, "metadata")
		if metadata["deletionTimestamp"] != nil {
			continue
		}
		labels := mapStringField(metadata, "labels")
		podGroupID := labels["fugue.io/edge-group-id"]
		if podGroupID != groupID && !(allowLegacy && podGroupID == "") {
			return nil, errors.New("pod edge group identity mismatch")
		}
		spec := mapField(item, "spec")
		node := stringValue(spec["nodeName"])
		pod := edgeGroupPod{Name: stringValue(metadata["name"]), UID: stringValue(metadata["uid"]), ResourceVersion: stringValue(metadata["resourceVersion"]), NodeName: node,
			SourceCommit: mapStringField(metadata, "annotations")["fugue.pro/source-commit"]}
		if pod.SourceCommit == "" && allowLegacy {
			pod.SourceCommit = legacySource
			pod.LegacyIdentity = true
		}
		containers, _ := spec["containers"].([]any)
		for _, rawContainer := range containers {
			entry, _ := rawContainer.(map[string]any)
			if stringValue(entry["name"]) == container {
				pod.ImageRef = stringValue(entry["image"])
			}
		}
		status := mapField(item, "status")
		for _, rawCondition := range anySlice(status["conditions"]) {
			condition, _ := rawCondition.(map[string]any)
			if stringValue(condition["type"]) == "Ready" && stringValue(condition["status"]) == "True" {
				pod.Ready = true
			}
		}
		for _, rawStatus := range anySlice(status["containerStatuses"]) {
			entry, _ := rawStatus.(map[string]any)
			if stringValue(entry["name"]) == container {
				pod.RestartCount = int64Value(entry["restartCount"])
				pod.ImageID = stringValue(entry["imageID"])
			}
		}
		// Snapshot reads are used to recover an active slot whose readiness is
		// already broken. Pod-level source annotations and container imageID are
		// runtime evidence, not the immutable identity needed for an exact delete
		// or node-cohort comparison; strict health reads still require both.
		identityIncomplete := pod.Name == "" || pod.UID == "" || pod.ResourceVersion == "" || pod.NodeName == "" || pod.ImageRef == ""
		if requireReady {
			identityIncomplete = identityIncomplete || pod.SourceCommit == "" || pod.ImageID == "" || !pod.Ready
		}
		if identityIncomplete {
			return nil, errors.New("pod identity or readiness is incomplete")
		}
		if _, exists := pods[node]; exists {
			return nil, errors.New("multiple edge pods occupy one group node")
		}
		pods[node] = pod
	}
	if len(pods) != expectedNodes {
		return nil, fmt.Errorf("edge group has %d healthy nodes, want %d", len(pods), expectedNodes)
	}
	return pods, nil
}

func (cluster *kubectlCluster) readEdgeWorkerHealth(ctx context.Context, pod edgeGroupPod, groupID string) (edgeWorkerHealth, error) {
	body, err := readPodHTTP(ctx, podHTTPEndpoint{Name: pod.Name, IP: pod.PodIP, Port: pod.HealthPort}, "/healthz")
	if err != nil {
		return edgeWorkerHealth{}, err
	}
	value, err := decodeJSONObject(body)
	if err != nil || value["healthy"] != true || stringValue(value["edge_group_id"]) != groupID || strings.TrimSpace(stringValue(value["bundle_version"])) == "" {
		return edgeWorkerHealth{}, errors.New("edge worker health is not group-bound and healthy")
	}
	health := edgeWorkerHealth{
		BundleGeneration:        strings.TrimSpace(stringValue(value["bundle_version"])),
		RouteBundleSource:       strings.TrimSpace(stringValue(value["route_bundle_source"])),
		ServingGeneration:       strings.TrimSpace(stringValue(value["serving_generation"])),
		InventoryProducerActive: value["inventory_producer_active"] == true,
		InventoryHeartbeatError: strings.TrimSpace(stringValue(value["inventory_heartbeat_error"])),
	}
	if raw, exists := value["publication_sequence"]; exists {
		health.PublicationSequence, err = uint64Value(raw)
		if err != nil {
			return edgeWorkerHealth{}, errors.New("edge worker publication sequence is invalid")
		}
	}
	if raw, exists := value["inventory_heartbeat_generation"]; exists {
		health.InventoryHeartbeatGeneration, err = uint64Value(raw)
		if err != nil {
			return edgeWorkerHealth{}, errors.New("edge worker inventory generation is invalid")
		}
	}
	if raw := strings.TrimSpace(stringValue(value["inventory_heartbeat_at"])); raw != "" {
		health.InventoryHeartbeatAt, err = time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return edgeWorkerHealth{}, errors.New("edge worker inventory timestamp is invalid")
		}
	}
	return health, nil
}

func edgePodHasGroupAuthority(pod edgeGroupPod) bool {
	return pod.RouteBundleSource == edgeGroupAuthoritySource && pod.PublicationSequence > 0 && pod.ServingGeneration != ""
}

func edgePodHasActiveInventory(pod edgeGroupPod) bool {
	return edgePodHasGroupAuthority(pod) && pod.InventoryProducerActive && pod.InventoryHeartbeatGeneration > 0 &&
		!pod.InventoryHeartbeatAt.IsZero() && pod.InventoryHeartbeatError == ""
}

func validateEdgeGroupAuthority(state edgeGroupState, transition declarativerelease.EdgeGroupABTransition) error {
	if err := validateActiveEdgeGroupAuthority(state, transition); err != nil {
		return err
	}
	for slot, pods := range map[string]map[string]edgeGroupPod{"a": state.WorkerA, "b": state.WorkerB} {
		for node, pod := range pods {
			if !edgePodHasGroupAuthority(pod) {
				return fmt.Errorf("edge group slot %s node %s has no verified group authority publication", slot, node)
			}
		}
	}
	return nil
}

func validateActiveEdgeGroupAuthority(state edgeGroupState, transition declarativerelease.EdgeGroupABTransition) error {
	if state.ActiveSlot != "a" && state.ActiveSlot != "b" {
		return errors.New("edge group authority active slot is invalid")
	}
	if transition.GroupID == "" {
		return errors.New("edge group authority transition is unbound")
	}
	for node, pod := range edgeWorkerPods(state, state.ActiveSlot) {
		if !edgePodHasGroupAuthority(pod) {
			return fmt.Errorf("edge group active slot %s node %s has no verified group authority publication", state.ActiveSlot, node)
		}
		if !edgePodHasActiveInventory(pod) {
			return fmt.Errorf("edge group active slot %s node %s has no verified inventory heartbeat", state.ActiveSlot, node)
		}
	}
	return nil
}

func (cluster *kubectlCluster) waitActiveEdgeWorkerAuthority(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, name string, target declarativerelease.TargetIdentity) error {
	deadline := time.Now().Add(cluster.timeout)
	for {
		pods, err := cluster.readEdgeDaemonSetPods(ctx, release, name, transition.WorkerContainer, transition.ExpectedNodes, transition.GroupID, true)
		if err == nil {
			converged := len(pods) == transition.ExpectedNodes
			for _, pod := range pods {
				converged = converged && edgePodMatchesTarget(pod, target) && edgePodHasActiveInventory(pod)
			}
			if converged {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("DaemonSet/%s did not publish active group authority and inventory evidence", name)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (cluster *kubectlCluster) readEdgeFrontHealth(ctx context.Context, pod edgeGroupPod) (edgeFrontHealth, error) {
	body, err := readPodHTTP(ctx, podHTTPEndpoint{Name: pod.Name, IP: pod.PodIP, Port: pod.HealthPort}, "/readyz")
	if err != nil {
		return edgeFrontHealth{}, err
	}
	value, err := decodeJSONObject(body)
	if err != nil || stringValue(value["status"]) != "ok" {
		return edgeFrontHealth{}, errors.New("edge front is not ready")
	}
	health := edgeFrontHealth{ActiveSlot: strings.ToLower(stringValue(value["active_slot"])), BundleGeneration: stringValue(value["bundle_generation"]),
		WorkerSourceCommit: stringValue(value["worker_source_commit"]), WorkerImageDigest: stringValue(value["worker_image_digest"]), RouteAuthority: stringValue(value["route_authority"])}
	if health.ActiveSlot != "a" && health.ActiveSlot != "b" {
		return edgeFrontHealth{}, errors.New("edge front active slot is invalid")
	}
	if rawGeneration, exists := value["activation_generation"]; exists {
		generation, parseErr := uint64Value(rawGeneration)
		if parseErr != nil || generation == 0 || health.BundleGeneration == "" || health.WorkerSourceCommit == "" || health.WorkerImageDigest == "" || health.RouteAuthority != edgeActivationAuthority {
			return edgeFrontHealth{}, errors.New("edge front activation evidence is invalid")
		}
		health.ActivationPresent = true
		health.Generation = generation
	}
	return health, nil
}

func (cluster *kubectlCluster) rollEdgeDaemonSet(ctx context.Context, client dynamic.Interface, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, name string, target declarativerelease.TargetIdentity) (map[string]edgeGroupPod, error) {
	return cluster.rollEdgeDaemonSetTarget(ctx, client, release, transition, name, target, true, false)
}

func (cluster *kubectlCluster) rollEdgeDaemonSetTarget(ctx context.Context, client dynamic.Interface, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, name string, target declarativerelease.TargetIdentity, requireGroupAuthority, replaceUnready bool) (map[string]edgeGroupPod, error) {
	container := transition.WorkerContainer
	// Inactive workers may be pre-authority during adoption, but their
	// immutable bundle generation is still required by the activation CAS.
	// Keep worker health evidence enabled while leaving the authority gate
	// controlled independently by requireGroupAuthority.
	includeHealth := edgeRollIncludesWorkerHealth(transition, name)
	if name == transition.FrontName {
		container = "edge-front"
		includeHealth = false
	}
	var current map[string]edgeGroupPod
	var err error
	if replaceUnready {
		current, err = cluster.readEdgeDaemonSetPodsForSnapshot(ctx, release, name, container, transition.ExpectedNodes, transition.GroupID, includeHealth)
	} else {
		current, err = cluster.readEdgeDaemonSetPods(ctx, release, name, container, transition.ExpectedNodes, transition.GroupID, includeHealth)
	}
	if err != nil {
		return nil, err
	}
	for _, node := range sortedEdgeNodes(current) {
		pod := current[node]
		if edgePodMatchesTarget(pod, target) && (!replaceUnready || pod.Ready && (!includeHealth || edgePodHasGroupAuthority(pod))) {
			continue
		}
		if err := deleteEdgePodExact(ctx, client, release.Workload.Namespace, pod); err != nil {
			return nil, err
		}
		if _, err := cluster.waitEdgePodTarget(ctx, release, transition, name, container, node, pod.UID, target, includeHealth, requireGroupAuthority); err != nil {
			return nil, err
		}
	}
	return cluster.readEdgeDaemonSetPods(ctx, release, name, container, transition.ExpectedNodes, transition.GroupID, includeHealth)
}

func edgeRollIncludesWorkerHealth(transition declarativerelease.EdgeGroupABTransition, name string) bool {
	return name != transition.FrontName
}

func deleteEdgePodExact(ctx context.Context, client dynamic.Interface, namespace string, pod edgeGroupPod) error {
	uid := types.UID(pod.UID)
	rv := pod.ResourceVersion
	foreground := metav1.DeletePropagationForeground
	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}, PropagationPolicy: &foreground}
	if err := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "pods"}).Namespace(namespace).Delete(ctx, pod.Name, options); err != nil {
		return fmt.Errorf("delete Pod/%s with UID/RV preconditions: %w", pod.Name, err)
	}
	return nil
}

func (cluster *kubectlCluster) waitEdgePodTarget(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, name, container, node, priorUID string, target declarativerelease.TargetIdentity, includeHealth, requireGroupAuthority bool) (edgeGroupPod, error) {
	deadline := time.Now().Add(cluster.timeout)
	for {
		pods, err := cluster.readEdgeDaemonSetPods(ctx, release, name, container, transition.ExpectedNodes, transition.GroupID, includeHealth)
		if err == nil {
			pod, exists := pods[node]
			authorityReady := !includeHealth || !requireGroupAuthority || edgePodHasGroupAuthority(pod)
			if exists && pod.UID != priorUID && edgePodMatchesTarget(pod, target) && authorityReady {
				return pod, nil
			}
		}
		if time.Now().After(deadline) {
			return edgeGroupPod{}, fmt.Errorf("DaemonSet/%s node %s did not converge to immutable target", name, node)
		}
		select {
		case <-ctx.Done():
			return edgeGroupPod{}, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (cluster *kubectlCluster) selectEdgeCASExecutor(ctx context.Context, namespace string, transition declarativerelease.EdgeGroupABTransition, candidates ...edgeGroupPod) (edgeGroupPod, error) {
	probe := edgeCASExecutorProbeArguments(transition)
	deadline := time.Now().Add(20 * time.Second)
	for {
		for _, pod := range candidates {
			if pod.Name == "" {
				continue
			}
			arguments := []string{"exec", "--namespace", namespace, pod.Name, "--container", transition.WorkerContainer, "--"}
			arguments = append(arguments, probe...)
			if _, err := cluster.kubectlRun(ctx, nil, arguments...); err == nil {
				return pod, nil
			}
		}
		if time.Now().After(deadline) {
			return edgeGroupPod{}, errors.New("no group-local worker contains the fixed activation CAS binary and writable state mount")
		}
		select {
		case <-ctx.Done():
			return edgeGroupPod{}, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func edgeCASExecutorProbeArguments(transition declarativerelease.EdgeGroupABTransition) []string {
	return []string{"sh", "-ceu", `test -x "$1" && test -d "$2" && test -w "$2"`, "sh", transition.CASBinary, path.Dir(transition.ActivationStatePath)}
}

func (cluster *kubectlCluster) runEdgeActivationCAS(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, pod edgeGroupPod, request edgeActivationRequest) (edgeActivationReceipt, error) {
	arguments := []string{"exec", "--namespace", release.Workload.Namespace, pod.Name, "--container", transition.WorkerContainer, "--", transition.CASBinary,
		"--state-file", transition.ActivationStatePath, "--group", request.GroupID,
		"--expected-generation", strconv.FormatUint(request.ExpectedGeneration, 10), "--expected-slot", request.ExpectedSlot, "--target-slot", request.TargetSlot,
		"--bundle-generation", request.BundleGeneration, "--worker-source-commit", request.WorkerSourceCommit, "--worker-image-digest", request.WorkerImageDigest,
		"--operation", request.Operation, "--rollback-of-generation", strconv.FormatUint(request.RollbackOfGeneration, 10), "--reason", request.Reason}
	raw, err := cluster.kubectlRun(ctx, nil, arguments...)
	if err != nil {
		return edgeActivationReceipt{}, err
	}
	decoder := json.NewDecoder(io.LimitReader(bytes.NewReader(raw), 64<<10))
	decoder.DisallowUnknownFields()
	var receipt edgeActivationReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return edgeActivationReceipt{}, errors.New("edge activation receipt is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || receipt.Schema != edgeActivationReceiptSchema || receipt.GroupID != transition.GroupID {
		return edgeActivationReceipt{}, errors.New("edge activation receipt identity is invalid")
	}
	if receipt.Current.GroupID != request.GroupID || receipt.Current.ActiveSlot != request.TargetSlot || receipt.Current.BundleGeneration != request.BundleGeneration ||
		receipt.Current.WorkerSourceCommit != request.WorkerSourceCommit || receipt.Current.WorkerImageDigest != request.WorkerImageDigest || receipt.Current.Operation != request.Operation {
		return edgeActivationReceipt{}, errors.New("edge activation receipt is not bound to the request")
	}
	return receipt, nil
}

func (cluster *kubectlCluster) readEdgeActivationState(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, pod edgeGroupPod) (edgeActivationState, bool, error) {
	return cluster.readEdgeActivationStateFromPod(ctx, release, transition, pod, transition.WorkerContainer)
}

func (cluster *kubectlCluster) readEdgeActivationStateFromPod(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, pod edgeGroupPod, container string) (edgeActivationState, bool, error) {
	raw, err := cluster.kubectlRun(ctx, nil, "exec", "--namespace", release.Workload.Namespace, pod.Name, "--container", container,
		"--", "sh", "-ceu", `if [ ! -e "$1" ]; then printf 'absent\n'; exit 0; fi; cat "$1"`, "sh", transition.ActivationStatePath)
	if err != nil {
		return edgeActivationState{}, false, err
	}
	if string(raw) == "absent\n" {
		return edgeActivationState{}, false, nil
	}
	decoder := json.NewDecoder(io.LimitReader(bytes.NewReader(raw), 64<<10))
	decoder.DisallowUnknownFields()
	var state edgeActivationState
	if err := decoder.Decode(&state); err != nil {
		return edgeActivationState{}, false, errors.New("edge activation state is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || state.Schema != edgeActivationStateSchema ||
		state.GroupID != transition.GroupID || state.Generation == 0 || state.Authority != edgeActivationAuthority ||
		(state.ActiveSlot != "a" && state.ActiveSlot != "b") {
		return edgeActivationState{}, false, errors.New("edge activation state identity is invalid")
	}
	return state, true, nil
}

func (cluster *kubectlCluster) waitFrontActivation(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, slot, source, digest string) (map[string]edgeFrontHealth, error) {
	deadline := time.Now().Add(cluster.timeout)
	for {
		// The old active slot may keep the Front Pod unready until the Worker
		// activation CAS moves traffic to the healthy candidate. Snapshot mode
		// retains its Pod endpoint so this wait can observe that CAS boundary.
		front, err := cluster.readEdgeDaemonSetPodsForSnapshot(ctx, release, transition.FrontName, "edge-front", transition.ExpectedNodes, transition.GroupID, false)
		if err == nil {
			health := make(map[string]edgeFrontHealth, len(front))
			matched := true
			for node, pod := range front {
				item, healthErr := cluster.readEdgeFrontHealth(ctx, pod)
				if healthErr != nil || !item.ActivationPresent || item.ActiveSlot != slot || (source != "" && item.WorkerSourceCommit != source) || (digest != "" && item.WorkerImageDigest != digest) {
					matched = false
					break
				}
				health[node] = item
			}
			if matched {
				return health, nil
			}
		}
		if time.Now().After(deadline) {
			return nil, errors.New("edge front activation did not converge")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func edgeWorkerPods(state edgeGroupState, slot string) map[string]edgeGroupPod {
	if slot == "a" {
		return state.WorkerA
	}
	return state.WorkerB
}

func edgeWorkerName(transition declarativerelease.EdgeGroupABTransition, slot string) string {
	if slot == "a" {
		return transition.WorkerAName
	}
	return transition.WorkerBName
}

func otherEdgeSlot(slot string) string {
	if slot == "a" {
		return "b"
	}
	return "a"
}

func edgePodMatchesTarget(pod edgeGroupPod, target declarativerelease.TargetIdentity) bool {
	return pod.Ready && pod.RestartCount == 0 && pod.SourceCommit == target.ConfigSHA && pod.ImageRef == target.ImageRef
}

func edgePodsMatchTarget(pods map[string]edgeGroupPod, target declarativerelease.TargetIdentity) bool {
	if len(pods) == 0 {
		return false
	}
	for _, pod := range pods {
		if !edgePodMatchesTarget(pod, target) {
			return false
		}
	}
	return true
}

func sameEdgeNodes(left, right map[string]edgeGroupPod) bool {
	if len(left) != len(right) {
		return false
	}
	for node := range left {
		if _, exists := right[node]; !exists {
			return false
		}
	}
	return true
}

func sortedEdgeNodes(pods map[string]edgeGroupPod) []string {
	nodes := make([]string, 0, len(pods))
	for node := range pods {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return nodes
}

func allFrontActivationPresent(values map[string]edgeFrontHealth) bool {
	for _, value := range values {
		if !value.ActivationPresent {
			return false
		}
	}
	return len(values) > 0
}

func immutableDigestFromRef(ref string) (string, error) {
	index := strings.LastIndex(ref, "@sha256:")
	if index < 1 {
		return "", errors.New("edge target is not an immutable digest reference")
	}
	digest := ref[index+1:]
	if len(digest) != len("sha256:")+64 {
		return "", errors.New("edge target digest is invalid")
	}
	return digest, nil
}

func uint64Value(value any) (uint64, error) {
	switch typed := value.(type) {
	case json.Number:
		return strconv.ParseUint(string(typed), 10, 64)
	case float64:
		if typed < 0 || typed != float64(uint64(typed)) {
			return 0, errors.New("value is not an unsigned integer")
		}
		return uint64(typed), nil
	default:
		return 0, errors.New("value is not an unsigned integer")
	}
}

func anySlice(value any) []any {
	values, _ := value.([]any)
	return values
}
