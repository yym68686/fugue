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
	"fugue/internal/edgecontrol"
	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

const (
	edgeActivationStateSchema          = edgegroupfront.ActivationStateSchemaV1
	edgeActivationReceiptSchema        = edgegroupfront.ActivationReceiptSchemaV1
	edgeActivationAuthority            = edgegroupfront.ActivationAuthority
	edgeActivationInitialize           = edgegroupfront.ActivationOperationInit
	edgeActivationPromote              = edgegroupfront.ActivationOperationPromote
	edgeActivationRollback             = edgegroupfront.ActivationOperationRollback
	edgeGroupAuthoritySource           = "edge-control-group-authority/v1"
	edgeCandidateStagePath             = edgecontrol.GroupCandidateStagePathV1
	edgeGroupRecoveryPath              = edgecontrol.GroupRecoveryPathV1
	edgeCandidateRecoveryPath          = edgecontrol.GroupCandidateRecoveryPathV1
	edgeCandidateStageSchema           = edgecontrol.GroupCandidateStageRequestSchemaV1
	edgeCandidateReceiptSchema         = edgecontrol.GroupCandidateStageReceiptSchemaV1
	edgeGroupRecoverySchema            = edgecontrol.GroupRecoveryRequestSchemaV1
	edgeGroupRecoveryReceiptSchema     = edgecontrol.GroupRecoveryReceiptSchemaV1
	edgeCandidateRecoverySchema        = edgecontrol.GroupCandidateRecoveryRequestSchemaV1
	edgeCandidateRecoveryReceiptSchema = edgecontrol.GroupCandidateRecoveryReceiptSchemaV1
	edgeCandidateStageAttempts         = 4
	edgeCandidateStageRetryBase        = 200 * time.Millisecond
	edgeGroupRecoveryHTTPTimeout       = 60 * time.Second
	edgeInventoryHeartbeatMaxAge       = 2 * time.Minute
	edgeInventoryHeartbeatClockSkew    = 30 * time.Second
)

var (
	edgeServingAuthorityTokenPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{1,255}$`)
	edgeSourceSHAPattern                  = regexp.MustCompile(`^[0-9a-f]{40}$`)
	edgePromotionDigestPattern            = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	errEdgeCandidateStageSequenceConflict = errors.New("stage edge Worker candidate: HTTP 409 (sequence_conflict)")
	errEdgeCandidateStageTransient        = errors.New("stage edge Worker candidate: transient transport failure")
	errEdgeInventoryHeartbeatUnavailable  = errors.New("edge inventory heartbeat is temporarily unavailable")
)

type edgeServingAuthorityWitness = edgecontrol.GroupServingAuthorityWitness
type edgeCandidateStageRequest = edgecontrol.GroupCandidateStageRequest
type edgeCandidateStageReceipt = edgecontrol.GroupCandidateStageReceipt

type edgeControlError struct {
	Schema string `json:"schema"`
	Error  string `json:"error"`
}

type edgeCandidateStageStatus struct {
	GroupID                    string `json:"edge_group_id"`
	Ready                      bool   `json:"ready"`
	ServingHealthy             bool   `json:"serving_healthy"`
	AuthoritySequence          uint64 `json:"authority_sequence"`
	CurrentPublicationSequence uint64 `json:"current_publication_sequence"`
	CandidateEpoch             uint64 `json:"candidate_epoch"`
	CandidateWorkerSourceSHA   string `json:"candidate_worker_source_sha"`
	PublicationDecision        string `json:"publication_decision"`
	PublishedBundleDigest      string `json:"published_bundle_digest"`
	BundleGeneration           string `json:"bundle_generation"`
	RecoveryEpoch              uint64 `json:"recovery_epoch"`
	LKGState                   string `json:"lkg_state"`
}

type edgeGroupRecoveryRequest = edgecontrol.GroupRecoveryRequest
type edgeGroupRecoveryReceipt = edgecontrol.GroupRecoveryReceipt
type edgeCandidateRecoveryRequest = edgecontrol.GroupCandidateRecoveryRequest
type edgeCandidateRecoveryReceipt = edgecontrol.GroupCandidateRecoveryReceipt

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

type edgeActivationRequest = edgegroupfront.ActivationCASRequest
type edgeActivationState = edgegroupfront.ActivationState
type edgeActivationReceipt = edgegroupfront.ActivationReceipt

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
	CandidateBundleLoaded        bool
	CandidateRecordDigest        string
	CandidateReleaseRecordDigest string
	CandidateWorkerSlot          string
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
	CandidateBundleLoaded        bool
	CandidateRecordDigest        string
	CandidateReleaseRecordDigest string
	CandidateWorkerSlot          string
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
	ApplySharedResources(context.Context) error
	ApplyCandidateResources(context.Context, string) error
	StageCandidate(context.Context, edgeGroupState, string, declarativerelease.TargetIdentity) (edgeCandidateStageReceipt, error)
	StageStandby(context.Context, edgeGroupState, string, declarativerelease.TargetIdentity) (edgeCandidateStageReceipt, error)
	DeclaredTarget(string) (declarativerelease.TargetIdentity, error)
	Roll(context.Context, string, declarativerelease.TargetIdentity, bool, bool) (map[string]edgeGroupPod, error)
	WaitCandidateWorkerAuthority(context.Context, string, declarativerelease.TargetIdentity, edgeCandidateStageReceipt) (map[string]edgeGroupPod, error)
	SelectCASExecutor(context.Context, ...edgeGroupPod) (edgeGroupPod, error)
	ReadActivation(context.Context, edgeGroupPod) (edgeActivationState, bool, error)
	WaitFront(context.Context, string, string, string) (map[string]edgeFrontHealth, error)
	WaitCurrentAuthority(context.Context, edgeCandidateStageReceipt) error
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

// ReconcileCommittedForward is the read-only recovery path for an Edge A/B
// transaction whose Guardian authority CAS committed before the executor lost
// its terminal receipt. Static manifests describe candidate intent, while
// CurrentAuthority owns the current and immediately reversible slot identities;
// both sources must agree with the live group for the full soak window.
func (cluster *kubectlCluster) ReconcileCommittedForward(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte) (declarativerelease.Observation, error) {
	if release.Transition == nil || release.Transition.Type != "edge-group-ab" || release.Transition.EdgeGroupAB == nil || !target.Present {
		return declarativerelease.Observation{}, errors.New("committed edge reconciliation is not transition-bound")
	}
	transition := *release.Transition.EdgeGroupAB
	config, err := loadComponentLeaseClientConfig()
	if err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("load Kubernetes client config: %w", err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("create Kubernetes dynamic client: %w", err)
	}
	runtime := &kubectlEdgeGroupRuntime{cluster: cluster, client: client, release: release, transition: transition, manifest: manifest}
	probeRelease := release
	probeRelease.Health = make([]declarativerelease.HealthProbe, 0, len(release.Health))
	for _, probe := range release.Health {
		if probe.Type == "daemonset" && (probe.Name == transition.WorkerAName || probe.Name == transition.WorkerBName) {
			continue
		}
		probeRelease.Health = append(probeRelease.Health, probe)
	}
	deadline := time.Now().Add(cluster.timeout + healthSoakDuration(release))
	tracker := healthSoakTracker{required: healthSoakDuration(release)}
	var observation declarativerelease.Observation
	var lastErr error
	for {
		observation, err = cluster.observeExpected(ctx, release, target.OCIRevision, manifest)
		if err == nil && observation.Matches(target, release, false) {
			current, _, currentErr := runtime.readCurrentAuthority(ctx)
			state, stateErr := runtime.Snapshot(ctx)
			if currentErr != nil {
				err = currentErr
			} else if stateErr != nil {
				err = stateErr
			} else if err = validateCommittedEdgeGroupState(state, current, transition, target); err == nil {
				err = cluster.committedEdgeNonWorkerResourcesConverged(ctx, release, transition, manifest)
			}
			if err == nil {
				var probeDigest string
				probeDigest, err = cluster.verifyProbes(ctx, probeRelease, target, manifest, observation)
				if err == nil {
					observation.HealthDigest = digestJoin(observation.HealthDigest, probeDigest)
				}
			}
			if err == nil {
				err = cluster.VerifyOwnershipConverged(ctx, release, manifest)
			}
		}
		if err == nil && observation.Matches(target, release, false) {
			if tracker.observe(time.Now(), true) {
				return observation, nil
			}
		} else {
			tracker.observe(time.Now(), false)
			lastErr = err
		}
		if time.Now().After(deadline) {
			if lastErr == nil {
				lastErr = errors.New("committed edge authority did not remain converged")
			}
			return observation, lastErr
		}
		select {
		case <-ctx.Done():
			return observation, waitHealthyTerminalError(ctx.Err(), lastErr)
		case <-time.After(2 * time.Second):
		}
	}
}

func validateCommittedEdgeGroupState(state edgeGroupState, current releaseguardian.CurrentAuthority, transition declarativerelease.EdgeGroupABTransition, target declarativerelease.TargetIdentity) error {
	digest, err := immutableDigestFromRef(target.ImageRef)
	if err != nil {
		return err
	}
	if current.Validate() != nil || current.GroupID != transition.GroupID || current.CurrentWorkerSourceSHA != target.ConfigSHA ||
		current.CurrentWorkerImageDigest != digest || current.CurrentWorkerSlot != releaseguardian.AuthoritySlot(state.ActiveSlot) ||
		current.PreviousRecordDigest == "" || current.PreviousWorkerSlot != releaseguardian.AuthoritySlot(otherEdgeSlot(state.ActiveSlot)) ||
		current.PreviousWorkerSourceSHA == "" || current.PreviousWorkerImageDigest == "" {
		return errors.New("CurrentAuthority does not bind the committed current/previous Edge slots")
	}
	if !edgePodsMatchTarget(state.Front, target) || !edgePodsMatchTarget(edgeWorkerPods(state, state.ActiveSlot), target) {
		return errors.New("committed Edge Front or active Worker differs from CurrentAuthority")
	}
	previousTarget := declarativerelease.TargetIdentity{Present: true, ConfigSHA: current.PreviousWorkerSourceSHA,
		ManifestSHA: current.PreviousWorkerSourceSHA, OCIRevision: current.PreviousWorkerSourceSHA,
		ImageRef: strings.Split(target.ImageRef, "@")[0] + "@" + current.PreviousWorkerImageDigest}
	if !edgePodsMatchTarget(edgeWorkerPods(state, string(current.PreviousWorkerSlot)), previousTarget) {
		return errors.New("previous Edge Worker differs from CurrentAuthority")
	}
	if err := validateEdgeGroupAuthority(state, transition); err != nil {
		return err
	}
	for _, health := range state.FrontHealth {
		if !edgeFrontHealthMatchesServingAuthority(health, current) {
			return errors.New("Edge Front does not serve the exact committed CurrentAuthority")
		}
	}
	return nil
}

func (cluster *kubectlCluster) committedEdgeNonWorkerResourcesConverged(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, manifest []byte) error {
	identities, err := declarativerelease.ResourceSetIdentities(manifest)
	if err != nil {
		return err
	}
	for _, identity := range identities {
		if identity.Kind == "DaemonSet" && (identity.Name == transition.WorkerAName || identity.Name == transition.WorkerBName) {
			continue
		}
		desired, desiredErr := declarativerelease.ResourceSetItem(manifest, identity)
		if desiredErr != nil {
			return desiredErr
		}
		liveRaw, getErr := cluster.getResource(ctx, identity)
		if getErr != nil || resourceAbsent(liveRaw) {
			return fmt.Errorf("read committed resource %s/%s: %w", identity.Kind, identity.Name, getErr)
		}
		live, decodeErr := decodeJSONObject(liveRaw)
		if decodeErr != nil {
			return decodeErr
		}
		if !declarativerelease.ResourceDesiredSubset(desired, live) {
			return fmt.Errorf("committed resource %s/%s has not converged", identity.Kind, identity.Name)
		}
	}
	return nil
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

type edgeGroupABPlan struct {
	before        edgeGroupState
	frontTarget   declarativerelease.TargetIdentity
	activeSlot    string
	activeName    string
	activeTarget  declarativerelease.TargetIdentity
	inactiveSlot  string
	inactiveName  string
	desiredDigest string
}

func prepareEdgeGroupAB(runtime edgeGroupTransitionRuntime, ctx context.Context, transition declarativerelease.EdgeGroupABTransition, target declarativerelease.TargetIdentity) (edgeGroupABPlan, error) {
	before, err := runtime.Snapshot(ctx)
	if err != nil {
		return edgeGroupABPlan{}, fmt.Errorf("capture edge group prewrite state: %w", err)
	}
	frontTarget, err := runtime.DeclaredTarget(transition.FrontName)
	if err != nil {
		return edgeGroupABPlan{}, fmt.Errorf("read declared edge Front target: %w", err)
	}
	activeSlot := before.ActiveSlot
	activeName := edgeWorkerName(transition, activeSlot)
	activeTarget, err := runtime.DeclaredTarget(activeName)
	if err != nil {
		return edgeGroupABPlan{}, fmt.Errorf("read declared active edge Worker target: %w", err)
	}
	desiredDigest, err := immutableDigestFromRef(target.ImageRef)
	if err != nil {
		return edgeGroupABPlan{}, err
	}
	return edgeGroupABPlan{
		before: before, frontTarget: frontTarget, activeSlot: activeSlot, activeName: activeName, activeTarget: activeTarget,
		inactiveSlot: otherEdgeSlot(activeSlot), inactiveName: edgeWorkerName(transition, otherEdgeSlot(activeSlot)), desiredDigest: desiredDigest,
	}, nil
}

func executeEdgeGroupAB(ctx context.Context, runtime edgeGroupTransitionRuntime, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, target declarativerelease.TargetIdentity) error {
	plan, err := prepareEdgeGroupAB(runtime, ctx, transition, target)
	if err != nil {
		return err
	}
	if target.ConfigSHA == release.ExpectedPreviousConfigSHA {
		return executeEdgeGroupLKGRestore(ctx, runtime, transition, target, plan)
	}
	if err := runtime.ApplySharedResources(ctx); err != nil {
		return fmt.Errorf("apply shared edge group resources before candidate staging: %w", err)
	}
	stage, candidatePods, err := stageEdgeGroupCandidate(ctx, runtime, release, target, plan)
	if err != nil {
		return err
	}
	frontPods, frontHealth, err := commitEdgeGroupAuthority(ctx, runtime, release, transition, target, plan, stage)
	if err != nil {
		return err
	}
	serving := edgeGroupState{Front: frontPods, FrontHealth: frontHealth, ActiveSlot: plan.inactiveSlot}
	if plan.inactiveSlot == "a" {
		serving.WorkerA, serving.WorkerB = candidatePods, plan.before.WorkerB
	} else {
		serving.WorkerA, serving.WorkerB = plan.before.WorkerA, candidatePods
	}
	standbyConverged := repairEdgeGroupStandby(ctx, runtime, plan, serving)
	return verifyEdgeGroupTransition(ctx, runtime, transition, target, plan, standbyConverged)
}

func executeEdgeGroupLKGRestore(ctx context.Context, runtime edgeGroupTransitionRuntime, transition declarativerelease.EdgeGroupABTransition, target declarativerelease.TargetIdentity, plan edgeGroupABPlan) error {
	if err := restoreEdgeLKGActivation(ctx, runtime, plan.before, transition, target); err != nil {
		return fmt.Errorf("restore exact edge LKG activation before workloads: %w", err)
	}
	if err := runtime.ApplyCandidateResources(ctx, ""); err != nil {
		return err
	}
	for _, name := range []string{plan.inactiveName, plan.activeName, transition.FrontName} {
		declared, err := runtime.DeclaredTarget(name)
		if err != nil {
			return err
		}
		if _, err := runtime.Roll(ctx, name, declared, name != transition.FrontName, true); err != nil {
			return fmt.Errorf("restore exact edge LKG workload %s: %w", name, err)
		}
	}
	return nil
}

func stageEdgeGroupCandidate(ctx context.Context, runtime edgeGroupTransitionRuntime, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, plan edgeGroupABPlan) (edgeCandidateStageReceipt, map[string]edgeGroupPod, error) {
	stage, err := runtime.StageCandidate(ctx, plan.before, plan.inactiveSlot, target)
	if err != nil {
		return edgeCandidateStageReceipt{}, nil, fmt.Errorf("stage inactive Worker candidate: %w", err)
	}
	if stage.WorkerSlot != plan.inactiveSlot || stage.CurrentWorkerSlot != plan.activeSlot || stage.WorkerSourceSHA != target.ConfigSHA ||
		stage.WorkerImageDigest != plan.desiredDigest || stage.AllowDegradedPrevious != (release.SupersedesFailedConfigSHA != "") || stage.StandbyOnly || stage.OrdinaryTrafficMutation {
		return edgeCandidateStageReceipt{}, nil, errors.New("inactive Worker candidate receipt is invalid")
	}
	if err := runtime.ApplyCandidateResources(ctx, plan.inactiveSlot); err != nil {
		return edgeCandidateStageReceipt{}, nil, err
	}
	// A superseding recovery candidate may prove its immutable bundle before it
	// owns group authority; the current publication can remain degraded here.
	candidatePods, err := runtime.Roll(ctx, plan.inactiveName, target, release.SupersedesFailedConfigSHA == "", release.SupersedesFailedConfigSHA != "")
	if err != nil {
		return edgeCandidateStageReceipt{}, nil, fmt.Errorf("roll inactive edge slot %s: %w", plan.inactiveSlot, err)
	}
	candidatePods, err = runtime.WaitCandidateWorkerAuthority(ctx, plan.inactiveName, target, stage)
	if err != nil {
		return edgeCandidateStageReceipt{}, nil, fmt.Errorf("verify inactive edge Worker candidate authority: %w", err)
	}
	return stage, candidatePods, nil
}

func commitEdgeGroupAuthority(ctx context.Context, runtime edgeGroupTransitionRuntime, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, target declarativerelease.TargetIdentity, plan edgeGroupABPlan, stage edgeCandidateStageReceipt) (map[string]edgeGroupPod, map[string]edgeFrontHealth, error) {
	var frontPods map[string]edgeGroupPod
	frontRecovered := false
	authorityCommitted := false
	if release.SupersedesFailedConfigSHA != "" && edgeFrontNeedsCodeRecovery(plan.before, plan.frontTarget) {
		frontRecoveryErr := runtime.ApplyCandidateResources(ctx, transition.FrontName)
		if frontRecoveryErr == nil {
			frontPods, frontRecoveryErr = runtime.Roll(ctx, transition.FrontName, plan.frontTarget, false, true)
		}
		if frontRecoveryErr != nil {
			// A concurrently committed Guardian transaction is authoritative. Do
			// not compensate it because independent Front maintenance raced SSA.
			if authorityErr := runtime.WaitCurrentAuthority(ctx, stage); authorityErr != nil {
				return nil, nil, fmt.Errorf("recover Front code before Guardian authority: %v; observe exact committed authority: %w", frontRecoveryErr, authorityErr)
			}
			authorityCommitted = true
		} else {
			frontRecovered = true
		}
	}
	// Guardian alone owns Control promotion, Front CAS, current pointer and
	// compensation. The executor observes that transaction before changing code.
	if !authorityCommitted {
		if err := runtime.WaitCurrentAuthority(ctx, stage); err != nil {
			return nil, nil, fmt.Errorf("wait Guardian current authority: %w", err)
		}
	}
	frontHealth, err := runtime.WaitFront(ctx, plan.inactiveSlot, target.ConfigSHA, plan.desiredDigest)
	if err != nil {
		return nil, nil, fmt.Errorf("observe Guardian authority switch: %w", err)
	}
	if err := runtime.ApplyCandidateResources(ctx, transition.FrontName); err != nil {
		return nil, nil, fmt.Errorf("apply Front candidate after Guardian authority switch: %w", err)
	}
	if !frontRecovered {
		frontPods, err = runtime.Roll(ctx, transition.FrontName, plan.frontTarget, true, release.SupersedesFailedConfigSHA != "")
		if err != nil {
			return nil, nil, fmt.Errorf("roll edge front after Guardian authority switch: %w", err)
		}
	}
	if err := runtime.WaitActiveWorkerAuthority(ctx, plan.inactiveName, target); err != nil {
		return nil, nil, fmt.Errorf("verify active edge worker authority: %w", err)
	}
	return frontPods, frontHealth, nil
}

func repairEdgeGroupStandby(ctx context.Context, runtime edgeGroupTransitionRuntime, plan edgeGroupABPlan, serving edgeGroupState) bool {
	// Front and the active Worker are already committed to the new authority at
	// this point.  Standby preparation is maintenance-only: a failed sequence,
	// receipt, or inactive-slot roll must not send the generic executor down its
	// workload LKG rollback path and split traffic authority from serving code.
	// Slot templates can lag a Guardian promotion that committed while this
	// superseding executor was starting. Never rewrite the former active slot
	// from such a stale declaration; preserve the exact runtime that Guardian
	// recorded as PreviousAuthority. Standby maintenance is allowed only when
	// the prewrite active cohort already matched the declared immutable target.
	standbyEligible := edgePodsMatchTarget(edgeWorkerPods(plan.before, plan.activeSlot), plan.activeTarget)
	var standby edgeCandidateStageReceipt
	var standbyErr error
	if standbyEligible {
		standby, standbyErr = runtime.StageStandby(ctx, serving, plan.activeSlot, plan.activeTarget)
	}
	standbyConverged := false
	if standbyEligible && standbyErr == nil {
		previousDigest, digestErr := immutableDigestFromRef(plan.activeTarget.ImageRef)
		receiptValid := digestErr == nil && standby.WorkerSlot == plan.activeSlot && standby.CurrentWorkerSlot == plan.inactiveSlot &&
			standby.WorkerSourceSHA == plan.activeTarget.ConfigSHA && standby.WorkerImageDigest == previousDigest &&
			!standby.AllowDegradedPrevious && standby.StandbyOnly && !standby.OrdinaryTrafficMutation
		if receiptValid && runtime.ApplyCandidateResources(ctx, plan.activeName) == nil {
			_, standbyErr = runtime.Roll(ctx, plan.activeName, plan.activeTarget, true, true)
			standbyConverged = standbyErr == nil
		}
	}
	return standbyConverged
}

func verifyEdgeGroupTransition(ctx context.Context, runtime edgeGroupTransitionRuntime, transition declarativerelease.EdgeGroupABTransition, target declarativerelease.TargetIdentity, plan edgeGroupABPlan, standbyConverged bool) error {
	final, err := runtime.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("capture final edge group state: %w", err)
	}
	if !edgePodsMatchTarget(final.Front, plan.frontTarget) || !edgePodsMatchTarget(edgeWorkerPods(final, plan.inactiveSlot), target) {
		return errors.New("edge group did not converge candidate current authority")
	}
	if standbyConverged && !edgePodsMatchTarget(edgeWorkerPods(final, plan.activeSlot), plan.activeTarget) {
		return errors.New("edge group did not converge candidate current and previous LKG")
	}
	if standbyConverged {
		if err := validateEdgeGroupAuthority(final, transition); err != nil {
			return err
		}
	} else if err := validateActiveEdgeGroupAuthority(final, transition); err != nil {
		return err
	}
	if final.ActiveSlot != plan.inactiveSlot {
		return errors.New("edge group activation did not converge to the promoted slot")
	}
	return nil
}

// restoreEdgeLKGActivation switches traffic to the exact predecessor Worker
// while that immutable standby is still present. The generic LKG apply rolls
// code after this boundary, so it must not be allowed to leave a newer durable
// activation pointer selecting code that rollback has already replaced.
func restoreEdgeLKGActivation(ctx context.Context, runtime edgeGroupTransitionRuntime, before edgeGroupState, transition declarativerelease.EdgeGroupABTransition, target declarativerelease.TargetIdentity) error {
	if runtime == nil || (before.ActiveSlot != "a" && before.ActiveSlot != "b") {
		return errors.New("rollback activation state is unavailable")
	}
	desiredDigest, err := immutableDigestFromRef(target.ImageRef)
	if err != nil {
		return err
	}
	allPods := make([]edgeGroupPod, 0, len(before.WorkerA)+len(before.WorkerB))
	for _, pods := range []map[string]edgeGroupPod{before.WorkerA, before.WorkerB} {
		for _, pod := range pods {
			allPods = append(allPods, pod)
		}
	}
	executor, err := runtime.SelectCASExecutor(ctx, allPods...)
	if err != nil {
		return err
	}
	current, exists, err := runtime.ReadActivation(ctx, executor)
	if err != nil {
		return err
	}
	if !exists || current.GroupID != transition.GroupID || current.Authority != edgeActivationAuthority ||
		(current.ActiveSlot != "a" && current.ActiveSlot != "b") || current.Generation == 0 {
		return errors.New("current rollback activation identity is invalid")
	}
	currentPods := edgeWorkerPods(before, current.ActiveSlot)
	if current.WorkerSourceCommit == target.ConfigSHA && current.WorkerImageDigest == desiredDigest {
		bundle, bundleErr := exactEdgeWorkerBundle(currentPods, target)
		if bundleErr != nil || bundle != current.BundleGeneration {
			return errors.New("current activation claims the LKG without exact Worker evidence")
		}
		return nil
	}

	targetSlot := ""
	targetBundle := ""
	for _, slot := range []string{"a", "b"} {
		bundle, bundleErr := exactEdgeWorkerBundle(edgeWorkerPods(before, slot), target)
		if bundleErr != nil {
			continue
		}
		if targetSlot != "" {
			return errors.New("rollback LKG Worker slot is ambiguous")
		}
		targetSlot, targetBundle = slot, bundle
	}
	if targetSlot == "" {
		return errors.New("rollback LKG Worker standby is unavailable")
	}
	request := edgeActivationRequest{
		GroupID: transition.GroupID, ExpectedGeneration: current.Generation, ExpectedSlot: current.ActiveSlot,
		TargetSlot: targetSlot, BundleGeneration: targetBundle, WorkerSourceCommit: target.ConfigSHA,
		WorkerImageDigest: desiredDigest, Operation: edgeActivationRollback, RollbackOfGeneration: current.Generation,
		Reason: "restore exact edge LKG before workload rollback",
	}
	if _, err := runtime.ActivationCAS(ctx, executor, request); err != nil {
		return err
	}
	if _, err := runtime.WaitFront(ctx, targetSlot, target.ConfigSHA, desiredDigest); err != nil {
		return fmt.Errorf("wait Front on restored LKG slot: %w", err)
	}
	return nil
}

func exactEdgeWorkerBundle(pods map[string]edgeGroupPod, target declarativerelease.TargetIdentity) (string, error) {
	if len(pods) == 0 {
		return "", errors.New("edge Worker evidence is absent")
	}
	bundle := ""
	for _, pod := range pods {
		if !edgePodMatchesTarget(pod, target) || !edgePodHasGroupAuthority(pod) || strings.TrimSpace(pod.BundleGeneration) == "" {
			return "", errors.New("edge Worker does not prove the exact LKG bundle")
		}
		if bundle == "" {
			bundle = pod.BundleGeneration
		} else if bundle != pod.BundleGeneration {
			return "", errors.New("edge Worker LKG bundle evidence diverged")
		}
	}
	return bundle, nil
}

func edgeFrontNeedsCodeRecovery(state edgeGroupState, target declarativerelease.TargetIdentity) bool {
	for _, pod := range state.Front {
		if !edgePodMatchesTarget(pod, target) {
			return true
		}
	}
	return false
}

func (runtime *kubectlEdgeGroupRuntime) Snapshot(ctx context.Context) (edgeGroupState, error) {
	return runtime.cluster.readEdgeGroupState(ctx, runtime.release, runtime.transition)
}

func (runtime *kubectlEdgeGroupRuntime) ApplySharedResources(ctx context.Context) error {
	return runtime.cluster.applyEdgeSharedResources(ctx, runtime.release, runtime.transition, runtime.manifest)
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
	if status.BundleGeneration == targetBase && status.PublicationDecision == "published" &&
		status.LKGState == "current" && status.Ready && status.ServingHealthy {
		// Edge Control already points at a newer publication of the exact
		// immutable LKG family and has proved it healthy. Re-signing it again
		// only extends the durable ledger write and can exhaust the Guardian
		// recovery deadline. A failed/unhealthy publication with the same base
		// bundle must still take the controlled recovery path.
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
	servingAuthority, err := runtime.readServingAuthorityWitness(ctx, before, status)
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

func (runtime *kubectlEdgeGroupRuntime) readServingAuthorityWitness(ctx context.Context, before edgeGroupState, status edgeCandidateStageStatus) (*edgeServingAuthorityWitness, error) {
	current, object, err := runtime.readCurrentAuthority(ctx)
	if err != nil || object == nil {
		return nil, err
	}
	witness, err := edgeServingAuthorityWitnessFromCurrentWithRecoveryAuthorities(before, current, runtime.transition.GroupID, string(object.GetUID()), object.GetResourceVersion(), runtime.release.SupersedesFailedConfigSHA != "", runtime.release.ExpectedPreviousConfigSHA, runtime.release.ExpectedPreviousImageDigest, runtime.release.SupersedesFailedConfigSHA)
	if err != nil || witness == nil {
		return witness, err
	}
	return edgeServingAuthorityWitnessWithCurrentPublication(before, witness, status, time.Now().UTC())
}

func edgeServingAuthorityWitnessWithCurrentPublication(before edgeGroupState, witness *edgeServingAuthorityWitness, status edgeCandidateStageStatus, now time.Time) (*edgeServingAuthorityWitness, error) {
	if witness == nil || !status.Ready || !status.ServingHealthy || status.PublicationDecision != "published" || status.LKGState != "current" ||
		status.BundleGeneration == "" || status.CurrentPublicationSequence == 0 || status.PublishedBundleDigest == "" {
		return witness, nil
	}
	workers := edgeWorkerPods(before, witness.WorkerSlot)
	if len(workers) == 0 || len(workers) != len(before.Front) || !sameEdgeNodes(workers, before.Front) {
		return witness, nil
	}
	servingVersion := ""
	for _, worker := range workers {
		digest, err := immutableDigestFromRef(worker.ImageRef)
		generation, publication, recovery, versionOK := parseEdgePublicationVersion(worker.BundleGeneration)
		if err != nil || !worker.Ready || worker.RestartCount != 0 || worker.SourceCommit != witness.WorkerSourceSHA ||
			digest != witness.WorkerImageDigest || !edgePodHasGroupAuthority(worker) || !edgePodHasActiveInventoryAt(worker, now) ||
			!versionOK || generation != status.BundleGeneration || worker.ServingGeneration != generation ||
			worker.PublicationSequence != publication || publication > status.CurrentPublicationSequence || recovery > status.RecoveryEpoch {
			return witness, nil
		}
		if servingVersion == "" {
			servingVersion = worker.BundleGeneration
		} else if servingVersion != worker.BundleGeneration {
			return nil, errors.New("active Edge Worker cohort disagrees on serving publication")
		}
	}
	updated := *witness
	updated.BundleVersion = servingVersion
	return &updated, nil
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
	return edgeServingAuthorityWitnessFromCurrentWithRecoveryAuthorities(before, current, groupID, uid, resourceVersion, allowDegradedRecovery,
		expectedLKGSourceSHA, expectedLKGImageDigest, "")
}

func edgeServingAuthorityWitnessFromCurrentWithRecoveryAuthorities(before edgeGroupState, current releaseguardian.CurrentAuthority, groupID, uid, resourceVersion string, allowDegradedRecovery bool, expectedLKGSourceSHA, expectedLKGImageDigest, supersededSourceSHA string) (*edgeServingAuthorityWitness, error) {
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
			if allowDegradedRecovery && edgeGroupStateMatchesSupersededPreviousAuthority(before, current, health, supersededSourceSHA, time.Now().UTC()) {
				return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
			}
			if allowDegradedRecovery && edgeFrontHealthMatchesExpectedLKG(health, current, expectedLKGSourceSHA, expectedLKGImageDigest) {
				return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
			}
			if allowDegradedRecovery && edgeGroupStateMatchesExplicitServingDrift(before, current, health, time.Now().UTC()) {
				return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
			}
			if !allowDegradedRecovery || !edgeFrontHealthMatchesDegradedServingAuthority(health, current) {
				return nil, fmt.Errorf("Guardian current authority does not match the serving Front slot: %s expected_lkg=%s",
					edgeDegradedServingAuthorityMismatch(health, current, allowDegradedRecovery), edgeExpectedLKGMismatch(health, current, expectedLKGSourceSHA, expectedLKGImageDigest))
			}
			return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
		}
		if !edgeFrontHealthMatchesServingAuthority(health, current) {
			if allowDegradedRecovery && edgeGroupStateMatchesSupersededPreviousAuthority(before, current, health, supersededSourceSHA, time.Now().UTC()) {
				return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
			}
			if allowDegradedRecovery && edgeFrontHealthMatchesExpectedLKG(health, current, expectedLKGSourceSHA, expectedLKGImageDigest) {
				return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
			}
			if allowDegradedRecovery && edgeGroupStateMatchesExplicitServingDrift(before, current, health, time.Now().UTC()) {
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
			if allowDegradedRecovery && edgeGroupStateMatchesSupersededPreviousAuthority(before, current, health, supersededSourceSHA, time.Now().UTC()) {
				return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
			}
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
			if allowDegradedRecovery && edgeGroupStateMatchesSupersededPreviousAuthority(before, current, health, supersededSourceSHA, time.Now().UTC()) {
				return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
			}
			if allowDegradedRecovery && edgeFrontHealthMatchesExpectedLKG(health, current, expectedLKGSourceSHA, expectedLKGImageDigest) {
				return edgeServingAuthorityWitnessFromFrontHealth(current, uid, resourceVersion, health), nil
			}
			if allowDegradedRecovery && edgeGroupStateMatchesExplicitServingDrift(before, current, health, time.Now().UTC()) {
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

// A superseding repair may observe Front briefly serving Guardian's previous
// authority while the current pointer already reflects the compensating LKG.
// Admit that state only with exact, live Worker cohort proof.
func edgeGroupStateMatchesSupersededPreviousAuthority(before edgeGroupState, current releaseguardian.CurrentAuthority, health edgeFrontHealth, supersededSourceSHA string, now time.Time) bool {
	if !edgeSourceSHAPattern.MatchString(supersededSourceSHA) || current.PreviousWorkerSlot.Validate() != nil ||
		current.PreviousWorkerSlot == current.CurrentWorkerSlot || current.PreviousWorkerSourceSHA != supersededSourceSHA ||
		!edgePromotionDigestPattern.MatchString(current.PreviousWorkerImageDigest) || current.PreviousBundleGeneration == "" ||
		!health.ActivationPresent || before.ActiveSlot != health.ActiveSlot || health.ActiveSlot != string(current.PreviousWorkerSlot) ||
		health.RouteAuthority != edgeActivationAuthority || health.Generation < current.PreviousFrontGeneration ||
		health.WorkerSourceCommit != current.PreviousWorkerSourceSHA || health.WorkerImageDigest != current.PreviousWorkerImageDigest {
		return false
	}
	previousFamily, _, _, previousOK := parseEdgePublicationVersion(current.PreviousBundleGeneration)
	healthFamily, _, _, healthOK := parseEdgePublicationVersion(health.BundleGeneration)
	if !previousOK || !healthOK || previousFamily != healthFamily {
		return false
	}
	workers := edgeWorkerPods(before, health.ActiveSlot)
	if len(workers) == 0 || len(workers) != len(before.Front) || !sameEdgeNodes(workers, before.Front) {
		return false
	}
	for _, worker := range workers {
		digest, err := immutableDigestFromRef(worker.ImageRef)
		workerFamily, workerPublication, _, workerOK := parseEdgePublicationVersion(worker.BundleGeneration)
		if err != nil || !worker.Ready || worker.RestartCount != 0 || worker.SourceCommit != supersededSourceSHA ||
			digest != current.PreviousWorkerImageDigest || !edgePodHasGroupAuthority(worker) || !edgePodHasActiveInventoryAt(worker, now) ||
			!workerOK || workerFamily != healthFamily || worker.ServingGeneration != workerFamily || worker.PublicationSequence != workerPublication {
			return false
		}
	}
	return true
}

// edgeGroupStateMatchesExplicitServingDrift admits a reviewed superseding
// recovery after an emergency Edge A/B repair advanced Front beyond Guardian.
// The activation file alone is insufficient: the exact active Worker cohort
// must be healthy, serve signed Edge Control authority, and own a fresh
// inventory heartbeat on the same node set.
func edgeGroupStateMatchesExplicitServingDrift(before edgeGroupState, current releaseguardian.CurrentAuthority, health edgeFrontHealth, now time.Time) bool {
	if !health.ActivationPresent || health.RouteAuthority != edgeActivationAuthority || before.ActiveSlot != health.ActiveSlot ||
		(health.ActiveSlot != "a" && health.ActiveSlot != "b") || health.Generation <= current.CurrentFrontGeneration ||
		!edgeSourceSHAPattern.MatchString(health.WorkerSourceCommit) || !edgePromotionDigestPattern.MatchString(health.WorkerImageDigest) {
		return false
	}
	delta := health.Generation - current.CurrentFrontGeneration
	if (health.ActiveSlot != string(current.CurrentWorkerSlot)) != (delta%2 == 1) {
		return false
	}
	_, activationPublication, activationRecovery, activationOK := parseEdgePublicationVersion(health.BundleGeneration)
	if !activationOK {
		return false
	}
	workers := edgeWorkerPods(before, health.ActiveSlot)
	if len(workers) == 0 || len(workers) != len(before.Front) || !sameEdgeNodes(workers, before.Front) {
		return false
	}
	for _, worker := range workers {
		digest, err := immutableDigestFromRef(worker.ImageRef)
		generation, publication, recovery, versionOK := parseEdgePublicationVersion(worker.BundleGeneration)
		if err != nil || !worker.Ready || worker.RestartCount != 0 || worker.SourceCommit != health.WorkerSourceCommit ||
			digest != health.WorkerImageDigest || !edgePodHasGroupAuthority(worker) || !edgePodHasActiveInventoryAt(worker, now) ||
			!versionOK || worker.ServingGeneration != generation || worker.PublicationSequence != publication ||
			publication < activationPublication || recovery < activationRecovery {
			return false
		}
	}
	return true
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

func (cluster *kubectlCluster) applyEdgeSharedResources(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, manifest []byte) error {
	identities, err := edgeSharedResourceIdentities(manifest, transition)
	if err != nil {
		return err
	}
	for _, identity := range identities {
		desired, desiredErr := declarativerelease.ResourceSetItem(manifest, identity)
		if desiredErr != nil {
			return desiredErr
		}
		encoded, encodeErr := declarativerelease.CanonicalJSON(desired)
		if encodeErr != nil {
			return encodeErr
		}
		if applyErr := cluster.applyResourceWithOwnershipConvergence(ctx, release, identity, desired, encoded, false); applyErr != nil {
			return fmt.Errorf("apply shared edge resource %s/%s: %w", identity.Kind, identity.Name, applyErr)
		}
	}
	return nil
}

func edgeSharedResourceIdentities(manifest []byte, transition declarativerelease.EdgeGroupABTransition) ([]declarativerelease.ResourceIdentity, error) {
	identities, err := declarativerelease.ResourceSetIdentities(manifest)
	if err != nil {
		return nil, err
	}
	workloads := map[string]struct{}{
		transition.FrontName:   {},
		transition.WorkerAName: {},
		transition.WorkerBName: {},
	}
	shared := make([]declarativerelease.ResourceIdentity, 0, len(identities))
	for _, identity := range identities {
		if identity.APIVersion == "apps/v1" && identity.Kind == "DaemonSet" {
			if _, exists := workloads[identity.Name]; exists {
				continue
			}
			return nil, fmt.Errorf("edge group resource set contains undeclared DaemonSet/%s", identity.Name)
		}
		shared = append(shared, identity)
	}
	return shared, nil
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

func (runtime *kubectlEdgeGroupRuntime) WaitCurrentAuthority(ctx context.Context, staged edgeCandidateStageReceipt) error {
	deadline := time.Now().Add(runtime.cluster.timeout)
	for {
		current, _, err := runtime.readCurrentAuthority(ctx)
		if err == nil && edgeCurrentAuthorityMatchesCandidate(current, staged) {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("Guardian CurrentAuthority did not converge to the staged candidate")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func edgeCurrentAuthorityMatchesCandidate(current releaseguardian.CurrentAuthority, staged edgeCandidateStageReceipt) bool {
	if current.Validate() != nil || current.GroupID != staged.GroupID || current.CurrentRecordDigest != staged.CandidateRecordDigest ||
		string(current.CurrentWorkerSlot) != staged.WorkerSlot || current.CurrentWorkerSourceSHA != staged.WorkerSourceSHA ||
		current.CurrentWorkerImageDigest != staged.WorkerImageDigest || current.PreviousWorkerSlot == current.CurrentWorkerSlot {
		return false
	}
	generation, _, _, ok := parseEdgePublicationVersion(current.CurrentBundleGeneration)
	return ok && generation == staged.CandidateBundleGeneration
}

func (runtime *kubectlEdgeGroupRuntime) WaitCandidateWorkerAuthority(ctx context.Context, name string, target declarativerelease.TargetIdentity, stage edgeCandidateStageReceipt) (map[string]edgeGroupPod, error) {
	return runtime.cluster.waitEdgeCandidateWorkerAuthority(ctx, runtime.release, runtime.transition, name, target, stage)
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
	pods, err := parseEdgeGroupPodsWithReadiness(podsRaw, container, expectedNodes, groupID, requireReady)
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
			pod.CandidateBundleLoaded = health.CandidateBundleLoaded
			pod.CandidateRecordDigest = health.CandidateRecordDigest
			pod.CandidateReleaseRecordDigest = health.CandidateReleaseRecordDigest
			pod.CandidateWorkerSlot = health.CandidateWorkerSlot
			pods[node] = pod
		}
	}
	return pods, nil
}

func edgeHealthPortNames() []string {
	return []string{"health"}
}

func parseEdgeGroupPodsWithReadiness(raw []byte, container string, expectedNodes int, groupID string, requireReady bool) (map[string]edgeGroupPod, error) {
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
		if podGroupID != groupID {
			return nil, errors.New("pod edge group identity mismatch")
		}
		spec := mapField(item, "spec")
		node := stringValue(spec["nodeName"])
		pod := edgeGroupPod{Name: stringValue(metadata["name"]), UID: stringValue(metadata["uid"]), ResourceVersion: stringValue(metadata["resourceVersion"]), NodeName: node,
			SourceCommit: mapStringField(metadata, "annotations")["fugue.pro/source-commit"]}
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
		BundleGeneration:             strings.TrimSpace(stringValue(value["bundle_version"])),
		RouteBundleSource:            strings.TrimSpace(stringValue(value["route_bundle_source"])),
		ServingGeneration:            strings.TrimSpace(stringValue(value["serving_generation"])),
		InventoryProducerActive:      value["inventory_producer_active"] == true,
		InventoryHeartbeatError:      strings.TrimSpace(stringValue(value["inventory_heartbeat_error"])),
		CandidateBundleLoaded:        value["candidate_bundle_loaded"] == true,
		CandidateRecordDigest:        strings.TrimSpace(stringValue(value["candidate_record_digest"])),
		CandidateReleaseRecordDigest: strings.TrimSpace(stringValue(value["candidate_release_record_digest"])),
		CandidateWorkerSlot:          strings.TrimSpace(stringValue(value["candidate_worker_slot"])),
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

// The stage receipt names the base generation; the candidate record digest
// binds the exact signed .pN.rM publication reported in BundleGeneration.
func edgePodHasCandidateAuthority(pod edgeGroupPod, stage edgeCandidateStageReceipt) bool {
	return edgePodHasGroupAuthority(pod) && pod.CandidateBundleLoaded && pod.ServingGeneration == stage.CandidateBundleGeneration &&
		pod.CandidateRecordDigest == stage.CandidateRecordDigest && pod.CandidateReleaseRecordDigest == stage.ReleaseRecordDigest &&
		pod.CandidateWorkerSlot == stage.WorkerSlot
}

func edgePodHasActiveInventory(pod edgeGroupPod) bool {
	return edgePodHasActiveInventoryAt(pod, time.Now().UTC())
}

func edgePodHasActiveInventoryAt(pod edgeGroupPod, now time.Time) bool {
	if !edgePodHasGroupAuthority(pod) || !pod.InventoryProducerActive || pod.InventoryHeartbeatGeneration == 0 || pod.InventoryHeartbeatAt.IsZero() {
		return false
	}
	heartbeatAt := pod.InventoryHeartbeatAt.UTC()
	now = now.UTC()
	return !heartbeatAt.After(now.Add(edgeInventoryHeartbeatClockSkew)) && now.Sub(heartbeatAt) <= edgeInventoryHeartbeatMaxAge
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
			return fmt.Errorf("%w: edge group active slot %s node %s has no verified inventory heartbeat", errEdgeInventoryHeartbeatUnavailable, state.ActiveSlot, node)
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

func (cluster *kubectlCluster) waitEdgeCandidateWorkerAuthority(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, name string, target declarativerelease.TargetIdentity, stage edgeCandidateStageReceipt) (map[string]edgeGroupPod, error) {
	if stage.WorkerSlot != "a" && stage.WorkerSlot != "b" || !edgePromotionDigestPattern.MatchString(stage.CandidateRecordDigest) ||
		!edgePromotionDigestPattern.MatchString(stage.ReleaseRecordDigest) || strings.TrimSpace(stage.CandidateBundleGeneration) == "" {
		return nil, errors.New("staged candidate authority witness is incomplete")
	}
	deadline := time.Now().Add(cluster.timeout)
	for {
		pods, err := cluster.readEdgeDaemonSetPods(ctx, release, name, transition.WorkerContainer, transition.ExpectedNodes, transition.GroupID, true)
		if err == nil {
			converged := len(pods) == transition.ExpectedNodes
			for _, pod := range pods {
				converged = converged && edgePodMatchesTarget(pod, target) && edgePodHasCandidateAuthority(pod, stage)
			}
			if converged {
				return pods, nil
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("DaemonSet/%s did not load the exact staged candidate authority", name)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
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
