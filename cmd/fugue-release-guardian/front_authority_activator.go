package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

const (
	frontActivationStatePath     = "/var/lib/fugue-edge-front/activation.json"
	frontActivationCASBinary     = "/usr/local/bin/fugue-edge-front-cas"
	frontHealthPort              = 7831
	authorityLeaseSeconds        = int32(120)
	postActivationRouteAttempts  = 12
	postActivationRouteSuccesses = 3
	postActivationRouteInterval  = time.Second
	postActivationRouteTimeout   = 20 * time.Second
)

var errFrontCompensationUnknown = errors.New("Front compensation is unknown")

type frontAuthorityConfig struct {
	GroupID         string
	Namespace       string
	ExpectedNodes   int
	RouteAddress    string
	RouteHost       string
	RoutePath       string
	RouteBodyDigest string
}

type podCommandExecutor interface {
	Exec(context.Context, string, string, string, ...string) ([]byte, error)
}

type frontAuthorityActivator struct {
	client   kubernetes.Interface
	executor podCommandExecutor
	config   frontAuthorityConfig
	holder   string
	now      func() time.Time
}

type heldAuthorityLease struct {
	client    kubernetes.Interface
	namespace string
	name      string
	uid       string
	rv        string
	holder    string
}

type frontAuthorityTransaction struct {
	activator *frontAuthorityActivator
	lease     *heldAuthorityLease
	receipt   releaseguardian.FrontAuthorityReceipt
	closed    bool
}

type kubePodExecutor struct {
	config *rest.Config
	client kubernetes.Interface
}

func (executor *kubePodExecutor) Exec(ctx context.Context, namespace, pod, container string, command ...string) ([]byte, error) {
	if executor == nil || executor.config == nil || executor.client == nil || namespace == "" || pod == "" || container == "" || len(command) == 0 {
		return nil, errors.New("pod exec request is invalid")
	}
	request := executor.client.CoreV1().RESTClient().Post().Resource("pods").Name(pod).Namespace(namespace).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{Container: container, Command: command, Stdout: true, Stderr: true}, scheme.ParameterCodec)
	stream, err := remotecommand.NewSPDYExecutor(executor.config, http.MethodPost, request.URL())
	if err != nil {
		return nil, err
	}
	var stdout, stderr bytes.Buffer
	if err := stream.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
		message := strings.TrimSpace(stderr.String())
		if len(message) > 256 {
			message = message[:256]
		}
		return nil, fmt.Errorf("pod exec failed: %w: %s", err, message)
	}
	if stdout.Len() == 0 || stdout.Len() > 64<<10 || stderr.Len() > 4<<10 {
		return nil, errors.New("pod exec output is invalid")
	}
	return stdout.Bytes(), nil
}

func newFrontAuthorityActivator(client kubernetes.Interface, executor podCommandExecutor, config frontAuthorityConfig, holder string) (*frontAuthorityActivator, error) {
	if client == nil || executor == nil || (releaseguardian.Key{Component: "edge", Group: config.GroupID}).Validate() != nil ||
		(releaseguardian.Key{Component: config.Namespace}).Validate() != nil || config.ExpectedNodes < 1 || config.ExpectedNodes > 100 ||
		len(holder) < 8 || len(holder) > 128 || strings.ContainsAny(holder, "\r\n\t ") {
		return nil, errors.New("Front authority activator configuration is invalid")
	}
	if config.RouteAddress != "" && (strings.TrimSpace(config.RouteHost) == "" || !strings.HasPrefix(config.RoutePath, "/") || !exactSHA256Digest(config.RouteBodyDigest)) {
		return nil, errors.New("Front authority route canary configuration is invalid")
	}
	return &frontAuthorityActivator{client: client, executor: executor, config: config, holder: holder, now: time.Now}, nil
}

func (activator *frontAuthorityActivator) BeginPromote(ctx context.Context, target releaseguardian.FrontAuthorityTarget) (releaseguardian.FrontAuthorityTransaction, error) {
	if target.GroupID != activator.config.GroupID || target.TargetSlot.Validate() != nil || !exactSourceSHA(target.WorkerSourceSHA) ||
		!exactSHA256Digest(target.WorkerImageDigest) || !exactSHA256Digest(target.WorkerCohortDigest) ||
		!exactSHA256Digest(target.CandidateRecordDigest) || !exactSHA256Digest(target.CanaryResultDigest) ||
		strings.TrimSpace(target.CandidateBundleGeneration) == "" || strings.TrimSpace(target.ServingGeneration) == "" ||
		strings.TrimSpace(target.FrontBundleGeneration) == "" {
		return nil, errors.New("Front authority target is invalid")
	}
	return activator.begin(ctx, target)
}

func (activator *frontAuthorityActivator) BeginRestore(ctx context.Context, current releaseguardian.CurrentAuthority) (releaseguardian.FrontAuthorityTransaction, error) {
	if current.Validate() != nil || current.GroupID != activator.config.GroupID || current.PreviousFrontGeneration == 0 ||
		current.CurrentFrontGeneration == 0 || strings.TrimSpace(current.CurrentBundleGeneration) == "" ||
		strings.TrimSpace(current.PreviousBundleGeneration) == "" || !exactSourceSHA(current.CurrentWorkerSourceSHA) ||
		!exactSourceSHA(current.PreviousWorkerSourceSHA) || !exactSHA256Digest(current.CurrentWorkerImageDigest) ||
		!exactSHA256Digest(current.PreviousWorkerImageDigest) || !exactSHA256Digest(current.CurrentRecordDigest) ||
		!exactSHA256Digest(current.PreviousRecordDigest) {
		return nil, errors.New("Front authority restore target is invalid")
	}
	target := releaseguardian.FrontAuthorityTarget{GroupID: current.GroupID, TargetSlot: current.PreviousWorkerSlot,
		CandidateBundleGeneration: current.PreviousBundleGeneration, ServingGeneration: current.PreviousBundleGeneration,
		FrontBundleGeneration: current.PreviousBundleGeneration, WorkerSourceSHA: current.PreviousWorkerSourceSHA,
		WorkerImageDigest: current.PreviousWorkerImageDigest, CandidateRecordDigest: current.PreviousRecordDigest,
		CanaryResultDigest: current.CurrentRecordDigest, PreviousSlot: current.CurrentWorkerSlot,
		PreviousFrontGeneration: current.CurrentFrontGeneration, PreviousBundleGeneration: current.CurrentBundleGeneration,
		PreviousWorkerSourceSHA: current.CurrentWorkerSourceSHA, PreviousWorkerImageDigest: current.CurrentWorkerImageDigest}
	return activator.beginWithOperation(ctx, target, edgegroupfront.ActivationOperationRollback,
		current.CurrentFrontGeneration, "restore Guardian group authority LKG")
}

func (activator *frontAuthorityActivator) begin(ctx context.Context, target releaseguardian.FrontAuthorityTarget) (releaseguardian.FrontAuthorityTransaction, error) {
	return activator.beginWithOperation(ctx, target, edgegroupfront.ActivationOperationPromote, 0, "promote Guardian verified candidate authority")
}

func (activator *frontAuthorityActivator) beginWithOperation(ctx context.Context, target releaseguardian.FrontAuthorityTarget, operation string, rollbackOf uint64, reason string) (releaseguardian.FrontAuthorityTransaction, error) {
	lease, err := activator.acquireLease(ctx)
	if err != nil {
		return nil, err
	}
	releaseOnError := true
	defer func() {
		if releaseOnError {
			_ = lease.release(context.WithoutCancel(ctx))
		}
	}()
	preflight, err := activator.preflightForOperation(ctx, target, operation)
	if err != nil {
		return nil, err
	}
	transaction, err := activator.applyWithLease(ctx, target, lease, preflight, operation, rollbackOf, reason)
	if err != nil {
		if errors.Is(err, errFrontCompensationUnknown) {
			releaseOnError = false
		}
		return nil, err
	}
	releaseOnError = false
	return transaction, nil
}

type frontAuthorityPreflight struct {
	workers            map[string]corev1.Pod
	states             map[string]edgegroupfront.ActivationState
	previousGeneration uint64
	alreadyAtNew       bool
}

func (activator *frontAuthorityActivator) preflight(ctx context.Context, target releaseguardian.FrontAuthorityTarget) (frontAuthorityPreflight, error) {
	return activator.preflightForOperation(ctx, target, edgegroupfront.ActivationOperationPromote)
}

func (activator *frontAuthorityActivator) preflightForOperation(ctx context.Context, target releaseguardian.FrontAuthorityTarget, operation string) (frontAuthorityPreflight, error) {
	if operation != edgegroupfront.ActivationOperationPromote && operation != edgegroupfront.ActivationOperationRollback {
		return frontAuthorityPreflight{}, errors.New("Front authority preflight operation is invalid")
	}
	workers, cohort, err := activator.observeWorkers(ctx, target)
	if err != nil {
		return frontAuthorityPreflight{}, err
	}
	if target.WorkerCohortDigest != "" && cohort.CohortDigest != target.WorkerCohortDigest {
		return frontAuthorityPreflight{}, errors.New("candidate worker cohort changed before Front CAS")
	}
	fronts, err := activator.observeFronts(ctx)
	if err != nil {
		return frontAuthorityPreflight{}, err
	}
	states := make(map[string]edgegroupfront.ActivationState, len(workers))
	alreadyAtNew := true
	previousGeneration := uint64(0)
	for node, worker := range workers {
		state, readErr := activator.readActivation(ctx, worker.Name)
		if readErr != nil || state.GroupID != target.GroupID {
			return frontAuthorityPreflight{}, errors.New("Front activation prewrite state is invalid")
		}
		front, exists := fronts[node]
		if !exists || front.ActiveSlot != state.ActiveSlot || front.Generation != state.Generation || front.BundleGeneration != state.BundleGeneration ||
			front.WorkerSourceCommit != state.WorkerSourceCommit || front.WorkerImageDigest != state.WorkerImageDigest {
			return frontAuthorityPreflight{}, errors.New("Front readiness does not match activation prewrite state")
		}
		if state.ActiveSlot == string(target.TargetSlot) {
			if !frontTargetGenerationMatches(state, target.PreviousFrontGeneration, operation) || state.PreviousSlot != string(target.PreviousSlot) ||
				state.BundleGeneration != target.FrontBundleGeneration || state.WorkerSourceCommit != target.WorkerSourceSHA ||
				state.WorkerImageDigest != target.WorkerImageDigest || state.Operation != operation {
				return frontAuthorityPreflight{}, errors.New("Front activation replay state is not target-bound")
			}
			candidatePrevious := state.Generation - 1
			if previousGeneration == 0 {
				previousGeneration = candidatePrevious
			} else if previousGeneration != candidatePrevious {
				return frontAuthorityPreflight{}, errors.New("Front activation replay generations are mixed")
			}
		} else {
			alreadyAtNew = false
			if state.ActiveSlot != string(target.PreviousSlot) || !frontLKGGenerationMatches(state, target.PreviousFrontGeneration, operation) ||
				state.BundleGeneration != target.PreviousBundleGeneration || state.WorkerSourceCommit != target.PreviousWorkerSourceSHA ||
				state.WorkerImageDigest != target.PreviousWorkerImageDigest {
				return frontAuthorityPreflight{}, errors.New("Front activation LKG is not target-bound")
			}
			if previousGeneration == 0 {
				previousGeneration = state.Generation
			} else if previousGeneration != state.Generation {
				return frontAuthorityPreflight{}, errors.New("Front activation LKG generations are mixed")
			}
		}
		states[node] = state
	}
	return frontAuthorityPreflight{workers: workers, states: states, previousGeneration: previousGeneration, alreadyAtNew: alreadyAtNew}, nil
}

func frontLKGGenerationMatches(state edgegroupfront.ActivationState, expected uint64, operation string) bool {
	if state.Generation == expected {
		return true
	}
	// Every failed promotion advances Front twice: candidate N+1 followed by
	// its exact rollback N+2. A durable prepared journal can survive more than
	// one such compensated attempt, so accept the latest even generation only
	// when it is itself an exact rollback of its immediately preceding write.
	// The caller separately binds slot, bundle, source and image to the LKG.
	return (operation == edgegroupfront.ActivationOperationPromote || operation == edgegroupfront.ActivationOperationRollback) &&
		state.Operation == edgegroupfront.ActivationOperationRollback &&
		state.Generation > expected && (state.Generation-expected)%2 == 0 && state.RollbackOfGeneration == state.Generation-1
}

func frontTargetGenerationMatches(state edgegroupfront.ActivationState, expected uint64, operation string) bool {
	if state.Generation == expected+1 {
		return true
	}
	return operation == edgegroupfront.ActivationOperationRollback && state.Operation == edgegroupfront.ActivationOperationRollback &&
		state.Generation > expected && (state.Generation-expected)%2 == 1 && state.RollbackOfGeneration == state.Generation-1
}

func (activator *frontAuthorityActivator) promoteWithLease(ctx context.Context, target releaseguardian.FrontAuthorityTarget, lease *heldAuthorityLease, preflight frontAuthorityPreflight) (releaseguardian.FrontAuthorityTransaction, error) {
	return activator.applyWithLease(ctx, target, lease, preflight, edgegroupfront.ActivationOperationPromote, 0,
		"promote Guardian verified candidate authority")
}

func (activator *frontAuthorityActivator) applyWithLease(ctx context.Context, target releaseguardian.FrontAuthorityTarget, lease *heldAuthorityLease, preflight frontAuthorityPreflight, operation string, rollbackOf uint64, reason string) (releaseguardian.FrontAuthorityTransaction, error) {
	if operation != edgegroupfront.ActivationOperationPromote && operation != edgegroupfront.ActivationOperationRollback {
		return nil, errors.New("Front authority operation is invalid")
	}
	workers, states := preflight.workers, preflight.states
	previousGeneration := preflight.previousGeneration
	if previousGeneration == 0 {
		return nil, errors.New("Front authority predecessor generation is unavailable")
	}
	if preflight.alreadyAtNew {
		receipt := releaseguardian.FrontAuthorityReceipt{GroupID: target.GroupID, PreviousSlot: target.PreviousSlot,
			PreviousGeneration: previousGeneration, PreviousBundleGeneration: target.PreviousBundleGeneration,
			PreviousWorkerSourceSHA: target.PreviousWorkerSourceSHA, PreviousWorkerImageDigest: target.PreviousWorkerImageDigest,
			TargetSlot: target.TargetSlot, TargetGeneration: previousGeneration + 1,
			TargetBundleGeneration: target.FrontBundleGeneration, TargetWorkerSourceSHA: target.WorkerSourceSHA,
			TargetWorkerImageDigest: target.WorkerImageDigest}
		return &frontAuthorityTransaction{activator: activator, lease: lease, receipt: receipt}, nil
	}
	changed := make(map[string]edgegroupfront.ActivationReceipt, len(workers))
	nodes := make([]string, 0, len(workers))
	for node := range workers {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		worker := workers[node]
		state := states[node]
		if state.ActiveSlot == string(target.TargetSlot) {
			previous := edgegroupfront.ActivationState{Schema: edgegroupfront.ActivationStateSchemaV1, GroupID: target.GroupID,
				Generation: target.PreviousFrontGeneration, ActiveSlot: string(target.PreviousSlot), BundleGeneration: target.PreviousBundleGeneration,
				WorkerSourceCommit: target.PreviousWorkerSourceSHA, WorkerImageDigest: target.PreviousWorkerImageDigest,
				Authority: edgegroupfront.ActivationAuthority, Operation: edgegroupfront.ActivationOperationPromote,
				Reason: "resume exact Guardian authority LKG witness", UpdatedAt: state.UpdatedAt}
			changed[node] = edgegroupfront.ActivationReceipt{Schema: edgegroupfront.ActivationReceiptSchemaV1, GroupID: target.GroupID,
				PreviousExists: true, Previous: &previous, Current: state}
			continue
		}
		receipt, casErr := activator.activationCAS(ctx, worker.Name, edgegroupfront.ActivationCASRequest{
			GroupID: target.GroupID, ExpectedGeneration: state.Generation, ExpectedSlot: state.ActiveSlot, TargetSlot: string(target.TargetSlot),
			BundleGeneration: target.FrontBundleGeneration, WorkerSourceCommit: target.WorkerSourceSHA, WorkerImageDigest: target.WorkerImageDigest,
			Operation: operation, RollbackOfGeneration: rollbackOf, Reason: reason,
		})
		if casErr != nil {
			if rollbackErr := activator.rollbackReceipts(context.WithoutCancel(ctx), workers, changed); rollbackErr != nil {
				return nil, errors.Join(casErr, fmt.Errorf("%w: %v", errFrontCompensationUnknown, rollbackErr))
			}
			return nil, casErr
		}
		changed[node] = receipt
	}
	if err := activator.waitFront(ctx, target); err != nil {
		if rollbackErr := activator.rollbackReceipts(context.WithoutCancel(ctx), workers, changed); rollbackErr != nil {
			return nil, errors.Join(err, fmt.Errorf("%w: %v", errFrontCompensationUnknown, rollbackErr))
		}
		return nil, err
	}
	if activator.config.RouteAddress != "" {
		if err := activator.verifyPublicRoute(ctx, target, operation == edgegroupfront.ActivationOperationRollback); err != nil {
			if rollbackErr := activator.rollbackReceipts(context.WithoutCancel(ctx), workers, changed); rollbackErr != nil {
				return nil, errors.Join(errors.New("post-activation public route canary failed"), fmt.Errorf("%w: %v", errFrontCompensationUnknown, rollbackErr))
			}
			return nil, errors.New("post-activation public route canary failed")
		}
	}
	for _, receipt := range changed {
		if receipt.Previous == nil || receipt.Previous.Generation != previousGeneration ||
			receipt.Previous.ActiveSlot != string(target.PreviousSlot) || receipt.Previous.BundleGeneration != target.PreviousBundleGeneration ||
			receipt.Previous.WorkerSourceCommit != target.PreviousWorkerSourceSHA || receipt.Previous.WorkerImageDigest != target.PreviousWorkerImageDigest ||
			receipt.Current.Generation != previousGeneration+1 || receipt.Current.ActiveSlot != string(target.TargetSlot) ||
			receipt.Current.BundleGeneration != target.FrontBundleGeneration || receipt.Current.WorkerSourceCommit != target.WorkerSourceSHA ||
			receipt.Current.WorkerImageDigest != target.WorkerImageDigest {
			if rollbackErr := activator.rollbackReceipts(context.WithoutCancel(ctx), workers, changed); rollbackErr != nil {
				return nil, errors.Join(errors.New("Front nodes produced divergent activation receipts"), fmt.Errorf("%w: %v", errFrontCompensationUnknown, rollbackErr))
			}
			return nil, errors.New("Front nodes produced divergent activation receipts")
		}
	}
	receipt := releaseguardian.FrontAuthorityReceipt{GroupID: target.GroupID, PreviousSlot: target.PreviousSlot,
		PreviousGeneration: previousGeneration, PreviousBundleGeneration: target.PreviousBundleGeneration,
		PreviousWorkerSourceSHA: target.PreviousWorkerSourceSHA, PreviousWorkerImageDigest: target.PreviousWorkerImageDigest,
		TargetSlot: target.TargetSlot, TargetGeneration: previousGeneration + 1, TargetBundleGeneration: target.FrontBundleGeneration,
		TargetWorkerSourceSHA: target.WorkerSourceSHA, TargetWorkerImageDigest: target.WorkerImageDigest}
	return &frontAuthorityTransaction{activator: activator, lease: lease, receipt: receipt}, nil
}

func (activator *frontAuthorityActivator) verifyPublicRoute(ctx context.Context, target releaseguardian.FrontAuthorityTarget, allowUnattestedLKG bool) error {
	if activator.config.RouteAddress == "" {
		return nil
	}
	probe := canaryProbe{Address: activator.config.RouteAddress, Host: activator.config.RouteHost, Path: activator.config.RoutePath}
	verifyCtx, cancel := context.WithTimeout(ctx, postActivationRouteTimeout)
	defer cancel()
	return waitForAuthorityRoute(verifyCtx, probe, activator.config.RouteBodyDigest, target.CandidateRecordDigest,
		target.TargetSlot, allowUnattestedLKG, postActivationRouteAttempts, postActivationRouteSuccesses,
		postActivationRouteInterval, requestPublicRouteWithHeaders)
}

type authorityRouteRequest func(context.Context, canaryProbe) (int, []byte, http.Header, error)

// waitForAuthorityRoute allows the Front process and its in-memory proxy
// configuration to converge after the activation CAS. It never weakens the
// route witness: every accepted sample must bind the exact body, record and
// worker slot, and a transient failure resets the consecutive-success count.
func waitForAuthorityRoute(ctx context.Context, probe canaryProbe, bodyDigest, recordDigest string,
	slot releaseguardian.AuthoritySlot, allowUnattestedLKG bool, attempts, requiredSuccesses int,
	interval time.Duration, request authorityRouteRequest) error {
	if request == nil || attempts < 1 || requiredSuccesses < 1 || requiredSuccesses > attempts || interval < 0 {
		return errors.New("post-activation route verification is invalid")
	}
	consecutive := 0
	for attempt := 0; attempt < attempts; attempt++ {
		status, body, headers, routeErr := request(ctx, probe)
		if authorityRouteMatches(status, body, headers, routeErr, bodyDigest, recordDigest, slot, allowUnattestedLKG) {
			consecutive++
			if consecutive == requiredSuccesses {
				return nil
			}
		} else {
			consecutive = 0
		}
		if attempt+1 == attempts {
			break
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return errors.New("public route did not converge on the activated authority")
		case <-timer.C:
		}
	}
	return errors.New("public route did not converge on the activated authority")
}

func (transaction *frontAuthorityTransaction) Receipt() releaseguardian.FrontAuthorityReceipt {
	return transaction.receipt
}

func (transaction *frontAuthorityTransaction) Commit(ctx context.Context) error {
	if transaction == nil || transaction.closed {
		return errors.New("Front authority transaction is already finalized")
	}
	if err := transaction.lease.release(context.WithoutCancel(ctx)); err != nil {
		return err
	}
	transaction.closed = true
	return nil
}

func (transaction *frontAuthorityTransaction) Rollback(ctx context.Context) error {
	if transaction == nil || transaction.closed {
		return errors.New("Front authority transaction is already finalized")
	}
	if err := transaction.rollbackFront(ctx); err != nil {
		return err
	}
	if err := transaction.lease.release(context.WithoutCancel(ctx)); err != nil {
		return err
	}
	transaction.closed = true
	return nil
}

func (transaction *frontAuthorityTransaction) rollbackFront(ctx context.Context) error {
	workers, _, err := transaction.activator.observeWorkers(ctx, releaseguardian.FrontAuthorityTarget{GroupID: transaction.receipt.GroupID,
		TargetSlot: transaction.receipt.TargetSlot, CandidateBundleGeneration: transaction.receipt.TargetBundleGeneration,
		WorkerSourceSHA: transaction.receipt.TargetWorkerSourceSHA, WorkerImageDigest: transaction.receipt.TargetWorkerImageDigest})
	if err != nil {
		return err
	}
	for _, worker := range workers {
		_, err := transaction.activator.activationCAS(ctx, worker.Name, edgegroupfront.ActivationCASRequest{
			GroupID: transaction.receipt.GroupID, ExpectedGeneration: transaction.receipt.TargetGeneration,
			ExpectedSlot: string(transaction.receipt.TargetSlot), TargetSlot: string(transaction.receipt.PreviousSlot),
			BundleGeneration: transaction.receipt.PreviousBundleGeneration, WorkerSourceCommit: transaction.receipt.PreviousWorkerSourceSHA,
			WorkerImageDigest: transaction.receipt.PreviousWorkerImageDigest, Operation: edgegroupfront.ActivationOperationRollback,
			RollbackOfGeneration: transaction.receipt.TargetGeneration, Reason: "rollback uncommitted Guardian authority",
		})
		if err != nil {
			return err
		}
	}
	return nil
}

type observedFront struct {
	Generation         uint64 `json:"activation_generation"`
	ActiveSlot         string `json:"active_slot"`
	BundleGeneration   string `json:"bundle_generation"`
	WorkerSourceCommit string `json:"worker_source_commit"`
	WorkerImageDigest  string `json:"worker_image_digest"`
	RouteAuthority     string `json:"route_authority"`
	Status             string `json:"status"`
}

func (activator *frontAuthorityActivator) observeWorkers(ctx context.Context, target releaseguardian.FrontAuthorityTarget) (map[string]corev1.Pod, releaseguardian.CandidateWorkerCohort, error) {
	selector := labels.Set{"fugue.io/edge-group-id": target.GroupID, "fugue.io/edge-slot": string(target.TargetSlot)}.AsSelector().String()
	list, err := activator.client.CoreV1().Pods(activator.config.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: int64(activator.config.ExpectedNodes + 1)})
	if err != nil || list.Continue != "" || len(list.Items) != activator.config.ExpectedNodes {
		return nil, releaseguardian.CandidateWorkerCohort{}, errors.New("target worker cohort is unavailable")
	}
	candidate := releaseguardian.CandidateAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CandidateAuthorityKind,
		GroupID: target.GroupID, RecordDigest: target.CandidateRecordDigest, BundleGeneration: target.CandidateBundleGeneration,
		WorkerSlot: target.TargetSlot, ReleaseRecordDigest: target.CanaryResultDigest, State: releaseguardian.CandidateAuthorityLoaded, Generation: 1}
	cohort, err := sealFrontCandidateWorkerCohort(list.Items, candidate)
	if err != nil || cohort.WorkerSourceSHA != target.WorkerSourceSHA || cohort.WorkerImageDigest != target.WorkerImageDigest {
		return nil, releaseguardian.CandidateWorkerCohort{}, errors.New("target worker identity changed before Front CAS")
	}
	workers := make(map[string]corev1.Pod, len(list.Items))
	for index := range list.Items {
		pod := list.Items[index]
		if _, exists := workers[pod.Spec.NodeName]; exists {
			return nil, releaseguardian.CandidateWorkerCohort{}, errors.New("multiple target workers share one node")
		}
		workers[pod.Spec.NodeName] = pod
	}
	return workers, cohort, nil
}

// observeAuthorityRuntime binds the traffic pointer to the exact running
// Worker cohort. Current authority additionally binds every Front process to
// the committed activation state; LKG health deliberately does not, because a
// failed activation may leave Front on the candidate while the inactive LKG
// Worker remains the only safe rollback target.
func (activator *frontAuthorityActivator) observeAuthorityRuntime(ctx context.Context, slot releaseguardian.AuthoritySlot,
	sourceSHA, imageDigest string, frontGeneration uint64, bundleGeneration string, requireFront bool) (bool, error) {
	if activator == nil || slot.Validate() != nil || !exactSourceSHA(sourceSHA) || !exactSHA256Digest(imageDigest) ||
		strings.TrimSpace(bundleGeneration) == "" || (requireFront && frontGeneration == 0) {
		return false, errors.New("authority runtime observation input is invalid")
	}
	workers, cohort, err := activator.observeWorkers(ctx, releaseguardian.FrontAuthorityTarget{GroupID: activator.config.GroupID,
		TargetSlot: slot, CandidateBundleGeneration: bundleGeneration, WorkerSourceSHA: sourceSHA, WorkerImageDigest: imageDigest})
	if err != nil || !authorityRuntimeMatches(workers, cohort, nil, slot, sourceSHA, imageDigest, frontGeneration, bundleGeneration, false) {
		return false, errors.New("authority Worker runtime does not match its pointer")
	}
	for _, worker := range workers {
		if worker.Status.PodIP == "" {
			return false, errors.New("authority Worker route generation is unavailable")
		}
		var health baselineWorkerHealth
		if err := readAuthorityBaselineJSON(ctx, "http://"+worker.Status.PodIP+":"+strconv.Itoa(workerHealthPort)+"/healthz", &health); err != nil ||
			!authorityWorkerHealthMatches(health, activator.config.GroupID, bundleGeneration) {
			return false, errors.New("authority Worker route generation does not match its pointer")
		}
	}
	if !requireFront {
		return true, nil
	}
	fronts, err := activator.observeFronts(ctx)
	if err != nil || len(fronts) != activator.config.ExpectedNodes {
		return false, errors.New("authority Front runtime is unavailable")
	}
	if !authorityRuntimeMatches(workers, cohort, fronts, slot, sourceSHA, imageDigest, frontGeneration, bundleGeneration, true) {
		return false, errors.New("authority Front runtime does not match its pointer")
	}
	return true, nil
}

func authorityWorkerHealthMatches(health baselineWorkerHealth, groupID, bundleGeneration string) bool {
	serving, publication, _, err := splitPromotedBundleVersion(bundleGeneration)
	return err == nil && health.Healthy && health.EdgeGroupID == groupID && health.BundleVersion == bundleGeneration &&
		health.ServingGeneration == serving && health.PublicationSequence == publication
}

func authorityRuntimeMatches(workers map[string]corev1.Pod, cohort releaseguardian.CandidateWorkerCohort, fronts map[string]observedFront,
	slot releaseguardian.AuthoritySlot, sourceSHA, imageDigest string, frontGeneration uint64, bundleGeneration string, requireFront bool) bool {
	if len(workers) == 0 || cohort.WorkerSourceSHA != sourceSHA || cohort.WorkerImageDigest != imageDigest {
		return false
	}
	for node, worker := range workers {
		if strings.TrimSpace(worker.Annotations["fugue.pro/source-commit"]) != sourceSHA || containerRuntimeDigest(worker, "edge") != imageDigest {
			return false
		}
		if !requireFront {
			continue
		}
		front, exists := fronts[node]
		if !exists || front.Generation != frontGeneration || front.ActiveSlot != string(slot) || front.BundleGeneration != bundleGeneration ||
			front.WorkerSourceCommit != sourceSHA || front.WorkerImageDigest != imageDigest {
			return false
		}
	}
	return !requireFront || len(fronts) == len(workers)
}

func containerRuntimeDigest(pod corev1.Pod, container string) string {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == container && status.Ready && status.RestartCount == 0 {
			return imageDigest(status.ImageID)
		}
	}
	return ""
}

func sealFrontCandidateWorkerCohort(pods []corev1.Pod, candidate releaseguardian.CandidateAuthority) (releaseguardian.CandidateWorkerCohort, error) {
	cohort := releaseguardian.CandidateWorkerCohort{GroupID: candidate.GroupID, WorkerSlot: candidate.WorkerSlot,
		BundleGeneration: candidate.BundleGeneration, Instances: make([]releaseguardian.CandidateWorkerInstance, 0, len(pods))}
	for index := range pods {
		pod := &pods[index]
		if pod.DeletionTimestamp != nil || pod.UID == "" || strings.TrimSpace(pod.Spec.NodeName) == "" || !podReady(pod.Status.Conditions) {
			return releaseguardian.CandidateWorkerCohort{}, errors.New("candidate worker cohort is not ready")
		}
		source, declaredDigest, runtimeDigest := strings.TrimSpace(pod.Annotations["fugue.pro/source-commit"]), "", ""
		for _, container := range pod.Spec.Containers {
			if container.Name == "edge" {
				declaredDigest = imageDigest(container.Image)
			}
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == "edge" {
				if !status.Ready || status.RestartCount != 0 {
					return releaseguardian.CandidateWorkerCohort{}, errors.New("candidate worker process is not stable")
				}
				runtimeDigest = imageDigest(status.ImageID)
			}
		}
		if !exactSourceSHA(source) || declaredDigest == "" || runtimeDigest != declaredDigest {
			return releaseguardian.CandidateWorkerCohort{}, errors.New("candidate worker immutable identity is invalid")
		}
		if cohort.WorkerSourceSHA == "" {
			cohort.WorkerSourceSHA, cohort.WorkerImageDigest = source, declaredDigest
		} else if cohort.WorkerSourceSHA != source || cohort.WorkerImageDigest != declaredDigest {
			return releaseguardian.CandidateWorkerCohort{}, errors.New("candidate worker cohort contains mixed releases")
		}
		cohort.Instances = append(cohort.Instances, releaseguardian.CandidateWorkerInstance{NodeName: pod.Spec.NodeName, PodUID: string(pod.UID)})
	}
	return cohort.Seal()
}

func (activator *frontAuthorityActivator) observeFronts(ctx context.Context) (map[string]observedFront, error) {
	selector := labels.Set{"fugue.io/edge-group-id": activator.config.GroupID}.AsSelector().String()
	list, err := activator.client.CoreV1().Pods(activator.config.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 101})
	if err != nil || list.Continue != "" {
		return nil, errors.New("Front cohort is unavailable")
	}
	fronts := map[string]observedFront{}
	for index := range list.Items {
		pod := &list.Items[index]
		isFront := false
		for _, container := range pod.Spec.Containers {
			isFront = isFront || container.Name == "edge-front"
		}
		if !isFront {
			continue
		}
		if pod.DeletionTimestamp != nil || !podReady(pod.Status.Conditions) || strings.TrimSpace(pod.Status.PodIP) == "" || strings.TrimSpace(pod.Spec.NodeName) == "" {
			return nil, errors.New("Front cohort is not ready")
		}
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+pod.Status.PodIP+":"+strconv.Itoa(frontHealthPort)+"/readyz", nil)
		response, requestErr := (&http.Client{Timeout: 5 * time.Second}).Do(request)
		if requestErr != nil {
			return nil, requestErr
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		response.Body.Close()
		var observed observedFront
		if readErr != nil || response.StatusCode != http.StatusOK || decodeStrictJSON(body, &observed) != nil || observed.Status != "ok" ||
			observed.RouteAuthority != edgegroupfront.ActivationAuthority || observed.Generation == 0 || observed.ActiveSlot == "" {
			return nil, errors.New("Front readiness evidence is invalid")
		}
		fronts[pod.Spec.NodeName] = observed
	}
	if len(fronts) != activator.config.ExpectedNodes {
		return nil, errors.New("Front cohort size is invalid")
	}
	return fronts, nil
}

func decodeStrictJSON(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("JSON has trailing data")
	}
	return nil
}

func (activator *frontAuthorityActivator) readActivation(ctx context.Context, pod string) (edgegroupfront.ActivationState, error) {
	raw, err := activator.executor.Exec(ctx, activator.config.Namespace, pod, "edge", "cat", frontActivationStatePath)
	var state edgegroupfront.ActivationState
	if err != nil || decodeStrictJSON(raw, &state) != nil || state.Schema != edgegroupfront.ActivationStateSchemaV1 || state.Authority != edgegroupfront.ActivationAuthority {
		return state, errors.New("activation state is invalid")
	}
	return state, nil
}

func (activator *frontAuthorityActivator) activationCAS(ctx context.Context, pod string, request edgegroupfront.ActivationCASRequest) (edgegroupfront.ActivationReceipt, error) {
	args := []string{frontActivationCASBinary, "--state-file", frontActivationStatePath, "--group", request.GroupID,
		"--expected-generation", strconv.FormatUint(request.ExpectedGeneration, 10), "--expected-slot", request.ExpectedSlot,
		"--target-slot", request.TargetSlot, "--bundle-generation", request.BundleGeneration, "--worker-source-commit", request.WorkerSourceCommit,
		"--worker-image-digest", request.WorkerImageDigest, "--operation", request.Operation,
		"--rollback-of-generation", strconv.FormatUint(request.RollbackOfGeneration, 10), "--reason", request.Reason}
	raw, err := activator.executor.Exec(ctx, activator.config.Namespace, pod, "edge", args...)
	if err != nil {
		state, readErr := activator.readActivation(ctx, pod)
		if readErr == nil && state.GroupID == request.GroupID && state.Generation == request.ExpectedGeneration+1 && state.ActiveSlot == request.TargetSlot &&
			state.BundleGeneration == request.BundleGeneration && state.WorkerSourceCommit == request.WorkerSourceCommit && state.WorkerImageDigest == request.WorkerImageDigest &&
			state.Operation == request.Operation && state.RollbackOfGeneration == request.RollbackOfGeneration {
			return edgegroupfront.ActivationReceipt{Schema: edgegroupfront.ActivationReceiptSchemaV1, GroupID: request.GroupID, Current: state}, nil
		}
		return edgegroupfront.ActivationReceipt{}, err
	}
	var receipt edgegroupfront.ActivationReceipt
	if decodeStrictJSON(raw, &receipt) != nil || receipt.Schema != edgegroupfront.ActivationReceiptSchemaV1 || receipt.GroupID != request.GroupID ||
		receipt.Current.Generation != request.ExpectedGeneration+1 || receipt.Current.ActiveSlot != request.TargetSlot || receipt.Current.BundleGeneration != request.BundleGeneration ||
		receipt.Current.WorkerSourceCommit != request.WorkerSourceCommit || receipt.Current.WorkerImageDigest != request.WorkerImageDigest || receipt.Current.Operation != request.Operation {
		return receipt, errors.New("activation receipt is not request-bound")
	}
	return receipt, nil
}

func (activator *frontAuthorityActivator) waitFront(ctx context.Context, target releaseguardian.FrontAuthorityTarget) error {
	deadline := activator.now().Add(30 * time.Second)
	for {
		fronts, err := activator.observeFronts(ctx)
		if err == nil {
			matched := true
			for _, front := range fronts {
				matched = matched && front.ActiveSlot == string(target.TargetSlot) && front.BundleGeneration == target.FrontBundleGeneration &&
					front.WorkerSourceCommit == target.WorkerSourceSHA && front.WorkerImageDigest == target.WorkerImageDigest
			}
			if matched && activator.targetWorkersLoaded(ctx, target) {
				return nil
			}
		}
		if activator.now().After(deadline) {
			return errors.New("Front did not converge to verified candidate")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (activator *frontAuthorityActivator) targetWorkersLoaded(ctx context.Context, target releaseguardian.FrontAuthorityTarget) bool {
	workers, _, err := activator.observeWorkers(ctx, target)
	if err != nil {
		return false
	}
	for _, worker := range workers {
		if worker.Status.PodIP == "" {
			return false
		}
		var health baselineWorkerHealth
		if readAuthorityBaselineJSON(ctx, "http://"+worker.Status.PodIP+":"+strconv.Itoa(workerHealthPort)+"/healthz", &health) != nil ||
			!authorityWorkerHealthMatches(health, target.GroupID, target.FrontBundleGeneration) {
			return false
		}
	}
	return true
}

func (activator *frontAuthorityActivator) rollbackReceipts(ctx context.Context, workers map[string]corev1.Pod, receipts map[string]edgegroupfront.ActivationReceipt) error {
	nodes := make([]string, 0, len(receipts))
	for node := range receipts {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		receipt := receipts[node]
		if receipt.Previous == nil {
			return errors.New("activation rollback lacks previous state")
		}
		previous := receipt.Previous
		state, stateErr := activator.readActivation(ctx, workers[node].Name)
		if stateErr != nil {
			return stateErr
		}
		if state.GroupID == receipt.GroupID && state.Generation == previous.Generation && state.ActiveSlot == previous.ActiveSlot &&
			state.BundleGeneration == previous.BundleGeneration && state.WorkerSourceCommit == previous.WorkerSourceCommit &&
			state.WorkerImageDigest == previous.WorkerImageDigest {
			continue
		}
		_, err := activator.activationCAS(ctx, workers[node].Name, edgegroupfront.ActivationCASRequest{GroupID: receipt.GroupID,
			ExpectedGeneration: receipt.Current.Generation, ExpectedSlot: receipt.Current.ActiveSlot, TargetSlot: previous.ActiveSlot,
			BundleGeneration: previous.BundleGeneration, WorkerSourceCommit: previous.WorkerSourceCommit, WorkerImageDigest: previous.WorkerImageDigest,
			Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: receipt.Current.Generation,
			Reason: "rollback partial Guardian authority activation"})
		if err != nil {
			return err
		}
	}
	return nil
}

func (activator *frontAuthorityActivator) acquireLease(ctx context.Context) (*heldAuthorityLease, error) {
	name := "fugue-authority-" + activator.config.GroupID
	leases := activator.client.CoordinationV1().Leases(activator.config.Namespace)
	now := metav1.NewMicroTime(activator.now().UTC().Truncate(time.Microsecond))
	object, err := leases.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		object, err = leases.Create(ctx, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: activator.config.Namespace,
			Labels: map[string]string{"app.kubernetes.io/managed-by": "fugue-release-guardian", "fugue.pro/group": activator.config.GroupID}},
			Spec: coordinationv1.LeaseSpec{HolderIdentity: stringPtr(activator.holder), LeaseDurationSeconds: int32Ptr(authorityLeaseSeconds), AcquireTime: &now, RenewTime: &now}}, metav1.CreateOptions{})
	} else if err == nil {
		holder := strings.TrimSpace(valueOrEmpty(object.Spec.HolderIdentity))
		active := holder != "" && object.Spec.RenewTime != nil && object.Spec.LeaseDurationSeconds != nil &&
			activator.now().UTC().Before(object.Spec.RenewTime.Time.Add(time.Duration(*object.Spec.LeaseDurationSeconds)*time.Second))
		if active {
			return nil, errors.New("group authority Lease is already held")
		}
		object.Spec.HolderIdentity, object.Spec.LeaseDurationSeconds, object.Spec.AcquireTime, object.Spec.RenewTime = stringPtr(activator.holder), int32Ptr(authorityLeaseSeconds), &now, &now
		object, err = leases.Update(ctx, object, metav1.UpdateOptions{})
	}
	if err != nil || object.UID == "" || object.ResourceVersion == "" || valueOrEmpty(object.Spec.HolderIdentity) != activator.holder {
		return nil, fmt.Errorf("acquire group authority Lease: %w", err)
	}
	return &heldAuthorityLease{client: activator.client, namespace: activator.config.Namespace, name: name, uid: string(object.UID), rv: object.ResourceVersion, holder: activator.holder}, nil
}

func (lease *heldAuthorityLease) release(ctx context.Context) error {
	object, err := lease.client.CoordinationV1().Leases(lease.namespace).Get(ctx, lease.name, metav1.GetOptions{})
	if err != nil || string(object.UID) != lease.uid || object.ResourceVersion != lease.rv || valueOrEmpty(object.Spec.HolderIdentity) != lease.holder {
		return errors.New("group authority Lease release CAS failed")
	}
	object.Spec.HolderIdentity, object.Spec.AcquireTime, object.Spec.RenewTime = stringPtr(""), nil, nil
	updated, err := lease.client.CoordinationV1().Leases(lease.namespace).Update(ctx, object, metav1.UpdateOptions{})
	if err != nil || valueOrEmpty(updated.Spec.HolderIdentity) != "" {
		return errors.New("group authority Lease release is unproven")
	}
	lease.rv = updated.ResourceVersion
	return nil
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func stringPtr(value string) *string { return &value }
func int32Ptr(value int32) *int32    { return &value }
