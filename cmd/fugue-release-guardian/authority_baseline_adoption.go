package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	frontReadyPort   = 7831
	workerHealthPort = 7832
)

type authorityBaselineConfig struct {
	GroupID              string
	ExpectedRecordDigest string
	ExpectedWorkerSlot   releaseguardian.AuthoritySlot
	ExpectedEpoch        int64
	FrontComponent       string
	ExpectedNodes        int
	SlotAddresses        map[releaseguardian.AuthoritySlot]string
	Host                 string
	Path                 string
	ExpectedBodyDigest   string
}

type authorityBaselineStore interface {
	LoadCandidate(context.Context, string) (releaseguardian.CandidateAuthority, types.UID, string, error)
	LoadCurrent(context.Context, string) (releaseguardian.CurrentAuthority, types.UID, string, error)
	LoadBaselineReceipt(context.Context, string) (releaseguardian.AuthorityBaselineReceipt, error)
	LoadNormalizationReceipt(context.Context, string) (releaseguardian.AuthorityNormalizationReceipt, bool, error)
	LoadNormalizationReceiptForRecovery(context.Context, string) (releaseguardian.AuthorityNormalizationReceipt, bool, error)
	LoadRouteBundleRecord(context.Context, string, string) (releaseguardian.RouteBundleRecord, error)
	AdoptCurrentBaseline(context.Context, releaseguardian.CurrentAuthority, releaseguardian.AuthorityBaselineReceipt, types.UID, string) (types.UID, string, error)
	NormalizeCurrentBaseline(context.Context, releaseguardian.CurrentAuthority, releaseguardian.AuthorityNormalizationReceipt, types.UID, string) (types.UID, string, error)
	SwitchCurrent(context.Context, releaseguardian.CurrentAuthority, types.UID, string) (types.UID, string, error)
}

type baselineFrontHealth struct {
	Status             string `json:"status"`
	ActiveSlot         string `json:"active_slot"`
	Generation         uint64 `json:"activation_generation"`
	BundleGeneration   string `json:"bundle_generation"`
	WorkerSourceCommit string `json:"worker_source_commit"`
	WorkerImageDigest  string `json:"worker_image_digest"`
	RouteAuthority     string `json:"route_authority"`
}

type baselineWorkerHealth struct {
	Healthy               bool   `json:"healthy"`
	EdgeGroupID           string `json:"edge_group_id"`
	BundleVersion         string `json:"bundle_version"`
	PublicationSequence   uint64 `json:"publication_sequence"`
	ServingGeneration     string `json:"serving_generation"`
	CandidateBundleLoaded bool   `json:"candidate_bundle_loaded"`
	CandidateRecordDigest string `json:"candidate_record_digest"`
	CandidateWorkerSlot   string `json:"candidate_worker_slot"`
}

func authorityRecoveryCohortLimit(expectedNodes int) int64 {
	// One control-plane Pod, one Front, and two slot Workers may exist per
	// node. Spare capacity covers a terminating predecessor without allowing
	// an unbounded recovery list.
	if expectedNodes < 1 {
		return 8
	}
	return int64(4*expectedNodes + 4)
}

func parseAuthorityBaselines(value string) ([]authorityBaselineConfig, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	configs := make([]authorityBaselineConfig, 0, 2)
	seen := map[string]bool{}
	for _, raw := range strings.Split(value, ";") {
		fields := strings.Split(raw, ",")
		if len(fields) != 11 {
			return nil, errors.New("authority baseline must be group,record,slot,epoch,front-component,node-count,address-a,address-b,host,path,body-digest")
		}
		epoch, epochErr := strconv.ParseInt(strings.TrimSpace(fields[3]), 10, 64)
		nodes, nodesErr := strconv.Atoi(strings.TrimSpace(fields[5]))
		config := authorityBaselineConfig{
			GroupID: strings.TrimSpace(fields[0]), ExpectedRecordDigest: strings.TrimSpace(fields[1]),
			ExpectedWorkerSlot: releaseguardian.AuthoritySlot(strings.TrimSpace(fields[2])), ExpectedEpoch: epoch,
			FrontComponent: strings.TrimSpace(fields[4]), ExpectedNodes: nodes,
			SlotAddresses: map[releaseguardian.AuthoritySlot]string{releaseguardian.AuthoritySlotA: strings.TrimSpace(fields[6]), releaseguardian.AuthoritySlotB: strings.TrimSpace(fields[7])},
			Host:          strings.TrimSpace(fields[8]), Path: strings.TrimSpace(fields[9]), ExpectedBodyDigest: strings.TrimSpace(fields[10]),
		}
		probe := candidateCanaryProbe{GroupID: config.GroupID, SlotAddresses: config.SlotAddresses, Host: config.Host, Path: config.Path,
			ExpectedBodyDigest: config.ExpectedBodyDigest, Interval: 10 * time.Second, KeyID: "baseline-adoption", SigningMaterialFile: "/baseline/token"}
		if epochErr != nil || nodesErr != nil || config.ExpectedEpoch < 1 || config.ExpectedNodes < 1 || config.ExpectedNodes > 100 ||
			!exactSHA256Digest(config.ExpectedRecordDigest) || config.ExpectedWorkerSlot.Validate() != nil ||
			(releaseguardian.Key{Component: config.FrontComponent}).Validate() != nil || !validCandidateProbe(probe) || seen[config.GroupID] {
			return nil, errors.New("authority baseline configuration is invalid")
		}
		seen[config.GroupID] = true
		configs = append(configs, config)
	}
	return configs, nil
}

func startAuthorityBaselineAdopters(ctx context.Context, store *releaseguardian.AuthorityStore, client kubernetes.Interface, namespace string, configs []authorityBaselineConfig, executors ...podCommandExecutor) {
	var executor podCommandExecutor
	if len(executors) > 0 {
		executor = executors[0]
	}
	for _, config := range configs {
		config := config
		go func() {
			for {
				done, err := adoptAuthorityBaselineOnce(ctx, store, client, namespace, config, time.Now().UTC(), executor)
				if err != nil {
					fmt.Fprintf(os.Stderr, "authority baseline %s: %v\n", config.GroupID, err)
				}
				if done {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Second):
				}
			}
		}()
	}
}

func adoptAuthorityBaselineOnce(ctx context.Context, store authorityBaselineStore, client kubernetes.Interface, namespace string, config authorityBaselineConfig, now time.Time, executors ...podCommandExecutor) (bool, error) {
	if store == nil || client == nil || strings.TrimSpace(namespace) == "" {
		return false, errors.New("authority baseline dependency is unavailable")
	}
	before, uid, rv, err := store.LoadCurrent(ctx, config.GroupID)
	if err != nil {
		return false, err
	}
	var baseline releaseguardian.AuthorityBaselineReceipt
	if before.BaselineReceiptDigest == "" {
		if before.CurrentRecordDigest != config.ExpectedRecordDigest || before.CurrentWorkerSlot != config.ExpectedWorkerSlot || before.AuthorityEpoch != config.ExpectedEpoch ||
			before.PreviousRecordDigest != "" || before.PreviousWorkerSlot != "" {
			return false, errors.New("authority baseline predecessor does not match the one-time authorization")
		}
	} else {
		baseline, err = store.LoadBaselineReceipt(ctx, config.GroupID)
		if err != nil || baseline.ReceiptDigest != before.BaselineReceiptDigest || baseline.BeforeRecordDigest != config.ExpectedRecordDigest ||
			baseline.BeforeWorkerSlot != config.ExpectedWorkerSlot || baseline.BeforeAuthorityEpoch != config.ExpectedEpoch {
			return false, errors.New("authority normalization is not bound to the original one-time authorization")
		}
	}
	var executor podCommandExecutor
	if len(executors) > 0 {
		executor = executors[0]
	}
	fronts, err := observeBaselineFronts(ctx, client, namespace, config, before, executor)
	if err != nil {
		return false, err
	}
	activeSlot := releaseguardian.AuthoritySlot("")
	for _, front := range fronts {
		if activeSlot == "" {
			activeSlot = releaseguardian.AuthoritySlot(front.health.ActiveSlot)
		} else if activeSlot != releaseguardian.AuthoritySlot(front.health.ActiveSlot) {
			return false, errors.New("Front baseline contains mixed active slots")
		}
	}
	if activeSlot.Validate() != nil {
		return false, errors.New("Front baseline has an invalid active authority")
	}
	if before.BaselineReceiptDigest != "" {
		repaired, repairErr := repairOrphanedAuthority(ctx, store, client, namespace, config, before, uid, rv, fronts)
		if repairErr != nil {
			return false, repairErr
		}
		if repaired {
			return true, nil
		}
	}
	workers, recordDigest, epoch, witnesses, err := observeBaselineWorkers(ctx, client, namespace, config, activeSlot, fronts, before)
	if err != nil {
		return false, err
	}
	_ = workers
	record, err := store.LoadRouteBundleRecord(ctx, config.GroupID, recordDigest)
	if err != nil || record.Epoch != epoch {
		return false, errors.New("active Front route record is unavailable")
	}
	status, body, headers, routeErr := requestAuthorityBaselineRoute(ctx, config.SlotAddresses[activeSlot], config.Host, config.Path)
	if routeErr != nil || status < 200 || status >= 300 || shaDigest(body) != config.ExpectedBodyDigest ||
		strings.TrimSpace(headers.Get("X-Fugue-Candidate-Record-Digest")) != recordDigest ||
		strings.TrimSpace(headers.Get("X-Fugue-Candidate-Worker-Slot")) != string(activeSlot) {
		return false, errors.New("active Front route does not attest the baseline record")
	}
	if before.BaselineReceiptDigest != "" && activeSlot == before.CurrentWorkerSlot && recordDigest == before.CurrentRecordDigest &&
		witnesses[0].ActivationGeneration == before.CurrentFrontGeneration && witnesses[0].BundleGeneration == before.CurrentBundleGeneration &&
		witnesses[0].WorkerSourceSHA == before.CurrentWorkerSourceSHA && witnesses[0].WorkerImageDigest == before.CurrentWorkerImageDigest {
		return true, nil
	}
	if before.BaselineReceiptDigest != "" {
		if activeSlot == before.CurrentWorkerSlot || before.PreviousRecordDigest == "" || before.PreviousWorkerSlot == "" || epoch <= before.AuthorityEpoch {
			return false, errors.New("authority normalization live state is not a single forward legacy switch")
		}
		after := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
			GroupID: config.GroupID, CurrentRecordDigest: recordDigest, CurrentWorkerSlot: activeSlot,
			CurrentFrontGeneration: witnesses[0].ActivationGeneration, CurrentBundleGeneration: witnesses[0].BundleGeneration,
			CurrentWorkerSourceSHA: witnesses[0].WorkerSourceSHA, CurrentWorkerImageDigest: witnesses[0].WorkerImageDigest,
			AuthorityEpoch: int64(epoch), BaselineReceiptDigest: before.BaselineReceiptDigest}
		normalization, sealErr := (releaseguardian.AuthorityNormalizationReceipt{GroupID: config.GroupID,
			BaselineReceiptDigest: before.BaselineReceiptDigest, Before: before, After: after,
			Nodes: witnesses, ObservedAt: now.Format(time.RFC3339Nano)}).Seal()
		if sealErr != nil {
			return false, sealErr
		}
		if _, _, err := store.NormalizeCurrentBaseline(ctx, after, normalization, uid, rv); err != nil {
			return false, err
		}
		return true, nil
	}
	receipt, err := (releaseguardian.AuthorityBaselineReceipt{
		GroupID: config.GroupID, BeforeRecordDigest: before.CurrentRecordDigest, BeforeWorkerSlot: before.CurrentWorkerSlot, BeforeAuthorityEpoch: before.AuthorityEpoch,
		RecordDigest: recordDigest, WorkerSlot: activeSlot, AuthorityEpoch: epoch, Nodes: witnesses, ObservedAt: now.Format(time.RFC3339Nano),
	}).Seal()
	if err != nil {
		return false, err
	}
	after := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: config.GroupID, CurrentRecordDigest: recordDigest, CurrentWorkerSlot: activeSlot,
		CurrentFrontGeneration: witnesses[0].ActivationGeneration, CurrentBundleGeneration: witnesses[0].BundleGeneration,
		CurrentWorkerSourceSHA: witnesses[0].WorkerSourceSHA, CurrentWorkerImageDigest: witnesses[0].WorkerImageDigest,
		AuthorityEpoch: epoch, BaselineReceiptDigest: receipt.ReceiptDigest}
	if _, _, err := store.AdoptCurrentBaseline(ctx, after, receipt, uid, rv); err != nil {
		return false, err
	}
	return true, nil
}

// repairOrphanedAuthority handles the narrow historical failure where Front
// already committed a verified candidate activation but CurrentAuthority was
// left pointing at the opposite slot. It only performs the metadata CAS when
// the activation, verified candidate, normalization receipt, and the live
// inactive Worker identities all agree. The subsequent authority runtime loop
// still has to execute the ordinary LKG restore transaction and public-route
// probe before the group can settle.
func repairOrphanedAuthority(ctx context.Context, store authorityBaselineStore, client kubernetes.Interface, namespace string, config authorityBaselineConfig, before releaseguardian.CurrentAuthority, uid types.UID, rv string, fronts map[string]observedBaselineFront) (bool, error) {
	candidate, _, _, err := store.LoadCandidate(ctx, config.GroupID)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("orphaned authority candidate is unavailable: %w", err)
	}
	if err := candidate.Validate(); err != nil {
		return false, fmt.Errorf("orphaned authority candidate is invalid: %w", err)
	}
	if candidate.State != releaseguardian.CandidateAuthorityVerified {
		if candidate.State == releaseguardian.CandidateAuthorityLoaded {
			return false, nil
		}
		return false, fmt.Errorf("orphaned authority candidate is not verified: state=%s", candidate.State)
	}
	if !candidate.HasPromotionWitness() {
		return false, errors.New("orphaned authority candidate promotion witness is incomplete")
	}
	if !candidate.HasWorkerReleaseIdentity() {
		return false, errors.New("orphaned authority candidate Worker identity is incomplete")
	}
	if record, recordErr := store.LoadRouteBundleRecord(ctx, config.GroupID, candidate.RecordDigest); recordErr != nil {
		return false, fmt.Errorf("orphaned authority candidate route record is unavailable: %w", recordErr)
	} else if record.Epoch != int64(candidate.CandidateEpoch) {
		return false, fmt.Errorf("orphaned authority candidate route epoch mismatch: record=%d candidate=%d", record.Epoch, candidate.CandidateEpoch)
	}
	normalization, exists, err := store.LoadNormalizationReceiptForRecovery(ctx, config.GroupID)
	if err != nil {
		return false, fmt.Errorf("orphaned authority normalization receipt is unavailable: %w", err)
	}
	if !exists {
		return false, errors.New("orphaned authority normalization receipt is absent")
	}
	if err := normalization.Validate(); err != nil {
		return false, fmt.Errorf("orphaned authority normalization receipt is invalid: %w", err)
	}
	front, ok := firstBaselineFront(fronts)
	if !ok {
		return false, errors.New("orphaned authority Front witness is absent")
	}
	if releaseguardian.AuthoritySlot(front.health.ActiveSlot) != candidate.WorkerSlot {
		return false, fmt.Errorf("orphaned authority slot mismatch: front=%s candidate=%s", front.health.ActiveSlot, candidate.WorkerSlot)
	}
	if front.health.WorkerSourceCommit != candidate.WorkerSourceSHA || front.health.WorkerImageDigest != candidate.WorkerImageDigest {
		return false, fmt.Errorf("orphaned authority Front identity mismatch: front=%s/%s candidate=%s/%s", front.health.WorkerSourceCommit, front.health.WorkerImageDigest, candidate.WorkerSourceSHA, candidate.WorkerImageDigest)
	}
	if authorityGenerationBase(front.health.BundleGeneration) != authorityGenerationBase(candidate.CurrentServingGeneration) {
		return false, fmt.Errorf("orphaned authority Front bundle mismatch: front=%s candidate-serving=%s", front.health.BundleGeneration, candidate.CurrentServingGeneration)
	}
	if before.CurrentWorkerSlot == candidate.WorkerSlot || before.CurrentRecordDigest == candidate.RecordDigest {
		return false, fmt.Errorf("orphaned authority current pointer already targets candidate: slot=%s record=%s", before.CurrentWorkerSlot, before.CurrentRecordDigest)
	}
	if before.CurrentWorkerSourceSHA != candidate.WorkerSourceSHA || before.CurrentWorkerImageDigest != candidate.WorkerImageDigest {
		return false, fmt.Errorf("orphaned authority current identity mismatch: current=%s/%s candidate=%s/%s", before.CurrentWorkerSourceSHA, before.CurrentWorkerImageDigest, candidate.WorkerSourceSHA, candidate.WorkerImageDigest)
	}
	if authorityGenerationBase(before.CurrentBundleGeneration) != authorityGenerationBase(candidate.CurrentServingGeneration) {
		return false, fmt.Errorf("orphaned authority current bundle mismatch: current=%s candidate-serving=%s", before.CurrentBundleGeneration, candidate.CurrentServingGeneration)
	}
	if normalization.After.CurrentWorkerSourceSHA == "" || normalization.After.CurrentWorkerImageDigest == "" {
		return false, errors.New("orphaned authority normalization identity is invalid")
	}
	workers, err := observeStableAuthorityWorkers(ctx, client, namespace, config, candidate.WorkerSlot, normalization.After.CurrentWorkerSourceSHA, normalization.After.CurrentWorkerImageDigest)
	if err != nil {
		return false, err
	}
	for node := range fronts {
		if _, ok := workers[node]; !ok {
			return false, errors.New("orphaned authority LKG Worker node set is incomplete")
		}
	}
	previousSource, previousImage := normalization.After.CurrentWorkerSourceSHA, normalization.After.CurrentWorkerImageDigest
	next := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: config.GroupID, CurrentRecordDigest: candidate.RecordDigest, CurrentWorkerSlot: candidate.WorkerSlot,
		CurrentFrontGeneration: front.health.Generation, CurrentBundleGeneration: front.health.BundleGeneration,
		CurrentWorkerSourceSHA: candidate.WorkerSourceSHA, CurrentWorkerImageDigest: candidate.WorkerImageDigest,
		PreviousRecordDigest: before.CurrentRecordDigest, PreviousWorkerSlot: func() releaseguardian.AuthoritySlot {
			if candidate.WorkerSlot == releaseguardian.AuthoritySlotA {
				return releaseguardian.AuthoritySlotB
			}
			return releaseguardian.AuthoritySlotA
		}(),
		PreviousFrontGeneration: before.CurrentFrontGeneration, PreviousBundleGeneration: before.CurrentBundleGeneration,
		PreviousWorkerSourceSHA: previousSource, PreviousWorkerImageDigest: previousImage,
		AuthorityEpoch: before.AuthorityEpoch + 1, BaselineReceiptDigest: before.BaselineReceiptDigest}
	if next.Validate() != nil {
		return false, errors.New("orphaned authority repair pointer is invalid")
	}
	if _, _, err := store.SwitchCurrent(ctx, next, uid, rv); err != nil {
		return false, err
	}
	return true, nil
}

func firstBaselineFront(fronts map[string]observedBaselineFront) (observedBaselineFront, bool) {
	for _, front := range fronts {
		return front, true
	}
	return observedBaselineFront{}, false
}

func authorityGenerationBase(value string) string {
	value = strings.TrimSpace(value)
	if pivot := strings.LastIndex(value, ".p"); pivot > 0 {
		return value[:pivot]
	}
	return value
}

func observeStableAuthorityWorkers(ctx context.Context, client kubernetes.Interface, namespace string, config authorityBaselineConfig, activeSlot releaseguardian.AuthoritySlot, source, image string) (map[string]corev1.Pod, error) {
	otherSlot := releaseguardian.AuthoritySlotA
	if activeSlot == releaseguardian.AuthoritySlotA {
		otherSlot = releaseguardian.AuthoritySlotB
	}
	selector := labels.Set{"fugue.io/edge-group-id": config.GroupID, "fugue.io/edge-slot": string(otherSlot)}.AsSelector().String()
	list, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: int64(config.ExpectedNodes + 1)})
	if err != nil || list.Continue != "" || len(list.Items) != config.ExpectedNodes {
		return nil, errors.New("orphaned authority LKG Worker cohort is unavailable")
	}
	workers := make(map[string]corev1.Pod, len(list.Items))
	for index := range list.Items {
		pod := list.Items[index]
		if pod.DeletionTimestamp != nil || pod.UID == "" || pod.ResourceVersion == "" || pod.Spec.NodeName == "" || !podReady(pod.Status.Conditions) ||
			strings.TrimSpace(pod.Annotations["fugue.pro/source-commit"]) != source || containerRuntimeImageDigest(pod, "edge") != image {
			return nil, errors.New("orphaned authority LKG Worker identity is invalid")
		}
		var health baselineWorkerHealth
		if pod.Status.PodIP == "" || readAuthorityBaselineJSON(ctx, "http://"+pod.Status.PodIP+":"+strconv.Itoa(workerHealthPort)+"/healthz", &health) != nil ||
			!health.Healthy || health.EdgeGroupID != config.GroupID {
			return nil, errors.New("orphaned authority LKG Worker health is invalid")
		}
		if _, exists := workers[pod.Spec.NodeName]; exists {
			return nil, errors.New("orphaned authority LKG Worker cohort has duplicate nodes")
		}
		workers[pod.Spec.NodeName] = pod
	}
	return workers, nil
}

type observedBaselineFront struct {
	pod    corev1.Pod
	health baselineFrontHealth
}

func observeBaselineFronts(ctx context.Context, client kubernetes.Interface, namespace string, config authorityBaselineConfig, before releaseguardian.CurrentAuthority, executor podCommandExecutor) (map[string]observedBaselineFront, error) {
	selector := labels.Set{"fugue.io/edge-group-id": config.GroupID, "app.kubernetes.io/component": config.FrontComponent}.AsSelector().String()
	list, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: int64(config.ExpectedNodes + 1)})
	if err != nil || list.Continue != "" || len(list.Items) != config.ExpectedNodes {
		if before.BaselineReceiptDigest != "" && executor != nil {
			return observeBaselineFrontsFromActivation(ctx, client, namespace, config, executor)
		}
		return nil, errors.New("Front baseline cohort is unavailable")
	}
	result := make(map[string]observedBaselineFront, len(list.Items))
	for index := range list.Items {
		pod := list.Items[index]
		if pod.DeletionTimestamp != nil || pod.UID == "" || pod.ResourceVersion == "" || pod.Spec.NodeName == "" || pod.Status.PodIP == "" || !podReady(pod.Status.Conditions) {
			if before.BaselineReceiptDigest != "" && executor != nil {
				return observeBaselineFrontsFromActivation(ctx, client, namespace, config, executor)
			}
			return nil, errors.New("Front baseline Pod is invalid")
		}
		var health baselineFrontHealth
		if err := readAuthorityBaselineJSON(ctx, "http://"+pod.Status.PodIP+":"+strconv.Itoa(frontReadyPort)+"/readyz", &health); err != nil || health.Status != "ok" ||
			health.Generation < 1 || releaseguardian.AuthoritySlot(health.ActiveSlot).Validate() != nil || health.RouteAuthority != "edge-control" ||
			!exactSourceSHA(health.WorkerSourceCommit) || !exactSHA256Digest(health.WorkerImageDigest) {
			if before.BaselineReceiptDigest != "" && executor != nil {
				return observeBaselineFrontsFromActivation(ctx, client, namespace, config, executor)
			}
			return nil, errors.New("Front baseline readiness is invalid")
		}
		if strings.TrimSpace(pod.Annotations["fugue.pro/source-commit"]) != health.WorkerSourceCommit ||
			containerRuntimeImageDigest(pod, "edge-front") != health.WorkerImageDigest {
			if before.BaselineReceiptDigest != "" && executor != nil {
				return observeBaselineFrontsFromActivation(ctx, client, namespace, config, executor)
			}
			return nil, errors.New("Front runtime does not match its activation")
		}
		result[pod.Spec.NodeName] = observedBaselineFront{pod: pod, health: health}
	}
	return result, nil
}

// observeBaselineFrontsFromActivation is only used while normalizing an
// existing baseline after a historical Front outage. The Front process may be
// unready, but its node-local activation CAS remains readable from a Worker.
// This produces planning evidence only; worker health and public-route
// attestation below still have to prove the exact record and runtime identity.
func observeBaselineFrontsFromActivation(ctx context.Context, client kubernetes.Interface, namespace string, config authorityBaselineConfig, executor podCommandExecutor) (map[string]observedBaselineFront, error) {
	selector := labels.Set{"fugue.io/edge-group-id": config.GroupID}.AsSelector().String()
	list, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: authorityRecoveryCohortLimit(config.ExpectedNodes)})
	if err != nil || list.Continue != "" {
		return nil, errors.New("Front activation recovery cohort is unavailable")
	}
	fronts := make(map[string]corev1.Pod, config.ExpectedNodes)
	workers := make(map[string][]corev1.Pod, config.ExpectedNodes)
	for index := range list.Items {
		pod := list.Items[index]
		if pod.DeletionTimestamp != nil || pod.UID == "" || pod.ResourceVersion == "" || strings.TrimSpace(pod.Spec.NodeName) == "" {
			return nil, errors.New("Front activation recovery Pod identity is incomplete")
		}
		isFront, isWorker := false, false
		for _, container := range pod.Spec.Containers {
			isFront = isFront || container.Name == "edge-front"
			isWorker = isWorker || container.Name == "edge"
		}
		if isFront && pod.Labels["app.kubernetes.io/component"] == config.FrontComponent {
			if _, exists := fronts[pod.Spec.NodeName]; exists {
				return nil, errors.New("Front activation recovery cohort has duplicate Front nodes")
			}
			fronts[pod.Spec.NodeName] = pod
		}
		if isWorker {
			workers[pod.Spec.NodeName] = append(workers[pod.Spec.NodeName], pod)
		}
	}
	if len(fronts) != config.ExpectedNodes || len(workers) != config.ExpectedNodes {
		return nil, errors.New("Front activation recovery cohort is incomplete")
	}
	result := make(map[string]observedBaselineFront, config.ExpectedNodes)
	activeSlot := ""
	for node, candidates := range workers {
		var state edgegroupfront.ActivationState
		var readErr error
		for _, worker := range candidates {
			state, readErr = readBaselineActivation(ctx, executor, namespace, worker.Name, config.GroupID)
			if readErr == nil {
				break
			}
		}
		if readErr != nil {
			return nil, fmt.Errorf("read Front activation recovery witness on node %s: %w", node, readErr)
		}
		if activeSlot == "" {
			activeSlot = state.ActiveSlot
		} else if activeSlot != state.ActiveSlot {
			return nil, errors.New("Front activation recovery cohort has mixed active slots")
		}
		front := fronts[node]
		result[node] = observedBaselineFront{pod: front, health: baselineFrontHealth{Status: "recovery-witness",
			ActiveSlot: state.ActiveSlot, Generation: state.Generation, BundleGeneration: state.BundleGeneration,
			WorkerSourceCommit: state.WorkerSourceCommit, WorkerImageDigest: state.WorkerImageDigest,
			RouteAuthority: state.Authority}}
	}
	return result, nil
}

func readBaselineActivation(ctx context.Context, executor podCommandExecutor, namespace, pod, groupID string) (edgegroupfront.ActivationState, error) {
	raw, err := executor.Exec(ctx, namespace, pod, "edge", "cat", frontActivationStatePath)
	if err != nil {
		return edgegroupfront.ActivationState{}, err
	}
	var state edgegroupfront.ActivationState
	if decodeStrictJSON(raw, &state) != nil {
		return edgegroupfront.ActivationState{}, errors.New("Front activation recovery witness is invalid")
	}
	validOperation := state.Operation == edgegroupfront.ActivationOperationInit || state.Operation == edgegroupfront.ActivationOperationPromote || state.Operation == edgegroupfront.ActivationOperationRollback
	validPreviousSlot := state.PreviousSlot == "" || state.PreviousSlot == "a" || state.PreviousSlot == "b"
	validRollback := state.Operation != edgegroupfront.ActivationOperationRollback || state.RollbackOfGeneration > 0
	if state.Schema != edgegroupfront.ActivationStateSchemaV1 || state.GroupID != groupID || state.Generation == 0 ||
		(state.ActiveSlot != "a" && state.ActiveSlot != "b") || !validPreviousSlot || state.Authority != edgegroupfront.ActivationAuthority || !validOperation || !validRollback ||
		strings.TrimSpace(state.BundleGeneration) == "" || !exactSourceSHA(state.WorkerSourceCommit) || !exactSHA256Digest(state.WorkerImageDigest) || state.UpdatedAt.IsZero() {
		return edgegroupfront.ActivationState{}, errors.New("Front activation recovery witness is invalid")
	}
	return state, nil
}

func observeBaselineWorkers(ctx context.Context, client kubernetes.Interface, namespace string, config authorityBaselineConfig, slot releaseguardian.AuthoritySlot, fronts map[string]observedBaselineFront, current releaseguardian.CurrentAuthority) (map[string]corev1.Pod, string, int64, []releaseguardian.AuthorityBaselineNodeWitness, error) {
	selector := labels.Set{"fugue.io/edge-group-id": config.GroupID, "fugue.io/edge-slot": string(slot)}.AsSelector().String()
	list, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: int64(config.ExpectedNodes + 1)})
	if err != nil || list.Continue != "" || len(list.Items) != config.ExpectedNodes {
		return nil, "", 0, nil, errors.New("active worker baseline cohort is unavailable")
	}
	workers := make(map[string]corev1.Pod, len(list.Items))
	witnesses := make([]releaseguardian.AuthorityBaselineNodeWitness, 0, len(list.Items))
	recordDigest := ""
	var epoch uint64
	for index := range list.Items {
		pod := list.Items[index]
		front, exists := fronts[pod.Spec.NodeName]
		if !exists || pod.DeletionTimestamp != nil || pod.UID == "" || pod.ResourceVersion == "" || pod.Status.PodIP == "" || !podReady(pod.Status.Conditions) {
			return nil, "", 0, nil, errors.New("active worker baseline Pod is invalid")
		}
		var health baselineWorkerHealth
		if err := readAuthorityBaselineJSON(ctx, "http://"+pod.Status.PodIP+":"+strconv.Itoa(workerHealthPort)+"/healthz", &health); err != nil || !health.Healthy ||
			health.EdgeGroupID != config.GroupID || health.CandidateWorkerSlot != string(slot) ||
			!exactSHA256Digest(health.CandidateRecordDigest) || health.PublicationSequence == 0 ||
			!strings.HasPrefix(health.BundleVersion, health.ServingGeneration+".p") || !strings.Contains(health.BundleVersion, ".p"+strconv.FormatUint(health.PublicationSequence, 10)+".") {
			return nil, "", 0, nil, errors.New("active worker baseline health is invalid")
		}
		source := strings.TrimSpace(pod.Annotations["fugue.pro/source-commit"])
		image := edgeRuntimeImageDigest(pod)
		if source != front.health.WorkerSourceCommit || image != front.health.WorkerImageDigest || front.health.BundleGeneration != health.BundleVersion {
			return nil, "", 0, nil, errors.New("Front and active worker baseline identities differ")
		}
		if recordDigest == "" {
			recordDigest, epoch = health.CandidateRecordDigest, health.PublicationSequence
		} else if recordDigest != health.CandidateRecordDigest || epoch != health.PublicationSequence {
			return nil, "", 0, nil, errors.New("active worker baseline contains mixed route records")
		}
		workers[pod.Spec.NodeName] = pod
		witnesses = append(witnesses, releaseguardian.AuthorityBaselineNodeWitness{NodeName: pod.Spec.NodeName,
			FrontPodUID: string(front.pod.UID), FrontResourceVersion: front.pod.ResourceVersion, WorkerPodUID: string(pod.UID), WorkerResourceVersion: pod.ResourceVersion,
			ActivationGeneration: front.health.Generation, BundleGeneration: health.BundleVersion, ServingGeneration: health.ServingGeneration,
			WorkerSourceSHA: source, WorkerImageDigest: image})
	}
	return workers, recordDigest, int64(epoch), witnesses, nil
}

func baselineWorkerHealthMatchesCurrent(health baselineWorkerHealth, front baselineFrontHealth, slot releaseguardian.AuthoritySlot, current releaseguardian.CurrentAuthority) bool {
	return current.BaselineReceiptDigest != "" && current.CurrentWorkerSlot == slot &&
		current.CurrentRecordDigest == health.CandidateRecordDigest &&
		current.CurrentFrontGeneration == front.Generation &&
		current.CurrentBundleGeneration == health.BundleVersion && current.CurrentBundleGeneration == front.BundleGeneration &&
		current.CurrentWorkerSourceSHA == front.WorkerSourceCommit && current.CurrentWorkerImageDigest == front.WorkerImageDigest
}

func edgeRuntimeImageDigest(pod corev1.Pod) string {
	return containerRuntimeImageDigest(pod, "edge")
}

func containerRuntimeImageDigest(pod corev1.Pod, container string) string {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == container && status.Ready && status.RestartCount == 0 {
			return imageDigest(status.ImageID)
		}
	}
	return ""
}

func readBoundedJSON(ctx context.Context, endpoint string, destination any) error {
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint, nil)
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("health endpoint rejected baseline observation")
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("health endpoint has trailing data")
	}
	return nil
}

var readAuthorityBaselineJSON = readBoundedJSON
var requestAuthorityBaselineRoute = requestCandidateRoute
