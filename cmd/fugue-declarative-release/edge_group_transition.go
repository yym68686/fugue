package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	edgeActivationStateSchema   = "edge-front-group-activation/v1"
	edgeActivationReceiptSchema = "edge-front-group-activation-receipt/v1"
	edgeActivationAuthority     = "edge-control"
	edgeActivationInitialize    = "initialize"
	edgeActivationPromote       = "promote"
	edgeActivationRollback      = "rollback"
)

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
	Name             string
	UID              string
	ResourceVersion  string
	NodeName         string
	SourceCommit     string
	ImageRef         string
	ImageID          string
	BundleGeneration string
	Ready            bool
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
	Front       map[string]edgeGroupPod
	FrontHealth map[string]edgeFrontHealth
	WorkerA     map[string]edgeGroupPod
	WorkerB     map[string]edgeGroupPod
	ActiveSlot  string
}

type edgeGroupTransitionRuntime interface {
	Snapshot(context.Context) (edgeGroupState, error)
	ApplyResources(context.Context) error
	Roll(context.Context, string, declarativerelease.TargetIdentity) (map[string]edgeGroupPod, error)
	SelectCASExecutor(context.Context, ...edgeGroupPod) (edgeGroupPod, error)
	ActivationCAS(context.Context, edgeGroupPod, edgeActivationRequest) (edgeActivationReceipt, error)
	WaitFront(context.Context, string, string, string) (map[string]edgeFrontHealth, error)
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
	config, err := clientcmd.BuildConfigFromFlags("", "")
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

func executeEdgeGroupAB(ctx context.Context, runtime edgeGroupTransitionRuntime, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, target declarativerelease.TargetIdentity) error {
	before, err := runtime.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("capture edge group prewrite state: %w", err)
	}
	if err := runtime.ApplyResources(ctx); err != nil {
		return err
	}

	activeSlot := before.ActiveSlot
	inactiveSlot := otherEdgeSlot(activeSlot)
	inactiveName := edgeWorkerName(transition, inactiveSlot)
	activeName := edgeWorkerName(transition, activeSlot)
	desiredDigest, err := immutableDigestFromRef(target.ImageRef)
	if err != nil {
		return err
	}

	inactive, err := runtime.Roll(ctx, inactiveName, target)
	if err != nil {
		return fmt.Errorf("roll inactive edge slot %s: %w", inactiveSlot, err)
	}

	frontHealth := before.FrontHealth
	if !allFrontActivationPresent(frontHealth) {
		activeWorkers := edgeWorkerPods(before, activeSlot)
		for _, node := range sortedEdgeNodes(before.Front) {
			if frontHealth[node].ActivationPresent {
				continue
			}
			current := activeWorkers[node]
			executor := inactive[node]
			if !current.Ready || current.BundleGeneration == "" {
				return fmt.Errorf("active edge slot %s on node %s has no healthy bundle", activeSlot, node)
			}
			currentDigest, digestErr := immutableDigestFromRef(current.ImageRef)
			if digestErr != nil {
				return fmt.Errorf("active edge slot %s on node %s image: %w", activeSlot, node, digestErr)
			}
			receipt, err := runtime.ActivationCAS(ctx, executor, edgeActivationRequest{
				GroupID: transition.GroupID, ExpectedGeneration: 0, ExpectedSlot: activeSlot, TargetSlot: activeSlot,
				BundleGeneration: current.BundleGeneration, WorkerSourceCommit: current.SourceCommit, WorkerImageDigest: currentDigest,
				Operation: edgeActivationInitialize, Reason: "initialize declarative edge group activation",
			})
			if err != nil {
				return fmt.Errorf("initialize edge activation on node %s: %w", node, err)
			}
			frontHealth[node] = edgeFrontHealth{ActiveSlot: receipt.Current.ActiveSlot, ActivationPresent: true, Generation: receipt.Current.Generation,
				BundleGeneration: receipt.Current.BundleGeneration, WorkerSourceCommit: receipt.Current.WorkerSourceCommit,
				WorkerImageDigest: receipt.Current.WorkerImageDigest, RouteAuthority: receipt.Current.Authority}
		}
	}

	rollback := target.ConfigSHA == release.ExpectedPreviousConfigSHA
	frontPods := before.Front
	if !rollback && !edgePodsMatchTarget(frontPods, target) {
		frontPods, err = runtime.Roll(ctx, transition.FrontName, target)
		if err != nil {
			return fmt.Errorf("roll edge front: %w", err)
		}
		frontHealth, err = runtime.WaitFront(ctx, activeSlot, "", "")
		if err != nil {
			return fmt.Errorf("verify edge front after rollout: %w", err)
		}
	}

	activeWorkers := edgeWorkerPods(before, activeSlot)
	if !edgePodsMatchTarget(activeWorkers, target) {
		for _, node := range sortedEdgeNodes(frontPods) {
			state := frontHealth[node]
			executor, execErr := runtime.SelectCASExecutor(ctx, activeWorkers[node], inactive[node])
			if execErr != nil {
				return fmt.Errorf("select edge CAS executor on node %s: %w", node, execErr)
			}
			operation := edgeActivationPromote
			rollbackOf := uint64(0)
			if rollback {
				operation = edgeActivationRollback
				rollbackOf = state.Generation
			}
			receipt, casErr := runtime.ActivationCAS(ctx, executor, edgeActivationRequest{
				GroupID: transition.GroupID, ExpectedGeneration: state.Generation, ExpectedSlot: activeSlot, TargetSlot: inactiveSlot,
				BundleGeneration: inactive[node].BundleGeneration, WorkerSourceCommit: target.ConfigSHA, WorkerImageDigest: desiredDigest,
				Operation: operation, RollbackOfGeneration: rollbackOf, Reason: "promote declarative edge group target",
			})
			if casErr != nil {
				return fmt.Errorf("switch edge activation on node %s: %w", node, casErr)
			}
			if receipt.Current.Generation != state.Generation+1 || receipt.Current.ActiveSlot != inactiveSlot {
				return fmt.Errorf("edge activation receipt on node %s did not bind the target", node)
			}
		}
		frontHealth, err = runtime.WaitFront(ctx, inactiveSlot, target.ConfigSHA, desiredDigest)
		if err != nil {
			return fmt.Errorf("verify promoted edge activation: %w", err)
		}
		if _, err := runtime.Roll(ctx, activeName, target); err != nil {
			return fmt.Errorf("roll previous active edge slot %s: %w", activeSlot, err)
		}
	}
	if !edgePodsMatchTarget(frontPods, target) {
		frontPods, err = runtime.Roll(ctx, transition.FrontName, target)
		if err != nil {
			return fmt.Errorf("roll edge front after activation: %w", err)
		}
	}

	final, err := runtime.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("capture final edge group state: %w", err)
	}
	if !edgePodsMatchTarget(final.Front, target) || !edgePodsMatchTarget(final.WorkerA, target) || !edgePodsMatchTarget(final.WorkerB, target) {
		return errors.New("edge group did not converge all artifact workloads")
	}
	if final.ActiveSlot != inactiveSlot && !edgePodsMatchTarget(activeWorkers, target) {
		return errors.New("edge group activation did not converge to the promoted slot")
	}
	return nil
}

func (runtime *kubectlEdgeGroupRuntime) Snapshot(ctx context.Context) (edgeGroupState, error) {
	return runtime.cluster.readEdgeGroupState(ctx, runtime.release, runtime.transition)
}

func (runtime *kubectlEdgeGroupRuntime) ApplyResources(ctx context.Context) error {
	return runtime.cluster.applyResourceSet(ctx, runtime.release, runtime.manifest, false)
}

func (runtime *kubectlEdgeGroupRuntime) Roll(ctx context.Context, name string, target declarativerelease.TargetIdentity) (map[string]edgeGroupPod, error) {
	return runtime.cluster.rollEdgeDaemonSet(ctx, runtime.client, runtime.release, runtime.transition, name, target)
}

func (runtime *kubectlEdgeGroupRuntime) SelectCASExecutor(ctx context.Context, candidates ...edgeGroupPod) (edgeGroupPod, error) {
	return runtime.cluster.selectEdgeCASExecutor(ctx, runtime.release.Workload.Namespace, runtime.transition, candidates...)
}

func (runtime *kubectlEdgeGroupRuntime) ActivationCAS(ctx context.Context, pod edgeGroupPod, request edgeActivationRequest) (edgeActivationReceipt, error) {
	return runtime.cluster.runEdgeActivationCAS(ctx, runtime.release, runtime.transition, pod, request)
}

func (runtime *kubectlEdgeGroupRuntime) WaitFront(ctx context.Context, slot, source, digest string) (map[string]edgeFrontHealth, error) {
	return runtime.cluster.waitFrontActivation(ctx, runtime.release, runtime.transition, slot, source, digest)
}

func (cluster *kubectlCluster) readEdgeGroupState(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition) (edgeGroupState, error) {
	front, err := cluster.readEdgeDaemonSetPods(ctx, release, transition.FrontName, "edge-front", transition.ExpectedNodes, transition.GroupID, false)
	if err != nil {
		return edgeGroupState{}, err
	}
	workerA, err := cluster.readEdgeDaemonSetPods(ctx, release, transition.WorkerAName, transition.WorkerContainer, transition.ExpectedNodes, transition.GroupID, true)
	if err != nil {
		return edgeGroupState{}, err
	}
	workerB, err := cluster.readEdgeDaemonSetPods(ctx, release, transition.WorkerBName, transition.WorkerContainer, transition.ExpectedNodes, transition.GroupID, true)
	if err != nil {
		return edgeGroupState{}, err
	}
	if !sameEdgeNodes(front, workerA) || !sameEdgeNodes(front, workerB) {
		return edgeGroupState{}, errors.New("edge group workloads do not share one exact node cohort")
	}
	frontHealth := make(map[string]edgeFrontHealth, len(front))
	activeSlot := ""
	for node, pod := range front {
		health, healthErr := cluster.readEdgeFrontHealth(ctx, release.Workload.Namespace, pod.Name)
		if healthErr != nil {
			return edgeGroupState{}, healthErr
		}
		if activeSlot == "" {
			activeSlot = health.ActiveSlot
		} else if activeSlot != health.ActiveSlot {
			return edgeGroupState{}, errors.New("edge group front nodes disagree on active slot")
		}
		frontHealth[node] = health
	}
	if activeSlot != "a" && activeSlot != "b" {
		return edgeGroupState{}, errors.New("edge group active slot is invalid")
	}
	return edgeGroupState{Front: front, FrontHealth: frontHealth, WorkerA: workerA, WorkerB: workerB, ActiveSlot: activeSlot}, nil
}

func (cluster *kubectlCluster) readEdgeDaemonSetPods(ctx context.Context, release declarativerelease.PlanRelease, name, container string, expectedNodes int, groupID string, includeWorkerHealth bool) (map[string]edgeGroupPod, error) {
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
	pods, err := parseEdgeGroupPods(podsRaw, container, expectedNodes, groupID)
	if err != nil {
		return nil, fmt.Errorf("parse DaemonSet/%s pods: %w", name, err)
	}
	if includeWorkerHealth {
		for node, pod := range pods {
			bundle, healthErr := cluster.readEdgeWorkerHealth(ctx, release.Workload.Namespace, pod.Name, groupID)
			if healthErr != nil {
				return nil, healthErr
			}
			pod.BundleGeneration = bundle
			pods[node] = pod
		}
	}
	return pods, nil
}

func parseEdgeGroupPods(raw []byte, container string, expectedNodes int, groupID string) (map[string]edgeGroupPod, error) {
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
		if labels["fugue.io/edge-group-id"] != groupID {
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
				if int64Value(entry["restartCount"]) != 0 {
					pod.Ready = false
				}
				pod.ImageID = stringValue(entry["imageID"])
			}
		}
		if pod.Name == "" || pod.UID == "" || pod.ResourceVersion == "" || pod.NodeName == "" || pod.SourceCommit == "" || pod.ImageRef == "" || pod.ImageID == "" || !pod.Ready {
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

func (cluster *kubectlCluster) readEdgeWorkerHealth(ctx context.Context, namespace, pod, groupID string) (string, error) {
	body, err := cluster.kubectlRun(ctx, nil, "get", "--raw", fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:health/proxy/healthz", namespace, pod))
	if err != nil {
		return "", err
	}
	value, err := decodeJSONObject(body)
	if err != nil || value["healthy"] != true || stringValue(value["edge_group_id"]) != groupID || strings.TrimSpace(stringValue(value["bundle_version"])) == "" {
		return "", errors.New("edge worker health is not group-bound and healthy")
	}
	return stringValue(value["bundle_version"]), nil
}

func (cluster *kubectlCluster) readEdgeFrontHealth(ctx context.Context, namespace, pod string) (edgeFrontHealth, error) {
	body, err := cluster.kubectlRun(ctx, nil, "get", "--raw", fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:health/proxy/readyz", namespace, pod))
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
	container := transition.WorkerContainer
	includeHealth := true
	if name == transition.FrontName {
		container = "edge-front"
		includeHealth = false
	}
	current, err := cluster.readEdgeDaemonSetPods(ctx, release, name, container, transition.ExpectedNodes, transition.GroupID, includeHealth)
	if err != nil {
		return nil, err
	}
	for _, node := range sortedEdgeNodes(current) {
		pod := current[node]
		if edgePodMatchesTarget(pod, target) {
			continue
		}
		if err := deleteEdgePodExact(ctx, client, release.Workload.Namespace, pod); err != nil {
			return nil, err
		}
		if _, err := cluster.waitEdgePodTarget(ctx, release, transition, name, container, node, pod.UID, target, includeHealth); err != nil {
			return nil, err
		}
	}
	return cluster.readEdgeDaemonSetPods(ctx, release, name, container, transition.ExpectedNodes, transition.GroupID, includeHealth)
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

func (cluster *kubectlCluster) waitEdgePodTarget(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, name, container, node, priorUID string, target declarativerelease.TargetIdentity, includeHealth bool) (edgeGroupPod, error) {
	deadline := time.Now().Add(cluster.timeout)
	for {
		pods, err := cluster.readEdgeDaemonSetPods(ctx, release, name, container, transition.ExpectedNodes, transition.GroupID, includeHealth)
		if err == nil {
			pod, exists := pods[node]
			if exists && pod.UID != priorUID && edgePodMatchesTarget(pod, target) {
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
	for _, pod := range candidates {
		if pod.Name == "" {
			continue
		}
		if _, err := cluster.kubectlRun(ctx, nil, "exec", "--namespace", namespace, pod.Name, "--container", transition.WorkerContainer, "--", "test", "-x", transition.CASBinary); err == nil {
			return pod, nil
		}
	}
	return edgeGroupPod{}, errors.New("no group-local worker contains the fixed activation CAS binary")
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

func (cluster *kubectlCluster) waitFrontActivation(ctx context.Context, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, slot, source, digest string) (map[string]edgeFrontHealth, error) {
	deadline := time.Now().Add(cluster.timeout)
	for {
		front, err := cluster.readEdgeDaemonSetPods(ctx, release, transition.FrontName, "edge-front", transition.ExpectedNodes, transition.GroupID, false)
		if err == nil {
			health := make(map[string]edgeFrontHealth, len(front))
			matched := true
			for node, pod := range front {
				item, healthErr := cluster.readEdgeFrontHealth(ctx, release.Workload.Namespace, pod.Name)
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
	return pod.Ready && pod.SourceCommit == target.ConfigSHA && pod.ImageRef == target.ImageRef
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
