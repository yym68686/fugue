package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

const (
	edgeActivationStateSchema   = "edge-front-group-activation/v1"
	edgeActivationReceiptSchema = "edge-front-group-activation-receipt/v1"
	edgeActivationAuthority     = "edge-control"
	edgeActivationInitialize    = "initialize"
	edgeActivationPromote       = "promote"
	edgeActivationRollback      = "rollback"
	edgeGroupAuthoritySource    = "edge-control-group-authority/v1"
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
	Front       map[string]edgeGroupPod
	FrontHealth map[string]edgeFrontHealth
	WorkerA     map[string]edgeGroupPod
	WorkerB     map[string]edgeGroupPod
	ActiveSlot  string
}

type edgeGroupTransitionRuntime interface {
	Snapshot(context.Context) (edgeGroupState, error)
	ApplyResources(context.Context) error
	Roll(context.Context, string, declarativerelease.TargetIdentity, bool) (map[string]edgeGroupPod, error)
	SelectCASExecutor(context.Context, ...edgeGroupPod) (edgeGroupPod, error)
	ReadActivation(context.Context, edgeGroupPod) (edgeActivationState, bool, error)
	ActivationCAS(context.Context, edgeGroupPod, edgeActivationRequest) (edgeActivationReceipt, error)
	WaitFront(context.Context, string, string, string) (map[string]edgeFrontHealth, error)
	WaitActiveWorkerAuthority(context.Context, string, declarativerelease.TargetIdentity) error
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

	inactive, err := runtime.Roll(ctx, inactiveName, target, true)
	if err != nil {
		return fmt.Errorf("roll inactive edge slot %s: %w", inactiveSlot, err)
	}

	frontHealth := before.FrontHealth
	resumedExistingActivation := false
	if !allFrontActivationPresent(frontHealth) {
		resumedExistingActivation = true
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
			existing, exists, readErr := runtime.ReadActivation(ctx, executor)
			if readErr != nil {
				return fmt.Errorf("read edge activation on node %s: %w", node, readErr)
			}
			if exists {
				if existing.Schema != edgeActivationStateSchema || existing.GroupID != transition.GroupID || existing.Generation == 0 ||
					existing.ActiveSlot != activeSlot || existing.Authority != edgeActivationAuthority {
					return fmt.Errorf("existing edge activation on node %s is not bound to the live group and slot", node)
				}
				frontHealth[node] = edgeFrontHealth{ActiveSlot: existing.ActiveSlot, ActivationPresent: true, Generation: existing.Generation,
					BundleGeneration: existing.BundleGeneration, WorkerSourceCommit: existing.WorkerSourceCommit,
					WorkerImageDigest: existing.WorkerImageDigest, RouteAuthority: existing.Authority}
				continue
			}
			resumedExistingActivation = false
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
		frontPods, err = runtime.Roll(ctx, transition.FrontName, target, true)
		if err != nil {
			return fmt.Errorf("roll edge front: %w", err)
		}
		if !resumedExistingActivation {
			frontHealth, err = runtime.WaitFront(ctx, activeSlot, "", "")
			if err != nil {
				return fmt.Errorf("verify edge front after rollout: %w", err)
			}
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
		if _, err := runtime.Roll(ctx, activeName, target, true); err != nil {
			return fmt.Errorf("roll previous active edge slot %s: %w", activeSlot, err)
		}
		activeSlot = inactiveSlot
	}
	if !edgePodsMatchTarget(frontPods, target) {
		frontPods, err = runtime.Roll(ctx, transition.FrontName, target, true)
		if err != nil {
			return fmt.Errorf("roll edge front after activation: %w", err)
		}
	}

	if err := runtime.WaitActiveWorkerAuthority(ctx, edgeWorkerName(transition, activeSlot), target); err != nil {
		return fmt.Errorf("verify active edge worker authority: %w", err)
	}
	final, err := runtime.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("capture final edge group state: %w", err)
	}
	if !edgePodsMatchTarget(final.Front, target) || !edgePodsMatchTarget(final.WorkerA, target) || !edgePodsMatchTarget(final.WorkerB, target) {
		return errors.New("edge group did not converge all artifact workloads")
	}
	if err := validateEdgeGroupAuthority(final, transition); err != nil {
		return err
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

func (runtime *kubectlEdgeGroupRuntime) Roll(ctx context.Context, name string, target declarativerelease.TargetIdentity, requireGroupAuthority bool) (map[string]edgeGroupPod, error) {
	return runtime.cluster.rollEdgeDaemonSetTarget(ctx, runtime.client, runtime.release, runtime.transition, name, target, requireGroupAuthority)
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
		health, healthErr := cluster.readEdgeFrontHealth(ctx, pod)
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
	if !requireReady {
		return pods, nil
	}
	// Both the front and worker edge containers expose their readiness endpoint
	// on the canonical named port "health".  The worker resources have always
	// declared that name; using "http" here made prewrite adoption reject the
	// healthy live bootstrap cohort before any production write.
	var endpoints []podHTTPEndpoint
	var endpointErr error
	for _, portName := range edgeHealthPortNames() {
		endpoints, endpointErr = podHTTPEndpointsFromJSON(podsRaw, container, portName)
		if endpointErr == nil {
			break
		}
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
		if pod.Name == "" || pod.UID == "" || pod.ResourceVersion == "" || pod.NodeName == "" || pod.SourceCommit == "" || pod.ImageRef == "" || pod.ImageID == "" || (requireReady && !pod.Ready) {
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
	for slot, pods := range map[string]map[string]edgeGroupPod{"a": state.WorkerA, "b": state.WorkerB} {
		for node, pod := range pods {
			if !edgePodHasGroupAuthority(pod) {
				return fmt.Errorf("edge group slot %s node %s has no verified group authority publication", slot, node)
			}
			if slot == state.ActiveSlot && !edgePodHasActiveInventory(pod) {
				return fmt.Errorf("edge group active slot %s node %s has no verified inventory heartbeat", slot, node)
			}
		}
	}
	if state.ActiveSlot != "a" && state.ActiveSlot != "b" {
		return errors.New("edge group authority active slot is invalid")
	}
	if transition.GroupID == "" {
		return errors.New("edge group authority transition is unbound")
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
	return cluster.rollEdgeDaemonSetTarget(ctx, client, release, transition, name, target, true)
}

func (cluster *kubectlCluster) rollEdgeDaemonSetTarget(ctx context.Context, client dynamic.Interface, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, name string, target declarativerelease.TargetIdentity, requireGroupAuthority bool) (map[string]edgeGroupPod, error) {
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
	raw, err := cluster.kubectlRun(ctx, nil, "exec", "--namespace", release.Workload.Namespace, pod.Name, "--container", transition.WorkerContainer,
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
		front, err := cluster.readEdgeDaemonSetPods(ctx, release, transition.FrontName, "edge-front", transition.ExpectedNodes, transition.GroupID, false)
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
