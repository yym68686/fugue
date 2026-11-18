package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"

	appsv1 "k8s.io/api/apps/v1"
	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const storageUnmountTransitionAnnotation = "fugue.pro/storage-unmount-transition"
const storageUnmountDeadline = 4 * time.Minute

var errStorageUnmountUnsettled = errors.New("storage unmount state is not yet verified")
var errStorageUnmountWaiting = errors.New("replacement is not yet ready for its business probe")

type storageUnmountTransition struct {
	OperationID   string    `json:"operation_id"`
	SourceKey     string    `json:"source_key"`
	CandidateKey  string    `json:"candidate_key"`
	SourcePod     string    `json:"source_pod"`
	SourceUID     string    `json:"source_uid"`
	ReplicaSet    string    `json:"replica_set"`
	ReplicaSetUID string    `json:"replica_set_uid"`
	Node          string    `json:"node"`
	Claim         string    `json:"claim"`
	ClaimUID      string    `json:"claim_uid"`
	PVUID         string    `json:"pv_uid"`
	ImageID       string    `json:"image_id"`
	MinReady      int32     `json:"min_ready"`
	Deadline      time.Time `json:"deadline"`
	RetireAllowed bool      `json:"retire_allowed,omitempty"`
}

func (s *Service) executeAppStorageUnmount(ctx context.Context, op model.Operation, app model.App) error {
	if !s.Config.KubectlApply || op.StorageUnmount == nil || op.ConfigBaseSpec == nil || op.DesiredSpec == nil {
		return fmt.Errorf("online unmount requires a managed Kubernetes operation with immutable request and baseline")
	}
	if app.ID != op.AppID || app.TenantID != op.TenantID {
		return fmt.Errorf("storage unmount operation ownership differs")
	}
	ctx, cancel := context.WithTimeout(ctx, storageUnmountDeadline+30*time.Second)
	defer cancel()
	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("%w: initialize managed runtime client: %v", errStorageUnmountUnsettled, err)
	}
	ns, name := runtime.NamespaceForTenant(app.TenantID), runtime.RuntimeAppResourceName(app)
	depPath := deploymentAPIPath(ns, name)
	lastMessage := op.ResultMessage
	for {
		finished, err := s.stepAppStorageUnmount(ctx, client, op, app, ns, name, depPath)
		message := ""
		switch {
		case errors.Is(err, errStorageUnmountWaiting):
			message = err.Error()
		case errors.Is(err, errStorageUnmountUnsettled):
			message = "storage unmount interlock retained: awaiting verifiable workload state"
		}
		if message != "" && message != lastMessage && ctx.Err() == nil {
			if _, updateErr := s.Store.UpdateOperationProgress(op.ID, message); updateErr == nil {
				lastMessage = message
			}
		}
		if finished || err != nil && !errors.Is(err, errStorageUnmountWaiting) {
			return err
		}
		if s.Logger != nil && err != nil {
			s.Logger.Printf("storage unmount app=%s operation=%s waiting: %v", app.ID, op.ID, err)
		}
		timer := time.NewTimer(3 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%w: %v", errStorageUnmountUnsettled, ctx.Err())
		case <-timer.C:
		}
	}
}

func (s *Service) stepAppStorageUnmount(ctx context.Context, c *kubeClient, op model.Operation, app model.App, ns, name, depPath string) (bool, error) {
	unsettled := func(err error) (bool, error) {
		return false, fmt.Errorf("%w: %v", errStorageUnmountUnsettled, err)
	}
	if err := s.ensureOperationStillActiveContext(ctx, op.ID); err != nil {
		return false, err
	}
	var dep appsv1.Deployment
	if _, err := c.doJSON(ctx, http.MethodGet, depPath, nil, &dep); err != nil {
		return unsettled(fmt.Errorf("observe unmount Deployment: %w", err))
	}
	if dep.UID == "" || dep.ResourceVersion == "" || dep.DeletionTimestamp != nil ||
		dep.Labels[runtime.FugueLabelAppID] != app.ID || dep.Labels[runtime.FugueLabelTenantID] != app.TenantID {
		return unsettled(fmt.Errorf("unmount Deployment ownership is unavailable"))
	}
	var transition storageUnmountTransition
	marker := dep.Annotations[storageUnmountTransitionAnnotation]
	if marker != "" && (json.Unmarshal([]byte(marker), &transition) != nil || transition.OperationID != op.ID ||
		transition.CandidateKey != dep.Spec.Template.Annotations[runtime.FugueAnnotationReleaseKey]) {
		return unsettled(fmt.Errorf("Deployment has different rollout ownership"))
	}
	if marker != "" && (!storageUnmountDeploymentShapeMatches(dep.Spec, transition) ||
		dep.Spec.Template.Spec.NodeSelector[kubeHostnameLabelKey] != transition.Node ||
		!storageUnmountTemplateHasGate(normalizeKubeMap(dep.Spec.Template), op.ID)) {
		return unsettled(fmt.Errorf("candidate Deployment no longer enforces the same-node gated RollingUpdate"))
	}
	if model.AppSpecSHA256(app.Spec) != model.AppSpecSHA256(*op.ConfigBaseSpec) {
		if marker != "" {
			return unsettled(fmt.Errorf("app specification changed after an unmount candidate was created"))
		}
		return false, fmt.Errorf("app specification changed since the unmount plan")
	}
	want, _, err := model.AppSpecWithoutPersistentMount(app.Spec, op.StorageUnmount.Path)
	if err != nil || model.AppSpecSHA256(want) != model.AppSpecSHA256(*op.DesiredSpec) || op.StorageUnmount.Validate(app.Spec) != nil {
		if marker != "" {
			return unsettled(fmt.Errorf("the persisted unmount intent no longer matches its guarded baseline"))
		}
		return false, fmt.Errorf("unmount intent must change exactly one persistent mount")
	}
	managed, found, err := c.getManagedApp(ctx, ns, runtime.ManagedAppResourceName(app))
	if err != nil || !found || managed.Spec.AppID != app.ID || managed.Spec.TenantID != app.TenantID || managed.Metadata.DeletionTimestamp != "" {
		return unsettled(fmt.Errorf("cannot verify managed workload snapshot"))
	}
	if marker == "" {
		if op.StartedAt == nil || time.Since(*op.StartedAt) > storageUnmountDeadline {
			return false, fmt.Errorf("unmount preflight expired without starting a candidate")
		}
		return s.startAppStorageUnmount(ctx, c, op, app, want, managed, ns, name, depPath, dep)
	}

	candidate, scheduling, err := s.storageUnmountCandidate(app, want, managed, transition)
	if err != nil {
		return unsettled(err)
	}
	if s.Renderer.ManagedAppReleaseKey(candidate, scheduling) != transition.CandidateKey {
		return unsettled(fmt.Errorf("candidate template inputs changed after staging"))
	}
	pods, err := listStorageUnmountPods(ctx, c, ns, name)
	if err != nil {
		return unsettled(err)
	}
	var replacement *corev1.Pod
	var original *corev1.Pod
	for i := range pods {
		p := &pods[i]
		if p.DeletionTimestamp == nil && p.Annotations[runtime.FugueStorageUnmountOperationAnnotation] == op.ID &&
			p.Annotations[runtime.FugueAnnotationReleaseKey] == transition.CandidateKey {
			if replacement != nil {
				return unsettled(fmt.Errorf("more than one candidate Pod has the unmount identity"))
			}
			replacement = p
		}
		if string(p.UID) == transition.SourceUID && p.Name == transition.SourcePod {
			original = p
		}
	}
	if replacement != nil && (replacement.Spec.NodeName != "" && replacement.Spec.NodeName != transition.Node || replacement.Spec.HostNetwork ||
		!storageUnmountPodMountsMatch(replacement, transition.Claim, want.PersistentStorage, op.StorageUnmount.Path) || !storageUnmountPodHasGate(replacement)) {
		return unsettled(fmt.Errorf("candidate Pod does not match the authorized placement or readiness gate"))
	}
	if replacement != nil && storageUnmountPodCondition(replacement, runtime.FugueStorageUnmountReadyCondition) {
		rs, err := storageUnmountReplicaSet(ctx, c, ns, dep, *replacement)
		if err != nil || rs.Spec.Template.Annotations[runtime.FugueAnnotationReleaseKey] != transition.CandidateKey ||
			storageUnmountPrimaryImageID(replacement) != transition.ImageID {
			return unsettled(fmt.Errorf("candidate traffic identity differs from business-gated template"))
		}
		if !transition.RetireAllowed {
			if !dep.Spec.Paused {
				return unsettled(fmt.Errorf("original retirement was not authorized, but the Deployment is no longer paused"))
			}
			if !storageUnmountServingPod(original) || !storageUnmountServingPod(replacement) ||
				dep.Status.ObservedGeneration != dep.Generation || dep.Status.AvailableReplicas != 2 {
				return false, fmt.Errorf("%w: awaiting both serving Pods on the paused Deployment", errStorageUnmountWaiting)
			}
			if err := verifyStorageUnmountEndpoints(ctx, c, ns, runtime.RuntimeAppServiceName(app), original, replacement); err != nil {
				return false, fmt.Errorf("%w: both verified Pod identities are not yet published", errStorageUnmountWaiting)
			}
			transition.RetireAllowed = true
			if err := setStorageUnmountDeploymentPaused(ctx, c, depPath, dep, transition, false); err != nil {
				return unsettled(err)
			}
			return false, nil
		}
		if dep.Spec.Paused {
			return unsettled(fmt.Errorf("authorized retirement cannot continue on an externally paused Deployment"))
		}
		if !storageUnmountPodCondition(replacement, string(corev1.PodReady)) ||
			dep.Status.ObservedGeneration != dep.Generation || dep.Status.UpdatedReplicas != 1 ||
			dep.Status.ReadyReplicas != 1 || dep.Status.AvailableReplicas != 1 || dep.Status.Replicas != 1 {
			return false, fmt.Errorf("%w: awaiting the verified Pod's rolling deployment to converge", errStorageUnmountWaiting)
		}
		if err := verifyStorageUnmountEndpoints(ctx, c, ns, runtime.RuntimeAppServiceName(app), replacement); err != nil {
			return false, fmt.Errorf("%w: candidate EndpointSlice has not converged", errStorageUnmountWaiting)
		}
		if managed.Spec.StorageUnmountOperationID != op.ID {
			if err := patchStorageUnmountManagedSnapshot(ctx, c, managed, candidate, scheduling); err != nil {
				return unsettled(err)
			}
		}
		message := "persistent mount removed after candidate business verification; existing PVC and data retained"
		completed, err := s.finalizeManagedOperation(ctx, op, model.OperationStatusCompleted, func() (model.Operation, error) {
			return s.Store.CompleteManagedOperationGuarded(op, &app, "", message, nil, nil, nil)
		})
		if err != nil {
			return unsettled(err)
		}
		s.logOperationAppEvent("completed", "info", completed, app, message, map[string]any{
			"mount_path": op.StorageUnmount.Path, "claim": transition.Claim, "node": transition.Node, "candidate_pod": replacement.Name,
		})
		return true, nil
	}
	if !transition.RetireAllowed && time.Now().After(transition.Deadline) {
		if managed.Spec.StorageUnmountOperationID == op.ID || !storageUnmountServingPod(original) {
			return unsettled(fmt.Errorf("admission deadline passed but the original serving Pod is no longer proven; no further mutation"))
		}
		if err := restoreStorageUnmountDeployment(ctx, c, ns, depPath, dep, transition); err != nil {
			return unsettled(err)
		}
		return false, fmt.Errorf("candidate did not pass its business probe by the deadline; original Deployment template restored without Recreate")
	}
	if replacement == nil || !storageUnmountPodCondition(replacement, string(corev1.ContainersReady)) {
		return false, errStorageUnmountWaiting
	}
	if replacement.Spec.NodeName != transition.Node || replacement.Spec.HostNetwork ||
		storageUnmountPrimaryImageID(replacement) != transition.ImageID {
		return false, fmt.Errorf("%w: candidate placement or immutable image differs", errStorageUnmountWaiting)
	}
	if !transition.RetireAllowed && !storageUnmountServingPod(original) {
		return unsettled(fmt.Errorf("original serving Pod is no longer confirmed before cutover"))
	}
	claim, err := loadUnmountClaim(ctx, c, ns, transition.Claim, transition.Node, want.PersistentStorage.StorageClassName)
	if err != nil || string(claim.pvc.UID) != transition.ClaimUID || string(claim.pv.UID) != transition.PVUID {
		return unsettled(fmt.Errorf("live PVC or PV no longer matches the unmount preflight"))
	}
	if err := verifyStorageUnmountServices(ctx, c, ns, runtime.RuntimeAppServiceName(app), replacement.Labels); err != nil {
		return unsettled(err)
	}
	// A paused Deployment can still retire old ReplicaSets if its new
	// ReplicaSet satisfies the desired replica count. Hold two explicitly,
	// with no further surge, while the candidate joins the endpoints.
	if !transition.RetireAllowed && !dep.Spec.Paused {
		rs, err := storageUnmountReplicaSet(ctx, c, ns, dep, *replacement)
		if err != nil || rs.Spec.Replicas == nil || *rs.Spec.Replicas != 1 ||
			!storageUnmountTemplateHasGate(normalizeKubeMap(rs.Spec.Template), op.ID) {
			return unsettled(fmt.Errorf("cannot hold the independently owned candidate ReplicaSet"))
		}
		if err := setStorageUnmountDeploymentPaused(ctx, c, depPath, dep, transition, true); err != nil {
			return unsettled(err)
		}
		return false, nil
	}
	if dep.Status.ObservedGeneration != dep.Generation {
		return false, errStorageUnmountWaiting
	}
	if err := s.admitStorageUnmountPod(ctx, c, ns, dep, *replacement, op, candidate); err != nil {
		return false, fmt.Errorf("%w: %v", errStorageUnmountWaiting, err)
	}
	return false, nil
}

func (s *Service) storageUnmountCandidate(app model.App, want model.AppSpec, managed runtime.ManagedAppObject, t storageUnmountTransition) (model.App, runtime.SchedulingConstraints, error) {
	snapshot := runtime.AppFromManagedApp(managed)
	sourceStorage := snapshot.Spec.PersistentStorage
	if !reflect.DeepEqual(sourceStorage, app.Spec.PersistentStorage) && !reflect.DeepEqual(sourceStorage, want.PersistentStorage) {
		return model.App{}, runtime.SchedulingConstraints{}, fmt.Errorf("managed storage differs from the authorized source and candidate")
	}
	scheduling := cloneControllerSchedulingConstraints(managed.Spec.Scheduling)
	if scheduling.NodeSelector == nil {
		scheduling.NodeSelector = make(map[string]string)
	}
	if node := scheduling.NodeSelector[kubeHostnameLabelKey]; node != "" && node != t.Node {
		return model.App{}, runtime.SchedulingConstraints{}, fmt.Errorf("candidate node conflicts with the managed placement")
	}
	scheduling.NodeSelector[kubeHostnameLabelKey] = t.Node
	snapshot.Spec.PersistentStorage = want.PersistentStorage
	snapshot.Spec.StorageUnmountOperationID = t.OperationID
	return s.Renderer.PrepareApp(snapshot), scheduling, nil
}

func (s *Service) startAppStorageUnmount(ctx context.Context, c *kubeClient, op model.Operation, app model.App, want model.AppSpec, managed runtime.ManagedAppObject, ns, name, depPath string, dep appsv1.Deployment) (bool, error) {
	sourceKey := dep.Spec.Template.Annotations[runtime.FugueAnnotationReleaseKey]
	sourceApp := s.Renderer.PrepareApp(runtime.AppFromManagedApp(managed))
	if sourceKey == "" || dep.Annotations[runtime.FugueAnnotationReleaseKey] != sourceKey ||
		s.Renderer.ManagedAppReleaseKey(sourceApp, managed.Spec.Scheduling) != sourceKey ||
		dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 || dep.Spec.Paused || dep.Status.ObservedGeneration != dep.Generation ||
		dep.Status.Replicas != 1 || dep.Status.UpdatedReplicas != 1 || dep.Status.AvailableReplicas != 1 || dep.Spec.Template.Spec.HostNetwork {
		return false, fmt.Errorf("unmount requires a converged, identifiable single-Pod Deployment")
	}
	if policy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID); err == nil {
		stable, err := s.Store.GetAppRelease(app.TenantID, true, policy.StableReleaseID)
		if err != nil || policy.Mode != model.AppTrafficModeSingle || policy.StableWeight != 100 ||
			policy.CandidateReleaseID != "" || policy.CandidateWeight != 0 ||
			stable.DeploymentName != name || stable.ServiceName != runtime.RuntimeAppServiceName(app) || stable.RevisionWorkload != nil {
			return false, fmt.Errorf("unmount requires canonical serving traffic without an independent release")
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return false, fmt.Errorf("unable to observe canonical release routing: %w", err)
	}
	pods, err := listStorageUnmountPods(ctx, c, ns, name)
	if err != nil || len(pods) != 1 {
		return false, fmt.Errorf("unmount requires exactly one observed live Pod")
	}
	p := pods[0]
	if p.UID == "" || p.Spec.NodeName == "" || p.DeletionTimestamp != nil || p.Spec.HostNetwork ||
		storageUnmountPrimaryImageID(&p) == "" || !storageUnmountPodCondition(&p, string(corev1.PodReady)) ||
		p.Annotations[runtime.FugueAnnotationReleaseKey] != sourceKey {
		return false, fmt.Errorf("unmount requires a Ready Pod on a known node with an observed immutable image")
	}
	if !storageUnmountPodHasDrain(&p) {
		return false, fmt.Errorf("the currently serving Pod has no verified Fugue drain lifecycle; no live unmount performed")
	}
	rs, err := storageUnmountReplicaSet(ctx, c, ns, dep, p)
	if err != nil || rs.Spec.Template.Annotations[runtime.FugueAnnotationReleaseKey] != sourceKey {
		return false, fmt.Errorf("could not bind the prior ReplicaSet for safe rollback")
	}
	if err := verifyStorageUnmountEndpoints(ctx, c, ns, runtime.RuntimeAppServiceName(app), &p); err != nil {
		return false, err
	}
	if err := verifyStorageUnmountServices(ctx, c, ns, runtime.RuntimeAppServiceName(app), p.Labels); err != nil {
		return false, err
	}
	claimName := runtime.PersistentStoragePVCName(app, *app.Spec.PersistentStorage)
	if !storageUnmountPodMountsMatch(&p, claimName, app.Spec.PersistentStorage, "") || runtime.PersistentStoragePVCName(app, *want.PersistentStorage) != claimName {
		return false, fmt.Errorf("source and candidate must use exactly the same existing PVC")
	}
	claim, err := loadUnmountClaim(ctx, c, ns, claimName, p.Spec.NodeName, want.PersistentStorage.StorageClassName)
	if err != nil {
		return false, err
	}
	review := authv1.SelfSubjectAccessReview{Spec: authv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &authv1.ResourceAttributes{
		Namespace: ns, Verb: "patch", Version: "v1", Resource: "pods", Subresource: "status",
	}}}
	if _, err := c.doJSON(ctx, http.MethodPost, "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", review, &review); err != nil || !review.Status.Allowed {
		return false, fmt.Errorf("controller cannot write the Pod readiness gate; Deployment unchanged")
	}
	t := storageUnmountTransition{OperationID: op.ID, SourceKey: sourceKey, SourcePod: p.Name, SourceUID: string(p.UID),
		ReplicaSet: rs.Name, ReplicaSetUID: string(rs.UID), Node: p.Spec.NodeName, Claim: claimName, ClaimUID: string(claim.pvc.UID),
		PVUID: string(claim.pv.UID), ImageID: storageUnmountPrimaryImageID(&p), MinReady: dep.Spec.MinReadySeconds,
		Deadline: op.StartedAt.Add(storageUnmountDeadline)}
	candidate, scheduling, err := s.storageUnmountCandidate(app, want, managed, t)
	if err != nil {
		return false, err
	}
	unlock, err := s.lockManagedRolloutCapacityDomainContext(ctx, scheduling, op.ID)
	if err != nil {
		return false, err
	}
	defer unlock()
	if err := s.preflightManagedAppZeroDowntimeRolloutCapacity(ctx, candidate, scheduling); err != nil {
		return false, err
	}
	objects := s.Renderer.BuildManagedAppChildObjects(candidate, scheduling, nil)
	expected := firstManagedAppDeploymentObject(objects, name)
	if !zeroDowntimeDeploymentStrategyIsSafe(expected) {
		return false, fmt.Errorf("unmount candidate did not render a zero-unavailable RollingUpdate")
	}
	if !revisionAppliedFieldsMatch("spec.selector", normalizeKubeValue(dep.Spec.Selector), normalizeKubeValue(nestedObjectValue(expected, "spec", "selector"))) {
		return false, fmt.Errorf("unmount would change the live Deployment selector")
	}
	t.CandidateKey = s.Renderer.ManagedAppReleaseKey(candidate, scheduling)
	template := normalizeKubeMap(nestedObjectValue(expected, "spec", "template"))
	if t.CandidateKey == "" || t.CandidateKey == sourceKey || template == nil || !storageUnmountTemplateHasGate(template, op.ID) {
		return false, fmt.Errorf("unmount candidate is missing an immutable business readiness gate")
	}
	templateLabels := objectStringMapValue(nestedObjectValue(template, "metadata", "labels"))
	if err := verifyStorageUnmountServices(ctx, c, ns, runtime.RuntimeAppServiceName(app), templateLabels); err != nil {
		return false, err
	}
	encoded, err := json.Marshal(t)
	if err != nil {
		return false, err
	}
	ann := make(map[string]string, len(dep.Annotations)+2)
	for k, v := range dep.Annotations {
		ann[k] = v
	}
	ann[storageUnmountTransitionAnnotation] = string(encoded)
	ann[runtime.FugueAnnotationReleaseKey] = t.CandidateKey
	if err := s.ensureOperationStillActiveContext(ctx, op.ID); err != nil {
		return false, err
	}
	patch := storageUnmountDeploymentPatch(dep, ann, template, nestedObjectValue(expected, "spec", "strategy"), nestedObjectValue(expected, "spec", "minReadySeconds"))
	if status, err := c.doRequest(ctx, http.MethodPatch, depPath, "application/json-patch+json", patch, nil); err != nil {
		return false, fmt.Errorf("%w: Deployment write outcome unavailable (HTTP %d)", errStorageUnmountUnsettled, status)
	}
	return false, nil
}

func listStorageUnmountPods(ctx context.Context, c *kubeClient, ns, workload string) ([]corev1.Pod, error) {
	var list corev1.PodList
	path := "/api/v1/namespaces/" + url.PathEscape(ns) + "/pods?labelSelector=" + url.QueryEscape(runtime.FugueLabelAppWorkload+"="+workload)
	if _, err := c.doJSON(ctx, http.MethodGet, path, nil, &list); err != nil || list.Continue != "" {
		return nil, fmt.Errorf("unmount Pod inventory is unavailable")
	}
	return list.Items, nil
}

func storageUnmountPodCondition(p *corev1.Pod, condition string) bool {
	for _, c := range p.Status.Conditions {
		if string(c.Type) == condition {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func storageUnmountServingPod(p *corev1.Pod) bool {
	return p != nil && p.DeletionTimestamp == nil && storageUnmountPodCondition(p, string(corev1.PodReady))
}

func storageUnmountPrimaryImageID(p *corev1.Pod) string {
	if len(p.Spec.Containers) > 0 {
		for _, c := range p.Status.ContainerStatuses {
			if c.Name == p.Spec.Containers[0].Name && c.State.Running != nil && c.ContainerID != "" {
				return c.ImageID
			}
		}
	}
	return ""
}

func storageUnmountPodHasDrain(p *corev1.Pod) bool {
	if len(p.Spec.Containers) == 0 || p.Spec.Containers[0].Lifecycle == nil || p.Spec.Containers[0].Lifecycle.PreStop == nil ||
		p.Spec.TerminationGracePeriodSeconds == nil || *p.Spec.TerminationGracePeriodSeconds <= 0 {
		return false
	}
	hook := p.Spec.Containers[0].Lifecycle.PreStop
	if hook.Sleep != nil {
		return hook.Sleep.Seconds > 0 && hook.Sleep.Seconds < *p.Spec.TerminationGracePeriodSeconds
	}
	if hook.HTTPGet == nil || hook.HTTPGet.Path != "/drain/prestop" || hook.HTTPGet.Host != "" || hook.HTTPGet.Port.IntValue() <= 0 {
		return false
	}
	for _, statuses := range [][]corev1.ContainerStatus{p.Status.InitContainerStatuses, p.Status.ContainerStatuses} {
		for _, status := range statuses {
			if status.Name == "fugue-drain-agent" && status.Ready && status.State.Running != nil && status.ContainerID != "" {
				return true
			}
		}
	}
	return false
}

func storageUnmountPodMountsMatch(p *corev1.Pod, claim string, spec *model.AppPersistentStorageSpec, removed string) bool {
	if spec == nil || len(p.Spec.Containers) == 0 || len(spec.Mounts) == 0 {
		return false
	}
	volumeName := ""
	for _, v := range p.Spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			if v.PersistentVolumeClaim.ClaimName != claim || volumeName != "" || v.PersistentVolumeClaim.ReadOnly || v.Name == "" {
				return false
			}
			volumeName = v.Name
		}
	}
	if volumeName == "" {
		return false
	}
	for _, containers := range [][]corev1.Container{p.Spec.Containers, p.Spec.InitContainers} {
		for _, container := range containers {
			for _, mount := range container.VolumeMounts {
				if removed != "" && mount.MountPath == removed {
					return false
				}
			}
		}
	}
	want := make(map[string]string, len(spec.Mounts))
	for _, mount := range spec.Mounts {
		want[mount.Path] = model.AppPersistentStorageMountSubPath(mount)
	}
	for _, mount := range p.Spec.Containers[0].VolumeMounts {
		subpath, expected := want[mount.MountPath]
		if mount.Name != volumeName && !expected {
			continue
		}
		if mount.Name != volumeName || !expected || mount.SubPath != subpath || mount.SubPathExpr != "" || mount.ReadOnly {
			return false
		}
		delete(want, mount.MountPath)
	}
	return len(want) == 0
}

func storageUnmountReplicaSet(ctx context.Context, c *kubeClient, ns string, dep appsv1.Deployment, p corev1.Pod) (appsv1.ReplicaSet, error) {
	var rs appsv1.ReplicaSet
	owner := metav1.GetControllerOf(&p)
	if owner == nil || owner.Kind != "ReplicaSet" || owner.Name == "" || owner.UID == "" {
		return rs, fmt.Errorf("Pod has no bound ReplicaSet")
	}
	if _, err := c.doJSON(ctx, http.MethodGet, recoveryObjectPath(ns, "replicasets", owner.Name), nil, &rs); err != nil {
		return rs, err
	}
	if uid := metav1.GetControllerOf(&rs); rs.UID != owner.UID || uid == nil || uid.Kind != "Deployment" || uid.UID != dep.UID ||
		rs.DeletionTimestamp != nil {
		return rs, fmt.Errorf("ReplicaSet ownership differs")
	}
	return rs, nil
}

func loadUnmountClaim(ctx context.Context, c *kubeClient, ns, name, node, class string) (storagePlacementClaim, error) {
	placement, err := loadStoragePlacement(ctx, c, ns, []storageClaimRequest{{Name: name, Class: class}})
	if err != nil || placement == nil || len(placement.claims) != 1 {
		return storagePlacementClaim{}, fmt.Errorf("unable to verify the existing PVC and PV")
	}
	claim := placement.claims[0]
	pvc, pv := claim.pvc, claim.pv
	if pvc == nil || pv == nil || pvc.UID == "" || pv.UID == "" || pvc.Status.Phase != corev1.ClaimBound ||
		pvc.Spec.VolumeName != pv.Name || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != ns ||
		pv.Spec.ClaimRef.Name != name || pv.Spec.ClaimRef.UID != pvc.UID ||
		!filesystemRWO(pvc.Spec.AccessModes, pvc.Spec.VolumeMode) || !filesystemRWO(pv.Spec.AccessModes, pv.Spec.VolumeMode) ||
		!model.AppRWOStorageClassSupportsSameNodeOnlineMount(claim.class.Name) {
		return storagePlacementClaim{}, fmt.Errorf("expected a bound filesystem ReadWriteOnce volume with matching PVC/PV identities")
	}
	if claim.class.Name == model.AppStorageClassFugueLonghornRWO && (claim.driver != longhornCSIDriver || claim.class.Provisioner != longhornCSIDriver) ||
		claim.class.Name == model.AppStorageClassFugueLocalRWO && (pv.Spec.CSI != nil || pv.Spec.Local == nil && pv.Spec.HostPath == nil) {
		return storagePlacementClaim{}, fmt.Errorf("live volume driver does not support the requested same-node overlap")
	}
	liveNode, found, err := c.getNode(ctx, node)
	if err != nil || !found || liveNode.Metadata.Labels[kubeHostnameLabelKey] != node || !kubeNodeReady(liveNode) ||
		liveNode.Spec.Unschedulable || kubeNodeConditionTrue(liveNode.Status.Conditions, "DiskPressure") ||
		placement.rejection(liveNode) != "" {
		return storagePlacementClaim{}, fmt.Errorf("the current node does not have verified eligibility for the existing volume")
	}
	return claim, nil
}

func verifyStorageUnmountEndpoints(ctx context.Context, c *kubeClient, ns, name string, pods ...*corev1.Pod) error {
	var slices discoveryv1.EndpointSliceList
	path := "/apis/discovery.k8s.io/v1/namespaces/" + url.PathEscape(ns) + "/endpointslices?labelSelector=" + url.QueryEscape(discoveryv1.LabelServiceName+"="+name)
	if _, err := c.doJSON(ctx, http.MethodGet, path, nil, &slices); err != nil || slices.Continue != "" {
		return fmt.Errorf("cannot verify canonical EndpointSlice inventory")
	}
	return matchStorageUnmountEndpoints(slices.Items, ns, pods)
}

func matchStorageUnmountEndpoints(slices []discoveryv1.EndpointSlice, ns string, pods []*corev1.Pod) error {
	expected := make(map[string]*corev1.Pod, len(pods))
	for _, p := range pods {
		if p == nil || p.UID == "" || p.Status.PodIP == "" || expected[string(p.UID)] != nil {
			return fmt.Errorf("unmount traffic proof requires distinct, observed Pod identities and IPs")
		}
		expected[string(p.UID)] = p
	}
	seen := make(map[string]bool, len(pods))
	for _, slice := range slices {
		for _, endpoint := range slice.Endpoints {
			ref := endpoint.TargetRef
			p := (*corev1.Pod)(nil)
			if ref != nil {
				p = expected[string(ref.UID)]
			}
			if p == nil || ref.Kind != "Pod" || ref.Namespace != ns || ref.Name != p.Name || len(endpoint.Addresses) == 0 ||
				endpoint.Conditions.Ready == nil || !*endpoint.Conditions.Ready || endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating {
				return fmt.Errorf("canonical EndpointSlice references an unexpected or unready Pod")
			}
			for _, address := range endpoint.Addresses {
				matches := address == p.Status.PodIP
				for _, ip := range p.Status.PodIPs {
					matches = matches || ip.IP == address
				}
				if !matches {
					return fmt.Errorf("canonical EndpointSlice address differs from the observed Pod")
				}
			}
			seen[string(p.UID)] = true
		}
	}
	if len(expected) == 0 || len(seen) != len(expected) {
		return fmt.Errorf("canonical EndpointSlice does not yet contain all expected Pods")
	}
	return nil
}

func verifyStorageUnmountServices(ctx context.Context, c *kubeClient, ns, name string, podLabels map[string]string) error {
	var services corev1.ServiceList
	path := "/api/v1/namespaces/" + url.PathEscape(ns) + "/services"
	if _, err := c.doJSON(ctx, http.MethodGet, path, nil, &services); err != nil || services.Continue != "" {
		return fmt.Errorf("unable to verify Services respect Pod readiness")
	}
	found := false
	for _, svc := range services.Items {
		selected := len(svc.Spec.Selector) > 0
		for k, v := range svc.Spec.Selector {
			selected = selected && podLabels[k] == v
		}
		if svc.Name == name {
			found = selected && svc.DeletionTimestamp == nil
		}
		if selected && svc.Spec.PublishNotReadyAddresses {
			return fmt.Errorf("a selecting Service publishes unready Pod addresses")
		}
	}
	if !found {
		return fmt.Errorf("canonical Service does not select the expected Pod")
	}
	return nil
}

func storageUnmountTemplateHasGate(template map[string]any, id string) bool {
	gates := mapSlice(nestedObjectValue(template, "spec", "readinessGates"))
	return nestedObjectString(template, "metadata", "annotations", runtime.FugueStorageUnmountOperationAnnotation) == id &&
		len(gates) == 1 && gates[0]["conditionType"] == runtime.FugueStorageUnmountReadyCondition
}

func storageUnmountPodHasGate(p *corev1.Pod) bool {
	for _, gate := range p.Spec.ReadinessGates {
		if string(gate.ConditionType) == runtime.FugueStorageUnmountReadyCondition {
			return true
		}
	}
	return false
}

func storageUnmountDeploymentPatch(dep appsv1.Deployment, annotations map[string]string, template, strategy, minReady any) []map[string]any {
	if minReady == nil {
		minReady = 0
	}
	return []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": dep.UID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": dep.ResourceVersion},
		{"op": "replace", "path": "/metadata/annotations", "value": annotations},
		{"op": "replace", "path": "/spec/template", "value": template},
		{"op": "replace", "path": "/spec/strategy", "value": strategy},
		{"op": "add", "path": "/spec/minReadySeconds", "value": minReady},
	}
}

func storageUnmountDeploymentShapeMatches(spec appsv1.DeploymentSpec, t storageUnmountTransition) bool {
	if spec.Replicas == nil || spec.Strategy.Type != appsv1.RollingUpdateDeploymentStrategyType || spec.Strategy.RollingUpdate == nil ||
		spec.Strategy.RollingUpdate.MaxSurge == nil || spec.Strategy.RollingUpdate.MaxUnavailable == nil {
		return false
	}
	replicas, surge, unavailable := int32(1), "1", "0"
	if spec.Paused {
		if t.RetireAllowed {
			return false
		}
		replicas, surge, unavailable = 2, "0", "1"
	}
	return *spec.Replicas == replicas && spec.Strategy.RollingUpdate.MaxSurge.String() == surge && spec.Strategy.RollingUpdate.MaxUnavailable.String() == unavailable
}

func setStorageUnmountDeploymentPaused(ctx context.Context, c *kubeClient, path string, dep appsv1.Deployment, t storageUnmountTransition, paused bool) error {
	marker, err := json.Marshal(t)
	if err != nil {
		return err
	}
	replicas, surge, unavailable := 1, 1, 0
	if paused {
		replicas, surge, unavailable = 2, 0, 1
	}
	strategy := map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxSurge": surge, "maxUnavailable": unavailable}}
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": dep.UID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": dep.ResourceVersion},
		{"op": "replace", "path": "/metadata/annotations/fugue.pro~1storage-unmount-transition", "value": string(marker)},
		{"op": "add", "path": "/spec/paused", "value": paused},
		{"op": "replace", "path": "/spec/replicas", "value": replicas},
		{"op": "replace", "path": "/spec/strategy", "value": strategy},
	}
	if status, err := c.doRequest(ctx, http.MethodPatch, path, "application/json-patch+json", patch, nil); err != nil {
		return fmt.Errorf("Deployment pause change outcome unavailable (HTTP %d)", status)
	}
	return nil
}

func restoreStorageUnmountDeployment(ctx context.Context, c *kubeClient, ns, path string, dep appsv1.Deployment, t storageUnmountTransition) error {
	var old appsv1.ReplicaSet
	if _, err := c.doJSON(ctx, http.MethodGet, recoveryObjectPath(ns, "replicasets", t.ReplicaSet), nil, &old); err != nil {
		return fmt.Errorf("cannot observe original ReplicaSet for rollback: %w", err)
	}
	if uid := metav1.GetControllerOf(&old); string(old.UID) != t.ReplicaSetUID || uid == nil || uid.UID != dep.UID ||
		old.DeletionTimestamp != nil || old.Status.AvailableReplicas != 1 || old.Spec.Template.Annotations[runtime.FugueAnnotationReleaseKey] != t.SourceKey {
		return fmt.Errorf("rollback refuses an unverified prior serving ReplicaSet")
	}
	delete(old.Spec.Template.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
	annotations := dep.Annotations
	delete(annotations, storageUnmountTransitionAnnotation)
	annotations[runtime.FugueAnnotationReleaseKey] = t.SourceKey
	strategy := map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxSurge": 1, "maxUnavailable": 0}}
	patch := append(storageUnmountDeploymentPatch(dep, annotations, old.Spec.Template, strategy, t.MinReady),
		map[string]any{"op": "add", "path": "/spec/paused", "value": false},
		map[string]any{"op": "replace", "path": "/spec/replicas", "value": 1})
	status, err := c.doRequest(ctx, http.MethodPatch, path, "application/json-patch+json", patch, nil)
	if err != nil {
		return fmt.Errorf("rollback Deployment update not verified (HTTP %d)", status)
	}
	return nil
}

func patchStorageUnmountManagedSnapshot(ctx context.Context, c *kubeClient, old runtime.ManagedAppObject, app model.App, scheduling runtime.SchedulingConstraints) error {
	want, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, scheduling))
	if err != nil {
		return err
	}
	if old.Metadata.UID == "" || old.Metadata.ResourceVersion == "" {
		return fmt.Errorf("managed app snapshot has no identity fence")
	}
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": old.Metadata.UID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": old.Metadata.ResourceVersion},
		{"op": "replace", "path": "/spec", "value": want.Spec},
	}
	status, err := c.doRequest(ctx, http.MethodPatch, managedAppAPIPath(old.Metadata.Namespace, old.Metadata.Name), "application/json-patch+json", patch, nil)
	if err != nil {
		return fmt.Errorf("managed app snapshot update not verified (HTTP %d)", status)
	}
	return nil
}

func (s *Service) admitStorageUnmountPod(ctx context.Context, c *kubeClient, ns string, dep appsv1.Deployment, p corev1.Pod, op model.Operation, app model.App) error {
	if p.DeletionTimestamp != nil || p.UID == "" || p.ResourceVersion == "" || !storageUnmountPodCondition(&p, string(corev1.ContainersReady)) ||
		p.Annotations[runtime.FugueAnnotationReleaseKey] != dep.Spec.Template.Annotations[runtime.FugueAnnotationReleaseKey] ||
		p.Annotations[runtime.FugueStorageUnmountOperationAnnotation] != op.ID {
		return fmt.Errorf("candidate Pod is not qualified for probing")
	}
	rs, err := storageUnmountReplicaSet(ctx, c, ns, dep, p)
	if err != nil || !storageUnmountTemplateHasGate(normalizeKubeMap(rs.Spec.Template), op.ID) ||
		rs.Spec.Template.Annotations[runtime.FugueAnnotationReleaseKey] != p.Annotations[runtime.FugueAnnotationReleaseKey] {
		return fmt.Errorf("candidate Pod is not owned by the gated Deployment")
	}
	if !storageUnmountPodHasGate(&p) || len(app.Spec.Ports) == 0 || op.StorageUnmount == nil {
		return fmt.Errorf("candidate Pod does not enforce the unmount readiness gate")
	}
	if err := runStorageUnmountProbe(ctx, p.Status.PodIP, app.Spec.Ports[0], op.StorageUnmount.Probe, app.Spec.Env); err != nil {
		return err
	}
	// The CAS uses the pre-request Pod resourceVersion. A restart, deletion,
	// changed conditions or a replaced Pod invalidates this probe receipt.
	condition := corev1.PodCondition{Type: corev1.PodConditionType(runtime.FugueStorageUnmountReadyCondition),
		Status: corev1.ConditionTrue, Reason: "BusinessProbePassed", LastTransitionTime: metav1.Now()}
	path := "/status/conditions/-"
	action := "add"
	for i, previous := range p.Status.Conditions {
		if previous.Type == condition.Type {
			path = "/status/conditions/" + strconv.Itoa(i)
			action = "replace"
			break
		}
	}
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": p.UID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": p.ResourceVersion},
		{"op": action, "path": path, "value": condition},
	}
	if _, err := c.doRequest(ctx, http.MethodPatch, recoveryObjectPath(ns, "pods", p.Name)+"/status", "application/json-patch+json", patch, nil); err != nil {
		return fmt.Errorf("could not persist candidate business readiness for the observed Pod")
	}
	return nil
}

// The gate is retained with its original revision, including after controller
// restart. Recreated Pods of that revision must pass the same business probe;
// a failed/pending operation can never be approved here.
func (s *Service) reconcileCompletedStorageUnmount(ctx context.Context, c *kubeClient, managed runtime.ManagedAppObject, app model.App) error {
	id := managed.Spec.StorageUnmountOperationID
	if id == "" {
		return nil
	}
	op, err := s.Store.GetOperation(id)
	if err != nil || op.Status != model.OperationStatusCompleted || op.Type != model.OperationTypeAppStorageUnmount ||
		op.AppID != app.ID || op.TenantID != app.TenantID || op.DesiredSpec == nil || op.StorageUnmount == nil {
		return fmt.Errorf("unmount-gated revision is missing a completed operation receipt")
	}
	live := s.Renderer.PrepareApp(runtime.AppFromManagedApp(managed))
	if !reflect.DeepEqual(live.Spec.PersistentStorage, app.Spec.PersistentStorage) || !reflect.DeepEqual(app.Spec.PersistentStorage, op.DesiredSpec.PersistentStorage) {
		return nil
	}
	ns, name := runtime.NamespaceForTenant(app.TenantID), runtime.RuntimeAppResourceName(live)
	depPath := deploymentAPIPath(ns, name)
	var dep appsv1.Deployment
	if _, err := c.doJSON(ctx, http.MethodGet, depPath, nil, &dep); err != nil {
		return err
	}
	key := s.Renderer.ManagedAppReleaseKey(live, managed.Spec.Scheduling)
	if key == "" || dep.Spec.Template.Annotations[runtime.FugueAnnotationReleaseKey] != key ||
		!storageUnmountTemplateHasGate(normalizeKubeMap(dep.Spec.Template), id) ||
		dep.Labels[runtime.FugueLabelAppID] != app.ID || dep.Labels[runtime.FugueLabelTenantID] != app.TenantID {
		return nil
	}
	pods, err := listStorageUnmountPods(ctx, c, ns, name)
	if err != nil {
		return err
	}
	for _, p := range pods {
		if p.DeletionTimestamp != nil || !storageUnmountPodCondition(&p, string(corev1.ContainersReady)) ||
			p.Annotations[runtime.FugueStorageUnmountOperationAnnotation] != id || storageUnmountPodCondition(&p, runtime.FugueStorageUnmountReadyCondition) {
			continue
		}
		if p.Spec.HostNetwork || p.Spec.NodeName != managed.Spec.Scheduling.NodeSelector[kubeHostnameLabelKey] ||
			live.Spec.PersistentStorage == nil || !storageUnmountPodMountsMatch(&p, runtime.PersistentStoragePVCName(live, *live.Spec.PersistentStorage), live.Spec.PersistentStorage, op.StorageUnmount.Path) {
			return fmt.Errorf("unmount-gated Pod drifted from the committed placement or claim")
		}
		if err := verifyStorageUnmountServices(ctx, c, ns, runtime.RuntimeAppServiceName(app), p.Labels); err != nil {
			return err
		}
		return s.admitStorageUnmountPod(ctx, c, ns, dep, p, op, live)
	}
	if marker := dep.Annotations[storageUnmountTransitionAnnotation]; marker != "" {
		var t storageUnmountTransition
		if json.Unmarshal([]byte(marker), &t) != nil || t.OperationID != id || t.CandidateKey != key {
			return fmt.Errorf("cannot clear a different unmount transition")
		}
		patch := []map[string]any{
			{"op": "test", "path": "/metadata/uid", "value": dep.UID},
			{"op": "test", "path": "/metadata/resourceVersion", "value": dep.ResourceVersion},
			{"op": "remove", "path": "/metadata/annotations/fugue.pro~1storage-unmount-transition"},
		}
		_, err = c.doRequest(ctx, http.MethodPatch, depPath, "application/json-patch+json", patch, nil)
	}
	return err
}

func runStorageUnmountProbe(ctx context.Context, ip string, port int, probe model.AppStorageUnmountProbe, env map[string]string) error {
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return runStorageUnmountProbeWithClient(ctx, client, ip, port, probe, env)
}

func runStorageUnmountProbeWithClient(ctx context.Context, client *http.Client, ip string, port int, probe model.AppStorageUnmountProbe, env map[string]string) error {
	addr, err := netip.ParseAddr(ip)
	if err != nil || !addr.IsGlobalUnicast() || addr.IsLoopback() || addr.Zone() != "" || port < 1 || port > 65535 || probe.Validate() != nil {
		return fmt.Errorf("invalid unmount candidate endpoint or business probe")
	}
	method := probe.Method
	if method == "" {
		method = http.MethodGet
	}
	target := "http://" + net.JoinHostPort(addr.String(), strconv.Itoa(port)) + probe.Path
	req, err := http.NewRequestWithContext(ctx, method, target, strings.NewReader(probe.Body))
	if err != nil {
		return fmt.Errorf("cannot construct the candidate business request")
	}
	if probe.Body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if probe.BearerEnv != "" {
		key := env[probe.BearerEnv]
		if key == "" || strings.ContainsAny(key, "\r\n") {
			return fmt.Errorf("candidate probe bearer environment reference is unavailable")
		}
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("candidate business request did not complete")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("candidate business request returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil || !bytes.Contains(body, []byte(probe.ExpectedBodyContains)) {
		return fmt.Errorf("candidate business response did not satisfy the required assertion")
	}
	return nil
}
