package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"

	"k8s.io/apimachinery/pkg/api/resource"
)

var errAppCPUResizeUnsettled = errors.New("app CPU resize is not terminally verified")
var errAppCPUResizeDrift = errors.New("app CPU resize runtime identity drifted")
var errAppCPUResizeAborted = errors.New("app CPU resize aborted after verified compensation")

// ReplicaSet replacements inherit the immutable release template. Restore the
// durable request target through the same guarded operation, including while a
// replacement is still Pending; never rewrite a Deployment to repair drift.
func (s *Service) reconcileAppCPUResizeTarget(ctx context.Context, client *kubeClient, app model.App) (bool, error) {
	if app.Spec.Replicas <= 0 || app.Spec.Resources == nil {
		return false, nil
	}
	workload, err := appCPUResizeWorkload(ctx, client, app)
	if err != nil {
		return false, err
	}
	pods, err := listAppCPUResizePods(ctx, client, app, workload)
	if err != nil {
		return false, err
	}
	for _, pod := range pods {
		ob, err := observeAppCPUResizePod(pod, runtime.RuntimeResourceName(app.Name))
		if err != nil {
			return false, err
		}
		if !resizeQuantitiesEqual(ob.DesiredResources.Requests["cpu"], strconv.FormatInt(app.Spec.Resources.CPUMilliCores, 10)+"m") {
			_, err := s.Store.CreateAppCPUResizeOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeAppCPUResize, RequestedByType: model.ActorTypeSystem, RequestedByID: "app-cpu-resize-reconcile", DesiredSpec: &app.Spec}, app.Spec)
			if errors.Is(err, store.ErrConflict) {
				return true, nil
			}
			return err == nil, err
		}
	}
	additional, _, err := additionalAppCPUResizeBaselines(ctx, client, app, workload, app.Spec.Resources.CPUMilliCores)
	if err != nil {
		return false, err
	}
	if len(additional) > 0 {
		_, err := s.Store.CreateAppCPUResizeOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeAppCPUResize, RequestedByType: model.ActorTypeSystem, RequestedByID: "app-cpu-resize-reconcile", DesiredSpec: &app.Spec}, app.Spec)
		if errors.Is(err, store.ErrConflict) {
			return true, nil
		}
		return err == nil, err
	}
	return false, nil
}

const appCPUResizePreflight = "app_cpu_resize_preflight"
const appCPUResizeAborting = "app_cpu_resize_aborting"

// The journal is durable before the first mutation, including on controller
// failover. It contains identity/resource facts only, never workload env or args.
type appCPUResizePodBaseline struct {
	Name      string                   `json:"name"`
	UID       string                   `json:"uid"`
	Node      string                   `json:"node"`
	Restarts  int                      `json:"restarts"`
	StartedAt string                   `json:"started_at"`
	Resources kubeResourceRequirements `json:"resources"`
	Pending   bool                     `json:"pending,omitempty"`
	// Empty workload fields identify serving Pods in journals from older controllers.
	Workload      string `json:"workload,omitempty"`
	DeploymentUID string `json:"deployment_uid,omitempty"`
	Generation    int64  `json:"generation,omitempty"`
	NotReady      bool   `json:"not_ready,omitempty"`
}

type appCPUResizeJournal struct {
	CreatedAt     time.Time                 `json:"created_at"`
	AbortReason   string                    `json:"abort_reason,omitempty"`
	Workload      string                    `json:"workload"`
	DeploymentUID string                    `json:"deployment_uid"`
	Generation    int64                     `json:"generation"`
	CPU           int64                     `json:"cpu_millicores"`
	Pods          []appCPUResizePodBaseline `json:"pods"`
	Deferred      []appCPUResizeDeferredPod `json:"deferred,omitempty"`
}

func (s *Service) executeAppCPUResize(ctx context.Context, op model.Operation, app model.App) (result error) {
	journal, recorded, err := s.loadAppCPUResizeJournal(op)
	if err != nil {
		// Evidence read failure cannot prove that no PATCH was accepted.
		return fmt.Errorf("%w: load resize journal: %v", errAppCPUResizeUnsettled, err)
	}
	defer func() {
		if recorded && result != nil && !errors.Is(result, errAppCPUResizeUnsettled) && !errors.Is(result, errAppCPUResizeAborted) {
			result = fmt.Errorf("%w: %v", errAppCPUResizeUnsettled, result)
		}
	}()
	if !s.Config.KubectlApply || !s.Config.AppCPURequestResizeEnabled {
		return fmt.Errorf("app CPU request in-place resize is disabled; no rolling fallback")
	}
	if op.DesiredSpec == nil || op.DesiredSpec.Resources == nil || op.ConfigBaseSpec == nil ||
		model.AppSpecSHA256(app.Spec) != model.AppSpecSHA256(*op.ConfigBaseSpec) {
		return fmt.Errorf("app CPU resize baseline changed")
	}
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	ns := runtime.NamespaceForTenant(app.TenantID)
	container := runtime.RuntimeResourceName(app.Name)
	capability, err := client.inspectManagedPostgresResizeCapability(ctx, ns)
	if err != nil {
		return err
	}
	if !capability.Available() {
		return fmt.Errorf("app CPU resize unavailable: %s", capability.Reason)
	}
	if !recorded {
		workload, err := appCPUResizeWorkload(ctx, client, app)
		if err != nil {
			return err
		}
		deployment, found, err := client.getDeployment(ctx, ns, workload)
		if err != nil {
			return err
		}
		if !found || deployment.Metadata.UID == "" || deployment.Status.ObservedGeneration < deployment.Metadata.Generation ||
			deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != app.Spec.Replicas {
			return fmt.Errorf("app CPU resize requires a stable deployment")
		}
		pods, err := listAppCPUResizePods(ctx, client, app, workload)
		if err != nil {
			return err
		}
		if len(pods) != app.Spec.Replicas {
			return fmt.Errorf("app CPU resize requires the complete replica set (%d/%d)", len(pods), app.Spec.Replicas)
		}
		journal = appCPUResizeJournal{CreatedAt: time.Now().UTC(), Workload: workload, DeploymentUID: deployment.Metadata.UID, Generation: deployment.Metadata.Generation, CPU: op.DesiredSpec.Resources.CPUMilliCores}
		for _, pod := range pods {
			ob, err := observeAppCPUResizePod(pod, container)
			if err != nil {
				return err
			}
			if err := verifyAppCPUResizeOwner(ctx, client, app, pod, workload, deployment.Metadata.UID); err != nil {
				return err
			}
			restoring := app.Spec.CPURequestTemplateMilliCores != nil && app.Spec.Resources.CPUMilliCores == journal.CPU
			pending := restoring && ob.Phase == "Pending"
			if err := assessAppCPURequestResize(ob, journal.CPU, pending); err != nil {
				return err
			}
			journal.Pods = append(journal.Pods, appCPUResizePodBaseline{Name: ob.PodName, UID: ob.PodUID, Node: ob.NodeName, Restarts: ob.RestartCount, StartedAt: ob.ContainerStartedAt, Resources: ob.DesiredResources, Pending: pending})
		}
		additional, deferred, err := additionalAppCPUResizeBaselines(ctx, client, app, workload, journal.CPU)
		if err != nil {
			return err
		}
		journal.Pods = append(journal.Pods, additional...)
		journal.Deferred = deferred
		if err := s.recordAppCPUResizeEvidence(op, app, appCPUResizePreflight, "preflight verified", journal); err != nil {
			return err
		}
		recorded = true
	}
	if journal.CPU != op.DesiredSpec.Resources.CPUMilliCores || appCPUResizeServingPodCount(journal) != app.Spec.Replicas || journal.Workload == "" {
		return fmt.Errorf("invalid persisted CPU resize journal")
	}
	// Abort is also journaled before mutation. A replacement or a persistently
	// deferred upscale must not hold code/configuration releases indefinitely.
	deadline := s.Config.ManagedAppRolloutTimeout
	if deadline <= 0 {
		deadline = 5 * time.Minute
	}
	if journal.AbortReason == "" && !journal.CreatedAt.IsZero() && time.Since(journal.CreatedAt) > deadline {
		journal.AbortReason = "CPU resize convergence deadline exceeded"
	}
	if journal.AbortReason != "" {
		return s.abortAppCPUResize(ctx, client, op, app, container, journal)
	}
	// From this point any failure retains the mutation interlock. Accepted is
	// never success, and neither a timeout nor a lost PATCH response redeploys.
	if err := s.convergeAppCPUResize(ctx, client, op, app, container, journal); err != nil {
		if errors.Is(err, errAppCPUResizeDrift) {
			journal.AbortReason = err.Error()
			return s.abortAppCPUResize(ctx, client, op, app, container, journal)
		}
		return fmt.Errorf("%w: %v", errAppCPUResizeUnsettled, err)
	}
	if err := s.recordAppCPUResizeEvidence(op, app, "app_cpu_resize_verified", "CPU request applied without replacing or restarting Pods", journal); err != nil {
		return fmt.Errorf("%w: %v", errAppCPUResizeUnsettled, err)
	}
	_, err = s.Store.CompleteManagedOperationGuarded(op, &app, "", fmt.Sprintf("CPU request target verified in place; %d serving/revision Pods journaled, %d revision Pods deferred; removed revisions excluded", len(journal.Pods), len(journal.Deferred)), nil, nil, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", errAppCPUResizeUnsettled, err)
	}
	return nil
}

func (s *Service) convergeAppCPUResize(ctx context.Context, client *kubeClient, op model.Operation, app model.App, container string, journal appCPUResizeJournal) error {
	ns := runtime.NamespaceForTenant(app.TenantID)
	applyingRecorded := false
	for _, baseline := range journal.Pods {
		if err := s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
			return err
		}
		pod, found, err := appCPUResizeBaselinePod(ctx, client, app, journal, baseline)
		if err != nil {
			return err
		}
		if !found {
			if baseline.Workload != "" {
				continue // Independent retirement/replacement may remove a revision Pod.
			}
			return fmt.Errorf("%w: Pod %s disappeared", errAppCPUResizeDrift, baseline.Name)
		}
		ob, err := observeAppCPUResizePod(pod, container)
		if err != nil {
			return err
		}
		if err := validateAppCPUResizeIdentity(ob, baseline); err != nil {
			return err
		}
		workload, uid := appCPUResizeBaselineOwner(journal, baseline)
		if err := verifyAppCPUResizeOwner(ctx, client, app, pod, workload, uid); err != nil {
			return err
		}
		target := cloneKubeResourceRequirements(baseline.Resources)
		target.Requests["cpu"] = strconv.FormatInt(journal.CPU, 10) + "m"
		if !appCPUResizeEnvelopeMatches(ob.DesiredResources, baseline.Resources, journal.CPU) {
			return fmt.Errorf("Pod resources changed outside the CPU request operation")
		}
		if !resizeQuantitiesEqual(ob.DesiredResources.Requests["cpu"], target.Requests["cpu"]) {
			if err := assessAppCPUResizeBaseline(ob, journal.CPU, baseline); err != nil {
				return err
			}
			if !applyingRecorded {
				if err := s.recordAppCPUResizeEvidence(op, app, "app_cpu_resize_applying", "applying CPU request through pods/resize", journal); err != nil {
					return err
				}
				applyingRecorded = true
			}
			if err := s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
				return err
			}
			requested := kubeResourceRequirements{Requests: map[string]string{"cpu": target.Requests["cpu"]}}
			if _, err := client.patchPodContainerResources(ctx, ns, ob.PodName, ob.PodUID, ob.ResourceVersion, container, ob.DesiredResources, requested); err != nil {
				return err
			}
		}
	}
	timeout := s.Config.ManagedAppRolloutTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	call, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		if err := s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
			return err
		}
		deployment, found, err := client.getDeployment(call, ns, journal.Workload)
		if err != nil {
			return err
		}
		if !found || deployment.Metadata.UID != journal.DeploymentUID || deployment.Metadata.Generation != journal.Generation {
			return fmt.Errorf("%w: deployment changed during CPU resize", errAppCPUResizeDrift)
		}
		workload, err := appCPUResizeWorkload(call, client, app)
		if err != nil {
			return err
		}
		if workload != journal.Workload {
			return fmt.Errorf("%w: serving workload changed during CPU resize", errAppCPUResizeDrift)
		}
		pods, err := listAppCPUResizePods(call, client, app, journal.Workload)
		if err != nil {
			return err
		}
		if len(pods) != appCPUResizeServingPodCount(journal) {
			return fmt.Errorf("%w: replica set changed during CPU resize", errAppCPUResizeDrift)
		}
		converged := true
		for _, baseline := range journal.Pods {
			pod, found, err := appCPUResizeBaselinePod(call, client, app, journal, baseline)
			if err != nil {
				return err
			}
			if !found && baseline.Workload != "" {
				continue
			}
			if !found {
				return fmt.Errorf("%w: serving Pod disappeared", errAppCPUResizeDrift)
			}
			ob, err := observeAppCPUResizePod(pod, container)
			if err != nil {
				return err
			}
			if err := validateAppCPUResizeIdentity(ob, baseline); err != nil {
				return err
			}
			if !appCPUResizeEnvelopeMatches(ob.DesiredResources, baseline.Resources, journal.CPU) {
				return fmt.Errorf("non-CPU resources changed during resize")
			}
			if !appCPUResizeBaselineConverged(ob, baseline, journal.CPU) {
				converged = false
			}
		}
		if converged {
			return nil
		}
		if err := waitManagedPostgresResizePollInterval(call, time.Second); err != nil {
			return err
		}
	}
}

func resizeQuantitiesEqual(a, b string) bool {
	x, ex := resource.ParseQuantity(a)
	y, ey := resource.ParseQuantity(b)
	return ex == nil && ey == nil && x.Cmp(y) == 0
}

func appCPUResizeEnvelopeMatches(current, baseline kubeResourceRequirements, target int64) bool {
	a, b := cloneKubeResourceRequirements(current), cloneKubeResourceRequirements(baseline)
	before, after := b.Requests["cpu"], strconv.FormatInt(target, 10)+"m"
	if !resizeQuantitiesEqual(a.Requests["cpu"], before) && !resizeQuantitiesEqual(a.Requests["cpu"], after) {
		return false
	}
	delete(a.Requests, "cpu")
	delete(b.Requests, "cpu")
	return appCPUResizeResourcesEqual(a, b)
}

// Preserve every resource key, not just the CPU/memory keys supported by the
// older database resize helper (for example ephemeral-storage and hugepages).
func appCPUResizeResourcesEqual(a, b kubeResourceRequirements) bool {
	for i, left := range []map[string]string{a.Requests, a.Limits} {
		right := []map[string]string{b.Requests, b.Limits}[i]
		if len(left) != len(right) {
			return false
		}
		for key, value := range left {
			if !resizeQuantitiesEqual(value, right[key]) {
				return false
			}
		}
	}
	return true
}

func validateAppCPUResizeIdentity(ob managedPostgresResizeObservation, baseline appCPUResizePodBaseline) error {
	if ob.PodName != baseline.Name || ob.PodUID != baseline.UID || ob.DeletionTimestamp != "" ||
		(baseline.Node != "" && ob.NodeName != baseline.Node) || ob.RestartCount != baseline.Restarts ||
		(!baseline.Pending && ob.ContainerStartedAt != baseline.StartedAt) {
		return fmt.Errorf("%w: Pod/container identity changed during CPU resize", errAppCPUResizeDrift)
	}
	return nil
}

func appCPURequestResizeConverged(ob managedPostgresResizeObservation, cpu int64) bool {
	target := strconv.FormatInt(cpu, 10) + "m"
	if ob.ActualResources == nil || !ob.PodReady || !ob.ContainerReady || ob.Phase != "Running" || resizePendingCondition(ob.Conditions) != nil ||
		(ob.Generation > 0 && ob.ObservedGeneration < ob.Generation) ||
		!resizeQuantitiesEqual(ob.DesiredResources.Requests["cpu"], target) || !resizeQuantitiesEqual(ob.ActualResources.Requests["cpu"], target) {
		return false
	}
	if allocated := ob.AllocatedResources["cpu"]; allocated != "" && !resizeQuantitiesEqual(allocated, target) {
		return false
	}
	for _, scope := range []struct{ desired, actual map[string]string }{
		{ob.DesiredResources.Requests, ob.ActualResources.Requests},
		{ob.DesiredResources.Limits, ob.ActualResources.Limits},
	} {
		for _, name := range []string{"cpu", "memory"} {
			if scope.desired[name] == "" && scope.actual[name] == "" {
				continue
			}
			if !resizeQuantitiesEqual(scope.desired[name], scope.actual[name]) {
				return false
			}
		}
	}
	return true
}

func assessAppCPURequestResize(ob managedPostgresResizeObservation, cpu int64, pending bool) error {
	if ob.PodUID == "" || ob.ResourceVersion == "" || ob.DeletionTimestamp != "" || cpu <= 0 || ob.PodResources != nil {
		return fmt.Errorf("CPU resize requires a live container-scoped Pod")
	}
	target := cloneKubeResourceRequirements(ob.DesiredResources)
	if target.Requests == nil {
		return fmt.Errorf("missing CPU resource request")
	}
	target.Requests["cpu"] = strconv.FormatInt(cpu, 10) + "m"
	// Changing the QoS class is forbidden by Kubernetes. With fixed memory and
	// limits only crossing the Guaranteed CPU equality can change that class.
	if appCPUResizeGuaranteed(ob, ob.DesiredResources) != appCPUResizeGuaranteed(ob, target) {
		return fmt.Errorf("CPU request resize would change Pod QoS class")
	}
	if pending {
		if ob.Phase != "Pending" {
			return fmt.Errorf("replacement Pod is not Pending")
		}
		if err := validateMergedPodResourceRequirements(target); err != nil {
			return err
		}
		return validateManagedPostgresResizePolicy(ob.ResizePolicy, target, ob.DesiredResources)
	}
	if ob.ActualResources == nil || ob.ContainerStartedAt == "" {
		return fmt.Errorf("actual container resources/start time not observed")
	}
	requested := kubeResourceRequirements{Requests: map[string]string{"cpu": target.Requests["cpu"]}}
	assessment := assessManagedPostgresResize(ob, requested, managedPostgresResizeSafetyOptions{AllowRequestDownscale: true})
	if assessment.State != managedPostgresResizeStateReady && assessment.State != managedPostgresResizeStateNoop {
		return fmt.Errorf("CPU resize blocked: %s (%s)", assessment.Reason, assessment.Message)
	}
	return nil
}

func appCPUResizeGuaranteed(ob managedPostgresResizeObservation, target kubeResourceRequirements) bool {
	containers := append(cloneKubeResizeContainerSpecs(ob.Containers), ob.InitContainers...)
	for _, container := range containers {
		r := container.Resources
		if container.Name == ob.ContainerName {
			r = target
		}
		for _, name := range []string{"cpu", "memory"} {
			if r.Requests[name] == "" || r.Limits[name] == "" || !resizeQuantitiesEqual(r.Requests[name], r.Limits[name]) {
				return false
			}
		}
	}
	return len(containers) > 0
}

func observeAppCPUResizePod(pod kubeResizePod, container string) (managedPostgresResizeObservation, error) {
	ob, err := observeManagedPostgresResize(pod, container)
	if err == nil || pod.Status.Phase != "Pending" {
		return ob, err
	}
	// A replacement not yet scheduled has no containerStatuses. Observe the
	// spec without inventing a running container or a successful allocation.
	for _, spec := range pod.Spec.Containers {
		if spec.Name == container {
			pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, struct {
				Name               string                    `json:"name"`
				Ready              bool                      `json:"ready,omitempty"`
				RestartCount       int                       `json:"restartCount,omitempty"`
				Resources          *kubeResourceRequirements `json:"resources,omitempty"`
				AllocatedResources map[string]string         `json:"allocatedResources,omitempty"`
				State              struct {
					Running *struct {
						StartedAt string `json:"startedAt,omitempty"`
					} `json:"running,omitempty"`
				} `json:"state,omitempty"`
			}{Name: container})
			return observeManagedPostgresResize(pod, container)
		}
	}
	return ob, err
}

func appCPUResizeWorkload(ctx context.Context, c *kubeClient, app model.App) (string, error) {
	name := runtime.RuntimeAppResourceName(app)
	svc, found, err := c.getRawObject(ctx, "/api/v1/namespaces/"+runtime.NamespaceForTenant(app.TenantID)+"/services/"+runtime.RuntimeAppServiceName(app))
	if err != nil {
		return "", err
	}
	if found {
		selector := objectStringMapValue(nestedObjectValue(svc, "spec", "selector"))
		if selector[runtime.FugueLabelAppID] != app.ID || selector[runtime.FugueLabelTenantID] != app.TenantID {
			return "", fmt.Errorf("service ownership mismatch")
		}
		if workload := selector[runtime.FugueLabelAppWorkload]; workload != "" {
			name = workload
		}
	}
	return name, nil
}

func listAppCPUResizePods(ctx context.Context, c *kubeClient, app model.App, workload string) ([]kubeResizePod, error) {
	// Older serving Pods predate the workload label. Select by app identity,
	// then prove the exact Deployment owner instead of rewriting their template
	// or admitting other revisions of the same app into the resize cohort.
	query := url.Values{"labelSelector": {runtime.FugueLabelAppID + "=" + app.ID}}
	var list struct {
		Items []kubeResizePod `json:"items"`
	}
	_, err := c.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+runtime.NamespaceForTenant(app.TenantID)+"/pods?"+query.Encode(), nil, &list)
	if err != nil {
		return nil, err
	}
	deployment, found, err := c.getDeployment(ctx, runtime.NamespaceForTenant(app.TenantID), workload)
	if err != nil {
		return nil, err
	}
	if !found || deployment.Metadata.UID == "" {
		return nil, fmt.Errorf("app CPU resize requires an identified serving deployment")
	}
	pods := make([]kubeResizePod, 0, len(list.Items))
	for _, pod := range list.Items {
		if label := pod.Metadata.Labels[runtime.FugueLabelAppWorkload]; label != "" && label != workload {
			continue
		}
		if !appCPUResizePodLabelsMatch(app, pod, workload) {
			return nil, fmt.Errorf("Pod ownership labels mismatch")
		}
		owned, err := appCPUResizePodOwnedByDeployment(ctx, c, app, pod, workload, deployment.Metadata.UID)
		if err != nil {
			return nil, err
		}
		if owned {
			pods = append(pods, pod)
		}
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Metadata.Name < pods[j].Metadata.Name })
	return pods, nil
}

func appCPUResizePodLabelsMatch(app model.App, pod kubeResizePod, workload string) bool {
	label := pod.Metadata.Labels[runtime.FugueLabelAppWorkload]
	return pod.Metadata.Labels[runtime.FugueLabelAppID] == app.ID &&
		pod.Metadata.Labels[runtime.FugueLabelTenantID] == app.TenantID &&
		pod.Metadata.Labels[runtime.FugueLabelManagedBy] == runtime.FugueLabelManagedByValue &&
		(label == "" || label == workload)
}

func appCPUResizePodOwnedByDeployment(ctx context.Context, c *kubeClient, app model.App, pod kubeResizePod, workload, uid string) (bool, error) {
	for _, owner := range pod.Metadata.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller || owner.Kind != "ReplicaSet" || owner.UID == "" {
			continue
		}
		rs, found, err := c.getRawObject(ctx, "/apis/apps/v1/namespaces/"+runtime.NamespaceForTenant(app.TenantID)+"/replicasets/"+url.PathEscape(owner.Name))
		if err != nil {
			return false, err
		}
		if !found || objectStringField(objectMapField(rs, "metadata"), "uid") != owner.UID {
			return false, fmt.Errorf("ReplicaSet ownership mismatch")
		}
		raw, _ := json.Marshal(nestedObjectValue(rs, "metadata", "ownerReferences"))
		var refs []kubeResizeOwnerReference
		if err := json.Unmarshal(raw, &refs); err != nil {
			return false, err
		}
		for _, ref := range refs {
			if ref.Controller != nil && *ref.Controller && ref.Kind == "Deployment" && ref.Name == workload && ref.UID == uid {
				return true, nil
			}
		}
	}
	return false, nil
}

func verifyAppCPUResizeOwner(ctx context.Context, c *kubeClient, app model.App, pod kubeResizePod, workload, uid string) error {
	if !appCPUResizePodLabelsMatch(app, pod, workload) {
		return fmt.Errorf("Pod ownership labels mismatch")
	}
	owned, err := appCPUResizePodOwnedByDeployment(ctx, c, app, pod, workload, uid)
	if err != nil {
		return err
	}
	if owned {
		return nil
	}
	return fmt.Errorf("Pod is not controlled by the verified app deployment")
}

func (s *Service) recordAppCPUResizeEvidence(op model.Operation, app model.App, kind, message string, journal appCPUResizeJournal) error {
	raw, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	// Evidence storage bounds payloads at 128 KiB. A recovery journal must
	// never be silently truncated after we have accepted a mutation.
	if len(raw) > 64*1024 {
		return fmt.Errorf("CPU resize journal exceeds the safe storage bound")
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	_, err = s.Store.RecordOperationEvidence(model.OperationEvidence{TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: op.ID, Type: kind, Source: model.OperationEvidenceSourceKubernetesAPI, Severity: model.OperationEvidenceSeverityInfo, Confidence: model.OperationEvidenceConfidenceConfirmed, RedactionStatus: model.OperationEvidenceRedactionNone, SubjectKind: "Deployment", SubjectName: journal.Workload, SubjectNamespace: runtime.NamespaceForTenant(app.TenantID), Summary: message, PayloadVersion: 1, Payload: payload})
	return err
}

func (s *Service) loadAppCPUResizeJournal(op model.Operation) (appCPUResizeJournal, bool, error) {
	evidence, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{appCPUResizePreflight, appCPUResizeAborting}, Limit: 1})
	if err != nil {
		return appCPUResizeJournal{}, false, err
	}
	for _, event := range evidence {
		if event.Type == appCPUResizePreflight || event.Type == appCPUResizeAborting {
			raw, err := json.Marshal(event.Payload)
			if err != nil {
				return appCPUResizeJournal{}, false, err
			}
			var journal appCPUResizeJournal
			err = json.Unmarshal(raw, &journal)
			return journal, true, err
		}
	}
	return appCPUResizeJournal{}, false, nil
}

// Compensate only surviving, original Pod UIDs and only the CPU value this
// operation owns. Replaced Pods cannot carry an outstanding PATCH for an old UID
// (the resize request has UID/resourceVersion preconditions). Never claim an
// in-place success if a container restarted or a rollout replaced it.
func (s *Service) abortAppCPUResize(ctx context.Context, client *kubeClient, op model.Operation, app model.App, container string, journal appCPUResizeJournal) error {
	if err := s.recordAppCPUResizeEvidence(op, app, appCPUResizeAborting, "compensating interrupted CPU resize", journal); err != nil {
		return err
	}
	ns := runtime.NamespaceForTenant(app.TenantID)
	for _, baseline := range journal.Pods {
		if err := s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
			return err
		}
		pod, found, err := client.getPodResizeState(ctx, ns, baseline.Name)
		if err != nil {
			return err
		}
		if !found || pod.Metadata.UID != baseline.UID || pod.Metadata.DeletionTimestamp != "" {
			continue
		}
		ob, err := observeAppCPUResizePod(pod, container)
		if err != nil {
			return err
		}
		if !appCPUResizeEnvelopeMatches(ob.DesiredResources, baseline.Resources, journal.CPU) {
			return fmt.Errorf("cannot compensate externally changed Pod resources")
		}
		if !resizeQuantitiesEqual(ob.DesiredResources.Requests["cpu"], baseline.Resources.Requests["cpu"]) {
			if err := validateMergedPodResourceRequirements(baseline.Resources); err != nil {
				return err
			}
			if ob.PodResources != nil || appCPUResizeGuaranteed(ob, ob.DesiredResources) != appCPUResizeGuaranteed(ob, baseline.Resources) {
				return fmt.Errorf("compensation cannot change resource scope or QoS")
			}
			if err := validateManagedPostgresResizePolicy(ob.ResizePolicy, baseline.Resources, ob.DesiredResources); err != nil {
				return err
			}
			requested := kubeResourceRequirements{Requests: map[string]string{"cpu": baseline.Resources.Requests["cpu"]}}
			if _, err := client.patchPodContainerResources(ctx, ns, ob.PodName, ob.PodUID, ob.ResourceVersion, container, ob.DesiredResources, requested); err != nil {
				return err
			}
			return fmt.Errorf("compensation accepted; waiting for actual resources")
		}
		cpu, err := resource.ParseQuantity(baseline.Resources.Requests["cpu"])
		if err != nil {
			return err
		}
		if !appCPUResizeBaselineConverged(ob, baseline, cpu.MilliValue()) {
			return fmt.Errorf("waiting for compensation to converge")
		}
	}
	if err := s.recordAppCPUResizeEvidence(op, app, "app_cpu_resize_aborted", "original surviving Pods restored; no deployment or configuration rollback required", journal); err != nil {
		return err
	}
	return fmt.Errorf("%w: %s", errAppCPUResizeAborted, journal.AbortReason)
}
