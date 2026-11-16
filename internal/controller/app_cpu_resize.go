package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"

	"k8s.io/apimachinery/pkg/api/resource"
)

var errAppRequestResizeUnsettled = errors.New("app resource request resize is not terminally verified")
var errAppRequestResizeDrift = errors.New("app resource request resize runtime identity drifted")
var errAppRequestResizeAborted = errors.New("app resource request resize aborted after verified compensation")

// ReplicaSet replacements inherit the immutable release template. Restore the
// durable request target through the same guarded operation, including while a
// replacement is still Pending; never rewrite a Deployment to repair drift.
func (s *Service) reconcileAppCPUResizeTarget(ctx context.Context, client *kubeClient, app model.App) (bool, error) {
	if app.Spec.Replicas <= 0 || app.Spec.Resources == nil {
		return false, nil
	}
	_, fingerprint, needed, err := s.planAppCPUResize(ctx, client, app, app.Spec.Resources.CPUMilliCores)
	if err != nil {
		var waiting *appRequestResizeWaiting
		if errors.As(err, &waiting) {
			if recordErr := s.recordAppCPUResizeWait(ctx, app, fingerprint, waiting.Error()); recordErr != nil {
				return false, recordErr
			}
		}
		return false, err
	}
	if !needed {
		return false, nil
	}
	admitted, err := s.admitAppCPUResizeReconcile(ctx, app, fingerprint)
	if err != nil {
		return false, err
	}
	if !admitted {
		return false, &appRequestResizeWaiting{reason: "CPU request restore is waiting for its retry budget or a relevant state change"}
	}
	_, err = s.Store.CreateAppRequestResizeOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeAppCPUResize, RequestedByType: model.ActorTypeSystem, RequestedByID: "app-cpu-resize-reconcile", DesiredSpec: &app.Spec}, app.Spec)
	if errors.Is(err, store.ErrConflict) {
		return true, nil
	}
	return err == nil, err
}

const appCPUResizePreflight = "app_cpu_resize_preflight"
const appCPUResizeAborting = "app_cpu_resize_aborting"

// The journal is durable before the first mutation, including on controller
// failover. It contains identity/resource facts only, never workload env or args.
type appRequestResizePodBaseline struct {
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

type appRequestResizeJournal struct {
	CreatedAt     time.Time                     `json:"created_at"`
	AbortReason   string                        `json:"abort_reason,omitempty"`
	Workload      string                        `json:"workload"`
	DeploymentUID string                        `json:"deployment_uid"`
	Generation    int64                         `json:"generation"`
	CPU           int64                         `json:"cpu_millicores"`
	Memory        int64                         `json:"memory_mebibytes,omitempty"`
	Pods          []appRequestResizePodBaseline `json:"pods"`
	Deferred      []appRequestResizeDeferredPod `json:"deferred,omitempty"`
}

func (s *Service) executeAppRequestResize(ctx context.Context, op model.Operation, app model.App) (result error) {
	journal, recorded, err := s.loadAppRequestResizeJournal(op)
	if err != nil {
		// Evidence read failure cannot prove that no PATCH was accepted.
		return fmt.Errorf("%w: load resize journal: %v", errAppRequestResizeUnsettled, err)
	}
	defer func() {
		if recorded && result != nil && !errors.Is(result, errAppRequestResizeUnsettled) && !errors.Is(result, errAppRequestResizeAborted) {
			result = fmt.Errorf("%w: %v", errAppRequestResizeUnsettled, result)
		}
	}()
	enabled := s.Config.AppCPURequestResizeEnabled
	if op.Type == model.OperationTypeAppMemoryResize {
		enabled = s.Config.AppMemoryRequestResizeEnabled
	}
	// Once journaled, recovery must still settle accepted mutations after an
	// operator disables new resizes. A feature flag is not cancellation proof.
	if !s.Config.KubectlApply || (!enabled && !recorded) {
		return fmt.Errorf("app request in-place resize is disabled; no rolling fallback")
	}
	if op.DesiredSpec == nil || op.DesiredSpec.Resources == nil || op.ConfigBaseSpec == nil ||
		model.AppSpecSHA256(app.Spec) != model.AppSpecSHA256(*op.ConfigBaseSpec) {
		return fmt.Errorf("app resource request resize baseline changed")
	}
	target := appOperationRequestResizeTarget(op)
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
		return fmt.Errorf("app resource request resize unavailable: %s", capability.Reason)
	}
	if !recorded {
		journal, _, _, err = s.planAppRequestResize(ctx, client, app, target)
		if err != nil {
			return err
		}
		if err := s.recordAppRequestResizeEvidence(op, app, appCPUResizePreflight, "preflight verified", journal); err != nil {
			return err
		}
		recorded = true
	}
	if journal.target() != target || (journal.CPU > 0 && journal.Memory > 0) || target.Value <= 0 || appRequestResizeServingPodCount(journal) != app.Spec.Replicas || journal.Workload == "" {
		return fmt.Errorf("invalid persisted resource request resize journal")
	}
	// Abort is also journaled before mutation. A replacement or a persistently
	// deferred upscale must not hold code/configuration releases indefinitely.
	deadline := s.Config.ManagedAppRolloutTimeout
	if deadline <= 0 {
		deadline = 5 * time.Minute
	}
	if journal.AbortReason == "" && !journal.CreatedAt.IsZero() && time.Since(journal.CreatedAt) > deadline {
		journal.AbortReason = "resource request resize convergence deadline exceeded"
	}
	if journal.AbortReason != "" {
		return s.abortAppRequestResize(ctx, client, op, app, container, journal)
	}
	// From this point any failure retains the mutation interlock. Accepted is
	// never success, and neither a timeout nor a lost PATCH response redeploys.
	if err := s.convergeAppRequestResize(ctx, client, op, app, container, journal); err != nil {
		if errors.Is(err, errAppRequestResizeDrift) {
			journal.AbortReason = err.Error()
			return s.abortAppRequestResize(ctx, client, op, app, container, journal)
		}
		return fmt.Errorf("%w: %v", errAppRequestResizeUnsettled, err)
	}
	if err := s.recordAppRequestResizeEvidence(op, app, "app_cpu_resize_verified", target.Resource+" request applied without replacing or restarting Pods", journal); err != nil {
		return fmt.Errorf("%w: %v", errAppRequestResizeUnsettled, err)
	}
	_, err = s.Store.CompleteManagedOperationGuarded(op, &app, "", fmt.Sprintf("%s request target verified in place; %d serving/revision Pods journaled, %d revision Pods deferred; removed revisions excluded", target.Resource, len(journal.Pods), len(journal.Deferred)), nil, nil, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", errAppRequestResizeUnsettled, err)
	}
	return nil
}

func (s *Service) convergeAppRequestResize(ctx context.Context, client *kubeClient, op model.Operation, app model.App, container string, journal appRequestResizeJournal) error {
	ns := runtime.NamespaceForTenant(app.TenantID)
	requestTarget := journal.target()
	applyingRecorded := false
	for _, baseline := range journal.Pods {
		if err := s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
			return err
		}
		pod, found, err := appRequestResizeBaselinePod(ctx, client, app, journal, baseline)
		if err != nil {
			return err
		}
		if !found {
			if baseline.Workload != "" {
				continue // Independent retirement/replacement may remove a revision Pod.
			}
			return fmt.Errorf("%w: Pod %s disappeared", errAppRequestResizeDrift, baseline.Name)
		}
		ob, err := observeAppRequestResizePod(pod, container)
		if err != nil {
			return err
		}
		if err := validateAppRequestResizeIdentity(ob, baseline); err != nil {
			return err
		}
		workload, uid := appRequestResizeBaselineOwner(journal, baseline)
		if err := verifyAppRequestResizeOwner(ctx, client, app, pod, workload, uid); err != nil {
			return err
		}
		target := cloneKubeResourceRequirements(baseline.Resources)
		target.Requests[requestTarget.Resource] = requestTarget.quantity()
		if !appRequestResizeEnvelopeMatches(ob.DesiredResources, baseline.Resources, requestTarget) {
			return fmt.Errorf("Pod resources changed outside the request operation")
		}
		if !resizeQuantitiesEqual(ob.DesiredResources.Requests[requestTarget.Resource], target.Requests[requestTarget.Resource]) {
			if err := assessAppRequestResizeBaseline(ob, requestTarget, baseline); err != nil {
				return err
			}
			if !applyingRecorded {
				if err := s.recordAppRequestResizeEvidence(op, app, "app_cpu_resize_applying", "applying "+requestTarget.Resource+" request through pods/resize", journal); err != nil {
					return err
				}
				applyingRecorded = true
			}
			if err := s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
				return err
			}
			requested := requestTarget.requested()
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
			return fmt.Errorf("%w: deployment changed during resource request resize", errAppRequestResizeDrift)
		}
		workload, err := appRequestResizeWorkload(call, client, app)
		if err != nil {
			return err
		}
		if workload != journal.Workload {
			return fmt.Errorf("%w: serving workload changed during resource request resize", errAppRequestResizeDrift)
		}
		pods, err := listAppRequestResizePods(call, client, app, journal.Workload)
		if err != nil {
			return err
		}
		if len(pods) != appRequestResizeServingPodCount(journal) {
			return fmt.Errorf("%w: replica set changed during resource request resize", errAppRequestResizeDrift)
		}
		converged := true
		for _, baseline := range journal.Pods {
			pod, found, err := appRequestResizeBaselinePod(call, client, app, journal, baseline)
			if err != nil {
				return err
			}
			if !found && baseline.Workload != "" {
				continue
			}
			if !found {
				return fmt.Errorf("%w: serving Pod disappeared", errAppRequestResizeDrift)
			}
			ob, err := observeAppRequestResizePod(pod, container)
			if err != nil {
				return err
			}
			if err := validateAppRequestResizeIdentity(ob, baseline); err != nil {
				return err
			}
			if !appRequestResizeEnvelopeMatches(ob.DesiredResources, baseline.Resources, requestTarget) {
				return fmt.Errorf("unowned resources changed during resize")
			}
			if !appRequestResizeBaselineConverged(ob, baseline, requestTarget) {
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
	return appRequestResizeEnvelopeMatches(current, baseline, appRequestResizeTarget{"cpu", target})
}
func appRequestResizeResourcesEqual(a, b kubeResourceRequirements) bool {
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

func validateAppRequestResizeIdentity(ob managedPostgresResizeObservation, baseline appRequestResizePodBaseline) error {
	if ob.PodName != baseline.Name || ob.PodUID != baseline.UID || ob.DeletionTimestamp != "" ||
		(baseline.Node != "" && ob.NodeName != baseline.Node) || ob.RestartCount != baseline.Restarts ||
		(!baseline.Pending && ob.ContainerStartedAt != baseline.StartedAt) {
		return fmt.Errorf("%w: Pod/container identity changed during resource request resize", errAppRequestResizeDrift)
	}
	return nil
}

func appCPURequestResizeConverged(ob managedPostgresResizeObservation, cpu int64) bool {
	return appRequestResizeConverged(ob, appRequestResizeTarget{"cpu", cpu})
}

func assessAppCPURequestResize(ob managedPostgresResizeObservation, cpu int64, pending bool) error {
	return assessAppRequestResize(ob, appRequestResizeTarget{"cpu", cpu}, pending)
}

func appRequestResizeGuaranteed(ob managedPostgresResizeObservation, target kubeResourceRequirements) bool {
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

func observeAppRequestResizePod(pod kubeResizePod, container string) (managedPostgresResizeObservation, error) {
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

func appRequestResizeWorkload(ctx context.Context, c *kubeClient, app model.App) (string, error) {
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

func listAppRequestResizePods(ctx context.Context, c *kubeClient, app model.App, workload string) ([]kubeResizePod, error) {
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
		return nil, fmt.Errorf("app resource request resize requires an identified serving deployment")
	}
	pods := make([]kubeResizePod, 0, len(list.Items))
	for _, pod := range list.Items {
		if label := pod.Metadata.Labels[runtime.FugueLabelAppWorkload]; label != "" && label != workload {
			continue
		}
		if !appRequestResizePodLabelsMatch(app, pod, workload) {
			return nil, fmt.Errorf("Pod ownership labels mismatch")
		}
		owned, err := appRequestResizePodOwnedByDeployment(ctx, c, app, pod, workload, deployment.Metadata.UID)
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

func appRequestResizePodLabelsMatch(app model.App, pod kubeResizePod, workload string) bool {
	label := pod.Metadata.Labels[runtime.FugueLabelAppWorkload]
	return pod.Metadata.Labels[runtime.FugueLabelAppID] == app.ID &&
		pod.Metadata.Labels[runtime.FugueLabelTenantID] == app.TenantID &&
		pod.Metadata.Labels[runtime.FugueLabelManagedBy] == runtime.FugueLabelManagedByValue &&
		(label == "" || label == workload)
}

func appRequestResizePodOwnedByDeployment(ctx context.Context, c *kubeClient, app model.App, pod kubeResizePod, workload, uid string) (bool, error) {
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

func verifyAppRequestResizeOwner(ctx context.Context, c *kubeClient, app model.App, pod kubeResizePod, workload, uid string) error {
	if !appRequestResizePodLabelsMatch(app, pod, workload) {
		return fmt.Errorf("Pod ownership labels mismatch")
	}
	owned, err := appRequestResizePodOwnedByDeployment(ctx, c, app, pod, workload, uid)
	if err != nil {
		return err
	}
	if owned {
		return nil
	}
	return fmt.Errorf("Pod is not controlled by the verified app deployment")
}

func (s *Service) recordAppRequestResizeEvidence(op model.Operation, app model.App, kind, message string, journal appRequestResizeJournal) error {
	if op.Type == model.OperationTypeAppMemoryResize {
		kind = strings.Replace(kind, "app_cpu_resize_", "app_memory_resize_", 1)
	}
	raw, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	// Evidence storage bounds payloads at 128 KiB. A recovery journal must
	// never be silently truncated after we have accepted a mutation.
	if len(raw) > 64*1024 {
		return fmt.Errorf("resource request resize journal exceeds the safe storage bound")
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	_, err = s.Store.RecordOperationEvidence(model.OperationEvidence{TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: op.ID, Type: kind, Source: model.OperationEvidenceSourceKubernetesAPI, Severity: model.OperationEvidenceSeverityInfo, Confidence: model.OperationEvidenceConfidenceConfirmed, RedactionStatus: model.OperationEvidenceRedactionNone, SubjectKind: "Deployment", SubjectName: journal.Workload, SubjectNamespace: runtime.NamespaceForTenant(app.TenantID), Summary: message, PayloadVersion: 1, Payload: payload})
	return err
}

func (s *Service) loadAppRequestResizeJournal(op model.Operation) (appRequestResizeJournal, bool, error) {
	preflight, aborting := appCPUResizePreflight, appCPUResizeAborting
	if op.Type == model.OperationTypeAppMemoryResize {
		preflight, aborting = "app_memory_resize_preflight", "app_memory_resize_aborting"
	}
	evidence, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{preflight, aborting}, Limit: 1})
	if err != nil {
		return appRequestResizeJournal{}, false, err
	}
	for _, event := range evidence {
		if event.Type == preflight || event.Type == aborting {
			raw, err := json.Marshal(event.Payload)
			if err != nil {
				return appRequestResizeJournal{}, false, err
			}
			var journal appRequestResizeJournal
			err = json.Unmarshal(raw, &journal)
			return journal, true, err
		}
	}
	return appRequestResizeJournal{}, false, nil
}

// Compensate only surviving, original Pod UIDs and only the request value this
// operation owns. Replaced Pods cannot carry an outstanding PATCH for an old UID
// (the resize request has UID/resourceVersion preconditions). Never claim an
// in-place success if a container restarted or a rollout replaced it.
func (s *Service) abortAppRequestResize(ctx context.Context, client *kubeClient, op model.Operation, app model.App, container string, journal appRequestResizeJournal) error {
	requestTarget := journal.target()
	if err := s.recordAppRequestResizeEvidence(op, app, appCPUResizeAborting, "compensating interrupted "+requestTarget.Resource+" resize", journal); err != nil {
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
		ob, err := observeAppRequestResizePod(pod, container)
		if err != nil {
			return err
		}
		if !appRequestResizeEnvelopeMatches(ob.DesiredResources, baseline.Resources, requestTarget) {
			return fmt.Errorf("cannot compensate externally changed Pod resources")
		}
		if !resizeQuantitiesEqual(ob.DesiredResources.Requests[requestTarget.Resource], baseline.Resources.Requests[requestTarget.Resource]) {
			if err := validateMergedPodResourceRequirements(baseline.Resources); err != nil {
				return err
			}
			if ob.PodResources != nil || appRequestResizeGuaranteed(ob, ob.DesiredResources) != appRequestResizeGuaranteed(ob, baseline.Resources) {
				return fmt.Errorf("compensation cannot change resource scope or QoS")
			}
			if err := validateManagedPostgresResizePolicy(ob.ResizePolicy, baseline.Resources, ob.DesiredResources); err != nil {
				return err
			}
			requested := kubeResourceRequirements{Requests: map[string]string{requestTarget.Resource: baseline.Resources.Requests[requestTarget.Resource]}}
			if _, err := client.patchPodContainerResources(ctx, ns, ob.PodName, ob.PodUID, ob.ResourceVersion, container, ob.DesiredResources, requested); err != nil {
				return err
			}
			return fmt.Errorf("compensation accepted; waiting for actual resources")
		}
		if !appRequestResizeOriginalConverged(ob, baseline, requestTarget.Resource) {
			return fmt.Errorf("waiting for compensation to converge")
		}
	}
	if err := s.recordAppRequestResizeEvidence(op, app, "app_cpu_resize_aborted", "original surviving Pods restored; no deployment or configuration rollback required", journal); err != nil {
		return err
	}
	return fmt.Errorf("%w: %s", errAppRequestResizeAborted, journal.AbortReason)
}
