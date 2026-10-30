package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type appCPUResizeDeferredPod struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// Discover actual workloads, not candidate name prefixes or mutable release roles.
// A Pod must prove the complete Pod -> ReplicaSet -> Deployment UID chain, and
// its Deployment template must independently identify the same managed app.
func additionalAppCPUResizeBaselines(ctx context.Context, c *kubeClient, app model.App, serving string, cpu int64) ([]appCPUResizePodBaseline, []appCPUResizeDeferredPod, error) {
	ns := runtime.NamespaceForTenant(app.TenantID)
	query := url.Values{"labelSelector": {runtime.FugueLabelAppID + "=" + app.ID}}
	var deployments struct {
		Items []kubeDeployment `json:"items"`
	}
	if _, err := c.doJSON(ctx, http.MethodGet, "/apis/apps/v1/namespaces/"+ns+"/deployments?"+query.Encode(), nil, &deployments); err != nil {
		return nil, nil, err
	}
	byName := make(map[string]kubeDeployment, len(deployments.Items))
	for _, deployment := range deployments.Items {
		labels := deployment.Spec.Template.Metadata.Labels
		if deployment.Metadata.Name == serving || deployment.Metadata.UID == "" || deployment.Metadata.DeletionTimestamp != "" ||
			labels[runtime.FugueLabelAppID] != app.ID || labels[runtime.FugueLabelTenantID] != app.TenantID ||
			labels[runtime.FugueLabelManagedBy] != runtime.FugueLabelManagedByValue ||
			(labels[runtime.FugueLabelAppWorkload] != "" && labels[runtime.FugueLabelAppWorkload] != deployment.Metadata.Name) {
			continue
		}
		byName[deployment.Metadata.Name] = deployment
	}
	if len(byName) == 0 {
		return nil, nil, nil
	}
	var pods struct {
		Items []kubeResizePod `json:"items"`
	}
	if _, err := c.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+ns+"/pods?"+query.Encode(), nil, &pods); err != nil {
		return nil, nil, err
	}
	var baselines []appCPUResizePodBaseline
	var deferred []appCPUResizeDeferredPod
	for _, pod := range pods.Items {
		if pod.Metadata.DeletionTimestamp != "" || pod.Status.Phase == "Succeeded" || pod.Status.Phase == "Failed" || pod.Metadata.Labels[runtime.FugueLabelAppWorkload] == serving {
			continue
		}
		name, uid, err := appCPUResizePodDeploymentOwner(ctx, c, app, pod)
		if err != nil {
			deferred = append(deferred, appCPUResizeDeferredPod{pod.Metadata.Name, err.Error()})
			continue
		}
		deployment, found := byName[name]
		if !found || uid != deployment.Metadata.UID || !appCPUResizePodLabelsMatch(app, pod, name) {
			continue
		}
		ob, err := observeAppCPUResizePod(pod, runtime.RuntimeResourceName(app.Name))
		if err != nil {
			deferred = append(deferred, appCPUResizeDeferredPod{pod.Metadata.Name, err.Error()})
			continue
		}
		baseline := appCPUResizePodBaseline{Name: ob.PodName, UID: ob.PodUID, Node: ob.NodeName, Restarts: ob.RestartCount, StartedAt: ob.ContainerStartedAt, Resources: ob.DesiredResources,
			Workload: name, DeploymentUID: uid, Generation: deployment.Metadata.Generation, NotReady: !ob.PodReady || !ob.ContainerReady,
			Pending: ob.Phase == "Pending" && ob.ContainerStartedAt == "" && ob.RestartCount == 0}
		if appCPUResizeBaselineConverged(ob, baseline, cpu) {
			continue
		}
		if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas <= 0 || deployment.Status.ObservedGeneration < deployment.Metadata.Generation {
			err = fmt.Errorf("revision deployment is retiring or has unobserved intent")
		} else {
			err = assessAppCPUResizeBaseline(ob, cpu, baseline)
		}
		if err != nil {
			deferred = append(deferred, appCPUResizeDeferredPod{pod.Metadata.Name, err.Error()})
			continue
		}
		baselines = append(baselines, baseline)
	}
	sort.Slice(baselines, func(i, j int) bool { return baselines[i].Name < baselines[j].Name })
	sort.Slice(deferred, func(i, j int) bool { return deferred[i].Name < deferred[j].Name })
	return baselines, deferred, nil
}

func appCPUResizePodDeploymentOwner(ctx context.Context, c *kubeClient, app model.App, pod kubeResizePod) (string, string, error) {
	for _, owner := range pod.Metadata.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller || owner.Kind != "ReplicaSet" || owner.UID == "" {
			continue
		}
		rs, found, err := c.getRawObject(ctx, "/apis/apps/v1/namespaces/"+runtime.NamespaceForTenant(app.TenantID)+"/replicasets/"+url.PathEscape(owner.Name))
		if err != nil {
			return "", "", err
		}
		if !found || objectStringField(objectMapField(rs, "metadata"), "uid") != owner.UID {
			return "", "", fmt.Errorf("ReplicaSet ownership mismatch")
		}
		raw, _ := json.Marshal(nestedObjectValue(rs, "metadata", "ownerReferences"))
		var refs []kubeResizeOwnerReference
		if err := json.Unmarshal(raw, &refs); err != nil {
			return "", "", err
		}
		for _, ref := range refs {
			if ref.Controller != nil && *ref.Controller && ref.Kind == "Deployment" && ref.UID != "" {
				return ref.Name, ref.UID, nil
			}
		}
	}
	return "", "", nil
}

func appCPUResizeServingPodCount(journal appCPUResizeJournal) int {
	count := 0
	for _, baseline := range journal.Pods {
		if baseline.Workload == "" {
			count++
		}
	}
	return count
}

func appCPUResizeBaselineOwner(journal appCPUResizeJournal, baseline appCPUResizePodBaseline) (string, string) {
	if baseline.Workload != "" {
		return baseline.Workload, baseline.DeploymentUID
	}
	return journal.Workload, journal.DeploymentUID
}

func appCPUResizeBaselinePod(ctx context.Context, c *kubeClient, app model.App, journal appCPUResizeJournal, baseline appCPUResizePodBaseline) (kubeResizePod, bool, error) {
	ns := runtime.NamespaceForTenant(app.TenantID)
	pod, found, err := c.getPodResizeState(ctx, ns, baseline.Name)
	if err != nil || baseline.Workload == "" {
		return pod, found, err
	}
	// Removal of a historical revision is normal lifecycle progress. A PATCH
	// guarded by its original UID cannot mutate a replacement Pod.
	if !found || pod.Metadata.UID != baseline.UID || pod.Metadata.DeletionTimestamp != "" {
		return pod, false, nil
	}
	deployment, found, err := c.getDeployment(ctx, ns, baseline.Workload)
	if err != nil {
		return pod, false, err
	}
	if !found || deployment.Metadata.DeletionTimestamp != "" || deployment.Spec.Replicas != nil && *deployment.Spec.Replicas == 0 {
		return pod, false, nil
	}
	if deployment.Metadata.UID != baseline.DeploymentUID || deployment.Metadata.Generation != baseline.Generation {
		return pod, false, fmt.Errorf("%w: revision deployment changed during CPU resize", errAppCPUResizeDrift)
	}
	if err := verifyAppCPUResizeOwner(ctx, c, app, pod, baseline.Workload, baseline.DeploymentUID); err != nil {
		return pod, false, err
	}
	return pod, true, nil
}

func assessAppCPUResizeBaseline(ob managedPostgresResizeObservation, cpu int64, baseline appCPUResizePodBaseline) error {
	if baseline.Workload != "" && baseline.NotReady {
		if baseline.Restarts > 0 {
			return fmt.Errorf("revision container is not ready after restarts; wait for a stable observation")
		}
		// App revision readiness may be failing for unrelated business reasons.
		// CPU-request-only resize still requires a real running allocation,
		// unchanged identity, QoS and a no-restart policy. Serving Pods retain
		// the stricter readiness gate. Never treat this as a health recovery.
		ob.PodReady, ob.ContainerReady = true, true
	}
	return assessAppCPURequestResize(ob, cpu, baseline.Pending && ob.Phase == "Pending")
}

func appCPUResizeBaselineConverged(ob managedPostgresResizeObservation, baseline appCPUResizePodBaseline, cpu int64) bool {
	if baseline.Workload != "" && baseline.Pending && ob.Phase == "Pending" && ob.ContainerStartedAt == "" && ob.RestartCount == 0 {
		// A never-started container has no runtime allocation to verify, even
		// if scheduling completes concurrently. Verify its admitted request
		// without inventing Ready/actual status.
		return ob.DeletionTimestamp == "" && resizePendingCondition(ob.Conditions) == nil && resizeQuantitiesEqual(ob.DesiredResources.Requests["cpu"], strconv.FormatInt(cpu, 10)+"m")
	}
	if baseline.Workload != "" && baseline.NotReady {
		ob.PodReady, ob.ContainerReady = true, true
	}
	return appCPURequestResizeConverged(ob, cpu)
}
