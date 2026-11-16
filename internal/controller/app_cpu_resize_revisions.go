package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type appRequestResizeDeferredPod struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// Discover actual workloads, not candidate name prefixes or mutable release roles.
// A Pod must prove the complete Pod -> ReplicaSet -> Deployment UID chain, and
// its Deployment template must independently identify the same managed app.
func additionalAppCPUResizeBaselines(ctx context.Context, c *kubeClient, app model.App, serving string, cpu int64) ([]appRequestResizePodBaseline, []appRequestResizeDeferredPod, error) {
	return additionalAppRequestResizeBaselines(ctx, c, app, serving, appRequestResizeTarget{"cpu", cpu})
}

func additionalAppRequestResizeBaselines(ctx context.Context, c *kubeClient, app model.App, serving string, target appRequestResizeTarget) ([]appRequestResizePodBaseline, []appRequestResizeDeferredPod, error) {
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
	var baselines []appRequestResizePodBaseline
	var deferred []appRequestResizeDeferredPod
	for _, pod := range pods.Items {
		if pod.Metadata.DeletionTimestamp != "" || pod.Status.Phase == "Succeeded" || pod.Status.Phase == "Failed" || pod.Metadata.Labels[runtime.FugueLabelAppWorkload] == serving {
			continue
		}
		name, uid, err := appRequestResizePodDeploymentOwner(ctx, c, app, pod)
		if err != nil {
			deferred = append(deferred, appRequestResizeDeferredPod{pod.Metadata.Name, err.Error()})
			continue
		}
		deployment, found := byName[name]
		if !found || uid != deployment.Metadata.UID || !appRequestResizePodLabelsMatch(app, pod, name) {
			continue
		}
		ob, err := observeAppRequestResizePod(pod, runtime.RuntimeResourceName(app.Name))
		if err != nil {
			deferred = append(deferred, appRequestResizeDeferredPod{pod.Metadata.Name, err.Error()})
			continue
		}
		baseline := appRequestResizePodBaseline{Name: ob.PodName, UID: ob.PodUID, Node: ob.NodeName, Restarts: ob.RestartCount, StartedAt: ob.ContainerStartedAt, Resources: ob.DesiredResources,
			Workload: name, DeploymentUID: uid, Generation: deployment.Metadata.Generation, NotReady: !ob.PodReady || !ob.ContainerReady,
			Pending: ob.Phase == "Pending" && ob.ContainerStartedAt == "" && ob.RestartCount == 0}
		if appRequestResizeBaselineConverged(ob, baseline, target) {
			continue
		}
		if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas <= 0 || deployment.Status.ObservedGeneration < deployment.Metadata.Generation {
			err = fmt.Errorf("revision deployment is retiring or has unobserved intent")
		} else {
			err = assessAppRequestResizeBaseline(ob, target, baseline)
		}
		if err != nil {
			deferred = append(deferred, appRequestResizeDeferredPod{pod.Metadata.Name, err.Error()})
			continue
		}
		baselines = append(baselines, baseline)
	}
	sort.Slice(baselines, func(i, j int) bool { return baselines[i].Name < baselines[j].Name })
	sort.Slice(deferred, func(i, j int) bool { return deferred[i].Name < deferred[j].Name })
	return baselines, deferred, nil
}

func appRequestResizePodDeploymentOwner(ctx context.Context, c *kubeClient, app model.App, pod kubeResizePod) (string, string, error) {
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

func appRequestResizeServingPodCount(journal appRequestResizeJournal) int {
	count := 0
	for _, baseline := range journal.Pods {
		if baseline.Workload == "" {
			count++
		}
	}
	return count
}

func appRequestResizeBaselineOwner(journal appRequestResizeJournal, baseline appRequestResizePodBaseline) (string, string) {
	if baseline.Workload != "" {
		return baseline.Workload, baseline.DeploymentUID
	}
	return journal.Workload, journal.DeploymentUID
}

func appRequestResizeBaselinePod(ctx context.Context, c *kubeClient, app model.App, journal appRequestResizeJournal, baseline appRequestResizePodBaseline) (kubeResizePod, bool, error) {
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
		return pod, false, fmt.Errorf("%w: revision deployment changed during resource request resize", errAppRequestResizeDrift)
	}
	if err := verifyAppRequestResizeOwner(ctx, c, app, pod, baseline.Workload, baseline.DeploymentUID); err != nil {
		return pod, false, err
	}
	return pod, true, nil
}

func assessAppCPUResizeBaseline(ob managedPostgresResizeObservation, cpu int64, baseline appRequestResizePodBaseline) error {
	return assessAppRequestResizeBaseline(ob, appRequestResizeTarget{"cpu", cpu}, baseline)
}

func appCPUResizeBaselineConverged(ob managedPostgresResizeObservation, baseline appRequestResizePodBaseline, cpu int64) bool {
	return appRequestResizeBaselineConverged(ob, baseline, appRequestResizeTarget{"cpu", cpu})
}
