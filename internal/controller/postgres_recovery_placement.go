package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// A CSI registration alone does not prove Longhorn can attach a volume: its
// manager must also know the node. Recovery can run while every app pod is down,
// so use storage and runtime facts rather than requiring a serving app pod.
func recoveryStorageTargetNode(ctx context.Context, client *kubeClient, targetRuntime model.Runtime, storageClass, requested string) (string, error) {
	sc, found, err := client.getStorageClass(ctx, storageClass)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("recovery target storage class %s not found", storageClass)
	}
	if sc.Provisioner != "driver.longhorn.io" {
		return requested, nil
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Status struct {
				Conditions []kubePodCondition `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	if _, err := client.doJSON(ctx, http.MethodGet, "/apis/longhorn.io/v1beta2/nodes", nil, &list); err != nil {
		return "", fmt.Errorf("observe Longhorn attachment nodes: %w", err)
	}
	selector := runtime.SchedulingForRuntime(targetRuntime).NodeSelector
	if len(selector) == 0 {
		return "", fmt.Errorf("recovery target runtime has no managed scheduling constraints")
	}
	var candidates []string
	for _, item := range list.Items {
		ready := false
		for _, condition := range item.Status.Conditions {
			if condition.Type == "Ready" && condition.Status == "True" {
				ready = true
			}
		}
		if !ready || (requested != "" && item.Metadata.Name != requested) {
			continue
		}
		node, found, err := client.getNode(ctx, item.Metadata.Name)
		if err != nil {
			return "", err
		}
		if found && managedSharedNodeSchedulable(node) && nodeLabelsMatchSelector(node.Metadata.Labels, selector) {
			candidates = append(candidates, item.Metadata.Name)
		}
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no ready Longhorn attachment node matches recovery runtime %s and requested node %q", targetRuntime.ID, requested)
	}
	sort.Strings(candidates)
	return candidates[0], nil
}

// CNPG join Jobs copy affinity at creation. After correcting the target node,
// replace only a Job whose containers have never started. Preserve its bound
// PVC: the new Job mounts the same volume and no source data is discarded.
func rescheduleUnstartedRecoveryJoins(ctx context.Context, client *kubeClient, namespace, name, targetNode string, target managedPostgresStorageTarget) error {
	if targetNode == "" || target.StorageClassName == "" {
		return nil
	}
	cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, name)
	if err != nil {
		return err
	}
	if !found || cluster.Status.CurrentPrimary == "" || cluster.Status.ReadyInstances < 1 || cluster.Spec.Storage.StorageClass != target.StorageClassName {
		return nil
	}
	pods, err := client.listPodsBySelector(ctx, namespace, fmt.Sprintf(managedPostgresPodSelectorTemplate, name))
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if pod.Status.Phase != "Pending" || pod.Spec.NodeName == "" || pod.Spec.NodeName == targetNode || pod.Metadata.DeletionTimestamp != "" {
			continue
		}
		unstarted := true
		for _, statuses := range [][]kubeContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
			for _, status := range statuses {
				if status.RestartCount != 0 || status.State.Running != nil || status.State.Terminated != nil || status.LastState.Running != nil || status.LastState.Terminated != nil {
					unstarted = false
				}
			}
		}
		if !unstarted {
			continue
		}
		claim := managedPostgresPVCNameForPod(pod)
		if claim == "" || claim == cluster.Status.CurrentPrimary {
			continue
		}
		pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, claim)
		if err != nil {
			return err
		}
		if !found || pvc.Spec.StorageClassName != target.StorageClassName || pvc.Metadata.Labels["cnpg.io/cluster"] != name || pvc.Metadata.Labels["cnpg.io/pvcRole"] != "PG_DATA" {
			continue
		}
		for _, owner := range pod.ObservedOwnerReferences {
			if !owner.Controller || owner.Kind != "Job" || owner.APIVersion != "batch/v1" || owner.UID == "" {
				continue
			}
			path := "/apis/batch/v1/namespaces/" + url.PathEscape(namespace) + "/jobs/" + url.PathEscape(owner.Name)
			var job struct {
				Metadata struct {
					UID             string                  `json:"uid"`
					ResourceVersion string                  `json:"resourceVersion"`
					OwnerReferences []kubePodOwnerReference `json:"ownerReferences"`
				} `json:"metadata"`
			}
			if _, err := client.doJSON(ctx, http.MethodGet, path, nil, &job); err != nil {
				return err
			}
			owned := false
			for _, ref := range job.Metadata.OwnerReferences {
				if ref.Controller && ref.Kind == "Cluster" && ref.APIVersion == "postgresql.cnpg.io/v1" && ref.UID == cluster.Metadata.UID && ref.UID != "" {
					owned = true
				}
			}
			if !owned || job.Metadata.UID != owner.UID || job.Metadata.ResourceVersion == "" {
				continue
			}
			body := map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Foreground", "preconditions": map[string]string{"uid": owner.UID, "resourceVersion": job.Metadata.ResourceVersion}}
			status, err := client.doJSON(ctx, http.MethodDelete, path, body, nil)
			if status != http.StatusNotFound && status != http.StatusConflict && err != nil {
				return fmt.Errorf("reschedule unstarted database join %s: %w", owner.Name, err)
			}
		}
	}
	return nil
}
