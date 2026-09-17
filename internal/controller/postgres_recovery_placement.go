package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

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

// An initializing PVC without a Job is not a reusable database instance in
// CNPG. Retain that volume outside the cluster's discovery/GC ownership, so
// CNPG can bootstrap a new replica from the still-ready primary. Never mark an
// uninitialized volume ready, delete it, or detach a claim used by any Pod/Job.
func retainAbandonedRecoveryClaims(ctx context.Context, client *kubeClient, namespace, name string, target managedPostgresStorageTarget) error {
	cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, name)
	if err != nil {
		return err
	}
	if !found || cluster.Metadata.UID == "" || cluster.Status.CurrentPrimary == "" || cluster.Status.ReadyInstances < 1 || cluster.Spec.Storage.StorageClass != target.StorageClassName {
		return nil
	}
	pods, err := client.listPodsBySelector(ctx, namespace, "")
	if err != nil {
		return err
	}
	var jobs struct {
		Items []struct {
			Spec struct {
				Template struct {
					Spec struct {
						Volumes []kubePodVolume `json:"volumes"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		} `json:"items"`
	}
	if _, err := client.doJSON(ctx, http.MethodGet, "/apis/batch/v1/namespaces/"+url.PathEscape(namespace)+"/jobs", nil, &jobs); err != nil {
		return err
	}
	inUse := map[string]bool{}
	for _, pod := range pods {
		for _, v := range pod.Spec.Volumes {
			if v.PersistentVolumeClaim != nil {
				inUse[v.PersistentVolumeClaim.ClaimName] = true
			}
		}
	}
	for _, job := range jobs.Items {
		for _, v := range job.Spec.Template.Spec.Volumes {
			if v.PersistentVolumeClaim != nil {
				inUse[v.PersistentVolumeClaim.ClaimName] = true
			}
		}
	}
	claims, err := client.listPersistentVolumeClaimNamesByLabel(ctx, namespace, "cnpg.io/cluster="+name+",cnpg.io/pvcRole=PG_DATA")
	if err != nil {
		return err
	}
	for _, claim := range claims {
		if claim == cluster.Status.CurrentPrimary || claim == cluster.Status.TargetPrimary || inUse[claim] {
			continue
		}
		pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, claim)
		if err != nil {
			return err
		}
		if !found || pvc.Metadata.UID == "" || pvc.Metadata.ResourceVersion == "" || pvc.Metadata.Annotations["cnpg.io/pvcStatus"] != "initializing" || pvc.Spec.StorageClassName != target.StorageClassName || pvc.Metadata.Labels["cnpg.io/instanceRole"] != "replica" {
			continue
		}
		path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/persistentvolumeclaims/" + url.PathEscape(claim)
		current, found, err := client.getRawObject(ctx, path)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		meta, _ := current["metadata"].(map[string]any)
		if meta["uid"] != pvc.Metadata.UID || meta["resourceVersion"] != pvc.Metadata.ResourceVersion {
			continue
		}
		refs, _ := meta["ownerReferences"].([]any)
		kept := []any{}
		for _, raw := range refs {
			ref, _ := raw.(map[string]any)
			if ref["uid"] == cluster.Metadata.UID && ref["kind"] == "Cluster" {
				continue
			}
			kept = append(kept, raw)
		}
		labels := map[string]any{}
		for key := range pvc.Metadata.Labels {
			if strings.HasPrefix(key, "cnpg.io/") {
				labels[key] = nil
			}
		}
		body := map[string]any{"metadata": map[string]any{"uid": pvc.Metadata.UID, "resourceVersion": pvc.Metadata.ResourceVersion, "ownerReferences": kept, "labels": labels, "annotations": map[string]string{"fugue.pro/recovery-retained-cluster-uid": cluster.Metadata.UID, "fugue.pro/recovery-retained-reason": "abandoned-initializing-replica"}}}
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		status, _, err := client.doRaw(ctx, http.MethodPatch, path, bytes.NewReader(raw), "application/merge-patch+json")
		if status == http.StatusConflict || status == http.StatusNotFound {
			continue
		}
		if err != nil {
			return err
		}
		if status >= 300 {
			return fmt.Errorf("retain abandoned initializing claim %s: status=%d", claim, status)
		}
	}
	return nil
}

// CNPG join Jobs copy affinity at creation. After correcting the target node,
// remove only a Job whose containers have never started. The abandoned claim
// is retained separately before CNPG bootstraps a fresh replica.
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
