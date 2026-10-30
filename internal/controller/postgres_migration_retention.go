package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// CNPG reattaches old local PVCs before scaling down. After a storage-class
// switchover that can deadlock against the new node pin. Retain the old data
// outside CNPG discovery and remove only its unassigned, never-started Pod.
func retainUnscheduledMigrationReplica(ctx context.Context, client *kubeClient, namespace string, app model.App, deployment runtime.ManagedBackingServiceDeployment, cluster kubeCloudNativePGCluster, pods []kubePod) error {
	if !migrationReplicaRetentionAllowed(app, deployment, cluster) {
		return nil
	}
	var primary kubePod
	for _, pod := range pods {
		ready := false
		for _, condition := range pod.Status.Conditions {
			ready = ready || (condition.Type == "Ready" && condition.Status == "True")
		}
		if pod.Metadata.Name == cluster.Status.CurrentPrimary && pod.Status.Phase == "Running" && ready && pod.Metadata.DeletionTimestamp == "" && podOwnedByPGCluster(pod, cluster.Metadata.UID) {
			primary = pod
		}
	}
	if primary.Spec.NodeName == "" {
		return nil
	}
	primaryClaim, found, err := client.getPersistentVolumeClaim(ctx, namespace, primary.Metadata.Name)
	if err != nil || !found || primaryClaim.Spec.StorageClassName == "" || (cluster.Spec.Storage.StorageClass != "" && primaryClaim.Spec.StorageClassName != cluster.Spec.Storage.StorageClass) {
		return err
	}
	for _, pod := range pods {
		if !unstartedMigrationReplica(pod, cluster) {
			continue
		}
		claim, found, err := client.getPersistentVolumeClaim(ctx, namespace, pod.Metadata.Name)
		if err != nil {
			return err
		}
		retained := claim.Metadata.Annotations["fugue.pro/recovery-retained-cluster-uid"] == cluster.Metadata.UID && claim.Metadata.Annotations["fugue.pro/recovery-retained-reason"] == "superseded-local-replica"
		if !found || claim.Metadata.UID == "" || claim.Metadata.ResourceVersion == "" || claim.Spec.VolumeName == "" || claim.Status.Phase != "Bound" || claim.Spec.StorageClassName == primaryClaim.Spec.StorageClassName || (!retained && (claim.Metadata.Labels["cnpg.io/cluster"] != cluster.Metadata.Name || claim.Metadata.Labels["cnpg.io/pvcRole"] != "PG_DATA")) {
			continue
		}
		pv, exists, err := client.getPersistentVolume(ctx, claim.Spec.VolumeName)
		if err != nil {
			return err
		}
		if !exists || pv.Spec.NodeAffinity.Required == nil || persistentVolumeNodeAffinityIncludesNode(pv, primary.Spec.NodeName) {
			continue
		}
		// No other Pod, including a transfer/backup Job Pod, may use the data.
		allPods, err := client.listPodsBySelector(ctx, namespace, "")
		if err != nil {
			return err
		}
		used := false
		for _, other := range allPods {
			for _, volume := range other.Spec.Volumes {
				if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claim.Metadata.Name && other.ObservedUID != pod.ObservedUID {
					used = true
				}
			}
		}
		if used {
			continue
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
		for _, job := range jobs.Items {
			for _, volume := range job.Spec.Template.Spec.Volumes {
				if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claim.Metadata.Name {
					used = true
				}
			}
		}
		if used {
			continue
		}
		fresh, exists, err := client.getCloudNativePGCluster(ctx, namespace, cluster.Metadata.Name)
		if err != nil {
			return err
		}
		if !exists || fresh.Metadata.UID != cluster.Metadata.UID || fresh.Metadata.Generation != cluster.Metadata.Generation || fresh.Status.CurrentPrimary != cluster.Status.CurrentPrimary || !migrationReplicaRetentionAllowed(app, deployment, fresh) {
			return nil
		}
		podPath := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods/" + url.PathEscape(pod.Metadata.Name)
		podRaw, exists, err := client.getRawObject(ctx, podPath)
		if err != nil {
			return err
		}
		var freshPod kubePod
		encoded, _ := json.Marshal(podRaw)
		if !exists || json.Unmarshal(encoded, &freshPod) != nil || freshPod.ObservedUID != pod.ObservedUID || !unstartedMigrationReplica(freshPod, cluster) {
			continue
		}
		claimPath := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/persistentvolumeclaims/" + url.PathEscape(claim.Metadata.Name)
		raw, exists, err := client.getRawObject(ctx, claimPath)
		if err != nil {
			return err
		}
		meta := objectMapField(raw, "metadata")
		if !exists || meta["uid"] != claim.Metadata.UID || meta["resourceVersion"] != claim.Metadata.ResourceVersion || meta["deletionTimestamp"] != nil {
			continue
		}
		refs, _ := meta["ownerReferences"].([]any)
		kept, owned := []any{}, false
		for _, rawRef := range refs {
			ref, _ := rawRef.(map[string]any)
			if ref["uid"] == cluster.Metadata.UID && ref["kind"] == runtime.CloudNativePGClusterKind && ref["apiVersion"] == runtime.CloudNativePGAPIVersion {
				owned = true
			} else {
				kept = append(kept, rawRef)
			}
		}
		if !owned && !retained {
			continue
		}
		// A previous reconciliation may have retained the claim but raced with
		// a Pod update. Resume the identity-fenced deletion without repatching.
		if !retained {
			labels := map[string]any{}
			for key := range claim.Metadata.Labels {
				if strings.HasPrefix(key, "cnpg.io/") {
					labels[key] = nil
				}
			}
			patch := map[string]any{"metadata": map[string]any{"uid": claim.Metadata.UID, "resourceVersion": claim.Metadata.ResourceVersion, "ownerReferences": kept, "labels": labels, "annotations": map[string]string{"fugue.pro/recovery-retained-cluster-uid": cluster.Metadata.UID, "fugue.pro/recovery-retained-reason": "superseded-local-replica"}}}
			encoded, _ = json.Marshal(patch)
			status, _, err := client.doRaw(ctx, http.MethodPatch, claimPath, bytes.NewReader(encoded), "application/merge-patch+json")
			if status == http.StatusConflict || status == http.StatusNotFound {
				continue
			}
			if err != nil {
				return err
			}
			if status >= 300 {
				return fmt.Errorf("retain superseded database claim %s: status=%d", claim.Metadata.Name, status)
			}
		}
		podMeta := objectMapField(podRaw, "metadata")
		body := map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": map[string]any{"uid": pod.ObservedUID, "resourceVersion": podMeta["resourceVersion"]}}
		status, err := client.doJSON(ctx, http.MethodDelete, podPath, body, nil)
		if err != nil && status != http.StatusConflict && status != http.StatusNotFound {
			return err
		}
	}
	return nil
}

func migrationReplicaRetentionAllowed(app model.App, deployment runtime.ManagedBackingServiceDeployment, cluster kubeCloudNativePGCluster) bool {
	if deployment.Suspended || deployment.DesiredInstances != 1 || cluster.Spec.Instances != 1 || cluster.Metadata.UID == "" || cluster.Metadata.DeletionTimestamp != "" || cluster.Spec.Storage.StorageClass != deployment.StorageClassName || cluster.Status.ReadyInstances < 1 || cluster.Status.Instances <= cluster.Spec.Instances || cluster.Status.CurrentPrimary == "" || cluster.Status.CurrentPrimary != cluster.Status.TargetPrimary || cluster.Metadata.Name != deployment.ResourceName {
		return false
	}
	if cluster.Metadata.Labels["app.kubernetes.io/managed-by"] != "fugue" || cluster.Metadata.Labels[runtime.FugueLabelTenantID] != app.TenantID || cluster.Metadata.Labels[runtime.FugueLabelProjectID] != app.ProjectID {
		return false
	}
	if deployment.ServiceID != "" {
		return cluster.Metadata.Labels["fugue.pro/backing-service-id"] == deployment.ServiceID
	}
	return cluster.Metadata.Labels[runtime.FugueLabelAppID] == app.ID
}

func podOwnedByPGCluster(pod kubePod, uid string) bool {
	for _, owner := range pod.ObservedOwnerReferences {
		if owner.UID == uid && owner.Kind == runtime.CloudNativePGClusterKind && owner.APIVersion == runtime.CloudNativePGAPIVersion {
			return true
		}
	}
	return false
}

func unstartedMigrationReplica(pod kubePod, cluster kubeCloudNativePGCluster) bool {
	return pod.ObservedUID != "" && pod.Metadata.Name != cluster.Status.CurrentPrimary && pod.Metadata.Name != cluster.Status.TargetPrimary && pod.Metadata.DeletionTimestamp == "" && pod.Status.Phase == "Pending" && pod.Spec.NodeName == "" && len(pod.Status.ContainerStatuses) == 0 && len(pod.Status.InitContainerStatuses) == 0 && podOwnedByPGCluster(pod, cluster.Metadata.UID)
}
