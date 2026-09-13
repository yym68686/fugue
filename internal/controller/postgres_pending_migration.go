package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// recoverUnboundPostgresMigrationReplicas replaces only never-scheduled join
// replicas whose empty claims belong to a previous storage intent. Bound or
// selected-node claims are left intact. The live cluster must already declare
// the target class so CNPG recreates the replica using the new intent.
func recoverUnboundPostgresMigrationReplicas(ctx context.Context, client *kubeClient, namespace, clusterName string, cluster kubeCloudNativePGCluster, target managedPostgresStorageTarget) error {
	if target.StorageClassName == "" || cluster.Spec.Storage.StorageClass != target.StorageClassName || cluster.Status.CurrentPrimary == "" || cluster.Status.ReadyInstances < 1 {
		return nil
	}
	pods, err := client.listPodsBySelector(ctx, namespace, fmt.Sprintf(managedPostgresPodSelectorTemplate, clusterName))
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if pod.Status.Phase != "Pending" || pod.Spec.NodeName != "" || pod.Metadata.DeletionTimestamp != "" || len(pod.Status.ContainerStatuses) != 0 || len(pod.Status.InitContainerStatuses) != 0 {
			continue
		}
		var job kubePodOwnerReference
		for _, owner := range pod.ObservedOwnerReferences {
			if owner.Controller && owner.Kind == "Job" && owner.APIVersion == "batch/v1" && owner.UID != "" {
				job = owner
			}
		}
		if job.Name == "" {
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
		if !found || pvc.Metadata.UID == "" || pvc.Metadata.ResourceVersion == "" || pvc.Spec.VolumeName != "" || pvc.Status.Phase != "Pending" || pvc.Metadata.Annotations["volume.kubernetes.io/selected-node"] != "" || pvc.Spec.StorageClassName == target.StorageClassName || pvc.Metadata.Labels["cnpg.io/cluster"] != clusterName || pvc.Metadata.Labels["cnpg.io/pvcRole"] != "PG_DATA" {
			continue
		}
		body := map[string]any{
			"apiVersion": "v1", "kind": "DeleteOptions",
			"preconditions": map[string]string{"uid": pvc.Metadata.UID, "resourceVersion": pvc.Metadata.ResourceVersion},
		}
		path := "/api/v1/namespaces/" + client.effectiveNamespace(namespace) + "/persistentvolumeclaims/" + url.PathEscape(claim)
		status, err := client.doJSON(ctx, http.MethodDelete, path, body, nil)
		if status == http.StatusConflict || status == http.StatusNotFound {
			continue
		}
		if err != nil {
			return fmt.Errorf("replace unbound postgres migration claim %s: %w", claim, err)
		}
		// PVC protection keeps the claim until this join Job's pod is gone.
		body = map[string]any{
			"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Background",
			"preconditions": map[string]string{"uid": job.UID},
		}
		path = "/apis/batch/v1/namespaces/" + client.effectiveNamespace(namespace) + "/jobs/" + url.PathEscape(strings.TrimSpace(job.Name))
		status, err = client.doJSON(ctx, http.MethodDelete, path, body, nil)
		if status != http.StatusNotFound && status != http.StatusConflict && err != nil {
			return fmt.Errorf("replace pending postgres migration join job %s: %w", job.Name, err)
		}
	}
	return nil
}
