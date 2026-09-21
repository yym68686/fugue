package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/runtime"
)

const appWorkloadMigrationAnnotation = "fugue.pro/workload-label-migration"

var errAppServiceWorkloadPreflight = errors.New("Service workload migration is pending")

// A started metadata migration must remain recoverable when a later image or
// storage preflight rejects the desired code release.
func (c *kubeClient) resumeAppWorkloadMigration(ctx context.Context, namespace, name, appID, tenantID string) error {
	deployment, found, err := c.getRawObject(ctx, deploymentAPIPath(namespace, name))
	if err != nil || !found {
		return err
	}
	annotations := objectStringMapValue(objectMapField(deployment, "metadata")["annotations"])
	if annotations[appWorkloadMigrationAnnotation] == "" {
		return nil
	}
	return c.prepareAppServiceWorkload(ctx, namespace, name, map[string]string{
		runtime.FugueLabelAppID: appID, runtime.FugueLabelTenantID: tenantID, runtime.FugueLabelAppWorkload: name,
	})
}

// Services are applied before Deployments. Label existing workloads first so
// narrowing a Service never waits for a replacement Pod to regain endpoints.
func (c *kubeClient) prepareAppServiceWorkloads(ctx context.Context, objects []map[string]any) error {
	prepared := map[string]bool{}
	for _, object := range objects {
		if objectStringField(object, "kind") != "Service" || objectStringField(object, "apiVersion") != "v1" {
			continue
		}
		selector := objectStringMapValue(nestedObjectValue(object, "spec", "selector"))
		workload := selector[runtime.FugueLabelAppWorkload]
		if workload == "" {
			continue
		}
		_, namespace := objectNameAndNamespace(c.namespace, object)
		key := namespace + "/" + workload
		if prepared[key] {
			continue
		}
		if err := c.prepareAppServiceWorkload(ctx, namespace, workload, selector); err != nil {
			return fmt.Errorf("%w: prepare Service workload %s: %w", errAppServiceWorkloadPreflight, key, err)
		}
		prepared[key] = true
	}
	for _, object := range objects {
		if !isDeploymentObject(object) {
			continue
		}
		name, namespace := objectNameAndNamespace(c.namespace, object)
		if !prepared[namespace+"/"+name] {
			continue
		}
		// A same-release template preservation can have copied the pre-migration
		// live template. Do not let its subsequent apply remove the new label.
		meta := objectMapValue(nestedObjectValue(object, "spec", "template", "metadata"))
		labels := objectStringMapValue(meta["labels"])
		if labels == nil {
			labels = map[string]string{}
		}
		labels[runtime.FugueLabelAppWorkload] = name
		meta["labels"] = labels
	}
	return nil
}

func (c *kubeClient) prepareAppServiceWorkload(ctx context.Context, namespace, name string, selector map[string]string) error {
	path := deploymentAPIPath(namespace, name)
	deployment, found, err := c.getRawObject(ctx, path)
	if err != nil || !found {
		return err
	}
	metadata := objectMapField(deployment, "metadata")
	uid := objectStringField(metadata, "uid")
	if uid == "" || !appWorkloadOwnerMatches(metadata, selector) {
		return fmt.Errorf("Deployment owner or UID differs")
	}
	templateLabels := objectStringMapValue(nestedObjectValue(deployment, "spec", "template", "metadata", "labels"))
	for key, value := range selector {
		if key != runtime.FugueLabelAppWorkload && templateLabels[key] != value {
			return fmt.Errorf("Service selector differs from Deployment template")
		}
	}
	if value := templateLabels[runtime.FugueLabelAppWorkload]; value != "" && value != name {
		return fmt.Errorf("Deployment already declares another workload")
	}
	annotations := objectStringMapValue(metadata["annotations"])
	migration := annotations[appWorkloadMigrationAnnotation]
	if migration != "" && migration != "resume" && migration != "paused" {
		return fmt.Errorf("unknown workload migration state")
	}
	if templateLabels[runtime.FugueLabelAppWorkload] == name && migration == "" {
		return nil
	}
	if templateLabels[runtime.FugueLabelAppWorkload] == "" && migration == "" {
		migration = "resume"
		if paused, _ := objectMapField(deployment, "spec")["paused"].(bool); paused {
			migration = "paused"
		}
		if err := c.patchAppWorkloadObject(ctx, path, deployment, map[string]any{
			"metadata": map[string]any{"annotations": map[string]any{appWorkloadMigrationAnnotation: migration}},
			"spec":     map[string]any{"paused": true},
		}); err != nil {
			return err
		}
		deployment, found, err = c.getRawObject(ctx, path)
		if err != nil {
			return fmt.Errorf("read paused Deployment: %w", err)
		}
		if !found {
			return fmt.Errorf("paused Deployment disappeared")
		}
	}
	if migration != "" {
		paused, _ := objectMapField(deployment, "spec")["paused"].(bool)
		if !paused || objectStringField(objectMapField(deployment, "metadata"), "uid") != uid {
			return fmt.Errorf("Deployment changed during workload migration")
		}
		waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		for !appWorkloadPauseObserved(deployment) {
			select {
			case <-waitCtx.Done():
				return fmt.Errorf("wait for Deployment pause: %w", waitCtx.Err())
			case <-time.After(200 * time.Millisecond):
			}
			deployment, found, err = c.getRawObject(waitCtx, path)
			if err != nil || !found || objectStringField(objectMapField(deployment, "metadata"), "uid") != uid {
				return fmt.Errorf("Deployment unavailable while waiting for pause")
			}
			if paused, _ := objectMapField(deployment, "spec")["paused"].(bool); !paused {
				return fmt.Errorf("Deployment resumed during workload migration")
			}
		}
	}
	query := url.Values{"labelSelector": {runtime.FugueLabelAppID + "=" + selector[runtime.FugueLabelAppID]}}
	var replicaSets kubeObjectList
	if _, err := c.doJSON(ctx, http.MethodGet, "/apis/apps/v1/namespaces/"+url.PathEscape(namespace)+"/replicasets?"+query.Encode(), nil, &replicaSets); err != nil {
		return err
	}
	owned := map[string]string{}
	for _, rs := range replicaSets.Items {
		rsMeta := objectMapField(rs, "metadata")
		if !appWorkloadControlledBy(rsMeta, "Deployment", name, uid) {
			continue
		}
		rsName, rsUID := objectStringField(rsMeta, "name"), objectStringField(rsMeta, "uid")
		if rsName == "" || rsUID == "" || !appWorkloadOwnerMatches(rsMeta, selector) {
			return fmt.Errorf("ReplicaSet identity differs")
		}
		labels := objectStringMapValue(nestedObjectValue(rs, "spec", "template", "metadata", "labels"))
		if value := labels[runtime.FugueLabelAppWorkload]; value != "" && value != name {
			return fmt.Errorf("ReplicaSet declares another workload")
		}
		if labels[runtime.FugueLabelAppWorkload] == "" {
			rsPath := "/apis/apps/v1/namespaces/" + url.PathEscape(namespace) + "/replicasets/" + url.PathEscape(rsName)
			if err := c.patchAppWorkloadObject(ctx, rsPath, rs, map[string]any{"spec": map[string]any{"template": map[string]any{"metadata": map[string]any{"labels": map[string]string{runtime.FugueLabelAppWorkload: name}}}}}); err != nil {
				if strings.Contains(err.Error(), "status=404") {
					continue
				}
				return err
			}
		}
		owned[rsName] = rsUID
	}
	var pods kubeObjectList
	if _, err := c.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+url.PathEscape(namespace)+"/pods?"+query.Encode(), nil, &pods); err != nil {
		return err
	}
	readyPods := 0
	for _, pod := range pods.Items {
		meta := objectMapField(pod, "metadata")
		matches := false
		for _, owner := range mapSlice(meta["ownerReferences"]) {
			rsName := objectStringField(owner, "name")
			if appWorkloadControlledBy(meta, "ReplicaSet", rsName, owned[rsName]) {
				matches = true
				break
			}
		}
		if !matches {
			continue
		}
		if !appWorkloadOwnerMatches(meta, selector) {
			return fmt.Errorf("Pod ownership differs")
		}
		if objectStringField(meta, "deletionTimestamp") == "" {
			for _, condition := range mapSlice(nestedObjectValue(pod, "status", "conditions")) {
				if condition["type"] == "Ready" && condition["status"] == "True" {
					readyPods++
				}
			}
		}
		labels := objectStringMapValue(meta["labels"])
		if value := labels[runtime.FugueLabelAppWorkload]; value != "" && value != name {
			return fmt.Errorf("Pod declares another workload")
		}
		if labels[runtime.FugueLabelAppWorkload] == "" {
			podPath := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods/" + url.PathEscape(objectStringField(meta, "name"))
			if err := c.patchAppWorkloadObject(ctx, podPath, pod, map[string]any{"metadata": map[string]any{"labels": map[string]string{runtime.FugueLabelAppWorkload: name}}}); err != nil {
				return err
			}
		}
	}
	if ready, _ := objectMapField(deployment, "status")["readyReplicas"].(float64); ready > 0 && readyPods == 0 {
		return fmt.Errorf("no ready Pod owned by the Service workload; selector unchanged")
	}
	if migration != "" {
		// Updating both templates while paused keeps the existing ReplicaSet as
		// the current one. A crash leaves a durable marker for the next reconcile.
		if err := c.patchAppWorkloadObject(ctx, path, deployment, map[string]any{"spec": map[string]any{"template": map[string]any{"metadata": map[string]any{"labels": map[string]string{runtime.FugueLabelAppWorkload: name}}}}}); err != nil {
			return err
		}
		current, found, err := c.getRawObject(ctx, path)
		if err != nil || !found || objectStringField(objectMapField(current, "metadata"), "uid") != uid {
			return fmt.Errorf("Deployment changed before workload migration completion")
		}
		currentMeta := objectMapField(current, "metadata")
		currentLabels := objectStringMapValue(nestedObjectValue(current, "spec", "template", "metadata", "labels"))
		if paused, _ := objectMapField(current, "spec")["paused"].(bool); !paused || currentLabels[runtime.FugueLabelAppWorkload] != name || objectStringMapValue(currentMeta["annotations"])[appWorkloadMigrationAnnotation] != migration {
			return fmt.Errorf("workload migration state changed before completion")
		}
		if err := c.patchAppWorkloadObject(ctx, path, current, map[string]any{
			"metadata": map[string]any{"annotations": map[string]any{appWorkloadMigrationAnnotation: nil}},
			"spec":     map[string]any{"paused": migration == "paused"},
		}); err != nil {
			return err
		}
	}
	return nil
}

func appWorkloadPauseObserved(deployment map[string]any) bool {
	paused, _ := objectMapField(deployment, "spec")["paused"].(bool)
	generation, _ := objectMapField(deployment, "metadata")["generation"].(float64)
	observed, _ := objectMapField(deployment, "status")["observedGeneration"].(float64)
	return paused && generation > 0 && observed >= generation
}

func appWorkloadOwnerMatches(metadata map[string]any, selector map[string]string) bool {
	labels := objectStringMapValue(metadata["labels"])
	for _, key := range []string{runtime.FugueLabelAppID, runtime.FugueLabelTenantID} {
		if selector[key] == "" || labels[key] != selector[key] {
			return false
		}
	}
	return labels[runtime.FugueLabelManagedBy] == runtime.FugueLabelManagedByValue
}

func appWorkloadControlledBy(metadata map[string]any, kind, name, uid string) bool {
	for _, owner := range mapSlice(metadata["ownerReferences"]) {
		controller, _ := owner["controller"].(bool)
		if controller && owner["kind"] == kind && owner["name"] == name && owner["uid"] == uid && uid != "" {
			return true
		}
	}
	return false
}

func (c *kubeClient) patchAppWorkloadObject(ctx context.Context, path string, current, patch map[string]any) error {
	meta := objectMapField(current, "metadata")
	uid, version := objectStringField(meta, "uid"), objectStringField(meta, "resourceVersion")
	if strings.TrimSpace(uid) == "" || strings.TrimSpace(version) == "" {
		return fmt.Errorf("workload patch requires UID and resourceVersion")
	}
	patchMeta := objectMapField(patch, "metadata")
	if patchMeta == nil {
		patchMeta = map[string]any{}
		patch["metadata"] = patchMeta
	}
	for attempt := 0; attempt < 4; attempt++ {
		patchMeta["uid"], patchMeta["resourceVersion"] = uid, version
		status, err := c.doRequest(ctx, http.MethodPatch, path, "application/merge-patch+json", patch, nil)
		if err == nil || status != http.StatusConflict || attempt == 3 {
			return err
		}
		fresh, found, readErr := c.getRawObject(ctx, path)
		if readErr != nil || !found {
			return err
		}
		freshMeta := objectMapField(fresh, "metadata")
		// Kubernetes status writes advance resourceVersion without changing
		// ownership or intent. Only those conflicts may be retried here.
		for _, key := range []string{"uid", "labels", "annotations", "ownerReferences", "deletionTimestamp"} {
			if !normalizedKubeValueEqual(meta[key], freshMeta[key]) {
				return err
			}
		}
		if !normalizedKubeValueEqual(current["spec"], fresh["spec"]) {
			return err
		}
		version = objectStringField(freshMeta, "resourceVersion")
		if version == "" {
			return err
		}
	}
	return nil
}
