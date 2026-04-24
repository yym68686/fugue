package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/runtime"
)

type kubeObjectList struct {
	Items []map[string]any `json:"items"`
}

type kubeDeployment struct {
	Metadata struct {
		Name       string `json:"name"`
		Generation int64  `json:"generation,omitempty"`
	} `json:"metadata"`
	Spec struct {
		Replicas *int `json:"replicas,omitempty"`
	} `json:"spec"`
	Status struct {
		ObservedGeneration  int64                         `json:"observedGeneration,omitempty"`
		Replicas            int                           `json:"replicas,omitempty"`
		UpdatedReplicas     int                           `json:"updatedReplicas,omitempty"`
		ReadyReplicas       int                           `json:"readyReplicas,omitempty"`
		AvailableReplicas   int                           `json:"availableReplicas,omitempty"`
		UnavailableReplicas int                           `json:"unavailableReplicas,omitempty"`
		Conditions          []runtime.ManagedAppCondition `json:"conditions,omitempty"`
	} `json:"status"`
}

type kubeCloudNativePGCluster struct {
	Metadata struct {
		Name        string            `json:"name"`
		Generation  int64             `json:"generation,omitempty"`
		Annotations map[string]string `json:"annotations,omitempty"`
	} `json:"metadata"`
	Spec struct {
		Instances int            `json:"instances,omitempty"`
		Affinity  map[string]any `json:"affinity,omitempty"`
	} `json:"spec"`
	Status struct {
		Phase                  string `json:"phase,omitempty"`
		PhaseReason            string `json:"phaseReason,omitempty"`
		ReadyInstances         int    `json:"readyInstances,omitempty"`
		CurrentPrimary         string `json:"currentPrimary,omitempty"`
		TargetPrimary          string `json:"targetPrimary,omitempty"`
		TargetPrimaryTimestamp string `json:"targetPrimaryTimestamp,omitempty"`
	} `json:"status"`
}

func (c *kubeClient) applyObject(ctx context.Context, obj map[string]any, out any) error {
	apiPath, err := runtime.ObjectAPIPath(c.namespace, obj)
	if err != nil {
		return err
	}
	if err := c.applyObjectAtPath(ctx, apiPath, obj, out); err == nil {
		return nil
	} else if shouldRetryDeploymentAfterStaleAppFilesVolumeMounts(obj, err) {
		name, namespace := objectNameAndNamespace(c.namespace, obj)
		if cleanupErr := c.removeDeploymentVolumeReferencesByName(ctx, namespace, name, runtime.AppFilesVolumeName); cleanupErr != nil {
			return fmt.Errorf("remove stale app file volume references after deployment apply failure: %w (original apply error: %v)", cleanupErr, err)
		}
		if retryErr := c.applyObjectAtPath(ctx, apiPath, obj, out); retryErr != nil {
			return fmt.Errorf("%w (after removing stale app file volume references)", retryErr)
		}
		return nil
	} else if !shouldRecreateDeploymentAfterImmutableSelector(obj, err) {
		return err
	} else {
		name, namespace := objectNameAndNamespace(c.namespace, obj)
		if err := c.deleteDeployment(ctx, namespace, name); err != nil {
			return fmt.Errorf("delete deployment %s/%s after immutable selector apply failure: %w", namespace, name, err)
		}
		if err := c.waitForDeploymentDeleted(ctx, namespace, name); err != nil {
			return fmt.Errorf("wait for deployment %s/%s deletion after immutable selector apply failure: %w", namespace, name, err)
		}
		return c.applyObjectAtPath(ctx, apiPath, obj, out)
	}
}

func (c *kubeClient) applyObjectAtPath(ctx context.Context, apiPath string, obj map[string]any, out any) error {
	query := url.Values{}
	query.Set("fieldManager", runtime.FugueLabelManagedByValue)
	query.Set("force", "true")
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	_, err := c.doRequest(ctx, http.MethodPatch, apiPath, "application/apply-patch+yaml", obj, out)
	return err
}

func (c *kubeClient) waitForDeploymentDeleted(ctx context.Context, namespace, name string) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		_, found, err := c.getDeployment(ctx, namespace, name)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *kubeClient) applyObjects(ctx context.Context, objects []map[string]any) error {
	for _, obj := range objects {
		if err := c.applyObject(ctx, obj, nil); err != nil {
			return err
		}
	}
	return nil
}

func (c *kubeClient) getManagedApp(ctx context.Context, namespace, name string) (runtime.ManagedAppObject, bool, error) {
	var raw map[string]any
	status, err := c.doJSON(ctx, http.MethodGet, managedAppAPIPath(namespace, name), nil, &raw)
	if err != nil {
		if status == http.StatusNotFound {
			return runtime.ManagedAppObject{}, false, nil
		}
		return runtime.ManagedAppObject{}, false, err
	}
	managed, err := runtime.ManagedAppObjectFromMap(raw)
	if err != nil {
		return runtime.ManagedAppObject{}, false, err
	}
	return managed, true, nil
}

func (c *kubeClient) listManagedApps(ctx context.Context) ([]runtime.ManagedAppObject, error) {
	var list kubeObjectList
	if _, err := c.doJSON(ctx, http.MethodGet, "/apis/"+runtime.ManagedAppAPIGroup+"/v1alpha1/"+runtime.ManagedAppPlural, nil, &list); err != nil {
		return nil, err
	}

	items := make([]runtime.ManagedAppObject, 0, len(list.Items))
	for _, raw := range list.Items {
		managed, err := runtime.ManagedAppObjectFromMap(raw)
		if err != nil {
			return nil, err
		}
		items = append(items, managed)
	}
	return items, nil
}

func (c *kubeClient) deleteManagedApp(ctx context.Context, namespace, name string) error {
	_, err := c.doRequest(ctx, http.MethodDelete, managedAppAPIPath(namespace, name), "", nil, nil)
	if err != nil {
		if strings.Contains(err.Error(), "status=404") {
			return nil
		}
		return err
	}
	return nil
}

func (c *kubeClient) patchManagedAppStatus(ctx context.Context, namespace, name string, status runtime.ManagedAppStatus) error {
	body := map[string]any{
		"status": status,
	}
	_, err := c.doRequest(
		ctx,
		http.MethodPatch,
		managedAppAPIPath(namespace, name)+"/status",
		"application/merge-patch+json",
		body,
		nil,
	)
	return err
}

func (c *kubeClient) patchCloudNativePGClusterStatus(
	ctx context.Context,
	namespace, name, targetPrimary, phase, phaseReason string,
) error {
	body := map[string]any{
		"status": map[string]any{
			"targetPrimary":          strings.TrimSpace(targetPrimary),
			"targetPrimaryTimestamp": time.Now().UTC().Format(time.RFC3339),
			"phase":                  strings.TrimSpace(phase),
			"phaseReason":            strings.TrimSpace(phaseReason),
		},
	}
	_, err := c.doRequest(
		ctx,
		http.MethodPatch,
		cloudNativePGClusterAPIPath(c.effectiveNamespace(namespace), name)+"/status",
		"application/merge-patch+json",
		body,
		nil,
	)
	return err
}

func (c *kubeClient) getDeployment(ctx context.Context, namespace, name string) (kubeDeployment, bool, error) {
	var deployment kubeDeployment
	status, err := c.doJSON(ctx, http.MethodGet, deploymentAPIPath(c.effectiveNamespace(namespace), name), nil, &deployment)
	if err != nil {
		if status == http.StatusNotFound {
			return kubeDeployment{}, false, nil
		}
		return kubeDeployment{}, false, err
	}
	return deployment, true, nil
}

func (c *kubeClient) getCloudNativePGCluster(ctx context.Context, namespace, name string) (kubeCloudNativePGCluster, bool, error) {
	var cluster kubeCloudNativePGCluster
	status, err := c.doJSON(ctx, http.MethodGet, cloudNativePGClusterAPIPath(c.effectiveNamespace(namespace), name), nil, &cluster)
	if err != nil {
		if status == http.StatusNotFound {
			return kubeCloudNativePGCluster{}, false, nil
		}
		return kubeCloudNativePGCluster{}, false, err
	}
	return cluster, true, nil
}

func (c *kubeClient) getVolSyncReplicationDestination(ctx context.Context, namespace, name string) (map[string]any, bool, error) {
	return c.getRawNamespacedObject(ctx, volSyncReplicationDestinationAPIPath(c.effectiveNamespace(namespace), name))
}

func (c *kubeClient) getVolSyncReplicationSource(ctx context.Context, namespace, name string) (map[string]any, bool, error) {
	return c.getRawNamespacedObject(ctx, volSyncReplicationSourceAPIPath(c.effectiveNamespace(namespace), name))
}

func (c *kubeClient) listDeploymentNamesByLabel(ctx context.Context, namespace, labelSelector string) ([]string, error) {
	return c.listNamespacedResourceNames(ctx, "/apis/apps/v1/namespaces/"+c.effectiveNamespace(namespace)+"/deployments", labelSelector)
}

func (c *kubeClient) listCloudNativePGClusterNamesByLabel(ctx context.Context, namespace, labelSelector string) ([]string, error) {
	names, err := c.listNamespacedResourceNames(ctx, "/apis/postgresql.cnpg.io/v1/namespaces/"+c.effectiveNamespace(namespace)+"/clusters", labelSelector)
	if isKubernetesResourceNotFound(err) {
		return nil, nil
	}
	return names, err
}

func (c *kubeClient) listServiceNamesByLabel(ctx context.Context, namespace, labelSelector string) ([]string, error) {
	return c.listNamespacedResourceNames(ctx, "/api/v1/namespaces/"+c.effectiveNamespace(namespace)+"/services", labelSelector)
}

func (c *kubeClient) listPersistentVolumeClaimNamesByLabel(ctx context.Context, namespace, labelSelector string) ([]string, error) {
	return c.listNamespacedResourceNames(ctx, "/api/v1/namespaces/"+c.effectiveNamespace(namespace)+"/persistentvolumeclaims", labelSelector)
}

func (c *kubeClient) listVolSyncReplicationDestinationNamesByLabel(ctx context.Context, namespace, labelSelector string) ([]string, error) {
	names, err := c.listNamespacedResourceNames(ctx, "/apis/volsync.backube/v1alpha1/namespaces/"+c.effectiveNamespace(namespace)+"/replicationdestinations", labelSelector)
	if isKubernetesResourceNotFound(err) {
		return nil, nil
	}
	return names, err
}

func (c *kubeClient) listVolSyncReplicationSourceNamesByLabel(ctx context.Context, namespace, labelSelector string) ([]string, error) {
	names, err := c.listNamespacedResourceNames(ctx, "/apis/volsync.backube/v1alpha1/namespaces/"+c.effectiveNamespace(namespace)+"/replicationsources", labelSelector)
	if isKubernetesResourceNotFound(err) {
		return nil, nil
	}
	return names, err
}

func (c *kubeClient) listSecretNamesByLabel(ctx context.Context, namespace, labelSelector string) ([]string, error) {
	return c.listNamespacedResourceNames(ctx, "/api/v1/namespaces/"+c.effectiveNamespace(namespace)+"/secrets", labelSelector)
}

func (c *kubeClient) deleteDeployment(ctx context.Context, namespace, name string) error {
	_, err := c.doRequest(ctx, http.MethodDelete, deploymentAPIPath(c.effectiveNamespace(namespace), name), "", nil, nil)
	return normalizeDeleteNotFound(err)
}

func (c *kubeClient) deleteCloudNativePGCluster(ctx context.Context, namespace, name string) error {
	_, err := c.doRequest(ctx, http.MethodDelete, cloudNativePGClusterAPIPath(c.effectiveNamespace(namespace), name), "", nil, nil)
	return normalizeDeleteNotFound(err)
}

func (c *kubeClient) deleteService(ctx context.Context, namespace, name string) error {
	_, err := c.doRequest(ctx, http.MethodDelete, "/api/v1/namespaces/"+c.effectiveNamespace(namespace)+"/services/"+url.PathEscape(name), "", nil, nil)
	return normalizeDeleteNotFound(err)
}

func (c *kubeClient) deletePersistentVolumeClaim(ctx context.Context, namespace, name string) error {
	_, err := c.doRequest(ctx, http.MethodDelete, "/api/v1/namespaces/"+c.effectiveNamespace(namespace)+"/persistentvolumeclaims/"+url.PathEscape(name), "", nil, nil)
	return normalizeDeleteNotFound(err)
}

func (c *kubeClient) deleteVolSyncReplicationDestination(ctx context.Context, namespace, name string) error {
	_, err := c.doRequest(ctx, http.MethodDelete, volSyncReplicationDestinationAPIPath(c.effectiveNamespace(namespace), name), "", nil, nil)
	return normalizeDeleteNotFound(err)
}

func (c *kubeClient) deleteVolSyncReplicationSource(ctx context.Context, namespace, name string) error {
	_, err := c.doRequest(ctx, http.MethodDelete, volSyncReplicationSourceAPIPath(c.effectiveNamespace(namespace), name), "", nil, nil)
	return normalizeDeleteNotFound(err)
}

func (c *kubeClient) deleteSecret(ctx context.Context, namespace, name string) error {
	_, err := c.doRequest(ctx, http.MethodDelete, "/api/v1/namespaces/"+c.effectiveNamespace(namespace)+"/secrets/"+url.PathEscape(name), "", nil, nil)
	return normalizeDeleteNotFound(err)
}

func (c *kubeClient) forceDeletePod(ctx context.Context, namespace, name string) error {
	apiPath := "/api/v1/namespaces/" + c.effectiveNamespace(namespace) + "/pods/" + url.PathEscape(strings.TrimSpace(name))
	query := url.Values{}
	query.Set("gracePeriodSeconds", "0")
	query.Set("propagationPolicy", "Background")
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	_, err := c.doRequest(ctx, http.MethodDelete, apiPath, "", nil, nil)
	return normalizeDeleteNotFound(err)
}

func (c *kubeClient) patchVolSyncReplicationSourceTrigger(ctx context.Context, namespace, name, manual string) error {
	body := map[string]any{
		"spec": map[string]any{
			"trigger": map[string]any{
				"manual": strings.TrimSpace(manual),
			},
		},
	}
	_, err := c.doRequest(ctx, http.MethodPatch, volSyncReplicationSourceAPIPath(c.effectiveNamespace(namespace), name), "application/merge-patch+json", body, nil)
	return err
}

func (c *kubeClient) listNamespacedResourceNames(ctx context.Context, apiPath, labelSelector string) ([]string, error) {
	query := url.Values{}
	if strings.TrimSpace(labelSelector) != "" {
		query.Set("labelSelector", labelSelector)
	}
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}

	var list kubeObjectList
	if _, err := c.doJSON(ctx, http.MethodGet, apiPath, nil, &list); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(list.Items))
	for _, item := range list.Items {
		metadata, _ := item["metadata"].(map[string]any)
		name, _ := metadata["name"].(string)
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	return names, nil
}

func (c *kubeClient) doRequest(ctx context.Context, method, apiPath, contentType string, body any, out any) (int, error) {
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("marshal kubernetes request: %w", err)
		}
		payload = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+apiPath, payload)
	if err != nil {
		return 0, fmt.Errorf("create kubernetes request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		if strings.TrimSpace(contentType) == "" {
			contentType = "application/json"
		}
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("kubernetes request %s %s: %w", method, apiPath, err)
	}
	defer resp.Body.Close()

	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("kubernetes request %s %s failed: status=%d body=%s", method, apiPath, resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	if out != nil && len(responseBody) > 0 {
		if err := json.Unmarshal(responseBody, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode kubernetes response: %w", err)
		}
	}
	return resp.StatusCode, nil
}

func managedAppAPIPath(namespace, name string) string {
	return "/apis/" + runtime.ManagedAppAPIGroup + "/v1alpha1/namespaces/" + url.PathEscape(strings.TrimSpace(namespace)) + "/" + runtime.ManagedAppPlural + "/" + url.PathEscape(strings.TrimSpace(name))
}

func cloudNativePGClusterAPIPath(namespace, name string) string {
	return "/apis/postgresql.cnpg.io/v1/namespaces/" + url.PathEscape(strings.TrimSpace(namespace)) + "/clusters/" + url.PathEscape(strings.TrimSpace(name))
}

func volSyncReplicationDestinationAPIPath(namespace, name string) string {
	return "/apis/volsync.backube/v1alpha1/namespaces/" + url.PathEscape(strings.TrimSpace(namespace)) + "/replicationdestinations/" + url.PathEscape(strings.TrimSpace(name))
}

func volSyncReplicationSourceAPIPath(namespace, name string) string {
	return "/apis/volsync.backube/v1alpha1/namespaces/" + url.PathEscape(strings.TrimSpace(namespace)) + "/replicationsources/" + url.PathEscape(strings.TrimSpace(name))
}

func (c *kubeClient) getRawNamespacedObject(ctx context.Context, apiPath string) (map[string]any, bool, error) {
	var out map[string]any
	status, err := c.doJSON(ctx, http.MethodGet, apiPath, nil, &out)
	if err != nil {
		if status == http.StatusNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	return out, true, nil
}

func normalizeDeleteNotFound(err error) error {
	if err == nil {
		return nil
	}
	if isKubernetesResourceNotFound(err) {
		return nil
	}
	return err
}

func isKubernetesResourceNotFound(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "status=404") ||
		strings.Contains(message, "could not find the requested resource")
}

func shouldRecreateDeploymentAfterImmutableSelector(obj map[string]any, err error) bool {
	if err == nil {
		return false
	}
	apiVersion, _ := obj["apiVersion"].(string)
	kind, _ := obj["kind"].(string)
	if apiVersion != "apps/v1" || kind != "Deployment" {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "spec.selector") && strings.Contains(message, "immutable")
}

func shouldRetryDeploymentAfterStaleAppFilesVolumeMounts(obj map[string]any, err error) bool {
	if err == nil {
		return false
	}
	apiVersion, _ := obj["apiVersion"].(string)
	kind, _ := obj["kind"].(string)
	if apiVersion != "apps/v1" || kind != "Deployment" {
		return false
	}
	if podSpecVolumeNamed(obj, runtime.AppFilesVolumeName) {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "volumemounts") &&
		strings.Contains(message, "not found") &&
		strings.Contains(message, strings.ToLower(runtime.AppFilesVolumeName))
}

func (c *kubeClient) removeDeploymentVolumeReferencesByName(ctx context.Context, namespace, name, volumeName string) error {
	var deployment map[string]any
	apiPath := deploymentAPIPath(c.effectiveNamespace(namespace), name)
	status, err := c.doJSON(ctx, http.MethodGet, apiPath, nil, &deployment)
	if err != nil {
		if status == http.StatusNotFound {
			return nil
		}
		return err
	}
	ops := deploymentVolumeReferenceRemoveOps(deployment, volumeName)
	if len(ops) == 0 {
		return nil
	}
	_, err = c.doRequest(ctx, http.MethodPatch, apiPath, "application/json-patch+json", ops, nil)
	return err
}

func deploymentVolumeReferenceRemoveOps(deployment map[string]any, volumeName string) []map[string]string {
	var ops []map[string]string
	podSpec, ok := nestedMap(deployment, "spec", "template", "spec")
	if !ok {
		return nil
	}
	ops = append(ops, volumeMountRemoveOps(podSpec, volumeName, "containers")...)
	ops = append(ops, volumeMountRemoveOps(podSpec, volumeName, "initContainers")...)
	ops = append(ops, volumeRemoveOps(podSpec, volumeName)...)
	return ops
}

func volumeMountRemoveOps(podSpec map[string]any, volumeName, containerField string) []map[string]string {
	containers := mapSlice(podSpec[containerField])
	if len(containers) == 0 {
		return nil
	}
	var ops []map[string]string
	for containerIndex, container := range containers {
		mounts := mapSlice(container["volumeMounts"])
		if len(mounts) == 0 {
			continue
		}
		for mountIndex := len(mounts) - 1; mountIndex >= 0; mountIndex-- {
			mount := mounts[mountIndex]
			if strings.TrimSpace(fmt.Sprint(mount["name"])) != volumeName {
				continue
			}
			ops = append(ops, map[string]string{
				"op":   "remove",
				"path": fmt.Sprintf("/spec/template/spec/%s/%d/volumeMounts/%d", containerField, containerIndex, mountIndex),
			})
		}
	}
	return ops
}

func volumeRemoveOps(podSpec map[string]any, volumeName string) []map[string]string {
	volumes := mapSlice(podSpec["volumes"])
	if len(volumes) == 0 {
		return nil
	}
	var ops []map[string]string
	for index := len(volumes) - 1; index >= 0; index-- {
		volume := volumes[index]
		if strings.TrimSpace(fmt.Sprint(volume["name"])) != volumeName {
			continue
		}
		ops = append(ops, map[string]string{
			"op":   "remove",
			"path": fmt.Sprintf("/spec/template/spec/volumes/%d", index),
		})
	}
	return ops
}

func podSpecVolumeNamed(obj map[string]any, volumeName string) bool {
	podSpec, ok := nestedMap(obj, "spec", "template", "spec")
	if !ok {
		return false
	}
	volumes := mapSlice(podSpec["volumes"])
	if len(volumes) == 0 {
		return false
	}
	for _, volume := range volumes {
		if strings.TrimSpace(fmt.Sprint(volume["name"])) == volumeName {
			return true
		}
	}
	return false
}

func mapSlice(value any) []map[string]any {
	switch typed := value.(type) {
	case []map[string]any:
		return typed
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			mapped, ok := item.(map[string]any)
			if !ok {
				continue
			}
			out = append(out, mapped)
		}
		return out
	default:
		return nil
	}
}

func nestedMap(obj map[string]any, keys ...string) (map[string]any, bool) {
	current := obj
	for _, key := range keys {
		next, ok := current[key].(map[string]any)
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func objectNameAndNamespace(defaultNamespace string, obj map[string]any) (string, string) {
	metadata, _ := obj["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	namespace, _ := metadata["namespace"].(string)
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(defaultNamespace)
	}
	return strings.TrimSpace(name), namespace
}

func deploymentAPIPath(namespace, name string) string {
	return "/apis/apps/v1/namespaces/" + url.PathEscape(strings.TrimSpace(namespace)) + "/deployments/" + url.PathEscape(strings.TrimSpace(name))
}
