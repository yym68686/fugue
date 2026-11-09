package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// A retained revision keeps its dependencies as well as its Deployment and
// Service. Read the executable references instead of the current App spec:
// canonical alignment and subsequent edits can both change that spec.
func (s *Service) preserveManagedAppWorkloadSecrets(ctx context.Context, client *kubeClient, namespace string, app model.App, deployments, secrets []string, desiredObjects []map[string]any, desiredByKind map[string]map[string]struct{}) error {
	if desiredByKind["Secret"] == nil {
		desiredByKind["Secret"] = make(map[string]struct{})
	}
	protected := desiredByKind["Secret"]
	known := make(map[string]struct{}, len(secrets))
	for _, name := range secrets {
		known[name] = struct{}{}
	}
	// Current canonical objects have already been applied. Only retained
	// independent revisions need additional Deployment reads.
	desiredDeployments := desiredObjectNamesByKind(desiredObjects)["Deployment"]
	for _, name := range deployments {
		if _, retained := desiredByKind["Deployment"][name]; !retained {
			continue
		}
		if _, current := desiredDeployments[name]; current {
			continue
		}
		dep, found, err := client.getRawObject(ctx, deploymentAPIPath(namespace, name))
		if err != nil {
			return fmt.Errorf("read retained workload Secret references: %w", err)
		}
		if !found {
			continue
		}
		podSpec := objectMapField(objectMapField(objectMapField(dep, "spec"), "template"), "spec")
		protectPodSpecSecrets(podSpec, protected)
		if err := s.restoreAppReleaseFileSecrets(ctx, client, namespace, app, dep, known); err != nil {
			return err
		}
	}
	// A Deployment can be gone while its old or terminating Pods still use a
	// Secret. Only pay for this inventory when a deletion is actually possible.
	needsPodInventory := false
	for _, name := range secrets {
		if _, keep := protected[name]; !keep {
			needsPodInventory = true
			break
		}
	}
	if needsPodInventory {
		for _, selector := range ownedAppSelectors(app.ID) {
			var pods kubeObjectList
			path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods?labelSelector=" + url.QueryEscape(selector) + "&limit=500"
			if _, err := client.doJSON(ctx, http.MethodGet, path, nil, &pods); err != nil {
				return fmt.Errorf("read Pod Secret references before pruning: %w", err)
			}
			// Fail closed on an incomplete inventory; ordinary reconciliation
			// can retry without deleting dependencies from unseen Pods.
			if pods.Metadata.Continue != "" {
				return fmt.Errorf("Pod Secret reference inventory is incomplete")
			}
			for _, pod := range pods.Items {
				protectPodSpecSecrets(objectMapField(pod, "spec"), protected)
			}
		}
	}
	return nil
}

func protectPodSpecSecrets(spec map[string]any, names map[string]struct{}) {
	add := func(value any) {
		if name, ok := value.(string); ok && strings.TrimSpace(name) != "" {
			names[name] = struct{}{}
		}
	}
	for _, volume := range mapSlice(spec["volumes"]) {
		add(objectMapField(volume, "secret")["secretName"])
		for _, source := range mapSlice(objectMapField(volume, "projected")["sources"]) {
			add(objectMapField(source, "secret")["name"])
		}
	}
	for _, ref := range mapSlice(spec["imagePullSecrets"]) {
		add(ref["name"])
	}
	for _, field := range []string{"containers", "initContainers", "ephemeralContainers"} {
		for _, container := range mapSlice(spec[field]) {
			for _, env := range mapSlice(container["env"]) {
				add(objectMapField(objectMapField(env, "valueFrom"), "secretKeyRef")["name"])
			}
			for _, env := range mapSlice(container["envFrom"]) {
				add(objectMapField(env, "secretRef")["name"])
			}
		}
	}
}

func (s *Service) restoreAppReleaseFileSecrets(ctx context.Context, client *kubeClient, namespace string, app model.App, dep map[string]any, known map[string]struct{}) error {
	if s == nil || s.Store == nil {
		return nil
	}
	volumes := mapSlice(nestedObjectValue(dep, "spec", "template", "spec", "volumes"))
	missing := make(map[string]struct{})
	for _, volume := range volumes {
		volumeName := objectStringField(volume, "name")
		name := objectStringField(objectMapField(volume, "secret"), "secretName")
		if volumeName == runtime.AppFilesVolumeName || strings.HasPrefix(volumeName, runtime.AppFilesVolumeName+"-") {
			if _, exists := known[name]; !exists && name != "" {
				missing[name] = struct{}{}
			}
		}
	}
	if len(missing) == 0 {
		return nil // No configuration reads or writes on the normal path.
	}
	meta := objectMapField(dep, "metadata")
	releaseID := objectStringMapValue(meta["labels"])[runtime.FugueLabelAppReleaseID]
	if releaseID == "" {
		return nil // Legacy/unbound resources cannot authorize reconstruction.
	}
	release, err := s.Store.GetAppRelease(app.TenantID, false, releaseID)
	if err != nil {
		return fmt.Errorf("read release for file Secret recovery: %w", err)
	}
	if !releaseFileSecretRecoveryAllowed(app, namespace, release, dep) {
		return nil
	}
	snapshot := app
	snapshot.Spec = *release.SpecSnapshot
	objects := runtime.BuildAppFilesSecretObjects(snapshot, release.RevisionWorkload.DeploymentName)
	// Verify the file layout too: never substitute today's chunk assignment
	// for a differently rendered historical volume.
	verified := 0
	for index, obj := range objects {
		name := objectStringField(objectMapField(obj, "metadata"), "name")
		if _, needed := missing[name]; !needed {
			continue
		}
		volumeName := runtime.AppFilesVolumeName
		if index > 0 {
			volumeName = fmt.Sprintf("%s-%d", volumeName, index)
		}
		if !releaseFileSecretVolumeMatches(volumes, volumeName, name, objectStringMapValue(obj["stringData"])) {
			return fmt.Errorf("release file Secret volume layout differs from immutable snapshot")
		}
		verified++
	}
	if verified != len(missing) {
		return fmt.Errorf("release file Secret references differ from immutable snapshot")
	}
	for _, obj := range objects {
		m := objectMapField(obj, "metadata")
		name := objectStringField(m, "name")
		if _, needed := missing[name]; !needed {
			continue
		}
		path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/secrets"
		if _, found, err := client.getRawObject(ctx, path+"/"+url.PathEscape(name)); err != nil {
			return err
		} else if found {
			continue // Never overwrite an existing or concurrently created Secret.
		}
		// Recheck both lifecycle and exact executable identity immediately before
		// creating. An owner reference also makes a concurrent retirement safe.
		latest, err := s.Store.GetAppRelease(app.TenantID, false, release.ID)
		if err != nil {
			return err
		}
		current, found, err := client.getRawObject(ctx, deploymentAPIPath(namespace, release.RevisionWorkload.DeploymentName))
		if err != nil {
			return err
		}
		if !found || !releaseFileSecretRecoveryAllowed(app, namespace, latest, current) || !latest.UpdatedAt.Equal(release.UpdatedAt) {
			return fmt.Errorf("release workload changed during file Secret recovery")
		}
		labels := objectStringMapValue(m["labels"])
		labels[runtime.FugueLabelAppReleaseID] = release.ID
		labels[runtime.FugueLabelAppReleaseRole] = objectStringMapValue(meta["labels"])[runtime.FugueLabelAppReleaseRole]
		m["labels"] = labels
		m["ownerReferences"] = []map[string]any{{"apiVersion": "apps/v1", "kind": "Deployment", "name": release.RevisionWorkload.DeploymentName, "uid": release.RevisionWorkload.DeploymentUID}}
		client.writeStats.record("create_attempted", obj)
		if status, err := client.doJSON(ctx, http.MethodPost, path, obj, nil); err != nil {
			return fmt.Errorf("restore release file Secret (status %d); retry after refreshing ownership: %w", status, err)
		}
	}
	return nil
}

func releaseFileSecretRecoveryAllowed(app model.App, namespace string, release model.AppRelease, dep map[string]any) bool {
	b := release.RevisionWorkload
	if app.Spec.Replicas <= 0 || b == nil || release.SpecSnapshot == nil || len(release.SpecSnapshot.Files) == 0 || release.AppID != app.ID || release.TenantID != app.TenantID ||
		(release.Status != model.AppReleaseStatusReady && release.Status != model.AppReleaseStatusServing) || release.Role == model.AppReleaseRoleRetired ||
		b.Namespace != namespace || namespace != runtime.NamespaceForTenant(app.TenantID) || b.DeploymentUID == "" || b.DeploymentGeneration < 1 || b.ReleaseKey == "" ||
		b.RuntimeID != release.RuntimeID || b.ImageRef != release.ResolvedImageRef {
		return false
	}
	m := objectMapField(dep, "metadata")
	generation, _ := m["generation"].(float64)
	return objectStringField(m, "name") == b.DeploymentName && objectStringField(m, "namespace") == namespace && objectStringField(m, "uid") == b.DeploymentUID &&
		objectStringField(m, "deletionTimestamp") == "" && generation == float64(b.DeploymentGeneration) &&
		appWorkloadOwnerMatches(m, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) &&
		objectStringMapValue(m["labels"])[runtime.FugueLabelAppReleaseID] == release.ID &&
		objectStringMapValue(m["annotations"])[runtime.FugueAnnotationReleaseKey] == b.ReleaseKey &&
		objectStringMapValue(nestedObjectValue(dep, "spec", "template", "metadata", "annotations"))[runtime.FugueAnnotationReleaseKey] == b.ReleaseKey
}

func releaseFileSecretVolumeMatches(volumes []map[string]any, volumeName, secretName string, data map[string]string) bool {
	for _, volume := range volumes {
		if volume["name"] != volumeName {
			continue
		}
		secret := objectMapField(volume, "secret")
		items := mapSlice(secret["items"])
		if secret["secretName"] != secretName || len(items) != len(data) {
			return false
		}
		seen := make(map[string]bool, len(items))
		for _, item := range items {
			key := objectStringField(item, "key")
			if _, ok := data[key]; !ok || seen[key] || item["path"] != key {
				return false
			}
			seen[key] = true
		}
		return true
	}
	return false
}
