package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

const (
	pvcSelectedNodeAnnotation = "volume.kubernetes.io/selected-node"
	kubeHostnameLabelKey      = "kubernetes.io/hostname"
)

func (s *Service) alignManagedPostgresRuntimeToObservedPrimary(ctx context.Context, app model.App) (model.AppSpec, bool, error) {
	desired := cloneControllerAppSpec(&app.Spec)
	if desired == nil {
		return model.AppSpec{}, false, nil
	}

	postgres := store.OwnedManagedPostgresSpec(app)
	if postgres == nil {
		return *desired, false, nil
	}

	desiredRuntimeID := strings.TrimSpace(postgres.RuntimeID)
	if desiredRuntimeID == "" {
		desiredRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	if desiredRuntimeID == "" {
		return *desired, false, nil
	}

	actualRuntimeID, detail, err := s.observedManagedPostgresPrimaryRuntimeID(ctx, app, *postgres)
	if err != nil {
		return *desired, false, err
	}
	if actualRuntimeID == "" || actualRuntimeID == desiredRuntimeID {
		return *desired, false, nil
	}

	if desired.Postgres == nil {
		desired.Postgres = cloneControllerPostgresSpec(postgres)
	} else {
		desired.Postgres = cloneControllerPostgresSpec(desired.Postgres)
	}

	targetRuntimeID := strings.TrimSpace(postgres.FailoverTargetRuntimeID)
	consumeFailover, err := s.shouldConsumeObservedManagedPostgresFailover(
		desiredRuntimeID,
		actualRuntimeID,
		targetRuntimeID,
	)
	if err != nil {
		return *desired, false, err
	}
	switch {
	case consumeFailover:
		desired.Postgres.RuntimeID = actualRuntimeID
		desired.Postgres.FailoverTargetRuntimeID = ""
		desired.Postgres.Instances = 1
		desired.Postgres.SynchronousReplicas = 0
		desired.Postgres.PrimaryPlacementPendingRebalance = false
		if s.Logger != nil {
			s.Logger.Printf(
				"consuming managed postgres failover for app %s from runtime %s to %s based on %s",
				app.ID,
				desiredRuntimeID,
				actualRuntimeID,
				detail,
			)
		}
		return *desired, true, nil
	case postgres.Instances == 1 && targetRuntimeID == "":
		desired.Postgres.RuntimeID = actualRuntimeID
		if s.Logger != nil {
			s.Logger.Printf(
				"aligning single-instance managed postgres for app %s from runtime %s to %s based on %s",
				app.ID,
				desiredRuntimeID,
				actualRuntimeID,
				detail,
			)
		}
		return *desired, true, nil
	default:
		return *desired, false, nil
	}
}

func (s *Service) shouldConsumeObservedManagedPostgresFailover(sourceRuntimeID, actualRuntimeID, targetRuntimeID string) (bool, error) {
	sourceRuntimeID = strings.TrimSpace(sourceRuntimeID)
	actualRuntimeID = strings.TrimSpace(actualRuntimeID)
	targetRuntimeID = strings.TrimSpace(targetRuntimeID)
	if sourceRuntimeID == "" || actualRuntimeID == "" || targetRuntimeID == "" {
		return false, nil
	}
	if actualRuntimeID != targetRuntimeID || actualRuntimeID == sourceRuntimeID {
		return false, nil
	}

	unavailable, err := s.runtimeUnavailable(sourceRuntimeID)
	if err != nil {
		return false, err
	}
	return unavailable, nil
}

func (s *Service) runtimeUnavailable(runtimeID string) (bool, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return false, nil
	}

	runtimeObj, err := s.Store.GetRuntime(runtimeID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return true, nil
		}
		return false, fmt.Errorf("load runtime %s: %w", runtimeID, err)
	}
	return strings.EqualFold(strings.TrimSpace(runtimeObj.Status), model.RuntimeStatusOffline), nil
}

func (s *Service) observedManagedPostgresPrimaryRuntimeID(ctx context.Context, app model.App, spec model.AppPostgresSpec) (string, string, error) {
	if strings.TrimSpace(app.TenantID) == "" || strings.TrimSpace(spec.ServiceName) == "" {
		return "", "", nil
	}

	client, err := s.kubeClient()
	if err != nil {
		return "", "", fmt.Errorf("initialize kubernetes client: %w", err)
	}

	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, spec.ServiceName)
	if err != nil {
		return "", "", fmt.Errorf("read cloudnativepg cluster %s/%s: %w", namespace, spec.ServiceName, err)
	}
	if !found {
		return "", "", nil
	}

	if primaryPod := strings.TrimSpace(cluster.Status.CurrentPrimary); primaryPod != "" {
		runtimeID, detail, err := s.runtimeIDForManagedPostgresPod(ctx, client, namespace, primaryPod)
		if err != nil {
			return "", "", err
		}
		if runtimeID != "" {
			return runtimeID, detail, nil
		}
	}

	pvcSelector := "cnpg.io/cluster=" + spec.ServiceName + ",role=primary"
	pvcNames, err := client.listPersistentVolumeClaimNamesByLabel(ctx, namespace, pvcSelector)
	if err != nil {
		return "", "", fmt.Errorf("list primary postgres pvcs for %s/%s: %w", namespace, spec.ServiceName, err)
	}
	for _, pvcName := range pvcNames {
		runtimeID, detail, err := s.runtimeIDForPersistentVolumeClaim(ctx, client, namespace, pvcName)
		if err != nil {
			return "", "", err
		}
		if runtimeID != "" {
			return runtimeID, detail, nil
		}
	}
	return "", "", nil
}

func (s *Service) runtimeIDForManagedPostgresPod(ctx context.Context, client *kubeClient, namespace, podName string) (string, string, error) {
	pod, found, err := client.getPod(ctx, namespace, podName)
	if err != nil {
		return "", "", fmt.Errorf("read postgres pod %s/%s: %w", namespace, podName, err)
	}
	if found {
		if nodeName := strings.TrimSpace(pod.Spec.NodeName); nodeName != "" {
			runtimeID, err := s.runtimeIDForNode(ctx, client, nodeName)
			if err != nil {
				return "", "", err
			}
			if runtimeID != "" {
				return runtimeID, "pod " + podName + " on node " + nodeName, nil
			}
		}
		if pvcName := managedPostgresPVCNameForPod(pod); pvcName != "" {
			return s.runtimeIDForPersistentVolumeClaim(ctx, client, namespace, pvcName)
		}
	}
	return s.runtimeIDForPersistentVolumeClaim(ctx, client, namespace, podName)
}

func managedPostgresPVCNameForPod(pod kubePod) string {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		if strings.TrimSpace(volume.Name) == "pgdata" {
			return strings.TrimSpace(volume.PersistentVolumeClaim.ClaimName)
		}
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		if claimName := strings.TrimSpace(volume.PersistentVolumeClaim.ClaimName); claimName != "" {
			return claimName
		}
	}
	return ""
}

func (s *Service) runtimeIDForPersistentVolumeClaim(ctx context.Context, client *kubeClient, namespace, pvcName string) (string, string, error) {
	pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, pvcName)
	if err != nil {
		return "", "", fmt.Errorf("read postgres pvc %s/%s: %w", namespace, pvcName, err)
	}
	if !found {
		return "", "", nil
	}

	if nodeName := strings.TrimSpace(pvc.Metadata.Annotations[pvcSelectedNodeAnnotation]); nodeName != "" {
		runtimeID, err := s.runtimeIDForNode(ctx, client, nodeName)
		if err != nil {
			return "", "", err
		}
		if runtimeID != "" {
			return runtimeID, "pvc " + pvcName + " selected-node " + nodeName, nil
		}
	}

	volumeName := strings.TrimSpace(pvc.Spec.VolumeName)
	if volumeName == "" {
		return "", "", nil
	}

	pv, found, err := client.getPersistentVolume(ctx, volumeName)
	if err != nil {
		if strings.Contains(err.Error(), "status=403") {
			return "", "", nil
		}
		return "", "", fmt.Errorf("read persistent volume %s for pvc %s/%s: %w", volumeName, namespace, pvcName, err)
	}
	if !found {
		return "", "", nil
	}

	if nodeName := persistentVolumeNodeName(pv); nodeName != "" {
		runtimeID, err := s.runtimeIDForNode(ctx, client, nodeName)
		if err != nil {
			return "", "", err
		}
		if runtimeID != "" {
			return runtimeID, "pv " + volumeName + " hostname " + nodeName, nil
		}
	}
	return "", "", nil
}

func persistentVolumeNodeName(pv kubePersistentVolume) string {
	required := pv.Spec.NodeAffinity.Required
	if required == nil {
		return ""
	}
	for _, term := range required.NodeSelectorTerms {
		for _, expr := range term.MatchExpressions {
			if strings.TrimSpace(expr.Key) != kubeHostnameLabelKey {
				continue
			}
			if !strings.EqualFold(strings.TrimSpace(expr.Operator), "In") && strings.TrimSpace(expr.Operator) != "" {
				continue
			}
			for _, value := range expr.Values {
				if value = strings.TrimSpace(value); value != "" {
					return value
				}
			}
		}
	}
	return ""
}

func (s *Service) runtimeIDForNode(ctx context.Context, client *kubeClient, nodeName string) (string, error) {
	node, found, err := client.getNode(ctx, nodeName)
	if err != nil {
		return "", fmt.Errorf("read kubernetes node %s: %w", nodeName, err)
	}
	if !found {
		return "", nil
	}

	if runtimeID := strings.TrimSpace(node.Metadata.Labels[runtimepkg.RuntimeIDLabelKey]); runtimeID != "" {
		return runtimeID, nil
	}

	if !strings.EqualFold(strings.TrimSpace(node.Metadata.Labels[runtimepkg.SharedPoolLabelKey]), runtimepkg.SharedPoolLabelValue) {
		return "", nil
	}

	location := runtimepkg.PlacementNodeSelectorForLabels(node.Metadata.Labels)
	if len(location) > 0 {
		if err := s.Store.SyncManagedSharedLocationRuntimes([]map[string]string{location}); err != nil && s.Logger != nil {
			s.Logger.Printf("sync managed shared location runtimes for node %s failed: %v", nodeName, err)
		}
	}

	runtimes, err := s.Store.ListRuntimes("", true)
	if err != nil {
		return "", fmt.Errorf("list runtimes for node %s: %w", nodeName, err)
	}
	return bestMatchingManagedSharedRuntimeID(node.Metadata.Labels, runtimes), nil
}

func bestMatchingManagedSharedRuntimeID(nodeLabels map[string]string, runtimes []model.Runtime) string {
	bestRuntimeID := ""
	bestScore := -1
	for _, runtimeObj := range runtimes {
		if runtimeObj.Type != model.RuntimeTypeManagedShared {
			continue
		}
		if runtimeObj.Status != "" && runtimeObj.Status != model.RuntimeStatusActive {
			continue
		}
		selector := runtimepkg.ManagedSharedNodeSelector(runtimeObj)
		if !nodeLabelsMatchSelector(nodeLabels, selector) {
			continue
		}
		score := len(selector)
		if score > bestScore || (score == bestScore && (bestRuntimeID == "" || runtimeObj.ID < bestRuntimeID)) {
			bestScore = score
			bestRuntimeID = runtimeObj.ID
		}
	}
	return bestRuntimeID
}

func nodeLabelsMatchSelector(nodeLabels, selector map[string]string) bool {
	for key, value := range selector {
		if strings.TrimSpace(nodeLabels[key]) != strings.TrimSpace(value) {
			return false
		}
	}
	return true
}

func cloneControllerAppSpec(spec *model.AppSpec) *model.AppSpec {
	if spec == nil {
		return nil
	}
	out := *spec
	if len(spec.Command) > 0 {
		out.Command = append([]string(nil), spec.Command...)
	}
	if len(spec.Args) > 0 {
		out.Args = append([]string(nil), spec.Args...)
	}
	if len(spec.Ports) > 0 {
		out.Ports = append([]int(nil), spec.Ports...)
	}
	out.Env = cloneControllerStringMap(spec.Env)
	if len(spec.Files) > 0 {
		out.Files = append([]model.AppFile(nil), spec.Files...)
	}
	if spec.Workspace != nil {
		workspace := *spec.Workspace
		out.Workspace = &workspace
	}
	if spec.PersistentStorage != nil {
		storage := *spec.PersistentStorage
		if len(spec.PersistentStorage.Mounts) > 0 {
			storage.Mounts = append([]model.AppPersistentStorageMount(nil), spec.PersistentStorage.Mounts...)
		}
		out.PersistentStorage = &storage
	}
	if spec.Failover != nil {
		failover := *spec.Failover
		out.Failover = &failover
	}
	if spec.Resources != nil {
		resources := *spec.Resources
		out.Resources = &resources
	}
	out.Postgres = cloneControllerPostgresSpec(spec.Postgres)
	model.ApplyAppSpecDefaults(&out)
	return &out
}

func cloneControllerPostgresSpec(spec *model.AppPostgresSpec) *model.AppPostgresSpec {
	if spec == nil {
		return nil
	}
	out := *spec
	if spec.Resources != nil {
		resources := *spec.Resources
		out.Resources = &resources
	}
	return &out
}

func cloneControllerStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}
