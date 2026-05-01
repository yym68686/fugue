package controller

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

const (
	movableRWOCopyPort                  = 8730
	sharedWorkspaceNFSComponentSelector = "app.kubernetes.io/component=shared-workspace-nfs"
)

type movableRWOCopyPlan struct {
	sourceClaimName     string
	sourceMountSubPath  string
	sourceCopyPath      string
	sourceSharedProject bool
	targetClaimName     string
	targetCopyPath      string
	sourceNodeName      string
}

func (s *Service) prepareMovableRWOStorageForOperation(
	ctx context.Context,
	op model.Operation,
	currentApp model.App,
	desiredApp model.App,
	targetScheduling runtimepkg.SchedulingConstraints,
) (model.App, bool, error) {
	targetStorage := desiredApp.Spec.PersistentStorage
	if !model.AppPersistentStorageSpecUsesMovableRWO(targetStorage) {
		return desiredApp, false, nil
	}
	if currentApp.Spec.PersistentStorage == nil {
		return desiredApp, false, nil
	}

	plan, desiredApp, changed, err := s.buildMovableRWOCopyPlan(ctx, op, currentApp, desiredApp)
	if err != nil || plan == nil {
		return desiredApp, changed, err
	}
	if strings.TrimSpace(plan.sourceClaimName) == strings.TrimSpace(plan.targetClaimName) {
		return desiredApp, changed, nil
	}

	client, err := s.kubeClient()
	if err != nil {
		return desiredApp, changed, fmt.Errorf("initialize kubernetes movable RWO migration client: %w", err)
	}
	namespace := runtimepkg.NamespaceForTenant(desiredApp.TenantID)
	if _, found, err := client.getPersistentVolumeClaim(ctx, namespace, plan.sourceClaimName); err != nil {
		return desiredApp, changed, fmt.Errorf("read movable RWO source pvc %s/%s: %w", namespace, plan.sourceClaimName, err)
	} else if !found {
		return desiredApp, changed, nil
	}

	if currentApp.Spec.Replicas > 0 {
		if _, err := s.acquireAppFenceLease(ctx, client, currentApp, op.ID); err != nil {
			return desiredApp, changed, fmt.Errorf("acquire movable RWO app fence: %w", err)
		}
		fencedApp := currentApp
		fencedApp.Spec.Replicas = 0
		sourceScheduling, err := s.managedSchedulingConstraintsForApp(ctx, fencedApp)
		if err != nil {
			return desiredApp, changed, err
		}
		if err := s.applyManagedAppDesiredState(ctx, fencedApp, sourceScheduling); err != nil {
			return desiredApp, changed, fmt.Errorf("scale source app to zero before movable RWO copy: %w", err)
		}
		if err := s.waitForManagedAppRollout(ctx, fencedApp, op.ID); err != nil {
			return desiredApp, changed, fmt.Errorf("wait for source app scale-down before movable RWO copy: %w", err)
		}
	}

	if err := s.copyMovableRWOVolume(ctx, client, namespace, currentApp, desiredApp, targetScheduling, *plan); err != nil {
		return desiredApp, changed, err
	}
	return desiredApp, changed, nil
}

func (s *Service) buildMovableRWOCopyPlan(ctx context.Context, op model.Operation, currentApp model.App, desiredApp model.App) (*movableRWOCopyPlan, model.App, bool, error) {
	sourceStorage := currentApp.Spec.PersistentStorage
	targetStorage := desiredApp.Spec.PersistentStorage
	if sourceStorage == nil || targetStorage == nil {
		return nil, desiredApp, false, nil
	}

	changed := false
	if strings.TrimSpace(targetStorage.SharedSubPath) != "" {
		nextStorage := *targetStorage
		nextStorage.SharedSubPath = ""
		desiredApp.Spec.PersistentStorage = &nextStorage
		targetStorage = &nextStorage
		changed = true
	}

	sourceClaim := currentPersistentStorageClaimName(currentApp, *sourceStorage)
	targetClaim := desiredPersistentStorageClaimName(desiredApp, *targetStorage)

	if strings.TrimSpace(sourceClaim) == strings.TrimSpace(targetClaim) && movableRWONeedsFreshClaim(op, currentApp, desiredApp) {
		targetClaim = movableRWOTargetClaimName(desiredApp, op.ID)
		nextStorage := *targetStorage
		nextStorage.ClaimName = targetClaim
		desiredApp.Spec.PersistentStorage = &nextStorage
		changed = true
	}

	if strings.TrimSpace(sourceClaim) == "" || strings.TrimSpace(targetClaim) == "" {
		return nil, desiredApp, changed, nil
	}

	namespace := runtimepkg.NamespaceForTenant(currentApp.TenantID)
	sourceNode := ""
	if !model.AppPersistentStorageSpecUsesSharedProjectRWX(sourceStorage) {
		client, err := s.kubeClient()
		if err != nil {
			return nil, desiredApp, changed, fmt.Errorf("initialize kubernetes movable RWO source lookup client: %w", err)
		}
		nodeName, found, err := managedSharedPostgresPVCNode(ctx, client, namespace, sourceClaim, nil)
		if err != nil {
			return nil, desiredApp, changed, err
		}
		if found {
			sourceNode = nodeName
		}
	}

	sourceMountSubPath := ""
	sourceCopyPath := "."
	targetCopyPath := "."
	if model.AppPersistentStorageSpecUsesSharedProjectRWX(sourceStorage) {
		sourceMountSubPath = strings.TrimSpace(sourceStorage.SharedSubPath)
		if model.AppPersistentStorageSpecUsesDirectSharedProjectDirectoryMount(sourceStorage) && len(targetStorage.Mounts) == 1 {
			targetCopyPath = model.AppPersistentStorageMountSubPath(targetStorage.Mounts[0])
		}
	}

	return &movableRWOCopyPlan{
		sourceClaimName:     sourceClaim,
		sourceMountSubPath:  sourceMountSubPath,
		sourceCopyPath:      sourceCopyPath,
		sourceSharedProject: model.AppPersistentStorageSpecUsesSharedProjectRWX(sourceStorage),
		targetClaimName:     targetClaim,
		targetCopyPath:      targetCopyPath,
		sourceNodeName:      sourceNode,
	}, desiredApp, changed, nil
}

func currentPersistentStorageClaimName(app model.App, storage model.AppPersistentStorageSpec) string {
	if model.AppPersistentStorageSpecUsesSharedProjectRWX(&storage) {
		return runtimepkg.ProjectSharedWorkspacePVCName(app)
	}
	return desiredPersistentStorageClaimName(app, storage)
}

func desiredPersistentStorageClaimName(app model.App, storage model.AppPersistentStorageSpec) string {
	if model.AppPersistentStorageSpecUsesSharedProjectRWX(&storage) {
		return runtimepkg.ProjectSharedWorkspacePVCName(app)
	}
	if claimName := model.SlugifyOptional(strings.TrimSpace(storage.ClaimName)); claimName != "" {
		if len(claimName) > 63 {
			return claimName[:63]
		}
		return claimName
	}
	return runtimepkg.WorkspacePVCName(app)
}

func movableRWONeedsFreshClaim(op model.Operation, currentApp model.App, desiredApp model.App) bool {
	if op.Type == model.OperationTypeMigrate {
		sourceRuntimeID := strings.TrimSpace(op.SourceRuntimeID)
		if sourceRuntimeID == "" {
			sourceRuntimeID = strings.TrimSpace(currentApp.Spec.RuntimeID)
		}
		targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
		if targetRuntimeID == "" {
			targetRuntimeID = strings.TrimSpace(desiredApp.Spec.RuntimeID)
		}
		return sourceRuntimeID != "" && targetRuntimeID != "" && sourceRuntimeID != targetRuntimeID
	}
	currentStorage := currentApp.Spec.PersistentStorage
	targetStorage := desiredApp.Spec.PersistentStorage
	if currentStorage == nil || targetStorage == nil {
		return false
	}
	if model.AppPersistentStorageSpecUsesSharedProjectRWX(currentStorage) {
		return false
	}
	return strings.TrimSpace(targetStorage.StorageClassName) != "" &&
		strings.TrimSpace(currentStorage.StorageClassName) != strings.TrimSpace(targetStorage.StorageClassName)
}

func movableRWOTargetClaimName(app model.App, operationID string) string {
	base := model.Slugify(runtimepkg.WorkspacePVCName(app))
	if base == "" {
		base = "app-workspace"
	}
	suffix := "mv-" + shortMovableRWOSuffix(operationID)
	maxBase := 63 - len(suffix) - 1
	if maxBase < 1 {
		return suffix
	}
	if len(base) > maxBase {
		base = base[:maxBase]
	}
	return base + "-" + suffix
}

func shortMovableRWOSuffix(value string) string {
	value = model.Slugify(strings.TrimSpace(value))
	if value == "" {
		return "manual"
	}
	if len(value) > 12 {
		return value[len(value)-12:]
	}
	return value
}

func (s *Service) copyMovableRWOVolume(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	currentApp model.App,
	desiredApp model.App,
	targetScheduling runtimepkg.SchedulingConstraints,
	plan movableRWOCopyPlan,
) error {
	names := movableRWOMigrationResourceNames(desiredApp, plan.targetClaimName)
	for _, name := range []string{names.sourcePod, names.targetPod, names.copyPod} {
		if err := client.deletePod(ctx, namespace, name); err != nil {
			return err
		}
		if err := waitForMovableRWOPodDeleted(ctx, client, namespace, name); err != nil {
			return err
		}
	}
	if err := client.deleteService(ctx, namespace, names.service); err != nil {
		return err
	}

	targetPVC := buildMovableRWOTargetPVC(namespace, desiredApp, plan.targetClaimName)
	if err := client.applyObject(ctx, targetPVC, nil); err != nil {
		return fmt.Errorf("apply movable RWO target pvc %s/%s: %w", namespace, plan.targetClaimName, err)
	}
	if plan.sourceSharedProject {
		if sourceScheduling, found, err := sharedWorkspaceNFSServerScheduling(ctx, client); err != nil {
			return fmt.Errorf("resolve shared workspace NFS source node: %w", err)
		} else if found {
			return s.copyMovableRWOVolumeViaTransferPods(ctx, client, namespace, names, plan, sourceScheduling, targetScheduling)
		}
		if err := client.applyObject(ctx, buildMovableRWOCopyPod(namespace, names.copyPod, names.labels, plan, targetScheduling), nil); err != nil {
			return fmt.Errorf("apply movable RWO copy pod %s/%s: %w", namespace, names.copyPod, err)
		}
		if err := waitForMovableRWOPodSucceeded(ctx, client, namespace, names.copyPod, s.movableRWOWaitTimeout()); err != nil {
			return fmt.Errorf("wait for movable RWO copy pod %s/%s: %w", namespace, names.copyPod, err)
		}
		if err := client.deletePod(ctx, namespace, names.copyPod); err != nil {
			return err
		}
		return nil
	}
	sourceScheduling, err := s.managedSchedulingConstraintsForApp(ctx, currentApp)
	if err != nil {
		return err
	}
	if strings.TrimSpace(plan.sourceNodeName) != "" {
		if sourceScheduling.NodeSelector == nil {
			sourceScheduling.NodeSelector = map[string]string{}
		}
		sourceScheduling.NodeSelector[kubeHostnameLabelKey] = strings.TrimSpace(plan.sourceNodeName)
	}
	return s.copyMovableRWOVolumeViaTransferPods(ctx, client, namespace, names, plan, sourceScheduling, targetScheduling)
}

func (s *Service) copyMovableRWOVolumeViaTransferPods(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	names movableRWOMigrationNames,
	plan movableRWOCopyPlan,
	sourceScheduling runtimepkg.SchedulingConstraints,
	targetScheduling runtimepkg.SchedulingConstraints,
) error {
	if err := client.applyObject(ctx, buildMovableRWOTargetService(namespace, names.service, names.labels), nil); err != nil {
		return fmt.Errorf("apply movable RWO transfer service %s/%s: %w", namespace, names.service, err)
	}
	if err := client.applyObject(ctx, buildMovableRWOTargetPod(namespace, names.targetPod, names.labels, plan.targetClaimName, plan.targetCopyPath, targetScheduling), nil); err != nil {
		return fmt.Errorf("apply movable RWO target pod %s/%s: %w", namespace, names.targetPod, err)
	}
	if err := waitForMovableRWOPodReady(ctx, client, namespace, names.targetPod, s.movableRWOWaitTimeout()); err != nil {
		return fmt.Errorf("wait for movable RWO target pod %s/%s: %w", namespace, names.targetPod, err)
	}

	if err := client.applyObject(ctx, buildMovableRWOSourcePod(namespace, names.sourcePod, names.labels, plan, names.service, sourceScheduling), nil); err != nil {
		return fmt.Errorf("apply movable RWO source pod %s/%s: %w", namespace, names.sourcePod, err)
	}
	if err := waitForMovableRWOPodSucceeded(ctx, client, namespace, names.sourcePod, s.movableRWOWaitTimeout()); err != nil {
		return fmt.Errorf("wait for movable RWO source pod %s/%s: %w", namespace, names.sourcePod, err)
	}
	if err := waitForMovableRWOPodSucceeded(ctx, client, namespace, names.targetPod, s.movableRWOWaitTimeout()); err != nil {
		return fmt.Errorf("wait for movable RWO target pod %s/%s: %w", namespace, names.targetPod, err)
	}

	_ = client.deletePod(context.Background(), namespace, names.sourcePod)
	_ = client.deletePod(context.Background(), namespace, names.targetPod)
	_ = client.deleteService(context.Background(), namespace, names.service)
	return nil
}

func sharedWorkspaceNFSServerScheduling(ctx context.Context, client *kubeClient) (runtimepkg.SchedulingConstraints, bool, error) {
	pods, err := client.listPodsBySelector(ctx, "", sharedWorkspaceNFSComponentSelector)
	if err != nil {
		return runtimepkg.SchedulingConstraints{}, false, err
	}
	for _, pod := range pods {
		if !strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "Running") || !podConditionTrue(pod, "Ready") {
			continue
		}
		if scheduling, ok := schedulingForPodNode(pod); ok {
			return scheduling, true, nil
		}
	}
	for _, pod := range pods {
		if strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "Failed") || strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "Succeeded") {
			continue
		}
		if scheduling, ok := schedulingForPodNode(pod); ok {
			return scheduling, true, nil
		}
	}
	return runtimepkg.SchedulingConstraints{}, false, nil
}

func schedulingForPodNode(pod kubePod) (runtimepkg.SchedulingConstraints, bool) {
	nodeName := strings.TrimSpace(pod.Spec.NodeName)
	if nodeName == "" {
		return runtimepkg.SchedulingConstraints{}, false
	}
	scheduling := runtimepkg.SchedulingConstraints{
		NodeSelector: map[string]string{kubeHostnameLabelKey: nodeName},
	}
	if len(pod.Spec.Tolerations) > 0 {
		scheduling.Tolerations = append([]runtimepkg.Toleration(nil), pod.Spec.Tolerations...)
	}
	return scheduling, true
}

func (s *Service) movableRWOWaitTimeout() time.Duration {
	timeout := s.Config.ManagedAppRolloutTimeout
	if timeout < 2*time.Hour {
		timeout = 2 * time.Hour
	}
	return timeout
}

type movableRWOMigrationNames struct {
	sourcePod string
	targetPod string
	copyPod   string
	service   string
	labels    map[string]string
}

func movableRWOMigrationResourceNames(app model.App, targetClaim string) movableRWOMigrationNames {
	base := model.Slugify(strings.TrimSpace(targetClaim))
	if base == "" {
		base = runtimepkg.WorkspacePVCName(app)
	}
	suffix := shortMovableRWOSuffix(targetClaim)
	name := func(role string) string {
		tail := "rwo-" + role + "-" + suffix
		maxBase := 63 - len(tail) - 1
		if maxBase < 1 {
			return tail
		}
		trimmed := base
		if len(trimmed) > maxBase {
			trimmed = trimmed[:maxBase]
		}
		return trimmed + "-" + tail
	}
	labels := map[string]string{
		runtimepkg.FugueLabelManagedBy: "fugue",
		"fugue.pro/volume-migration":   suffix,
	}
	return movableRWOMigrationNames{
		sourcePod: name("src"),
		targetPod: name("dst"),
		copyPod:   name("copy"),
		service:   name("svc"),
		labels:    labels,
	}
}

func buildMovableRWOTargetPVC(namespace string, app model.App, claimName string) map[string]any {
	storage := app.Spec.PersistentStorage
	storageSize := "10Gi"
	storageClass := ""
	if storage != nil {
		if strings.TrimSpace(storage.StorageSize) != "" {
			storageSize = strings.TrimSpace(storage.StorageSize)
		}
		storageClass = strings.TrimSpace(storage.StorageClassName)
	}
	labels := movableRWOAppLabels(app)
	spec := map[string]any{
		"accessModes": []string{"ReadWriteOnce"},
		"resources": map[string]any{
			"requests": map[string]any{
				"storage": storageSize,
			},
		},
	}
	if storageClass != "" {
		spec["storageClassName"] = storageClass
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata": map[string]any{
			"name":      claimName,
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": spec,
	}
}

func movableRWOAppLabels(app model.App) map[string]string {
	labels := map[string]string{
		runtimepkg.FugueLabelName:      model.Slugify(strings.TrimSpace(app.Name)),
		runtimepkg.FugueLabelManagedBy: runtimepkg.FugueLabelManagedByValue,
	}
	if strings.TrimSpace(app.ID) != "" {
		labels[runtimepkg.FugueLabelAppID] = strings.TrimSpace(app.ID)
		labels[runtimepkg.FugueLabelOwnerAppID] = strings.TrimSpace(app.ID)
	}
	if strings.TrimSpace(app.TenantID) != "" {
		labels[runtimepkg.FugueLabelTenantID] = strings.TrimSpace(app.TenantID)
	}
	if strings.TrimSpace(app.ProjectID) != "" {
		labels[runtimepkg.FugueLabelProjectID] = strings.TrimSpace(app.ProjectID)
	}
	return labels
}

func buildMovableRWOTargetService(namespace, name string, labels map[string]string) map[string]any {
	selector := clonePlacementStringMap(labels)
	selector["fugue.pro/volume-migration-role"] = "target"
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"selector": selector,
			"ports": []map[string]any{
				{
					"name":       "copy",
					"port":       movableRWOCopyPort,
					"targetPort": movableRWOCopyPort,
					"protocol":   "TCP",
				},
			},
		},
	}
}

func buildMovableRWOTargetPod(namespace, name string, labels map[string]string, claimName, targetPath string, scheduling runtimepkg.SchedulingConstraints) map[string]any {
	podLabels := clonePlacementStringMap(labels)
	podLabels["fugue.pro/volume-migration-role"] = "target"
	podSpec := map[string]any{
		"restartPolicy": "Never",
		"containers": []map[string]any{
			{
				"name":  "receiver",
				"image": "busybox:1.36",
				"command": []string{
					"sh",
					"-lc",
					`set -eu
target="$1"
mkdir -p "$target"
nc -l -p 8730 | tar -xpf - -C "$target"`,
					"sh",
					path.Join("/dst", cleanRelativeCopyPath(targetPath)),
				},
				"ports": []map[string]any{
					{"containerPort": movableRWOCopyPort, "protocol": "TCP"},
				},
				"volumeMounts": []map[string]any{
					{"name": "data", "mountPath": "/dst"},
				},
			},
		},
		"volumes": []map[string]any{
			{
				"name": "data",
				"persistentVolumeClaim": map[string]any{
					"claimName": claimName,
				},
			},
		},
	}
	applyMovableRWOScheduling(podSpec, scheduling)
	return movableRWOPodObject(namespace, name, podLabels, podSpec)
}

func buildMovableRWOCopyPod(namespace, name string, labels map[string]string, plan movableRWOCopyPlan, scheduling runtimepkg.SchedulingConstraints) map[string]any {
	podLabels := clonePlacementStringMap(labels)
	podLabels["fugue.pro/volume-migration-role"] = "copy"
	sourceMount := map[string]any{
		"name":      "source",
		"mountPath": "/src",
		"readOnly":  true,
	}
	if strings.TrimSpace(plan.sourceMountSubPath) != "" {
		sourceMount["subPath"] = strings.TrimSpace(plan.sourceMountSubPath)
	}
	podSpec := map[string]any{
		"restartPolicy": "Never",
		"containers": []map[string]any{
			{
				"name":  "copy",
				"image": "busybox:1.36",
				"command": []string{
					"sh",
					"-lc",
					`set -eu
source="$1"
target="$2"
if [ ! -d "$source" ]; then
  mkdir -p /tmp/fugue-empty
  source=/tmp/fugue-empty
fi
mkdir -p "$target"
tar -cpf - -C "$source" . | tar -xpf - -C "$target"`,
					"sh",
					path.Join("/src", cleanRelativeCopyPath(plan.sourceCopyPath)),
					path.Join("/dst", cleanRelativeCopyPath(plan.targetCopyPath)),
				},
				"volumeMounts": []map[string]any{
					sourceMount,
					{"name": "target", "mountPath": "/dst"},
				},
			},
		},
		"volumes": []map[string]any{
			{
				"name": "source",
				"persistentVolumeClaim": map[string]any{
					"claimName": plan.sourceClaimName,
					"readOnly":  true,
				},
			},
			{
				"name": "target",
				"persistentVolumeClaim": map[string]any{
					"claimName": plan.targetClaimName,
				},
			},
		},
	}
	applyMovableRWOScheduling(podSpec, scheduling)
	return movableRWOPodObject(namespace, name, podLabels, podSpec)
}

func buildMovableRWOSourcePod(namespace, name string, labels map[string]string, plan movableRWOCopyPlan, targetServiceName string, scheduling runtimepkg.SchedulingConstraints) map[string]any {
	podLabels := clonePlacementStringMap(labels)
	podLabels["fugue.pro/volume-migration-role"] = "source"
	volumeMount := map[string]any{
		"name":      "data",
		"mountPath": "/src",
		"readOnly":  true,
	}
	if strings.TrimSpace(plan.sourceMountSubPath) != "" {
		volumeMount["subPath"] = strings.TrimSpace(plan.sourceMountSubPath)
	}
	podSpec := map[string]any{
		"restartPolicy": "Never",
		"containers": []map[string]any{
			{
				"name":  "sender",
				"image": "busybox:1.36",
				"command": []string{
					"sh",
					"-lc",
					`set -eu
source="$1"
target="$2"
if [ ! -d "$source" ]; then
  mkdir -p /tmp/fugue-empty
  source=/tmp/fugue-empty
fi
tar -cpf - -C "$source" . | nc "$target" 8730`,
					"sh",
					path.Join("/src", cleanRelativeCopyPath(plan.sourceCopyPath)),
					targetServiceName,
				},
				"volumeMounts": []map[string]any{volumeMount},
			},
		},
		"volumes": []map[string]any{
			{
				"name": "data",
				"persistentVolumeClaim": map[string]any{
					"claimName": plan.sourceClaimName,
					"readOnly":  true,
				},
			},
		},
	}
	applyMovableRWOScheduling(podSpec, scheduling)
	return movableRWOPodObject(namespace, name, podLabels, podSpec)
}

func movableRWOPodObject(namespace, name string, labels map[string]string, podSpec map[string]any) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": podSpec,
	}
}

func applyMovableRWOScheduling(podSpec map[string]any, scheduling runtimepkg.SchedulingConstraints) {
	if len(scheduling.NodeSelector) > 0 {
		podSpec["nodeSelector"] = clonePlacementStringMap(scheduling.NodeSelector)
	}
	if len(scheduling.Tolerations) > 0 {
		tolerations := make([]map[string]any, 0, len(scheduling.Tolerations))
		for _, toleration := range scheduling.Tolerations {
			item := map[string]any{}
			if strings.TrimSpace(toleration.Key) != "" {
				item["key"] = strings.TrimSpace(toleration.Key)
			}
			if strings.TrimSpace(toleration.Operator) != "" {
				item["operator"] = strings.TrimSpace(toleration.Operator)
			}
			if strings.TrimSpace(toleration.Value) != "" {
				item["value"] = strings.TrimSpace(toleration.Value)
			}
			if strings.TrimSpace(toleration.Effect) != "" {
				item["effect"] = strings.TrimSpace(toleration.Effect)
			}
			tolerations = append(tolerations, item)
		}
		podSpec["tolerations"] = tolerations
	}
}

func waitForMovableRWOPodDeleted(ctx context.Context, client *kubeClient, namespace, name string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		_, found, err := client.getPod(waitCtx, namespace, name)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func waitForMovableRWOPodReady(ctx context.Context, client *kubeClient, namespace, name string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		pod, found, err := client.getPod(waitCtx, namespace, name)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("pod not found")
		}
		if pod.Status.Phase == "Failed" || pod.Status.Phase == "Succeeded" {
			return fmt.Errorf("pod reached phase %s before becoming ready: %s", pod.Status.Phase, strings.TrimSpace(pod.Status.Message))
		}
		if podConditionTrue(pod, "Ready") {
			time.Sleep(500 * time.Millisecond)
			return nil
		}
		select {
		case <-waitCtx.Done():
			return waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func waitForMovableRWOPodSucceeded(ctx context.Context, client *kubeClient, namespace, name string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		pod, found, err := client.getPod(waitCtx, namespace, name)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("pod not found")
		}
		switch pod.Status.Phase {
		case "Succeeded":
			return nil
		case "Failed":
			message := strings.TrimSpace(pod.Status.Message)
			if message == "" {
				message = strings.TrimSpace(pod.Status.Reason)
			}
			return fmt.Errorf("pod failed: %s", message)
		}
		select {
		case <-waitCtx.Done():
			return waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func podConditionTrue(pod kubePod, conditionType string) bool {
	for _, condition := range pod.Status.Conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), conditionType) &&
			strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
			return true
		}
	}
	return false
}

func cleanRelativeCopyPath(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
	if value == "" || value == "." {
		return "."
	}
	value = strings.TrimPrefix(value, "/")
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "."
	}
	return cleaned
}
