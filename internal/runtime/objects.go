package runtime

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"path"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/model"
)

const (
	defaultPostgresImage          = ""
	defaultPostgresStorage        = "1Gi"
	defaultPostgresInstances      = 1
	defaultWorkspaceStorage       = "10Gi"
	defaultWaitImage              = "busybox:1.36"
	defaultHelperCPURequest       = "25m"
	defaultHelperCPULimit         = "100m"
	defaultHelperMemoryRequest    = "32Mi"
	defaultHelperMemoryLimit      = "128Mi"
	defaultHelperEphemeralRequest = "32Mi"
	defaultHelperEphemeralLimit   = "128Mi"
	AppFilesVolumeName            = "app-files"
	appFilesVolumeName            = AppFilesVolumeName
	appFilesSourceMountPath       = "/fugue-app-files"
	AppWorkspaceContainerName     = "fugue-workspace"
	workspaceVolumeName           = "app-workspace"
	workspaceSidecarName          = AppWorkspaceContainerName
	persistentStorageRootPath     = "/fugue-persistent-storage"
	projectSharedStorageComponent = "project-shared-persistent-storage"

	CloudNativePGAPIVersion           = "postgresql.cnpg.io/v1"
	CloudNativePGClusterKind          = "Cluster"
	CloudNativePGReconcilePodSpecAnno = "cnpg.io/reconcilePodSpec"
	CloudNativePGReconcilePodSpecHold = "disabled"
	KubernetesNetworkPolicyAPIVersion = "networking.k8s.io/v1"
	VolSyncAPIVersion                 = "volsync.backube/v1alpha1"
	VolSyncReplicationSourceKind      = "ReplicationSource"
	VolSyncReplicationDestinationKind = "ReplicationDestination"
)

func buildAppObjects(app model.App, scheduling SchedulingConstraints) []map[string]any {
	return buildAppObjectsWithPlacements(app, scheduling, nil)
}

func buildAppObjectsWithPlacements(app model.App, scheduling SchedulingConstraints, postgresPlacements map[string][]SchedulingConstraints) []map[string]any {
	return buildAppObjectsWithOwner(app, scheduling, postgresPlacements, nil)
}

func buildAppObjectsWithOwner(app model.App, scheduling SchedulingConstraints, postgresPlacements map[string][]SchedulingConstraints, ownerRef *OwnerReference) []map[string]any {
	namespace := NamespaceForTenant(app.TenantID)
	appRuntimeName := RuntimeAppResourceName(app)
	postgresResources := managedPostgresResources(namespace, app, postgresPlacements)
	labels := appLabels(app)
	objects := []map[string]any{
		buildNamespaceObject(namespace),
	}

	if len(app.Spec.Files) > 0 {
		objects = append(objects, buildAppFilesSecretObject(namespace, appRuntimeName, app.Spec.Files, labels))
	}

	if workspaceSpec := normalizeRuntimeAppWorkspaceSpec(app); workspaceSpec != nil {
		objects = append(objects,
			buildAppWorkspacePVCObject(namespace, app, labels, *workspaceSpec),
		)
		if AppVolumeReplicationEnabled(app) {
			objects = append(objects, buildWorkspaceReplicationDestinationObject(namespace, app, labels, *workspaceSpec))
		}
	} else if storageSpec := normalizeRuntimeAppPersistentStorageSpec(app); storageSpec != nil {
		if model.AppPersistentStorageSpecUsesSharedProjectRWX(storageSpec) {
			objects = append(objects, buildProjectSharedPersistentStoragePVCObject(namespace, app, *storageSpec))
		} else {
			objects = append(objects, buildAppPersistentStoragePVCObject(namespace, app, labels, *storageSpec))
			if AppVolumeReplicationEnabled(app) {
				objects = append(objects, buildPersistentStorageReplicationDestinationObject(namespace, app, labels, *storageSpec))
			}
		}
	}

	for _, postgres := range postgresResources {
		objects = append(objects, buildManagedPostgresObjects(namespace, postgres)...)
	}

	objects = append(objects, buildAppDeploymentObject(namespace, app, labels, scheduling, postgresResources))
	if serviceObject := buildAppServiceObject(namespace, app, labels); serviceObject != nil {
		objects = append(objects, serviceObject)
	}
	if aliasObject := buildComposeServiceAliasObject(namespace, app); aliasObject != nil {
		objects = append(objects, aliasObject)
	}
	if aliasObject := buildLegacyComposeAppNameAliasObject(namespace, app); aliasObject != nil {
		objects = append(objects, aliasObject)
	}
	if networkPolicyObject := buildAppNetworkPolicyObject(namespace, app, labels); networkPolicyObject != nil {
		objects = append(objects, networkPolicyObject)
	}
	attachOwnerReference(objects, ownerRef)
	return objects
}

func buildNamespaceObject(namespace string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]any{
			"name": namespace,
		},
	}
}

func appLabels(app model.App) map[string]string {
	labels := map[string]string{
		FugueLabelName:      sanitizeName(app.Name),
		FugueLabelManagedBy: FugueLabelManagedByValue,
	}
	if id := strings.TrimSpace(app.ID); id != "" {
		labels[FugueLabelAppID] = id
		labels[FugueLabelOwnerAppID] = id
	}
	if tenantID := strings.TrimSpace(app.TenantID); tenantID != "" {
		labels[FugueLabelTenantID] = tenantID
	}
	if projectID := strings.TrimSpace(app.ProjectID); projectID != "" {
		labels[FugueLabelProjectID] = projectID
	}
	return labels
}

func projectSharedPersistentStorageLabels(app model.App) map[string]string {
	labels := map[string]string{
		FugueLabelName:      ProjectSharedWorkspacePVCName(app),
		FugueLabelComponent: projectSharedStorageComponent,
		FugueLabelManagedBy: FugueLabelManagedByValue,
	}
	if tenantID := strings.TrimSpace(app.TenantID); tenantID != "" {
		labels[FugueLabelTenantID] = tenantID
	}
	if projectID := strings.TrimSpace(app.ProjectID); projectID != "" {
		labels[FugueLabelProjectID] = projectID
	}
	return labels
}

func postgresLabels(resource postgresRuntimeResource) map[string]string {
	labels := map[string]string{
		FugueLabelName:      resource.resourceName,
		FugueLabelComponent: "postgres",
		FugueLabelManagedBy: FugueLabelManagedByValue,
	}
	if tenantID := strings.TrimSpace(resource.tenantID); tenantID != "" {
		labels[FugueLabelTenantID] = tenantID
	}
	if projectID := strings.TrimSpace(resource.projectID); projectID != "" {
		labels[FugueLabelProjectID] = projectID
	}
	if serviceID := strings.TrimSpace(resource.serviceID); serviceID != "" {
		labels[FugueLabelBackingServiceID] = serviceID
	}
	if serviceType := strings.TrimSpace(resource.serviceType); serviceType != "" {
		labels[FugueLabelBackingServiceType] = serviceType
	}
	if ownerAppID := strings.TrimSpace(resource.ownerAppID); ownerAppID != "" {
		labels[FugueLabelOwnerAppID] = ownerAppID
	}
	return labels
}

func labelSubset(labels map[string]string, keys ...string) map[string]string {
	subset := make(map[string]string, len(keys))
	for _, key := range keys {
		if value := strings.TrimSpace(labels[key]); value != "" {
			subset[key] = value
		}
	}
	return subset
}

func buildAppFilesSecretObject(namespace, appName string, files []model.AppFile, labels map[string]string) map[string]any {
	stringData := make(map[string]string, len(files))
	for index, file := range files {
		stringData[fileKey(index)] = file.Content
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":      appFilesSecretName(appName),
			"namespace": namespace,
			"labels":    labels,
		},
		"type":       "Opaque",
		"stringData": stringData,
	}
}

func buildPostgresSecretObject(namespace, secretName string, labels map[string]string, spec model.AppPostgresSpec) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":      secretName,
			"namespace": namespace,
			"labels":    labels,
		},
		"type": "Opaque",
		"stringData": map[string]string{
			"POSTGRES_DB":       spec.Database,
			"POSTGRES_USER":     spec.User,
			"POSTGRES_PASSWORD": spec.Password,
			"username":          spec.User,
			"password":          spec.Password,
			"database":          spec.Database,
		},
	}
}

func buildPostgresClusterObject(namespace, secretName, resourceName string, labels map[string]string, spec model.AppPostgresSpec, placements []SchedulingConstraints) map[string]any {
	clusterSpec := map[string]any{
		"instances":             spec.Instances,
		"enableSuperuserAccess": false,
		"bootstrap": map[string]any{
			"initdb": map[string]any{
				"database": spec.Database,
				"owner":    spec.User,
				"secret": map[string]any{
					"name": secretName,
				},
			},
		},
		"storage": map[string]any{
			"size": spec.StorageSize,
		},
	}
	if strings.TrimSpace(spec.StorageClassName) != "" {
		clusterSpec["storage"].(map[string]any)["storageClass"] = strings.TrimSpace(spec.StorageClassName)
	}
	if strings.TrimSpace(spec.Image) != "" {
		clusterSpec["imageName"] = strings.TrimSpace(spec.Image)
	}
	if resources := runtimeResourceRequirements(spec.Resources); resources != nil {
		clusterSpec["resources"] = resources
	}
	clusterSpec["minSyncReplicas"] = spec.SynchronousReplicas
	clusterSpec["maxSyncReplicas"] = spec.SynchronousReplicas
	if affinity := buildPostgresAffinity(spec, placements); len(affinity) > 0 {
		clusterSpec["affinity"] = affinity
	}

	metadata := map[string]any{
		"name":      resourceName,
		"namespace": namespace,
		"labels":    labels,
	}
	if annotations := buildPostgresClusterAnnotations(spec); len(annotations) > 0 {
		metadata["annotations"] = annotations
	}

	return map[string]any{
		"apiVersion": CloudNativePGAPIVersion,
		"kind":       CloudNativePGClusterKind,
		"metadata":   metadata,
		"spec":       clusterSpec,
	}
}

func buildPostgresClusterAnnotations(spec model.AppPostgresSpec) map[string]string {
	if !spec.PrimaryPlacementPendingRebalance {
		return nil
	}
	// Hold pod-spec reconciliation during two-phase failover changes so the
	// current primary is not restarted just because placement changes.
	return map[string]string{
		CloudNativePGReconcilePodSpecAnno: CloudNativePGReconcilePodSpecHold,
	}
}

func buildPostgresServiceObject(namespace, resourceName string, labels map[string]string, spec model.AppPostgresSpec) map[string]any {
	clusterName := strings.TrimSpace(spec.ServiceName)
	if clusterName == "" {
		clusterName = strings.TrimSpace(resourceName)
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      resourceName,
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"selector": map[string]string{
				"cnpg.io/cluster":      clusterName,
				"cnpg.io/instanceRole": "primary",
			},
			"ports": []map[string]any{
				{
					"name":       "tcp-5432",
					"port":       5432,
					"targetPort": 5432,
					"protocol":   "TCP",
				},
			},
		},
	}
}

func buildManagedPostgresObjects(namespace string, resource postgresRuntimeResource) []map[string]any {
	labels := postgresLabels(resource)
	return []map[string]any{
		buildPostgresSecretObject(namespace, resource.secretName, labels, resource.spec),
		buildPostgresServiceObject(namespace, resource.resourceName, labels, resource.spec),
		buildPostgresClusterObject(namespace, resource.secretName, resource.resourceName, labels, resource.spec, resource.placements),
	}
}

func buildAppDeploymentObject(namespace string, app model.App, labels map[string]string, scheduling SchedulingConstraints, postgresResources []postgresRuntimeResource) map[string]any {
	resourceName := RuntimeAppResourceName(app)
	container := map[string]any{
		"name":  sanitizeName(app.Name),
		"image": app.Spec.Image,
	}
	if pullPolicy := imagePullPolicyForImage(app.Spec.Image); pullPolicy != "" {
		container["imagePullPolicy"] = pullPolicy
	}
	if resources := runtimeAppResourceRequirements(app.Spec); resources != nil {
		container["resources"] = resources
	}
	if len(app.Spec.Command) > 0 {
		container["command"] = app.Spec.Command
	}
	if len(app.Spec.Args) > 0 {
		container["args"] = app.Spec.Args
	}
	if len(app.Spec.Ports) > 0 {
		ports := make([]map[string]any, 0, len(app.Spec.Ports))
		for _, port := range app.Spec.Ports {
			ports = append(ports, map[string]any{
				"containerPort": port,
				"protocol":      "TCP",
			})
		}
		container["ports"] = ports
		container["readinessProbe"] = buildAppTCPReadinessProbe(app.Spec.Ports[0])
	}
	if env := mergedRuntimeEnv(app); len(env) > 0 {
		container["env"] = buildEnvObjects(env)
	}

	volumeMounts := []map[string]any{}
	volumes := []map[string]any{}
	sidecars := []map[string]any{}
	initContainers := []map[string]any{}
	if len(app.Spec.Files) > 0 {
		items := make([]map[string]any, 0, len(app.Spec.Files))
		for index, file := range app.Spec.Files {
			key := fileKey(index)
			mode := appFileMode(file)
			items = append(items, map[string]any{
				"key":  key,
				"path": key,
				"mode": mode,
			})
		}
		volumes = append(volumes, map[string]any{
			"name": appFilesVolumeName,
			"secret": map[string]any{
				"secretName": appFilesSecretName(resourceName),
				"items":      items,
			},
		})
		volumeMounts = append(volumeMounts, buildAppFileVolumeMounts(app.Spec.Files)...)
	}
	if workspaceSpec := normalizeRuntimeAppWorkspaceSpec(app); workspaceSpec != nil {
		volumeMounts = append(volumeMounts, map[string]any{
			"name":      workspaceVolumeName,
			"mountPath": workspaceSpec.MountPath,
		})
		volumes = append(volumes, map[string]any{
			"name": workspaceVolumeName,
			"persistentVolumeClaim": map[string]any{
				"claimName": WorkspacePVCName(app),
			},
		})
		initContainers = append(initContainers, buildAppWorkspaceInitContainer(*workspaceSpec))
		sidecars = append(sidecars, buildAppWorkspaceSidecar(*workspaceSpec))
	} else if storageSpec := normalizeRuntimeAppPersistentStorageSpec(app); storageSpec != nil {
		volumeMounts = append(volumeMounts, buildPersistentStorageVolumeMounts(*storageSpec)...)
		volumes = append(volumes, map[string]any{
			"name": workspaceVolumeName,
			"persistentVolumeClaim": map[string]any{
				"claimName": persistentStoragePVCName(app, *storageSpec),
			},
		})
		if !model.AppPersistentStorageSpecUsesDirectSharedProjectDirectoryMount(storageSpec) {
			initContainers = append(initContainers, buildAppPersistentStorageInitContainer(*storageSpec))
			sidecars = append(sidecars, buildAppPersistentStorageSidecar(*storageSpec))
		}
	}
	container["volumeMounts"] = volumeMounts

	podSpec := map[string]any{
		"containers": []map[string]any{container},
		"volumes":    volumes,
	}
	if len(sidecars) > 0 {
		podSpec["containers"] = append(podSpec["containers"].([]map[string]any), sidecars...)
	}

	if len(postgresResources) > 0 {
		postgresInitContainers := make([]map[string]any, 0, len(postgresResources))
		for index, postgres := range postgresResources {
			name := "wait-postgres"
			if index > 0 {
				name = "wait-postgres-" + strconv.Itoa(index+1)
			}
			postgresInitContainers = append(postgresInitContainers, map[string]any{
				"name":  name,
				"image": defaultWaitImage,
				"command": []string{
					"sh",
					"-c",
					"until nc -z " + model.PostgresRWServiceName(postgres.spec.ServiceName) + " 5432; do sleep 1; done",
				},
				"resources": runtimeHelperResourceRequirements(),
			})
		}
		initContainers = append(initContainers, postgresInitContainers...)
	}
	if len(initContainers) > 0 {
		podSpec["initContainers"] = initContainers
	}
	applyScheduling(&podSpec, scheduling)

	templateMetadata := map[string]any{
		"labels": labels,
	}
	if annotations := buildAppTemplateAnnotations(app.Spec); len(annotations) > 0 {
		templateMetadata["annotations"] = annotations
	}

	object := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      resourceName,
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"replicas": app.Spec.Replicas,
			"strategy": deploymentStrategy(app),
			"selector": map[string]any{
				"matchLabels": labels,
			},
			"template": map[string]any{
				"metadata": templateMetadata,
				"spec":     podSpec,
			},
		},
	}
	annotateManagedDeploymentReleaseKey(object)
	return object
}

func buildAppFileVolumeMounts(files []model.AppFile) []map[string]any {
	mounts := make([]map[string]any, 0, len(files))
	for index, file := range files {
		target := strings.TrimSpace(file.Path)
		if target == "" {
			continue
		}
		mounts = append(mounts, map[string]any{
			"name":      appFilesVolumeName,
			"mountPath": target,
			"subPath":   fileKey(index),
			"readOnly":  true,
		})
	}
	return mounts
}

func buildAppFilesInitContainer(files []model.AppFile) map[string]any {
	return map[string]any{
		"name":  "init-app-files",
		"image": defaultWaitImage,
		"command": []string{
			"sh",
			"-lc",
			appFilesInitScript(),
			"sh",
			appFilesSourceMountPath,
			buildAppFilesPlan(files),
		},
		"securityContext": map[string]any{
			"runAsUser": 0,
		},
		"resources": runtimeHelperResourceRequirements(),
		"volumeMounts": []map[string]any{
			{
				"name":      appFilesVolumeName,
				"mountPath": appFilesSourceMountPath,
				"readOnly":  true,
			},
		},
	}
}

func appFilesInitScript() string {
	return `set -eu

src_root="$1"
plan="$2"
plan_file="$(mktemp)"
trap 'rm -f "${plan_file}"' EXIT
printf '%s\n' "${plan}" > "${plan_file}"
tab="$(printf '\t')"

while IFS="${tab}" read -r key mode target; do
  if [ -z "${key}" ]; then
    continue
  fi
  src="${src_root}/${key}"
  if [ ! -f "${src}" ]; then
    echo "missing app file payload: ${key}" >&2
    exit 1
  fi
  parent="$(dirname "${target}")"
  mkdir -p "${parent}"
  if [ -d "${target}" ]; then
    echo "app file target is a directory: ${target}" >&2
    exit 1
  fi
  rm -f "${target}"
  cat "${src}" > "${target}"
  chmod "${mode}" "${target}"
done < "${plan_file}"`
}

func buildAppFilesPlan(files []model.AppFile) string {
	lines := make([]string, 0, len(files))
	for index, file := range files {
		lines = append(lines, strings.Join([]string{
			fileKey(index),
			strconv.FormatInt(int64(appFileMode(file)), 8),
			strings.TrimSpace(file.Path),
		}, "\t"))
	}
	return strings.Join(lines, "\n")
}

func appFileMode(file model.AppFile) int32 {
	if file.Mode > 0 {
		return file.Mode
	}
	if file.Secret {
		return 0o600
	}
	return 0o644
}

func buildAppTCPReadinessProbe(port int) map[string]any {
	return map[string]any{
		"tcpSocket": map[string]any{
			"port": port,
		},
		"initialDelaySeconds": 0,
		"periodSeconds":       1,
		"timeoutSeconds":      1,
		"failureThreshold":    30,
	}
}

func imagePullPolicyForImage(image string) string {
	image = strings.TrimSpace(image)
	if image == "" {
		return ""
	}
	if strings.Contains(image, "@") || isFugueManagedImmutableTag(image) {
		return "IfNotPresent"
	}
	return "Always"
}

func isFugueManagedImmutableTag(image string) bool {
	image = strings.TrimSpace(image)
	if image == "" || !strings.Contains(image, "/fugue-apps/") {
		return false
	}
	slash := strings.LastIndex(image, "/")
	colon := strings.LastIndex(image, ":")
	if colon <= slash {
		return false
	}
	tag := strings.TrimSpace(image[colon+1:])
	if tag == "" {
		return false
	}
	for _, prefix := range []string{"git-", "upload-", "image-"} {
		if strings.HasPrefix(tag, prefix) && len(tag) > len(prefix) {
			return true
		}
	}
	return false
}

func runtimeAppResourceRequirements(spec model.AppSpec) map[string]any {
	return runtimeResourceRequirementsForClass(spec.Resources, model.EffectiveWorkloadClass(spec))
}

func runtimeResourceRequirements(spec *model.ResourceSpec) map[string]any {
	return runtimeStaticResourceRequirementsFromSpec(spec, true)
}

func runtimeResourceRequirementsForClass(spec *model.ResourceSpec, workloadClass string) map[string]any {
	if spec == nil {
		return nil
	}

	requests := map[string]string{}
	limits := map[string]string{}

	if spec.CPUMilliCores > 0 {
		cpu := strconv.FormatInt(spec.CPUMilliCores, 10) + "m"
		requests["cpu"] = cpu
		if spec.CPULimitMilliCores > 0 {
			limits["cpu"] = strconv.FormatInt(spec.CPULimitMilliCores, 10) + "m"
		} else if model.EffectiveWorkloadClass(model.AppSpec{WorkloadClass: workloadClass}) == model.WorkloadClassCritical {
			limits["cpu"] = cpu
		}
	}
	if spec.MemoryMebibytes > 0 {
		memory := strconv.FormatInt(spec.MemoryMebibytes, 10) + "Mi"
		requests["memory"] = memory
		if spec.MemoryLimitMebibytes > 0 {
			limits["memory"] = strconv.FormatInt(spec.MemoryLimitMebibytes, 10) + "Mi"
		} else {
			limits["memory"] = defaultMemoryLimitForWorkloadClass(spec.MemoryMebibytes, workloadClass)
		}
	}
	if len(requests) == 0 {
		return nil
	}

	return map[string]any{
		"requests": requests,
		"limits":   limits,
	}
}

func runtimeStaticResourceRequirementsFromSpec(spec *model.ResourceSpec, defaultLimits bool) map[string]any {
	if spec == nil {
		return nil
	}
	requests := map[string]string{}
	limits := map[string]string{}
	if spec.CPUMilliCores > 0 {
		cpu := strconv.FormatInt(spec.CPUMilliCores, 10) + "m"
		requests["cpu"] = cpu
		if spec.CPULimitMilliCores > 0 {
			limits["cpu"] = strconv.FormatInt(spec.CPULimitMilliCores, 10) + "m"
		} else if defaultLimits {
			limits["cpu"] = cpu
		}
	}
	if spec.MemoryMebibytes > 0 {
		memory := strconv.FormatInt(spec.MemoryMebibytes, 10) + "Mi"
		requests["memory"] = memory
		if spec.MemoryLimitMebibytes > 0 {
			limits["memory"] = strconv.FormatInt(spec.MemoryLimitMebibytes, 10) + "Mi"
		} else if defaultLimits {
			limits["memory"] = memory
		}
	}
	return runtimeStaticResourceRequirements(requests, limits)
}

func defaultMemoryLimitForWorkloadClass(requestMiB int64, workloadClass string) string {
	if requestMiB <= 0 {
		return ""
	}
	switch model.EffectiveWorkloadClass(model.AppSpec{WorkloadClass: workloadClass}) {
	case model.WorkloadClassDemo, model.WorkloadClassBatch:
		return strconv.FormatInt(requestMiB, 10) + "Mi"
	case model.WorkloadClassService:
		return strconv.FormatInt(requestMiB, 10) + "Mi"
	default:
		return strconv.FormatInt(requestMiB, 10) + "Mi"
	}
}

func runtimeHelperResourceRequirements() map[string]any {
	return runtimeStaticResourceRequirements(
		map[string]string{
			"cpu":               defaultHelperCPURequest,
			"memory":            defaultHelperMemoryRequest,
			"ephemeral-storage": defaultHelperEphemeralRequest,
		},
		map[string]string{
			"cpu":               defaultHelperCPULimit,
			"memory":            defaultHelperMemoryLimit,
			"ephemeral-storage": defaultHelperEphemeralLimit,
		},
	)
}

func runtimeStaticResourceRequirements(requests, limits map[string]string) map[string]any {
	object := map[string]any{}
	if cloned := cloneRuntimeResourceValues(requests); len(cloned) > 0 {
		object["requests"] = cloned
	}
	if cloned := cloneRuntimeResourceValues(limits); len(cloned) > 0 {
		object["limits"] = cloned
	}
	if len(object) == 0 {
		return nil
	}
	return object
}

func cloneRuntimeResourceValues(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			cloned[key] = trimmed
		}
	}
	if len(cloned) == 0 {
		return nil
	}
	return cloned
}

type postgresRuntimeResource struct {
	baseName     string
	resourceName string
	secretName   string
	spec         model.AppPostgresSpec
	placements   []SchedulingConstraints
	serviceID    string
	serviceType  string
	ownerAppID   string
	tenantID     string
	projectID    string
}

type ManagedBackingServiceDeployment struct {
	ServiceID    string
	ResourceName string
	ResourceKind string
	RuntimeKey   string
}

func ManagedAppReleaseKey(app model.App, scheduling SchedulingConstraints) string {
	namespace := NamespaceForTenant(app.TenantID)
	object := buildAppDeploymentObject(namespace, app, appLabels(app), scheduling, managedPostgresResources(namespace, app, nil))
	return managedDeploymentRuntimeKey(object)
}

func ManagedBackingServiceDeployments(app model.App, scheduling SchedulingConstraints) []ManagedBackingServiceDeployment {
	return ManagedBackingServiceDeploymentsWithPlacements(app, scheduling, nil)
}

func ManagedBackingServiceDeploymentsWithPlacements(app model.App, scheduling SchedulingConstraints, postgresPlacements map[string][]SchedulingConstraints) []ManagedBackingServiceDeployment {
	namespace := NamespaceForTenant(app.TenantID)
	resources := managedPostgresResources(namespace, app, postgresPlacements)
	deployments := make([]ManagedBackingServiceDeployment, 0, len(resources))
	for _, resource := range resources {
		if strings.TrimSpace(resource.serviceID) == "" {
			continue
		}
		object := buildPostgresClusterObject(namespace, resource.secretName, resource.resourceName, postgresLabels(resource), resource.spec, resource.placements)
		deployments = append(deployments, ManagedBackingServiceDeployment{
			ServiceID:    resource.serviceID,
			ResourceName: resource.resourceName,
			ResourceKind: CloudNativePGClusterKind,
			RuntimeKey:   managedDeploymentRuntimeKey(object),
		})
	}
	return deployments
}

func managedPostgresResources(namespace string, app model.App, postgresPlacements map[string][]SchedulingConstraints) []postgresRuntimeResource {
	servicesByID := make(map[string]model.BackingService, len(app.BackingServices))
	for _, service := range app.BackingServices {
		servicesByID[service.ID] = service
	}

	resources := make([]postgresRuntimeResource, 0)
	for _, binding := range app.Bindings {
		service, ok := servicesByID[binding.ServiceID]
		if !ok {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) {
			continue
		}
		if !isManagedRuntimeBackingService(service) || service.Spec.Postgres == nil {
			continue
		}
		baseName := runtimeBackingServiceBaseName(service.Name, app.Name)
		spec := normalizeRuntimePostgresSpec(baseName, *service.Spec.Postgres)
		resources = append(resources, postgresRuntimeResource{
			baseName:     baseName,
			resourceName: spec.ServiceName,
			secretName:   postgresSecretName(baseName),
			spec:         spec,
			placements:   postgresPlacements[spec.ServiceName],
			serviceID:    service.ID,
			serviceType:  service.Type,
			ownerAppID:   service.OwnerAppID,
			tenantID:     service.TenantID,
			projectID:    service.ProjectID,
		})
	}

	if len(resources) == 0 && app.Spec.Postgres != nil {
		baseName := runtimeBackingServiceBaseName("", app.Name)
		spec := normalizeRuntimePostgresSpec(baseName, *app.Spec.Postgres)
		resources = append(resources, postgresRuntimeResource{
			baseName:     baseName,
			resourceName: spec.ServiceName,
			secretName:   postgresSecretName(baseName),
			spec:         spec,
			placements:   postgresPlacements[spec.ServiceName],
			serviceType:  model.BackingServiceTypePostgres,
			ownerAppID:   app.ID,
			tenantID:     app.TenantID,
			projectID:    app.ProjectID,
		})
	}

	return resources
}

func managedDeploymentRuntimeKey(obj map[string]any) string {
	metadata, _ := obj["metadata"].(map[string]any)
	spec, _ := obj["spec"].(map[string]any)
	specPayload := any(nil)
	if spec != nil {
		if template, ok := spec["template"]; ok && template != nil {
			specPayload = map[string]any{
				"template": template,
			}
		} else {
			specPayload = spec
		}
	}
	payload := map[string]any{
		"apiVersion": obj["apiVersion"],
		"kind":       obj["kind"],
		"metadata": map[string]any{
			"name":      metadata["name"],
			"namespace": metadata["namespace"],
		},
		"spec": specPayload,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func annotateManagedDeploymentReleaseKey(obj map[string]any) {
	key := managedDeploymentRuntimeKey(obj)
	if key == "" {
		return
	}
	metadata, _ := obj["metadata"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
		obj["metadata"] = metadata
	}
	annotations := map[string]string{}
	if existing, ok := metadata["annotations"].(map[string]string); ok {
		for name, value := range existing {
			annotations[name] = value
		}
	} else if existing, ok := metadata["annotations"].(map[string]any); ok {
		for name, value := range existing {
			if text, ok := value.(string); ok {
				annotations[name] = text
			}
		}
	}
	annotations[FugueAnnotationReleaseKey] = key
	metadata["annotations"] = annotations
}

func mergedRuntimeEnv(app model.App) map[string]string {
	merged := make(map[string]string)
	hasManagedPostgresBinding := false
	servicesByID := make(map[string]model.BackingService, len(app.BackingServices))
	for _, service := range app.BackingServices {
		servicesByID[service.ID] = service
	}

	for _, binding := range app.Bindings {
		for key, value := range binding.Env {
			merged[key] = value
		}
		service, ok := servicesByID[binding.ServiceID]
		if !ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) {
			hasManagedPostgresBinding = true
			if service.Spec.Postgres != nil {
				for key, value := range defaultRuntimePostgresEnv(*service.Spec.Postgres) {
					merged[key] = value
				}
			}
		}
	}

	if !hasManagedPostgresBinding && app.Spec.Postgres != nil {
		baseName := runtimeBackingServiceBaseName("", app.Name)
		for key, value := range defaultRuntimePostgresEnv(normalizeRuntimePostgresSpec(baseName, *app.Spec.Postgres)) {
			if _, exists := merged[key]; !exists {
				merged[key] = value
			}
		}
	}

	for key, value := range app.Spec.Env {
		merged[key] = value
	}

	if len(merged) == 0 {
		return nil
	}
	return merged
}

func buildAppServiceObject(namespace string, app model.App, labels map[string]string) map[string]any {
	if !model.AppHasClusterService(app.Spec) {
		return nil
	}

	servicePorts := appServicePorts(app.Spec.Ports)
	if len(servicePorts) == 0 {
		return nil
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      RuntimeAppResourceName(app),
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"selector": labels,
			"ports":    servicePorts,
		},
	}
}

func appServicePorts(ports []int) []map[string]any {
	servicePorts := make([]map[string]any, 0, len(ports))
	for _, port := range ports {
		if port <= 0 {
			continue
		}
		servicePorts = append(servicePorts, map[string]any{
			"name":       "tcp-" + strconv.Itoa(port),
			"port":       port,
			"targetPort": port,
			"protocol":   "TCP",
		})
	}
	return servicePorts
}

func buildAppNetworkPolicyObject(namespace string, app model.App, labels map[string]string) map[string]any {
	policy := app.Spec.NetworkPolicy
	if policy == nil {
		return nil
	}

	podSelector := labelSubset(labels, FugueLabelAppID)
	if len(podSelector) == 0 {
		podSelector = labels
	}

	spec := map[string]any{
		"podSelector": map[string]any{
			"matchLabels": podSelector,
		},
	}
	policyTypes := make([]string, 0, 2)
	if networkPolicyDirectionRestricted(policy.Ingress) {
		policyTypes = append(policyTypes, "Ingress")
		spec["ingress"] = buildNetworkPolicyIngressRules(policy.Ingress)
	}
	if networkPolicyDirectionRestricted(policy.Egress) {
		policyTypes = append(policyTypes, "Egress")
		spec["egress"] = buildNetworkPolicyEgressRules(policy.Egress)
	}
	if len(policyTypes) == 0 {
		return nil
	}
	spec["policyTypes"] = policyTypes

	return map[string]any{
		"apiVersion": KubernetesNetworkPolicyAPIVersion,
		"kind":       "NetworkPolicy",
		"metadata": map[string]any{
			"name":      networkPolicyName(RuntimeAppResourceName(app)),
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": spec,
	}
}

func networkPolicyDirectionRestricted(direction *model.AppNetworkPolicyDirectionSpec) bool {
	return direction != nil && model.NormalizeAppNetworkPolicyMode(direction.Mode) == model.AppNetworkPolicyModeRestricted
}

func buildNetworkPolicyIngressRules(direction *model.AppNetworkPolicyDirectionSpec) []map[string]any {
	if direction == nil {
		return nil
	}
	rules := make([]map[string]any, 0, len(direction.AllowApps))
	for _, peer := range direction.AllowApps {
		rule := map[string]any{
			"from": []map[string]any{
				networkPolicyAppPeerSelector(peer.AppID),
			},
		}
		if ports := buildNetworkPolicyPorts(peer.Ports); len(ports) > 0 {
			rule["ports"] = ports
		}
		rules = append(rules, rule)
	}
	return rules
}

func buildNetworkPolicyEgressRules(direction *model.AppNetworkPolicyDirectionSpec) []map[string]any {
	if direction == nil {
		return nil
	}
	rules := make([]map[string]any, 0, len(direction.AllowApps)+2)
	if direction.AllowDNS {
		rules = append(rules, map[string]any{
			"ports": []map[string]any{
				{"protocol": "UDP", "port": 53},
				{"protocol": "TCP", "port": 53},
			},
		})
	}
	if direction.AllowPublicInternet {
		rules = append(rules, map[string]any{
			"to": []map[string]any{
				{
					"ipBlock": map[string]any{
						"cidr":   "0.0.0.0/0",
						"except": privateEgressCIDRBlocks(),
					},
				},
			},
		})
	}
	for _, peer := range direction.AllowApps {
		rule := map[string]any{
			"to": []map[string]any{
				networkPolicyAppPeerSelector(peer.AppID),
			},
		}
		if ports := buildNetworkPolicyPorts(peer.Ports); len(ports) > 0 {
			rule["ports"] = ports
		}
		rules = append(rules, rule)
	}
	return rules
}

func networkPolicyAppPeerSelector(appID string) map[string]any {
	return map[string]any{
		"podSelector": map[string]any{
			"matchLabels": map[string]string{
				FugueLabelAppID: strings.TrimSpace(appID),
			},
		},
	}
}

func buildNetworkPolicyPorts(ports []int) []map[string]any {
	if len(ports) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(ports))
	for _, port := range ports {
		if port <= 0 || port > 65535 {
			continue
		}
		out = append(out, map[string]any{
			"protocol": "TCP",
			"port":     port,
		})
	}
	return out
}

func privateEgressCIDRBlocks() []string {
	return []string{
		"10.0.0.0/8",
		"100.64.0.0/10",
		"127.0.0.0/8",
		"169.254.0.0/16",
		"172.16.0.0/12",
		"192.168.0.0/16",
	}
}

func networkPolicyName(appResourceName string) string {
	name := sanitizeName(appResourceName)
	if name == "" {
		name = "app"
	}
	const suffix = "-network"
	if len(name)+len(suffix) > 63 {
		name = strings.TrimRight(name[:63-len(suffix)], "-")
	}
	return name + suffix
}

func buildComposeServiceAliasObject(namespace string, app model.App) map[string]any {
	if app.Source == nil || !model.AppHasClusterService(app.Spec) {
		return nil
	}
	composeService := strings.TrimSpace(app.Source.ComposeService)
	aliasName := ComposeServiceAliasName(app.ProjectID, composeService)
	if aliasName == "" || aliasName == RuntimeAppResourceName(app) {
		return nil
	}
	servicePorts := appServicePorts(app.Spec.Ports)
	if len(servicePorts) == 0 {
		return nil
	}

	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      aliasName,
			"namespace": namespace,
			"labels":    composeServiceAliasLabels(app, composeService),
		},
		"spec": map[string]any{
			"selector": appLabels(app),
			"ports":    servicePorts,
		},
	}
}

func buildLegacyComposeAppNameAliasObject(namespace string, app model.App) map[string]any {
	if app.Source == nil || !model.AppHasClusterService(app.Spec) {
		return nil
	}
	composeService := strings.TrimSpace(app.Source.ComposeService)
	if composeService == "" {
		return nil
	}
	aliasName := RuntimeResourceName(app.Name)
	if aliasName == "" {
		return nil
	}
	if aliasName == RuntimeAppResourceName(app) || aliasName == ComposeServiceAliasName(app.ProjectID, composeService) {
		return nil
	}
	servicePorts := appServicePorts(app.Spec.Ports)
	if len(servicePorts) == 0 {
		return nil
	}

	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      aliasName,
			"namespace": namespace,
			"labels":    legacyComposeAppNameAliasLabels(app),
		},
		"spec": map[string]any{
			"selector": appLabels(app),
			"ports":    servicePorts,
		},
	}
}

func composeServiceAliasLabels(app model.App, composeService string) map[string]string {
	labels := appLabels(app)
	labels[FugueLabelComponent] = "compose-service-alias"
	if composeService = sanitizeName(composeService); composeService != "" {
		labels[FugueLabelName] = composeService
	}
	return labels
}

func legacyComposeAppNameAliasLabels(app model.App) map[string]string {
	labels := appLabels(app)
	labels[FugueLabelComponent] = "legacy-compose-app-name-alias"
	if name := RuntimeResourceName(app.Name); name != "" {
		labels[FugueLabelName] = name
	}
	return labels
}

func deploymentStrategy(app model.App) map[string]any {
	if normalizeRuntimeAppWorkspaceSpec(app) != nil || normalizeRuntimeAppPersistentStorageSpec(app) != nil {
		return map[string]any{"type": "Recreate"}
	}
	return map[string]any{
		"type": "RollingUpdate",
		"rollingUpdate": map[string]any{
			"maxUnavailable": 0,
			"maxSurge":       1,
		},
	}
}

func buildEnvObjects(env map[string]string) []map[string]any {
	if len(env) == 0 {
		return nil
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	objects := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, map[string]any{
			"name":  key,
			"value": env[key],
		})
	}
	return objects
}

func buildAppTemplateAnnotations(spec model.AppSpec) map[string]string {
	annotations := map[string]string{}
	if checksum := appFilesChecksum(spec.Files); checksum != "" {
		annotations["fugue.pro/files-checksum"] = checksum
	}
	if token := strings.TrimSpace(spec.RestartToken); token != "" {
		annotations["fugue.pro/restart-token"] = token
	}
	if len(annotations) == 0 {
		return nil
	}
	return annotations
}

func appFilesChecksum(files []model.AppFile) string {
	if len(files) == 0 {
		return ""
	}
	payload, err := json.Marshal(files)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func applyScheduling(podSpec *map[string]any, scheduling SchedulingConstraints) {
	if len(scheduling.NodeSelector) > 0 {
		nodeSelector := make(map[string]string, len(scheduling.NodeSelector))
		for key, value := range scheduling.NodeSelector {
			nodeSelector[key] = value
		}
		(*podSpec)["nodeSelector"] = nodeSelector
	}
	if len(scheduling.Tolerations) > 0 {
		tolerations := make([]map[string]any, 0, len(scheduling.Tolerations))
		for _, toleration := range scheduling.Tolerations {
			tolerations = append(tolerations, map[string]any{
				"key":      toleration.Key,
				"operator": toleration.Operator,
				"value":    toleration.Value,
				"effect":   toleration.Effect,
			})
		}
		(*podSpec)["tolerations"] = tolerations
	}
}

func buildPostgresAffinity(spec model.AppPostgresSpec, placements []SchedulingConstraints) map[string]any {
	if len(placements) == 0 {
		return nil
	}

	affinity := map[string]any{}
	if nodeAffinity := buildPostgresNodeAffinity(placements); len(nodeAffinity) > 0 {
		affinity["nodeAffinity"] = nodeAffinity
	}
	if tolerations := buildPostgresTolerations(placements); len(tolerations) > 0 {
		affinity["tolerations"] = tolerations
	}
	if spec.Instances > 1 && len(placements) > 1 {
		affinity["enablePodAntiAffinity"] = true
		affinity["podAntiAffinityType"] = "required"
		affinity["topologyKey"] = "kubernetes.io/hostname"
	}
	if len(affinity) == 0 {
		return nil
	}
	return affinity
}

func buildPostgresNodeAffinity(placements []SchedulingConstraints) map[string]any {
	terms := make([]map[string]any, 0, len(placements))
	for _, placement := range placements {
		expressions := selectorMatchExpressions(placement.NodeSelector)
		if len(expressions) == 0 {
			continue
		}
		terms = append(terms, map[string]any{
			"matchExpressions": expressions,
		})
	}
	if len(terms) == 0 {
		return nil
	}
	return map[string]any{
		"requiredDuringSchedulingIgnoredDuringExecution": map[string]any{
			"nodeSelectorTerms": terms,
		},
	}
}

func selectorMatchExpressions(selector map[string]string) []map[string]any {
	if len(selector) == 0 {
		return nil
	}
	keys := make([]string, 0, len(selector))
	for key := range selector {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	expressions := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		expressions = append(expressions, map[string]any{
			"key":      key,
			"operator": "In",
			"values":   []string{selector[key]},
		})
	}
	return expressions
}

func buildPostgresTolerations(placements []SchedulingConstraints) []map[string]any {
	seen := make(map[string]struct{})
	tolerations := make([]map[string]any, 0)
	for _, placement := range placements {
		for _, toleration := range placement.Tolerations {
			key := strings.Join([]string{
				toleration.Key,
				toleration.Operator,
				toleration.Value,
				toleration.Effect,
			}, "\x00")
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			tolerations = append(tolerations, map[string]any{
				"key":      toleration.Key,
				"operator": toleration.Operator,
				"value":    toleration.Value,
				"effect":   toleration.Effect,
			})
		}
	}
	if len(tolerations) == 0 {
		return nil
	}
	return tolerations
}

func normalizeRuntimePostgresSpec(baseName string, spec model.AppPostgresSpec) model.AppPostgresSpec {
	spec.Image = model.NormalizeManagedPostgresImage(spec.Image)
	if strings.TrimSpace(spec.Image) == "" {
		spec.Image = defaultPostgresImage
	}
	if strings.TrimSpace(spec.Database) == "" {
		spec.Database = baseName
	}
	if strings.TrimSpace(spec.User) == "" {
		spec.User = model.DefaultManagedPostgresUser(baseName)
	}
	spec.ServiceName = normalizePostgresResourceName(spec.ServiceName, baseName)
	if strings.TrimSpace(spec.StorageSize) == "" {
		spec.StorageSize = defaultPostgresStorage
	}
	spec.StorageClassName = strings.TrimSpace(spec.StorageClassName)
	spec.RuntimeID = strings.TrimSpace(spec.RuntimeID)
	spec.FailoverTargetRuntimeID = strings.TrimSpace(spec.FailoverTargetRuntimeID)
	spec.PrimaryNodeName = strings.TrimSpace(spec.PrimaryNodeName)
	if spec.Instances <= 0 {
		spec.Instances = defaultPostgresInstances
	}
	if spec.Instances < 1 {
		spec.Instances = 1
	}
	if spec.FailoverTargetRuntimeID != "" && spec.Instances < 2 {
		spec.Instances = 2
	}
	if spec.SynchronousReplicas < 0 {
		spec.SynchronousReplicas = 0
	}
	if spec.FailoverTargetRuntimeID != "" && spec.SynchronousReplicas < 1 {
		spec.SynchronousReplicas = 1
	}
	if spec.SynchronousReplicas >= spec.Instances {
		spec.SynchronousReplicas = spec.Instances - 1
	}
	return spec
}

func normalizeRuntimeAppWorkspaceSpec(app model.App) *model.AppWorkspaceSpec {
	if app.Spec.Workspace == nil {
		return nil
	}
	spec := *app.Spec.Workspace
	mountPath, err := model.NormalizeAppWorkspaceMountPath(spec.MountPath)
	if err != nil {
		return nil
	}
	spec.MountPath = mountPath

	storagePath, err := model.NormalizeAppWorkspaceStoragePath(spec.StoragePath)
	if err != nil {
		return nil
	}
	if storagePath == "" {
		namespace := NamespaceForTenant(app.TenantID)
		storagePath = path.Join("/var/lib/fugue/tenant-data", namespace, "apps", workspaceStorageBaseName(app), "workspace")
	}
	spec.StoragePath = storagePath
	if strings.TrimSpace(spec.StorageSize) == "" {
		spec.StorageSize = defaultWorkspaceStorage
	}
	spec.StorageClassName = strings.TrimSpace(spec.StorageClassName)
	return &spec
}

func normalizeRuntimeAppPersistentStorageSpec(app model.App) *model.AppPersistentStorageSpec {
	if app.Spec.PersistentStorage == nil {
		return nil
	}
	spec := *app.Spec.PersistentStorage
	mode, err := model.NormalizeAppPersistentStorageMode(spec.Mode)
	if err != nil {
		return nil
	}
	spec.Mode = mode
	storagePath, err := model.NormalizeAppPersistentStoragePath(spec.StoragePath)
	if err != nil {
		return nil
	}
	if storagePath == "" {
		namespace := NamespaceForTenant(app.TenantID)
		storagePath = path.Join("/var/lib/fugue/tenant-data", namespace, "apps", workspaceStorageBaseName(app), "persistent-storage")
	}
	spec.StoragePath = storagePath
	if strings.TrimSpace(spec.StorageSize) == "" {
		spec.StorageSize = defaultWorkspaceStorage
	}
	spec.StorageClassName = strings.TrimSpace(spec.StorageClassName)
	if claimName := strings.TrimSpace(spec.ClaimName); claimName != "" {
		spec.ClaimName = sanitizeName(claimName)
	}
	sharedSubPath, err := model.NormalizeAppPersistentStorageSharedSubPath(spec.SharedSubPath)
	if err != nil {
		return nil
	}
	if spec.Mode == model.AppPersistentStorageModeSharedProjectRWX {
		if strings.TrimSpace(app.ProjectID) == "" {
			return nil
		}
		if sharedSubPath == "" {
			sharedSubPath = path.Join("apps", workspaceStorageBaseName(app))
		}
	}
	spec.SharedSubPath = sharedSubPath
	if len(spec.Mounts) == 0 {
		return nil
	}

	mounts := make([]model.AppPersistentStorageMount, 0, len(spec.Mounts))
	for _, mount := range spec.Mounts {
		kind, err := model.NormalizeAppPersistentStorageMountKind(mount.Kind)
		if err != nil {
			return nil
		}
		pathValue, err := model.NormalizeAppPersistentStorageMountPath(kind, mount.Path)
		if err != nil {
			return nil
		}
		normalized := mount
		normalized.Kind = kind
		normalized.Path = pathValue
		if normalized.Mode == 0 {
			normalized.Mode = defaultPersistentStorageMountMode(normalized)
		}
		mounts = append(mounts, normalized)
	}
	spec.Mounts = mounts
	return &spec
}

func defaultPersistentStorageMountMode(mount model.AppPersistentStorageMount) int32 {
	switch strings.TrimSpace(strings.ToLower(mount.Kind)) {
	case model.AppPersistentStorageMountKindDirectory:
		return 0o755
	case model.AppPersistentStorageMountKindFile:
		if mount.Secret {
			return 0o600
		}
		return 0o644
	default:
		return 0o644
	}
}

func persistentStorageContainerRootPath(spec model.AppPersistentStorageSpec) string {
	if !model.AppPersistentStorageSpecUsesSharedProjectRWX(&spec) || strings.TrimSpace(spec.SharedSubPath) == "" {
		return persistentStorageRootPath
	}
	return path.Join(persistentStorageRootPath, spec.SharedSubPath)
}

func persistentStorageMountSubPath(spec model.AppPersistentStorageSpec, mount model.AppPersistentStorageMount) string {
	subPath := model.AppPersistentStorageMountSubPath(mount)
	if !model.AppPersistentStorageSpecUsesSharedProjectRWX(&spec) || strings.TrimSpace(spec.SharedSubPath) == "" {
		return subPath
	}
	return path.Join(spec.SharedSubPath, subPath)
}

func persistentStoragePVCName(app model.App, spec model.AppPersistentStorageSpec) string {
	if model.AppPersistentStorageSpecUsesSharedProjectRWX(&spec) {
		return ProjectSharedWorkspacePVCName(app)
	}
	if strings.TrimSpace(spec.ClaimName) != "" {
		return sanitizeName(spec.ClaimName)
	}
	return WorkspacePVCName(app)
}

func AppHasReplicableVolume(app model.App) bool {
	return model.AppSpecHasReplicableVolume(app.Spec)
}

func AppVolumeReplicationEnabled(app model.App) bool {
	return model.AppSpecVolumeReplicationEnabled(app.Spec)
}

func AppUsesWorkspaceReplication(app model.App) bool {
	return AppVolumeReplicationEnabled(app)
}

func workspaceStorageBaseName(app model.App) string {
	if id := strings.TrimSpace(app.ID); id != "" {
		return id
	}
	name := sanitizeName(app.Name)
	if name == "" {
		return "app"
	}
	return name
}

func buildAppWorkspaceInitContainer(spec model.AppWorkspaceSpec) map[string]any {
	return map[string]any{
		"name":  "init-workspace",
		"image": defaultWaitImage,
		"command": []string{
			"sh",
			"-lc",
			workspaceInitScript(),
			"sh",
			spec.MountPath,
			strings.TrimSpace(spec.ResetToken),
		},
		"securityContext": map[string]any{
			"runAsUser": 0,
		},
		"resources": runtimeHelperResourceRequirements(),
		"volumeMounts": []map[string]any{
			{
				"name":      workspaceVolumeName,
				"mountPath": spec.MountPath,
			},
		},
	}
}

func buildAppWorkspacePVCObject(namespace string, app model.App, labels map[string]string, spec model.AppWorkspaceSpec) map[string]any {
	pvcSpec := map[string]any{
		"accessModes": []string{"ReadWriteOnce"},
		"resources": map[string]any{
			"requests": map[string]any{
				"storage": spec.StorageSize,
			},
		},
	}
	if strings.TrimSpace(spec.StorageClassName) != "" {
		pvcSpec["storageClassName"] = strings.TrimSpace(spec.StorageClassName)
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata": map[string]any{
			"name":      WorkspacePVCName(app),
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": pvcSpec,
	}
}

func buildWorkspaceReplicationDestinationObject(namespace string, app model.App, labels map[string]string, spec model.AppWorkspaceSpec) map[string]any {
	rsyncTLS := map[string]any{
		"copyMethod": "Direct",
		"capacity":   spec.StorageSize,
		"accessModes": []string{
			"ReadWriteOnce",
		},
	}
	if strings.TrimSpace(spec.StorageClassName) != "" {
		rsyncTLS["storageClassName"] = strings.TrimSpace(spec.StorageClassName)
	}
	return map[string]any{
		"apiVersion": VolSyncAPIVersion,
		"kind":       VolSyncReplicationDestinationKind,
		"metadata": map[string]any{
			"name":      WorkspaceReplicationDestinationName(app),
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"trigger": map[string]any{
				"manual": "bootstrap",
			},
			"rsyncTLS": rsyncTLS,
		},
	}
}

func BuildWorkspaceReplicationSourceObject(app model.App, ownerRef *OwnerReference, address, keySecret string) map[string]any {
	namespace := NamespaceForTenant(app.TenantID)
	labels := appLabels(app)
	trigger := map[string]any{
		"schedule": model.EffectiveAppVolumeReplicationSchedule(app.Spec),
	}
	if model.EffectiveAppVolumeReplicationMode(app.Spec) == model.AppVolumeReplicationModeManual {
		trigger = map[string]any{
			"manual": "bootstrap",
		}
	}
	source := map[string]any{
		"apiVersion": VolSyncAPIVersion,
		"kind":       VolSyncReplicationSourceKind,
		"metadata": map[string]any{
			"name":      WorkspaceReplicationSourceName(app),
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"sourcePVC": WorkspacePVCName(app),
			"trigger":   trigger,
			"rsyncTLS": map[string]any{
				"address":    strings.TrimSpace(address),
				"keySecret":  strings.TrimSpace(keySecret),
				"copyMethod": "Direct",
			},
		},
	}
	attachOwnerReference([]map[string]any{source}, ownerRef)
	return source
}

func buildAppWorkspaceSidecar(spec model.AppWorkspaceSpec) map[string]any {
	return map[string]any{
		"name":  workspaceSidecarName,
		"image": defaultWaitImage,
		"command": []string{
			"sh",
			"-lc",
			"trap 'exit 0' TERM INT; while :; do sleep 3600; done",
		},
		"resources": runtimeHelperResourceRequirements(),
		"volumeMounts": []map[string]any{
			{
				"name":      workspaceVolumeName,
				"mountPath": spec.MountPath,
			},
		},
	}
}

func buildPersistentStorageVolumeMounts(spec model.AppPersistentStorageSpec) []map[string]any {
	if model.AppPersistentStorageSpecUsesDirectSharedProjectDirectoryMount(&spec) {
		mount := spec.Mounts[0]
		volumeMount := map[string]any{
			"name":      workspaceVolumeName,
			"mountPath": mount.Path,
		}
		if strings.TrimSpace(spec.SharedSubPath) != "" {
			volumeMount["subPath"] = spec.SharedSubPath
		}
		return []map[string]any{volumeMount}
	}

	mounts := make([]map[string]any, 0, len(spec.Mounts))
	for _, mount := range spec.Mounts {
		mounts = append(mounts, map[string]any{
			"name":      workspaceVolumeName,
			"mountPath": mount.Path,
			"subPath":   persistentStorageMountSubPath(spec, mount),
		})
	}
	return mounts
}

func buildAppPersistentStorageInitContainer(spec model.AppPersistentStorageSpec) map[string]any {
	return map[string]any{
		"name":  "init-persistent-storage",
		"image": defaultWaitImage,
		"command": []string{
			"sh",
			"-lc",
			persistentStorageInitScript(),
			"sh",
			persistentStorageContainerRootPath(spec),
			strings.TrimSpace(spec.ResetToken),
			buildPersistentStorageMountPlan(spec),
		},
		"securityContext": map[string]any{
			"runAsUser": 0,
		},
		"resources": runtimeHelperResourceRequirements(),
		"volumeMounts": []map[string]any{
			{
				"name":      workspaceVolumeName,
				"mountPath": persistentStorageRootPath,
			},
		},
	}
}

func buildAppPersistentStoragePVCObject(namespace string, app model.App, labels map[string]string, spec model.AppPersistentStorageSpec) map[string]any {
	return buildPersistentStoragePVCObject(namespace, persistentStoragePVCName(app, spec), labels, []string{"ReadWriteOnce"}, spec.StorageSize, spec.StorageClassName)
}

func buildProjectSharedPersistentStoragePVCObject(namespace string, app model.App, spec model.AppPersistentStorageSpec) map[string]any {
	return buildPersistentStoragePVCObject(namespace, ProjectSharedWorkspacePVCName(app), projectSharedPersistentStorageLabels(app), []string{"ReadWriteMany"}, spec.StorageSize, spec.StorageClassName)
}

func buildPersistentStoragePVCObject(namespace, name string, labels map[string]string, accessModes []string, storageSize, storageClassName string) map[string]any {
	if len(accessModes) == 0 {
		accessModes = []string{"ReadWriteOnce"}
	}
	pvcSpec := map[string]any{
		"accessModes": accessModes,
		"resources": map[string]any{
			"requests": map[string]any{
				"storage": storageSize,
			},
		},
	}
	if strings.TrimSpace(storageClassName) != "" {
		pvcSpec["storageClassName"] = strings.TrimSpace(storageClassName)
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": pvcSpec,
	}
}

func buildPersistentStorageReplicationDestinationObject(namespace string, app model.App, labels map[string]string, spec model.AppPersistentStorageSpec) map[string]any {
	rsyncTLS := map[string]any{
		"copyMethod": "Direct",
		"capacity":   spec.StorageSize,
		"accessModes": []string{
			"ReadWriteOnce",
		},
	}
	if strings.TrimSpace(spec.StorageClassName) != "" {
		rsyncTLS["storageClassName"] = strings.TrimSpace(spec.StorageClassName)
	}
	return map[string]any{
		"apiVersion": VolSyncAPIVersion,
		"kind":       VolSyncReplicationDestinationKind,
		"metadata": map[string]any{
			"name":      WorkspaceReplicationDestinationName(app),
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"trigger": map[string]any{
				"manual": "bootstrap",
			},
			"rsyncTLS": rsyncTLS,
		},
	}
}

func buildAppPersistentStorageSidecar(spec model.AppPersistentStorageSpec) map[string]any {
	return map[string]any{
		"name":  workspaceSidecarName,
		"image": defaultWaitImage,
		"command": []string{
			"sh",
			"-lc",
			"trap 'exit 0' TERM INT; while :; do sleep 3600; done",
		},
		"resources":    runtimeHelperResourceRequirements(),
		"volumeMounts": buildPersistentStorageVolumeMounts(spec),
	}
}

func buildPersistentStorageMountPlan(spec model.AppPersistentStorageSpec) string {
	lines := make([]string, 0, len(spec.Mounts))
	for _, mount := range spec.Mounts {
		lines = append(lines, strings.Join([]string{
			strings.TrimSpace(strings.ToLower(mount.Kind)),
			model.AppPersistentStorageMountKey(mount),
			strconv.FormatInt(int64(mount.Mode), 8),
			base64.StdEncoding.EncodeToString([]byte(mount.SeedContent)),
		}, "\t"))
	}
	return strings.Join(lines, "\n")
}

func persistentStorageInitScript() string {
	return `storage_root="$1"
token="$2"
plan="$3"
state_dir="$storage_root/` + model.AppPersistentStorageInternalDirName + `"
mounts_dir="$storage_root/` + model.AppPersistentStorageMountRootPath("") + `"
marker="$state_dir/reset-token"
mkdir -p "$state_dir" "$mounts_dir"
if [ -n "$token" ]; then
  current=""
  if [ -f "$marker" ]; then
    current="$(cat "$marker" 2>/dev/null || true)"
  fi
  if [ "$current" != "$token" ]; then
    rm -rf "$mounts_dir"/* 2>/dev/null || true
    mkdir -p "$mounts_dir"
    printf '%s' "$token" > "$marker"
  fi
fi
if [ -z "$plan" ]; then
  exit 0
fi
printf '%s\n' "$plan" | while IFS='	' read -r kind key mode seed; do
  [ -n "$kind" ] || continue
  target="$mounts_dir/$key"
  case "$kind" in
    directory)
      mkdir -p "$target"
      if [ -n "$mode" ] && [ "$mode" != "0" ]; then
        chmod "$mode" "$target" 2>/dev/null || true
      fi
      ;;
    file)
      mkdir -p "$(dirname "$target")"
      if [ ! -f "$target" ]; then
        : > "$target"
        if [ -n "$seed" ]; then
          printf '%s' "$seed" | base64 -d > "$target"
        fi
      fi
      if [ -n "$mode" ] && [ "$mode" != "0" ]; then
        chmod "$mode" "$target" 2>/dev/null || true
      fi
      ;;
  esac
done`
}

func workspaceInitScript() string {
	return `workspace="$1"
token="$2"
state_dir="$workspace/` + model.AppWorkspaceInternalDirName + `"
marker="$state_dir/reset-token"
mkdir -p "$workspace"
chmod 0777 "$workspace"
if [ -n "$token" ]; then
  current=""
  if [ -f "$marker" ]; then
    current="$(cat "$marker" 2>/dev/null || true)"
  fi
  if [ "$current" != "$token" ]; then
    rm -rf "$workspace"/..?* "$workspace"/.[!.]* "$workspace"/* 2>/dev/null || true
    mkdir -p "$state_dir"
    printf '%s' "$token" > "$marker"
  fi
fi`
}

func appFilesSecretName(appName string) string {
	return appName + "-files"
}

func WorkspacePVCName(app model.App) string {
	return normalizePostgresAuxiliaryName(workspaceStorageBaseName(app), "workspace")
}

func ProjectSharedWorkspacePVCName(app model.App) string {
	base := strings.TrimSpace(app.ProjectID)
	if base == "" {
		base = strings.TrimSpace(app.TenantID)
	}
	if base == "" {
		base = "project"
	}
	return normalizePostgresAuxiliaryName(base, "shared-workspace")
}

func WorkspaceReplicationDestinationName(app model.App) string {
	return normalizePostgresAuxiliaryName(workspaceStorageBaseName(app), "workspace-dst")
}

func WorkspaceReplicationSourceName(app model.App) string {
	return normalizePostgresAuxiliaryName(workspaceStorageBaseName(app), "workspace-src")
}

func postgresResourceName(appName string) string {
	return appName + "-postgres"
}

func postgresSecretName(appName string) string {
	return appName + "-pgsec"
}

func normalizePostgresResourceName(name, baseName string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = postgresResourceName(baseName)
	}
	name = model.Slugify(name)
	if len(name) > 63 {
		return name[:63]
	}
	return name
}

func normalizePostgresAuxiliaryName(base, suffix string) string {
	base = model.Slugify(strings.TrimSpace(base))
	suffix = model.Slugify(strings.TrimSpace(suffix))
	if base == "" {
		base = "postgres"
	}
	if suffix == "" {
		if len(base) > 63 {
			return base[:63]
		}
		return base
	}
	name := base + "-" + suffix
	if len(name) <= 63 {
		return name
	}
	maxBaseLen := 63 - len(suffix) - 1
	if maxBaseLen <= 0 {
		return name[:63]
	}
	return base[:maxBaseLen] + "-" + suffix
}

func runtimeBackingServiceBaseName(serviceName, fallback string) string {
	name := strings.TrimSpace(serviceName)
	if name == "" {
		name = fallback
	}
	name = sanitizeName(name)
	if name == "" {
		return "service"
	}
	return name
}

func defaultRuntimePostgresEnv(spec model.AppPostgresSpec) map[string]string {
	return map[string]string{
		"DB_TYPE":     "postgres",
		"DB_HOST":     model.PostgresRWServiceName(spec.ServiceName),
		"DB_PORT":     "5432",
		"DB_USER":     spec.User,
		"DB_PASSWORD": spec.Password,
		"DB_NAME":     spec.Database,
	}
}

func postgresRWServiceName(clusterName string) string {
	return model.PostgresRWServiceName(clusterName)
}

func serviceFQDN(namespace, serviceName string) string {
	namespace = strings.TrimSpace(namespace)
	serviceName = strings.TrimSpace(serviceName)
	if serviceName == "" {
		return ""
	}
	if namespace == "" {
		return serviceName
	}
	return serviceName + "." + namespace + ".svc.cluster.local"
}

func postgresRWServiceFQDN(namespace, clusterName string) string {
	return serviceFQDN(namespace, postgresRWServiceName(clusterName))
}

func isManagedRuntimeBackingService(service model.BackingService) bool {
	provisioner := strings.TrimSpace(service.Provisioner)
	return provisioner == "" || strings.EqualFold(provisioner, model.BackingServiceProvisionerManaged)
}

func fileKey(index int) string {
	return "file-" + strconv.Itoa(index)
}

func sanitizeName(name string) string {
	name = model.Slugify(name)
	if len(name) > 50 {
		return name[:50]
	}
	return name
}
