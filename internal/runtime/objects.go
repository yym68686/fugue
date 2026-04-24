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
	defaultPostgresImage                = ""
	defaultPostgresStorage              = "1Gi"
	defaultPostgresInstances            = 1
	defaultPostgresSynchronousReplicas  = 1
	defaultWorkspaceStorage             = "10Gi"
	defaultWorkspaceReplicationSchedule = "*/5 * * * *"
	defaultWaitImage                    = "busybox:1.36"
	defaultHelperCPURequest             = "25m"
	defaultHelperCPULimit               = "100m"
	defaultHelperMemoryRequest          = "32Mi"
	defaultHelperMemoryLimit            = "128Mi"
	defaultHelperEphemeralRequest       = "32Mi"
	defaultHelperEphemeralLimit         = "128Mi"
	appFilesVolumeName                  = "app-files"
	appFilesSourceMountPath             = "/fugue-app-files"
	AppWorkspaceContainerName           = "fugue-workspace"
	workspaceVolumeName                 = "app-workspace"
	workspaceSidecarName                = AppWorkspaceContainerName
	persistentStorageRootPath           = "/fugue-persistent-storage"

	CloudNativePGAPIVersion           = "postgresql.cnpg.io/v1"
	CloudNativePGClusterKind          = "Cluster"
	CloudNativePGReconcilePodSpecAnno = "cnpg.io/reconcilePodSpec"
	CloudNativePGReconcilePodSpecHold = "disabled"
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
			buildWorkspaceReplicationDestinationObject(namespace, app, labels, *workspaceSpec),
		)
	} else if storageSpec := normalizeRuntimeAppPersistentStorageSpec(app); storageSpec != nil {
		objects = append(objects,
			buildAppPersistentStoragePVCObject(namespace, app, labels, *storageSpec),
			buildPersistentStorageReplicationDestinationObject(namespace, app, labels, *storageSpec),
		)
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
	if spec.SynchronousReplicas > 0 && spec.Instances > 1 {
		clusterSpec["minSyncReplicas"] = spec.SynchronousReplicas
		clusterSpec["maxSyncReplicas"] = spec.SynchronousReplicas
	}
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
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      resourceName,
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"type":         "ExternalName",
			"externalName": postgresRWServiceFQDN(namespace, spec.ServiceName),
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
	if resources := runtimeResourceRequirements(app.Spec.Resources); resources != nil {
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
				"claimName": WorkspacePVCName(app),
			},
		})
		initContainers = append(initContainers, buildAppPersistentStorageInitContainer(*storageSpec))
		sidecars = append(sidecars, buildAppPersistentStorageSidecar(*storageSpec))
	}
	if len(volumeMounts) > 0 {
		container["volumeMounts"] = volumeMounts
	}

	podSpec := map[string]any{
		"containers": []map[string]any{container},
	}
	if len(sidecars) > 0 {
		podSpec["containers"] = append(podSpec["containers"].([]map[string]any), sidecars...)
	}
	if len(volumes) > 0 {
		podSpec["volumes"] = volumes
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
					"until nc -z " + model.PostgresRWServiceName(postgres.spec.ServiceName) + " 5432; do sleep 2; done",
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

	return map[string]any{
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
		"initialDelaySeconds": 1,
		"periodSeconds":       2,
		"timeoutSeconds":      1,
		"failureThreshold":    15,
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

func runtimeResourceRequirements(spec *model.ResourceSpec) map[string]any {
	if spec == nil {
		return nil
	}

	requests := map[string]string{}
	limits := map[string]string{}

	if spec.CPUMilliCores > 0 {
		cpu := strconv.FormatInt(spec.CPUMilliCores, 10) + "m"
		requests["cpu"] = cpu
		limits["cpu"] = cpu
	}
	if spec.MemoryMebibytes > 0 {
		memory := strconv.FormatInt(spec.MemoryMebibytes, 10) + "Mi"
		requests["memory"] = memory
		limits["memory"] = memory
	}
	if len(requests) == 0 {
		return nil
	}

	return map[string]any{
		"requests": requests,
		"limits":   limits,
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

	servicePorts := make([]map[string]any, 0, len(app.Spec.Ports))
	for _, port := range app.Spec.Ports {
		servicePorts = append(servicePorts, map[string]any{
			"name":       "tcp-" + strconv.Itoa(port),
			"port":       port,
			"targetPort": port,
			"protocol":   "TCP",
		})
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

func buildComposeServiceAliasObject(namespace string, app model.App) map[string]any {
	if app.Source == nil || !model.AppHasClusterService(app.Spec) {
		return nil
	}
	composeService := strings.TrimSpace(app.Source.ComposeService)
	aliasName := ComposeServiceAliasName(app.ProjectID, composeService)
	if aliasName == "" || aliasName == RuntimeAppResourceName(app) {
		return nil
	}
	serviceFQDN := serviceFQDN(namespace, RuntimeAppResourceName(app))
	if serviceFQDN == "" {
		return nil
	}

	servicePorts := make([]map[string]any, 0, len(app.Spec.Ports))
	for _, port := range app.Spec.Ports {
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
			"type":         "ExternalName",
			"externalName": serviceFQDN,
			"ports":        servicePorts,
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
	serviceFQDN := serviceFQDN(namespace, RuntimeAppResourceName(app))
	if serviceFQDN == "" {
		return nil
	}

	servicePorts := make([]map[string]any, 0, len(app.Spec.Ports))
	for _, port := range app.Spec.Ports {
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
			"type":         "ExternalName",
			"externalName": serviceFQDN,
			"ports":        servicePorts,
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
	if spec.SynchronousReplicas == 0 && spec.Instances > 1 {
		spec.SynchronousReplicas = defaultPostgresSynchronousReplicas
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
			"trigger": map[string]any{
				"schedule": defaultWorkspaceReplicationSchedule,
			},
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
	mounts := make([]map[string]any, 0, len(spec.Mounts))
	for _, mount := range spec.Mounts {
		mounts = append(mounts, map[string]any{
			"name":      workspaceVolumeName,
			"mountPath": mount.Path,
			"subPath":   model.AppPersistentStorageMountSubPath(mount),
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
			persistentStorageRootPath,
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
	return buildPersistentStoragePVCObject(namespace, app, labels, spec.StorageSize, spec.StorageClassName)
}

func buildPersistentStoragePVCObject(namespace string, app model.App, labels map[string]string, storageSize, storageClassName string) map[string]any {
	pvcSpec := map[string]any{
		"accessModes": []string{"ReadWriteOnce"},
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
			"name":      WorkspacePVCName(app),
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
