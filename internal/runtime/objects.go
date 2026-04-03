package runtime

import (
	"crypto/sha256"
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
	AppWorkspaceContainerName           = "fugue-workspace"
	workspaceVolumeName                 = "app-workspace"
	workspaceSidecarName                = AppWorkspaceContainerName

	CloudNativePGAPIVersion           = "postgresql.cnpg.io/v1"
	CloudNativePGClusterKind          = "Cluster"
	VolSyncAPIVersion                 = "volsync.backube/v1alpha1"
	VolSyncReplicationSourceKind      = "ReplicationSource"
	VolSyncReplicationDestinationKind = "ReplicationDestination"
)

func buildAppObjects(app model.App, scheduling SchedulingConstraints) []map[string]any {
	return buildAppObjectsWithOwner(app, scheduling, nil)
}

func buildAppObjectsWithOwner(app model.App, scheduling SchedulingConstraints, ownerRef *OwnerReference) []map[string]any {
	namespace := NamespaceForTenant(app.TenantID)
	appName := sanitizeName(app.Name)
	postgresResources := managedPostgresResources(namespace, app)
	labels := appLabels(app)
	objects := []map[string]any{
		buildNamespaceObject(namespace),
	}

	if len(app.Spec.Files) > 0 {
		objects = append(objects, buildAppFilesSecretObject(namespace, appName, app.Spec.Files, labels))
	}

	if workspaceSpec := normalizeRuntimeAppWorkspaceSpec(app); workspaceSpec != nil {
		objects = append(objects,
			buildAppWorkspacePVCObject(namespace, app, labels, *workspaceSpec),
			buildWorkspaceReplicationDestinationObject(namespace, app, labels, *workspaceSpec),
		)
	}

	for _, postgres := range postgresResources {
		objects = append(objects, buildManagedPostgresObjects(namespace, postgres)...)
	}

	objects = append(objects,
		buildAppDeploymentObject(namespace, app, labels, scheduling, postgresResources),
		buildAppServiceObject(namespace, app, labels),
	)
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

func buildPostgresClusterObject(namespace, secretName, resourceName string, labels map[string]string, spec model.AppPostgresSpec) map[string]any {
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
	if spec.SynchronousReplicas > 0 && spec.Instances > 1 {
		clusterSpec["minSyncReplicas"] = spec.SynchronousReplicas
		clusterSpec["maxSyncReplicas"] = spec.SynchronousReplicas
	}

	return map[string]any{
		"apiVersion": CloudNativePGAPIVersion,
		"kind":       CloudNativePGClusterKind,
		"metadata": map[string]any{
			"name":      resourceName,
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": clusterSpec,
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
		buildPostgresClusterObject(namespace, resource.secretName, resource.resourceName, labels, resource.spec),
	}
}

func buildAppDeploymentObject(namespace string, app model.App, labels map[string]string, scheduling SchedulingConstraints, postgresResources []postgresRuntimeResource) map[string]any {
	container := map[string]any{
		"name":  sanitizeName(app.Name),
		"image": app.Spec.Image,
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
			mode := int32(0o600)
			if file.Mode > 0 {
				mode = file.Mode
			}
			items = append(items, map[string]any{
				"key":  key,
				"path": key,
				"mode": mode,
			})
			volumeMounts = append(volumeMounts, map[string]any{
				"name":      "app-files",
				"mountPath": strings.TrimSpace(file.Path),
				"subPath":   key,
				"readOnly":  true,
			})
		}
		volumes = append(volumes, map[string]any{
			"name": "app-files",
			"secret": map[string]any{
				"secretName": appFilesSecretName(sanitizeName(app.Name)),
				"items":      items,
			},
		})
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
			"name":      sanitizeName(app.Name),
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

type postgresRuntimeResource struct {
	baseName     string
	resourceName string
	secretName   string
	spec         model.AppPostgresSpec
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
	object := buildAppDeploymentObject(namespace, app, appLabels(app), scheduling, managedPostgresResources(namespace, app))
	return managedDeploymentRuntimeKey(object)
}

func ManagedBackingServiceDeployments(app model.App, scheduling SchedulingConstraints) []ManagedBackingServiceDeployment {
	namespace := NamespaceForTenant(app.TenantID)
	resources := managedPostgresResources(namespace, app)
	deployments := make([]ManagedBackingServiceDeployment, 0, len(resources))
	for _, resource := range resources {
		if strings.TrimSpace(resource.serviceID) == "" {
			continue
		}
		object := buildPostgresClusterObject(namespace, resource.secretName, resource.resourceName, postgresLabels(resource), resource.spec)
		deployments = append(deployments, ManagedBackingServiceDeployment{
			ServiceID:    resource.serviceID,
			ResourceName: resource.resourceName,
			ResourceKind: CloudNativePGClusterKind,
			RuntimeKey:   managedDeploymentRuntimeKey(object),
		})
	}
	return deployments
}

func managedPostgresResources(namespace string, app model.App) []postgresRuntimeResource {
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
	servicePorts := make([]map[string]any, 0, len(app.Spec.Ports))
	for _, port := range app.Spec.Ports {
		servicePorts = append(servicePorts, map[string]any{
			"name":       "tcp-" + strconv.Itoa(port),
			"port":       port,
			"targetPort": port,
			"protocol":   "TCP",
		})
	}
	if len(servicePorts) == 0 {
		servicePorts = append(servicePorts, map[string]any{
			"name":       "tcp-80",
			"port":       80,
			"targetPort": 80,
			"protocol":   "TCP",
		})
	}

	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      sanitizeName(app.Name),
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"selector": labels,
			"ports":    servicePorts,
		},
	}
}

func deploymentStrategy(app model.App) map[string]any {
	if normalizeRuntimeAppWorkspaceSpec(app) != nil {
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

func normalizeRuntimePostgresSpec(baseName string, spec model.AppPostgresSpec) model.AppPostgresSpec {
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
	if spec.Instances <= 0 {
		spec.Instances = defaultPostgresInstances
	}
	if spec.Instances < 1 {
		spec.Instances = 1
	}
	if spec.SynchronousReplicas < 0 {
		spec.SynchronousReplicas = 0
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
		"volumeMounts": []map[string]any{
			{
				"name":      workspaceVolumeName,
				"mountPath": spec.MountPath,
			},
		},
	}
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
