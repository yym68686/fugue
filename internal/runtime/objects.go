package runtime

import (
	"path"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/model"
)

const (
	defaultPostgresImage = "postgres:17.6-alpine"
	defaultWaitImage     = "busybox:1.36"
)

func buildAppObjects(app model.App, scheduling SchedulingConstraints) []map[string]any {
	namespace := NamespaceForTenant(app.TenantID)
	appName := sanitizeName(app.Name)
	objects := []map[string]any{
		buildNamespaceObject(namespace),
	}

	if len(app.Spec.Files) > 0 {
		objects = append(objects, buildAppFilesSecretObject(namespace, appName, app.Spec.Files))
	}

	if postgres := normalizedPostgresSpec(namespace, app); postgres != nil {
		objects = append(objects,
			buildPostgresSecretObject(namespace, appName, *postgres),
			buildPostgresServiceObject(namespace, appName, *postgres),
			buildPostgresDeploymentObject(namespace, appName, *postgres, scheduling),
		)
	}

	labels := appLabels(appName)
	objects = append(objects,
		buildAppDeploymentObject(namespace, app, labels, scheduling),
		buildAppServiceObject(namespace, app, labels),
	)
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

func appLabels(appName string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       appName,
		"app.kubernetes.io/managed-by": "fugue",
	}
}

func postgresLabels(appName string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       postgresResourceName(appName),
		"app.kubernetes.io/component":  "postgres",
		"app.kubernetes.io/managed-by": "fugue",
	}
}

func buildAppFilesSecretObject(namespace, appName string, files []model.AppFile) map[string]any {
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
			"labels":    appLabels(appName),
		},
		"type":       "Opaque",
		"stringData": stringData,
	}
}

func buildPostgresSecretObject(namespace, appName string, spec model.AppPostgresSpec) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":      postgresSecretName(appName),
			"namespace": namespace,
			"labels":    postgresLabels(appName),
		},
		"type": "Opaque",
		"stringData": map[string]string{
			"POSTGRES_DB":       spec.Database,
			"POSTGRES_USER":     spec.User,
			"POSTGRES_PASSWORD": spec.Password,
		},
	}
}

func buildPostgresServiceObject(namespace, appName string, spec model.AppPostgresSpec) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      spec.ServiceName,
			"namespace": namespace,
			"labels":    postgresLabels(appName),
		},
		"spec": map[string]any{
			"selector": postgresLabels(appName),
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

func buildPostgresDeploymentObject(namespace, appName string, spec model.AppPostgresSpec, scheduling SchedulingConstraints) map[string]any {
	labels := postgresLabels(appName)
	podSpec := map[string]any{
		"initContainers": []map[string]any{
			{
				"name":  "init-data-dir",
				"image": spec.Image,
				"command": []string{
					"sh",
					"-c",
					"mkdir -p /var/lib/postgresql/data && chown -R $(id -u postgres):$(id -g postgres) /var/lib/postgresql/data",
				},
				"securityContext": map[string]any{
					"runAsUser": 0,
				},
				"volumeMounts": []map[string]any{
					{
						"name":      "postgres-data",
						"mountPath": "/var/lib/postgresql/data",
					},
				},
			},
		},
		"containers": []map[string]any{
			{
				"name":  "postgres",
				"image": spec.Image,
				"env": []map[string]any{
					{
						"name": "POSTGRES_DB",
						"valueFrom": map[string]any{
							"secretKeyRef": map[string]any{
								"name": postgresSecretName(appName),
								"key":  "POSTGRES_DB",
							},
						},
					},
					{
						"name": "POSTGRES_USER",
						"valueFrom": map[string]any{
							"secretKeyRef": map[string]any{
								"name": postgresSecretName(appName),
								"key":  "POSTGRES_USER",
							},
						},
					},
					{
						"name": "POSTGRES_PASSWORD",
						"valueFrom": map[string]any{
							"secretKeyRef": map[string]any{
								"name": postgresSecretName(appName),
								"key":  "POSTGRES_PASSWORD",
							},
						},
					},
				},
				"ports": []map[string]any{
					{
						"containerPort": 5432,
						"protocol":      "TCP",
					},
				},
				"volumeMounts": []map[string]any{
					{
						"name":      "postgres-data",
						"mountPath": "/var/lib/postgresql/data",
					},
				},
			},
		},
		"volumes": []map[string]any{
			{
				"name": "postgres-data",
				"hostPath": map[string]any{
					"path": spec.StoragePath,
					"type": "DirectoryOrCreate",
				},
			},
		},
	}
	applyScheduling(&podSpec, scheduling)

	return map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      postgresResourceName(appName),
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"replicas": 1,
			"strategy": map[string]any{
				"type": "Recreate",
			},
			"selector": map[string]any{
				"matchLabels": labels,
			},
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": labels,
				},
				"spec": podSpec,
			},
		},
	}
}

func buildAppDeploymentObject(namespace string, app model.App, labels map[string]string, scheduling SchedulingConstraints) map[string]any {
	container := map[string]any{
		"name":  sanitizeName(app.Name),
		"image": app.Spec.Image,
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
	}
	if len(app.Spec.Env) > 0 {
		container["env"] = buildEnvObjects(app.Spec.Env)
	}

	volumeMounts := []map[string]any{}
	volumes := []map[string]any{}
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
	if len(volumeMounts) > 0 {
		container["volumeMounts"] = volumeMounts
	}

	podSpec := map[string]any{
		"containers": []map[string]any{container},
	}
	if len(volumes) > 0 {
		podSpec["volumes"] = volumes
	}

	if postgres := normalizedPostgresSpec(namespace, app); postgres != nil {
		podSpec["initContainers"] = []map[string]any{
			{
				"name":  "wait-postgres",
				"image": defaultWaitImage,
				"command": []string{
					"sh",
					"-c",
					"until nc -z " + postgres.ServiceName + " 5432; do sleep 2; done",
				},
			},
		}
	}
	applyScheduling(&podSpec, scheduling)

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
			"selector": map[string]any{
				"matchLabels": labels,
			},
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": labels,
				},
				"spec": podSpec,
			},
		},
	}
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

func normalizedPostgresSpec(namespace string, app model.App) *model.AppPostgresSpec {
	if app.Spec.Postgres == nil {
		return nil
	}
	spec := *app.Spec.Postgres
	if strings.TrimSpace(spec.Image) == "" {
		spec.Image = defaultPostgresImage
	}
	if strings.TrimSpace(spec.Database) == "" {
		spec.Database = sanitizeName(app.Name)
	}
	if strings.TrimSpace(spec.User) == "" {
		spec.User = "postgres"
	}
	if strings.TrimSpace(spec.ServiceName) == "" {
		spec.ServiceName = postgresResourceName(sanitizeName(app.Name))
	}
	if strings.TrimSpace(spec.StoragePath) == "" {
		spec.StoragePath = path.Join("/var/lib/fugue/tenant-data", namespace, sanitizeName(app.Name), "postgres")
	}
	return &spec
}

func appFilesSecretName(appName string) string {
	return appName + "-files"
}

func postgresResourceName(appName string) string {
	return appName + "-postgres"
}

func postgresSecretName(appName string) string {
	return appName + "-pgsec"
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
