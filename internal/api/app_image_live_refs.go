package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

type liveManagedImageReference struct {
	ImageRef string
	Source   string
}

func (s *Server) liveManagedImageRefSet(ctx context.Context, apps []model.App) map[string]struct{} {
	refs := make(map[string]struct{})
	for _, reference := range s.liveManagedImageReferences(ctx, apps) {
		refs[reference.ImageRef] = struct{}{}
	}
	return refs
}

func (s *Server) liveManagedImageReferences(ctx context.Context, apps []model.App) []liveManagedImageReference {
	if s == nil || strings.TrimSpace(s.registryPushBase) == "" {
		return nil
	}
	client, err := s.requireClusterNodeClient()
	if err != nil {
		if s.log != nil {
			s.log.Printf("skip live managed image reference scan: %v", err)
		}
		return nil
	}
	defer client.closeIdleConnections()

	var out []liveManagedImageReference
	if deployments, err := client.listDeploymentObjects(ctx); err != nil {
		if s.log != nil {
			s.log.Printf("skip cluster live managed image reference scan for deployments: %v", err)
		}
	} else {
		for _, deployment := range deployments {
			out = append(out, s.liveManagedImageReferencesFromDeployment(deployment)...)
		}
	}
	if statefulSets, err := client.listStatefulSetObjects(ctx); err != nil {
		if s.log != nil {
			s.log.Printf("skip cluster live managed image reference scan for statefulsets: %v", err)
		}
	} else {
		for _, statefulSet := range statefulSets {
			out = append(out, s.liveManagedImageReferencesFromPodSpec(
				"statefulset",
				statefulSet.Namespace,
				statefulSet.Name,
				statefulSet.Spec.Template.Spec,
			)...)
		}
	}
	if daemonSets, err := client.listDaemonSetObjects(ctx); err != nil {
		if s.log != nil {
			s.log.Printf("skip cluster live managed image reference scan for daemonsets: %v", err)
		}
	} else {
		for _, daemonSet := range daemonSets {
			out = append(out, s.liveManagedImageReferencesFromPodSpec(
				"daemonset",
				daemonSet.Namespace,
				daemonSet.Name,
				daemonSet.Spec.Template.Spec,
			)...)
		}
	}

	// Keep the per-app lookup as a fallback for clusters whose API credentials
	// can read named deployments but cannot list workloads cluster-wide.
	for _, app := range apps {
		if strings.TrimSpace(app.ID) == "" || strings.TrimSpace(app.TenantID) == "" {
			continue
		}
		namespace := runtime.NamespaceForTenant(app.TenantID)
		name := runtime.RuntimeAppResourceName(app)
		deployment, found, err := client.readDeploymentObject(ctx, namespace, name)
		if err != nil {
			if s.log != nil {
				s.log.Printf("skip live managed image reference scan for deployment %s/%s: %v", namespace, name, err)
			}
			continue
		}
		if !found {
			continue
		}
		out = append(out, s.liveManagedImageReferencesFromDeployment(deployment)...)
	}
	return out
}

func (s *Server) liveManagedImageReferencesFromDeployment(deployment appsv1.Deployment) []liveManagedImageReference {
	return s.liveManagedImageReferencesFromPodSpec(
		"deployment",
		deployment.Namespace,
		deployment.Name,
		deployment.Spec.Template.Spec,
	)
}

func (s *Server) liveManagedImageReferencesFromPodSpec(
	kind string,
	namespace string,
	name string,
	podSpec corev1.PodSpec,
) []liveManagedImageReference {
	var out []liveManagedImageReference
	appendContainer := func(container corev1.Container, init bool) {
		containerName := strings.TrimSpace(container.Name)
		if ref := s.managedImageRefFromRuntimeValue(container.Image); ref != "" {
			source := fmt.Sprintf("%s %s/%s container %s image", kind, namespace, name, containerName)
			if init {
				source = fmt.Sprintf("%s %s/%s init container %s image", kind, namespace, name, containerName)
			}
			out = append(out, liveManagedImageReference{ImageRef: ref, Source: source})
		}
		for _, env := range container.Env {
			if ref := s.managedImageRefFromRuntimeValue(env.Value); ref != "" {
				source := fmt.Sprintf("%s %s/%s container %s env %s", kind, namespace, name, containerName, strings.TrimSpace(env.Name))
				if init {
					source = fmt.Sprintf("%s %s/%s init container %s env %s", kind, namespace, name, containerName, strings.TrimSpace(env.Name))
				}
				out = append(out, liveManagedImageReference{ImageRef: ref, Source: source})
			}
		}
	}
	for _, container := range podSpec.InitContainers {
		appendContainer(container, true)
	}
	for _, container := range podSpec.Containers {
		appendContainer(container, false)
	}
	return out
}

func (s *Server) managedImageRefFromRuntimeValue(value string) string {
	if s == nil {
		return ""
	}
	if ref := s.registryRefFromRuntimeImageRef(value); ref != "" {
		return ref
	}
	return managedImageRefFromFugueAppsPath(value, s.registryPushBase)
}

func (c *clusterNodeClient) readDeploymentObject(ctx context.Context, namespace, name string) (appsv1.Deployment, bool, error) {
	namespace = strings.TrimSpace(namespace)
	name = strings.TrimSpace(name)
	if namespace == "" || name == "" {
		return appsv1.Deployment{}, false, fmt.Errorf("namespace and deployment name are required")
	}
	var deployment appsv1.Deployment
	if err := c.doJSON(ctx, http.MethodGet, clusterWorkloadAPIPath(namespace, "deployment", name), &deployment); err != nil {
		if strings.Contains(err.Error(), "status=404") {
			return appsv1.Deployment{}, false, nil
		}
		return appsv1.Deployment{}, false, err
	}
	return deployment, true, nil
}

func (c *clusterNodeClient) listDeploymentObjects(ctx context.Context) ([]appsv1.Deployment, error) {
	var workloads appsv1.DeploymentList
	if err := c.doJSON(ctx, http.MethodGet, "/apis/apps/v1/deployments", &workloads); err != nil {
		return nil, err
	}
	return workloads.Items, nil
}

func (c *clusterNodeClient) listStatefulSetObjects(ctx context.Context) ([]appsv1.StatefulSet, error) {
	var workloads appsv1.StatefulSetList
	if err := c.doJSON(ctx, http.MethodGet, "/apis/apps/v1/statefulsets", &workloads); err != nil {
		return nil, err
	}
	return workloads.Items, nil
}

func (c *clusterNodeClient) listDaemonSetObjects(ctx context.Context) ([]appsv1.DaemonSet, error) {
	var workloads appsv1.DaemonSetList
	if err := c.doJSON(ctx, http.MethodGet, "/apis/apps/v1/daemonsets", &workloads); err != nil {
		return nil, err
	}
	return workloads.Items, nil
}
