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

	var out []liveManagedImageReference
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
	var out []liveManagedImageReference
	appendContainer := func(container corev1.Container, init bool) {
		containerName := strings.TrimSpace(container.Name)
		if ref := s.managedImageRefFromRuntimeValue(container.Image); ref != "" {
			source := fmt.Sprintf("deployment %s/%s container %s image", deployment.Namespace, deployment.Name, containerName)
			if init {
				source = fmt.Sprintf("deployment %s/%s init container %s image", deployment.Namespace, deployment.Name, containerName)
			}
			out = append(out, liveManagedImageReference{ImageRef: ref, Source: source})
		}
		for _, env := range container.Env {
			if ref := s.managedImageRefFromRuntimeValue(env.Value); ref != "" {
				source := fmt.Sprintf("deployment %s/%s container %s env %s", deployment.Namespace, deployment.Name, containerName, strings.TrimSpace(env.Name))
				if init {
					source = fmt.Sprintf("deployment %s/%s init container %s env %s", deployment.Namespace, deployment.Name, containerName, strings.TrimSpace(env.Name))
				}
				out = append(out, liveManagedImageReference{ImageRef: ref, Source: source})
			}
		}
	}
	for _, container := range deployment.Spec.Template.Spec.InitContainers {
		appendContainer(container, true)
	}
	for _, container := range deployment.Spec.Template.Spec.Containers {
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
