package controller

import (
	"context"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func (s *Service) liveManagedImageRefSet(ctx context.Context, apps []model.App) map[string]struct{} {
	refs := make(map[string]struct{})
	if s == nil || strings.TrimSpace(s.registryPushBase) == "" {
		return refs
	}

	client, err := s.kubeClient()
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("skip live managed image reference scan: %v", err)
		}
		return refs
	}

	for _, app := range apps {
		if strings.TrimSpace(app.ID) == "" || strings.TrimSpace(app.TenantID) == "" {
			continue
		}
		namespace := runtime.NamespaceForTenant(app.TenantID)
		name := runtime.RuntimeAppResourceName(app)
		deployment, found, err := client.getRawDeployment(ctx, namespace, name)
		if err != nil {
			if s.Logger != nil {
				s.Logger.Printf("skip live managed image reference scan for deployment %s/%s: %v", namespace, name, err)
			}
			continue
		}
		if !found {
			continue
		}
		for imageRef := range s.liveManagedImageRefsFromDeployment(deployment) {
			refs[imageRef] = struct{}{}
		}
	}
	return refs
}

func (s *Service) liveManagedImageRefsFromDeployment(deployment map[string]any) map[string]struct{} {
	refs := make(map[string]struct{})
	podSpec, ok := nestedMap(deployment, "spec", "template", "spec")
	if !ok {
		return refs
	}
	for _, containerField := range []string{"initContainers", "containers"} {
		for _, container := range mapSlice(podSpec[containerField]) {
			if ref := s.managedImageRefFromRuntimeValue(fmt.Sprint(container["image"])); ref != "" {
				refs[ref] = struct{}{}
			}
			for _, env := range mapSlice(container["env"]) {
				if ref := s.managedImageRefFromRuntimeValue(fmt.Sprint(env["value"])); ref != "" {
					refs[ref] = struct{}{}
				}
			}
		}
	}
	return refs
}

func (s *Service) managedImageRefFromRuntimeValue(value string) string {
	if s == nil {
		return ""
	}
	if ref := managedRegistryRefFromRuntimeImageRef(value, s.registryPushBase, s.registryPullBase); ref != "" {
		return ref
	}
	return managedImageRefFromFugueAppsPath(value, s.registryPushBase)
}
