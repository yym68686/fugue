package controller

import (
	"context"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/sourceimport"
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

	for _, resource := range []string{"deployments", "statefulsets", "daemonsets"} {
		workloads, listErr := client.listWorkloads(ctx, resource)
		if listErr != nil {
			if s.Logger != nil {
				s.Logger.Printf("skip cluster live managed image reference scan for %s: %v", resource, listErr)
			}
			continue
		}
		for _, workload := range workloads {
			for imageRef := range s.liveManagedImageRefsFromWorkload(workload) {
				refs[imageRef] = struct{}{}
			}
		}
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
	return s.liveManagedImageRefsFromWorkload(deployment)
}

func (s *Service) liveManagedImageRefsFromWorkload(workload map[string]any) map[string]struct{} {
	refs := make(map[string]struct{})
	podSpec, ok := nestedMap(workload, "spec", "template", "spec")
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

func (s *Service) managedImageDigestInUse(ctx context.Context, candidate string, refs map[string]struct{}) (bool, error) {
	liveDigests := make(map[string]struct{})
	for ref := range refs {
		if digest := imageDigest(ref); digest != "" {
			liveDigests[digest] = struct{}{}
		}
	}
	if len(liveDigests) == 0 {
		return false, nil
	}

	candidateDigest := imageDigest(candidate)
	if candidateDigest == "" {
		resolve := s.resolveManagedImageDigestRef
		if resolve == nil {
			resolve = sourceimport.ResolveRemoteImageDigestRef
		}
		resolved, err := resolve(ctx, candidate)
		if err != nil {
			return false, fmt.Errorf("resolve candidate image digest %s: %w", candidate, err)
		}
		candidateDigest = imageDigest(resolved)
	}
	if candidateDigest == "" {
		return false, fmt.Errorf("resolve candidate image digest %s returned no digest", candidate)
	}
	_, inUse := liveDigests[candidateDigest]
	return inUse, nil
}

func imageDigest(imageRef string) string {
	imageRef = strings.ToLower(strings.TrimSpace(imageRef))
	index := strings.LastIndex(imageRef, "@sha256:")
	if index < 0 {
		return ""
	}
	digest := imageRef[index+1:]
	if len(digest) != len("sha256:")+64 {
		return ""
	}
	for _, r := range digest[len("sha256:"):] {
		if r < '0' || r > '9' && r < 'a' || r > 'f' {
			return ""
		}
	}
	return digest
}
