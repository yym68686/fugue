package controller

import (
	"context"
	"fmt"
	"strings"
)

func (s *Service) reconcileManagedAppPlatformEnvDrift(ctx context.Context, client *kubeClient, namespace string, childObjects []map[string]any) error {
	for _, desired := range childObjects {
		if !isDeploymentObject(desired) {
			continue
		}
		name, objectNamespace := objectNameAndNamespace(namespace, desired)
		if name == "" {
			continue
		}
		live, found, err := client.getRawDeployment(ctx, objectNamespace, name)
		if err != nil {
			return fmt.Errorf("read live deployment %s/%s for env drift: %w", objectNamespace, name, err)
		}
		if !found {
			continue
		}
		ops := deploymentPlatformEnvDriftRemoveOps(desired, live)
		if len(ops) == 0 {
			continue
		}
		if err := client.patchDeploymentJSONPatch(ctx, objectNamespace, name, ops); err != nil {
			return fmt.Errorf("prune platform env drift from deployment %s/%s: %w", objectNamespace, name, err)
		}
	}
	return nil
}

func isDeploymentObject(obj map[string]any) bool {
	apiVersion, _ := obj["apiVersion"].(string)
	kind, _ := obj["kind"].(string)
	return apiVersion == "apps/v1" && kind == "Deployment"
}

func deploymentPlatformEnvDriftRemoveOps(desired, live map[string]any) []map[string]string {
	desiredPodSpec, ok := nestedMap(desired, "spec", "template", "spec")
	if !ok {
		return nil
	}
	livePodSpec, ok := nestedMap(live, "spec", "template", "spec")
	if !ok {
		return nil
	}

	var ops []map[string]string
	for _, containerField := range []string{"containers", "initContainers"} {
		ops = append(ops, deploymentPlatformEnvContainerRemoveOps(desiredPodSpec, livePodSpec, containerField)...)
	}
	return ops
}

func deploymentPlatformEnvContainerRemoveOps(desiredPodSpec, livePodSpec map[string]any, containerField string) []map[string]string {
	desiredEnvByContainer := desiredPlatformEnvNamesByContainer(desiredPodSpec, containerField)
	if len(desiredEnvByContainer) == 0 {
		return nil
	}

	liveContainers := mapSlice(livePodSpec[containerField])
	if len(liveContainers) == 0 {
		return nil
	}

	var ops []map[string]string
	for containerIndex, liveContainer := range liveContainers {
		containerName := strings.TrimSpace(fmt.Sprint(liveContainer["name"]))
		desiredEnvNames, ok := desiredEnvByContainer[containerName]
		if !ok {
			continue
		}
		env := mapSlice(liveContainer["env"])
		for envIndex := len(env) - 1; envIndex >= 0; envIndex-- {
			envName := strings.TrimSpace(fmt.Sprint(env[envIndex]["name"]))
			if !isManagedPlatformEnvName(envName) {
				continue
			}
			if _, keep := desiredEnvNames[envName]; keep {
				continue
			}
			ops = append(ops, map[string]string{
				"op":   "remove",
				"path": fmt.Sprintf("/spec/template/spec/%s/%d/env/%d", containerField, containerIndex, envIndex),
			})
		}
	}
	return ops
}

func desiredPlatformEnvNamesByContainer(podSpec map[string]any, containerField string) map[string]map[string]struct{} {
	containers := mapSlice(podSpec[containerField])
	out := make(map[string]map[string]struct{}, len(containers))
	for _, container := range containers {
		containerName := strings.TrimSpace(fmt.Sprint(container["name"]))
		if containerName == "" {
			continue
		}
		envNames := make(map[string]struct{})
		for _, env := range mapSlice(container["env"]) {
			envName := strings.TrimSpace(fmt.Sprint(env["name"]))
			if envName == "" {
				continue
			}
			envNames[envName] = struct{}{}
		}
		out[containerName] = envNames
	}
	return out
}

func isManagedPlatformEnvName(name string) bool {
	name = strings.TrimSpace(name)
	return strings.HasPrefix(name, "ARGUS_") || strings.HasPrefix(name, "FUGUE_")
}
