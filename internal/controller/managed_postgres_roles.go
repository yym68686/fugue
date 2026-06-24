package controller

import (
	"context"
	"fmt"
	"strings"

	"fugue/internal/runtime"
)

func reconcileCloudNativePGManagedRoles(ctx context.Context, client *kubeClient, defaultNamespace string, objects []map[string]any) error {
	for _, obj := range objects {
		if strings.TrimSpace(objectStringField(obj, "apiVersion")) != runtime.CloudNativePGAPIVersion ||
			strings.TrimSpace(objectStringField(obj, "kind")) != runtime.CloudNativePGClusterKind {
			continue
		}
		desiredRoles := cloudNativePGManagedRolesFromObject(obj)
		if len(desiredRoles) == 0 {
			continue
		}
		name, namespace := objectNameAndNamespace(defaultNamespace, obj)
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("cloudnativepg cluster object missing metadata.name")
		}
		namespace = client.effectiveNamespace(namespace)
		current, found, err := client.getRawNamespacedObject(ctx, cloudNativePGClusterAPIPath(namespace, name))
		if err != nil {
			return fmt.Errorf("read cloudnativepg cluster %s/%s: %w", namespace, name, err)
		}
		if !found {
			continue
		}
		currentRoles := cloudNativePGManagedRolesFromObject(current)
		mergedRoles, changed := mergeCloudNativePGManagedRoles(currentRoles, desiredRoles)
		if !changed {
			continue
		}
		if err := client.patchCloudNativePGManagedRoles(ctx, namespace, name, mergedRoles); err != nil {
			return fmt.Errorf("patch cloudnativepg cluster %s/%s managed roles: %w", namespace, name, err)
		}
	}
	return nil
}

func cloudNativePGManagedRolesFromObject(obj map[string]any) []map[string]any {
	spec := normalizeKubeMap(obj["spec"])
	if spec == nil {
		return nil
	}
	managed := normalizeKubeMap(spec["managed"])
	if managed == nil {
		return nil
	}
	return normalizeCloudNativePGManagedRoles(managed["roles"])
}

func normalizeCloudNativePGManagedRoles(value any) []map[string]any {
	normalized := normalizeKubeValue(value)
	items, ok := normalized.([]any)
	if !ok {
		return nil
	}
	roles := make([]map[string]any, 0, len(items))
	for _, item := range items {
		role, ok := item.(map[string]any)
		if !ok {
			continue
		}
		roles = append(roles, cloneKubeMap(role))
	}
	return roles
}

func mergeCloudNativePGManagedRoles(currentRoles, desiredRoles []map[string]any) ([]map[string]any, bool) {
	if len(desiredRoles) == 0 {
		return cloneCloudNativePGManagedRoles(currentRoles), false
	}
	merged := cloneCloudNativePGManagedRoles(currentRoles)
	indexByName := make(map[string]int, len(merged))
	for index, role := range merged {
		name := managedRoleName(role)
		if name == "" {
			continue
		}
		indexByName[name] = index
	}

	changed := false
	for _, desiredRole := range desiredRoles {
		name := managedRoleName(desiredRole)
		if name == "" {
			continue
		}
		desiredCopy := cloneKubeMap(desiredRole)
		if index, ok := indexByName[name]; ok {
			if !normalizedKubeValueEqual(merged[index], desiredCopy) {
				merged[index] = desiredCopy
				changed = true
			}
			continue
		}
		indexByName[name] = len(merged)
		merged = append(merged, desiredCopy)
		changed = true
	}
	return merged, changed
}

func cloneCloudNativePGManagedRoles(roles []map[string]any) []map[string]any {
	if len(roles) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(roles))
	for _, role := range roles {
		out = append(out, cloneKubeMap(role))
	}
	return out
}

func cloneKubeMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	normalized := normalizeKubeMap(in)
	if normalized == nil {
		normalized = in
	}
	out := make(map[string]any, len(normalized))
	for key, value := range normalized {
		out[key] = value
	}
	return out
}

func managedRoleName(role map[string]any) string {
	if role == nil {
		return ""
	}
	value, _ := role["name"].(string)
	return strings.TrimSpace(value)
}
