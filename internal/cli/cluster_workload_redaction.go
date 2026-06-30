package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

func sanitizeClusterWorkloadForOutput(workload model.ClusterWorkloadDetail, redact bool) model.ClusterWorkloadDetail {
	if !redact {
		return workload
	}
	out := workload
	out.Labels = redactDiagnosticStringMap(workload.Labels)
	out.Annotations = redactDiagnosticStringMap(workload.Annotations)
	out.NodeSelector = redactDiagnosticStringMap(workload.NodeSelector)
	out.Tolerations = redactDiagnosticStringSlice(workload.Tolerations)
	out.Conditions = sanitizeClusterWorkloadConditions(workload.Conditions)
	out.Pods = sanitizeClusterPodsForOutput(workload.Pods)
	out.Manifest = redactKubernetesDiagnosticManifest(workload.Manifest)
	return out
}

func sanitizeClusterWorkloadConditions(conditions []model.ClusterWorkloadCondition) []model.ClusterWorkloadCondition {
	if len(conditions) == 0 {
		return nil
	}
	out := make([]model.ClusterWorkloadCondition, len(conditions))
	copy(out, conditions)
	for index := range out {
		out[index].Message = redactDiagnosticString(out[index].Message)
	}
	return out
}

func sanitizeClusterPodsForOutput(pods []model.ClusterPod) []model.ClusterPod {
	if len(pods) == 0 {
		return nil
	}
	out := make([]model.ClusterPod, len(pods))
	for index, pod := range pods {
		out[index] = pod
		out[index].Labels = redactDiagnosticStringMap(pod.Labels)
		if len(pod.Containers) > 0 {
			out[index].Containers = make([]model.ClusterPodContainer, len(pod.Containers))
			copy(out[index].Containers, pod.Containers)
			for containerIndex := range out[index].Containers {
				out[index].Containers[containerIndex].Message = redactDiagnosticString(out[index].Containers[containerIndex].Message)
			}
		}
	}
	return out
}

func redactKubernetesDiagnosticManifest(manifest map[string]any) map[string]any {
	if len(manifest) == 0 {
		return nil
	}
	redacted, ok := redactKubernetesDiagnosticValue(manifest, "", false).(map[string]any)
	if !ok {
		return nil
	}
	return redacted
}

func redactKubernetesDiagnosticValue(value any, key string, force bool) any {
	if force {
		return redactDiagnosticJSONSecretValue(value)
	}
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for childKey, child := range typed {
			normalizedKey := normalizeDiagnosticKey(childKey)
			switch {
			case diagnosticKeyLooksSensitive(childKey):
				out[childKey] = redactDiagnosticJSONSecretValue(child)
			case normalizedKey == "env":
				out[childKey] = redactKubernetesEnvValue(child)
			case normalizedKey == "envfrom":
				out[childKey] = redactDiagnosticJSONSecretValue(child)
			case diagnosticKeyRedactsChildren(childKey):
				out[childKey] = redactKubernetesDiagnosticValue(child, childKey, true)
			default:
				out[childKey] = redactKubernetesDiagnosticValue(child, childKey, false)
			}
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, child := range typed {
			if normalizeDiagnosticKey(key) == "env" {
				out[index] = redactKubernetesEnvEntry(child)
				continue
			}
			out[index] = redactKubernetesDiagnosticValue(child, key, false)
		}
		return out
	case string:
		return redactDiagnosticString(typed)
	default:
		return typed
	}
}

func redactKubernetesEnvValue(value any) any {
	switch typed := value.(type) {
	case []any:
		out := make([]any, len(typed))
		for index, child := range typed {
			out[index] = redactKubernetesEnvEntry(child)
		}
		return out
	default:
		return redactDiagnosticJSONSecretValue(value)
	}
}

func redactKubernetesEnvEntry(value any) any {
	entry, ok := value.(map[string]any)
	if !ok {
		return redactDiagnosticJSONSecretValue(value)
	}
	out := make(map[string]any, len(entry))
	for key, child := range entry {
		normalizedKey := normalizeDiagnosticKey(key)
		switch normalizedKey {
		case "name":
			out[key] = redactDiagnosticString(valueAsString(child))
		case "value", "valuefrom":
			out[key] = redactDiagnosticJSONSecretValue(child)
		default:
			if diagnosticKeyLooksSensitive(key) {
				out[key] = redactDiagnosticJSONSecretValue(child)
				continue
			}
			out[key] = redactKubernetesDiagnosticValue(child, key, false)
		}
	}
	return out
}

func valueAsString(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func normalizeDiagnosticKey(key string) string {
	normalized := strings.ToLower(strings.TrimSpace(key))
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	return normalized
}
