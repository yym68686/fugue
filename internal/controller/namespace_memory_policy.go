package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
)

// The ConfigMap is written by the independent configuration lane. A code
// rollback does not remove namespace admission policy or change serving specs.
func (s *Service) ensureNamespaceMemoryPolicy(ctx context.Context, client *kubeClient, namespace string) error {
	controlNamespace := strings.TrimSpace(s.Config.ControlPlaneNamespace)
	if controlNamespace == "" {
		return nil
	}
	var config struct {
		Data map[string]string `json:"data"`
	}
	status, err := client.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+url.PathEscape(controlNamespace)+"/configmaps/fugue-workload-memory-policy", nil, &config)
	if status == http.StatusNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	var policy struct {
		APIVersion           string   `json:"apiVersion"`
		Kind                 string   `json:"kind"`
		NamespacePrefixes    []string `json:"namespacePrefixes"`
		DefaultMemoryRequest string   `json:"defaultMemoryRequest"`
	}
	if err := json.Unmarshal([]byte(config.Data["policy.json"]), &policy); err != nil {
		return err
	}
	if policy.APIVersion != "constraints.fugue.dev/v1" || policy.Kind != "WorkloadMemoryPolicy" {
		return fmt.Errorf("unsupported workload memory policy")
	}
	selected := false
	for _, prefix := range policy.NamespacePrefixes {
		if prefix != "" && strings.HasPrefix(namespace, prefix) {
			selected = true
		}
	}
	if !selected {
		return nil
	}
	quantity, err := resource.ParseQuantity(policy.DefaultMemoryRequest)
	if err != nil || quantity.Sign() <= 0 {
		return fmt.Errorf("invalid default memory request")
	}
	path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/limitranges"
	var existing struct {
		Items []struct {
			Spec struct {
				Limits []struct {
					DefaultRequest map[string]string `json:"defaultRequest"`
					Default        map[string]string `json:"default"`
				} `json:"limits"`
			} `json:"spec"`
		} `json:"items"`
	}
	if _, err = client.doJSON(ctx, http.MethodGet, path, nil, &existing); err != nil {
		return err
	}
	for _, item := range existing.Items {
		for _, rule := range item.Spec.Limits {
			if rule.DefaultRequest["memory"] != "" || rule.Default["memory"] != "" {
				return nil
			}
		}
	}
	object := map[string]any{"apiVersion": "v1", "kind": "LimitRange", "metadata": map[string]any{"name": "fugue-default-memory-request", "namespace": namespace, "labels": map[string]string{"app.kubernetes.io/managed-by": "fugue-workload-memory-policy"}}, "spec": map[string]any{"limits": []map[string]any{{"type": "Container", "defaultRequest": map[string]string{"memory": policy.DefaultMemoryRequest}}}}}
	status, err = client.doJSON(ctx, http.MethodPost, path, object, nil)
	if status == http.StatusConflict {
		return nil
	}
	return err
}
