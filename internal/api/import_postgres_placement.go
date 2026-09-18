package api

import (
	"context"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type importStorageCapacity struct {
	StorageClassName string                `json:"storageClassName"`
	Capacity         *resource.Quantity    `json:"capacity"`
	NodeTopology     *metav1.LabelSelector `json:"nodeTopology"`
}

// New imports may choose among platform-approved Postgres classes when an
// explicit physical target cannot provision the configured default. Existing
// databases and explicit storage intent never pass through this defaulting path.
func (s *Server) applyNewImportedPostgresPlacement(spec *model.AppPostgresSpec, appRuntimeID string) {
	if spec == nil || strings.TrimSpace(spec.StorageClassName) != "" {
		return
	}
	runtimeID := firstNonEmpty(strings.TrimSpace(spec.RuntimeID), strings.TrimSpace(appRuntimeID))
	if runtimeID == "" {
		return
	}
	target, err := s.store.GetRuntime(runtimeID)
	if err != nil || strings.TrimSpace(target.ClusterNodeName) == "" {
		return
	}
	factory := s.newClusterNodeClient
	if factory == nil {
		return
	}
	client, err := factory()
	if err != nil {
		return
	}
	defer client.closeIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var node kubeNode
	if err := client.doJSON(ctx, http.MethodGet, "/api/v1/nodes/"+url.PathEscape(target.ClusterNodeName), &node); err != nil {
		return
	}
	var capacities struct {
		Items []importStorageCapacity `json:"items"`
	}
	if err := client.doJSON(ctx, http.MethodGet, "/apis/storage.k8s.io/v1/csistoragecapacities", &capacities); err != nil {
		s.log.Printf("observe imported postgres capacity for runtime %s: %v", runtimeID, err)
		return
	}
	var classes struct {
		Items []struct {
			Metadata struct {
				Name   string            `json:"name"`
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := client.doJSON(ctx, http.MethodGet, "/apis/storage.k8s.io/v1/storageclasses", &classes); err != nil {
		return
	}
	approved := map[string]bool{}
	for _, class := range classes.Items {
		if class.Metadata.Labels["fugue.pro/managed-postgres"] == "true" {
			approved[class.Metadata.Name] = true
		}
	}
	size, err := resource.ParseQuantity(firstNonEmpty(strings.TrimSpace(spec.StorageSize), model.DefaultManagedPostgresStorageSize))
	if err != nil {
		return
	}
	spec.StorageClassName = selectImportedPostgresStorageClass(s.effectiveDefaultManagedPostgresStorageClassName(), node.Metadata.Labels, capacities.Items, approved, size)
}

func selectImportedPostgresStorageClass(preferred string, nodeLabels map[string]string, capacities []importStorageCapacity, approved map[string]bool, size resource.Quantity) string {
	seen := map[string]bool{}
	fits := map[string]bool{}
	for _, capacity := range capacities {
		if capacity.Capacity == nil || capacity.NodeTopology == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(capacity.NodeTopology)
		if err != nil || !selector.Matches(labels.Set(nodeLabels)) {
			continue
		}
		seen[capacity.StorageClassName] = true
		if capacity.Capacity.Cmp(size) >= 0 {
			fits[capacity.StorageClassName] = true
		}
	}
	// Unknown capacity is not proof of exhaustion, and must not change intent.
	if !seen[preferred] || fits[preferred] {
		return preferred
	}
	candidates := []string{}
	for class, allowed := range approved {
		if allowed && fits[class] {
			candidates = append(candidates, class)
		}
	}
	sort.Strings(candidates)
	if len(candidates) > 0 {
		return candidates[0]
	}
	return preferred
}
