package controller

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

// completedBuilderImageDestination resolves location only. The caller must
// still verify the complete manifest/blob graph before publishing a replica.
func (s *Service) completedBuilderImageDestination(ctx context.Context, app model.App, op model.Operation, result sourceimport.GitHubImportResult) (importImageDestination, error) {
	if app.ID == "" || app.TenantID == "" || op.ID == "" || strings.TrimSpace(result.BuildJobName) == "" {
		return importImageDestination{}, fmt.Errorf("build identity is incomplete")
	}
	// Only node-local pushes can be located from the executing Pod. An explicit
	// remote registry must never be attributed to the builder's node.
	expected := cacheEndpointImageRef("http://"+strings.TrimPrefix(strings.TrimPrefix(s.builderRegistryPushBase, "http://"), "https://"), result.ImageRef)
	host := endpointHost(s.builderRegistryPushBase)
	ip := net.ParseIP(host)
	if (host != "localhost" && (ip == nil || !ip.IsLoopback())) || expected == "" || result.DestinationImageRef != expected {
		return importImageDestination{}, fmt.Errorf("build output does not identify the configured node-local registry")
	}
	client, err := s.kubeClient()
	if err != nil {
		return importImageDestination{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	labels := map[string]string{"fugue.pro/operation-id": op.ID, "fugue.pro/app-id": app.ID, "fugue.pro/tenant-id": app.TenantID}
	var job struct {
		Metadata struct {
			UID    string            `json:"uid"`
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
		Status struct {
			Succeeded int `json:"succeeded"`
		} `json:"status"`
	}
	if _, err := client.doJSON(ctx, http.MethodGet, "/apis/batch/v1/namespaces/"+client.effectiveNamespace("")+"/jobs/"+url.PathEscape(result.BuildJobName), nil, &job); err != nil {
		return importImageDestination{}, err
	}
	if job.Metadata.UID == "" || job.Status.Succeeded < 1 {
		return importImageDestination{}, fmt.Errorf("build job has no successful identity")
	}
	for key, value := range labels {
		if job.Metadata.Labels[key] != value {
			return importImageDestination{}, fmt.Errorf("build job ownership does not match import")
		}
	}
	var pods struct {
		Items []struct {
			Metadata struct {
				OwnerReferences []struct {
					UID        string `json:"uid"`
					Kind       string `json:"kind"`
					Controller bool   `json:"controller"`
				} `json:"ownerReferences"`
			} `json:"metadata"`
			Spec struct {
				NodeName string `json:"nodeName"`
			} `json:"spec"`
			Status struct {
				Phase string `json:"phase"`
			} `json:"status"`
		} `json:"items"`
	}
	selector := url.Values{"labelSelector": {"job-name=" + result.BuildJobName}}
	if _, err := client.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+client.effectiveNamespace("")+"/pods?"+selector.Encode(), nil, &pods); err != nil {
		return importImageDestination{}, err
	}
	node := ""
	for _, pod := range pods.Items {
		owned := false
		for _, owner := range pod.Metadata.OwnerReferences {
			owned = owned || (owner.Controller && owner.Kind == "Job" && owner.UID == job.Metadata.UID)
		}
		if !owned || pod.Status.Phase != "Succeeded" || strings.TrimSpace(pod.Spec.NodeName) == "" {
			continue
		}
		if node != "" && node != pod.Spec.NodeName {
			return importImageDestination{}, fmt.Errorf("successful build pods identify multiple nodes")
		}
		node = pod.Spec.NodeName
	}
	if node == "" {
		return importImageDestination{}, fmt.Errorf("no successful pod owned by the current build job")
	}
	runtimeObj, found := s.runtimeForClusterNode(ctx, node)
	if !found {
		return importImageDestination{}, fmt.Errorf("completed builder node has no active image-cache runtime")
	}
	return s.importImageDestinationForRuntime(runtimeObj), nil
}
