package api

import (
	"context"
	"strings"
	"time"
)

type readinessCheckResult struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

func (s *Server) readinessCheckResults(ctx context.Context) map[string]readinessCheckResult {
	results := map[string]readinessCheckResult{}

	storeCtx, cancelStore := context.WithTimeout(ctx, 3*time.Second)
	defer cancelStore()
	if err := s.store.CheckReadiness(storeCtx); err != nil {
		results["store"] = readinessCheckResult{
			Status:  "error",
			Message: err.Error(),
		}
	} else {
		results["store"] = readinessCheckResult{Status: "ok"}
	}

	namespace := strings.TrimSpace(s.controlPlaneNamespace)
	if namespace == "" {
		results["kubernetes_api"] = readinessCheckResult{
			Status:  "skipped",
			Message: "control plane namespace is not configured",
		}
		return results
	}

	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		results["kubernetes_api"] = readinessCheckResult{
			Status:  "error",
			Message: err.Error(),
		}
		return results
	}

	kubeCtx, cancelKube := context.WithTimeout(ctx, 3*time.Second)
	defer cancelKube()
	if _, err := client.listDeployments(kubeCtx, namespace); err != nil {
		results["kubernetes_api"] = readinessCheckResult{
			Status:  "error",
			Message: err.Error(),
		}
		return results
	}

	results["kubernetes_api"] = readinessCheckResult{Status: "ok"}
	return results
}

func readinessHasFailure(results map[string]readinessCheckResult) bool {
	for _, result := range results {
		if result.Status == "error" {
			return true
		}
	}
	return false
}
