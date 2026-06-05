package api

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	readinessKubernetesAPICacheTTL = 30 * time.Second
	readinessKubernetesAPITimeout  = time.Second
)

type readinessCheckResult struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`

	critical bool
}

func (s *Server) readinessCheckResults(ctx context.Context) map[string]readinessCheckResult {
	results := map[string]readinessCheckResult{}

	storeCtx, cancelStore := context.WithTimeout(ctx, 3*time.Second)
	defer cancelStore()
	if err := s.store.CheckReadiness(storeCtx); err != nil {
		results["store"] = readinessCheckResult{
			Status:   "error",
			Message:  err.Error(),
			critical: true,
		}
	} else {
		results["store"] = readinessCheckResult{Status: "ok"}
	}
	results["observability"] = s.observabilityReadinessResult()

	namespace := strings.TrimSpace(s.controlPlaneNamespace)
	if namespace == "" {
		results["kubernetes_api"] = readinessCheckResult{
			Status:  "skipped",
			Message: "control plane namespace is not configured",
		}
		return results
	}

	results["kubernetes_api"] = s.readinessKubernetesAPIResult(ctx, namespace)
	return results
}

func (s *Server) observabilityReadinessResult() readinessCheckResult {
	cfg := s.observabilityConfig.Normalize()
	if !cfg.Enabled {
		return readinessCheckResult{
			Status:  "skipped",
			Message: "observability exporters are disabled",
		}
	}
	if err := cfg.Validate(); err != nil {
		return readinessCheckResult{
			Status:  "degraded",
			Message: err.Error(),
		}
	}
	exporters := cfg.Exporters()
	if len(exporters) == 0 {
		return readinessCheckResult{
			Status:  "degraded",
			Message: "observability is enabled but no exporters are configured",
		}
	}
	return readinessCheckResult{
		Status:  "ok",
		Message: fmt.Sprintf("configured exporters: %s; retention: %s", strings.Join(exporters, ","), cfg.Retention.String()),
	}
}

func (s *Server) readinessKubernetesAPIResult(ctx context.Context, namespace string) readinessCheckResult {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return readinessCheckResult{
			Status:  "skipped",
			Message: "control plane namespace is not configured",
		}
	}

	result, err := s.readinessKubernetesAPICache.do("namespace:"+namespace, func() (readinessCheckResult, error) {
		return s.fetchReadinessKubernetesAPIResult(ctx, namespace), nil
	})
	if err != nil {
		return readinessCheckResult{
			Status:  "degraded",
			Message: err.Error(),
		}
	}
	return result
}

func (s *Server) fetchReadinessKubernetesAPIResult(ctx context.Context, namespace string) readinessCheckResult {
	if ctx == nil {
		ctx = context.Background()
	}

	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		return readinessCheckResult{
			Status:  "degraded",
			Message: err.Error(),
		}
	}

	kubeCtx, cancelKube := context.WithTimeout(ctx, readinessKubernetesAPITimeout)
	defer cancelKube()
	if _, err := client.listDeployments(kubeCtx, namespace); err != nil {
		return readinessCheckResult{
			Status:  "degraded",
			Message: err.Error(),
		}
	}

	return readinessCheckResult{Status: "ok"}
}

func readinessHasFailure(results map[string]readinessCheckResult) bool {
	for _, result := range results {
		if result.critical && result.Status == "error" {
			return true
		}
	}
	return false
}

func readinessHasDegradation(results map[string]readinessCheckResult) bool {
	for _, result := range results {
		switch result.Status {
		case "", "ok", "skipped":
		default:
			return true
		}
	}
	return false
}
