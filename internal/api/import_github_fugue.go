package api

import (
	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

func shouldInspectFugueManifestImport(req importGitHubRequest, buildStrategy string) bool {
	return shouldInspectComposeImport(req, buildStrategy)
}

func (s *Server) importFugueManifestGitHubStack(principal model.Principal, tenantID string, req importGitHubRequest, runtimeID string, replicas int, description string, baseName string, manifest sourceimport.GitHubFugueManifest) (map[string]any, model.App, model.Operation, error) {
	result, err := s.importResolvedGitHubTopology(principal, tenantID, req, runtimeID, replicas, description, baseName, manifest.Topology())
	if err != nil {
		return nil, model.App{}, model.Operation{}, err
	}
	return map[string]any{
		"app":        sanitizeAppForAPI(result.PrimaryApp),
		"operation":  sanitizeOperationForAPI(result.PrimaryOp),
		"apps":       sanitizeAppsForAPI(result.Apps),
		"operations": sanitizeOperationsForAPI(result.Operations),
		"fugue_manifest": map[string]any{
			"manifest_path":    manifest.ManifestPath,
			"primary_service":  result.PrimaryService,
			"services":         result.ServiceDetails,
			"warnings":         result.Warnings,
			"inference_report": result.InferenceReport,
		},
	}, result.PrimaryApp, result.PrimaryOp, nil
}
