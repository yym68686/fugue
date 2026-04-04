package api

import (
	"errors"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

var errInvalidComposeImport = errors.New("invalid compose import")

type composeAppPlan struct {
	Service sourceimport.ComposeService
	AppName string
	Route   *model.AppRoute
	Source  model.AppSource
	Spec    model.AppSpec
}

func invalidComposeImportf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errInvalidComposeImport, fmt.Sprintf(format, args...))
}

func invalidComposeImport(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", errInvalidComposeImport, err)
}

func shouldInspectComposeImport(req importGitHubRequest, buildStrategy string) bool {
	if normalizeBuildStrategy(buildStrategy) != model.AppBuildStrategyAuto {
		return false
	}
	if strings.TrimSpace(req.SourceDir) != "" || strings.TrimSpace(req.DockerfilePath) != "" || strings.TrimSpace(req.BuildContextDir) != "" {
		return false
	}
	if strings.TrimSpace(req.ConfigContent) != "" || len(req.Files) > 0 || req.Postgres != nil {
		return false
	}
	return true
}

func (s *Server) importComposeGitHubStack(principal model.Principal, tenantID string, req importGitHubRequest, runtimeID string, replicas int, description string, baseName string, stack sourceimport.GitHubComposeStack) (map[string]any, model.App, model.Operation, error) {
	result, err := s.importResolvedGitHubTopology(principal, tenantID, req, runtimeID, replicas, description, baseName, stack.Topology())
	if err != nil {
		return nil, model.App{}, model.Operation{}, err
	}
	return map[string]any{
		"app":        sanitizeAppForAPI(result.PrimaryApp),
		"operation":  sanitizeOperationForAPI(result.PrimaryOp),
		"apps":       sanitizeAppsForAPI(result.Apps),
		"operations": sanitizeOperationsForAPI(result.Operations),
		"compose_stack": map[string]any{
			"compose_path":     stack.ComposePath,
			"primary_service":  result.PrimaryService,
			"services":         result.ServiceDetails,
			"warnings":         result.Warnings,
			"inference_report": result.InferenceReport,
		},
	}, result.PrimaryApp, result.PrimaryOp, nil
}

func pickPrimaryComposeService(services []sourceimport.ComposeService) sourceimport.ComposeService {
	service, _ := sourceimport.SelectPrimaryTopologyService(services, "")
	return service
}

func composePostgresSpec(service sourceimport.ComposeService, ownerAppName string) (model.AppPostgresSpec, error) {
	return sourceimport.ManagedPostgresSpec(service, ownerAppName)
}

func rewriteComposeEnvironment(env map[string]string, hosts map[string]string) map[string]string {
	return sourceimport.RewriteServiceEnvironment(env, hosts)
}

func applyManagedPostgresEnvironment(env map[string]string, spec model.AppPostgresSpec) map[string]string {
	return sourceimport.ApplyManagedPostgresEnvironment(env, spec)
}
