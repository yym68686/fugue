package api

import (
	"errors"
	"net/http"
	"sort"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/sourceimport"
)

type inspectGitHubTemplateRequest struct {
	Branch         string `json:"branch"`
	RepoAuthToken  string `json:"repo_auth_token"`
	RepoURL        string `json:"repo_url"`
	RepoVisibility string `json:"repo_visibility"`
}

func (s *Server) handleInspectGitHubTemplate(w http.ResponseWriter, r *http.Request) {
	_ = mustPrincipal(r)

	var req inspectGitHubTemplateRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	repoURL := strings.TrimSpace(req.RepoURL)
	if repoURL == "" {
		httpx.WriteError(w, http.StatusBadRequest, "repo_url is required")
		return
	}
	if _, _, err := sourceimport.ParseGitHubRepoURL(repoURL); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	repoAuthToken := strings.TrimSpace(req.RepoAuthToken)
	repoVisibility := normalizeGitHubRepoVisibility(req.RepoVisibility)
	if repoVisibility == "" {
		if repoAuthToken != "" {
			repoVisibility = "private"
		} else {
			repoVisibility = "public"
		}
	}
	if repoVisibility == "private" && repoAuthToken == "" {
		httpx.WriteError(w, http.StatusBadRequest, "repo_auth_token is required for private GitHub repositories")
		return
	}

	inspection, err := s.importer.InspectGitHubTemplate(r.Context(), sourceimport.GitHubFugueManifestInspectRequest{
		Branch:        strings.TrimSpace(req.Branch),
		RepoAuthToken: repoAuthToken,
		RepoURL:       repoURL,
	})
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	response := map[string]any{
		"repository": map[string]any{
			"repo_url":            repoURL,
			"repo_visibility":     repoVisibility,
			"repo_owner":          inspection.RepoOwner,
			"repo_name":           inspection.RepoName,
			"branch":              inspection.Branch,
			"commit_sha":          inspection.CommitSHA,
			"commit_committed_at": inspection.CommitCommittedAt,
			"default_app_name":    inspection.DefaultAppName,
		},
		"fugue_manifest": nil,
		"compose_stack":  nil,
		"template":       nil,
	}

	if inspection.Manifest != nil {
		response["fugue_manifest"] = sanitizeGitHubTemplateManifest(inspection.Manifest)
		response["template"] = sanitizeGitHubTemplateMetadata(inspection.Manifest.Template)
	} else {
		stack, composeErr := s.importer.InspectGitHubCompose(r.Context(), sourceimport.GitHubComposeInspectRequest{
			Branch:        strings.TrimSpace(req.Branch),
			RepoAuthToken: repoAuthToken,
			RepoURL:       repoURL,
		})
		switch {
		case composeErr == nil:
			response["compose_stack"] = sanitizeGitHubTemplateComposeStack(&stack)
		case !errors.Is(composeErr, sourceimport.ErrComposeNotFound):
			httpx.WriteError(w, http.StatusBadRequest, composeErr.Error())
			return
		}
	}

	httpx.WriteJSON(w, http.StatusOK, response)
}

func sanitizeGitHubTemplateService(service sourceimport.ComposeService) map[string]any {
	serviceInfo := map[string]any{
		"build_context_dir":             service.BuildContextDir,
		"build_strategy":                service.BuildStrategy,
		"compose_service":               service.Name,
		"dockerfile_path":               service.DockerfilePath,
		"internal_port":                 service.InternalPort,
		"kind":                          service.Kind,
		"persistent_storage_seed_files": sanitizePersistentStorageSeedFiles(service.PersistentStorageSeedFiles),
		"published":                     service.Published,
		"service":                       service.Name,
		"service_type":                  service.ServiceType,
		"source_dir":                    service.SourceDir,
	}
	if service.BackingService {
		serviceInfo["backing_service"] = true
	}
	if len(service.Bindings) > 0 {
		targets := make([]string, 0, len(service.Bindings))
		for _, binding := range service.Bindings {
			targets = append(targets, binding.Service)
		}
		sort.Strings(targets)
		serviceInfo["binding_targets"] = targets
	}
	return serviceInfo
}

func sanitizeGitHubTemplateManifest(manifest *sourceimport.GitHubFugueManifest) map[string]any {
	if manifest == nil {
		return nil
	}

	services := make([]map[string]any, 0, len(manifest.Services))
	for _, service := range manifest.Services {
		services = append(services, sanitizeGitHubTemplateService(service))
	}

	return map[string]any{
		"manifest_path":    manifest.ManifestPath,
		"primary_service":  manifest.PrimaryService,
		"services":         services,
		"warnings":         append([]string(nil), manifest.Warnings...),
		"inference_report": append([]sourceimport.TopologyInference(nil), manifest.InferenceReport...),
	}
}

func sanitizeGitHubTemplateComposeStack(stack *sourceimport.GitHubComposeStack) map[string]any {
	if stack == nil {
		return nil
	}

	services := make([]map[string]any, 0, len(stack.Services))
	for _, service := range stack.Services {
		services = append(services, sanitizeGitHubTemplateService(service))
	}

	primaryService := ""
	if service, err := sourceimport.SelectPrimaryTopologyService(stack.Services, ""); err == nil {
		primaryService = service.Name
	}

	return map[string]any{
		"compose_path":     stack.ComposePath,
		"primary_service":  primaryService,
		"services":         services,
		"warnings":         append([]string(nil), stack.Warnings...),
		"inference_report": append([]sourceimport.TopologyInference(nil), stack.InferenceReport...),
	}
}

func sanitizeGitHubTemplateMetadata(template *sourceimport.GitHubTemplateMetadata) map[string]any {
	if template == nil {
		return nil
	}

	variables := make([]map[string]any, 0, len(template.Variables))
	for _, variable := range template.Variables {
		variables = append(variables, map[string]any{
			"default_value": variable.DefaultValue,
			"description":   variable.Description,
			"generate":      variable.Generate,
			"key":           variable.Key,
			"label":         variable.Label,
			"required":      variable.Required,
			"secret":        variable.Secret,
		})
	}

	return map[string]any{
		"default_runtime": template.DefaultRuntime,
		"demo_url":        template.DemoURL,
		"description":     template.Description,
		"docs_url":        template.DocsURL,
		"name":            template.Name,
		"slug":            template.Slug,
		"source_mode":     template.SourceMode,
		"variables":       variables,
	}
}

func sanitizePersistentStorageSeedFiles(files []sourceimport.PersistentStorageSeedFile) []map[string]any {
	if len(files) == 0 {
		return []map[string]any{}
	}

	items := make([]map[string]any, 0, len(files))
	for _, file := range files {
		path := strings.TrimSpace(file.Path)
		if path == "" {
			continue
		}

		items = append(items, map[string]any{
			"mode":         file.Mode,
			"path":         path,
			"seed_content": file.SeedContent,
		})
	}

	return items
}
