package api

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handlePatchAppSource(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}

	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	var req struct {
		OriginSource *model.AppSource `json:"origin_source"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.OriginSource == nil {
		httpx.WriteError(w, http.StatusBadRequest, "origin_source is required")
		return
	}

	originSource, err := s.normalizePatchedAppOriginSource(app, req.OriginSource)
	if err != nil {
		if strings.HasPrefix(err.Error(), "source ") || strings.HasPrefix(err.Error(), "repo_") || strings.HasPrefix(err.Error(), "image_ref") || strings.HasPrefix(err.Error(), "upload_id") || strings.HasPrefix(err.Error(), "unsupported ") {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		s.writeStoreError(w, err)
		return
	}

	currentOrigin := model.AppOriginSource(app)
	if reflect.DeepEqual(currentOrigin, originSource) {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"app":             sanitizeAppForAPI(app),
			"already_current": true,
		})
		return
	}

	updatedApp, err := s.store.UpdateAppOriginSource(app.ID, *originSource)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	auditMetadata := map[string]string{
		"origin_source_type": strings.TrimSpace(originSource.Type),
		"origin_source_ref":  appSourceAuditRef(originSource),
	}
	if strings.TrimSpace(originSource.ComposeService) != "" {
		auditMetadata["compose_service"] = strings.TrimSpace(originSource.ComposeService)
	}
	if strings.TrimSpace(originSource.RepoBranch) != "" {
		auditMetadata["repo_branch"] = strings.TrimSpace(originSource.RepoBranch)
	}
	s.appendAudit(principal, "app.patch_source", "app", updatedApp.ID, updatedApp.TenantID, auditMetadata)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"app":             sanitizeAppForAPI(updatedApp),
		"already_current": false,
	})
}

func (s *Server) normalizePatchedAppOriginSource(app model.App, patch *model.AppSource) (*model.AppSource, error) {
	if patch == nil {
		return nil, fmt.Errorf("origin_source is required")
	}

	baseline := currentAppOriginSourceBaseline(app)
	merged := mergeAppOriginSourcePatch(baseline, patch)
	normalized, err := s.normalizeCreateAppSource(merged)
	if err != nil {
		return nil, err
	}

	if len(merged.ComposeDependsOn) > 0 {
		normalized.ComposeDependsOn = append([]string(nil), merged.ComposeDependsOn...)
	}
	if appSourceOwnershipMatches(baseline, normalized) {
		normalized.CommitSHA = strings.TrimSpace(baseline.CommitSHA)
		normalized.CommitCommittedAt = strings.TrimSpace(baseline.CommitCommittedAt)
		normalized.ResolvedImageRef = strings.TrimSpace(baseline.ResolvedImageRef)
		if strings.TrimSpace(normalized.DetectedProvider) == "" {
			normalized.DetectedProvider = strings.TrimSpace(baseline.DetectedProvider)
		}
		if strings.TrimSpace(normalized.DetectedStack) == "" {
			normalized.DetectedStack = strings.TrimSpace(baseline.DetectedStack)
		}
	}

	return normalized, nil
}

func currentAppOriginSourceBaseline(app model.App) *model.AppSource {
	if source := model.AppOriginSource(app); source != nil {
		return source
	}
	return model.AppBuildSource(app)
}

func mergeAppOriginSourcePatch(baseline, patch *model.AppSource) *model.AppSource {
	if patch == nil {
		return nil
	}
	if baseline == nil {
		return cloneAppSource(patch)
	}

	base := cloneAppSource(baseline)
	patchType := requestedAppSourcePatchType(baseline, patch)
	if patchType != "" && !strings.EqualFold(strings.TrimSpace(base.Type), patchType) {
		base = &model.AppSource{
			Type:            patchType,
			SourceDir:       strings.TrimSpace(baseline.SourceDir),
			BuildStrategy:   strings.TrimSpace(baseline.BuildStrategy),
			DockerfilePath:  strings.TrimSpace(baseline.DockerfilePath),
			BuildContextDir: strings.TrimSpace(baseline.BuildContextDir),
			ImageNameSuffix: strings.TrimSpace(baseline.ImageNameSuffix),
			ComposeService:  strings.TrimSpace(baseline.ComposeService),
		}
		if len(baseline.ComposeDependsOn) > 0 {
			base.ComposeDependsOn = append([]string(nil), baseline.ComposeDependsOn...)
		}
	}

	if value := strings.TrimSpace(patch.Type); value != "" {
		base.Type = value
	}
	if value := strings.TrimSpace(patch.RepoURL); value != "" {
		base.RepoURL = value
	}
	if value := strings.TrimSpace(patch.RepoBranch); value != "" {
		base.RepoBranch = value
	}
	if value := strings.TrimSpace(patch.RepoAuthToken); value != "" {
		base.RepoAuthToken = value
	}
	if value := strings.TrimSpace(patch.ImageRef); value != "" {
		base.ImageRef = value
	}
	if value := strings.TrimSpace(patch.UploadID); value != "" {
		base.UploadID = value
	}
	if value := strings.TrimSpace(patch.SourceDir); value != "" {
		base.SourceDir = value
	}
	if value := strings.TrimSpace(patch.BuildStrategy); value != "" {
		base.BuildStrategy = value
	}
	if value := strings.TrimSpace(patch.DockerfilePath); value != "" {
		base.DockerfilePath = value
	}
	if value := strings.TrimSpace(patch.BuildContextDir); value != "" {
		base.BuildContextDir = value
	}
	if value := strings.TrimSpace(patch.ImageNameSuffix); value != "" {
		base.ImageNameSuffix = value
	}
	if value := strings.TrimSpace(patch.ComposeService); value != "" {
		base.ComposeService = value
	}
	if len(patch.ComposeDependsOn) > 0 {
		base.ComposeDependsOn = append([]string(nil), patch.ComposeDependsOn...)
	}
	if value := strings.TrimSpace(patch.DetectedProvider); value != "" {
		base.DetectedProvider = value
	}
	if value := strings.TrimSpace(patch.DetectedStack); value != "" {
		base.DetectedStack = value
	}

	return base
}

func requestedAppSourcePatchType(baseline, patch *model.AppSource) string {
	if patch == nil {
		return ""
	}
	if explicit := strings.TrimSpace(patch.Type); explicit != "" {
		return explicit
	}
	switch {
	case strings.TrimSpace(patch.RepoURL) != "" || strings.TrimSpace(patch.RepoBranch) != "" || strings.TrimSpace(patch.RepoAuthToken) != "":
		hasRepoAuth := strings.TrimSpace(patch.RepoAuthToken) != ""
		if !hasRepoAuth && baseline != nil && strings.TrimSpace(baseline.Type) == model.AppSourceTypeGitHubPrivate {
			hasRepoAuth = true
		}
		return model.ResolveGitHubAppSourceType("", hasRepoAuth)
	case strings.TrimSpace(patch.ImageRef) != "":
		return model.AppSourceTypeDockerImage
	case strings.TrimSpace(patch.UploadID) != "":
		return model.AppSourceTypeUpload
	case baseline != nil:
		return strings.TrimSpace(baseline.Type)
	default:
		return ""
	}
}

func appSourceOwnershipMatches(left, right *model.AppSource) bool {
	if left == nil || right == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(left.ComposeService), strings.TrimSpace(right.ComposeService)) {
		return false
	}
	switch {
	case model.IsGitHubAppSourceType(left.Type) || model.IsGitHubAppSourceType(right.Type):
		if !model.IsGitHubAppSourceType(left.Type) || !model.IsGitHubAppSourceType(right.Type) {
			return false
		}
		return strings.EqualFold(strings.TrimSpace(strings.TrimSuffix(left.RepoURL, ".git")), strings.TrimSpace(strings.TrimSuffix(right.RepoURL, ".git")))
	case strings.EqualFold(strings.TrimSpace(left.Type), model.AppSourceTypeUpload) || strings.EqualFold(strings.TrimSpace(right.Type), model.AppSourceTypeUpload):
		return strings.EqualFold(strings.TrimSpace(left.Type), model.AppSourceTypeUpload) &&
			strings.EqualFold(strings.TrimSpace(right.Type), model.AppSourceTypeUpload) &&
			strings.EqualFold(strings.TrimSpace(left.UploadID), strings.TrimSpace(right.UploadID))
	case strings.EqualFold(strings.TrimSpace(left.Type), model.AppSourceTypeDockerImage) || strings.EqualFold(strings.TrimSpace(right.Type), model.AppSourceTypeDockerImage):
		return strings.EqualFold(strings.TrimSpace(left.Type), model.AppSourceTypeDockerImage) &&
			strings.EqualFold(strings.TrimSpace(right.Type), model.AppSourceTypeDockerImage) &&
			strings.EqualFold(strings.TrimSpace(left.ImageRef), strings.TrimSpace(right.ImageRef))
	default:
		return strings.EqualFold(strings.TrimSpace(left.Type), strings.TrimSpace(right.Type)) &&
			strings.EqualFold(appSourceAuditRef(left), appSourceAuditRef(right))
	}
}

func appSourceAuditRef(source *model.AppSource) string {
	if source == nil {
		return ""
	}
	switch {
	case model.IsGitHubAppSourceType(source.Type):
		return strings.TrimSpace(source.RepoURL)
	case strings.EqualFold(strings.TrimSpace(source.Type), model.AppSourceTypeUpload):
		return strings.TrimSpace(source.UploadID)
	case strings.EqualFold(strings.TrimSpace(source.Type), model.AppSourceTypeDockerImage):
		return strings.TrimSpace(source.ImageRef)
	default:
		return strings.TrimSpace(source.Type)
	}
}
