package controller

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

func (s *Service) executeManagedImportOperation(ctx context.Context, op model.Operation, app model.App) error {
	if op.DesiredSpec == nil {
		return fmt.Errorf("import operation %s missing desired spec", op.ID)
	}
	if op.DesiredSource == nil {
		return fmt.Errorf("import operation %s missing desired source", op.ID)
	}
	if strings.TrimSpace(s.registryPushBase) == "" {
		return fmt.Errorf("controller registry push base is not configured")
	}

	importCtx, cancel := context.WithTimeout(ctx, importSourceTimeout())
	defer cancel()

	jobLabels := map[string]string{
		"fugue.pro/operation-id": op.ID,
		"fugue.pro/app-id":       app.ID,
		"fugue.pro/tenant-id":    app.TenantID,
	}

	var output sourceimport.GitHubSourceImportOutput
	var err error
	switch strings.TrimSpace(op.DesiredSource.Type) {
	case model.AppSourceTypeGitHubPublic:
		if strings.TrimSpace(op.DesiredSource.RepoURL) == "" {
			return fmt.Errorf("import operation %s missing repo_url", op.ID)
		}
		output, err = s.importer.ImportPublicGitHubSource(importCtx, sourceimport.GitHubSourceImportRequest{
			RepoURL:          strings.TrimSpace(op.DesiredSource.RepoURL),
			Branch:           strings.TrimSpace(op.DesiredSource.RepoBranch),
			SourceDir:        strings.TrimSpace(op.DesiredSource.SourceDir),
			DockerfilePath:   strings.TrimSpace(op.DesiredSource.DockerfilePath),
			BuildContextDir:  strings.TrimSpace(op.DesiredSource.BuildContextDir),
			BuildStrategy:    strings.TrimSpace(op.DesiredSource.BuildStrategy),
			RegistryPushBase: s.registryPushBase,
			ImageRepository:  "fugue-apps",
			ImageNameSuffix:  strings.TrimSpace(op.DesiredSource.ImageNameSuffix),
			ComposeService:   strings.TrimSpace(op.DesiredSource.ComposeService),
			JobLabels:        jobLabels,
		})
	case model.AppSourceTypeUpload:
		if strings.TrimSpace(op.DesiredSource.UploadID) == "" {
			return fmt.Errorf("import operation %s missing upload_id", op.ID)
		}
		upload, archiveBytes, err := s.Store.GetSourceUploadArchive(strings.TrimSpace(op.DesiredSource.UploadID))
		if err != nil {
			return fmt.Errorf("load source upload %s: %w", op.DesiredSource.UploadID, err)
		}
		if upload.TenantID != app.TenantID {
			return fmt.Errorf("source upload %s is not visible to tenant %s", upload.ID, app.TenantID)
		}
		archiveURL, err := sourceUploadDownloadURL(s.Config.SourceUploadBaseURL, upload.ID, upload.DownloadToken)
		if err != nil {
			return err
		}
		output, err = s.importer.ImportUploadedArchiveSource(importCtx, sourceimport.UploadSourceImportRequest{
			UploadID:           upload.ID,
			ArchiveFilename:    upload.Filename,
			ArchiveSHA256:      upload.SHA256,
			ArchiveSizeBytes:   upload.SizeBytes,
			ArchiveData:        archiveBytes,
			ArchiveDownloadURL: archiveURL,
			AppName:            app.Name,
			SourceDir:          strings.TrimSpace(op.DesiredSource.SourceDir),
			DockerfilePath:     strings.TrimSpace(op.DesiredSource.DockerfilePath),
			BuildContextDir:    strings.TrimSpace(op.DesiredSource.BuildContextDir),
			BuildStrategy:      strings.TrimSpace(op.DesiredSource.BuildStrategy),
			RegistryPushBase:   s.registryPushBase,
			ImageRepository:    "fugue-apps",
			ImageNameSuffix:    strings.TrimSpace(op.DesiredSource.ImageNameSuffix),
			ComposeService:     strings.TrimSpace(op.DesiredSource.ComposeService),
			JobLabels:          jobLabels,
		})
	default:
		return fmt.Errorf("import operation %s only supports github-public or upload source", op.ID)
	}
	if err != nil {
		return err
	}

	finalSpec := cloneImportSpec(*op.DesiredSpec)
	finalSource := output.Source
	runtimeImageRef, err := rewriteImportedImageRef(strings.TrimSpace(output.ImportResult.ImageRef), s.registryPushBase, s.registryPullBase)
	if err != nil {
		return err
	}
	finalSpec.Image = runtimeImageRef
	if finalSpec.Replicas <= 0 {
		finalSpec.Replicas = 1
	}
	if strings.TrimSpace(finalSpec.RuntimeID) == "" {
		finalSpec.RuntimeID = "runtime_managed_shared"
	}
	if requestedPort := firstPositivePort(finalSpec.Ports); requestedPort > 0 {
		finalSpec.Ports = []int{requestedPort}
	} else if detectedPort := effectiveImportPort(output.ImportResult.DetectedPort, output.ImportResult.BuildStrategy); detectedPort > 0 {
		finalSpec.Ports = []int{detectedPort}
	}
	finalSpec.Env = mergeImportEnv(finalSpec.Env, output.ImportResult.SuggestedEnv)

	deployOp, err := s.Store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: op.RequestedByType,
		RequestedByID:   op.RequestedByID,
		AppID:           app.ID,
		DesiredSpec:     &finalSpec,
		DesiredSource:   &finalSource,
	})
	if err != nil {
		return fmt.Errorf("queue deploy after import: %w", err)
	}

	message := fmt.Sprintf("import build completed; queued deploy operation %s", deployOp.ID)
	if _, err := s.Store.CompleteManagedOperationWithResult(op.ID, "", message, &finalSpec, &finalSource); err != nil {
		return fmt.Errorf("complete import operation %s: %w", op.ID, err)
	}

	s.Logger.Printf("operation %s completed import build; pushed_image=%s runtime_image=%s deploy=%s", op.ID, output.ImportResult.ImageRef, finalSpec.Image, deployOp.ID)
	return nil
}

func importSourceTimeout() time.Duration {
	return 25 * time.Minute
}

func sourceUploadDownloadURL(baseURL, uploadID, token string) (string, error) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return "", fmt.Errorf("source upload base url is empty")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse source upload base url: %w", err)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/v1/source-uploads/" + strings.TrimSpace(uploadID) + "/archive"
	query := parsed.Query()
	query.Set("download_token", strings.TrimSpace(token))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func effectiveImportPort(detected int, buildStrategy string) int {
	if detected > 0 {
		return detected
	}
	switch strings.TrimSpace(buildStrategy) {
	case model.AppBuildStrategyStaticSite:
		return 80
	case model.AppBuildStrategyBuildpacks:
		return 8080
	case model.AppBuildStrategyNixpacks:
		return 3000
	default:
		return 80
	}
}

func firstPositivePort(ports []int) int {
	for _, port := range ports {
		if port > 0 {
			return port
		}
	}
	return 0
}

func mergeImportEnv(current, suggested map[string]string) map[string]string {
	if len(current) == 0 && len(suggested) == 0 {
		return nil
	}
	merged := make(map[string]string, len(current)+len(suggested))
	for key, value := range current {
		merged[key] = value
	}
	for key, value := range suggested {
		if strings.TrimSpace(merged[key]) != "" {
			continue
		}
		merged[key] = value
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

func cloneImportSpec(spec model.AppSpec) model.AppSpec {
	out := spec
	if len(spec.Command) > 0 {
		out.Command = append([]string(nil), spec.Command...)
	}
	if len(spec.Args) > 0 {
		out.Args = append([]string(nil), spec.Args...)
	}
	if len(spec.Ports) > 0 {
		out.Ports = append([]int(nil), spec.Ports...)
	}
	if len(spec.Env) > 0 {
		out.Env = make(map[string]string, len(spec.Env))
		for key, value := range spec.Env {
			out.Env[key] = value
		}
	}
	if len(spec.Files) > 0 {
		out.Files = append([]model.AppFile(nil), spec.Files...)
	}
	if spec.Workspace != nil {
		workspace := *spec.Workspace
		out.Workspace = &workspace
	}
	if spec.Postgres != nil {
		postgres := *spec.Postgres
		out.Postgres = &postgres
	}
	return out
}

func rewriteImportedImageRef(imageRef, pushBase, pullBase string) (string, error) {
	imageRef = strings.TrimSpace(imageRef)
	pushBase = strings.Trim(strings.TrimSpace(pushBase), "/")
	pullBase = strings.Trim(strings.TrimSpace(pullBase), "/")
	if imageRef == "" {
		return "", fmt.Errorf("imported image reference is empty")
	}
	if pullBase == "" || pullBase == pushBase {
		return imageRef, nil
	}
	if pushBase == "" {
		return imageRef, nil
	}
	prefix := pushBase + "/"
	if !strings.HasPrefix(imageRef, prefix) {
		return "", fmt.Errorf("imported image %q does not match configured registry push base %q", imageRef, pushBase)
	}
	return pullBase + "/" + strings.TrimPrefix(imageRef, prefix), nil
}
