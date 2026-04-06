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
	stateful := op.DesiredSpec.Workspace != nil || op.DesiredSpec.PersistentStorage != nil || op.DesiredSpec.Postgres != nil
	var err error
	var output sourceimport.GitHubSourceImportOutput
	switch strings.TrimSpace(op.DesiredSource.Type) {
	case model.AppSourceTypeDockerImage:
		if strings.TrimSpace(op.DesiredSource.ImageRef) == "" {
			return fmt.Errorf("import operation %s missing image_ref", op.ID)
		}
		output, err = s.importer.ImportDockerImageSource(importCtx, sourceimport.DockerImageSourceImportRequest{
			AppName:          app.Name,
			ImageNameSuffix:  strings.TrimSpace(op.DesiredSource.ImageNameSuffix),
			ImageRef:         strings.TrimSpace(op.DesiredSource.ImageRef),
			RegistryPushBase: s.registryPushBase,
			ImageRepository:  "fugue-apps",
		})
	case model.AppSourceTypeGitHubPublic, model.AppSourceTypeGitHubPrivate:
		if strings.TrimSpace(op.DesiredSource.RepoURL) == "" {
			return fmt.Errorf("import operation %s missing repo_url", op.ID)
		}
		output, err = s.importer.ImportGitHubSource(importCtx, sourceimport.GitHubSourceImportRequest{
			SourceType:       strings.TrimSpace(op.DesiredSource.Type),
			RepoURL:          strings.TrimSpace(op.DesiredSource.RepoURL),
			RepoAuthToken:    strings.TrimSpace(op.DesiredSource.RepoAuthToken),
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
			Stateful:         stateful,
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
			Stateful:           stateful,
		})
	default:
		return fmt.Errorf("import operation %s only supports github-backed, image-backed, or upload source", op.ID)
	}
	if err != nil {
		return err
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}

	composeSuggestedEnv, composeEnvErr := s.suggestComposeServiceEnv(importCtx, app, *op.DesiredSource)
	if composeEnvErr != nil && s.Logger != nil {
		s.Logger.Printf("skip compose env refresh for app %s source=%s compose_service=%s: %v", app.ID, op.DesiredSource.Type, op.DesiredSource.ComposeService, composeEnvErr)
	}
	output.ImportResult.SuggestedEnv = mergeSuggestedImportEnv(output.ImportResult.SuggestedEnv, composeSuggestedEnv)

	finalSpec := cloneImportSpec(*op.DesiredSpec)
	finalSource := restoreQueuedSourceMetadata(output.Source, *op.DesiredSource)
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
	if model.AppUsesBackgroundNetwork(finalSpec) {
		finalSpec.Ports = nil
	} else if requestedPort := firstPositivePort(finalSpec.Ports); requestedPort > 0 {
		finalSpec.Ports = []int{requestedPort}
	} else if detectedPort := effectiveImportPort(output.ImportResult.DetectedPort, output.ImportResult.BuildStrategy); detectedPort > 0 {
		finalSpec.Ports = []int{detectedPort}
	}
	finalSpec.Env = mergeImportEnv(finalSpec.Env, output.ImportResult.SuggestedEnv)
	finalSpec.Command = mergeImportCommand(finalSpec.Command, finalSpec.Args, output.ImportResult.SuggestedStartupCommand)

	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}

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
	if err := s.syncTenantBillingImageStorage(ctx, app.TenantID); err != nil && s.Logger != nil {
		s.Logger.Printf("skip billing image storage sync after import op=%s tenant=%s: %v", op.ID, app.TenantID, err)
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

func mergeImportCommand(current, args []string, suggested string) []string {
	if len(current) > 0 || len(args) > 0 {
		return current
	}
	suggested = strings.TrimSpace(suggested)
	if suggested == "" {
		return nil
	}
	return []string{"sh", "-lc", suggested}
}

func mergeSuggestedImportEnv(base, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	merged := make(map[string]string, len(base)+len(override))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range override {
		merged[key] = value
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

func restoreQueuedSourceMetadata(imported model.AppSource, queued model.AppSource) model.AppSource {
	imported.ImageNameSuffix = strings.TrimSpace(queued.ImageNameSuffix)
	imported.ComposeService = strings.TrimSpace(queued.ComposeService)
	if len(queued.ComposeDependsOn) > 0 {
		imported.ComposeDependsOn = append([]string(nil), queued.ComposeDependsOn...)
	} else {
		imported.ComposeDependsOn = nil
	}
	return imported
}

func (s *Service) suggestComposeServiceEnv(ctx context.Context, app model.App, source model.AppSource) (map[string]string, error) {
	composeService := strings.TrimSpace(source.ComposeService)
	if composeService == "" {
		return nil, nil
	}

	appHosts, managedPostgresByOwner, err := s.projectComposeServiceState(app)
	if err != nil {
		return nil, err
	}

	switch strings.TrimSpace(source.Type) {
	case model.AppSourceTypeGitHubPublic, model.AppSourceTypeGitHubPrivate:
		return s.importer.SuggestGitHubComposeServiceEnv(ctx, sourceimport.GitHubComposeServiceEnvRequest{
			RepoURL:                strings.TrimSpace(source.RepoURL),
			RepoAuthToken:          strings.TrimSpace(source.RepoAuthToken),
			Branch:                 strings.TrimSpace(source.RepoBranch),
			ComposeService:         composeService,
			AppHosts:               appHosts,
			ManagedPostgresByOwner: managedPostgresByOwner,
		})
	case model.AppSourceTypeUpload:
		if strings.TrimSpace(source.UploadID) == "" {
			return nil, nil
		}
		upload, archiveBytes, err := s.Store.GetSourceUploadArchive(strings.TrimSpace(source.UploadID))
		if err != nil {
			return nil, fmt.Errorf("load source upload %s for compose env refresh: %w", source.UploadID, err)
		}
		return s.importer.SuggestUploadedComposeServiceEnv(ctx, sourceimport.UploadComposeServiceEnvRequest{
			ArchiveFilename:        upload.Filename,
			ArchiveSHA256:          upload.SHA256,
			ArchiveSizeBytes:       upload.SizeBytes,
			ArchiveData:            archiveBytes,
			AppName:                app.Name,
			ComposeService:         composeService,
			AppHosts:               appHosts,
			ManagedPostgresByOwner: managedPostgresByOwner,
		})
	default:
		return nil, nil
	}
}

func (s *Service) projectComposeServiceState(app model.App) (map[string]string, map[string]model.AppPostgresSpec, error) {
	apps, err := s.Store.ListApps(app.TenantID, false)
	if err != nil {
		return nil, nil, fmt.Errorf("list project apps for compose env refresh: %w", err)
	}

	appHosts := make(map[string]string)
	managedPostgresByOwner := make(map[string]model.AppPostgresSpec)
	for _, candidate := range apps {
		if candidate.ProjectID != app.ProjectID || candidate.Source == nil {
			continue
		}
		composeService := strings.TrimSpace(candidate.Source.ComposeService)
		if composeService == "" {
			continue
		}
		appHosts[composeService] = strings.TrimSpace(candidate.Name)
		if postgres := appOwnedPostgresSpec(candidate); postgres != nil {
			managedPostgresByOwner[composeService] = *postgres
		}
	}
	if len(appHosts) == 0 {
		return nil, nil, nil
	}
	if len(managedPostgresByOwner) == 0 {
		managedPostgresByOwner = nil
	}
	return appHosts, managedPostgresByOwner, nil
}

func appOwnedPostgresSpec(app model.App) *model.AppPostgresSpec {
	for _, service := range app.BackingServices {
		if service.Type != model.BackingServiceTypePostgres || service.Spec.Postgres == nil {
			continue
		}
		specCopy := *service.Spec.Postgres
		return &specCopy
	}
	return nil
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
	if spec.PersistentStorage != nil {
		storage := *spec.PersistentStorage
		if len(spec.PersistentStorage.Mounts) > 0 {
			storage.Mounts = append([]model.AppPersistentStorageMount(nil), spec.PersistentStorage.Mounts...)
		}
		out.PersistentStorage = &storage
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
