package controller

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"fugue/internal/appimages"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

func (s *Service) executeManagedImportOperation(ctx context.Context, op model.Operation, app model.App) (err error) {
	timer := newControllerOperationTimer(s.now)
	defer func() {
		timer.Log(s.Logger, "managed_import_operation", op, err)
	}()

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
	placementNodeSelector := s.importBuildPlacementNodeSelector(ctx, app, op)
	var output sourceimport.GitHubSourceImportOutput
	queuedDockerImageRef := ""
	stopImportProgress := s.startImportOperationProgressHeartbeat(importCtx, op.ID)
	switch strings.TrimSpace(op.DesiredSource.Type) {
	case model.AppSourceTypeDockerImage:
		queuedDockerImageRef = strings.TrimSpace(op.DesiredSource.ImageRef)
		if queuedDockerImageRef == "" {
			return fmt.Errorf("import operation %s missing image_ref", op.ID)
		}
		output, err = s.importer.ImportDockerImageSource(importCtx, sourceimport.DockerImageSourceImportRequest{
			AppName:          app.Name,
			ImageNameSuffix:  strings.TrimSpace(op.DesiredSource.ImageNameSuffix),
			ImageRef:         controllerReachableImportImageRef(queuedDockerImageRef, s.registryPushBase, s.registryPullBase),
			RegistryPushBase: s.registryPushBase,
			ImageRepository:  "fugue-apps",
		})
	case model.AppSourceTypeGitHubPublic, model.AppSourceTypeGitHubPrivate:
		if strings.TrimSpace(op.DesiredSource.RepoURL) == "" {
			return fmt.Errorf("import operation %s missing repo_url", op.ID)
		}
		output, err = s.importer.ImportGitHubSource(importCtx, sourceimport.GitHubSourceImportRequest{
			SourceType:            strings.TrimSpace(op.DesiredSource.Type),
			RepoURL:               strings.TrimSpace(op.DesiredSource.RepoURL),
			RepoAuthToken:         strings.TrimSpace(op.DesiredSource.RepoAuthToken),
			Branch:                strings.TrimSpace(op.DesiredSource.RepoBranch),
			SourceDir:             strings.TrimSpace(op.DesiredSource.SourceDir),
			DockerfilePath:        strings.TrimSpace(op.DesiredSource.DockerfilePath),
			BuildContextDir:       strings.TrimSpace(op.DesiredSource.BuildContextDir),
			BuildStrategy:         strings.TrimSpace(op.DesiredSource.BuildStrategy),
			RegistryPushBase:      s.registryPushBase,
			ImageRepository:       "fugue-apps",
			ImageNameSuffix:       strings.TrimSpace(op.DesiredSource.ImageNameSuffix),
			ComposeService:        strings.TrimSpace(op.DesiredSource.ComposeService),
			JobLabels:             jobLabels,
			PlacementNodeSelector: placementNodeSelector,
			Stateful:              stateful,
		})
	case model.AppSourceTypeUpload:
		if strings.TrimSpace(op.DesiredSource.UploadID) == "" {
			return fmt.Errorf("import operation %s missing upload_id", op.ID)
		}
		upload, archiveBytes, loadErr := s.Store.GetSourceUploadArchive(strings.TrimSpace(op.DesiredSource.UploadID))
		if loadErr != nil {
			return fmt.Errorf("load source upload %s: %w", op.DesiredSource.UploadID, loadErr)
		}
		if upload.TenantID != app.TenantID {
			return fmt.Errorf("source upload %s is not visible to tenant %s", upload.ID, app.TenantID)
		}
		archiveURL, archiveURLErr := sourceUploadDownloadURL(s.Config.SourceUploadBaseURL, upload.ID, upload.DownloadToken)
		if archiveURLErr != nil {
			return archiveURLErr
		}
		output, err = s.importer.ImportUploadedArchiveSource(importCtx, sourceimport.UploadSourceImportRequest{
			UploadID:              upload.ID,
			ArchiveFilename:       upload.Filename,
			ArchiveSHA256:         upload.SHA256,
			ArchiveSizeBytes:      upload.SizeBytes,
			ArchiveData:           archiveBytes,
			ArchiveDownloadURL:    archiveURL,
			AppName:               app.Name,
			SourceDir:             strings.TrimSpace(op.DesiredSource.SourceDir),
			DockerfilePath:        strings.TrimSpace(op.DesiredSource.DockerfilePath),
			BuildContextDir:       strings.TrimSpace(op.DesiredSource.BuildContextDir),
			BuildStrategy:         strings.TrimSpace(op.DesiredSource.BuildStrategy),
			RegistryPushBase:      s.registryPushBase,
			ImageRepository:       "fugue-apps",
			ImageNameSuffix:       strings.TrimSpace(op.DesiredSource.ImageNameSuffix),
			ComposeService:        strings.TrimSpace(op.DesiredSource.ComposeService),
			JobLabels:             jobLabels,
			PlacementNodeSelector: placementNodeSelector,
			Stateful:              stateful,
		})
	default:
		return fmt.Errorf("import operation %s only supports github-backed, image-backed, or upload source", op.ID)
	}
	stopImportProgress()
	if err != nil {
		return err
	}
	if queuedDockerImageRef != "" {
		output.Source.ImageRef = queuedDockerImageRef
	}
	timer.Mark("import_source")
	s.updateOperationProgress(op.ID, "import build completed; validating image output")
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	if s.Logger != nil {
		s.Logger.Printf(
			"import operation %s importer output source_type=%s build_strategy=%s import_image=%s resolved_image=%s build_job=%s compose_service=%s detected_provider=%s",
			op.ID,
			importOutputSourceType(output, *op.DesiredSource),
			importOutputBuildStrategy(output, *op.DesiredSource),
			strings.TrimSpace(output.ImportResult.ImageRef),
			strings.TrimSpace(output.Source.ResolvedImageRef),
			strings.TrimSpace(output.ImportResult.BuildJobName),
			importOutputComposeService(output, *op.DesiredSource),
			strings.TrimSpace(output.Source.DetectedProvider),
		)
	}
	if err := validateImportedManagedImageOutput(op, *op.DesiredSource, output); err != nil {
		return err
	}
	timer.Mark("validate_import")
	s.updateOperationProgress(op.ID, "import image built; preparing deploy spec")

	composeSuggestedEnv, composeEnvErr := s.suggestComposeServiceEnv(importCtx, app, *op.DesiredSource)
	if composeEnvErr != nil && s.Logger != nil {
		s.Logger.Printf("skip compose env refresh for app %s source=%s compose_service=%s: %v", app.ID, op.DesiredSource.Type, op.DesiredSource.ComposeService, composeEnvErr)
	}
	output.ImportResult.SuggestedEnv = mergeSuggestedImportEnv(output.ImportResult.SuggestedEnv, composeSuggestedEnv)
	timer.Mark("suggest_env")

	finalSpec := cloneImportSpec(*op.DesiredSpec)
	finalSource := restoreQueuedSourceMetadata(output.Source, *op.DesiredSource)
	deployOriginSource := persistedOriginSourceAfterImport(op.DesiredOriginSource, *op.DesiredSource, finalSource)
	managedImageRef, runtimeImageRef, err := s.resolveImportedManagedImageRef(importCtx, app, *op.DesiredSource, finalSource, strings.TrimSpace(output.ImportResult.ImageRef))
	if err != nil {
		return err
	}
	timer.Mark("resolve_image")
	s.updateOperationProgress(op.ID, "import image resolved; queueing deploy")
	finalSource.ResolvedImageRef = managedImageRef
	if deployOriginSource != nil && originSourceShouldAdoptImportedBuild(*deployOriginSource, *op.DesiredSource) {
		deployOriginSource.ResolvedImageRef = managedImageRef
		if detected := strings.TrimSpace(finalSource.DetectedProvider); detected != "" {
			deployOriginSource.DetectedProvider = detected
		}
		if stack := strings.TrimSpace(finalSource.DetectedStack); stack != "" {
			deployOriginSource.DetectedStack = stack
		}
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
	} else if shouldAutoBackgroundImportedApp(*op.DesiredSource, output.ImportResult) {
		finalSpec.NetworkMode = model.AppNetworkModeBackground
		finalSpec.Ports = nil
	} else if detectedPort := effectiveImportPort(output.ImportResult.DetectedPort, output.ImportResult.BuildStrategy); detectedPort > 0 {
		finalSpec.Ports = []int{detectedPort}
	}
	finalSpec.Env = mergeImportEnv(finalSpec.Env, output.ImportResult.SuggestedEnv)
	finalSpec.Command = mergeImportCommand(finalSpec.Command, finalSpec.Args, output.ImportResult.SuggestedStartupCommand)
	finalSpec.RestartToken = model.NewID("restart")
	s.recordImportedImageLocation(app, op, managedImageRef, runtimeImageRef)
	hydrateApp := app
	hydrateApp.Spec = finalSpec
	if scheduling, scheduleErr := s.managedSchedulingConstraintsForApp(ctx, hydrateApp); scheduleErr == nil {
		s.scheduleImageHydration(ctx, hydrateApp, s.deployImageTarget(hydrateApp, scheduling), runtimeImageRef)
	} else if s.Logger != nil {
		s.Logger.Printf("skip post-import image hydration app=%s op=%s image=%s: %v", app.ID, op.ID, runtimeImageRef, scheduleErr)
	}

	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}

	deployOp, err := s.Store.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     op.RequestedByType,
		RequestedByID:       op.RequestedByID,
		AppID:               app.ID,
		DesiredSpec:         &finalSpec,
		DesiredSource:       &finalSource,
		DesiredOriginSource: deployOriginSource,
	})
	if err != nil {
		return fmt.Errorf("queue deploy after import: %w", err)
	}
	timer.Mark("queue_deploy")
	s.updateOperationProgress(op.ID, fmt.Sprintf("import build completed; queued deploy operation %s", deployOp.ID))

	message := fmt.Sprintf("import build completed; queued deploy operation %s", deployOp.ID)
	if _, err := s.Store.CompleteManagedOperationWithResult(op.ID, "", message, &finalSpec, &finalSource); err != nil {
		return fmt.Errorf("complete import operation %s: %w", op.ID, err)
	}
	timer.Mark("complete_import")
	if err := s.syncTenantBillingImageStorage(ctx, app.TenantID); err != nil && s.Logger != nil {
		s.Logger.Printf("skip billing image storage sync after import op=%s tenant=%s: %v", op.ID, app.TenantID, err)
	}
	timer.Mark("billing_sync")

	s.Logger.Printf("operation %s completed import build; managed_image=%s runtime_image=%s deploy=%s", op.ID, managedImageRef, finalSpec.Image, deployOp.ID)
	return nil
}

func (s *Service) importBuildPlacementNodeSelector(ctx context.Context, app model.App, op model.Operation) map[string]string {
	if s == nil || op.DesiredSpec == nil {
		return nil
	}
	buildApp := app
	buildApp.Spec = *op.DesiredSpec
	if op.DesiredSource != nil {
		model.SetAppSourceState(&buildApp, op.DesiredOriginSource, op.DesiredSource)
	}
	scheduling, err := s.managedSchedulingConstraintsForApp(ctx, buildApp)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("skip import build placement app=%s op=%s: %v", app.ID, op.ID, err)
		}
		return nil
	}
	return clonePlacementStringMap(scheduling.NodeSelector)
}

func (s *Service) startImportOperationProgressHeartbeat(ctx context.Context, operationID string) func() {
	now := time.Now
	if s != nil && s.now != nil {
		now = s.now
	}
	startedAt := now()
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	s.updateOperationProgress(operationID, importOperationProgressMessage(0))

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				elapsed := now().Sub(startedAt)
				if elapsed < 0 {
					elapsed = time.Since(startedAt)
				}
				s.updateOperationProgress(operationID, importOperationProgressMessage(elapsed))
			}
		}
	}()
	return func() {
		close(done)
	}
}

func importOperationProgressMessage(elapsed time.Duration) string {
	if elapsed < time.Second {
		return "import started; waiting for source build or image push"
	}
	return fmt.Sprintf("import still running (%s); waiting for source build or image push", formatOperationProgressDuration(elapsed))
}

func formatOperationProgressDuration(value time.Duration) string {
	value = value.Round(time.Second)
	if value < time.Minute {
		return fmt.Sprintf("%ds", int(value.Seconds()))
	}
	if value < time.Hour {
		minutes := int(value / time.Minute)
		seconds := int((value % time.Minute) / time.Second)
		if seconds == 0 {
			return fmt.Sprintf("%dm", minutes)
		}
		return fmt.Sprintf("%dm%02ds", minutes, seconds)
	}
	hours := int(value / time.Hour)
	minutes := int((value % time.Hour) / time.Minute)
	if minutes == 0 {
		return fmt.Sprintf("%dh", hours)
	}
	return fmt.Sprintf("%dh%02dm", hours, minutes)
}

func (s *Service) updateOperationProgress(operationID, message string) {
	if s == nil || s.Store == nil {
		return
	}
	if _, err := s.Store.UpdateOperationProgress(operationID, message); err != nil {
		if errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrInvalidInput) {
			return
		}
		if s.Logger != nil {
			s.Logger.Printf("update operation %s progress failed: %v", operationID, err)
		}
	}
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

func shouldAutoBackgroundImportedApp(source model.AppSource, result sourceimport.GitHubImportResult) bool {
	if strings.TrimSpace(source.ComposeService) != "" {
		return false
	}
	return !result.ExposesPublicService
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

func validateImportedManagedImageOutput(op model.Operation, queuedSource model.AppSource, output sourceimport.GitHubSourceImportOutput) error {
	sourceType := importOutputSourceType(output, queuedSource)
	buildStrategy := importOutputBuildStrategy(output, queuedSource)
	composeService := importOutputComposeService(output, queuedSource)
	if strings.TrimSpace(output.ImportResult.ImageRef) != "" || strings.TrimSpace(output.Source.ResolvedImageRef) != "" {
		if !importRequiresBuilderJobEvidence(sourceType, buildStrategy) || strings.TrimSpace(output.ImportResult.BuildJobName) != "" {
			return nil
		}
		return fmt.Errorf(
			"import operation %s importer did not report builder job evidence (source_type=%s build_strategy=%s compose_service=%s); refusing to continue with an unverified image build",
			op.ID,
			sourceType,
			buildStrategy,
			composeService,
		)
	}
	return fmt.Errorf(
		"import operation %s importer did not report a managed image reference (source_type=%s build_strategy=%s compose_service=%s); refusing to infer one from queued metadata",
		op.ID,
		sourceType,
		buildStrategy,
		composeService,
	)
}

func importRequiresBuilderJobEvidence(sourceType, buildStrategy string) bool {
	sourceType = strings.TrimSpace(sourceType)
	buildStrategy = strings.TrimSpace(buildStrategy)
	if sourceType != model.AppSourceTypeUpload {
		return false
	}
	switch buildStrategy {
	case model.AppBuildStrategyDockerfile, model.AppBuildStrategyBuildpacks, model.AppBuildStrategyNixpacks:
		return true
	default:
		return false
	}
}

func importOutputSourceType(output sourceimport.GitHubSourceImportOutput, queuedSource model.AppSource) string {
	if sourceType := strings.TrimSpace(output.Source.Type); sourceType != "" {
		return sourceType
	}
	return strings.TrimSpace(queuedSource.Type)
}

func importOutputBuildStrategy(output sourceimport.GitHubSourceImportOutput, queuedSource model.AppSource) string {
	if buildStrategy := strings.TrimSpace(output.ImportResult.BuildStrategy); buildStrategy != "" {
		return buildStrategy
	}
	if buildStrategy := strings.TrimSpace(output.Source.BuildStrategy); buildStrategy != "" {
		return buildStrategy
	}
	return strings.TrimSpace(queuedSource.BuildStrategy)
}

func importOutputComposeService(output sourceimport.GitHubSourceImportOutput, queuedSource model.AppSource) string {
	if composeService := strings.TrimSpace(output.Source.ComposeService); composeService != "" {
		return composeService
	}
	return strings.TrimSpace(queuedSource.ComposeService)
}

const (
	defaultImportImageInspectRetryDelay  = 250 * time.Millisecond
	defaultImportImageInspectMaxAttempts = 4
)

func (s *Service) resolveImportedManagedImageRef(
	ctx context.Context,
	app model.App,
	queuedSource model.AppSource,
	importedSource model.AppSource,
	importResultImageRef string,
) (string, string, error) {
	seen := make(map[string]struct{})
	orderedCandidates := make([]string, 0, 4)
	appendCandidate := func(candidate string) {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			return
		}
		if _, ok := seen[candidate]; ok {
			return
		}
		seen[candidate] = struct{}{}
		orderedCandidates = append(orderedCandidates, candidate)
	}
	appendCandidate(importResultImageRef)
	appendCandidate(importedSource.ResolvedImageRef)

	importedForInference := importedSource
	importedForInference.ResolvedImageRef = ""
	queuedForInference := queuedSource
	queuedForInference.ResolvedImageRef = ""

	appendInferredCandidate := func(source model.AppSource) {
		candidate := strings.TrimSpace(appimages.ManagedImageRefForSource(app, &source, "", s.registryPushBase, s.registryPullBase))
		if candidate == "" {
			return
		}
		appendCandidate(candidate)
	}
	appendInferredCandidate(importedForInference)
	appendInferredCandidate(queuedForInference)

	if len(orderedCandidates) == 0 {
		return "", "", fmt.Errorf("import completed without an image reference and no managed image candidate could be inferred")
	}
	if s.inspectManagedImage == nil {
		return "", "", fmt.Errorf("import completed but controller image inspection is not configured, so image availability could not be confirmed for: %s", strings.Join(orderedCandidates, ", "))
	}

	var inspectErr error
	for _, candidate := range orderedCandidates {
		runtimeImageRef, err := s.rewriteImportedRuntimeImageRef(ctx, candidate)
		if err != nil {
			if s.Logger != nil {
				s.Logger.Printf("ignore invalid imported managed image ref for app %s candidate=%s: %v", app.ID, candidate, err)
			}
			continue
		}
		exists, lastErr := s.inspectManagedImageWithRetry(ctx, candidate)
		if lastErr != nil && s.Logger != nil {
			s.Logger.Printf("inspect imported managed image failed for app %s candidate=%s: %v", app.ID, candidate, lastErr)
		}
		if lastErr != nil && inspectErr == nil {
			inspectErr = fmt.Errorf("inspect image %s: %w", candidate, lastErr)
		}
		if !exists {
			continue
		}
		return candidate, runtimeImageRef, nil
	}
	if inspectErr != nil {
		return "", "", fmt.Errorf("import completed but managed image availability could not be confirmed: %w", inspectErr)
	}
	return "", "", fmt.Errorf(
		"import completed but managed image candidates were not confirmed in the registry after %d attempts: %s",
		s.importImageInspectAttempts(),
		strings.Join(orderedCandidates, ", "),
	)
}

func (s *Service) rewriteImportedRuntimeImageRef(ctx context.Context, imageRef string) (string, error) {
	candidate := strings.TrimSpace(imageRef)
	if candidate == "" {
		return "", fmt.Errorf("imported image reference is empty")
	}

	if s != nil && s.resolveManagedImageDigestRef != nil {
		digestRef, err := s.resolveManagedImageDigestRef(ctx, candidate)
		if err != nil {
			if s.Logger != nil {
				s.Logger.Printf("skip digest pin for imported managed image %s: %v", candidate, err)
			}
		} else if strings.TrimSpace(digestRef) != "" {
			candidate = strings.TrimSpace(digestRef)
		}
	}

	return rewriteImportedImageRef(candidate, s.registryPushBase, s.registryPullBase)
}

func (s *Service) inspectManagedImageWithRetry(ctx context.Context, candidate string) (bool, error) {
	attempts := s.importImageInspectAttempts()
	delay := s.importImageInspectDelay()
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		exists, _, err := s.inspectManagedImage(ctx, candidate)
		if err == nil && exists {
			return true, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = nil
		}
		if attempt == attempts {
			break
		}
		if !sleepContext(ctx, delay) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return false, ctxErr
			}
			return false, lastErr
		}
	}
	return false, lastErr
}

func (s *Service) importImageInspectAttempts() int {
	if s == nil || s.importImageInspectMaxAttempts <= 0 {
		return defaultImportImageInspectMaxAttempts
	}
	return s.importImageInspectMaxAttempts
}

func (s *Service) importImageInspectDelay() time.Duration {
	if s == nil || s.importImageInspectRetryDelay <= 0 {
		return defaultImportImageInspectRetryDelay
	}
	return s.importImageInspectRetryDelay
}

func restoreQueuedSourceMetadata(imported model.AppSource, queued model.AppSource) model.AppSource {
	if strings.TrimSpace(imported.Type) == "" {
		imported.Type = strings.TrimSpace(queued.Type)
	}
	if strings.TrimSpace(imported.RepoURL) == "" {
		imported.RepoURL = strings.TrimSpace(queued.RepoURL)
	}
	if strings.TrimSpace(imported.RepoBranch) == "" {
		imported.RepoBranch = strings.TrimSpace(queued.RepoBranch)
	}
	if strings.TrimSpace(imported.RepoAuthToken) == "" {
		imported.RepoAuthToken = strings.TrimSpace(queued.RepoAuthToken)
	}
	if strings.TrimSpace(imported.ImageRef) == "" {
		imported.ImageRef = strings.TrimSpace(queued.ImageRef)
	}
	if strings.TrimSpace(imported.UploadID) == "" {
		imported.UploadID = strings.TrimSpace(queued.UploadID)
	}
	if strings.TrimSpace(imported.UploadFilename) == "" {
		imported.UploadFilename = strings.TrimSpace(queued.UploadFilename)
	}
	if strings.TrimSpace(imported.ArchiveSHA256) == "" {
		imported.ArchiveSHA256 = strings.TrimSpace(queued.ArchiveSHA256)
	}
	if imported.ArchiveSizeBytes <= 0 {
		imported.ArchiveSizeBytes = queued.ArchiveSizeBytes
	}
	if strings.TrimSpace(imported.SourceDir) == "" {
		imported.SourceDir = strings.TrimSpace(queued.SourceDir)
	}
	if strings.TrimSpace(imported.BuildStrategy) == "" {
		imported.BuildStrategy = strings.TrimSpace(queued.BuildStrategy)
	}
	if strings.TrimSpace(imported.CommitSHA) == "" {
		imported.CommitSHA = strings.TrimSpace(queued.CommitSHA)
	}
	if strings.TrimSpace(imported.CommitCommittedAt) == "" {
		imported.CommitCommittedAt = strings.TrimSpace(queued.CommitCommittedAt)
	}
	if strings.TrimSpace(imported.DockerfilePath) == "" {
		imported.DockerfilePath = strings.TrimSpace(queued.DockerfilePath)
	}
	if strings.TrimSpace(imported.BuildContextDir) == "" {
		imported.BuildContextDir = strings.TrimSpace(queued.BuildContextDir)
	}
	if strings.TrimSpace(imported.DetectedProvider) == "" {
		imported.DetectedProvider = strings.TrimSpace(queued.DetectedProvider)
	}
	if strings.TrimSpace(imported.DetectedStack) == "" {
		imported.DetectedStack = strings.TrimSpace(queued.DetectedStack)
	}
	imported.ImageNameSuffix = strings.TrimSpace(queued.ImageNameSuffix)
	imported.ComposeService = strings.TrimSpace(queued.ComposeService)
	if len(queued.ComposeDependsOn) > 0 {
		imported.ComposeDependsOn = append([]string(nil), queued.ComposeDependsOn...)
	} else {
		imported.ComposeDependsOn = nil
	}
	return imported
}

func persistedOriginSourceAfterImport(desiredOrigin *model.AppSource, queuedBuild model.AppSource, importedBuild model.AppSource) *model.AppSource {
	if desiredOrigin == nil {
		origin := importedBuild
		return &origin
	}
	if originSourceShouldAdoptImportedBuild(*desiredOrigin, queuedBuild) {
		origin := restoreQueuedSourceMetadata(importedBuild, *desiredOrigin)
		return &origin
	}
	return model.CloneAppSource(desiredOrigin)
}

func originSourceShouldAdoptImportedBuild(origin model.AppSource, queuedBuild model.AppSource) bool {
	originType := strings.TrimSpace(origin.Type)
	queuedType := strings.TrimSpace(queuedBuild.Type)
	if originType == "" || queuedType == "" || originType != queuedType {
		return false
	}
	switch originType {
	case model.AppSourceTypeGitHubPublic, model.AppSourceTypeGitHubPrivate:
		return strings.EqualFold(normalizeImportSourceRepoURL(origin.RepoURL), normalizeImportSourceRepoURL(queuedBuild.RepoURL))
	case model.AppSourceTypeUpload:
		return strings.TrimSpace(origin.UploadID) != "" &&
			strings.EqualFold(strings.TrimSpace(origin.UploadID), strings.TrimSpace(queuedBuild.UploadID))
	case model.AppSourceTypeDockerImage:
		return strings.EqualFold(strings.TrimSpace(origin.ImageRef), strings.TrimSpace(queuedBuild.ImageRef))
	default:
		return false
	}
}

func normalizeImportSourceRepoURL(raw string) string {
	raw = strings.TrimSpace(strings.TrimSuffix(raw, ".git"))
	lower := strings.ToLower(raw)
	lower = strings.TrimPrefix(lower, "https://")
	lower = strings.TrimPrefix(lower, "http://")
	lower = strings.TrimPrefix(lower, "ssh://")
	lower = strings.TrimPrefix(lower, "git@github.com:")
	lower = strings.TrimPrefix(lower, "github.com/")
	return lower
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
		if model.AppHasClusterService(candidate.Spec) {
			if aliasName := runtime.ComposeServiceAliasName(candidate.ProjectID, composeService); aliasName != "" {
				appHosts[composeService] = aliasName
			}
		}
		if postgres := appOwnedPostgresSpec(candidate); postgres != nil {
			managedPostgresByOwner[composeService] = *postgres
		}
	}
	if len(appHosts) == 0 {
		appHosts = nil
	}
	if len(managedPostgresByOwner) == 0 {
		managedPostgresByOwner = nil
	}
	if appHosts == nil && managedPostgresByOwner == nil {
		return nil, nil, nil
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

func controllerReachableImportImageRef(imageRef, pushBase, pullBase string) string {
	imageRef = strings.TrimSpace(imageRef)
	pushBase = strings.Trim(strings.TrimSpace(pushBase), "/")
	pullBase = strings.Trim(strings.TrimSpace(pullBase), "/")
	if imageRef == "" || pushBase == "" || pullBase == "" || pullBase == pushBase {
		return imageRef
	}
	prefix := pullBase + "/"
	if !strings.HasPrefix(imageRef, prefix) {
		return imageRef
	}
	return pushBase + "/" + strings.TrimPrefix(imageRef, prefix)
}
