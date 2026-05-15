package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Service) syncGitHubApps(ctx context.Context) error {
	apps, err := s.Store.ListApps("", true)
	if err != nil {
		return fmt.Errorf("list apps for github sync: %w", err)
	}

	currentTime := time.Now()
	if s.now != nil {
		currentTime = s.now()
	}

	for _, app := range apps {
		originSource := model.AppOriginSource(app)
		if !shouldAutoSyncGitHubApp(app) {
			continue
		}
		hasActiveOp, err := s.Store.HasActiveOperationByApp(app.TenantID, true, app.ID)
		if err != nil {
			s.Logger.Printf("github sync active operation check failed for app=%s: %v", app.ID, err)
			continue
		}
		if hasActiveOp {
			continue
		}

		checkCtx, cancel := context.WithTimeout(ctx, s.Config.GitHubSyncTimeout)
		latestCommit, resolvedBranch, err := s.resolveLatestGitHubCommit(checkCtx, *originSource)
		cancel()
		if err != nil {
			s.Logger.Printf(
				"github sync check failed for app=%s repo=%s branch=%s: %v",
				app.ID,
				strings.TrimSpace(originSource.RepoURL),
				strings.TrimSpace(originSource.RepoBranch),
				err,
			)
			continue
		}
		latestCommit = strings.TrimSpace(latestCommit)
		if latestCommit == "" || latestCommit == strings.TrimSpace(originSource.CommitSHA) {
			continue
		}
		ops, err := s.Store.ListOperationsWithDesiredSourceByApp(app.TenantID, true, app.ID)
		if err != nil {
			s.Logger.Printf("github sync tracked commit lookup failed for app=%s latest_commit=%s: %v", app.ID, shortCommitSHA(latestCommit), err)
			continue
		}
		trackedCommitStates := collectTrackedGitHubCommitStates(ops)
		var retryBaseOperation *model.Operation
		if state, found := trackedCommitStates[trackedGitHubCommitStateKey(app.ID, latestCommit)]; found {
			if !gitHubTrackedCommitRetryReady(state, currentTime, s.Config.GitHubSyncRetryBaseDelay, s.Config.GitHubSyncRetryMaxDelay) {
				continue
			}
			if strings.TrimSpace(state.lastManualOperation.ID) != "" {
				retryBaseOperation = &state.lastManualOperation
			}
		}

		op, err := s.queueGitHubAutoRebuild(app, resolvedBranch, latestCommit, retryBaseOperation)
		if err != nil {
			if errors.Is(err, store.ErrConflict) {
				continue
			}
			s.Logger.Printf(
				"github sync queue failed for app=%s repo=%s branch=%s latest_commit=%s: %v",
				app.ID,
				strings.TrimSpace(originSource.RepoURL),
				resolvedBranch,
				shortCommitSHA(latestCommit),
				err,
			)
			continue
		}

		s.Logger.Printf(
			"github sync queued rebuild for app=%s repo=%s branch=%s old_commit=%s latest_commit=%s op=%s",
			app.ID,
			strings.TrimSpace(originSource.RepoURL),
			resolvedBranch,
			shortCommitSHA(originSource.CommitSHA),
			shortCommitSHA(latestCommit),
			op.ID,
		)
	}

	return nil
}

func (s *Service) resolveLatestGitHubCommit(ctx context.Context, source model.AppSource) (string, string, error) {
	resolver := s.latestGitHubCommit
	if resolver == nil {
		return "", "", fmt.Errorf("github commit resolver is not configured")
	}
	return resolver(
		ctx,
		strings.TrimSpace(source.RepoURL),
		strings.TrimSpace(source.RepoAuthToken),
		strings.TrimSpace(source.RepoBranch),
	)
}

func trackedGitHubCommitForOperation(op model.Operation) string {
	switch op.Type {
	case model.OperationTypeImport, model.OperationTypeDeploy:
	default:
		return ""
	}
	if op.DesiredSource == nil || !model.IsGitHubAppSourceType(op.DesiredSource.Type) {
		return ""
	}
	return strings.TrimSpace(op.DesiredSource.CommitSHA)
}

func queueableGitHubSource(source model.AppSource, branch string, commit string) (model.AppSource, error) {
	if !model.IsGitHubAppSourceType(source.Type) {
		return model.AppSource{}, fmt.Errorf("unsupported source type %q", source.Type)
	}
	if strings.TrimSpace(source.RepoURL) == "" {
		return model.AppSource{}, fmt.Errorf("repo_url is required")
	}

	buildStrategy := strings.TrimSpace(source.BuildStrategy)
	if buildStrategy == "" {
		buildStrategy = model.AppBuildStrategyStaticSite
	}

	return model.AppSource{
		Type:             model.ResolveGitHubAppSourceType(source.Type, strings.TrimSpace(source.RepoAuthToken) != ""),
		RepoURL:          strings.TrimSpace(source.RepoURL),
		RepoBranch:       strings.TrimSpace(branch),
		RepoAuthToken:    strings.TrimSpace(source.RepoAuthToken),
		SourceDir:        strings.TrimSpace(source.SourceDir),
		BuildStrategy:    buildStrategy,
		CommitSHA:        strings.TrimSpace(commit),
		DockerfilePath:   strings.TrimSpace(source.DockerfilePath),
		BuildContextDir:  strings.TrimSpace(source.BuildContextDir),
		ImageNameSuffix:  strings.TrimSpace(source.ImageNameSuffix),
		ComposeService:   strings.TrimSpace(source.ComposeService),
		ComposeDependsOn: append([]string(nil), source.ComposeDependsOn...),
		DetectedProvider: strings.TrimSpace(source.DetectedProvider),
		DetectedStack:    strings.TrimSpace(source.DetectedStack),
	}, nil
}

func (s *Service) queueGitHubAutoRebuild(app model.App, branch string, commit string, retryBaseOperation *model.Operation) (model.Operation, error) {
	spec := cloneImportSpec(app.Spec)
	if strings.TrimSpace(spec.RuntimeID) == "" {
		spec.RuntimeID = "runtime_managed_shared"
	}

	originSource := model.AppOriginSource(app)
	if originSource == nil {
		return model.Operation{}, fmt.Errorf("app does not have github origin source")
	}
	sourceBase := *originSource
	if retryBaseOperation != nil {
		if retryBaseOperation.DesiredSpec != nil {
			spec = cloneImportSpec(*retryBaseOperation.DesiredSpec)
			if strings.TrimSpace(spec.RuntimeID) == "" {
				spec.RuntimeID = "runtime_managed_shared"
			}
		}
		if retryBaseOperation.DesiredSource != nil && model.IsGitHubAppSourceType(retryBaseOperation.DesiredSource.Type) {
			sourceBase = *retryBaseOperation.DesiredSource
		}
	}
	source, err := queueableGitHubSource(sourceBase, branch, commit)
	if err != nil {
		return model.Operation{}, err
	}

	return s.Store.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeImport,
		RequestedByType:     model.ActorTypeBootstrap,
		RequestedByID:       model.OperationRequestedByGitHubSyncController,
		AppID:               app.ID,
		DesiredSpec:         &spec,
		DesiredSource:       &source,
		DesiredOriginSource: model.CloneAppSource(&source),
	})
}

func shouldAutoSyncGitHubApp(app model.App) bool {
	originSource := model.AppOriginSource(app)
	if originSource == nil {
		return false
	}
	if !model.IsGitHubAppSourceType(originSource.Type) {
		return false
	}
	if strings.TrimSpace(originSource.RepoURL) == "" {
		return false
	}
	return app.Spec.Replicas > 0
}

func shortCommitSHA(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 12 {
		return value[:12]
	}
	return value
}

type trackedGitHubCommitState struct {
	consecutiveAutomaticFailures int
	lastAutomaticFailure         model.Operation
	lastManualOperation          model.Operation
}

const (
	gitHubTrackedCommitMaxAutomaticFailures       = 3
	gitHubTrackedCommitTransientFailureRetryDelay = 30 * time.Second
)

func collectTrackedGitHubCommitStates(ops []model.Operation) map[string]trackedGitHubCommitState {
	states := make(map[string]trackedGitHubCommitState)
	for _, op := range ops {
		appID := strings.TrimSpace(op.AppID)
		commit := trackedGitHubCommitForOperation(op)
		if appID == "" || commit == "" {
			continue
		}

		key := trackedGitHubCommitStateKey(appID, commit)
		state := states[key]
		if gitHubTrackedCommitResetOperation(op) {
			state.consecutiveAutomaticFailures = 0
			state.lastAutomaticFailure = model.Operation{}
		}
		if gitHubTrackedCommitManualOperation(op) {
			state.lastManualOperation = op
		}
		if gitHubTrackedCommitAutomaticFailure(op) {
			state.consecutiveAutomaticFailures++
			state.lastAutomaticFailure = op
		}
		states[key] = state
	}
	return states
}

func trackedGitHubCommitStateKey(appID, commit string) string {
	return strings.TrimSpace(appID) + "\x00" + strings.TrimSpace(commit)
}

func gitHubReleaseSucceeded(op model.Operation) bool {
	return op.Type == model.OperationTypeDeploy && op.Status == model.OperationStatusCompleted
}

func gitHubTrackedCommitResetOperation(op model.Operation) bool {
	if trackedGitHubCommitForOperation(op) == "" {
		return false
	}
	if gitHubReleaseSucceeded(op) {
		return true
	}
	return !gitHubTrackedCommitRequestedByGitHubSync(op)
}

func gitHubTrackedCommitAutomaticFailure(op model.Operation) bool {
	return trackedGitHubCommitForOperation(op) != "" &&
		gitHubTrackedCommitRequestedByGitHubSync(op) &&
		op.Status == model.OperationStatusFailed
}

func gitHubTrackedCommitManualOperation(op model.Operation) bool {
	return trackedGitHubCommitForOperation(op) != "" && !gitHubTrackedCommitRequestedByGitHubSync(op)
}

func gitHubTrackedCommitRequestedByGitHubSync(op model.Operation) bool {
	return strings.TrimSpace(op.RequestedByID) == model.OperationRequestedByGitHubSyncController
}

func gitHubTrackedCommitRetryReady(
	state trackedGitHubCommitState,
	now time.Time,
	baseDelay time.Duration,
	maxDelay time.Duration,
) bool {
	if state.consecutiveAutomaticFailures >= gitHubTrackedCommitMaxAutomaticFailures {
		return false
	}
	if state.lastAutomaticFailure.ID == "" {
		return true
	}

	retryDelay := gitHubTrackedCommitRetryDelay(baseDelay, maxDelay, state.consecutiveAutomaticFailures)
	if gitHubTrackedCommitTransientFailure(state.lastAutomaticFailure) &&
		retryDelay > gitHubTrackedCommitTransientFailureRetryDelay {
		retryDelay = gitHubTrackedCommitTransientFailureRetryDelay
	}
	if retryDelay <= 0 {
		return true
	}

	retryAnchor := state.lastAutomaticFailure.UpdatedAt
	if state.lastAutomaticFailure.CompletedAt != nil {
		retryAnchor = *state.lastAutomaticFailure.CompletedAt
	}
	return !now.Before(retryAnchor.Add(retryDelay))
}

func gitHubTrackedCommitRetryDelay(baseDelay, maxDelay time.Duration, consecutiveFailures int) time.Duration {
	if consecutiveFailures <= 0 {
		return 0
	}
	if baseDelay <= 0 {
		baseDelay = 5 * time.Minute
	}
	if maxDelay <= 0 {
		maxDelay = time.Hour
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay
	}

	delay := baseDelay
	for failures := 1; failures < consecutiveFailures; failures++ {
		if delay >= maxDelay {
			return maxDelay
		}
		if delay > maxDelay/2 {
			return maxDelay
		}
		delay *= 2
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func gitHubTrackedCommitTransientFailure(op model.Operation) bool {
	if op.ID == "" {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(op.ErrorMessage + " " + op.ResultMessage))
	for _, marker := range []string{
		"check active app operations",
		"context deadline exceeded",
		"iterate operations",
		"store timeout",
		"connection reset",
		"connection refused",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}
