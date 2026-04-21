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
	ops, err := s.Store.ListOperations("", true)
	if err != nil {
		return fmt.Errorf("list operations for github sync: %w", err)
	}

	inFlightApps := make(map[string]struct{})
	trackedCommitStates := collectTrackedGitHubCommitStates(ops)
	for _, op := range ops {
		appID := strings.TrimSpace(op.AppID)
		if appID == "" {
			continue
		}
		switch op.Status {
		case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
			inFlightApps[appID] = struct{}{}
		}
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
		if _, blocked := inFlightApps[strings.TrimSpace(app.ID)]; blocked {
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
		if state, found := trackedCommitStates[trackedGitHubCommitStateKey(app.ID, latestCommit)]; found &&
			!gitHubTrackedCommitRetryReady(state, currentTime, s.Config.GitHubSyncRetryBaseDelay, s.Config.GitHubSyncRetryMaxDelay) {
			continue
		}

		op, err := s.queueGitHubAutoRebuild(app, resolvedBranch, latestCommit)
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

func (s *Service) queueGitHubAutoRebuild(app model.App, branch string, commit string) (model.Operation, error) {
	spec := cloneImportSpec(app.Spec)
	if strings.TrimSpace(spec.RuntimeID) == "" {
		spec.RuntimeID = "runtime_managed_shared"
	}

	originSource := model.AppOriginSource(app)
	if originSource == nil {
		return model.Operation{}, fmt.Errorf("app does not have github origin source")
	}
	source, err := queueableGitHubSource(*originSource, branch, commit)
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
	latest              model.Operation
	consecutiveFailures int
}

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
		if op.Status == model.OperationStatusFailed {
			state.consecutiveFailures++
		}
		if gitHubReleaseSucceeded(op) {
			state.consecutiveFailures = 0
		}
		state.latest = op
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

func gitHubTrackedCommitRetryReady(
	state trackedGitHubCommitState,
	now time.Time,
	baseDelay time.Duration,
	maxDelay time.Duration,
) bool {
	if state.latest.Status != model.OperationStatusFailed {
		return true
	}

	retryDelay := gitHubTrackedCommitRetryDelay(baseDelay, maxDelay, state.consecutiveFailures)
	if retryDelay <= 0 {
		return true
	}

	retryAnchor := state.latest.UpdatedAt
	if state.latest.CompletedAt != nil {
		retryAnchor = *state.latest.CompletedAt
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
