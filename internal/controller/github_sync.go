package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Service) syncGitHubApps(ctx context.Context) error {
	apps, err := s.Store.ListAppsMetadata("", true)
	if err != nil {
		return fmt.Errorf("list apps for github sync: %w", err)
	}

	for _, app := range apps {
		if err := ctx.Err(); err != nil {
			return err
		}
		currentTime := s.gitHubSyncNow()
		wasOOMBlocked := app.Status.SourceSync != nil && app.Status.SourceSync.LastErrorCode == "builder_memory_not_increased"
		originSource := model.AppOriginSource(app)
		if !shouldAutoSyncGitHubApp(app) {
			continue
		}
		if !gitHubSourceSyncCheckReady(app.Status.SourceSync, currentTime) {
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
		if ctx.Err() != nil {
			return ctx.Err()
		}
		currentTime = s.gitHubSyncNow()
		if err != nil {
			s.recordGitHubSourceSyncFailure(ctx, app, *originSource, err, currentTime)
			continue
		}
		if !s.recordGitHubSourceSyncSuccess(ctx, app, currentTime) {
			continue
		}
		latestCommit = strings.TrimSpace(latestCommit)
		if latestCommit == "" || latestCommit == strings.TrimSpace(originSource.CommitSHA) {
			continue
		}
		// Resolve configuration again after the remote check. Never enqueue a
		// build for a source rebind or a replica pause that happened meanwhile.
		current, loadErr := s.Store.GetAppMetadata(app.ID)
		if loadErr != nil || !shouldAutoSyncGitHubApp(current) || !reflect.DeepEqual(model.AppOriginSource(current), originSource) {
			continue
		}
		app = current
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ops, err := s.Store.ListOperationsWithDesiredSourceByApp(app.TenantID, true, app.ID)
		if err != nil {
			s.Logger.Printf("github sync tracked commit lookup failed for app=%s latest_commit=%s: %v", app.ID, shortCommitSHA(latestCommit), err)
			continue
		}
		trackedCommitStates := collectTrackedGitHubCommitStates(ops)
		var retryBaseOperation *model.Operation
		state, found := trackedCommitStates[trackedGitHubCommitStateKey(app.ID, latestCommit)]
		if found {
			if strings.TrimSpace(state.lastManualOperation.ID) != "" {
				retryBaseOperation = &state.lastManualOperation
			}
		}
		buildSpec, buildSource := gitHubRetryInputs(app, retryBaseOperation)
		buildSource, err = queueableGitHubSource(buildSource, resolvedBranch, latestCommit)
		if err != nil {
			s.Logger.Printf("github sync invalid build source app=%s: %v", app.ID, err)
			continue
		}
		fingerprint := buildInputFingerprint(buildSource, buildSpec, s.effectiveImportBuilderPolicy(s.importBuilderMemoryCeilingBytes(app.TenantID)))
		ledger, historyErr := s.readBuildFailureLedger(ctx, app)
		if historyErr != nil {
			s.Logger.Printf("github sync build admission history unavailable app=%s: %v", app.ID, historyErr)
			continue
		}
		reuseFingerprint := importBuildReuseFingerprint(buildSource, buildSpec, s.effectiveImportBuilderPolicy(s.importBuilderMemoryCeilingBytes(app.TenantID)))
		reusable, reuseErr := s.reusableGitHubBuild(ctx, app, reuseFingerprint)
		if reuseErr != nil {
			s.recordGitHubReleaseDecision(ctx, app, latestCommit, fingerprint, "blocked_artifact_verification", state, len(ops))
			s.Logger.Printf("github release saved artifact verification failed app=%s: %v", app.ID, reuseErr)
			continue
		}
		deploySpec := buildSpec
		if reusable != nil {
			preview := *reusable
			preview.Type = model.OperationTypeDeploy
			preview, err = store.RebasedManagedDeploy(preview, app)
			if err != nil {
				s.recordGitHubReleaseDecision(ctx, app, latestCommit, fingerprint, "blocked_configuration_conflict", state, len(ops))
				continue
			}
			deploySpec = *preview.DesiredSpec
		}
		if billingErr := s.Store.CheckDeployBilling(ctx, app.ID, deploySpec); billingErr != nil {
			s.recordGitHubReleaseDecision(ctx, app, latestCommit, fingerprint, "blocked_deploy_admission", state, len(ops))
			s.Logger.Printf("github release waiting for deploy admission app=%s: %v", app.ID, billingErr)
			continue
		}
		if reusable == nil {
			if cachedBuildRejection(ledger, fingerprint) != nil {
				s.recordGitHubReleaseDecision(ctx, app, latestCommit, fingerprint, "blocked_build_admission", state, len(ops))
				continue
			}
			admitted, admissionErr := s.githubBuildOOMAdmission(ctx, app, buildSource, buildSpec, s.effectiveImportBuilderPolicy(s.importBuilderMemoryCeilingBytes(app.TenantID)))
			if admissionErr != nil {
				s.Logger.Printf("github sync OOM admission check failed app=%s: %v", app.ID, admissionErr)
				continue
			}
			if !admitted {
				s.recordGitHubReleaseDecision(ctx, app, latestCommit, fingerprint, "blocked_build_memory", state, len(ops))
				continue
			}
		}
		if found && !wasOOMBlocked && !buildPolicyChangedSinceFailure(ledger, state.lastAutomaticFailure, fingerprint) &&
			!(reusable != nil && gitHubTrackedCommitBillingFailure(state.lastAutomaticFailure)) &&
			!gitHubTrackedCommitRetryReady(state, currentTime, s.Config.GitHubSyncRetryBaseDelay, s.Config.GitHubSyncRetryMaxDelay) {
			s.recordGitHubReleaseDecision(ctx, app, latestCommit, fingerprint, "blocked_retry_budget_or_backoff", state, len(ops))
			continue
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
		var op model.Operation
		if reusable != nil {
			op, err = s.Store.CreateDeployOperationAfterImport(reusable.ID, model.Operation{
				TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy,
				RequestedByType: model.ActorTypeBootstrap, RequestedByID: model.OperationRequestedByGitHubSyncController,
				DesiredSpec: reusable.DesiredSpec, DesiredSource: reusable.DesiredSource, DesiredOriginSource: reusable.DesiredOriginSource,
			})
		} else {
			op, err = s.queueGitHubAutoRebuild(app, resolvedBranch, latestCommit, retryBaseOperation)
		}
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

		decision := "queued_build"
		if wasOOMBlocked {
			decision = "queued_build_after_memory_admission_recovered"
		}
		if buildPolicyChangedSinceFailure(ledger, state.lastAutomaticFailure, fingerprint) {
			decision = "queued_build_after_input_policy_change"
		}
		if reusable != nil {
			decision = "queued_deploy_with_verified_build"
		}
		s.recordGitHubReleaseDecision(ctx, app, latestCommit, fingerprint, decision, state, len(ops))
		s.Logger.Printf(
			"github sync queued release for app=%s repo=%s branch=%s old_commit=%s latest_commit=%s op=%s",
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
	if op.DesiredOriginSource != nil && model.IsGitHubAppSourceType(op.DesiredOriginSource.Type) {
		return strings.TrimSpace(op.DesiredOriginSource.CommitSHA)
	}
	if op.DesiredSource != nil && model.IsGitHubAppSourceType(op.DesiredSource.Type) {
		return strings.TrimSpace(op.DesiredSource.CommitSHA)
	}
	return ""
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
	originSource := model.AppOriginSource(app)
	if originSource == nil {
		return model.Operation{}, fmt.Errorf("app does not have github origin source")
	}
	spec, sourceBase := gitHubRetryInputs(app, retryBaseOperation)
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

func gitHubRetryInputs(app model.App, retryBaseOperation *model.Operation) (model.AppSpec, model.AppSource) {
	spec := cloneImportSpec(app.Spec)
	sourceBase := *model.AppOriginSource(app)
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
	if strings.TrimSpace(spec.RuntimeID) == "" {
		spec.RuntimeID = "runtime_managed_shared"
	}
	return spec, sourceBase
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
	return gitHubTrackedCommitManualOperation(op)
}

func gitHubTrackedCommitAutomaticFailure(op model.Operation) bool {
	return trackedGitHubCommitForOperation(op) != "" &&
		gitHubTrackedCommitRequestedByGitHubSync(op) &&
		op.Status == model.OperationStatusFailed
}

func gitHubTrackedCommitManualOperation(op model.Operation) bool {
	if trackedGitHubCommitForOperation(op) == "" || gitHubTrackedCommitRequestedByGitHubSync(op) {
		return false
	}
	return op.RequestedByType != model.ActorTypeSystem && !strings.HasPrefix(op.RequestedByID, "fugue-controller/")
}

func gitHubTrackedCommitBillingFailure(op model.Operation) bool {
	return strings.Contains(strings.ToLower(op.ErrorMessage), "billing balance depleted")
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
