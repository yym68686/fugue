package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/store"
)

const autoGitHubSyncRequestedByID = "fugue-controller/github-sync"

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
	for _, op := range ops {
		switch op.Status {
		case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
			inFlightApps[strings.TrimSpace(op.AppID)] = struct{}{}
		}
	}

	for _, app := range apps {
		if !shouldAutoSyncGitHubApp(app) {
			continue
		}
		if _, blocked := inFlightApps[strings.TrimSpace(app.ID)]; blocked {
			continue
		}

		checkCtx, cancel := context.WithTimeout(ctx, s.Config.GitHubSyncTimeout)
		latestCommit, resolvedBranch, err := s.resolveLatestGitHubCommit(checkCtx, *app.Source)
		cancel()
		if err != nil {
			s.Logger.Printf(
				"github sync check failed for app=%s repo=%s branch=%s: %v",
				app.ID,
				strings.TrimSpace(app.Source.RepoURL),
				strings.TrimSpace(app.Source.RepoBranch),
				err,
			)
			continue
		}
		if strings.TrimSpace(latestCommit) == "" || strings.TrimSpace(latestCommit) == strings.TrimSpace(app.Source.CommitSHA) {
			continue
		}

		op, err := s.queueGitHubAutoRebuild(app, resolvedBranch)
		if err != nil {
			if errors.Is(err, store.ErrConflict) {
				continue
			}
			s.Logger.Printf(
				"github sync queue failed for app=%s repo=%s branch=%s latest_commit=%s: %v",
				app.ID,
				strings.TrimSpace(app.Source.RepoURL),
				resolvedBranch,
				shortCommitSHA(latestCommit),
				err,
			)
			continue
		}

		s.Logger.Printf(
			"github sync queued rebuild for app=%s repo=%s branch=%s old_commit=%s latest_commit=%s op=%s",
			app.ID,
			strings.TrimSpace(app.Source.RepoURL),
			resolvedBranch,
			shortCommitSHA(app.Source.CommitSHA),
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
	return resolver(ctx, strings.TrimSpace(source.RepoURL), strings.TrimSpace(source.RepoBranch))
}

func queueableGitHubSource(source model.AppSource, branch string) (model.AppSource, error) {
	if strings.TrimSpace(source.Type) != model.AppSourceTypeGitHubPublic {
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
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          strings.TrimSpace(source.RepoURL),
		RepoBranch:       strings.TrimSpace(branch),
		SourceDir:        strings.TrimSpace(source.SourceDir),
		BuildStrategy:    buildStrategy,
		DockerfilePath:   strings.TrimSpace(source.DockerfilePath),
		BuildContextDir:  strings.TrimSpace(source.BuildContextDir),
		ImageNameSuffix:  strings.TrimSpace(source.ImageNameSuffix),
		ComposeService:   strings.TrimSpace(source.ComposeService),
		DetectedProvider: strings.TrimSpace(source.DetectedProvider),
		DetectedStack:    strings.TrimSpace(source.DetectedStack),
	}, nil
}

func (s *Service) queueGitHubAutoRebuild(app model.App, branch string) (model.Operation, error) {
	spec := cloneImportSpec(app.Spec)
	if strings.TrimSpace(spec.RuntimeID) == "" {
		spec.RuntimeID = "runtime_managed_shared"
	}

	source, err := queueableGitHubSource(*app.Source, branch)
	if err != nil {
		return model.Operation{}, err
	}

	return s.Store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeBootstrap,
		RequestedByID:   autoGitHubSyncRequestedByID,
		AppID:           app.ID,
		DesiredSpec:     &spec,
		DesiredSource:   &source,
	})
}

func shouldAutoSyncGitHubApp(app model.App) bool {
	if app.Source == nil {
		return false
	}
	if strings.TrimSpace(app.Source.Type) != model.AppSourceTypeGitHubPublic {
		return false
	}
	if strings.TrimSpace(app.Source.RepoURL) == "" {
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
