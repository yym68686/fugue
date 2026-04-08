package sourceimport

import (
	"context"
	"fmt"
	"strings"

	"fugue/internal/model"
)

type GitHubSourceImportRequest struct {
	SourceType            string
	RepoURL               string
	RepoAuthToken         string
	Branch                string
	SourceDir             string
	DockerfilePath        string
	BuildContextDir       string
	BuildStrategy         string
	RegistryPushBase      string
	ImageRepository       string
	ImageNameSuffix       string
	ComposeService        string
	JobLabels             map[string]string
	PlacementNodeSelector map[string]string
	Stateful              bool
}

type GitHubSourceImportOutput struct {
	ImportResult GitHubImportResult
	Source       model.AppSource
}

func (i *Importer) ImportGitHubSource(ctx context.Context, req GitHubSourceImportRequest) (GitHubSourceImportOutput, error) {
	buildStrategy := normalizeGitHubBuildStrategy(req.BuildStrategy)
	if buildStrategy == "" {
		buildStrategy = model.AppBuildStrategyAuto
	}
	sourceType := model.ResolveGitHubAppSourceType(req.SourceType, strings.TrimSpace(req.RepoAuthToken) != "")

	switch buildStrategy {
	case model.AppBuildStrategyAuto:
		importResult, err := i.ImportGitHubAuto(ctx, GitHubAutoImportRequest{
			SourceType:            sourceType,
			RepoURL:               req.RepoURL,
			RepoAuthToken:         req.RepoAuthToken,
			Branch:                req.Branch,
			SourceDir:             req.SourceDir,
			DockerfilePath:        req.DockerfilePath,
			BuildContextDir:       req.BuildContextDir,
			RegistryPushBase:      req.RegistryPushBase,
			ImageRepository:       req.ImageRepository,
			ImageNameSuffix:       req.ImageNameSuffix,
			JobLabels:             req.JobLabels,
			PlacementNodeSelector: req.PlacementNodeSelector,
			Stateful:              req.Stateful,
		})
		if err != nil {
			return GitHubSourceImportOutput{}, err
		}
		return GitHubSourceImportOutput{
			ImportResult: importResult,
			Source: model.AppSource{
				Type:              sourceType,
				RepoURL:           strings.TrimSpace(req.RepoURL),
				RepoBranch:        importResult.Branch,
				RepoAuthToken:     strings.TrimSpace(req.RepoAuthToken),
				ResolvedImageRef:  importResult.ImageRef,
				SourceDir:         importResult.SourceDir,
				BuildStrategy:     importResult.BuildStrategy,
				CommitSHA:         importResult.CommitSHA,
				CommitCommittedAt: importResult.CommitCommittedAt,
				DockerfilePath:    importResult.DockerfilePath,
				BuildContextDir:   importResult.BuildContextDir,
				ImageNameSuffix:   strings.TrimSpace(req.ImageNameSuffix),
				ComposeService:    strings.TrimSpace(req.ComposeService),
				DetectedProvider:  strings.TrimSpace(importResult.DetectedProvider),
				DetectedStack:     strings.TrimSpace(importResult.DetectedStack),
			},
		}, nil
	case model.AppBuildStrategyStaticSite:
		importResult, err := i.ImportGitHubStaticSite(ctx, GitHubImportRequest{
			SourceType:            sourceType,
			RepoURL:               req.RepoURL,
			RepoAuthToken:         req.RepoAuthToken,
			Branch:                req.Branch,
			SourceDir:             req.SourceDir,
			RegistryPushBase:      req.RegistryPushBase,
			ImageRepository:       req.ImageRepository,
			ImageNameSuffix:       req.ImageNameSuffix,
			JobLabels:             req.JobLabels,
			PlacementNodeSelector: req.PlacementNodeSelector,
			Stateful:              req.Stateful,
		})
		if err != nil {
			return GitHubSourceImportOutput{}, err
		}
		return GitHubSourceImportOutput{
			ImportResult: importResult,
			Source: model.AppSource{
				Type:              sourceType,
				RepoURL:           strings.TrimSpace(req.RepoURL),
				RepoBranch:        importResult.Branch,
				RepoAuthToken:     strings.TrimSpace(req.RepoAuthToken),
				ResolvedImageRef:  importResult.ImageRef,
				SourceDir:         importResult.SourceDir,
				BuildStrategy:     importResult.BuildStrategy,
				CommitSHA:         importResult.CommitSHA,
				CommitCommittedAt: importResult.CommitCommittedAt,
				ImageNameSuffix:   strings.TrimSpace(req.ImageNameSuffix),
				ComposeService:    strings.TrimSpace(req.ComposeService),
				DetectedProvider:  strings.TrimSpace(importResult.DetectedProvider),
				DetectedStack:     strings.TrimSpace(importResult.DetectedStack),
			},
		}, nil
	case model.AppBuildStrategyDockerfile:
		importResult, err := i.ImportGitHubDockerfileImage(ctx, GitHubDockerImportRequest{
			SourceType:            sourceType,
			RepoURL:               req.RepoURL,
			RepoAuthToken:         req.RepoAuthToken,
			Branch:                req.Branch,
			DockerfilePath:        req.DockerfilePath,
			BuildContextDir:       req.BuildContextDir,
			RegistryPushBase:      req.RegistryPushBase,
			ImageRepository:       req.ImageRepository,
			ImageNameSuffix:       req.ImageNameSuffix,
			JobLabels:             req.JobLabels,
			PlacementNodeSelector: req.PlacementNodeSelector,
			Stateful:              req.Stateful,
		})
		if err != nil {
			return GitHubSourceImportOutput{}, err
		}
		return GitHubSourceImportOutput{
			ImportResult: importResult,
			Source: model.AppSource{
				Type:              sourceType,
				RepoURL:           strings.TrimSpace(req.RepoURL),
				RepoBranch:        importResult.Branch,
				RepoAuthToken:     strings.TrimSpace(req.RepoAuthToken),
				ResolvedImageRef:  importResult.ImageRef,
				BuildStrategy:     importResult.BuildStrategy,
				CommitSHA:         importResult.CommitSHA,
				CommitCommittedAt: importResult.CommitCommittedAt,
				DockerfilePath:    importResult.DockerfilePath,
				BuildContextDir:   importResult.BuildContextDir,
				ImageNameSuffix:   strings.TrimSpace(req.ImageNameSuffix),
				ComposeService:    strings.TrimSpace(req.ComposeService),
				DetectedProvider:  strings.TrimSpace(importResult.DetectedProvider),
				DetectedStack:     strings.TrimSpace(importResult.DetectedStack),
			},
		}, nil
	case model.AppBuildStrategyBuildpacks:
		importResult, err := i.ImportGitHubBuildpacks(ctx, GitHubBuildpacksImportRequest{
			SourceType:            sourceType,
			RepoURL:               req.RepoURL,
			RepoAuthToken:         req.RepoAuthToken,
			Branch:                req.Branch,
			SourceDir:             req.SourceDir,
			RegistryPushBase:      req.RegistryPushBase,
			ImageRepository:       req.ImageRepository,
			ImageNameSuffix:       req.ImageNameSuffix,
			JobLabels:             req.JobLabels,
			PlacementNodeSelector: req.PlacementNodeSelector,
			Stateful:              req.Stateful,
		})
		if err != nil {
			return GitHubSourceImportOutput{}, err
		}
		return GitHubSourceImportOutput{
			ImportResult: importResult,
			Source: model.AppSource{
				Type:              sourceType,
				RepoURL:           strings.TrimSpace(req.RepoURL),
				RepoBranch:        importResult.Branch,
				RepoAuthToken:     strings.TrimSpace(req.RepoAuthToken),
				ResolvedImageRef:  importResult.ImageRef,
				SourceDir:         importResult.SourceDir,
				BuildStrategy:     importResult.BuildStrategy,
				CommitSHA:         importResult.CommitSHA,
				CommitCommittedAt: importResult.CommitCommittedAt,
				ImageNameSuffix:   strings.TrimSpace(req.ImageNameSuffix),
				ComposeService:    strings.TrimSpace(req.ComposeService),
				DetectedProvider:  strings.TrimSpace(importResult.DetectedProvider),
				DetectedStack:     strings.TrimSpace(importResult.DetectedStack),
			},
		}, nil
	case model.AppBuildStrategyNixpacks:
		importResult, err := i.ImportGitHubNixpacks(ctx, GitHubNixpacksImportRequest{
			SourceType:            sourceType,
			RepoURL:               req.RepoURL,
			RepoAuthToken:         req.RepoAuthToken,
			Branch:                req.Branch,
			SourceDir:             req.SourceDir,
			RegistryPushBase:      req.RegistryPushBase,
			ImageRepository:       req.ImageRepository,
			ImageNameSuffix:       req.ImageNameSuffix,
			JobLabels:             req.JobLabels,
			PlacementNodeSelector: req.PlacementNodeSelector,
			Stateful:              req.Stateful,
		})
		if err != nil {
			return GitHubSourceImportOutput{}, err
		}
		return GitHubSourceImportOutput{
			ImportResult: importResult,
			Source: model.AppSource{
				Type:              sourceType,
				RepoURL:           strings.TrimSpace(req.RepoURL),
				RepoBranch:        importResult.Branch,
				RepoAuthToken:     strings.TrimSpace(req.RepoAuthToken),
				ResolvedImageRef:  importResult.ImageRef,
				SourceDir:         importResult.SourceDir,
				BuildStrategy:     importResult.BuildStrategy,
				CommitSHA:         importResult.CommitSHA,
				CommitCommittedAt: importResult.CommitCommittedAt,
				ImageNameSuffix:   strings.TrimSpace(req.ImageNameSuffix),
				ComposeService:    strings.TrimSpace(req.ComposeService),
				DetectedProvider:  strings.TrimSpace(importResult.DetectedProvider),
				DetectedStack:     strings.TrimSpace(importResult.DetectedStack),
			},
		}, nil
	default:
		return GitHubSourceImportOutput{}, fmt.Errorf("unsupported build strategy %q", buildStrategy)
	}
}

func normalizeGitHubBuildStrategy(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.AppBuildStrategyAuto:
		return model.AppBuildStrategyAuto
	case model.AppBuildStrategyStaticSite:
		return model.AppBuildStrategyStaticSite
	case model.AppBuildStrategyDockerfile:
		return model.AppBuildStrategyDockerfile
	case model.AppBuildStrategyBuildpacks:
		return model.AppBuildStrategyBuildpacks
	case model.AppBuildStrategyNixpacks:
		return model.AppBuildStrategyNixpacks
	default:
		return strings.TrimSpace(strings.ToLower(raw))
	}
}
