package sourceimport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	defaultNixpacksImage = "ghcr.io/railwayapp/nixpacks:latest"
	defaultGitCloneImage = "alpine/git:latest"
)

type GitHubAutoImportRequest struct {
	SourceType            string
	RepoURL               string
	RepoAuthToken         string
	Branch                string
	SourceDir             string
	DockerfilePath        string
	BuildContextDir       string
	RegistryPushBase      string
	ImageRepository       string
	ImageNameSuffix       string
	JobLabels             map[string]string
	PlacementNodeSelector map[string]string
	Stateful              bool
}

type GitHubNixpacksImportRequest struct {
	SourceType            string
	RepoURL               string
	RepoAuthToken         string
	Branch                string
	SourceDir             string
	RegistryPushBase      string
	ImageRepository       string
	ImageNameSuffix       string
	JobLabels             map[string]string
	PlacementNodeSelector map[string]string
	Stateful              bool
}

type nixpacksBuildRequest struct {
	RepoURL               string
	RepoAuthToken         string
	Branch                string
	CommitSHA             string
	SourceLabel           string
	ArchiveDownloadURL    string
	SourceDir             string
	ImageRef              string
	SourceOverlayFiles    []sourceOverlayFile
	SystemPackages        []string
	JobLabels             map[string]string
	PlacementNodeSelector map[string]string
	PodPolicy             BuilderPodPolicy
	WorkloadProfile       builderWorkloadProfile
	Placement             builderJobPlacement
}

func (i *Importer) ImportGitHubAuto(ctx context.Context, req GitHubAutoImportRequest) (GitHubImportResult, error) {
	if strings.TrimSpace(req.RegistryPushBase) == "" {
		return GitHubImportResult{}, fmt.Errorf("registry push base is empty")
	}
	repo, err := i.cloneGitHubRepo(ctx, req.RepoURL, req.RepoAuthToken, req.Branch, "github-auto-import-*")
	if err != nil {
		return GitHubImportResult{}, err
	}
	defer releaseClonedRepo(repo)

	buildStrategy, sourceDir, dockerfilePath, buildContextDir, err := detectAutoImportInputs(repo.RepoDir, req.SourceDir, req.DockerfilePath, req.BuildContextDir)
	if err != nil {
		return GitHubImportResult{}, err
	}

	switch buildStrategy {
	case model.AppBuildStrategyDockerfile:
		return importDockerfileFromClonedRepo(ctx, repo, req.RepoURL, req.RepoAuthToken, dockerfilePath, buildContextDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful)
	case model.AppBuildStrategyStaticSite:
		return importStaticSiteFromClonedRepo(ctx, repo, req.RepoURL, req.RepoAuthToken, sourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful)
	case model.AppBuildStrategyBuildpacks:
		return importBuildpacksFromClonedRepo(ctx, repo, req.RepoURL, req.RepoAuthToken, sourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful)
	case model.AppBuildStrategyNixpacks:
		return importNixpacksFromClonedRepo(ctx, repo, req.RepoURL, req.RepoAuthToken, sourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful)
	default:
		return GitHubImportResult{}, fmt.Errorf("unsupported auto-detected build strategy %q", buildStrategy)
	}
}

func (i *Importer) ImportGitHubNixpacks(ctx context.Context, req GitHubNixpacksImportRequest) (GitHubImportResult, error) {
	if strings.TrimSpace(req.RegistryPushBase) == "" {
		return GitHubImportResult{}, fmt.Errorf("registry push base is empty")
	}
	repo, err := i.cloneGitHubRepo(ctx, req.RepoURL, req.RepoAuthToken, req.Branch, "github-nixpacks-import-*")
	if err != nil {
		return GitHubImportResult{}, err
	}
	defer releaseClonedRepo(repo)

	return importNixpacksFromClonedRepo(ctx, repo, req.RepoURL, req.RepoAuthToken, req.SourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful)
}

func importNixpacksFromClonedRepo(ctx context.Context, repo clonedGitHubRepo, repoURL, repoAuthToken, sourceDir, registryPushBase, imageRepository, imageNameSuffix string, jobLabels, placementNodeSelector map[string]string, builderPolicy BuilderPodPolicy, stateful bool) (GitHubImportResult, error) {
	normalizedSourceDir, err := normalizeRepoSourceDir(repo.RepoDir, sourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	provider, port, exposesPublicService := detectZeroConfigProviderAndPortSignal(repo.RepoDir, normalizedSourceDir)
	detectedStack := detectPrimaryTechStack(repo.RepoDir, normalizedSourceDir)
	sourceOverlayFiles, pythonAnalysis, err := buildPythonOverlayFiles(repo.RepoDir, normalizedSourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	systemPackages, err := analyzeSystemPackages(repo.RepoDir, normalizedSourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}

	imageRef := defaultImportedImageRef(registryPushBase, imageRepository, repo, imageNameSuffix)
	if err := buildAndPushNixpacksImage(ctx, nixpacksBuildRequest{
		RepoURL:               repoURL,
		RepoAuthToken:         repoAuthToken,
		Branch:                repo.Branch,
		CommitSHA:             repo.CommitSHA,
		SourceDir:             normalizedSourceDir,
		ImageRef:              imageRef,
		SourceOverlayFiles:    sourceOverlayFiles,
		SystemPackages:        systemPackages.Packages,
		JobLabels:             jobLabels,
		PlacementNodeSelector: placementNodeSelector,
		PodPolicy:             builderPolicy,
		WorkloadProfile: builderWorkloadProfileFor(
			model.AppBuildStrategyNixpacks,
			stateful,
		),
	}); err != nil {
		return GitHubImportResult{}, err
	}

	return GitHubImportResult{
		RepoOwner:               repo.RepoOwner,
		RepoName:                repo.RepoName,
		Branch:                  repo.Branch,
		CommitSHA:               repo.CommitSHA,
		CommitCommittedAt:       repo.CommitCommittedAt,
		SourceDir:               normalizeImportedSourceDirValue(normalizedSourceDir),
		BuildStrategy:           model.AppBuildStrategyNixpacks,
		ImageRef:                imageRef,
		DefaultAppName:          repo.DefaultAppName,
		DetectedPort:            port,
		ExposesPublicService:    exposesPublicService,
		DetectedProvider:        provider,
		DetectedStack:           detectedStack,
		SuggestedEnv:            suggestedNixpacksEnv(port),
		SuggestedStartupCommand: pythonAnalysis.SuggestedStartCommand,
	}, nil
}

func detectAutoImportInputs(repoDir, requestedSourceDir, requestedDockerfilePath, requestedBuildContextDir string) (string, string, string, string, error) {
	if strings.TrimSpace(requestedDockerfilePath) != "" || strings.TrimSpace(requestedBuildContextDir) != "" {
		dockerfilePath, buildContextDir, err := detectDockerBuildInputs(repoDir, requestedDockerfilePath, requestedBuildContextDir)
		if err != nil {
			return "", "", "", "", err
		}
		return model.AppBuildStrategyDockerfile, "", dockerfilePath, buildContextDir, nil
	}

	requestedSourceDir = strings.TrimSpace(requestedSourceDir)
	if requestedSourceDir != "" {
		defaultDockerfileInSourceDir := filepath.ToSlash(filepath.Join(requestedSourceDir, "Dockerfile"))
		if _, _, err := detectDockerBuildInputs(repoDir, defaultDockerfileInSourceDir, requestedSourceDir); err == nil {
			return model.AppBuildStrategyDockerfile, "", defaultDockerfileInSourceDir, filepath.ToSlash(requestedSourceDir), nil
		}
		if staticDir, err := detectStaticSiteDir(repoDir, requestedSourceDir); err == nil {
			return model.AppBuildStrategyStaticSite, relativeImportedSourceDir(repoDir, staticDir), "", "", nil
		}
		normalizedSourceDir, err := normalizeRepoSourceDir(repoDir, requestedSourceDir)
		if err != nil {
			return "", "", "", "", err
		}
		if buildpacksSupported(repoDir, normalizedSourceDir) {
			return model.AppBuildStrategyBuildpacks, normalizedSourceDir, "", "", nil
		}
		return model.AppBuildStrategyNixpacks, normalizedSourceDir, "", "", nil
	}

	if _, err := os.Stat(filepath.Join(repoDir, "Dockerfile")); err == nil {
		dockerfilePath, buildContextDir, err := detectDockerBuildInputs(repoDir, "", "")
		if err != nil {
			return "", "", "", "", err
		}
		return model.AppBuildStrategyDockerfile, "", dockerfilePath, buildContextDir, nil
	}

	if staticDir, err := detectAutoStaticSiteDir(repoDir); err == nil {
		return model.AppBuildStrategyStaticSite, relativeImportedSourceDir(repoDir, staticDir), "", "", nil
	}

	if buildpacksSupported(repoDir, ".") {
		return model.AppBuildStrategyBuildpacks, ".", "", "", nil
	}

	return model.AppBuildStrategyNixpacks, ".", "", "", nil
}

func detectNixpacksProviderAndPort(repoDir, sourceDir string) (string, int) {
	return detectZeroConfigProviderAndPort(repoDir, sourceDir)
}

func detectZeroConfigProviderAndPort(repoDir, sourceDir string) (string, int) {
	provider, port, _ := detectZeroConfigProviderAndPortSignal(repoDir, sourceDir)
	return provider, port
}

func detectZeroConfigProviderAndPortSignal(repoDir, sourceDir string) (string, int, bool) {
	appDir := repoDir
	if strings.TrimSpace(sourceDir) != "" && strings.TrimSpace(sourceDir) != "." {
		appDir = filepath.Join(repoDir, filepath.FromSlash(sourceDir))
	}

	pythonAnalysis, err := analyzePythonProjectInDir(appDir)
	if err != nil {
		pythonAnalysis = pythonProjectAnalysis{}
	}

	switch {
	case pathExists(filepath.Join(appDir, "package.json")):
		return "nodejs", 3000, detectNodePublicService(appDir)
	case pythonAnalysis.IsPythonProject:
		exposesPublicService := pythonAnalysis.HasWebEntrypoint
		if pythonProjectPrefersBackgroundNetwork(pythonAnalysis) {
			exposesPublicService = false
		}
		if pythonAnalysis.DetectedPort > 0 {
			return "python", pythonAnalysis.DetectedPort, exposesPublicService
		}
		return "python", 8000, exposesPublicService
	case pathExists(filepath.Join(appDir, "go.mod")):
		return "go", 8080, detectGoPublicService(appDir)
	case pathExists(filepath.Join(appDir, "pom.xml")) ||
		pathExists(filepath.Join(appDir, "build.gradle")) ||
		pathExists(filepath.Join(appDir, "build.gradle.kts")):
		return "java", 8080, detectJavaPublicService(appDir)
	case pathExists(filepath.Join(appDir, "Gemfile")):
		return "ruby", 3000, detectRubyPublicService(appDir)
	case pathExists(filepath.Join(appDir, "composer.json")):
		return "php", 8080, detectPHPPublicService(appDir)
	case hasGlob(filepath.Join(appDir, "*.csproj")):
		return "dotnet", 8080, detectDotnetPublicService(appDir)
	case pathExists(filepath.Join(appDir, "Cargo.toml")):
		return "rust", 3000, detectRustPublicService(appDir)
	default:
		return "generic", 3000, false
	}
}

func buildpacksSupported(repoDir, sourceDir string) bool {
	provider, _ := detectZeroConfigProviderAndPort(repoDir, sourceDir)
	switch provider {
	case "nodejs", "python", "go", "java", "ruby", "php", "dotnet":
		return true
	default:
		return false
	}
}

func suggestedNixpacksEnv(port int) map[string]string {
	if port <= 0 {
		return nil
	}
	return map[string]string{
		"PORT": strconv.Itoa(port),
	}
}

func normalizeImportedSourceDirValue(sourceDir string) string {
	sourceDir = strings.TrimSpace(sourceDir)
	if sourceDir == "." {
		return ""
	}
	return sourceDir
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func hasGlob(pattern string) bool {
	matches, err := filepath.Glob(pattern)
	return err == nil && len(matches) > 0
}

func buildAndPushNixpacksImage(ctx context.Context, req nixpacksBuildRequest) error {
	namespace, err := currentNamespace()
	if err != nil {
		return err
	}

	jobName := buildJobName(dockerfileBuildRequest{
		RepoURL:            req.RepoURL,
		CommitSHA:          req.CommitSHA,
		SourceLabel:        req.SourceLabel,
		ArchiveDownloadURL: req.ArchiveDownloadURL,
		BuildContextDir:    req.SourceDir,
		ImageRef:           req.ImageRef,
		JobLabels:          req.JobLabels,
	})
	if err := deleteBuilderJobIfPresent(ctx, namespace, jobName); err != nil {
		return err
	}
	placement, releasePlacement, err := acquireBuilderPlacement(ctx, namespace, jobName, req.PodPolicy, req.WorkloadProfile, req.PlacementNodeSelector)
	if err != nil {
		return fmt.Errorf("select builder placement: %w", err)
	}
	defer releasePlacement()
	req.Placement = placement

	jobObject, err := buildNixpacksJobObject(namespace, jobName, req)
	if err != nil {
		return err
	}
	if err := kubectlRun(ctx, jobObject, "-n", namespace, "apply", "-f", "-"); err != nil {
		return fmt.Errorf("apply nixpacks job: %w", err)
	}
	if err := waitForBuilderJob(ctx, namespace, jobName, 30*time.Minute); err != nil {
		return fmt.Errorf("nixpacks job %s: %w", jobName, err)
	}
	return nil
}

func buildNixpacksJobObject(namespace, jobName string, req nixpacksBuildRequest) (map[string]any, error) {
	workingDir := "/workspace/repo"
	if strings.TrimSpace(req.SourceDir) != "" && strings.TrimSpace(req.SourceDir) != "." {
		workingDir += "/" + filepath.ToSlash(strings.TrimSpace(req.SourceDir))
	}
	nixpacksScript := buildNixpacksScript(req.SystemPackages)
	initContainers := []map[string]any{}
	if strings.TrimSpace(req.ArchiveDownloadURL) != "" {
		initContainers = buildArchiveDownloadInitContainers(req.ArchiveDownloadURL)
	} else {
		initContainers = buildGitCloneInitContainers(req.RepoURL, req.Branch, req.CommitSHA, req.RepoAuthToken)
	}
	sourceOverlayContainer, err := buildSourceOverlayInitContainer(workingDir, req.SourceOverlayFiles)
	if err != nil {
		return nil, err
	}
	if sourceOverlayContainer != nil {
		initContainers = append(initContainers, sourceOverlayContainer)
	}

	jobObject := map[string]any{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata": map[string]any{
			"name":      jobName,
			"namespace": namespace,
			"labels": map[string]string{
				"app.kubernetes.io/managed-by": "fugue",
				"app.kubernetes.io/component":  "builder",
			},
		},
		"spec": map[string]any{
			"backoffLimit":            0,
			"ttlSecondsAfterFinished": 3600,
			"template": map[string]any{
				"spec": map[string]any{
					"restartPolicy": "Never",
					"volumes": []map[string]any{
						{
							"name":     "workspace",
							"emptyDir": map[string]any{},
						},
					},
					"initContainers": append(initContainers, map[string]any{
						"name":       "nixpacks",
						"image":      defaultNixpacksImage,
						"command":    []string{"sh", "-lc", nixpacksScript},
						"workingDir": workingDir,
						"volumeMounts": []map[string]any{
							{"name": "workspace", "mountPath": "/workspace"},
						},
					}),
					"containers": []map[string]any{
						{
							"name":  "kaniko",
							"image": defaultKanikoImage,
							"args":  kanikoDestinationArgs(req.ImageRef, "--context=dir:///workspace/generated", "--dockerfile=/workspace/generated/Dockerfile"),
							"volumeMounts": []map[string]any{
								{"name": "workspace", "mountPath": "/workspace"},
							},
						},
					},
				},
			},
		},
	}
	metadata := jobObject["metadata"].(map[string]any)
	metadata["labels"] = mergeBuilderLabels(metadata["labels"].(map[string]string), req.JobLabels)
	podSpec := jobObject["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	applyBuilderPodPolicy(podSpec, req.PodPolicy, req.WorkloadProfile)
	applyBuilderPlacement(podSpec, req.Placement)
	return jobObject, nil
}

func buildNixpacksScript(systemPackages []string) string {
	aptArgs := ""
	if len(systemPackages) > 0 {
		quotedPackages := make([]string, 0, len(systemPackages))
		for _, pkg := range systemPackages {
			pkg = strings.TrimSpace(pkg)
			if pkg == "" {
				continue
			}
			quotedPackages = append(quotedPackages, shellQuoteForOverlay(pkg))
		}
		if len(quotedPackages) > 0 {
			aptArgs = " --apt " + strings.Join(quotedPackages, " ")
		}
	}
	return "set -eu\nmkdir -p /workspace/generated\nnixpacks plan . --format json" + aptArgs + " > /workspace/generated/nixpacks-plan.json\nnixpacks build . --out /workspace/generated" + aptArgs + "\ntest -f /workspace/generated/Dockerfile\n"
}
