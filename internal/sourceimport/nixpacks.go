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
	RepoURL          string
	Branch           string
	SourceDir        string
	DockerfilePath   string
	BuildContextDir  string
	RegistryPushBase string
	ImageRepository  string
	ImageNameSuffix  string
	JobLabels        map[string]string
	Stateful         bool
}

type GitHubNixpacksImportRequest struct {
	RepoURL          string
	Branch           string
	SourceDir        string
	RegistryPushBase string
	ImageRepository  string
	ImageNameSuffix  string
	JobLabels        map[string]string
	Stateful         bool
}

type nixpacksBuildRequest struct {
	RepoURL            string
	Branch             string
	CommitSHA          string
	SourceLabel        string
	ArchiveDownloadURL string
	SourceDir          string
	ImageRef           string
	JobLabels          map[string]string
	PodPolicy          BuilderPodPolicy
	WorkloadProfile    builderWorkloadProfile
	Placement          builderJobPlacement
}

func (i *Importer) ImportPublicGitHubAuto(ctx context.Context, req GitHubAutoImportRequest) (GitHubImportResult, error) {
	if strings.TrimSpace(req.RegistryPushBase) == "" {
		return GitHubImportResult{}, fmt.Errorf("registry push base is empty")
	}
	repo, err := i.clonePublicGitHubRepo(ctx, req.RepoURL, req.Branch, "github-auto-import-*")
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
		return importDockerfileFromClonedRepo(ctx, repo, req.RepoURL, dockerfilePath, buildContextDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, i.BuilderPolicy, req.Stateful)
	case model.AppBuildStrategyStaticSite:
		return importStaticSiteFromClonedRepo(repo, sourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix)
	case model.AppBuildStrategyBuildpacks:
		return importBuildpacksFromClonedRepo(ctx, repo, req.RepoURL, sourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, i.BuilderPolicy, req.Stateful)
	case model.AppBuildStrategyNixpacks:
		return importNixpacksFromClonedRepo(ctx, repo, req.RepoURL, sourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, i.BuilderPolicy, req.Stateful)
	default:
		return GitHubImportResult{}, fmt.Errorf("unsupported auto-detected build strategy %q", buildStrategy)
	}
}

func (i *Importer) ImportPublicGitHubNixpacks(ctx context.Context, req GitHubNixpacksImportRequest) (GitHubImportResult, error) {
	if strings.TrimSpace(req.RegistryPushBase) == "" {
		return GitHubImportResult{}, fmt.Errorf("registry push base is empty")
	}
	repo, err := i.clonePublicGitHubRepo(ctx, req.RepoURL, req.Branch, "github-nixpacks-import-*")
	if err != nil {
		return GitHubImportResult{}, err
	}
	defer releaseClonedRepo(repo)

	return importNixpacksFromClonedRepo(ctx, repo, req.RepoURL, req.SourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, i.BuilderPolicy, req.Stateful)
}

func importNixpacksFromClonedRepo(ctx context.Context, repo clonedGitHubRepo, repoURL, sourceDir, registryPushBase, imageRepository, imageNameSuffix string, jobLabels map[string]string, builderPolicy BuilderPodPolicy, stateful bool) (GitHubImportResult, error) {
	normalizedSourceDir, err := normalizeRepoSourceDir(repo.RepoDir, sourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	provider, port := detectNixpacksProviderAndPort(repo.RepoDir, normalizedSourceDir)

	imageRef := defaultImportedImageRef(registryPushBase, imageRepository, repo, imageNameSuffix)
	if err := buildAndPushNixpacksImage(ctx, nixpacksBuildRequest{
		RepoURL:   repoURL,
		Branch:    repo.Branch,
		CommitSHA: repo.CommitSHA,
		SourceDir: normalizedSourceDir,
		ImageRef:  imageRef,
		JobLabels: jobLabels,
		PodPolicy: builderPolicy,
		WorkloadProfile: builderWorkloadProfileFor(
			model.AppBuildStrategyNixpacks,
			stateful,
		),
	}); err != nil {
		return GitHubImportResult{}, err
	}

	return GitHubImportResult{
		RepoOwner:         repo.RepoOwner,
		RepoName:          repo.RepoName,
		Branch:            repo.Branch,
		CommitSHA:         repo.CommitSHA,
		CommitCommittedAt: repo.CommitCommittedAt,
		SourceDir:         normalizeImportedSourceDirValue(normalizedSourceDir),
		BuildStrategy:     model.AppBuildStrategyNixpacks,
		ImageRef:          imageRef,
		DefaultAppName:    repo.DefaultAppName,
		DetectedPort:      port,
		DetectedProvider:  provider,
		SuggestedEnv:      suggestedNixpacksEnv(port),
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

func detectAutoStaticSiteDir(repoDir string) (string, error) {
	candidates := []string{".", "dist", "build", "site"}
	for _, candidate := range candidates {
		fullPath := repoDir
		if candidate != "." {
			fullPath = filepath.Join(repoDir, candidate)
		}
		info, err := os.Stat(fullPath)
		if err != nil || !info.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(fullPath, "index.html")); err == nil {
			return fullPath, nil
		}
	}
	return "", fmt.Errorf("no ready static-site entrypoint found")
}

func detectNixpacksProviderAndPort(repoDir, sourceDir string) (string, int) {
	return detectZeroConfigProviderAndPort(repoDir, sourceDir)
}

func detectZeroConfigProviderAndPort(repoDir, sourceDir string) (string, int) {
	appDir := repoDir
	if strings.TrimSpace(sourceDir) != "" && strings.TrimSpace(sourceDir) != "." {
		appDir = filepath.Join(repoDir, filepath.FromSlash(sourceDir))
	}

	switch {
	case pathExists(filepath.Join(appDir, "package.json")):
		return "nodejs", 3000
	case pathExists(filepath.Join(appDir, "pyproject.toml")) ||
		pathExists(filepath.Join(appDir, "requirements.txt")) ||
		pathExists(filepath.Join(appDir, "Pipfile")) ||
		pathExists(filepath.Join(appDir, "manage.py")):
		return "python", 8000
	case pathExists(filepath.Join(appDir, "go.mod")):
		return "go", 8080
	case pathExists(filepath.Join(appDir, "pom.xml")) ||
		pathExists(filepath.Join(appDir, "build.gradle")) ||
		pathExists(filepath.Join(appDir, "build.gradle.kts")):
		return "java", 8080
	case pathExists(filepath.Join(appDir, "Gemfile")):
		return "ruby", 3000
	case pathExists(filepath.Join(appDir, "composer.json")):
		return "php", 8080
	case hasGlob(filepath.Join(appDir, "*.csproj")):
		return "dotnet", 8080
	case pathExists(filepath.Join(appDir, "Cargo.toml")):
		return "rust", 3000
	default:
		return "generic", 3000
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
		RepoURL:     req.RepoURL,
		CommitSHA:   req.CommitSHA,
		SourceLabel: req.SourceLabel,
	})
	_ = kubectlRun(ctx, nil, "-n", namespace, "delete", "job", jobName, "--ignore-not-found=true", "--wait=false")
	placement, releasePlacement, err := acquireBuilderPlacement(ctx, namespace, jobName, req.PodPolicy, req.WorkloadProfile)
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
	initContainers := []map[string]any{}
	if strings.TrimSpace(req.ArchiveDownloadURL) != "" {
		initContainers = buildArchiveDownloadInitContainers(req.ArchiveDownloadURL)
	} else {
		cloneArgs := gitCloneArgs(req.RepoURL, "/workspace/repo", req.Branch)
		initContainers = []map[string]any{
			{
				"name":         "git-clone",
				"image":        defaultGitCloneImage,
				"command":      append([]string{"git"}, cloneArgs...),
				"volumeMounts": []map[string]any{{"name": "workspace", "mountPath": "/workspace"}},
			},
			{
				"name":         "git-checkout",
				"image":        defaultGitCloneImage,
				"command":      []string{"git", "-C", "/workspace/repo", "checkout", strings.TrimSpace(req.CommitSHA)},
				"volumeMounts": []map[string]any{{"name": "workspace", "mountPath": "/workspace"}},
			},
		}
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
						"command":    []string{"sh", "-lc", "set -euo pipefail\nmkdir -p /workspace/generated\nnixpacks plan . --format json > /workspace/generated/nixpacks-plan.json\nnixpacks build . --out /workspace/generated\ntest -f /workspace/generated/Dockerfile\n"},
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
