package sourceimport

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	defaultBuildpacksImage            = "docker.io/library/docker:27-dind"
	defaultPackVersion                = "0.39.1"
	defaultPaketoBuilderImage         = "docker.io/paketobuildpacks/builder-jammy-base:latest"
	defaultPaketoAptBuildpack         = "paketo-buildpacks/apt"
	defaultBuildpacksContainerNetwork = "host"
)

type GitHubBuildpacksImportRequest struct {
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

type buildpacksBuildRequest struct {
	RepoURL               string
	RepoAuthToken         string
	Branch                string
	CommitSHA             string
	SourceLabel           string
	ArchiveDownloadURL    string
	SourceDir             string
	ImageRef              string
	SourceOverlayFiles    []sourceOverlayFile
	JobLabels             map[string]string
	PlacementNodeSelector map[string]string
	DetectedProvider      string
	IncludeAptBuildpack   bool
	PodPolicy             BuilderPodPolicy
	WorkloadProfile       builderWorkloadProfile
	Placement             builderJobPlacement
}

func (i *Importer) ImportGitHubBuildpacks(ctx context.Context, req GitHubBuildpacksImportRequest) (GitHubImportResult, error) {
	if strings.TrimSpace(req.RegistryPushBase) == "" {
		return GitHubImportResult{}, fmt.Errorf("registry push base is empty")
	}
	repo, err := i.cloneGitHubRepo(ctx, req.RepoURL, req.RepoAuthToken, req.Branch, "github-buildpacks-import-*")
	if err != nil {
		return GitHubImportResult{}, err
	}
	defer releaseClonedRepo(repo)

	return importBuildpacksFromClonedRepo(ctx, repo, req.RepoURL, req.RepoAuthToken, req.SourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful)
}

func importBuildpacksFromClonedRepo(ctx context.Context, repo clonedGitHubRepo, repoURL, repoAuthToken, sourceDir, registryPushBase, imageRepository, imageNameSuffix string, jobLabels, placementNodeSelector map[string]string, builderPolicy BuilderPodPolicy, stateful bool) (GitHubImportResult, error) {
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
	systemOverlayFiles, systemPackages, err := buildBuildpacksSystemPackageOverlayFiles(repo.RepoDir, normalizedSourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	sourceOverlayFiles = append(sourceOverlayFiles, systemOverlayFiles...)

	imageRef := defaultImportedImageRef(registryPushBase, imageRepository, repo, imageNameSuffix)
	if err := buildAndPushBuildpacksImage(ctx, buildpacksBuildRequest{
		RepoURL:               repoURL,
		RepoAuthToken:         repoAuthToken,
		Branch:                repo.Branch,
		CommitSHA:             repo.CommitSHA,
		SourceDir:             normalizedSourceDir,
		ImageRef:              imageRef,
		SourceOverlayFiles:    sourceOverlayFiles,
		JobLabels:             jobLabels,
		PlacementNodeSelector: placementNodeSelector,
		DetectedProvider:      provider,
		IncludeAptBuildpack:   len(systemPackages.Packages) > 0 || systemPackages.HasExplicitBuildpackApt,
		PodPolicy:             builderPolicy,
		WorkloadProfile: builderWorkloadProfileFor(
			model.AppBuildStrategyBuildpacks,
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
		BuildStrategy:           model.AppBuildStrategyBuildpacks,
		ImageRef:                imageRef,
		DefaultAppName:          repo.DefaultAppName,
		DetectedPort:            port,
		ExposesPublicService:    exposesPublicService,
		DetectedProvider:        provider,
		DetectedStack:           detectedStack,
		SuggestedEnv:            suggestedBuildpacksEnv(port),
		SuggestedStartupCommand: pythonAnalysis.SuggestedStartCommand,
	}, nil
}

func detectBuildpacksProviderAndPort(repoDir, sourceDir string) (string, int) {
	return detectZeroConfigProviderAndPort(repoDir, sourceDir)
}

func suggestedBuildpacksEnv(port int) map[string]string {
	if port <= 0 {
		return nil
	}
	return map[string]string{
		"PORT": fmt.Sprintf("%d", port),
	}
}

func buildAndPushBuildpacksImage(ctx context.Context, req buildpacksBuildRequest) error {
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
	placement, releasePlacement, err := acquireBuilderPlacement(ctx, namespace, jobName, req.PodPolicy, req.WorkloadProfile, req.PlacementNodeSelector)
	if err != nil {
		return fmt.Errorf("select builder placement: %w", err)
	}
	defer releasePlacement()
	req.Placement = placement

	jobObject, err := buildBuildpacksJobObject(namespace, jobName, req)
	if err != nil {
		return err
	}
	if err := kubectlRun(ctx, jobObject, "-n", namespace, "apply", "-f", "-"); err != nil {
		return fmt.Errorf("apply buildpacks job: %w", err)
	}
	if err := waitForBuilderJob(ctx, namespace, jobName, 30*time.Minute); err != nil {
		return fmt.Errorf("buildpacks job %s: %w", jobName, err)
	}
	return nil
}

func buildBuildpacksJobObject(namespace, jobName string, req buildpacksBuildRequest) (map[string]any, error) {
	workingDir := "/workspace/repo"
	if strings.TrimSpace(req.SourceDir) != "" && strings.TrimSpace(req.SourceDir) != "." {
		workingDir += "/" + filepath.ToSlash(strings.TrimSpace(req.SourceDir))
	}

	script := buildpacksJobScript(workingDir, req.ImageRef, req.DetectedProvider, req.IncludeAptBuildpack)
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
						{
							"name":     "docker-data",
							"emptyDir": map[string]any{},
						},
					},
					"initContainers": initContainers,
					"containers": []map[string]any{
						{
							"name":    "buildpacks",
							"image":   defaultBuildpacksImage,
							"command": []string{"sh", "-lc", script},
							"securityContext": map[string]any{
								"privileged": true,
							},
							"volumeMounts": []map[string]any{
								{"name": "workspace", "mountPath": "/workspace"},
								{"name": "docker-data", "mountPath": "/var/lib/docker"},
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

func buildpacksJobScript(workingDir, imageRef, provider string, includeAptBuildpack bool) string {
	registryHost := registryHostFromImageRef(imageRef)
	packArgs := []string{
		"build",
		shellQuoteForOverlay(imageRef),
		"--path",
		shellQuoteForOverlay(workingDir),
		"--builder",
		shellQuoteForOverlay(defaultPaketoBuilderImage),
		"--publish",
		"--trust-builder",
	}
	if includeAptBuildpack {
		if buildpackRef := paketoLanguageBuildpack(provider); buildpackRef != "" {
			packArgs = append(packArgs,
				"--buildpack", shellQuoteForOverlay(defaultPaketoAptBuildpack),
				"--buildpack", shellQuoteForOverlay(buildpackRef),
			)
		}
	}
	if isInsecureRegistryHost(registryHost) {
		packArgs = append(packArgs, "--insecure-registry", shellQuoteForOverlay(registryHost))
	}
	packArgs = append(packArgs, "--network", shellQuoteForOverlay(defaultBuildpacksContainerNetwork))
	return fmt.Sprintf(`set -euo pipefail
apk add --no-cache curl tar >/dev/null
export DOCKER_HOST=unix:///var/run/docker.sock
mkdir -p /var/run /workspace/bin
dockerd --host="$DOCKER_HOST" --data-root /var/lib/docker >/workspace/dockerd.log 2>&1 &
dockerd_pid=$!
cleanup() {
  kill "$dockerd_pid" >/dev/null 2>&1 || true
  wait "$dockerd_pid" >/dev/null 2>&1 || true
}
trap cleanup EXIT
for _ in $(seq 1 60); do
  if docker info >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker info >/dev/null 2>&1
case "$(uname -m)" in
  x86_64|amd64) pack_archive="pack-v%s-linux.tgz" ;;
  aarch64|arm64) pack_archive="pack-v%s-linux-arm64.tgz" ;;
  *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
esac
pack_url="https://github.com/buildpacks/pack/releases/download/v%s/${pack_archive}"
curl -fsSL "$pack_url" -o /tmp/pack.tgz
tar -xzf /tmp/pack.tgz -C /workspace/bin pack
/workspace/bin/pack %s
`, defaultPackVersion, defaultPackVersion, defaultPackVersion, strings.Join(packArgs, " "))
}

func paketoLanguageBuildpack(provider string) string {
	switch strings.TrimSpace(strings.ToLower(provider)) {
	case "dotnet":
		return "paketo-buildpacks/dotnet-core"
	case "go":
		return "paketo-buildpacks/go"
	case "java":
		return "paketo-buildpacks/java"
	case "nodejs":
		return "paketo-buildpacks/nodejs"
	case "php":
		return "paketo-buildpacks/php"
	case "python":
		return "paketo-buildpacks/python"
	case "ruby":
		return "paketo-buildpacks/ruby"
	default:
		return ""
	}
}
