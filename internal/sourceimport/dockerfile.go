package sourceimport

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	defaultKanikoImage          = "gcr.io/kaniko-project/executor:v1.23.2-debug"
	serviceAccountNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

type GitHubDockerImportRequest struct {
	RepoURL               string
	Branch                string
	DockerfilePath        string
	BuildContextDir       string
	RegistryPushBase      string
	ImageRepository       string
	ImageNameSuffix       string
	JobLabels             map[string]string
	PlacementNodeSelector map[string]string
	Stateful              bool
}

func (i *Importer) ImportPublicGitHubDockerfileImage(ctx context.Context, req GitHubDockerImportRequest) (GitHubImportResult, error) {
	if strings.TrimSpace(req.RegistryPushBase) == "" {
		return GitHubImportResult{}, fmt.Errorf("registry push base is empty")
	}
	repo, err := i.clonePublicGitHubRepo(ctx, req.RepoURL, req.Branch, "github-docker-import-*")
	if err != nil {
		return GitHubImportResult{}, err
	}
	defer releaseClonedRepo(repo)

	return importDockerfileFromClonedRepo(ctx, repo, req.RepoURL, req.DockerfilePath, req.BuildContextDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful)
}

type dockerfileBuildRequest struct {
	RepoURL               string
	Branch                string
	CommitSHA             string
	SourceLabel           string
	ArchiveDownloadURL    string
	DockerfilePath        string
	BuildContextDir       string
	ImageRef              string
	JobLabels             map[string]string
	PlacementNodeSelector map[string]string
	PodPolicy             BuilderPodPolicy
	WorkloadProfile       builderWorkloadProfile
	Placement             builderJobPlacement
}

func detectDockerBuildInputs(repoDir, dockerfilePath, buildContextDir string) (string, string, error) {
	dockerfilePath = strings.TrimSpace(dockerfilePath)
	if dockerfilePath == "" {
		dockerfilePath = "Dockerfile"
	}
	buildContextDir = strings.TrimSpace(buildContextDir)
	if buildContextDir == "" {
		buildContextDir = "."
	}

	dockerfileFullPath, err := secureRepoJoin(repoDir, dockerfilePath)
	if err != nil {
		return "", "", err
	}
	if info, err := os.Stat(dockerfileFullPath); err != nil || info.IsDir() {
		return "", "", fmt.Errorf("dockerfile_path %q does not exist", dockerfilePath)
	}

	buildContextFullPath, err := secureRepoJoin(repoDir, buildContextDir)
	if err != nil {
		return "", "", err
	}
	if info, err := os.Stat(buildContextFullPath); err != nil || !info.IsDir() {
		return "", "", fmt.Errorf("build_context_dir %q does not exist", buildContextDir)
	}

	relDockerfile, err := filepath.Rel(repoDir, dockerfileFullPath)
	if err != nil {
		return "", "", fmt.Errorf("rel dockerfile path: %w", err)
	}
	relContext, err := filepath.Rel(repoDir, buildContextFullPath)
	if err != nil {
		return "", "", fmt.Errorf("rel build context path: %w", err)
	}
	relDockerfile = filepath.ToSlash(relDockerfile)
	relContext = filepath.ToSlash(relContext)
	if relContext == "" {
		relContext = "."
	}
	return relDockerfile, relContext, nil
}

func importDockerfileFromClonedRepo(ctx context.Context, repo clonedGitHubRepo, repoURL, dockerfilePath, buildContextDir, registryPushBase, imageRepository, imageNameSuffix string, jobLabels, placementNodeSelector map[string]string, builderPolicy BuilderPodPolicy, stateful bool) (GitHubImportResult, error) {
	dockerfilePath, buildContextDir, err := detectDockerBuildInputs(repo.RepoDir, dockerfilePath, buildContextDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	detectedPort, err := detectDockerfilePort(repo.RepoDir, dockerfilePath)
	if err != nil {
		return GitHubImportResult{}, err
	}
	detectedStack := detectPrimaryTechStack(repo.RepoDir, buildContextDir)

	imageRef := defaultImportedImageRef(registryPushBase, imageRepository, repo, imageNameSuffix)
	if err := buildAndPushDockerfileImage(ctx, dockerfileBuildRequest{
		RepoURL:               repoURL,
		Branch:                repo.Branch,
		CommitSHA:             repo.CommitSHA,
		DockerfilePath:        dockerfilePath,
		BuildContextDir:       buildContextDir,
		ImageRef:              imageRef,
		JobLabels:             jobLabels,
		PlacementNodeSelector: placementNodeSelector,
		PodPolicy:             builderPolicy,
		WorkloadProfile:       builderWorkloadProfileFor(model.AppBuildStrategyDockerfile, stateful),
	}); err != nil {
		return GitHubImportResult{}, err
	}

	return GitHubImportResult{
		RepoOwner:         repo.RepoOwner,
		RepoName:          repo.RepoName,
		Branch:            repo.Branch,
		CommitSHA:         repo.CommitSHA,
		CommitCommittedAt: repo.CommitCommittedAt,
		BuildStrategy:     model.AppBuildStrategyDockerfile,
		DockerfilePath:    dockerfilePath,
		BuildContextDir:   buildContextDir,
		ImageRef:          imageRef,
		DefaultAppName:    repo.DefaultAppName,
		DetectedPort:      detectedPort,
		DetectedProvider:  model.AppBuildStrategyDockerfile,
		DetectedStack:     detectedStack,
	}, nil
}

func detectDockerfilePort(repoDir, dockerfilePath string) (int, error) {
	fullPath, err := secureRepoJoin(repoDir, dockerfilePath)
	if err != nil {
		return 0, err
	}
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return 0, fmt.Errorf("read dockerfile %q: %w", dockerfilePath, err)
	}

	detected := 0
	for _, rawLine := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		upper := strings.ToUpper(line)
		if !strings.HasPrefix(upper, "EXPOSE ") {
			continue
		}
		fields := strings.Fields(line)
		for _, field := range fields[1:] {
			value := strings.TrimSpace(field)
			if idx := strings.Index(value, "/"); idx >= 0 {
				value = value[:idx]
			}
			port, err := strconv.Atoi(value)
			if err != nil || port <= 0 {
				continue
			}
			detected = port
			break
		}
	}
	if detected > 0 {
		return detected, nil
	}
	return 80, nil
}

func secureRepoJoin(repoDir, rel string) (string, error) {
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("absolute paths are not supported")
	}
	fullPath := filepath.Clean(filepath.Join(repoDir, rel))
	repoDir = filepath.Clean(repoDir)
	if fullPath != repoDir && !strings.HasPrefix(fullPath, repoDir+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes repository root", rel)
	}
	return fullPath, nil
}

func buildAndPushDockerfileImage(ctx context.Context, req dockerfileBuildRequest) error {
	namespace, err := currentNamespace()
	if err != nil {
		return err
	}

	jobName := buildJobName(req)
	_ = kubectlRun(ctx, nil, "-n", namespace, "delete", "job", jobName, "--ignore-not-found=true", "--wait=false")
	placement, releasePlacement, err := acquireBuilderPlacement(ctx, namespace, jobName, req.PodPolicy, req.WorkloadProfile, req.PlacementNodeSelector)
	if err != nil {
		return fmt.Errorf("select builder placement: %w", err)
	}
	defer releasePlacement()
	req.Placement = placement

	jobObject, err := buildKanikoJobObject(namespace, jobName, req)
	if err != nil {
		return err
	}
	if err := kubectlRun(ctx, jobObject, "-n", namespace, "apply", "-f", "-"); err != nil {
		return fmt.Errorf("apply kaniko job: %w", err)
	}
	if err := waitForBuilderJob(ctx, namespace, jobName, 25*time.Minute); err != nil {
		return fmt.Errorf("kaniko job %s: %w", jobName, err)
	}
	return nil
}

func currentNamespace() (string, error) {
	if value := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); value != "" {
		return value, nil
	}
	data, err := os.ReadFile(serviceAccountNamespacePath)
	if err != nil {
		return "", fmt.Errorf("read service account namespace: %w", err)
	}
	namespace := strings.TrimSpace(string(data))
	if namespace == "" {
		return "", fmt.Errorf("service account namespace is empty")
	}
	return namespace, nil
}

func buildJobName(req dockerfileBuildRequest) string {
	base := model.Slugify(strings.TrimSuffix(filepath.Base(req.RepoURL), ".git"))
	if base == "" {
		base = model.Slugify(req.SourceLabel)
	}
	if base == "" {
		base = "build"
	}
	suffix := shortCommit(req.CommitSHA)
	if suffix == "" {
		suffix = "source"
	}
	name := "fugue-build-" + base + "-" + suffix
	if len(name) > 63 {
		name = name[:63]
	}
	return strings.TrimRight(name, "-")
}

func buildKanikoJobObject(namespace, jobName string, req dockerfileBuildRequest) (map[string]any, error) {
	if strings.TrimSpace(req.ArchiveDownloadURL) != "" {
		return buildArchiveKanikoJobObject(namespace, jobName, req)
	}
	contextURL, err := buildGitContextURL(req.RepoURL, req.Branch, req.CommitSHA)
	if err != nil {
		return nil, err
	}
	kanikoDockerfilePath, err := kanikoDockerfilePath(req.DockerfilePath, req.BuildContextDir)
	if err != nil {
		return nil, err
	}

	args := kanikoDestinationArgs(req.ImageRef,
		"--context="+contextURL,
		"--dockerfile="+kanikoDockerfilePath,
		"--git=branch="+req.Branch,
		"--git=single-branch=true",
		"--git=recurse-submodules=true",
	)
	if strings.TrimSpace(req.BuildContextDir) != "" && strings.TrimSpace(req.BuildContextDir) != "." {
		args = append(args, "--context-sub-path="+req.BuildContextDir)
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
					"containers": []map[string]any{
						{
							"name":  "kaniko",
							"image": defaultKanikoImage,
							"args":  args,
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

func buildArchiveKanikoJobObject(namespace, jobName string, req dockerfileBuildRequest) (map[string]any, error) {
	args := kanikoDestinationArgs(
		req.ImageRef,
		"--context=dir:///workspace/repo",
		"--dockerfile=/workspace/repo/"+filepath.ToSlash(strings.TrimSpace(req.DockerfilePath)),
	)
	if strings.TrimSpace(req.BuildContextDir) != "" && strings.TrimSpace(req.BuildContextDir) != "." {
		args = append(args, "--context-sub-path="+filepath.ToSlash(strings.TrimSpace(req.BuildContextDir)))
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
					"initContainers": buildArchiveDownloadInitContainers(req.ArchiveDownloadURL),
					"containers": []map[string]any{
						{
							"name":  "kaniko",
							"image": defaultKanikoImage,
							"args":  args,
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

func kanikoDockerfilePath(dockerfilePath, buildContextDir string) (string, error) {
	dockerfilePath = filepath.ToSlash(strings.TrimSpace(dockerfilePath))
	if dockerfilePath == "" {
		return "", fmt.Errorf("dockerfile path is empty")
	}
	buildContextDir = filepath.ToSlash(strings.TrimSpace(buildContextDir))
	if buildContextDir == "" || buildContextDir == "." {
		return dockerfilePath, nil
	}

	relPath, err := filepath.Rel(filepath.FromSlash(buildContextDir), filepath.FromSlash(dockerfilePath))
	if err != nil {
		return "", fmt.Errorf("rel dockerfile path from build context: %w", err)
	}
	relPath = filepath.ToSlash(relPath)
	if relPath == "." || relPath == "" {
		return filepath.Base(dockerfilePath), nil
	}
	if strings.HasPrefix(relPath, "../") || relPath == ".." {
		return "", fmt.Errorf("dockerfile_path %q must be inside build_context_dir %q for kaniko git builds", dockerfilePath, buildContextDir)
	}
	return relPath, nil
}

func buildGitContextURL(repoURL, branch, commitSHA string) (string, error) {
	owner, repo, err := parseGitHubRepoURL(repoURL)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(branch) == "" {
		branch = "main"
	}
	contextURL := fmt.Sprintf("git://github.com/%s/%s.git#refs/heads/%s", owner, repo, branch)
	if strings.TrimSpace(commitSHA) != "" {
		contextURL += "#" + strings.TrimSpace(commitSHA)
	}
	return contextURL, nil
}

func kubectlRun(ctx context.Context, obj map[string]any, args ...string) error {
	output, err := kubectlOutput(ctx, obj, args...)
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func kubectlOutput(ctx context.Context, obj map[string]any, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	if obj != nil {
		payload, err := json.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("marshal kubectl object: %w", err)
		}
		cmd.Stdin = strings.NewReader(string(payload))
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("kubectl %s: %w", strings.Join(args, " "), err)
	}
	return output, nil
}
