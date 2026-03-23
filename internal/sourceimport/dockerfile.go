package sourceimport

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"fugue/internal/model"
)

const (
	defaultKanikoImage          = "gcr.io/kaniko-project/executor:v1.23.2-debug"
	serviceAccountNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

type GitHubDockerImportRequest struct {
	RepoURL          string
	Branch           string
	DockerfilePath   string
	BuildContextDir  string
	RegistryPushBase string
	ImageRepository  string
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

	return importDockerfileFromClonedRepo(ctx, repo, req.RepoURL, req.DockerfilePath, req.BuildContextDir, req.RegistryPushBase, req.ImageRepository)
}

type dockerfileBuildRequest struct {
	RepoURL         string
	Branch          string
	CommitSHA       string
	DockerfilePath  string
	BuildContextDir string
	ImageRef        string
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

func importDockerfileFromClonedRepo(ctx context.Context, repo clonedGitHubRepo, repoURL, dockerfilePath, buildContextDir, registryPushBase, imageRepository string) (GitHubImportResult, error) {
	dockerfilePath, buildContextDir, err := detectDockerBuildInputs(repo.RepoDir, dockerfilePath, buildContextDir)
	if err != nil {
		return GitHubImportResult{}, err
	}

	imageRef := defaultImportedImageRef(registryPushBase, imageRepository, repo)
	if err := buildAndPushDockerfileImage(ctx, dockerfileBuildRequest{
		RepoURL:         repoURL,
		Branch:          repo.Branch,
		CommitSHA:       repo.CommitSHA,
		DockerfilePath:  dockerfilePath,
		BuildContextDir: buildContextDir,
		ImageRef:        imageRef,
	}); err != nil {
		return GitHubImportResult{}, err
	}

	return GitHubImportResult{
		RepoOwner:        repo.RepoOwner,
		RepoName:         repo.RepoName,
		Branch:           repo.Branch,
		CommitSHA:        repo.CommitSHA,
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		DockerfilePath:   dockerfilePath,
		BuildContextDir:  buildContextDir,
		ImageRef:         imageRef,
		DefaultAppName:   repo.DefaultAppName,
		DetectedPort:     80,
		DetectedProvider: model.AppBuildStrategyDockerfile,
	}, nil
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

	jobObject, err := buildKanikoJobObject(namespace, jobName, req)
	if err != nil {
		return err
	}
	defer kubectlRun(context.Background(), nil, "-n", namespace, "delete", "job", jobName, "--ignore-not-found=true", "--wait=false")

	if err := kubectlRun(ctx, jobObject, "-n", namespace, "apply", "-f", "-"); err != nil {
		return fmt.Errorf("apply kaniko job: %w", err)
	}
	if err := kubectlRun(ctx, nil, "-n", namespace, "wait", "--for=condition=complete", "--timeout=25m", "job/"+jobName); err != nil {
		logs, _ := kubectlOutput(ctx, nil, "-n", namespace, "logs", "job/"+jobName, "--all-containers=true", "--tail=-1")
		describe, _ := kubectlOutput(ctx, nil, "-n", namespace, "describe", "job/"+jobName)
		return fmt.Errorf("wait for kaniko job %s: %w\nlogs:\n%s\ndescribe:\n%s", jobName, err, strings.TrimSpace(string(logs)), strings.TrimSpace(string(describe)))
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
		base = "build"
	}
	name := "fugue-build-" + base + "-" + shortCommit(req.CommitSHA)
	if len(name) > 63 {
		name = name[:63]
	}
	return strings.TrimRight(name, "-")
}

func buildKanikoJobObject(namespace, jobName string, req dockerfileBuildRequest) (map[string]any, error) {
	contextURL, err := buildGitContextURL(req.RepoURL, req.Branch, req.CommitSHA)
	if err != nil {
		return nil, err
	}

	args := []string{
		"--context=" + contextURL,
		"--dockerfile=" + req.DockerfilePath,
		"--destination=" + req.ImageRef,
		"--cleanup",
		"--git=branch=" + req.Branch,
		"--git=single-branch=true",
		"--git=recurse-submodules=true",
	}
	if strings.TrimSpace(req.BuildContextDir) != "" && strings.TrimSpace(req.BuildContextDir) != "." {
		args = append(args, "--context-sub-path="+req.BuildContextDir)
	}

	return map[string]any{
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
			"ttlSecondsAfterFinished": 300,
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
	}, nil
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
