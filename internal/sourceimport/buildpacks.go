package sourceimport

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	defaultBuildpacksImage    = "docker.io/library/docker:27-dind"
	defaultPackVersion        = "0.39.1"
	defaultPaketoBuilderImage = "docker.io/paketobuildpacks/builder-jammy-base:latest"
)

type GitHubBuildpacksImportRequest struct {
	RepoURL          string
	Branch           string
	SourceDir        string
	RegistryPushBase string
	ImageRepository  string
	JobLabels        map[string]string
}

type buildpacksBuildRequest struct {
	RepoURL   string
	Branch    string
	CommitSHA string
	SourceDir string
	ImageRef  string
	JobLabels map[string]string
}

func (i *Importer) ImportPublicGitHubBuildpacks(ctx context.Context, req GitHubBuildpacksImportRequest) (GitHubImportResult, error) {
	if strings.TrimSpace(req.RegistryPushBase) == "" {
		return GitHubImportResult{}, fmt.Errorf("registry push base is empty")
	}
	repo, err := i.clonePublicGitHubRepo(ctx, req.RepoURL, req.Branch, "github-buildpacks-import-*")
	if err != nil {
		return GitHubImportResult{}, err
	}
	defer releaseClonedRepo(repo)

	return importBuildpacksFromClonedRepo(ctx, repo, req.RepoURL, req.SourceDir, req.RegistryPushBase, req.ImageRepository, req.JobLabels)
}

func importBuildpacksFromClonedRepo(ctx context.Context, repo clonedGitHubRepo, repoURL, sourceDir, registryPushBase, imageRepository string, jobLabels map[string]string) (GitHubImportResult, error) {
	normalizedSourceDir, err := normalizeRepoSourceDir(repo.RepoDir, sourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	provider, port := detectBuildpacksProviderAndPort(repo.RepoDir, normalizedSourceDir)

	imageRef := defaultImportedImageRef(registryPushBase, imageRepository, repo)
	if err := buildAndPushBuildpacksImage(ctx, buildpacksBuildRequest{
		RepoURL:   repoURL,
		Branch:    repo.Branch,
		CommitSHA: repo.CommitSHA,
		SourceDir: normalizedSourceDir,
		ImageRef:  imageRef,
		JobLabels: jobLabels,
	}); err != nil {
		return GitHubImportResult{}, err
	}

	return GitHubImportResult{
		RepoOwner:        repo.RepoOwner,
		RepoName:         repo.RepoName,
		Branch:           repo.Branch,
		CommitSHA:        repo.CommitSHA,
		SourceDir:        normalizeImportedSourceDirValue(normalizedSourceDir),
		BuildStrategy:    model.AppBuildStrategyBuildpacks,
		ImageRef:         imageRef,
		DefaultAppName:   repo.DefaultAppName,
		DetectedPort:     port,
		DetectedProvider: provider,
		SuggestedEnv:     suggestedBuildpacksEnv(port),
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
		RepoURL:   req.RepoURL,
		CommitSHA: req.CommitSHA,
	})
	_ = kubectlRun(ctx, nil, "-n", namespace, "delete", "job", jobName, "--ignore-not-found=true", "--wait=false")

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
	cloneArgs := gitCloneArgs(req.RepoURL, "/workspace/repo", req.Branch)
	workingDir := "/workspace/repo"
	if strings.TrimSpace(req.SourceDir) != "" && strings.TrimSpace(req.SourceDir) != "." {
		workingDir += "/" + filepath.ToSlash(strings.TrimSpace(req.SourceDir))
	}

	script := buildpacksJobScript(workingDir, req.ImageRef)
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
					"initContainers": []map[string]any{
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
					},
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
	return jobObject, nil
}

func buildpacksJobScript(workingDir, imageRef string) string {
	registryHost := registryHostFromImageRef(imageRef)
	insecureRegistryFlag := ""
	if isInsecureRegistryHost(registryHost) {
		insecureRegistryFlag = fmt.Sprintf(" --insecure-registry %q", registryHost)
	}
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
/workspace/bin/pack build %q --path %q --builder %q --publish --trust-builder%s
`, defaultPackVersion, defaultPackVersion, defaultPackVersion, imageRef, workingDir, defaultPaketoBuilderImage, insecureRegistryFlag)
}

func registryHostFromImageRef(imageRef string) string {
	host := strings.TrimSpace(imageRef)
	if idx := strings.Index(host, "/"); idx >= 0 {
		host = host[:idx]
	}
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	}
	return strings.Trim(strings.TrimSpace(host), "[]")
}

func isInsecureRegistryHost(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	return host == "localhost" || net.ParseIP(host) != nil
}
