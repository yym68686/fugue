package sourceimport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
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
	SourceType            string
	RepoURL               string
	RepoAuthToken         string
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

func (i *Importer) ImportGitHubDockerfileImage(ctx context.Context, req GitHubDockerImportRequest) (GitHubImportResult, error) {
	if strings.TrimSpace(req.RegistryPushBase) == "" {
		return GitHubImportResult{}, fmt.Errorf("registry push base is empty")
	}
	repo, err := i.cloneGitHubRepo(ctx, req.RepoURL, req.RepoAuthToken, req.Branch, "github-docker-import-*")
	if err != nil {
		return GitHubImportResult{}, err
	}
	defer releaseClonedRepo(repo)

	return importDockerfileFromClonedRepo(ctx, repo, req.RepoURL, req.RepoAuthToken, req.DockerfilePath, req.BuildContextDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful, i.Logger)
}

type dockerfileBuildRequest struct {
	RepoURL               string
	RepoAuthToken         string
	Branch                string
	CommitSHA             string
	SourceLabel           string
	ArchiveDownloadURL    string
	DockerfilePath        string
	BuildContextDir       string
	ImageRef              string
	SourceOverlayFiles    []sourceOverlayFile
	JobLabels             map[string]string
	PlacementNodeSelector map[string]string
	PodPolicy             BuilderPodPolicy
	WorkloadProfile       builderWorkloadProfile
	Placement             builderJobPlacement
	Logger                *log.Logger
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

func importDockerfileFromClonedRepo(ctx context.Context, repo clonedGitHubRepo, repoURL, repoAuthToken, dockerfilePath, buildContextDir, registryPushBase, imageRepository, imageNameSuffix string, jobLabels, placementNodeSelector map[string]string, builderPolicy BuilderPodPolicy, stateful bool, logger *log.Logger) (GitHubImportResult, error) {
	dockerfilePath, buildContextDir, err := detectDockerBuildInputs(repo.RepoDir, dockerfilePath, buildContextDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	detectedPort, exposesPublicService, err := detectDockerfilePortSignal(repo.RepoDir, dockerfilePath)
	if err != nil {
		return GitHubImportResult{}, err
	}
	detectedStack := detectPrimaryTechStack(repo.RepoDir, buildContextDir)
	if exposesPublicService && shouldSuppressDetectedPublicServiceForProject(repo.RepoDir, buildContextDir, detectedStack) {
		exposesPublicService = false
	}

	imageRef := defaultImportedImageRef(registryPushBase, imageRepository, repo, imageNameSuffix)
	buildReq := dockerfileBuildRequest{
		RepoURL:               repoURL,
		RepoAuthToken:         repoAuthToken,
		Branch:                repo.Branch,
		CommitSHA:             repo.CommitSHA,
		DockerfilePath:        dockerfilePath,
		BuildContextDir:       buildContextDir,
		ImageRef:              imageRef,
		JobLabels:             jobLabels,
		PlacementNodeSelector: placementNodeSelector,
		PodPolicy:             builderPolicy,
		WorkloadProfile:       builderWorkloadProfileFor(model.AppBuildStrategyDockerfile, stateful),
		Logger:                logger,
	}
	buildJobNameValue := buildJobName(buildReq)
	if err := buildAndPushDockerfileImage(ctx, buildReq); err != nil {
		return GitHubImportResult{}, err
	}

	return GitHubImportResult{
		RepoOwner:            repo.RepoOwner,
		RepoName:             repo.RepoName,
		Branch:               repo.Branch,
		CommitSHA:            repo.CommitSHA,
		CommitCommittedAt:    repo.CommitCommittedAt,
		BuildStrategy:        model.AppBuildStrategyDockerfile,
		DockerfilePath:       dockerfilePath,
		BuildContextDir:      buildContextDir,
		ImageRef:             imageRef,
		BuildJobName:         buildJobNameValue,
		DefaultAppName:       repo.DefaultAppName,
		DetectedPort:         detectedPort,
		ExposesPublicService: exposesPublicService,
		DetectedProvider:     model.AppBuildStrategyDockerfile,
		DetectedStack:        detectedStack,
	}, nil
}

func detectDockerfilePort(repoDir, dockerfilePath string) (int, error) {
	port, _, err := detectDockerfilePortSignal(repoDir, dockerfilePath)
	return port, err
}

func detectDockerfilePortSignal(repoDir, dockerfilePath string) (int, bool, error) {
	fullPath, err := secureRepoJoin(repoDir, dockerfilePath)
	if err != nil {
		return 0, false, err
	}
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return 0, false, fmt.Errorf("read dockerfile %q: %w", dockerfilePath, err)
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
		return detected, true, nil
	}
	return 80, false, nil
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
	logger := effectiveBuilderLogger(req.Logger)
	namespace, err := currentNamespace()
	if err != nil {
		logger.Printf("builder job namespace resolve failed kind=dockerfile image=%s err=%v", strings.TrimSpace(req.ImageRef), err)
		return err
	}

	jobName := buildJobName(req)
	logger.Printf("builder job preflight kind=dockerfile stage=delete-existing name=%s namespace=%s image=%s", jobName, namespace, strings.TrimSpace(req.ImageRef))
	if err := deleteBuilderJobIfPresent(ctx, namespace, jobName); err != nil {
		logger.Printf("builder job preflight failed kind=dockerfile stage=delete-existing name=%s namespace=%s image=%s err=%v", jobName, namespace, strings.TrimSpace(req.ImageRef), err)
		return err
	}
	logger.Printf("builder job preflight complete kind=dockerfile stage=delete-existing name=%s namespace=%s image=%s", jobName, namespace, strings.TrimSpace(req.ImageRef))
	logger.Printf("builder job preflight kind=dockerfile stage=reserve-placement name=%s namespace=%s image=%s", jobName, namespace, strings.TrimSpace(req.ImageRef))
	placement, releasePlacement, err := acquireBuilderPlacement(ctx, namespace, jobName, req.PodPolicy, req.WorkloadProfile, req.PlacementNodeSelector)
	if err != nil {
		logger.Printf("builder job preflight failed kind=dockerfile stage=reserve-placement name=%s namespace=%s image=%s err=%v", jobName, namespace, strings.TrimSpace(req.ImageRef), err)
		return fmt.Errorf("select builder placement: %w", err)
	}
	defer releasePlacement()
	req.Placement = placement
	logger.Printf("builder job preflight complete kind=dockerfile stage=reserve-placement name=%s namespace=%s image=%s placement=%s", jobName, namespace, strings.TrimSpace(req.ImageRef), builderPlacementSummary(placement))
	logger.Printf(
		"builder job start kind=dockerfile name=%s namespace=%s image=%s operation=%s app=%s placement=%s",
		jobName,
		namespace,
		strings.TrimSpace(req.ImageRef),
		strings.TrimSpace(req.JobLabels["fugue.pro/operation-id"]),
		strings.TrimSpace(req.JobLabels["fugue.pro/app-id"]),
		builderPlacementSummary(placement),
	)

	jobObject, err := buildKanikoJobObject(namespace, jobName, req)
	if err != nil {
		return err
	}
	if err := kubectlRun(ctx, jobObject, "-n", namespace, "apply", "-f", "-"); err != nil {
		logger.Printf("builder job apply failed kind=dockerfile name=%s namespace=%s image=%s err=%v", jobName, namespace, strings.TrimSpace(req.ImageRef), err)
		return fmt.Errorf("apply kaniko job: %w", err)
	}
	logger.Printf("builder job applied kind=dockerfile name=%s namespace=%s image=%s", jobName, namespace, strings.TrimSpace(req.ImageRef))
	if err := waitForBuilderJob(ctx, namespace, jobName, 25*time.Minute); err != nil {
		logger.Printf("builder job failed kind=dockerfile name=%s namespace=%s image=%s err=%v", jobName, namespace, strings.TrimSpace(req.ImageRef), err)
		return fmt.Errorf("kaniko job %s: %w", jobName, err)
	}
	logger.Printf("builder job completed kind=dockerfile name=%s namespace=%s image=%s", jobName, namespace, strings.TrimSpace(req.ImageRef))
	return nil
}

func effectiveBuilderLogger(logger *log.Logger) *log.Logger {
	if logger != nil {
		return logger
	}
	return log.Default()
}

func builderPlacementSummary(placement builderJobPlacement) string {
	parts := make([]string, 0, 2)
	if preferred := strings.TrimSpace(placement.PreferredHostname); preferred != "" {
		parts = append(parts, "preferred="+preferred)
	}
	if len(placement.CandidateHostnames) > 0 {
		parts = append(parts, "candidates="+strings.Join(placement.CandidateHostnames, ","))
	}
	if len(parts) == 0 {
		return "unconstrained"
	}
	return strings.Join(parts, " ")
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
	if salt := buildJobSalt(req); salt != "" {
		suffix += "-" + salt
	}
	name := "fugue-build-" + base + "-" + suffix
	if len(name) > 63 {
		name = name[:63]
	}
	return strings.TrimRight(name, "-")
}

func buildJobSalt(req dockerfileBuildRequest) string {
	seedParts := make([]string, 0, 3)
	if opID := strings.TrimSpace(req.JobLabels["fugue.pro/operation-id"]); opID != "" {
		seedParts = append(seedParts, "op="+opID)
	}
	if imageRef := strings.TrimSpace(req.ImageRef); imageRef != "" {
		seedParts = append(seedParts, "image="+imageRef)
	}
	fallback := strings.TrimSpace(strings.Join([]string{
		strings.TrimSpace(req.DockerfilePath),
		strings.TrimSpace(req.BuildContextDir),
		strings.TrimSpace(req.ArchiveDownloadURL),
		strings.TrimSpace(req.RepoURL),
		strings.TrimSpace(req.SourceLabel),
	}, "\x00"))
	if fallback != "" {
		seedParts = append(seedParts, "fallback="+fallback)
	}
	if len(seedParts) == 0 {
		return ""
	}
	sum := sha256.Sum256([]byte(strings.Join(seedParts, "\x00")))
	return hex.EncodeToString(sum[:4])
}

func deleteBuilderJobIfPresent(ctx context.Context, namespace, jobName string) error {
	if err := kubectlRun(ctx, nil, "-n", namespace, "delete", "job", jobName, "--ignore-not-found=true", "--wait=true", "--timeout=60s"); err != nil {
		return fmt.Errorf("delete previous builder job %s: %w", jobName, err)
	}
	return nil
}

func buildKanikoJobObject(namespace, jobName string, req dockerfileBuildRequest) (map[string]any, error) {
	args := kanikoDestinationArgs(req.ImageRef,
		"--context=dir:///workspace/repo",
		"--dockerfile=/workspace/repo/"+filepath.ToSlash(strings.TrimSpace(req.DockerfilePath)),
	)
	if strings.TrimSpace(req.BuildContextDir) != "" && strings.TrimSpace(req.BuildContextDir) != "." {
		args = append(args, "--context-sub-path="+req.BuildContextDir)
	}
	initContainers := buildArchiveDownloadInitContainers(req.ArchiveDownloadURL)
	if strings.TrimSpace(req.ArchiveDownloadURL) == "" {
		initContainers = buildGitCloneInitContainers(req.RepoURL, req.Branch, req.CommitSHA, req.RepoAuthToken)
	}
	sourceOverlayContainer, err := buildSourceOverlayInitContainer("/workspace/repo", req.SourceOverlayFiles)
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
					"initContainers": initContainers,
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

func buildArchiveKanikoJobObject(namespace, jobName string, req dockerfileBuildRequest) (map[string]any, error) {
	if strings.TrimSpace(req.ArchiveDownloadURL) == "" {
		return nil, fmt.Errorf("archive download url is empty")
	}
	return buildKanikoJobObject(namespace, jobName, req)
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
