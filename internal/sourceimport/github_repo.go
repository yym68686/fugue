package sourceimport

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"fugue/internal/model"
)

type clonedGitHubRepo struct {
	RepoOwner         string
	RepoName          string
	RepoDir           string
	Branch            string
	CommitSHA         string
	CommitCommittedAt string
	DefaultAppName    string
}

func gitHubAuthConfigArgs(repoAuthToken string) []string {
	repoAuthToken = strings.TrimSpace(repoAuthToken)
	if repoAuthToken == "" {
		return nil
	}
	header := "AUTHORIZATION: basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+repoAuthToken))
	return []string{
		"-c",
		"http.https://github.com/.extraheader=" + header,
	}
}

func gitCommandArgsWithGitHubAuth(repoAuthToken string, args ...string) []string {
	return append(gitHubAuthConfigArgs(repoAuthToken), args...)
}

func gitCommandEnv() map[string]string {
	return map[string]string{
		"GIT_TERMINAL_PROMPT": "0",
	}
}

func buildGitCloneInitContainers(repoURL, branch, commitSHA, repoAuthToken string) []map[string]any {
	containers := []map[string]any{
		{
			"name":         "git-clone",
			"image":        defaultGitCloneImage,
			"command":      append([]string{"git"}, gitCommandArgsWithGitHubAuth(repoAuthToken, gitCloneArgs(repoURL, "/workspace/repo", branch)...)...),
			"env":          []map[string]any{{"name": "GIT_TERMINAL_PROMPT", "value": "0"}},
			"volumeMounts": []map[string]any{{"name": "workspace", "mountPath": "/workspace"}},
		},
	}
	if strings.TrimSpace(commitSHA) == "" {
		return containers
	}
	return append(containers, map[string]any{
		"name":         "git-checkout",
		"image":        defaultGitCloneImage,
		"command":      []string{"git", "-C", "/workspace/repo", "checkout", strings.TrimSpace(commitSHA)},
		"env":          []map[string]any{{"name": "GIT_TERMINAL_PROMPT", "value": "0"}},
		"volumeMounts": []map[string]any{{"name": "workspace", "mountPath": "/workspace"}},
	})
}

func (i *Importer) cloneGitHubRepo(ctx context.Context, repoURL, repoAuthToken, branch, tempPrefix string) (clonedGitHubRepo, error) {
	owner, repo, err := parseGitHubRepoURL(repoURL)
	if err != nil {
		return clonedGitHubRepo{}, err
	}

	if err := os.MkdirAll(i.WorkDir, 0o755); err != nil {
		return clonedGitHubRepo{}, fmt.Errorf("create import work dir: %w", err)
	}

	if strings.TrimSpace(tempPrefix) == "" {
		tempPrefix = "github-import-*"
	}
	repoDir, err := os.MkdirTemp(i.WorkDir, tempPrefix)
	if err != nil {
		return clonedGitHubRepo{}, fmt.Errorf("create import temp dir: %w", err)
	}

	args := gitCommandArgsWithGitHubAuth(repoAuthToken, gitCloneArgs(repoURL, repoDir, branch)...)
	if output, err := runCombinedOutputWithEnv(ctx, "", gitCommandEnv(), "git", args...); err != nil {
		_ = os.RemoveAll(repoDir)
		return clonedGitHubRepo{}, fmt.Errorf("git clone: %w: %s", err, strings.TrimSpace(string(output)))
	}

	checkedOutBranch, err := gitOutput(ctx, repoDir, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		_ = os.RemoveAll(repoDir)
		return clonedGitHubRepo{}, err
	}
	commitSHA, err := gitOutput(ctx, repoDir, "rev-parse", "HEAD")
	if err != nil {
		_ = os.RemoveAll(repoDir)
		return clonedGitHubRepo{}, err
	}
	commitCommittedAt, err := gitOutput(ctx, repoDir, "show", "-s", "--format=%cI", "HEAD")
	if err != nil {
		commitCommittedAt = ""
	}

	return clonedGitHubRepo{
		RepoOwner:         owner,
		RepoName:          repo,
		RepoDir:           repoDir,
		Branch:            checkedOutBranch,
		CommitSHA:         commitSHA,
		CommitCommittedAt: commitCommittedAt,
		DefaultAppName:    model.Slugify(repo),
	}, nil
}

func releaseClonedRepo(repo clonedGitHubRepo) {
	if strings.TrimSpace(repo.RepoDir) == "" {
		return
	}
	_ = os.RemoveAll(repo.RepoDir)
}

func defaultImportedImageRef(registryPushBase, imageRepository string, repo clonedGitHubRepo, imageNameSuffix string) string {
	imageRepository = strings.Trim(strings.TrimSpace(imageRepository), "/")
	if imageRepository == "" {
		imageRepository = "fugue-apps"
	}
	repoPath := model.Slugify(repo.RepoOwner) + "-" + model.Slugify(repo.RepoName)
	if suffix := model.SlugifyOptional(imageNameSuffix); suffix != "" {
		repoPath += "-" + suffix
	}
	return fmt.Sprintf("%s/%s/%s:git-%s", strings.TrimSpace(registryPushBase), imageRepository, repoPath, shortCommit(repo.CommitSHA))
}

func normalizeRepoSourceDir(repoDir, sourceDir string) (string, error) {
	sourceDir = strings.TrimSpace(sourceDir)
	if sourceDir == "" {
		return ".", nil
	}
	fullPath, err := secureRepoJoin(repoDir, sourceDir)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(fullPath)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("source_dir %q does not exist", sourceDir)
	}
	rel, err := filepath.Rel(repoDir, fullPath)
	if err != nil {
		return "", fmt.Errorf("rel source dir: %w", err)
	}
	rel = filepath.ToSlash(rel)
	if rel == "" {
		return ".", nil
	}
	return rel, nil
}

func relativeImportedSourceDir(repoDir, fullPath string) string {
	rel, err := filepath.Rel(repoDir, fullPath)
	if err != nil {
		return ""
	}
	rel = filepath.ToSlash(rel)
	if rel == "." {
		return ""
	}
	return rel
}
