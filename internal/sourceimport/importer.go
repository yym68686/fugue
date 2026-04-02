package sourceimport

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

const staticSiteBaseImage = "index.docker.io/library/caddy:2.10.2-alpine"

type Importer struct {
	WorkDir       string
	Logger        *log.Logger
	BuilderPolicy BuilderPodPolicy
}

type GitHubImportRequest struct {
	SourceType       string
	RepoURL          string
	RepoAuthToken    string
	Branch           string
	SourceDir        string
	RegistryPushBase string
	ImageRepository  string
	ImageNameSuffix  string
}

type GitHubImportResult struct {
	RepoOwner         string
	RepoName          string
	Branch            string
	CommitSHA         string
	CommitCommittedAt string
	SourceDir         string
	BuildStrategy     string
	DockerfilePath    string
	BuildContextDir   string
	ImageRef          string
	DefaultAppName    string
	DetectedPort      int
	DetectedProvider  string
	DetectedStack     string
	SuggestedEnv      map[string]string
}

func NewImporter(workDir string, logger *log.Logger, builderPolicy BuilderPodPolicy) *Importer {
	if logger == nil {
		logger = log.Default()
	}
	if strings.TrimSpace(workDir) == "" {
		workDir = "./data/import"
	}
	return &Importer{
		WorkDir:       workDir,
		Logger:        logger,
		BuilderPolicy: builderPolicy,
	}
}

func (i *Importer) ImportGitHubStaticSite(ctx context.Context, req GitHubImportRequest) (GitHubImportResult, error) {
	if strings.TrimSpace(req.RegistryPushBase) == "" {
		return GitHubImportResult{}, fmt.Errorf("registry push base is empty")
	}
	repo, err := i.cloneGitHubRepo(ctx, req.RepoURL, req.RepoAuthToken, req.Branch, "github-import-*")
	if err != nil {
		return GitHubImportResult{}, err
	}
	defer releaseClonedRepo(repo)

	return importStaticSiteFromClonedRepo(repo, req.SourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix)
}

func gitOutput(ctx context.Context, repoDir string, args ...string) (string, error) {
	cmdArgs := append([]string{"-C", repoDir}, args...)
	output, err := runCombinedOutput(ctx, "", "git", cmdArgs...)
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

func gitCloneArgs(repoURL, repoDir, branch string) []string {
	args := []string{
		"clone",
		"--depth", "1",
		"--recurse-submodules",
		"--shallow-submodules",
	}
	if branch = strings.TrimSpace(branch); branch != "" {
		args = append(args, "--branch", branch)
	}
	args = append(args, repoURL, repoDir)
	return args
}

func ParseGitHubRepoURL(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("repo_url is required")
	}
	const prefix = "https://github.com/"
	if !strings.HasPrefix(raw, prefix) {
		return "", "", fmt.Errorf("only https://github.com/<owner>/<repo> is supported in this MVP")
	}
	path := strings.TrimPrefix(raw, prefix)
	path = strings.TrimSuffix(path, ".git")
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("invalid GitHub repository URL")
	}
	return model.Slugify(parts[0]), model.Slugify(parts[1]), nil
}

func parseGitHubRepoURL(raw string) (string, string, error) {
	return ParseGitHubRepoURL(raw)
}

func importStaticSiteFromClonedRepo(repo clonedGitHubRepo, requestedSourceDir, registryPushBase, imageRepository, imageNameSuffix string) (GitHubImportResult, error) {
	sourceDir, err := detectStaticSiteDir(repo.RepoDir, requestedSourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	detectedStack := detectPrimaryTechStack(repo.RepoDir, relativeImportedSourceDir(repo.RepoDir, sourceDir))

	imageRef := defaultImportedImageRef(registryPushBase, imageRepository, repo, imageNameSuffix)
	if err := buildAndPushStaticSiteImage(sourceDir, imageRef); err != nil {
		return GitHubImportResult{}, err
	}

	return GitHubImportResult{
		RepoOwner:         repo.RepoOwner,
		RepoName:          repo.RepoName,
		Branch:            repo.Branch,
		CommitSHA:         repo.CommitSHA,
		CommitCommittedAt: repo.CommitCommittedAt,
		SourceDir:         relativeImportedSourceDir(repo.RepoDir, sourceDir),
		BuildStrategy:     model.AppBuildStrategyStaticSite,
		ImageRef:          imageRef,
		DefaultAppName:    repo.DefaultAppName,
		DetectedPort:      80,
		DetectedStack:     detectedStack,
	}, nil
}

func detectStaticSiteDir(repoDir, requested string) (string, error) {
	candidates := []string{}
	if strings.TrimSpace(requested) != "" {
		candidates = append(candidates, strings.TrimSpace(requested))
	} else {
		candidates = append(candidates, ".", "dist", "build", "public", "site")
	}

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

	if strings.TrimSpace(requested) != "" {
		return "", fmt.Errorf("source_dir %q does not contain index.html", requested)
	}
	return "", fmt.Errorf("no static-site entrypoint found; this MVP currently requires index.html in root, dist/, build/, public/, or site/")
}

func buildAndPushStaticSiteImage(sourceDir, imageRef string) error {
	baseRef, err := name.ParseReference(staticSiteBaseImage)
	if err != nil {
		return fmt.Errorf("parse base image reference: %w", err)
	}
	baseImage, err := remote.Image(baseRef)
	if err != nil {
		return fmt.Errorf("pull static-site base image: %w", err)
	}

	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		reader, writer := io.Pipe()
		go func() {
			writerErr := writeStaticSiteLayer(writer, sourceDir)
			_ = writer.CloseWithError(writerErr)
		}()
		return reader, nil
	})
	if err != nil {
		return fmt.Errorf("create static-site layer: %w", err)
	}

	image, err := mutate.Append(baseImage, mutate.Addendum{Layer: layer})
	if err != nil {
		return fmt.Errorf("append static-site layer: %w", err)
	}

	tag, err := name.NewTag(imageRef, destinationTagOptions(imageRef)...)
	if err != nil {
		return fmt.Errorf("parse destination image reference: %w", err)
	}
	if err := remote.Write(tag, image); err != nil {
		return fmt.Errorf("push image to internal registry: %w", err)
	}
	return nil
}

func destinationTagOptions(imageRef string) []name.Option {
	if isInsecureRegistryHost(registryHostFromImageRef(imageRef)) {
		return []name.Option{name.Insecure}
	}
	return nil
}

func mergeBuilderLabels(base map[string]string, extra map[string]string) map[string]string {
	if len(base) == 0 && len(extra) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(extra))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range extra {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		out[key] = value
	}
	return out
}

func writeStaticSiteLayer(w io.Writer, sourceDir string) error {
	tw := tar.NewWriter(w)
	defer tw.Close()

	entries, err := collectStaticSiteEntries(sourceDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := writeStaticSiteTarEntry(tw, sourceDir, entry); err != nil {
			return err
		}
	}
	return nil
}

func collectStaticSiteEntries(sourceDir string) ([]string, error) {
	entries := []string{}
	err := filepath.WalkDir(sourceDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if rel == ".git" || strings.HasPrefix(rel, ".git"+string(filepath.Separator)) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		entries = append(entries, rel)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk static-site source: %w", err)
	}
	sort.Strings(entries)
	return entries, nil
}

func writeStaticSiteTarEntry(tw *tar.Writer, sourceDir, rel string) error {
	fullPath := filepath.Join(sourceDir, rel)
	info, err := os.Lstat(fullPath)
	if err != nil {
		return fmt.Errorf("stat %s: %w", rel, err)
	}

	name := filepath.ToSlash(filepath.Join("usr/share/caddy", rel))
	header, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return fmt.Errorf("create tar header for %s: %w", rel, err)
	}
	header.Name = name
	header.ModTime = time.Unix(0, 0)
	header.AccessTime = time.Unix(0, 0)
	header.ChangeTime = time.Unix(0, 0)

	if info.IsDir() {
		if !strings.HasSuffix(header.Name, "/") {
			header.Name += "/"
		}
		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("write dir header for %s: %w", rel, err)
		}
		return nil
	}

	if info.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(fullPath)
		if err != nil {
			return fmt.Errorf("read symlink %s: %w", rel, err)
		}
		header.Linkname = target
		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("write symlink header for %s: %w", rel, err)
		}
		return nil
	}

	if !info.Mode().IsRegular() {
		return nil
	}

	if err := tw.WriteHeader(header); err != nil {
		return fmt.Errorf("write file header for %s: %w", rel, err)
	}
	file, err := os.Open(fullPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", rel, err)
	}
	defer file.Close()
	if _, err := io.Copy(tw, file); err != nil {
		return fmt.Errorf("copy file %s into image layer: %w", rel, err)
	}
	return nil
}

func shortCommit(sha string) string {
	sha = strings.TrimSpace(sha)
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

func runCombinedOutput(ctx context.Context, dir, name string, args ...string) ([]byte, error) {
	return runCombinedOutputWithEnv(ctx, dir, nil, name, args...)
}

func runCombinedOutputWithEnv(ctx context.Context, dir string, env map[string]string, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if strings.TrimSpace(dir) != "" {
		cmd.Dir = dir
	}
	if len(env) > 0 {
		cmd.Env = append([]string{}, os.Environ()...)
		for key, value := range env {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}
	return cmd.CombinedOutput()
}
