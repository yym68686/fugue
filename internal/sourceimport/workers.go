package sourceimport

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"github.com/pelletier/go-toml/v2"
)

// The adapter and its toolchain are versioned with the platform release. Secrets
// are supplied at runtime, never passed to the builder or written into an image.
const workersRuntimeVersion = "1.20261108.1"
const workersEsbuildVersion = "0.28.2"
const workersCompatibilityDate = "2026-11-08"
const workersOverlayDir = ".fugue-workers"
const workersPort = 8080

//go:embed workers_runtime/*
var workersRuntimeFiles embed.FS

type workersProject struct {
	Main               string            `json:"main"`
	Format             string            `json:"format"`
	CompatibilityDate  string            `json:"compatibilityDate"`
	CompatibilityFlags []string          `json:"compatibilityFlags"`
	JSONVars           []string          `json:"jsonVars"`
	SuggestedEnv       map[string]string `json:"-"`
}

var workersModuleHandler = regexp.MustCompile(`\bexport\s+default\s*\{\s*(?:async\s+)?fetch\s*[:(]`)
var workersServiceHandler = regexp.MustCompile(`\baddEventListener\s*\(\s*['"]fetch['"]\s*,`)

// detectWorkersProject is conservative for configuration-free repositories:
// exactly one standalone handler is required. Explicit Wrangler metadata wins
// over language heuristics; unsupported bindings must never fall back to Node.
func detectWorkersProject(repoDir, sourceDir string) (*workersProject, error) {
	normalized, err := normalizeRepoSourceDir(repoDir, sourceDir)
	if err != nil {
		return nil, err
	}
	root, err := workersSafePath(repoDir, normalized)
	if err != nil {
		return nil, err
	}
	for _, name := range []string{"wrangler.jsonc", "wrangler.json", "wrangler.toml"} {
		path := filepath.Join(root, name)
		if _, err := os.Lstat(path); os.IsNotExist(err) {
			continue
		}
		path, err = workersSafePath(root, name)
		if err != nil {
			return nil, err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if len(data) > 1<<20 {
			return nil, fmt.Errorf("Workers configuration exceeds 1 MiB")
		}
		var raw map[string]any
		if strings.HasSuffix(name, ".toml") {
			err = toml.Unmarshal(data, &raw)
		} else {
			err = json.Unmarshal(workersJSONC(data), &raw)
		}
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", name, err)
		}
		return workersProjectFromConfig(root, raw)
	}
	// A declared server start command takes precedence over heuristic detection.
	if manifest, err := readPackageManifest(root); err == nil && strings.TrimSpace(manifest.Scripts["start"]) != "" {
		return nil, nil
	}
	var candidates []string
	visited, total := 0, int64(0)
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "node_modules" || entry.Name() == "vendor" || entry.Name() == "dist" || entry.Name() == "test" || entry.Name() == "tests" || entry.Name() == "examples") {
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(path)
		if ext != ".js" && ext != ".mjs" && ext != ".ts" {
			return nil
		}
		if strings.HasSuffix(path, ".d.ts") || strings.Contains(entry.Name(), ".test.") || strings.Contains(entry.Name(), ".spec.") {
			return nil
		}
		visited++
		if visited > 256 {
			return fs.SkipAll
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > 2<<20 {
			return nil
		}
		total += info.Size()
		if total > 8<<20 {
			return fs.SkipAll
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if workersScriptFormat(string(data)) != "" {
			rel, _ := filepath.Rel(root, path)
			candidates = append(candidates, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	if visited > 256 || total > 8<<20 {
		return nil, fmt.Errorf("Workers entrypoint scan limit reached; declare main in a Wrangler configuration")
	}
	if len(candidates) != 1 {
		return nil, fmt.Errorf("multiple Workers entrypoints detected (%s); declare main in a Wrangler configuration", strings.Join(candidates, ", "))
	}
	return workersProjectFromConfig(root, map[string]any{"main": candidates[0]})
}

func workersProjectFromConfig(root string, raw map[string]any) (*workersProject, error) {
	allowed := map[string]bool{"$schema": true, "name": true, "main": true, "compatibility_date": true, "compatibility_flags": true, "vars": true, "account_id": true, "workers_dev": true, "preview_urls": true, "routes": true, "route": true, "send_metrics": true, "observability": true}
	var unsupported []string
	for key := range raw {
		if !allowed[key] {
			unsupported = append(unsupported, key)
		}
	}
	sort.Strings(unsupported)
	if len(unsupported) > 0 {
		return nil, fmt.Errorf("unsupported Workers configuration/bindings: %s; Fugue currently supports HTTP/WebSocket handlers, vars and runtime secrets", strings.Join(unsupported, ", "))
	}
	main, _ := raw["main"].(string)
	if strings.TrimSpace(main) == "" {
		return nil, fmt.Errorf("Workers main entrypoint is required")
	}
	path, err := workersSafePath(root, main)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Workers main: %w", err)
	}
	rel, _ := filepath.Rel(root, path)
	p := &workersProject{Main: filepath.ToSlash(rel), Format: workersScriptFormat(string(data)), CompatibilityDate: workersCompatibilityDate, SuggestedEnv: map[string]string{}}
	// Wrangler module entrypoints may export an imported framework handler.
	if p.Format == "" {
		p.Format = "modules"
	}
	if value, exists := raw["compatibility_date"]; exists {
		date, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("Workers compatibility_date must be a date string")
		}
		parsed, err := time.Parse("2006-01-02", date)
		if err != nil || parsed.Format("2006-01-02") != date || date > workersCompatibilityDate {
			return nil, fmt.Errorf("Workers compatibility_date %q is invalid or newer than runtime %s", date, workersRuntimeVersion)
		}
		p.CompatibilityDate = date
	}
	if flags, exists := raw["compatibility_flags"]; exists {
		items, ok := flags.([]any)
		if !ok {
			return nil, fmt.Errorf("Workers compatibility_flags must be a string array")
		}
		for _, item := range items {
			flag, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("Workers compatibility_flags must be a string array")
			}
			p.CompatibilityFlags = append(p.CompatibilityFlags, flag)
		}
	}
	if value, exists := raw["vars"]; exists {
		vars, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Workers vars must be an object")
		}
		for key, value := range vars {
			if text, ok := value.(string); ok {
				p.SuggestedEnv[key] = text
			} else {
				data, err := json.Marshal(value)
				if err != nil {
					return nil, err
				}
				p.SuggestedEnv[key] = string(data)
				p.JSONVars = append(p.JSONVars, key)
			}
		}
		sort.Strings(p.JSONVars)
	}
	return p, nil
}

func workersSafePath(root, rel string) (string, error) {
	path, err := secureRepoJoin(root, rel)
	if err != nil {
		return "", err
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	if resolved != resolvedRoot && !strings.HasPrefix(resolved, resolvedRoot+string(filepath.Separator)) {
		return "", fmt.Errorf("Workers path %q escapes source directory through a symlink", rel)
	}
	return resolved, nil
}

// Strip comments and string/template bodies before looking for a module handler.
// For service workers keep literal strings so only a real fetch listener matches.
func workersScriptFormat(source string) string {
	masked := workersMaskJS(source, true)
	if workersModuleHandler.MatchString(masked) {
		return "modules"
	}
	for _, match := range workersServiceHandler.FindAllStringIndex(workersMaskJS(source, false), -1) {
		if strings.HasPrefix(masked[match[0]:], "addEventListener") {
			return "service-worker"
		}
	}
	return ""
}

func workersMaskJS(source string, maskStrings bool) string {
	b := []byte(source)
	for i := 0; i < len(b); {
		start := i
		if i+1 < len(b) && b[i] == '/' && (b[i+1] == '/' || b[i+1] == '*') {
			line := b[i+1] == '/'
			i += 2
			for i < len(b) {
				if line && b[i] == '\n' {
					break
				}
				if !line && i+1 < len(b) && b[i] == '*' && b[i+1] == '/' {
					i += 2
					break
				}
				i++
			}
			for j := start; j < i; j++ {
				if b[j] != '\n' {
					b[j] = ' '
				}
			}
			continue
		}
		if b[i] == '\'' || b[i] == '"' || b[i] == '`' {
			quote := b[i]
			i++
			for i < len(b) {
				if b[i] == '\\' {
					i += 2
					continue
				}
				if b[i] == quote {
					i++
					break
				}
				i++
			}
			if i > len(b) {
				i = len(b)
			}
			if maskStrings || quote == '`' {
				for j := start; j < i; j++ {
					if b[j] != '\n' {
						b[j] = ' '
					}
				}
			}
			continue
		}
		i++
	}
	return string(b)
}

func workersJSONC(data []byte) []byte {
	// Comments are removed while preserving quoted URLs and escaped quotes.
	b := []byte(workersMaskJS(string(data), false))
	inString, escaped := false, false
	for i := 0; i < len(b); i++ {
		if inString {
			if escaped {
				escaped = false
			} else if b[i] == '\\' {
				escaped = true
			} else if b[i] == '"' {
				inString = false
			}
			continue
		}
		if b[i] == '"' {
			inString = true
			continue
		}
		if b[i] == ',' {
			j := i + 1
			for j < len(b) && (b[j] == ' ' || b[j] == '\n' || b[j] == '\r' || b[j] == '\t') {
				j++
			}
			if j < len(b) && (b[j] == '}' || b[j] == ']') {
				b[i] = ' '
			}
		}
	}
	return b
}

func workersBuildOverlay(repoDir, sourceDir string) (*workersProject, []sourceOverlayFile, error) {
	p, err := detectWorkersProject(repoDir, sourceDir)
	if err != nil {
		return nil, nil, err
	}
	if p == nil {
		return nil, nil, fmt.Errorf("no Workers entrypoint found; declare main in a Wrangler configuration")
	}
	if _, err := os.Lstat(filepath.Join(repoDir, workersOverlayDir)); !os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("reserved Workers build directory %s already exists", workersOverlayDir)
	}
	normalized, err := normalizeRepoSourceDir(repoDir, sourceDir)
	if err != nil {
		return nil, nil, err
	}
	meta := struct {
		*workersProject
		SourceDir string `json:"sourceDir"`
	}{p, normalized}
	data, err := json.Marshal(meta)
	if err != nil {
		return nil, nil, err
	}
	files := []sourceOverlayFile{{RelativePath: workersOverlayDir + "/project.json", Content: string(data)}}
	entries, err := workersRuntimeFiles.ReadDir("workers_runtime")
	if err != nil {
		return nil, nil, err
	}
	for _, entry := range entries {
		data, err := workersRuntimeFiles.ReadFile("workers_runtime/" + entry.Name())
		if err != nil {
			return nil, nil, err
		}
		files = append(files, sourceOverlayFile{RelativePath: workersOverlayDir + "/" + entry.Name(), Content: strings.NewReplacer("__WORKERD_VERSION__", workersRuntimeVersion, "__ESBUILD_VERSION__", workersEsbuildVersion).Replace(string(data))})
	}
	return p, files, nil
}

func buildWorkersImage(ctx context.Context, repoDir, sourceDir string, req dockerfileBuildRequest) (GitHubImportResult, error) {
	p, overlay, err := workersBuildOverlay(repoDir, sourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	req.DockerfilePath = workersOverlayDir + "/Dockerfile"
	req.BuildContextDir = "."
	req.SourceOverlayFiles = overlay
	job := buildJobName(req)
	if err := buildAndPushDockerfileImage(ctx, req); err != nil {
		return GitHubImportResult{}, err
	}
	return GitHubImportResult{BuildStrategy: model.AppBuildStrategyWorkers, SourceDir: normalizeImportedSourceDirValue(sourceDir), ImageRef: req.ImageRef, DestinationImageRef: req.DestinationImageRef, BuildJobName: job, DetectedPort: workersPort, ExposesPublicService: true, DetectedProvider: "workerd", DetectedStack: "cloudflare-workers", SuggestedEnv: p.SuggestedEnv}, nil
}

func (i *Importer) ImportGitHubWorkers(ctx context.Context, req GitHubAutoImportRequest) (GitHubImportResult, error) {
	repo, err := i.cloneGitHubRepo(ctx, req.RepoURL, req.RepoAuthToken, req.Branch, "github-workers-import-*")
	if err != nil {
		return GitHubImportResult{}, err
	}
	defer releaseClonedRepo(repo)
	return importWorkersFromClonedRepo(ctx, repo, req, i)
}

func importWorkersFromClonedRepo(ctx context.Context, repo clonedGitHubRepo, req GitHubAutoImportRequest, i *Importer) (GitHubImportResult, error) {
	image := defaultImportedImageRef(req.RegistryPushBase, req.ImageRepository, repo, req.ImageNameSuffix, req.JobLabels)
	result, err := buildWorkersImage(ctx, repo.RepoDir, req.SourceDir, dockerfileBuildRequest{RepoURL: req.RepoURL, RepoAuthToken: req.RepoAuthToken, Branch: repo.Branch, CommitSHA: repo.CommitSHA, ImageRef: image, DestinationImageRef: importBuilderDestinationImageRef(image, req.RegistryPushBase, req.DestinationRegistryPushBase), JobLabels: req.JobLabels, PlacementNodeSelector: req.PlacementNodeSelector, PodPolicy: i.BuilderPolicy, WorkloadProfile: builderWorkloadProfileFor(model.AppBuildStrategyWorkers, req.Stateful), Logger: i.Logger})
	result.RepoOwner = repo.RepoOwner
	result.RepoName = repo.RepoName
	result.Branch = repo.Branch
	result.CommitSHA = repo.CommitSHA
	result.CommitCommittedAt = repo.CommitCommittedAt
	result.DefaultAppName = repo.DefaultAppName
	return result, err
}

func (i *Importer) importWorkersFromUpload(ctx context.Context, src extractedUploadSource, req UploadSourceImportRequest) (GitHubImportResult, error) {
	if strings.TrimSpace(req.ArchiveDownloadURL) == "" {
		return GitHubImportResult{}, fmt.Errorf("archive download url is required for Workers builds")
	}
	image := defaultUploadedImageRef(req.RegistryPushBase, req.ImageRepository, src.DefaultAppName, src.ArchiveSHA256, req.ImageNameSuffix, req.JobLabels)
	result, err := buildWorkersImage(ctx, src.RootDir, req.SourceDir, dockerfileBuildRequest{CommitSHA: src.ArchiveSHA256, SourceLabel: src.DefaultAppName, ArchiveDownloadURL: req.ArchiveDownloadURL, ImageRef: image, DestinationImageRef: importBuilderDestinationImageRef(image, req.RegistryPushBase, req.DestinationRegistryPushBase), JobLabels: req.JobLabels, PlacementNodeSelector: req.PlacementNodeSelector, PodPolicy: i.BuilderPolicy, WorkloadProfile: builderWorkloadProfileFor(model.AppBuildStrategyWorkers, req.Stateful), Logger: i.Logger})
	result.CommitSHA = src.ArchiveSHA256
	result.DefaultAppName = src.DefaultAppName
	return result, err
}
