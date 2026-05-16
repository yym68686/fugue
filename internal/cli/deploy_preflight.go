package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"fugue/internal/model"
)

func validateLocalDeployPreflight(rootDir string, opts deployCommonOptions) error {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return fmt.Errorf("deploy preflight failed: project directory is required")
	}

	archiveIgnore, err := loadSourceArchiveIgnore(rootDir)
	if err != nil {
		return err
	}

	var issues []string
	if sourceDir := strings.TrimSpace(opts.SourceDir); sourceDir != "" {
		issues = append(issues, validatePreflightDirectory(rootDir, sourceDir, "source_dir", archiveIgnore)...)
	}
	if buildContext := strings.TrimSpace(opts.BuildContextDir); buildContext != "" {
		issues = append(issues, validatePreflightDirectory(rootDir, buildContext, "build_context_dir", archiveIgnore)...)
	}

	dockerfilePath := strings.TrimSpace(opts.DockerfilePath)
	buildStrategy := strings.TrimSpace(opts.BuildStrategy)
	if dockerfilePath != "" || strings.EqualFold(buildStrategy, model.AppBuildStrategyDockerfile) {
		if dockerfilePath == "" {
			dockerfilePath = "Dockerfile"
		}
		issues = append(issues, validatePreflightFile(rootDir, dockerfilePath, "dockerfile_path", archiveIgnore)...)
	} else if fileExists(filepath.Join(rootDir, "Dockerfile")) {
		issues = append(issues, validatePreflightFile(rootDir, "Dockerfile", "dockerfile_path", archiveIgnore)...)
	}

	if len(issues) > 0 {
		return fmt.Errorf("deploy preflight failed: %s", strings.Join(issues, "; "))
	}
	return nil
}

func validatePreflightDirectory(rootDir, relPath, field string, archiveIgnore sourceArchiveIgnore) []string {
	relPath, err := normalizePreflightRelativePath(relPath)
	if err != nil {
		return []string{fmt.Sprintf("%s %q is invalid: %v", field, relPath, err)}
	}
	fullPath := filepath.Join(rootDir, filepath.FromSlash(relPath))
	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{fmt.Sprintf("%s %q does not exist", field, relPath)}
		}
		return []string{fmt.Sprintf("%s %q cannot be read: %v", field, relPath, err)}
	}
	if !info.IsDir() {
		return []string{fmt.Sprintf("%s %q is not a directory", field, relPath)}
	}
	if preflightArchiveExcludesPath(relPath, true, archiveIgnore) {
		return []string{fmt.Sprintf("%s %q is excluded from the upload archive by .dockerignore or Fugue's local archive rules", field, relPath)}
	}
	return nil
}

func validatePreflightFile(rootDir, relPath, field string, archiveIgnore sourceArchiveIgnore) []string {
	relPath, err := normalizePreflightRelativePath(relPath)
	if err != nil {
		return []string{fmt.Sprintf("%s %q is invalid: %v", field, relPath, err)}
	}
	fullPath := filepath.Join(rootDir, filepath.FromSlash(relPath))
	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{fmt.Sprintf("%s %q does not exist", field, relPath)}
		}
		return []string{fmt.Sprintf("%s %q cannot be read: %v", field, relPath, err)}
	}
	if info.IsDir() {
		return []string{fmt.Sprintf("%s %q is a directory, not a file", field, relPath)}
	}
	if preflightArchiveExcludesPath(relPath, false, archiveIgnore) {
		return []string{fmt.Sprintf("%s %q is excluded from the upload archive by .dockerignore or Fugue's local archive rules", field, relPath)}
	}
	return nil
}

func normalizePreflightRelativePath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "." {
		return ".", nil
	}
	if filepath.IsAbs(raw) {
		return "", fmt.Errorf("must be relative to the project root")
	}
	cleaned := filepath.ToSlash(filepath.Clean(raw))
	if cleaned == "." {
		return ".", nil
	}
	if strings.HasPrefix(cleaned, "../") || cleaned == ".." {
		return "", fmt.Errorf("must stay inside the project root")
	}
	return cleaned, nil
}

func preflightArchiveExcludesPath(relPath string, isDir bool, archiveIgnore sourceArchiveIgnore) bool {
	for _, segment := range strings.Split(relPath, "/") {
		if shouldSkipArchiveSegment(segment, isDir) {
			return true
		}
	}
	if !archiveIgnore.hasExcludes {
		return false
	}
	matched, err := archiveIgnore.matcher.MatchesOrParentMatches(relPath)
	return err == nil && matched
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
