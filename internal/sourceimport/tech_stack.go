package sourceimport

import (
	"os"
	"path/filepath"
	"strings"
)

type packageManifest struct {
	Dependencies         map[string]any    `json:"dependencies"`
	DevDependencies      map[string]any    `json:"devDependencies"`
	PeerDependencies     map[string]any    `json:"peerDependencies"`
	OptionalDependencies map[string]any    `json:"optionalDependencies"`
	Scripts              map[string]string `json:"scripts"`
}

func detectPrimaryTechStack(repoDir, sourceDir string) string {
	for _, dir := range techStackDetectionDirs(repoDir, sourceDir) {
		if stack := detectPrimaryTechStackInDir(dir); stack != "" {
			return stack
		}
	}
	return ""
}

func techStackDetectionDirs(repoDir, sourceDir string) []string {
	repoDir = filepath.Clean(strings.TrimSpace(repoDir))
	if repoDir == "" {
		return nil
	}

	startDir := repoDir
	sourceDir = strings.TrimSpace(sourceDir)
	if sourceDir != "" && sourceDir != "." {
		if filepath.IsAbs(sourceDir) {
			startDir = filepath.Clean(sourceDir)
		} else {
			startDir = filepath.Join(repoDir, filepath.FromSlash(sourceDir))
		}
	}

	if info, err := os.Stat(startDir); err == nil && !info.IsDir() {
		startDir = filepath.Dir(startDir)
	}
	if !pathWithinRepo(repoDir, startDir) {
		startDir = repoDir
	}

	dirs := make([]string, 0, 4)
	seen := make(map[string]struct{})
	for current := startDir; ; current = filepath.Dir(current) {
		if !pathWithinRepo(repoDir, current) {
			break
		}
		if _, ok := seen[current]; !ok {
			seen[current] = struct{}{}
			dirs = append(dirs, current)
		}
		if current == repoDir {
			break
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
	}
	if len(dirs) == 0 {
		return []string{repoDir}
	}
	return dirs
}

func pathWithinRepo(repoDir, target string) bool {
	rel, err := filepath.Rel(filepath.Clean(repoDir), filepath.Clean(target))
	if err != nil {
		return false
	}
	return rel == "." || (!strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != "..")
}

func detectPrimaryTechStackInDir(appDir string) string {
	pythonAnalysis, err := analyzePythonProjectInDir(appDir)
	if err != nil {
		pythonAnalysis = pythonProjectAnalysis{}
	}

	switch {
	case pathExists(filepath.Join(appDir, "package.json")):
		return detectNodeTechStack(appDir)
	case pythonAnalysis.IsPythonProject:
		return "python"
	case pathExists(filepath.Join(appDir, "go.mod")):
		return "go"
	case pathExists(filepath.Join(appDir, "pom.xml")) ||
		pathExists(filepath.Join(appDir, "build.gradle")) ||
		pathExists(filepath.Join(appDir, "build.gradle.kts")):
		return "java"
	case pathExists(filepath.Join(appDir, "Gemfile")):
		return "ruby"
	case pathExists(filepath.Join(appDir, "composer.json")):
		return "php"
	case hasGlob(filepath.Join(appDir, "*.csproj")):
		return "dotnet"
	case pathExists(filepath.Join(appDir, "Cargo.toml")):
		return "rust"
	default:
		return ""
	}
}

func detectNodeTechStack(appDir string) string {
	manifest, err := readPackageManifest(appDir)
	if err != nil {
		return "nodejs"
	}

	if manifest.hasDependency("next") {
		return "nextjs"
	}
	if manifest.hasDependency("react") || manifest.hasDependency("react-dom") {
		return "react"
	}
	return "nodejs"
}

func (manifest packageManifest) hasDependency(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for _, deps := range []map[string]any{
		manifest.Dependencies,
		manifest.DevDependencies,
		manifest.PeerDependencies,
		manifest.OptionalDependencies,
	} {
		if deps == nil {
			continue
		}
		if _, ok := deps[name]; ok {
			return true
		}
	}
	return false
}

func (manifest packageManifest) hasScript(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" || len(manifest.Scripts) == 0 {
		return false
	}
	value, ok := manifest.Scripts[name]
	return ok && strings.TrimSpace(value) != ""
}
