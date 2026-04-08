package sourceimport

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const generatedStaticSiteDockerfilePath = ".fugue/Dockerfile.static-site-build"

var staticSiteExplicitCandidates = []string{
	".",
	"dist",
	"build",
	"public",
	"site",
	"out",
	".output/public",
}

var staticSiteAutoCandidates = []string{
	"dist",
	"build",
	"site",
	"out",
	".output/public",
	".",
}

var staticSiteBuildOutputCandidates = []string{
	"dist",
	"build",
	"out",
	"site",
	".output/public",
}

var staticSiteSourceBuildDependencies = []string{
	"@angular/core",
	"@sveltejs/kit",
	"astro",
	"gatsby",
	"lit",
	"parcel",
	"preact",
	"react",
	"react-dom",
	"rollup",
	"solid-js",
	"svelte",
	"vite",
	"vue",
	"webpack",
}

var staticSiteSourceBuildScriptMarkers = []string{
	"astro build",
	"gatsby build",
	"ng build",
	"parcel build",
	"react-scripts build",
	"rollup --config",
	"rollup -c",
	"vite build",
	"vitepress build",
	"webpack",
}

var staticSiteSourceBuildConfigFiles = []string{
	"angular.json",
	"astro.config.js",
	"astro.config.mjs",
	"astro.config.ts",
	"jsconfig.json",
	"parcelrc",
	".parcelrc",
	"postcss.config.cjs",
	"postcss.config.js",
	"postcss.config.mjs",
	"postcss.config.ts",
	"rollup.config.cjs",
	"rollup.config.js",
	"rollup.config.mjs",
	"rollup.config.ts",
	"svelte.config.js",
	"svelte.config.ts",
	"tailwind.config.cjs",
	"tailwind.config.js",
	"tailwind.config.mjs",
	"tailwind.config.ts",
	"tsconfig.json",
	"vite.config.cjs",
	"vite.config.js",
	"vite.config.mjs",
	"vite.config.ts",
	"webpack.config.cjs",
	"webpack.config.js",
	"webpack.config.mjs",
	"webpack.config.ts",
}

var staticSiteSourceEntryExtensions = []string{
	".astro",
	".cts",
	".jsx",
	".mts",
	".svelte",
	".ts",
	".tsx",
	".vue",
}

type staticSiteImportPlan struct {
	SourceDir        string
	DetectedProvider string
	DetectedStack    string
	DockerfilePath   string
	BuildContextDir  string
	SourceOverlay    []sourceOverlayFile
}

func staticSiteCandidatePaths(repoDir string, candidates []string) []string {
	paths := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		fullPath := repoDir
		if candidate != "." {
			fullPath = filepath.Join(repoDir, filepath.FromSlash(candidate))
		}
		paths = append(paths, fullPath)
	}
	return paths
}

func detectStaticSiteDir(repoDir, requested string) (string, error) {
	candidates := staticSiteCandidatePaths(repoDir, staticSiteExplicitCandidates)
	if trimmed := strings.TrimSpace(requested); trimmed != "" {
		candidates = staticSiteCandidatePaths(repoDir, []string{trimmed})
	}

	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err != nil || !info.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(candidate, "index.html")); err == nil {
			return candidate, nil
		}
	}

	if strings.TrimSpace(requested) != "" {
		return "", fmt.Errorf("source_dir %q does not contain index.html", requested)
	}
	return "", fmt.Errorf("no static-site entrypoint found; this MVP currently requires index.html in root, dist/, build/, public/, site/, out/, or .output/public/")
}

func detectAutoStaticSiteDir(repoDir string) (string, error) {
	for _, candidate := range staticSiteCandidatePaths(repoDir, staticSiteAutoCandidates) {
		info, err := os.Stat(candidate)
		if err != nil || !info.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(candidate, "index.html")); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("no ready static-site entrypoint found")
}

func planStaticSiteImport(repoDir, sourceDir string) (staticSiteImportPlan, error) {
	normalizedSourceDir, err := normalizeRepoSourceDir(repoDir, sourceDir)
	if err != nil {
		return staticSiteImportPlan{}, err
	}
	plan := staticSiteImportPlan{
		SourceDir:     normalizeImportedSourceDirValue(normalizedSourceDir),
		DetectedStack: detectPrimaryTechStack(repoDir, normalizedSourceDir),
	}
	if !shouldBuildStaticSiteSource(repoDir, normalizedSourceDir) {
		return plan, nil
	}

	dockerfileContent := buildStaticSiteSourceDockerfile(normalizedSourceDir)
	plan.DetectedProvider = "nodejs"
	plan.DockerfilePath = generatedStaticSiteDockerfilePath
	plan.BuildContextDir = "."
	plan.SourceOverlay = []sourceOverlayFile{
		{
			RelativePath: ".dockerignore",
			Content: strings.Join([]string{
				".git",
				".gitmodules",
				"node_modules",
				"coverage",
				"tmp",
				"",
			}, "\n"),
		},
		{
			RelativePath: generatedStaticSiteDockerfilePath,
			Content:      dockerfileContent,
		},
	}
	return plan, nil
}

func shouldBuildStaticSiteSource(repoDir, sourceDir string) bool {
	appDir := repoDir
	if strings.TrimSpace(sourceDir) != "" && strings.TrimSpace(sourceDir) != "." {
		appDir = filepath.Join(repoDir, filepath.FromSlash(sourceDir))
	}
	if !pathExists(filepath.Join(appDir, "index.html")) || !pathExists(filepath.Join(appDir, "package.json")) {
		return false
	}

	manifest, err := readPackageManifest(appDir)
	if err != nil || !manifest.hasScript("build") {
		return false
	}

	return staticSiteSourceHasBuildConfig(appDir) ||
		manifest.hasAnyDependency(staticSiteSourceBuildDependencies...) ||
		manifest.scriptContainsAny(staticSiteSourceBuildScriptMarkers...) ||
		staticSiteIndexReferencesSourceEntrypoints(filepath.Join(appDir, "index.html"))
}

func staticSiteSourceHasBuildConfig(appDir string) bool {
	for _, name := range staticSiteSourceBuildConfigFiles {
		if pathExists(filepath.Join(appDir, name)) {
			return true
		}
	}
	return false
}

func staticSiteIndexReferencesSourceEntrypoints(indexPath string) bool {
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return false
	}
	content := strings.ToLower(string(data))
	for _, ext := range staticSiteSourceEntryExtensions {
		quoted := strings.ToLower(ext) + `"`
		singleQuoted := strings.ToLower(ext) + `'`
		if strings.Contains(content, quoted) || strings.Contains(content, singleQuoted) {
			return true
		}
	}
	return false
}

func buildStaticSiteSourceDockerfile(sourceDir string) string {
	sourceDir = strings.TrimSpace(sourceDir)
	if sourceDir == "" {
		sourceDir = "."
	}
	return fmt.Sprintf(`FROM docker.io/library/node:22-alpine AS build
WORKDIR /workspace
COPY . /workspace/
RUN set -eu; \
    apk add --no-cache git python3 make g++ >/dev/null; \
    source_dir=%s; \
    cd "$source_dir"; \
    export CI=true; \
    corepack enable >/dev/null 2>&1 || true; \
    if [ -f pnpm-lock.yaml ]; then \
      corepack prepare pnpm@latest --activate >/dev/null 2>&1 || true; \
      pnpm install --frozen-lockfile || pnpm install; \
      pnpm run build; \
    elif [ -f yarn.lock ]; then \
      corepack prepare yarn@stable --activate >/dev/null 2>&1 || true; \
      yarn install --frozen-lockfile || yarn install --immutable || yarn install; \
      yarn run build; \
    elif [ -f package-lock.json ] || [ -f npm-shrinkwrap.json ]; then \
      npm ci || npm install; \
      npm run build; \
    else \
      npm install; \
      npm run build; \
    fi; \
    output_dir=''; \
    for candidate in %s; do \
      if [ -f "$candidate/index.html" ]; then output_dir="$candidate"; break; fi; \
    done; \
    if [ -z "$output_dir" ]; then \
      echo "no static build output found after build; expected index.html in dist/, build/, out/, site/, or .output/public/" >&2; \
      exit 1; \
    fi; \
    mkdir -p /tmp/fugue-static-site; \
    cp -R "$output_dir"/. /tmp/fugue-static-site/

FROM %s
COPY --from=build /tmp/fugue-static-site/ /usr/share/caddy/
`, shellQuoteForOverlay(sourceDir), staticSiteDockerfileCandidateList(), staticSiteBaseImage)
}

func staticSiteDockerfileCandidateList() string {
	quoted := make([]string, 0, len(staticSiteBuildOutputCandidates))
	for _, candidate := range staticSiteBuildOutputCandidates {
		quoted = append(quoted, shellQuoteForOverlay(candidate))
	}
	return strings.Join(quoted, " ")
}
