package sourceimport

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

var nodePublicServiceDependencies = []string{
	"@nestjs/core",
	"@nestjs/platform-express",
	"@nestjs/platform-fastify",
	"@nuxt/kit",
	"@remix-run/node",
	"@sveltejs/kit",
	"astro",
	"express",
	"fastify",
	"gatsby",
	"hono",
	"koa",
	"next",
	"nuxt",
	"react",
	"react-dom",
	"serve",
	"solid-start",
	"vite",
}

var nodePublicServiceScriptMarkers = []string{
	"astro ",
	"express",
	"fastify",
	"gatsby ",
	"http-server",
	"nest ",
	"next ",
	"nuxt ",
	"react-scripts",
	"remix ",
	"serve ",
	"solid-start",
	"vite ",
}

var goPublicServiceMarkers = []string{
	".listenandserve(",
	".listenandservetls(",
	".newsourcemux(",
	".handlefunc(",
	"gin.default(",
	"gin.new(",
	"fiber.new(",
	"echo.new(",
	"chi.newrouter(",
	"mux.newrouter(",
	"httprouter.new(",
}

var javaPublicServiceMarkers = []string{
	"spring-boot-starter-web",
	"spring-boot-starter-webflux",
	"quarkus-resteasy",
	"quarkus-rest",
	"micronaut-http-server-netty",
	"io.javalin",
	"ktor-server",
}

var rubyPublicServiceMarkers = []string{
	"gem \"rails\"",
	"gem 'rails'",
	"gem \"sinatra\"",
	"gem 'sinatra'",
	"gem \"hanami\"",
	"gem 'hanami'",
	"gem \"puma\"",
	"gem 'puma'",
	"gem \"rackup\"",
	"gem 'rackup'",
}

var phpPublicServiceMarkers = []string{
	"\"laravel/framework\"",
	"\"symfony/http-kernel\"",
	"\"slim/slim\"",
	"\"cakephp/cakephp\"",
}

var dotnetPublicServiceMarkers = []string{
	`sdk="microsoft.net.sdk.web"`,
	`sdk='microsoft.net.sdk.web'`,
	"microsoft.aspnetcore",
}

var rustPublicServiceMarkers = []string{
	"actix-web",
	"axum",
	"warp",
	"rocket",
	"poem",
	"salvo",
}

func readPackageManifest(appDir string) (packageManifest, error) {
	manifestPath := filepath.Join(appDir, "package.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return packageManifest{}, err
	}

	var manifest packageManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return packageManifest{}, err
	}
	return manifest, nil
}

func (manifest packageManifest) hasAnyDependency(names ...string) bool {
	for _, name := range names {
		if manifest.hasDependency(name) {
			return true
		}
	}
	return false
}

func (manifest packageManifest) scriptContainsAny(markers ...string) bool {
	if len(manifest.Scripts) == 0 {
		return false
	}

	for _, script := range manifest.Scripts {
		content := strings.ToLower(strings.TrimSpace(script))
		if content == "" {
			continue
		}
		for _, marker := range markers {
			if strings.Contains(content, strings.ToLower(strings.TrimSpace(marker))) {
				return true
			}
		}
	}
	return false
}

func detectNodePublicService(appDir string) bool {
	manifest, err := readPackageManifest(appDir)
	if err != nil {
		return false
	}

	return manifest.hasAnyDependency(nodePublicServiceDependencies...) ||
		manifest.scriptContainsAny(nodePublicServiceScriptMarkers...)
}

func detectGoPublicService(appDir string) bool {
	return projectFilesContainAny(appDir, ".go", goPublicServiceMarkers)
}

func detectJavaPublicService(appDir string) bool {
	return anyFileContainsAny([]string{
		filepath.Join(appDir, "pom.xml"),
		filepath.Join(appDir, "build.gradle"),
		filepath.Join(appDir, "build.gradle.kts"),
	}, javaPublicServiceMarkers)
}

func detectRubyPublicService(appDir string) bool {
	return anyFileContainsAny([]string{
		filepath.Join(appDir, "Gemfile"),
	}, rubyPublicServiceMarkers)
}

func detectPHPPublicService(appDir string) bool {
	return anyFileContainsAny([]string{
		filepath.Join(appDir, "composer.json"),
	}, phpPublicServiceMarkers)
}

func detectDotnetPublicService(appDir string) bool {
	return projectFilesContainAny(appDir, ".csproj", dotnetPublicServiceMarkers)
}

func detectRustPublicService(appDir string) bool {
	return anyFileContainsAny([]string{
		filepath.Join(appDir, "Cargo.toml"),
	}, rustPublicServiceMarkers)
}

func anyFileContainsAny(paths []string, markers []string) bool {
	for _, path := range paths {
		if fileContainsAny(path, markers) {
			return true
		}
	}
	return false
}

func fileContainsAny(path string, markers []string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	content := strings.ToLower(string(data))
	for _, marker := range markers {
		if strings.Contains(content, strings.ToLower(strings.TrimSpace(marker))) {
			return true
		}
	}
	return false
}

func projectFilesContainAny(appDir, extension string, markers []string) bool {
	found := false
	_ = filepath.WalkDir(appDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if entry.IsDir() {
			if path != appDir && shouldSkipProjectScanDir(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if extension != "" && filepath.Ext(entry.Name()) != extension {
			return nil
		}
		if fileContainsAny(path, markers) {
			found = true
			return fs.SkipAll
		}
		return nil
	})
	return found
}

func shouldSkipProjectScanDir(name string) bool {
	switch name {
	case ".git", ".hg", ".svn", ".venv", ".tox", ".pytest_cache", ".mypy_cache", ".ruff_cache",
		"__pycache__", "build", "coverage", "dist", "env", "node_modules", "site-packages",
		"target", "tmp", "vendor", "venv":
		return true
	default:
		return strings.HasPrefix(name, ".")
	}
}
