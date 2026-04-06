package sourceimport

import (
	"bufio"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type systemPackageAnalysis struct {
	Packages                []string
	HasExplicitBuildpackApt bool
}

type systemCommandPattern struct {
	Pattern *regexp.Regexp
}

var (
	pythonSystemPackageAliasByImport = map[string][]string{
		"git": {"git"},
	}
	pythonSystemPackageAliasByRequirement = map[string][]string{
		"gitpython": {"git"},
	}
	nodeSystemPackageAliasByDependency = map[string][]string{
		"simple-git": {"git"},
	}
	systemPackageAliasByCommand = map[string][]string{
		"convert":   {"imagemagick"},
		"curl":      {"curl"},
		"ffmpeg":    {"ffmpeg"},
		"ffprobe":   {"ffmpeg"},
		"git":       {"git"},
		"gs":        {"ghostscript"},
		"identify":  {"imagemagick"},
		"jq":        {"jq"},
		"magick":    {"imagemagick"},
		"mysql":     {"default-mysql-client"},
		"mysqldump": {"default-mysql-client"},
		"pdfinfo":   {"poppler-utils"},
		"pdftotext": {"poppler-utils"},
		"pg_dump":   {"postgresql-client"},
		"psql":      {"postgresql-client"},
		"redis-cli": {"redis-tools"},
		"rsync":     {"rsync"},
		"scp":       {"openssh-client"},
		"ssh":       {"openssh-client"},
		"tesseract": {"tesseract-ocr"},
		"wget":      {"wget"},
	}
	systemPackageCommandPatterns = []systemCommandPattern{
		{Pattern: regexp.MustCompile(`(?m)\bsubprocess\.(?:run|Popen|call|check_call|check_output)\s*\(\s*(?:\[[^\]]*?)?["']([^"'\s]+)`)},
		{Pattern: regexp.MustCompile(`(?m)\bos\.(?:system|popen)\s*\(\s*["']([^"'\s]+)`)},
		{Pattern: regexp.MustCompile(`(?m)\bexec\.CommandContext\s*\(\s*[^,]+,\s*["']([^"']+)["']`)},
		{Pattern: regexp.MustCompile(`(?m)\bexec\.Command\s*\(\s*["']([^"']+)["']`)},
		{Pattern: regexp.MustCompile(`(?m)\b(?:spawn|execFile|execSync|spawnSync|execa)\s*\(\s*["']([^"']+)["']`)},
		{Pattern: regexp.MustCompile(`(?m)\bexec\s*\(\s*["']([^"'\s]+)`)},
		{Pattern: regexp.MustCompile(`(?m)\b(?:system|exec|spawn|shell_exec|passthru|proc_open)\s*\(?\s*["']([^"'\s]+)`)},
	}
	systemPackageTextExtensions = map[string]struct{}{
		".bash": {},
		".cjs":  {},
		".go":   {},
		".js":   {},
		".jsx":  {},
		".ksh":  {},
		".mjs":  {},
		".php":  {},
		".py":   {},
		".rb":   {},
		".sh":   {},
		".ts":   {},
		".tsx":  {},
		".zsh":  {},
	}
	systemPackageTextNames = map[string]struct{}{
		"Makefile": {},
		"Procfile": {},
		"Justfile": {},
	}
)

func analyzeSystemPackages(repoDir, sourceDir string) (systemPackageAnalysis, error) {
	normalizedSourceDir, err := normalizeRepoSourceDir(repoDir, sourceDir)
	if err != nil {
		return systemPackageAnalysis{}, err
	}

	appDir := repoDir
	if normalizedSourceDir != "." {
		appDir = filepath.Join(repoDir, filepath.FromSlash(normalizedSourceDir))
	}
	return analyzeSystemPackagesInDir(appDir)
}

func analyzeSystemPackagesInDir(appDir string) (systemPackageAnalysis, error) {
	analysis := systemPackageAnalysis{
		HasExplicitBuildpackApt: pathExists(filepath.Join(appDir, "Aptfile")),
	}

	packageSet := make(map[string]struct{})

	if imports, err := collectPythonImportsInDir(appDir); err != nil {
		return analysis, err
	} else {
		addPackagesFromAliasMap(packageSet, pythonSystemPackageAliasByImport, imports)
	}

	if requirements, err := collectPythonRequirementNames(appDir); err != nil {
		return analysis, err
	} else {
		addPackagesFromAliasMap(packageSet, pythonSystemPackageAliasByRequirement, requirements)
	}

	if dependencies, err := collectNodeDependencyNames(appDir); err != nil {
		return analysis, err
	} else {
		addPackagesFromAliasMap(packageSet, nodeSystemPackageAliasByDependency, dependencies)
	}

	if commands, err := collectKnownSystemCommandsInDir(appDir); err != nil {
		return analysis, err
	} else {
		addPackagesFromAliasMap(packageSet, systemPackageAliasByCommand, commands)
	}

	analysis.Packages = sortedPackages(packageSet)
	return analysis, nil
}

func buildBuildpacksSystemPackageOverlayFiles(repoDir, sourceDir string) ([]sourceOverlayFile, systemPackageAnalysis, error) {
	analysis, err := analyzeSystemPackages(repoDir, sourceDir)
	if err != nil {
		return nil, analysis, err
	}
	if len(analysis.Packages) == 0 || analysis.HasExplicitBuildpackApt {
		return nil, analysis, nil
	}
	return []sourceOverlayFile{
		{
			RelativePath:  "Aptfile",
			Content:       buildGeneratedBuildpackAptfile(analysis.Packages),
			OnlyIfMissing: true,
		},
	}, analysis, nil
}

func collectPythonImportsInDir(appDir string) ([]string, error) {
	_, pythonFiles, err := collectPythonSourceFiles(appDir)
	if err != nil {
		return nil, err
	}

	importSet := make(map[string]struct{})
	for _, filePath := range pythonFiles {
		contentBytes, err := os.ReadFile(filePath)
		if err != nil {
			return nil, err
		}
		for _, importPath := range collectPythonImportPaths(string(contentBytes)) {
			importPath = strings.TrimSpace(importPath)
			if importPath == "" {
				continue
			}
			importSet[strings.ToLower(importPath)] = struct{}{}
		}
	}

	return sortedPackages(importSet), nil
}

func collectPythonRequirementNames(appDir string) ([]string, error) {
	manifestPath := filepath.Join(appDir, "requirements.txt")
	if !pathExists(manifestPath) {
		return nil, nil
	}
	file, err := os.Open(manifestPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	requirements := make(map[string]struct{})
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		name := normalizePythonRequirementName(scanner.Text())
		if name == "" {
			continue
		}
		requirements[name] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return sortedPackages(requirements), nil
}

func normalizePythonRequirementName(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.HasPrefix(raw, "#") || strings.HasPrefix(raw, "-") {
		return ""
	}
	if hashIndex := strings.Index(raw, "#"); hashIndex >= 0 {
		raw = strings.TrimSpace(raw[:hashIndex])
	}
	if envIndex := strings.Index(raw, ";"); envIndex >= 0 {
		raw = strings.TrimSpace(raw[:envIndex])
	}
	for _, delimiter := range []string{"==", ">=", "<=", "!=", "~=", ">", "<"} {
		if before, _, ok := strings.Cut(raw, delimiter); ok {
			raw = strings.TrimSpace(before)
			break
		}
	}
	if extraIndex := strings.Index(raw, "["); extraIndex >= 0 {
		raw = strings.TrimSpace(raw[:extraIndex])
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	return strings.ToLower(strings.ReplaceAll(raw, "_", "-"))
}

func collectNodeDependencyNames(appDir string) ([]string, error) {
	manifestPath := filepath.Join(appDir, "package.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var manifest packageManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, nil
	}

	dependencies := make(map[string]struct{})
	for _, deps := range []map[string]any{
		manifest.Dependencies,
		manifest.OptionalDependencies,
	} {
		for name := range deps {
			name = strings.TrimSpace(strings.ToLower(name))
			if name == "" {
				continue
			}
			dependencies[name] = struct{}{}
		}
	}
	return sortedPackages(dependencies), nil
}

func collectKnownSystemCommandsInDir(appDir string) ([]string, error) {
	commandSet := make(map[string]struct{})
	err := filepath.WalkDir(appDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != appDir && shouldSkipSystemPackageScanDir(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !shouldScanSystemPackageFile(path, entry.Name()) {
			return nil
		}
		info, err := entry.Info()
		if err == nil && info.Size() > 1<<20 {
			return nil
		}
		contentBytes, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		content := string(contentBytes)
		for command := range collectKnownSystemCommandsFromContent(content, entry.Name()) {
			commandSet[command] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sortedPackages(commandSet), nil
}

func shouldSkipSystemPackageScanDir(name string) bool {
	switch name {
	case ".git", ".hg", ".svn", ".venv", ".tox", ".pytest_cache", ".mypy_cache", ".ruff_cache",
		".next", "__pycache__", "build", "coverage", "dist", "env", "node_modules", "site-packages", "target", "tmp", "vendor", "venv":
		return true
	default:
		return strings.HasPrefix(name, ".")
	}
}

func shouldScanSystemPackageFile(path, name string) bool {
	if _, ok := systemPackageTextNames[name]; ok {
		return true
	}
	ext := strings.ToLower(filepath.Ext(path))
	_, ok := systemPackageTextExtensions[ext]
	return ok
}

func collectKnownSystemCommandsFromContent(content, fileName string) map[string]struct{} {
	commandSet := make(map[string]struct{})
	for _, pattern := range systemPackageCommandPatterns {
		matches := pattern.Pattern.FindAllStringSubmatch(content, -1)
		for _, match := range matches {
			if len(match) < 2 {
				continue
			}
			command := normalizeKnownSystemCommand(match[1])
			if _, ok := systemPackageAliasByCommand[command]; ok {
				commandSet[command] = struct{}{}
			}
		}
	}

	if isShellLikeSystemPackageFile(fileName) {
		scanner := bufio.NewScanner(strings.NewReader(content))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if strings.EqualFold(fileName, "Procfile") {
				if _, after, ok := strings.Cut(line, ":"); ok {
					line = strings.TrimSpace(after)
				}
			}
			for _, command := range splitShellCommandCandidates(line) {
				command = normalizeKnownSystemCommand(command)
				if _, ok := systemPackageAliasByCommand[command]; ok {
					commandSet[command] = struct{}{}
				}
			}
		}
	}

	return commandSet
}

func isShellLikeSystemPackageFile(fileName string) bool {
	switch strings.ToLower(fileName) {
	case "procfile", "makefile", "justfile":
		return true
	default:
		ext := strings.ToLower(filepath.Ext(fileName))
		switch ext {
		case ".sh", ".bash", ".zsh", ".ksh":
			return true
		default:
			return false
		}
	}
}

func splitShellCommandCandidates(line string) []string {
	fields := strings.FieldsFunc(line, func(r rune) bool {
		switch r {
		case ' ', '\t', ';', '|', '&':
			return true
		default:
			return false
		}
	})
	if len(fields) == 0 {
		return nil
	}
	index := 0
	for index < len(fields) {
		field := strings.TrimSpace(fields[index])
		switch {
		case field == "":
			index++
		case field == "sudo" || field == "env":
			index++
		case strings.Contains(field, "=") && !strings.HasPrefix(field, "=") && !strings.HasSuffix(field, "="):
			index++
		default:
			candidates := make([]string, 0, 1)
			candidates = append(candidates, field)
			return candidates
		}
	}
	return nil
}

func normalizeKnownSystemCommand(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	raw = strings.Trim(raw, `"'`)
	if raw == "" {
		return ""
	}
	raw = filepath.Base(raw)
	return strings.ToLower(raw)
}

func addPackagesFromAliasMap(packageSet map[string]struct{}, aliasMap map[string][]string, keys []string) {
	for _, key := range keys {
		key = strings.TrimSpace(strings.ToLower(key))
		if key == "" {
			continue
		}
		packages, ok := aliasMap[key]
		if !ok {
			continue
		}
		for _, pkg := range packages {
			pkg = strings.TrimSpace(pkg)
			if pkg == "" {
				continue
			}
			packageSet[pkg] = struct{}{}
		}
	}
}

func sortedPackages(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	items := make([]string, 0, len(values))
	for value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		items = append(items, value)
	}
	sort.Strings(items)
	return items
}

func buildGeneratedBuildpackAptfile(packages []string) string {
	return strings.Join(packages, "\n") + "\n"
}
