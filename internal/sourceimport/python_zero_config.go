package sourceimport

import (
	"bufio"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type pythonProjectAnalysis struct {
	IsPythonProject       bool
	HasDependencyManifest bool
	DetectedPort          int
	InferredRequirements  []string
}

var (
	pythonUvicornPortPattern = regexp.MustCompile(`(?s)\buvicorn\.run\s*\([^)]*?\bport\s*=\s*([0-9]{2,5})`)
	pythonAppRunPortPattern  = regexp.MustCompile(`(?s)\bapp\.run\s*\([^)]*?\bport\s*=\s*([0-9]{2,5})`)
	pythonImportNamePattern  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

var pythonDependencyAliasByImport = map[string]string{
	"bs4":                  "beautifulsoup4",
	"cloudscraper":         "cloudscraper",
	"cv2":                  "opencv-python",
	"Crypto":               "pycryptodome",
	"dateutil":             "python-dateutil",
	"dotenv":               "python-dotenv",
	"fastapi":              "fastapi",
	"fitz":                 "PyMuPDF",
	"google.generativeai":  "google-generativeai",
	"googleapiclient":      "google-api-python-client",
	"jwt":                  "PyJWT",
	"multipart":            "python-multipart",
	"OpenSSL":              "pyOpenSSL",
	"PIL":                  "Pillow",
	"pkg_resources":        "setuptools",
	"playwright.async_api": "playwright",
	"playwright.sync_api":  "playwright",
	"sklearn":              "scikit-learn",
	"uvicorn":              "uvicorn",
	"yaml":                 "PyYAML",
}

var pythonStdlibModules = map[string]struct{}{
	"__future__": {}, "abc": {}, "argparse": {}, "array": {}, "ast": {}, "asyncio": {}, "base64": {}, "binascii": {},
	"bisect": {}, "builtins": {}, "bz2": {}, "calendar": {}, "cmath": {}, "collections": {}, "concurrent": {},
	"configparser": {}, "contextlib": {}, "contextvars": {}, "copy": {}, "csv": {}, "ctypes": {}, "dataclasses": {},
	"datetime": {}, "decimal": {}, "difflib": {}, "dis": {}, "email": {}, "enum": {}, "faulthandler": {}, "fcntl": {},
	"filecmp": {}, "fileinput": {}, "fnmatch": {}, "fractions": {}, "functools": {}, "gc": {}, "getopt": {},
	"getpass": {}, "gettext": {}, "glob": {}, "graphlib": {}, "gzip": {}, "hashlib": {}, "heapq": {}, "hmac": {},
	"html": {}, "http": {}, "imaplib": {}, "importlib": {}, "inspect": {}, "io": {}, "ipaddress": {}, "itertools": {},
	"json": {}, "logging": {}, "lzma": {}, "mailbox": {}, "math": {}, "mimetypes": {}, "multiprocessing": {},
	"netrc": {}, "operator": {}, "os": {}, "pathlib": {}, "pickle": {}, "pkgutil": {}, "platform": {}, "plistlib": {},
	"pprint": {}, "profile": {}, "pstats": {}, "queue": {}, "random": {}, "re": {}, "resource": {}, "runpy": {},
	"sched": {}, "secrets": {}, "selectors": {}, "shelve": {}, "shlex": {}, "shutil": {}, "signal": {}, "site": {},
	"smtplib": {}, "socket": {}, "socketserver": {}, "sqlite3": {}, "ssl": {}, "stat": {}, "statistics": {},
	"string": {}, "stringprep": {}, "struct": {}, "subprocess": {}, "sys": {}, "sysconfig": {}, "tarfile": {},
	"tempfile": {}, "textwrap": {}, "threading": {}, "time": {}, "timeit": {}, "tkinter": {}, "token": {},
	"tokenize": {}, "traceback": {}, "tracemalloc": {}, "tty": {}, "types": {}, "typing": {}, "unicodedata": {},
	"unittest": {}, "urllib": {}, "uuid": {}, "venv": {}, "warnings": {}, "wave": {}, "weakref": {}, "webbrowser": {},
	"wsgiref": {}, "xml": {}, "xmlrpc": {}, "zipfile": {}, "zipimport": {}, "zoneinfo": {},
}

func analyzePythonProject(repoDir, sourceDir string) (pythonProjectAnalysis, error) {
	normalizedSourceDir, err := normalizeRepoSourceDir(repoDir, sourceDir)
	if err != nil {
		return pythonProjectAnalysis{}, err
	}

	appDir := repoDir
	if normalizedSourceDir != "." {
		appDir = filepath.Join(repoDir, filepath.FromSlash(normalizedSourceDir))
	}

	return analyzePythonProjectInDir(appDir)
}

func analyzePythonProjectInDir(appDir string) (pythonProjectAnalysis, error) {
	analysis := pythonProjectAnalysis{
		HasDependencyManifest: hasPythonDependencyManifest(appDir),
	}

	localModules, pythonFiles, err := collectPythonSourceFiles(appDir)
	if err != nil {
		return analysis, err
	}

	if analysis.HasDependencyManifest || len(pythonFiles) > 0 {
		analysis.IsPythonProject = true
	}
	if len(pythonFiles) == 0 {
		return analysis, nil
	}

	imports := make(map[string]struct{})
	for _, filePath := range pythonFiles {
		contentBytes, err := os.ReadFile(filePath)
		if err != nil {
			return analysis, err
		}
		content := string(contentBytes)
		for _, importPath := range collectPythonImportPaths(content) {
			imports[importPath] = struct{}{}
		}
		if analysis.DetectedPort == 0 {
			analysis.DetectedPort = detectPythonPortFromContent(content)
		}
	}

	if !analysis.HasDependencyManifest {
		analysis.InferredRequirements = inferPythonRequirements(imports, localModules)
	}

	return analysis, nil
}

func buildPythonOverlayFiles(repoDir, sourceDir string) ([]sourceOverlayFile, pythonProjectAnalysis, error) {
	analysis, err := analyzePythonProject(repoDir, sourceDir)
	if err != nil {
		return nil, analysis, err
	}
	if !analysis.IsPythonProject || analysis.HasDependencyManifest {
		return nil, analysis, nil
	}

	return []sourceOverlayFile{
		{
			RelativePath:  "requirements.txt",
			Content:       buildGeneratedPythonRequirements(analysis.InferredRequirements),
			OnlyIfMissing: true,
		},
	}, analysis, nil
}

func hasPythonDependencyManifest(appDir string) bool {
	return pathExists(filepath.Join(appDir, "pyproject.toml")) ||
		pathExists(filepath.Join(appDir, "requirements.txt")) ||
		pathExists(filepath.Join(appDir, "Pipfile"))
}

func collectPythonSourceFiles(appDir string) (map[string]struct{}, []string, error) {
	localModules := make(map[string]struct{})
	pythonFiles := make([]string, 0)

	err := filepath.WalkDir(appDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != appDir && shouldSkipPythonScanDir(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(entry.Name()) != ".py" {
			return nil
		}

		pythonFiles = append(pythonFiles, path)
		rel, err := filepath.Rel(appDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		parts := strings.Split(rel, "/")
		switch len(parts) {
		case 0:
			return nil
		case 1:
			moduleName := strings.TrimSuffix(parts[0], ".py")
			if moduleName != "__init__" && pythonImportNamePattern.MatchString(moduleName) {
				localModules[moduleName] = struct{}{}
			}
		default:
			if pythonImportNamePattern.MatchString(parts[0]) {
				localModules[parts[0]] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	return localModules, pythonFiles, nil
}

func shouldSkipPythonScanDir(name string) bool {
	switch name {
	case ".git", ".hg", ".svn", ".venv", ".tox", ".pytest_cache", ".mypy_cache", ".ruff_cache",
		"__pycache__", "build", "dist", "env", "node_modules", "site-packages", "venv":
		return true
	default:
		return strings.HasPrefix(name, ".")
	}
}

func collectPythonImportPaths(content string) []string {
	imports := make([]string, 0)
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "import ") {
			rest := strings.TrimSpace(strings.TrimPrefix(line, "import "))
			for _, clause := range strings.Split(rest, ",") {
				clause = strings.TrimSpace(clause)
				if clause == "" {
					continue
				}
				fields := strings.Fields(clause)
				if len(fields) == 0 {
					continue
				}
				imports = append(imports, fields[0])
			}
			continue
		}
		if !strings.HasPrefix(line, "from ") {
			continue
		}
		rest := strings.TrimSpace(strings.TrimPrefix(line, "from "))
		modulePath, _, ok := strings.Cut(rest, " import ")
		if !ok {
			continue
		}
		modulePath = strings.TrimSpace(modulePath)
		if modulePath == "" || strings.HasPrefix(modulePath, ".") {
			continue
		}
		imports = append(imports, modulePath)
	}
	return imports
}

func inferPythonRequirements(imports map[string]struct{}, localModules map[string]struct{}) []string {
	if len(imports) == 0 {
		return nil
	}

	requirementSet := make(map[string]struct{})
	for importPath := range imports {
		requirement := inferPythonRequirement(importPath, localModules)
		if requirement == "" {
			continue
		}
		requirementSet[requirement] = struct{}{}
	}

	requirements := make([]string, 0, len(requirementSet))
	for requirement := range requirementSet {
		requirements = append(requirements, requirement)
	}
	sort.Strings(requirements)
	return requirements
}

func inferPythonRequirement(importPath string, localModules map[string]struct{}) string {
	importPath = strings.TrimSpace(importPath)
	if importPath == "" {
		return ""
	}
	if requirement, ok := pythonDependencyAliasByImport[importPath]; ok {
		return requirement
	}

	topLevel := importPath
	if head, _, ok := strings.Cut(importPath, "."); ok {
		topLevel = head
	}
	topLevel = strings.TrimSpace(topLevel)
	if topLevel == "" {
		return ""
	}
	if _, ok := localModules[topLevel]; ok {
		return ""
	}
	if _, ok := pythonStdlibModules[topLevel]; ok {
		return ""
	}
	if requirement, ok := pythonDependencyAliasByImport[topLevel]; ok {
		return requirement
	}
	return strings.ToLower(topLevel)
}

func detectPythonPortFromContent(content string) int {
	for _, pattern := range []*regexp.Regexp{pythonUvicornPortPattern, pythonAppRunPortPattern} {
		matches := pattern.FindStringSubmatch(content)
		if len(matches) < 2 {
			continue
		}
		port, err := strconv.Atoi(matches[1])
		if err == nil && port > 0 && port <= 65535 {
			return port
		}
	}
	return 0
}

func buildGeneratedPythonRequirements(requirements []string) string {
	lines := []string{
		"# Generated by Fugue because no Python dependency manifest was found.",
		"# Replace this with an explicit requirements.txt or pyproject.toml for reproducible builds.",
	}
	lines = append(lines, requirements...)
	return strings.Join(lines, "\n") + "\n"
}
