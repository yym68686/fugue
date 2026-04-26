package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunVersionCheckLatestShowsAvailableRelease(t *testing.T) {
	restore := overrideCLITestBuildInfo(t, "v1.2.3", "abc123", "2026-04-15T00:00:00Z")
	defer restore()

	server := newCLIReleaseTestServer(t, "v1.2.4", []byte("new-binary"))
	defer server.Close()

	t.Setenv("FUGUE_RELEASE_API_URL", server.URL)
	overrideCLIUserCacheDir(t, t.TempDir())

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams([]string{"version", "--check-latest"}, &stdout, &stderr); err != nil {
		t.Fatalf("run version --check-latest: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"version=v1.2.3",
		"commit=abc123",
		"built_at=2026-04-15T00:00:00Z",
		"latest_version=v1.2.4",
		"update_available=true",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected version output to contain %q, got %q", want, out)
		}
	}
}

func TestRunRootVersionFlagShowsBuildInfo(t *testing.T) {
	restore := overrideCLITestBuildInfo(t, "v1.2.3", "abc123", "2026-04-15T00:00:00Z")
	defer restore()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams([]string{"--version"}, &stdout, &stderr); err != nil {
		t.Fatalf("run --version: %v", err)
	}

	if got := stderr.String(); got != "" {
		t.Fatalf("expected --version to keep stderr quiet, got %q", got)
	}
	out := stdout.String()
	for _, want := range []string{
		"version=v1.2.3",
		"commit=abc123",
		"built_at=2026-04-15T00:00:00Z",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected --version output to contain %q, got %q", want, out)
		}
	}
}

func TestRunUpgradeCheckShowsFromAndToVersions(t *testing.T) {
	restore := overrideCLITestBuildInfo(t, "v1.2.3", "abc123", "2026-04-15T00:00:00Z")
	defer restore()

	server := newCLIReleaseTestServer(t, "v1.2.4", []byte("new-binary"))
	defer server.Close()

	t.Setenv("FUGUE_RELEASE_API_URL", server.URL)
	overrideCLIUserCacheDir(t, t.TempDir())

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams([]string{"upgrade", "--check"}, &stdout, &stderr); err != nil {
		t.Fatalf("run upgrade --check: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"from_version=v1.2.3",
		"to_version=v1.2.4",
		"status=available",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected upgrade check output to contain %q, got %q", want, out)
		}
	}
}

func TestRunUpgradeCheckTreatsDevBuildAsUpgradeable(t *testing.T) {
	restore := overrideCLITestBuildInfo(t, "dev-deadbeef", "deadbeef", "2026-04-15T00:00:00Z")
	defer restore()

	server := newCLIReleaseTestServer(t, "v1.2.4", []byte("new-binary"))
	defer server.Close()

	t.Setenv("FUGUE_RELEASE_API_URL", server.URL)
	overrideCLIUserCacheDir(t, t.TempDir())

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams([]string{"upgrade", "--check"}, &stdout, &stderr); err != nil {
		t.Fatalf("run upgrade --check for dev build: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"from_version=dev-deadbeef",
		"to_version=v1.2.4",
		"status=available",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected upgrade check output to contain %q, got %q", want, out)
		}
	}
}

func TestRunUpgradeReplacesCurrentBinary(t *testing.T) {
	restore := overrideCLITestBuildInfo(t, "v1.2.3", "abc123", "2026-04-15T00:00:00Z")
	defer restore()
	restorePlatform := overrideCLIPlatform(t, "linux", "amd64")
	defer restorePlatform()

	server := newCLIReleaseTestServer(t, "v1.2.4", []byte("new-binary"))
	defer server.Close()

	t.Setenv("FUGUE_RELEASE_API_URL", server.URL)
	overrideCLIUserCacheDir(t, t.TempDir())

	executablePath := filepath.Join(t.TempDir(), "fugue")
	if err := os.WriteFile(executablePath, []byte("old-binary"), 0o755); err != nil {
		t.Fatalf("write fake executable: %v", err)
	}
	restoreExecutable := overrideCLIExecutablePath(t, executablePath)
	defer restoreExecutable()
	resolvedExecutablePath, err := resolveCLIExecutableDestination()
	if err != nil {
		t.Fatalf("resolve executable destination: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams([]string{"upgrade"}, &stdout, &stderr); err != nil {
		t.Fatalf("run upgrade: %v", err)
	}

	installed, err := os.ReadFile(executablePath)
	if err != nil {
		t.Fatalf("read upgraded executable: %v", err)
	}
	if string(installed) != "new-binary" {
		t.Fatalf("expected upgraded executable contents, got %q", string(installed))
	}

	out := stdout.String()
	for _, want := range []string{
		"from_version=v1.2.3",
		"to_version=v1.2.4",
		"status=upgraded",
		"binary_path=" + resolvedExecutablePath,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected upgrade output to contain %q, got %q", want, out)
		}
	}
}

func TestMaybeWarnAboutCLIUpdatePrintsOncePerCacheWindow(t *testing.T) {
	restore := overrideCLITestBuildInfo(t, "v1.2.3", "abc123", "2026-04-15T00:00:00Z")
	defer restore()

	server := newCLIReleaseTestServer(t, "v1.2.4", []byte("new-binary"))
	defer server.Close()

	t.Setenv("FUGUE_RELEASE_API_URL", server.URL)
	overrideCLIUserCacheDir(t, t.TempDir())

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cli := newCLI(&stdout, &stderr)
	root := cli.newRootCommand()
	projectCmd, _, err := root.Find([]string{"project"})
	if err != nil {
		t.Fatalf("find project command: %v", err)
	}

	cli.maybeWarnAboutCLIUpdate(projectCmd)
	if got := stderr.String(); !strings.Contains(got, "A new fugue CLI is available: v1.2.3 -> v1.2.4. Run 'fugue upgrade' to update.") {
		t.Fatalf("expected update reminder, got %q", got)
	}

	stderr.Reset()
	cli.maybeWarnAboutCLIUpdate(projectCmd)
	if got := stderr.String(); got != "" {
		t.Fatalf("expected cached reminder to stay quiet, got %q", got)
	}
}

func newCLIReleaseTestServer(t *testing.T, latestVersion string, binaryPayload []byte) *httptest.Server {
	t.Helper()

	archiveName := "fugue_linux_amd64.tar.gz"
	archiveBytes := buildCLITarGz(t, "fugue", binaryPayload)
	checksum := sha256Hex(t, archiveBytes)

	mux := http.NewServeMux()
	var server *httptest.Server
	mux.HandleFunc("/repos/yym68686/fugue/releases/latest", func(w http.ResponseWriter, r *http.Request) {
		payload := githubReleasePayload{
			TagName: latestVersion,
			HTMLURL: "https://github.com/yym68686/fugue/releases/tag/" + latestVersion,
			Assets: []githubReleaseAsset{
				{Name: archiveName, BrowserDownloadURL: server.URL + "/downloads/" + archiveName},
				{Name: cliChecksumsAssetName, BrowserDownloadURL: server.URL + "/downloads/" + cliChecksumsAssetName},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Fatalf("encode release payload: %v", err)
		}
	})
	mux.HandleFunc("/downloads/"+archiveName, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(archiveBytes)
	})
	mux.HandleFunc("/downloads/"+cliChecksumsAssetName, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintf(w, "%s  %s\n", checksum, archiveName)
	})

	server = httptest.NewServer(mux)
	return server
}

func buildCLITarGz(t *testing.T, fileName string, payload []byte) []byte {
	t.Helper()

	var compressed bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressed)
	tarWriter := tar.NewWriter(gzipWriter)

	header := &tar.Header{
		Name: fileName,
		Mode: 0o755,
		Size: int64(len(payload)),
	}
	if err := tarWriter.WriteHeader(header); err != nil {
		t.Fatalf("write tar header: %v", err)
	}
	if _, err := tarWriter.Write(payload); err != nil {
		t.Fatalf("write tar payload: %v", err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	return compressed.Bytes()
}

func sha256Hex(t *testing.T, payload []byte) string {
	t.Helper()

	filePath := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(filePath, payload, 0o644); err != nil {
		t.Fatalf("write payload file: %v", err)
	}
	checksum, err := sha256File(filePath)
	if err != nil {
		t.Fatalf("sha256 file: %v", err)
	}
	return checksum
}

func overrideCLITestBuildInfo(t *testing.T, version, commit, builtAt string) func() {
	t.Helper()

	previousVersion := buildVersion
	previousCommit := buildCommit
	previousBuiltAt := buildTime
	buildVersion = version
	buildCommit = commit
	buildTime = builtAt
	return func() {
		buildVersion = previousVersion
		buildCommit = previousCommit
		buildTime = previousBuiltAt
	}
}

func overrideCLIUserCacheDir(t *testing.T, cacheDir string) {
	t.Helper()

	previous := cliUserCacheDir
	cliUserCacheDir = func() (string, error) {
		return cacheDir, nil
	}
	t.Cleanup(func() {
		cliUserCacheDir = previous
	})
}

func overrideCLIPlatform(t *testing.T, goos, goarch string) func() {
	t.Helper()

	previousOS := cliOS
	previousArch := cliArch
	cliOS = goos
	cliArch = goarch
	return func() {
		cliOS = previousOS
		cliArch = previousArch
	}
}

func overrideCLIExecutablePath(t *testing.T, executablePath string) func() {
	t.Helper()

	previous := cliExecutablePath
	cliExecutablePath = func() (string, error) {
		return executablePath, nil
	}
	return func() {
		cliExecutablePath = previous
	}
}
