package cli

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	defaultCLIReleaseRepo       = "yym68686/fugue"
	defaultCLIReleaseAPIBaseURL = "https://api.github.com"
	cliUpdateCheckTTL           = 12 * time.Hour
	cliUpdateNotifyTTL          = 24 * time.Hour
	cliUpdateCheckTimeout       = 1200 * time.Millisecond
	cliUpgradeFetchTimeout      = 30 * time.Second
	cliChecksumsAssetName       = "fugue_checksums.txt"
)

var (
	buildVersion = "dev"
	buildCommit  = "unknown"
	buildTime    = ""

	cliNow            = time.Now
	cliUserCacheDir   = os.UserCacheDir
	cliExecutablePath = os.Executable
	cliOS             = runtime.GOOS
	cliArch           = runtime.GOARCH
)

type cliBuildInfo struct {
	Version string `json:"version"`
	Commit  string `json:"commit,omitempty"`
	BuiltAt string `json:"built_at,omitempty"`
}

type cliVersionCommandResult struct {
	Version         string `json:"version"`
	Commit          string `json:"commit,omitempty"`
	BuiltAt         string `json:"built_at,omitempty"`
	UpdateAvailable bool   `json:"update_available,omitempty"`
	LatestVersion   string `json:"latest_version,omitempty"`
	ReleaseURL      string `json:"release_url,omitempty"`
}

type cliUpgradeResult struct {
	FromVersion     string `json:"from_version"`
	ToVersion       string `json:"to_version,omitempty"`
	UpdateAvailable bool   `json:"update_available,omitempty"`
	UpToDate        bool   `json:"up_to_date,omitempty"`
	Scheduled       bool   `json:"scheduled,omitempty"`
	BinaryPath      string `json:"binary_path,omitempty"`
	ReleaseURL      string `json:"release_url,omitempty"`
	Status          string `json:"status,omitempty"`
}

type cliUpdateCheckCache struct {
	Repo           string `json:"repo"`
	CurrentVersion string `json:"current_version"`
	LatestVersion  string `json:"latest_version,omitempty"`
	ReleaseURL     string `json:"release_url,omitempty"`
	CheckedAt      string `json:"checked_at,omitempty"`
	NotifiedAt     string `json:"notified_at,omitempty"`
}

type githubReleasePayload struct {
	TagName string               `json:"tag_name"`
	HTMLURL string               `json:"html_url"`
	Assets  []githubReleaseAsset `json:"assets"`
}

type githubReleaseAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

type cliReleaseInfo struct {
	TagName string
	HTMLURL string
	Assets  map[string]string
}

func (c *CLI) newVersionCommand() *cobra.Command {
	opts := struct {
		CheckLatest bool
	}{}

	cmd := &cobra.Command{
		Use:   "version",
		Short: "Show the Fugue CLI build version and available updates",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			info := currentCLIBuildInfo()
			result := cliVersionCommandResult{
				Version: info.Version,
				Commit:  info.Commit,
				BuiltAt: info.BuiltAt,
			}
			if opts.CheckLatest {
				ctx, cancel := context.WithTimeout(cmd.Context(), cliUpdateCheckTimeout)
				defer cancel()

				release, err := fetchCLIRelease(ctx, "latest")
				if err != nil {
					return err
				}
				result.LatestVersion = release.TagName
				result.ReleaseURL = release.HTMLURL
				result.UpdateAvailable = shouldUpgradeToCLIRelease(info.Version, release.TagName)
			}

			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return renderCLIVersionResult(c.stdout, result)
		},
	}
	cmd.Flags().BoolVar(&opts.CheckLatest, "check-latest", false, "Also look up the latest released Fugue CLI version")
	return cmd
}

func (c *CLI) newUpgradeCommand() *cobra.Command {
	opts := struct {
		Check bool
	}{}

	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade this Fugue CLI binary to the latest released version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			info := currentCLIBuildInfo()

			c.progressf("checking the latest Fugue CLI release")
			ctx, cancel := context.WithTimeout(cmd.Context(), cliUpgradeFetchTimeout)
			defer cancel()

			release, err := fetchCLIRelease(ctx, "latest")
			if err != nil {
				return err
			}

			result := cliUpgradeResult{
				FromVersion:     info.Version,
				ToVersion:       release.TagName,
				UpdateAvailable: shouldUpgradeToCLIRelease(info.Version, release.TagName),
				ReleaseURL:      release.HTMLURL,
			}

			if opts.Check {
				result.UpToDate = !result.UpdateAvailable
				if result.UpToDate {
					result.Status = "up-to-date"
				} else {
					result.Status = "available"
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, result)
				}
				return renderCLIUpgradeResult(c.stdout, result)
			}

			if !result.UpdateAvailable {
				result.UpToDate = true
				result.Status = "up-to-date"
				if c.wantsJSON() {
					return writeJSON(c.stdout, result)
				}
				return renderCLIUpgradeResult(c.stdout, result)
			}

			destinationPath, err := resolveCLIExecutableDestination()
			if err != nil {
				return err
			}
			result.BinaryPath = destinationPath

			archiveName, err := cliArchiveAssetName()
			if err != nil {
				return err
			}

			c.progressf("downloading %s for %s -> %s", archiveName, info.Version, release.TagName)
			extractedBinary, cleanup, err := downloadAndExtractCLIReleaseBinary(ctx, release)
			if err != nil {
				return err
			}

			c.progressf("installing %s", destinationPath)
			scheduled, err := installCLIReleaseBinary(extractedBinary, destinationPath)
			if err != nil {
				cleanup()
				return err
			}
			if !scheduled {
				cleanup()
			}
			result.Scheduled = scheduled
			if scheduled {
				result.Status = "scheduled"
			} else {
				result.Status = "upgraded"
			}

			resetCLIUpdateNoticeCache(info.Version, release.TagName, release.HTMLURL)

			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return renderCLIUpgradeResult(c.stdout, result)
		},
	}
	cmd.Flags().BoolVar(&opts.Check, "check", false, "Only check whether a newer Fugue CLI release is available")
	return cmd
}

func currentCLIBuildInfo() cliBuildInfo {
	return cliBuildInfo{
		Version: firstNonEmptyTrimmed(buildVersion, "dev"),
		Commit:  firstNonEmptyTrimmed(buildCommit),
		BuiltAt: firstNonEmptyTrimmed(buildTime),
	}
}

func renderCLIVersionResult(w io.Writer, result cliVersionCommandResult) error {
	pairs := []kvPair{
		{Key: "version", Value: result.Version},
		{Key: "commit", Value: result.Commit},
		{Key: "built_at", Value: result.BuiltAt},
	}
	if result.LatestVersion != "" {
		pairs = append(pairs,
			kvPair{Key: "latest_version", Value: result.LatestVersion},
			kvPair{Key: "update_available", Value: strconv.FormatBool(result.UpdateAvailable)},
		)
		if result.ReleaseURL != "" {
			pairs = append(pairs, kvPair{Key: "release_url", Value: result.ReleaseURL})
		}
	}
	return writeKeyValues(w, pairs...)
}

func renderCLIUpgradeResult(w io.Writer, result cliUpgradeResult) error {
	pairs := []kvPair{
		{Key: "from_version", Value: result.FromVersion},
		{Key: "to_version", Value: result.ToVersion},
		{Key: "status", Value: result.Status},
	}
	if result.Status == "" {
		pairs = append(pairs,
			kvPair{Key: "update_available", Value: strconv.FormatBool(result.UpdateAvailable)},
			kvPair{Key: "up_to_date", Value: strconv.FormatBool(result.UpToDate)},
		)
	}
	if result.BinaryPath != "" {
		pairs = append(pairs, kvPair{Key: "binary_path", Value: result.BinaryPath})
	}
	if result.ReleaseURL != "" {
		pairs = append(pairs, kvPair{Key: "release_url", Value: result.ReleaseURL})
	}
	return writeKeyValues(w, pairs...)
}

func (c *CLI) maybeWarnAboutCLIUpdate(cmd *cobra.Command) {
	if c == nil || c.wantsJSON() || shouldSkipCLIUpdateCheck(cmd) {
		return
	}

	currentVersion := currentCLIReleaseTag()
	if currentVersion == "" {
		return
	}

	cache, _ := loadCLIUpdateCheckCache()
	now := cliNow()
	repo := cliReleaseRepo()
	if cacheValidForCurrentCLI(cache, repo, currentVersion, now) {
		if hasNewerCLIRelease(currentVersion, cache.LatestVersion) && noticeDue(cache, now) {
			c.progressf("A new fugue CLI is available: %s -> %s. Run 'fugue upgrade' to update.", currentVersion, cache.LatestVersion)
			cache.NotifiedAt = now.UTC().Format(time.RFC3339)
			_ = saveCLIUpdateCheckCache(cache)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), cliUpdateCheckTimeout)
	defer cancel()

	release, err := fetchCLIRelease(ctx, "latest")
	if err != nil {
		return
	}

	cache = cliUpdateCheckCache{
		Repo:           repo,
		CurrentVersion: currentVersion,
		LatestVersion:  release.TagName,
		ReleaseURL:     release.HTMLURL,
		CheckedAt:      now.UTC().Format(time.RFC3339),
	}
	if hasNewerCLIRelease(currentVersion, release.TagName) && noticeDue(cache, now) {
		c.progressf("A new fugue CLI is available: %s -> %s. Run 'fugue upgrade' to update.", currentVersion, release.TagName)
		cache.NotifiedAt = now.UTC().Format(time.RFC3339)
	}
	_ = saveCLIUpdateCheckCache(cache)
}

func shouldSkipCLIUpdateCheck(cmd *cobra.Command) bool {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("FUGUE_SKIP_UPDATE_CHECK")), "1") ||
		strings.EqualFold(strings.TrimSpace(os.Getenv("FUGUE_SKIP_UPDATE_CHECK")), "true") {
		return true
	}
	if cmd == nil {
		return false
	}
	switch cmd.CommandPath() {
	case "fugue version", "fugue upgrade":
		return true
	default:
		return false
	}
}

func currentCLIReleaseTag() string {
	version := canonicalizeReleaseTag(currentCLIBuildInfo().Version)
	if !isComparableReleaseTag(version) {
		return ""
	}
	return version
}

func canonicalizeReleaseTag(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if strings.EqualFold(trimmed, "latest") {
		return "latest"
	}
	if strings.HasPrefix(trimmed, "v") || strings.HasPrefix(trimmed, "V") {
		return "v" + strings.TrimPrefix(strings.TrimPrefix(trimmed, "v"), "V")
	}
	if trimmed[0] >= '0' && trimmed[0] <= '9' {
		return "v" + trimmed
	}
	return trimmed
}

func hasNewerCLIRelease(currentVersion, latestVersion string) bool {
	current := canonicalizeReleaseTag(currentVersion)
	latest := canonicalizeReleaseTag(latestVersion)
	if !isComparableReleaseTag(current) || !isComparableReleaseTag(latest) {
		return false
	}
	return compareReleaseTags(current, latest) < 0
}

func shouldUpgradeToCLIRelease(currentVersion, latestVersion string) bool {
	latest := canonicalizeReleaseTag(latestVersion)
	if latest == "" || latest == "latest" {
		return false
	}

	current := canonicalizeReleaseTag(currentVersion)
	if current == "" {
		return true
	}
	if isComparableReleaseTag(current) && isComparableReleaseTag(latest) {
		return compareReleaseTags(current, latest) < 0
	}
	return current != latest
}

func isComparableReleaseTag(raw string) bool {
	_, ok := parseReleaseTag(raw)
	return ok
}

type parsedReleaseTag struct {
	Numbers    []int
	PreRelease []string
}

func parseReleaseTag(raw string) (parsedReleaseTag, bool) {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimPrefix(trimmed, "v")
	trimmed = strings.TrimPrefix(trimmed, "V")
	if trimmed == "" {
		return parsedReleaseTag{}, false
	}
	if plus := strings.Index(trimmed, "+"); plus >= 0 {
		trimmed = trimmed[:plus]
	}
	preRelease := ""
	if dash := strings.Index(trimmed, "-"); dash >= 0 {
		preRelease = trimmed[dash+1:]
		trimmed = trimmed[:dash]
	}
	numberParts := strings.Split(trimmed, ".")
	numbers := make([]int, 0, len(numberParts))
	for _, part := range numberParts {
		part = strings.TrimSpace(part)
		if part == "" {
			return parsedReleaseTag{}, false
		}
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return parsedReleaseTag{}, false
		}
		numbers = append(numbers, value)
	}
	parsed := parsedReleaseTag{Numbers: numbers}
	if preRelease != "" {
		parsed.PreRelease = strings.Split(preRelease, ".")
	}
	return parsed, true
}

func compareReleaseTags(left, right string) int {
	parsedLeft, okLeft := parseReleaseTag(left)
	parsedRight, okRight := parseReleaseTag(right)
	if !okLeft || !okRight {
		return strings.Compare(canonicalizeReleaseTag(left), canonicalizeReleaseTag(right))
	}

	maxLength := len(parsedLeft.Numbers)
	if len(parsedRight.Numbers) > maxLength {
		maxLength = len(parsedRight.Numbers)
	}
	for index := 0; index < maxLength; index++ {
		leftValue := 0
		rightValue := 0
		if index < len(parsedLeft.Numbers) {
			leftValue = parsedLeft.Numbers[index]
		}
		if index < len(parsedRight.Numbers) {
			rightValue = parsedRight.Numbers[index]
		}
		switch {
		case leftValue < rightValue:
			return -1
		case leftValue > rightValue:
			return 1
		}
	}
	return compareReleasePreRelease(parsedLeft.PreRelease, parsedRight.PreRelease)
}

func compareReleasePreRelease(left, right []string) int {
	if len(left) == 0 && len(right) == 0 {
		return 0
	}
	if len(left) == 0 {
		return 1
	}
	if len(right) == 0 {
		return -1
	}

	maxLength := len(left)
	if len(right) > maxLength {
		maxLength = len(right)
	}
	for index := 0; index < maxLength; index++ {
		switch {
		case index >= len(left):
			return -1
		case index >= len(right):
			return 1
		}
		leftPart := strings.TrimSpace(left[index])
		rightPart := strings.TrimSpace(right[index])
		leftNumber, leftNumberOK := strconv.Atoi(leftPart)
		rightNumber, rightNumberOK := strconv.Atoi(rightPart)
		switch {
		case leftNumberOK == nil && rightNumberOK == nil:
			switch {
			case leftNumber < rightNumber:
				return -1
			case leftNumber > rightNumber:
				return 1
			}
		case leftNumberOK == nil:
			return -1
		case rightNumberOK == nil:
			return 1
		default:
			switch {
			case leftPart < rightPart:
				return -1
			case leftPart > rightPart:
				return 1
			}
		}
	}
	return 0
}

func cliReleaseRepo() string {
	return firstNonEmptyTrimmed(os.Getenv("FUGUE_INSTALL_REPO"), defaultCLIReleaseRepo)
}

func cliReleaseAPIBaseURL() string {
	return firstNonEmptyTrimmed(os.Getenv("FUGUE_RELEASE_API_URL"), defaultCLIReleaseAPIBaseURL)
}

func cliReleaseAuthToken() string {
	return firstNonEmptyTrimmed(os.Getenv("FUGUE_RELEASE_GITHUB_TOKEN"), os.Getenv("GITHUB_TOKEN"))
}

func fetchCLIRelease(ctx context.Context, version string) (cliReleaseInfo, error) {
	repo := cliReleaseRepo()
	apiURL, err := cliReleaseEndpoint(repo, version)
	if err != nil {
		return cliReleaseInfo{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return cliReleaseInfo{}, fmt.Errorf("build release request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "fugue-cli/"+firstNonEmptyTrimmed(currentCLIBuildInfo().Version, "dev"))
	if token := cliReleaseAuthToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := (&http.Client{Timeout: cliUpgradeFetchTimeout}).Do(req)
	if err != nil {
		return cliReleaseInfo{}, fmt.Errorf("lookup latest release: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return cliReleaseInfo{}, fmt.Errorf("lookup latest release: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payload githubReleasePayload
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return cliReleaseInfo{}, fmt.Errorf("decode release response: %w", err)
	}

	release := cliReleaseInfo{
		TagName: canonicalizeReleaseTag(payload.TagName),
		HTMLURL: strings.TrimSpace(payload.HTMLURL),
		Assets:  make(map[string]string, len(payload.Assets)),
	}
	for _, asset := range payload.Assets {
		name := strings.TrimSpace(asset.Name)
		downloadURL := strings.TrimSpace(asset.BrowserDownloadURL)
		if name == "" || downloadURL == "" {
			continue
		}
		release.Assets[name] = downloadURL
	}
	if release.TagName == "" {
		return cliReleaseInfo{}, fmt.Errorf("release response did not contain a tag name")
	}
	return release, nil
}

func cliReleaseEndpoint(repo, version string) (string, error) {
	repo = strings.TrimSpace(repo)
	parts := strings.Split(repo, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", fmt.Errorf("release repo must use owner/name form")
	}

	baseURL := strings.TrimRight(cliReleaseAPIBaseURL(), "/")
	releasePath := fmt.Sprintf("/repos/%s/%s/releases/latest", url.PathEscape(parts[0]), url.PathEscape(parts[1]))
	normalizedVersion := canonicalizeReleaseTag(version)
	if normalizedVersion != "" && normalizedVersion != "latest" {
		releasePath = fmt.Sprintf("/repos/%s/%s/releases/tags/%s", url.PathEscape(parts[0]), url.PathEscape(parts[1]), url.PathEscape(normalizedVersion))
	}
	return baseURL + releasePath, nil
}

func downloadAndExtractCLIReleaseBinary(ctx context.Context, release cliReleaseInfo) (string, func(), error) {
	archiveName, err := cliArchiveAssetName()
	if err != nil {
		return "", func() {}, err
	}
	archiveURL := strings.TrimSpace(release.Assets[archiveName])
	if archiveURL == "" {
		return "", func() {}, fmt.Errorf("release %s is missing asset %s", release.TagName, archiveName)
	}
	checksumsURL := strings.TrimSpace(release.Assets[cliChecksumsAssetName])
	if checksumsURL == "" {
		return "", func() {}, fmt.Errorf("release %s is missing asset %s", release.TagName, cliChecksumsAssetName)
	}

	tempDir, err := os.MkdirTemp("", "fugue-upgrade-*")
	if err != nil {
		return "", func() {}, fmt.Errorf("create temp dir: %w", err)
	}
	cleanup := func() {
		_ = os.RemoveAll(tempDir)
	}

	archivePath := filepath.Join(tempDir, archiveName)
	if err := downloadCLIReleaseAsset(ctx, archiveURL, archivePath); err != nil {
		cleanup()
		return "", func() {}, err
	}

	checksumsPath := filepath.Join(tempDir, cliChecksumsAssetName)
	if err := downloadCLIReleaseAsset(ctx, checksumsURL, checksumsPath); err != nil {
		cleanup()
		return "", func() {}, err
	}

	expectedChecksum, err := readCLIAssetChecksum(checksumsPath, archiveName)
	if err != nil {
		cleanup()
		return "", func() {}, err
	}
	actualChecksum, err := sha256File(archivePath)
	if err != nil {
		cleanup()
		return "", func() {}, err
	}
	if actualChecksum != expectedChecksum {
		cleanup()
		return "", func() {}, fmt.Errorf("checksum mismatch for %s", archiveName)
	}

	binaryPath, err := extractCLIArchiveBinary(archivePath, tempDir)
	if err != nil {
		cleanup()
		return "", func() {}, err
	}
	return binaryPath, cleanup, nil
}

func cliArchiveAssetName() (string, error) {
	switch cliOS {
	case "linux", "darwin":
		switch cliArch {
		case "amd64", "arm64":
			return fmt.Sprintf("fugue_%s_%s.tar.gz", cliOS, cliArch), nil
		default:
			return "", fmt.Errorf("unsupported architecture for CLI upgrade: %s", cliArch)
		}
	case "windows":
		switch cliArch {
		case "amd64", "arm64":
			return fmt.Sprintf("fugue_%s_%s.zip", cliOS, cliArch), nil
		default:
			return "", fmt.Errorf("unsupported architecture for CLI upgrade: %s", cliArch)
		}
	default:
		return "", fmt.Errorf("unsupported operating system for CLI upgrade: %s", cliOS)
	}
}

func downloadCLIReleaseAsset(ctx context.Context, rawURL, destinationPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("build download request: %w", err)
	}
	req.Header.Set("User-Agent", "fugue-cli/"+firstNonEmptyTrimmed(currentCLIBuildInfo().Version, "dev"))

	resp, err := (&http.Client{Timeout: cliUpgradeFetchTimeout}).Do(req)
	if err != nil {
		return fmt.Errorf("download release asset: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("download release asset: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	file, err := os.Create(destinationPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", destinationPath, err)
	}
	defer file.Close()

	if _, err := io.Copy(file, resp.Body); err != nil {
		return fmt.Errorf("write %s: %w", destinationPath, err)
	}
	return nil
}

func readCLIAssetChecksum(checksumsPath, assetName string) (string, error) {
	data, err := os.ReadFile(checksumsPath)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", checksumsPath, err)
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 2 {
			continue
		}
		if fields[len(fields)-1] == assetName {
			return strings.ToLower(fields[0]), nil
		}
	}
	return "", fmt.Errorf("checksum entry for %s not found", assetName)
}

func sha256File(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", filePath, err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash %s: %w", filePath, err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func extractCLIArchiveBinary(archivePath, tempDir string) (string, error) {
	switch {
	case strings.HasSuffix(archivePath, ".zip"):
		return extractCLIArchiveBinaryFromZip(archivePath, tempDir)
	case strings.HasSuffix(archivePath, ".tar.gz"):
		return extractCLIArchiveBinaryFromTarGz(archivePath, tempDir)
	default:
		return "", fmt.Errorf("unsupported archive format: %s", archivePath)
	}
}

func extractCLIArchiveBinaryFromZip(archivePath, tempDir string) (string, error) {
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", archivePath, err)
	}
	defer reader.Close()

	for _, entry := range reader.File {
		if filepath.Base(entry.Name) != "fugue.exe" {
			continue
		}
		file, err := entry.Open()
		if err != nil {
			return "", fmt.Errorf("open archive entry %s: %w", entry.Name, err)
		}
		defer file.Close()

		destinationPath := filepath.Join(tempDir, "fugue.exe")
		if err := writeExecutableFile(destinationPath, file, 0o755); err != nil {
			return "", err
		}
		return destinationPath, nil
	}
	return "", fmt.Errorf("archive %s did not contain fugue.exe", archivePath)
}

func extractCLIArchiveBinaryFromTarGz(archivePath, tempDir string) (string, error) {
	file, err := os.Open(archivePath)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", archivePath, err)
	}
	defer file.Close()

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", archivePath, err)
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		switch {
		case err == io.EOF:
			return "", fmt.Errorf("archive %s did not contain fugue", archivePath)
		case err != nil:
			return "", fmt.Errorf("read %s: %w", archivePath, err)
		}
		if header == nil || filepath.Base(header.Name) != "fugue" {
			continue
		}
		destinationPath := filepath.Join(tempDir, "fugue")
		if err := writeExecutableFile(destinationPath, tarReader, os.FileMode(header.Mode)); err != nil {
			return "", err
		}
		return destinationPath, nil
	}
}

func writeExecutableFile(destinationPath string, source io.Reader, mode os.FileMode) error {
	file, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return fmt.Errorf("create %s: %w", destinationPath, err)
	}
	defer file.Close()

	if _, err := io.Copy(file, source); err != nil {
		return fmt.Errorf("write %s: %w", destinationPath, err)
	}
	if mode == 0 {
		mode = 0o755
	}
	if err := file.Chmod(mode.Perm()); err != nil {
		return fmt.Errorf("chmod %s: %w", destinationPath, err)
	}
	return nil
}

func resolveCLIExecutableDestination() (string, error) {
	executablePath, err := cliExecutablePath()
	if err != nil {
		return "", fmt.Errorf("resolve current executable: %w", err)
	}
	executablePath = strings.TrimSpace(executablePath)
	if executablePath == "" {
		return "", fmt.Errorf("resolve current executable: empty path")
	}
	if resolvedPath, err := filepath.EvalSymlinks(executablePath); err == nil && strings.TrimSpace(resolvedPath) != "" {
		executablePath = resolvedPath
	}
	absolutePath, err := filepath.Abs(executablePath)
	if err != nil {
		return "", fmt.Errorf("resolve absolute executable path: %w", err)
	}
	return absolutePath, nil
}

func installCLIReleaseBinary(sourcePath, destinationPath string) (bool, error) {
	switch cliOS {
	case "windows":
		return true, scheduleWindowsCLIReplacement(sourcePath, destinationPath)
	default:
		return false, replaceCLIExecutableAtomically(sourcePath, destinationPath)
	}
}

func replaceCLIExecutableAtomically(sourcePath, destinationPath string) error {
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open %s: %w", sourcePath, err)
	}
	defer sourceFile.Close()

	mode := os.FileMode(0o755)
	if info, err := os.Stat(destinationPath); err == nil {
		mode = info.Mode().Perm()
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat %s: %w", destinationPath, err)
	}

	destinationDir := filepath.Dir(destinationPath)
	tempFile, err := os.CreateTemp(destinationDir, ".fugue-upgrade-*")
	if err != nil {
		return fmt.Errorf("create temp file in %s: %w", destinationDir, err)
	}
	tempPath := tempFile.Name()
	defer func() {
		_ = os.Remove(tempPath)
	}()

	if _, err := io.Copy(tempFile, sourceFile); err != nil {
		tempFile.Close()
		return fmt.Errorf("write temporary binary: %w", err)
	}
	if err := tempFile.Chmod(mode); err != nil {
		tempFile.Close()
		return fmt.Errorf("chmod temporary binary: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close temporary binary: %w", err)
	}
	if err := os.Rename(tempPath, destinationPath); err != nil {
		return fmt.Errorf("replace %s: %w", destinationPath, err)
	}
	return nil
}

func scheduleWindowsCLIReplacement(sourcePath, destinationPath string) error {
	scriptPath := filepath.Join(os.TempDir(), fmt.Sprintf("fugue-upgrade-%d.ps1", cliNow().UnixNano()))
	script := strings.TrimSpace(`
param(
  [int]$ParentPid,
  [string]$SourcePath,
  [string]$DestinationPath
)

while (Get-Process -Id $ParentPid -ErrorAction SilentlyContinue) {
  Start-Sleep -Milliseconds 200
}

$destinationDir = Split-Path -Parent $DestinationPath
if ($destinationDir) {
  New-Item -ItemType Directory -Path $destinationDir -Force | Out-Null
}

$copied = $false
for ($attempt = 0; $attempt -lt 50 -and -not $copied; $attempt++) {
  try {
    Copy-Item -LiteralPath $SourcePath -Destination $DestinationPath -Force
    $copied = $true
  } catch {
    Start-Sleep -Milliseconds 200
  }
}

Remove-Item -LiteralPath $SourcePath -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $PSCommandPath -Force -ErrorAction SilentlyContinue
`)
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		return fmt.Errorf("write upgrade helper script: %w", err)
	}

	command := exec.Command(
		"powershell",
		"-NoProfile",
		"-ExecutionPolicy", "Bypass",
		"-File", scriptPath,
		"-ParentPid", strconv.Itoa(os.Getpid()),
		"-SourcePath", sourcePath,
		"-DestinationPath", destinationPath,
	)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		return fmt.Errorf("start Windows upgrade helper: %w", err)
	}
	return nil
}

func cacheValidForCurrentCLI(cache cliUpdateCheckCache, repo, currentVersion string, now time.Time) bool {
	if strings.TrimSpace(cache.Repo) != strings.TrimSpace(repo) {
		return false
	}
	if strings.TrimSpace(cache.CurrentVersion) != strings.TrimSpace(currentVersion) {
		return false
	}
	checkedAt, err := time.Parse(time.RFC3339, strings.TrimSpace(cache.CheckedAt))
	if err != nil {
		return false
	}
	return now.Sub(checkedAt) < cliUpdateCheckTTL
}

func noticeDue(cache cliUpdateCheckCache, now time.Time) bool {
	notifiedAt, err := time.Parse(time.RFC3339, strings.TrimSpace(cache.NotifiedAt))
	if err != nil {
		return true
	}
	return now.Sub(notifiedAt) >= cliUpdateNotifyTTL
}

func cliUpdateCheckCachePath() (string, error) {
	cacheDir, err := cliUserCacheDir()
	if err != nil {
		return "", err
	}
	cacheDir = strings.TrimSpace(cacheDir)
	if cacheDir == "" {
		return "", fmt.Errorf("user cache dir is empty")
	}
	return filepath.Join(cacheDir, "fugue", "cli-update-check.json"), nil
}

func loadCLIUpdateCheckCache() (cliUpdateCheckCache, error) {
	cachePath, err := cliUpdateCheckCachePath()
	if err != nil {
		return cliUpdateCheckCache{}, err
	}
	data, err := os.ReadFile(cachePath)
	if err != nil {
		return cliUpdateCheckCache{}, err
	}
	var cache cliUpdateCheckCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return cliUpdateCheckCache{}, err
	}
	return cache, nil
}

func saveCLIUpdateCheckCache(cache cliUpdateCheckCache) error {
	cachePath, err := cliUpdateCheckCachePath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(cachePath, data, 0o644)
}

func resetCLIUpdateNoticeCache(previousVersion, installedVersion, releaseURL string) {
	previousTag := currentCLIReleaseTag()
	if previousTag == "" {
		previousTag = canonicalizeReleaseTag(previousVersion)
	}
	nextTag := canonicalizeReleaseTag(installedVersion)
	if previousTag == "" || nextTag == "" || previousTag == nextTag {
		return
	}
	now := cliNow().UTC().Format(time.RFC3339)
	_ = saveCLIUpdateCheckCache(cliUpdateCheckCache{
		Repo:           cliReleaseRepo(),
		CurrentVersion: nextTag,
		LatestVersion:  nextTag,
		ReleaseURL:     releaseURL,
		CheckedAt:      now,
	})
}
