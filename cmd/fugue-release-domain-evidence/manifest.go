package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"fugue/internal/releasedomain"
)

const (
	canonicalManifestInputLimit = 8 << 20
	canonicalOwnershipLimit     = 4 << 20

	canonicalArgumentsError = "fugue-release-domain-evidence canonicalize-manifest: invalid arguments"
	canonicalOwnershipError = "fugue-release-domain-evidence canonicalize-manifest: release-domain ownership input is invalid"
	canonicalInputError     = "fugue-release-domain-evidence canonicalize-manifest: private render input is invalid"
	canonicalHelmError      = "fugue-release-domain-evidence canonicalize-manifest: private Helm render input is invalid"
	canonicalManifestError  = "fugue-release-domain-evidence canonicalize-manifest: private rendered manifest is invalid"
	canonicalOutputError    = "fugue-release-domain-evidence canonicalize-manifest: private canonical output failed"
)

type canonicalManifestOptions struct {
	ownershipPath  string
	inputPath      string
	inputFormat    string
	excludeHooks   bool
	namespace      string
	releaseName    string
	releaseVersion uint64
	outputPath     string
}

func runCanonicalizeManifest(args []string, _ io.Writer, stderr io.Writer) int {
	options, err := parseCanonicalManifestFlags(args, stderr)
	if err != nil {
		fmt.Fprintln(stderr, canonicalArgumentsError)
		return 1
	}
	ownership, ownershipResolvedPath, err := readBoundedRegularFile(options.ownershipPath, canonicalOwnershipLimit, false)
	if err != nil {
		fmt.Fprintln(stderr, canonicalOwnershipError)
		return 1
	}
	spec, err := releasedomain.LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		fmt.Fprintln(stderr, canonicalOwnershipError)
		return 1
	}
	input, inputResolvedPath, err := readBoundedRegularFile(options.inputPath, canonicalManifestInputLimit, true)
	if err != nil {
		fmt.Fprintln(stderr, canonicalInputError)
		return 1
	}
	manifest := input
	if options.inputFormat == "helm-release-json" {
		manifest, err = extractCanonicalHelmReleaseManifest(
			input,
			options.releaseName,
			options.namespace,
			options.releaseVersion,
			options.excludeHooks,
		)
		if err != nil {
			fmt.Fprintln(stderr, canonicalHelmError)
			return 1
		}
	}
	canonical, err := releasedomain.CanonicalizeRenderedManifest(manifest, spec, options.namespace)
	if err != nil {
		fmt.Fprintln(stderr, canonicalManifestError)
		return 1
	}
	if err := writePrivateAtomicFile(options.outputPath, canonical, inputResolvedPath, ownershipResolvedPath); err != nil {
		fmt.Fprintln(stderr, canonicalOutputError)
		return 1
	}
	return 0
}

func parseCanonicalManifestFlags(args []string, _ io.Writer) (canonicalManifestOptions, error) {
	options := canonicalManifestOptions{
		ownershipPath: "deploy/release-domains/ownership-v1.yaml",
		inputFormat:   "manifest",
	}
	flags := flag.NewFlagSet("canonicalize-manifest", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.ownershipPath, "ownership", options.ownershipPath, "release-domain ownership YAML")
	flags.StringVar(&options.inputPath, "input", "", "private raw render input")
	flags.StringVar(&options.inputFormat, "input-format", options.inputFormat, "manifest or helm-release-json")
	flags.BoolVar(&options.excludeHooks, "exclude-hooks", false, "exclude Helm release hooks from helm-release-json input")
	flags.StringVar(&options.namespace, "namespace", "", "effective release namespace")
	flags.StringVar(&options.releaseName, "release-name", "", "expected Helm release name for helm-release-json")
	flags.Uint64Var(&options.releaseVersion, "release-version", 0, "expected Helm release version for helm-release-json")
	flags.StringVar(&options.outputPath, "output", "", "private canonical manifest output")
	excludeHooksCount := 0
	for _, argument := range args {
		switch {
		case argument == "--exclude-hooks":
			excludeHooksCount++
		case argument == "-exclude-hooks", strings.HasPrefix(argument, "--exclude-hooks="), strings.HasPrefix(argument, "-exclude-hooks="):
			return canonicalManifestOptions{}, fmt.Errorf("--exclude-hooks must be a unique bare flag")
		}
	}
	if excludeHooksCount > 1 {
		return canonicalManifestOptions{}, fmt.Errorf("--exclude-hooks must be a unique bare flag")
	}
	if err := flags.Parse(args); err != nil {
		return canonicalManifestOptions{}, err
	}
	if flags.NArg() != 0 {
		return canonicalManifestOptions{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	for name, value := range map[string]string{
		"--ownership": options.ownershipPath,
		"--input":     options.inputPath,
		"--namespace": options.namespace,
		"--output":    options.outputPath,
	} {
		if strings.TrimSpace(value) == "" {
			return canonicalManifestOptions{}, fmt.Errorf("%s is required", name)
		}
		if !utf8.ValidString(value) {
			return canonicalManifestOptions{}, fmt.Errorf("%s must be valid UTF-8", name)
		}
	}
	if options.outputPath == "-" {
		return canonicalManifestOptions{}, fmt.Errorf("--output must be a private file path, not stdout")
	}
	inputAbsolute, err := filepath.Abs(options.inputPath)
	if err != nil {
		return canonicalManifestOptions{}, fmt.Errorf("resolve --input: %w", err)
	}
	outputAbsolute, err := filepath.Abs(options.outputPath)
	if err != nil {
		return canonicalManifestOptions{}, fmt.Errorf("resolve --output: %w", err)
	}
	if inputAbsolute == outputAbsolute {
		return canonicalManifestOptions{}, fmt.Errorf("--input and --output must differ")
	}
	switch options.inputFormat {
	case "manifest":
		if options.releaseName != "" || options.releaseVersion != 0 || options.excludeHooks {
			return canonicalManifestOptions{}, fmt.Errorf("--release-name, --release-version, and --exclude-hooks are valid only for helm-release-json")
		}
	case "helm-release-json":
		if strings.TrimSpace(options.releaseName) == "" || !utf8.ValidString(options.releaseName) {
			return canonicalManifestOptions{}, fmt.Errorf("--release-name is required and must be valid UTF-8 for helm-release-json")
		}
		if options.releaseVersion == 0 {
			return canonicalManifestOptions{}, fmt.Errorf("--release-version is required for helm-release-json")
		}
	default:
		return canonicalManifestOptions{}, fmt.Errorf("--input-format must be manifest or helm-release-json")
	}
	return options, nil
}

func extractCanonicalHelmReleaseManifest(
	data []byte,
	expectedName string,
	expectedNamespace string,
	expectedVersion uint64,
	excludeHooks bool,
) ([]byte, error) {
	extracted, err := releasedomain.ExtractHelmReleaseManifest(
		data,
		expectedName,
		expectedNamespace,
		expectedVersion,
	)
	if err != nil || !excludeHooks {
		return extracted, err
	}

	// ExtractHelmReleaseManifest above strictly validates the complete Helm
	// release envelope, including every hook. Decode only the already-validated
	// main manifest here so --exclude-hooks is an explicit evidence policy and
	// never depends on Helm omitting release.hooks from dry-run JSON.
	var envelope struct {
		Manifest string `json:"manifest"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	return []byte(envelope.Manifest), nil
}

func readBoundedRegularFile(filename string, limit int64, requirePrivate bool) ([]byte, string, error) {
	if limit <= 0 {
		return nil, "", fmt.Errorf("invalid read limit %d", limit)
	}
	linkInfo, err := os.Lstat(filename)
	if err != nil {
		return nil, "", err
	}
	if linkInfo.Mode()&os.ModeSymlink != 0 || !linkInfo.Mode().IsRegular() {
		return nil, "", fmt.Errorf("path must be a regular non-symlink file")
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, "", err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > limit {
		return nil, "", fmt.Errorf("file size %d exceeds limit %d or is not regular", info.Size(), limit)
	}
	if !os.SameFile(linkInfo, info) {
		return nil, "", fmt.Errorf("file identity changed while opening")
	}
	if requirePrivate && info.Mode().Perm()&0o077 != 0 {
		return nil, "", fmt.Errorf("private render input mode %o grants group or other access", info.Mode().Perm())
	}
	absolute, err := filepath.Abs(filename)
	if err != nil {
		return nil, "", fmt.Errorf("resolve file path: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, "", fmt.Errorf("resolve file path: %w", err)
	}
	resolvedInfo, err := os.Stat(resolved)
	if err != nil || !os.SameFile(info, resolvedInfo) {
		return nil, "", fmt.Errorf("file parent path changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, "", err
	}
	if int64(len(data)) > limit {
		return nil, "", fmt.Errorf("file grew beyond limit %d while reading", limit)
	}
	resolvedInfo, err = os.Stat(resolved)
	if err != nil || !os.SameFile(info, resolvedInfo) {
		return nil, "", fmt.Errorf("file parent path changed while reading")
	}
	return data, resolved, nil
}
