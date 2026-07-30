// fugue-backup-release-plan validates and prints one immutable,
// observation-only backup cell candidate. It has no cluster, registry,
// workflow, credential, or release adapter capability.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"fugue/internal/backuprelease"
)

const (
	maxCandidateRequestBytes  = 4 << 20
	maxCandidateManifestBytes = 4 << 20
	maxCandidateChartBytes    = 4 << 20
	maxCandidateChartFiles    = 256
	maxCandidateChartDirs     = 64
	maxCandidateChartPath     = 512
	maxCandidateJSONDepth     = 64
	maxCandidateJSONTokens    = 100000
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "backup release candidate: %v\n", err)
		os.Exit(2)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if stdout == nil || stderr == nil {
		return errors.New("output writers are required")
	}
	flags := flag.NewFlagSet("fugue-backup-release-plan", flag.ContinueOnError)
	flags.SetOutput(stderr)
	requestPath := flags.String("request", "", "absolute path to the v1 candidate request JSON")
	manifestPath := flags.String("manifest", "", "absolute path to the exact Helm-rendered manifest")
	chartPath := flags.String("chart", "", "absolute path to the exact backup observer chart directory")
	digestChartOnly := flags.Bool("digest-chart", false, "print the deterministic chart digest and perform no candidate planning")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *digestChartOnly {
		if flags.NArg() != 0 || *requestPath != "" || *manifestPath != "" || !canonicalAbsolutePath(*chartPath) {
			return errors.New("--digest-chart requires only one canonical absolute --chart path")
		}
		digest, err := digestChartDirectory(*chartPath)
		if err != nil {
			return fmt.Errorf("digest chart: %w", err)
		}
		_, err = fmt.Fprintln(stdout, digest)
		return err
	}
	if flags.NArg() != 0 || !canonicalAbsolutePath(*requestPath) || !canonicalAbsolutePath(*manifestPath) ||
		!canonicalAbsolutePath(*chartPath) || *requestPath == *manifestPath ||
		pathWithinRoot(*requestPath, *chartPath) || pathWithinRoot(*manifestPath, *chartPath) {
		return errors.New("--request, --manifest, and --chart must be distinct canonical absolute paths outside the chart")
	}
	requestInfo, err := regularFileInfo(*requestPath, maxCandidateRequestBytes)
	if err != nil {
		return fmt.Errorf("inspect request: %w", err)
	}
	manifestInfo, err := regularFileInfo(*manifestPath, maxCandidateManifestBytes)
	if err != nil {
		return fmt.Errorf("inspect manifest: %w", err)
	}
	if os.SameFile(requestInfo, manifestInfo) {
		return errors.New("request and manifest must not alias the same file")
	}
	requestBytes, err := readStableRegularFile(*requestPath, requestInfo, maxCandidateRequestBytes)
	if err != nil {
		return fmt.Errorf("read request: %w", err)
	}
	manifest, err := readStableRegularFile(*manifestPath, manifestInfo, maxCandidateManifestBytes)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	if err := rejectDuplicateJSONKeys(requestBytes); err != nil {
		return fmt.Errorf("validate request JSON: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(requestBytes))
	decoder.DisallowUnknownFields()
	var request backuprelease.CandidateRequest
	if err := decoder.Decode(&request); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("decode trailing request data: %w", err)
		}
		return errors.New("request must contain exactly one JSON object")
	}
	chartDigest, err := digestChartDirectory(*chartPath)
	if err != nil {
		return fmt.Errorf("digest chart: %w", err)
	}
	if request.ChartDigest != chartDigest {
		return errors.New("request chartDigest does not match the exact chart directory")
	}
	candidate, err := backuprelease.BuildCandidate(request, manifest)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(candidate)
}

func digestChartDirectory(root string) (string, error) {
	if !canonicalAbsolutePath(root) {
		return "", errors.New("chart root must be a canonical absolute path")
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return "", err
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("chart root must be a non-symlink directory")
	}
	type chartFile struct {
		path string
		data []byte
	}
	type chartDirectory struct {
		path string
		info os.FileInfo
	}
	files := make([]chartFile, 0, 8)
	directories := []chartDirectory{{path: root, info: rootInfo}}
	var total int64
	err = filepath.WalkDir(root, func(currentPath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if currentPath == root {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("chart contains symlink %s", currentPath)
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return infoErr
		}
		relative, relativeErr := filepath.Rel(root, currentPath)
		if relativeErr != nil || relative == "." || filepath.IsAbs(relative) || relative == ".." ||
			strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return errors.New("chart entry path escaped the chart root")
		}
		relative = filepath.ToSlash(relative)
		if len(relative) > maxCandidateChartPath || strings.Contains(relative, "\\") {
			return errors.New("chart entry path is not a bounded canonical POSIX path")
		}
		if entry.IsDir() {
			if !info.IsDir() {
				return errors.New("chart directory entry changed type")
			}
			if len(directories) >= maxCandidateChartDirs {
				return errors.New("chart contains too many directories")
			}
			directories = append(directories, chartDirectory{path: currentPath, info: info})
			return nil
		}
		if !info.Mode().IsRegular() || len(files) >= maxCandidateChartFiles || info.Size() < 0 ||
			total+info.Size() > maxCandidateChartBytes {
			return errors.New("chart must contain only a bounded regular-file closure")
		}
		data, readErr := readStableRegularFile(currentPath, info, maxCandidateChartBytes-total)
		if readErr != nil {
			return readErr
		}
		total += int64(len(data))
		files = append(files, chartFile{path: relative, data: data})
		return nil
	})
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", errors.New("chart directory is empty")
	}
	for _, directory := range directories {
		after, statErr := os.Lstat(directory.path)
		if statErr != nil || !after.IsDir() || after.Mode()&os.ModeSymlink != 0 || !os.SameFile(directory.info, after) {
			return "", errors.New("chart directory identity changed while it was read")
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].path < files[j].path })
	hash := sha256.New()
	_, _ = io.WriteString(hash, "fugue-backup-observer-chart-v1\x00")
	for _, file := range files {
		_, _ = fmt.Fprintf(hash, "%d:%s\x00%d:", len(file.path), file.path, len(file.data))
		_, _ = hash.Write(file.data)
		_, _ = hash.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func regularFileInfo(path string, maximum int64) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maximum {
		return nil, errors.New("input must be a non-empty bounded regular file")
	}
	return info, nil
}

func readStableRegularFile(path string, expected os.FileInfo, maximum int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if expected == nil || maximum < 0 || !opened.Mode().IsRegular() || !os.SameFile(expected, opened) ||
		expected.Size() != opened.Size() || opened.Size() < 0 || opened.Size() > maximum {
		return nil, errors.New("regular file identity changed before it was read")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maximum {
		return nil, errors.New("input exceeds the size limit")
	}
	after, err := file.Stat()
	pathAfter, pathErr := os.Lstat(path)
	if err != nil || pathErr != nil || !pathAfter.Mode().IsRegular() || !os.SameFile(opened, after) ||
		!os.SameFile(opened, pathAfter) || after.Size() != opened.Size() || int64(len(data)) != opened.Size() {
		return nil, errors.New("regular file changed while it was read")
	}
	return data, nil
}

func canonicalAbsolutePath(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && filepath.IsAbs(value) && filepath.Clean(value) == value
}

func rejectDuplicateJSONKeys(document []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	tokens := 0
	if err := validateUniqueJSONValue(decoder, 0, &tokens); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errors.New("request JSON contains trailing data")
	}
	return nil
}

func validateUniqueJSONValue(decoder *json.Decoder, depth int, tokens *int) error {
	if decoder == nil || tokens == nil || depth > maxCandidateJSONDepth || *tokens >= maxCandidateJSONTokens {
		return errors.New("request JSON exceeds structural limits")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	(*tokens)++
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, keyErr := decoder.Token()
			if keyErr != nil {
				return keyErr
			}
			(*tokens)++
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("request JSON object key is not a string")
			}
			if _, exists := keys[key]; exists {
				return fmt.Errorf("request JSON repeats key %q", key)
			}
			keys[key] = struct{}{}
			if err := validateUniqueJSONValue(decoder, depth+1, tokens); err != nil {
				return err
			}
		}
		closing, closeErr := decoder.Token()
		if closeErr != nil || closing != json.Delim('}') {
			return errors.New("request JSON object is not closed")
		}
		(*tokens)++
	case '[':
		for decoder.More() {
			if err := validateUniqueJSONValue(decoder, depth+1, tokens); err != nil {
				return err
			}
		}
		closing, closeErr := decoder.Token()
		if closeErr != nil || closing != json.Delim(']') {
			return errors.New("request JSON array is not closed")
		}
		(*tokens)++
	default:
		return errors.New("request JSON contains an unexpected delimiter")
	}
	if *tokens > maxCandidateJSONTokens {
		return errors.New("request JSON exceeds structural limits")
	}
	return nil
}

func pathWithinRoot(candidate, root string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !filepath.IsAbs(relative) &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
