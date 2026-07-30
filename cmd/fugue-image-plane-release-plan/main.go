// fugue-image-plane-release-plan validates and prints one immutable,
// observation-only cell candidate. It has no cluster, registry, workflow, or
// release adapter capability.
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

	"fugue/internal/imageplanerelease"
)

const (
	maxCandidateRequestBytes  = 4 << 20
	maxCandidateManifestBytes = 8 << 20
	maxCandidateChartBytes    = 4 << 20
	maxCandidateChartFiles    = 256
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "image-plane release candidate: %v\n", err)
		os.Exit(2)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("fugue-image-plane-release-plan", flag.ContinueOnError)
	flags.SetOutput(stderr)
	requestPath := flags.String("request", "", "absolute path to the v1 candidate request JSON")
	manifestPath := flags.String("manifest", "", "absolute path to the exact Helm-rendered manifest")
	chartPath := flags.String("chart", "", "absolute path to the exact image-plane chart directory")
	digestChartOnly := flags.Bool("digest-chart", false, "print the deterministic chart digest and perform no candidate planning")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *digestChartOnly {
		if flags.NArg() != 0 || *requestPath != "" || *manifestPath != "" || !filepath.IsAbs(*chartPath) || filepath.Clean(*chartPath) != *chartPath {
			return errors.New("--digest-chart requires only one canonical absolute --chart path")
		}
		digest, err := digestChartDirectory(*chartPath)
		if err != nil {
			return fmt.Errorf("digest chart: %w", err)
		}
		_, err = fmt.Fprintln(stdout, digest)
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*requestPath) || !filepath.IsAbs(*manifestPath) || !filepath.IsAbs(*chartPath) ||
		filepath.Clean(*requestPath) != *requestPath || filepath.Clean(*manifestPath) != *manifestPath || filepath.Clean(*chartPath) != *chartPath ||
		*requestPath == *manifestPath || *requestPath == *chartPath || *manifestPath == *chartPath {
		return errors.New("--request, --manifest, and --chart must be distinct canonical absolute paths")
	}
	requestBytes, err := readBoundedRegularFile(*requestPath, maxCandidateRequestBytes)
	if err != nil {
		return fmt.Errorf("read request: %w", err)
	}
	manifest, err := readBoundedRegularFile(*manifestPath, maxCandidateManifestBytes)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(requestBytes))
	decoder.DisallowUnknownFields()
	var request imageplanerelease.CandidateRequest
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
	candidate, err := imageplanerelease.BuildCandidate(request, manifest)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(candidate)
}

func digestChartDirectory(root string) (string, error) {
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
	files := make([]chartFile, 0, 8)
	var total int64
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("chart contains symlink %s", path)
		}
		if entry.IsDir() {
			return nil
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return infoErr
		}
		if !info.Mode().IsRegular() || len(files) >= maxCandidateChartFiles ||
			info.Size() < 0 || total+info.Size() > maxCandidateChartBytes {
			return errors.New("chart must contain only a bounded regular-file closure")
		}
		relative, relativeErr := filepath.Rel(root, path)
		if relativeErr != nil || relative == "." || filepath.IsAbs(relative) || strings.HasPrefix(relative, "..") {
			return errors.New("chart file path escaped the chart root")
		}
		data, readErr := readStableRegularFile(path, info, maxCandidateChartBytes-total)
		if readErr != nil {
			return readErr
		}
		total += int64(len(data))
		files = append(files, chartFile{path: filepath.ToSlash(relative), data: data})
		return nil
	})
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", errors.New("chart directory is empty")
	}
	rootAfter, err := os.Lstat(root)
	if err != nil || !rootAfter.IsDir() || !os.SameFile(rootInfo, rootAfter) {
		return "", errors.New("chart root identity changed while it was read")
	}
	sort.Slice(files, func(i, j int) bool { return files[i].path < files[j].path })
	hash := sha256.New()
	_, _ = io.WriteString(hash, "fugue-image-plane-chart-v1\x00")
	for _, file := range files {
		_, _ = fmt.Fprintf(hash, "%d:%s\x00%d:", len(file.path), file.path, len(file.data))
		_, _ = hash.Write(file.data)
		_, _ = hash.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func readBoundedRegularFile(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maximum {
		return nil, errors.New("input must be a non-empty bounded regular file")
	}
	return readStableRegularFile(path, info, maximum)
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
	if expected == nil || !opened.Mode().IsRegular() || !os.SameFile(expected, opened) ||
		expected.Size() != opened.Size() || opened.Size() < 0 || opened.Size() > maximum {
		return nil, errors.New("regular file identity changed before it was read")
	}
	limited := io.LimitReader(file, maximum+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maximum {
		return nil, errors.New("input exceeds the size limit")
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(opened, after) || after.Size() != opened.Size() || int64(len(data)) != opened.Size() {
		return nil, errors.New("regular file changed while it was read")
	}
	return data, nil
}
