package registrymaintenance

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type ScanResult struct {
	StorageUsedBytes       int64
	StorageCapacityBytes   int64
	BlobCount              int64
	BlobBytes              int64
	ReferencedBlobCount    int64
	ReferencedBlobBytes    int64
	UnreferencedBlobCount  int64
	UnreferencedBlobBytes  int64
	KeepDigestCount        int64
	MissingKeepDigestCount int64
	ManifestRevisionCount  int64
}

type manifestDescriptor struct {
	Digest string `json:"digest"`
}

type manifestEnvelope struct {
	Config    manifestDescriptor   `json:"config"`
	Layers    []manifestDescriptor `json:"layers"`
	Manifests []manifestDescriptor `json:"manifests"`
	Blobs     []manifestDescriptor `json:"blobs"`
	Subject   manifestDescriptor   `json:"subject"`
}

func Scan(root string, keepDigests []string) (ScanResult, error) {
	root = filepath.Clean(strings.TrimSpace(root))
	if root == "." || root == "" {
		return ScanResult{}, fmt.Errorf("registry root is required")
	}

	result := ScanResult{}
	if err := populateFilesystemUsage(root, &result); err != nil {
		return ScanResult{}, err
	}

	blobPaths := make(map[string]string)
	blobSizes := make(map[string]int64)
	blobsRoot := filepath.Join(root, "blobs")
	if err := filepath.WalkDir(blobsRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "data" {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		digest := digestFromBlobDataPath(blobsRoot, path)
		if digest == "" {
			return nil
		}
		blobPaths[digest] = path
		blobSizes[digest] = info.Size()
		result.BlobCount++
		result.BlobBytes += info.Size()
		return nil
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return ScanResult{}, fmt.Errorf("scan registry blobs: %w", err)
	}

	roots := make(map[string]struct{})
	repositoriesRoot := filepath.Join(root, "repositories")
	if err := filepath.WalkDir(repositoriesRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "link" || !strings.Contains(filepath.ToSlash(path), "/_manifests/revisions/") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		digest := normalizeDigest(string(data))
		if digest == "" {
			return nil
		}
		roots[digest] = struct{}{}
		result.ManifestRevisionCount++
		return nil
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return ScanResult{}, fmt.Errorf("scan registry manifest revisions: %w", err)
	}

	seenKeep := make(map[string]struct{})
	for _, digest := range keepDigests {
		digest = normalizeDigest(digest)
		if digest == "" {
			continue
		}
		if _, exists := seenKeep[digest]; exists {
			continue
		}
		seenKeep[digest] = struct{}{}
		result.KeepDigestCount++
		if _, exists := blobPaths[digest]; !exists {
			result.MissingKeepDigestCount++
			continue
		}
		roots[digest] = struct{}{}
	}

	referenced := make(map[string]struct{})
	queue := make([]string, 0, len(roots))
	for digest := range roots {
		queue = append(queue, digest)
	}
	for len(queue) > 0 {
		digest := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if _, exists := referenced[digest]; exists {
			continue
		}
		path, exists := blobPaths[digest]
		if !exists {
			continue
		}
		referenced[digest] = struct{}{}

		children, err := manifestChildDigests(path)
		if err != nil {
			continue
		}
		for _, child := range children {
			if _, exists := referenced[child]; !exists {
				queue = append(queue, child)
			}
		}
	}

	for digest, size := range blobSizes {
		if _, exists := referenced[digest]; exists {
			result.ReferencedBlobCount++
			result.ReferencedBlobBytes += size
			continue
		}
		result.UnreferencedBlobCount++
		result.UnreferencedBlobBytes += size
	}
	return result, nil
}

func ReadKeepDigests(path string) ([]string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var digests []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if digest := normalizeDigest(scanner.Text()); digest != "" {
			digests = append(digests, digest)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return digests, nil
}

func populateFilesystemUsage(root string, result *ScanResult) error {
	var stats syscall.Statfs_t
	if err := syscall.Statfs(root, &stats); err != nil {
		return fmt.Errorf("stat registry filesystem: %w", err)
	}
	blockSize := int64(stats.Bsize)
	result.StorageCapacityBytes = int64(stats.Blocks) * blockSize
	result.StorageUsedBytes = int64(stats.Blocks-stats.Bfree) * blockSize
	return nil
}

func digestFromBlobDataPath(blobsRoot, path string) string {
	relative, err := filepath.Rel(blobsRoot, path)
	if err != nil {
		return ""
	}
	parts := strings.Split(filepath.ToSlash(relative), "/")
	if len(parts) < 4 || parts[len(parts)-1] != "data" {
		return ""
	}
	algorithm := strings.TrimSpace(parts[0])
	hexDigest := strings.TrimSpace(parts[len(parts)-2])
	if algorithm == "" || hexDigest == "" {
		return ""
	}
	return normalizeDigest(algorithm + ":" + hexDigest)
}

func manifestChildDigests(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var manifest manifestEnvelope
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	candidates := make([]manifestDescriptor, 0, 2+len(manifest.Layers)+len(manifest.Manifests)+len(manifest.Blobs))
	candidates = append(candidates, manifest.Config, manifest.Subject)
	candidates = append(candidates, manifest.Layers...)
	candidates = append(candidates, manifest.Manifests...)
	candidates = append(candidates, manifest.Blobs...)

	children := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if digest := normalizeDigest(candidate.Digest); digest != "" {
			children = append(children, digest)
		}
	}
	return children, nil
}

func normalizeDigest(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	parts := strings.SplitN(value, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return ""
	}
	for _, r := range parts[1] {
		if r < '0' || r > '9' && r < 'a' || r > 'f' {
			return ""
		}
	}
	return parts[0] + ":" + parts[1]
}
