package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// inspectBlobUploads reports incomplete registry upload files without changing
// them. Uploads live outside the content-addressed blob tree, so the normal
// blob inventory cannot account for them.
func (c *imageCache) inspectBlobUploads(now time.Time) imageCacheUploadCleanup {
	cleanup, err := c.collectStaleBlobUploads(now)
	if err != nil {
		return imageCacheUploadCleanup{Skipped: []imageCacheUploadEntry{{Path: c.blobUploadDir(), Reason: "upload_scan_failed"}}}
	}
	return cleanup
}

func (c *imageCache) cleanupStaleBlobUploads(now time.Time) (imageCacheUploadCleanup, error) {
	cleanup, err := c.collectStaleBlobUploads(now)
	if err != nil {
		return imageCacheUploadCleanup{}, err
	}
	if len(cleanup.Candidates) == 0 {
		return cleanup, nil
	}
	groups := map[string][]imageCacheUploadEntry{}
	for _, entry := range cleanup.Candidates {
		groups[imageCacheUploadGroup(entry.Path)] = append(groups[imageCacheUploadGroup(entry.Path)], entry)
	}
	for group, entries := range groups {
		open, err := c.uploadGroupHasOpenFile(group, entries)
		if err != nil {
			return cleanup, err
		}
		if open {
			cleanup.Candidates = removeUploadEntries(cleanup.Candidates, entries)
			for _, entry := range entries {
				entry.Reason = "active_upload"
				cleanup.Skipped = append(cleanup.Skipped, entry)
			}
			continue
		}
		for _, entry := range entries {
			if err := os.Remove(entry.Path); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return cleanup, fmt.Errorf("delete stale image-cache upload %s: %w", entry.Path, err)
			}
			cleanup.Deleted = append(cleanup.Deleted, entry)
			cleanup.DeletedBytes += entry.SizeBytes
		}
	}
	cleanup.CandidateBytes = 0
	for _, entry := range cleanup.Candidates {
		cleanup.CandidateBytes += entry.SizeBytes
	}
	return cleanup, nil
}

func (c *imageCache) collectStaleBlobUploads(now time.Time) (imageCacheUploadCleanup, error) {
	var cleanup imageCacheUploadCleanup
	if c == nil || strings.TrimSpace(c.storeDir) == "" {
		return cleanup, nil
	}
	ttl := c.uploadTTL
	if ttl <= 0 {
		ttl = defaultImageCacheUploadTTL
	}
	entries, err := os.ReadDir(c.storeDir)
	if errors.Is(err, os.ErrNotExist) {
		return cleanup, nil
	}
	if err != nil {
		return cleanup, err
	}
	paths := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "upload-") {
			continue
		}
		paths = append(paths, filepath.Join(c.storeDir, entry.Name()))
	}
	if uploadEntries, readErr := os.ReadDir(c.blobUploadDir()); readErr == nil {
		for _, entry := range uploadEntries {
			if entry.IsDir() || strings.HasSuffix(entry.Name(), ".tmp") {
				continue
			}
			paths = append(paths, filepath.Join(c.blobUploadDir(), entry.Name()))
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return cleanup, readErr
	}
	sort.Strings(paths)
	for _, path := range paths {
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return cleanup, err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		activity := info.ModTime()
		if strings.HasPrefix(filepath.Base(path), "upload-") && filepath.Dir(path) == c.blobUploadDir() {
			if state, stateErr := c.readBlobUploadState(strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))); stateErr == nil {
				activity = latestUploadTime(activity, state.CreatedAt, state.UpdatedAt)
			}
		}
		if activity.IsZero() || now.Sub(activity) < ttl {
			continue
		}
		entry := imageCacheUploadEntry{
			Path:       path,
			SizeBytes:  info.Size(),
			ModifiedAt: activity.UTC().Format(time.RFC3339),
			Reason:     "expired_upload",
		}
		cleanup.Candidates = append(cleanup.Candidates, entry)
		cleanup.CandidateBytes += entry.SizeBytes
	}
	return cleanup, nil
}

func latestUploadTime(values ...time.Time) time.Time {
	var latest time.Time
	for _, value := range values {
		if value.After(latest) {
			latest = value
		}
	}
	return latest
}

func imageCacheUploadGroup(path string) string {
	base := filepath.Base(path)
	if filepath.Base(filepath.Dir(path)) == imageCacheUploadDirName {
		return filepath.Join(filepath.Dir(path), strings.TrimSuffix(strings.TrimSuffix(base, ".data"), ".json"))
	}
	return path
}

func removeUploadEntries(all []imageCacheUploadEntry, remove []imageCacheUploadEntry) []imageCacheUploadEntry {
	set := map[string]struct{}{}
	for _, entry := range remove {
		set[entry.Path] = struct{}{}
	}
	out := all[:0]
	for _, entry := range all {
		if _, ok := set[entry.Path]; !ok {
			out = append(out, entry)
		}
	}
	return out
}

func (c *imageCache) uploadGroupHasOpenFile(group string, entries []imageCacheUploadEntry) (bool, error) {
	for _, entry := range entries {
		open, err := c.uploadPathHasOpenFile(entry.Path)
		if err != nil {
			return false, err
		}
		if open {
			return true, nil
		}
	}
	return false, nil
}

func (c *imageCache) uploadPathHasOpenFile(path string) (bool, error) {
	if runtime.GOOS != "linux" {
		// The deployed image-cache runs on Linux. On another platform, fail
		// closed rather than deleting a file without an open-file check.
		return true, nil
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return true, err
	}
	cleanPath, err := filepath.Abs(path)
	if err != nil {
		return true, err
	}
	for _, process := range entries {
		if !process.IsDir() || strings.Trim(process.Name(), "0123456789") != "" {
			continue
		}
		fds, err := os.ReadDir(filepath.Join("/proc", process.Name(), "fd"))
		if err != nil {
			continue
		}
		for _, fd := range fds {
			target, err := os.Readlink(filepath.Join("/proc", process.Name(), "fd", fd.Name()))
			if err == nil && target == cleanPath {
				return true, nil
			}
		}
	}
	return false, nil
}
