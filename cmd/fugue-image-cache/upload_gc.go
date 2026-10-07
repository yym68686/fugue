package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

type imageCacheUploadEntry struct {
	Path       string `json:"path"`
	SizeBytes  int64  `json:"size_bytes"`
	ModifiedAt string `json:"modified_at,omitempty"`
	Reason     string `json:"reason,omitempty"`
	info       os.FileInfo
}

type imageCacheUploadCleanup struct {
	Candidates      []imageCacheUploadEntry `json:"candidates,omitempty"`
	Skipped         []imageCacheUploadEntry `json:"skipped,omitempty"`
	Deleted         []imageCacheUploadEntry `json:"deleted,omitempty"`
	CandidateBytes  int64                   `json:"candidate_bytes"`
	DeletedBytes    int64                   `json:"deleted_bytes"`
	TotalBytes      int64                   `json:"total_bytes"`
	StateMismatches int                     `json:"state_mismatches"`
	Error           string                  `json:"error,omitempty"`
}

// The upload expiry policy is independent of manifest retirement. Only known
// temporary layouts are scanned; expiry never grants authority over a blob or
// serving artifact. All HTTP upload mutations hold uploadBarrier for reading.
func (c *imageCache) runUploadMaintenance(ctx context.Context) {
	interval := c.uploadGCInterval
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	run := func() {
		var result imageCacheUploadCleanup
		if c.uploadGCMode == "delete" {
			var err error
			result, err = c.cleanupStaleBlobUploads(time.Now().UTC())
			if err != nil {
				c.metrics.uploadGCErrors.Add(1)
				log.Printf("upload maintenance failed: %v", err)
				return
			}
		} else {
			result = c.inspectBlobUploads(time.Now().UTC())
		}
		c.metrics.uploadTempBytes.Store(uint64(result.TotalBytes))
		c.metrics.uploadExpiredBytes.Store(uint64(result.CandidateBytes))
		c.metrics.uploadStateMismatches.Store(uint64(result.StateMismatches))
		c.metrics.uploadDeletedBytes.Add(uint64(result.DeletedBytes))
		if result.DeletedBytes > 0 {
			raw, _ := json.Marshal(result)
			log.Printf("upload expiry receipt %s", raw)
		}
	}
	run()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func (c *imageCache) inspectBlobUploads(now time.Time) imageCacheUploadCleanup {
	result, err := c.collectStaleBlobUploads(now)
	if err != nil {
		result.Error = "upload_scan_failed"
	}
	return result
}

func (c *imageCache) cleanupStaleBlobUploads(now time.Time) (imageCacheUploadCleanup, error) {
	if !c.uploadBarrier.TryLock() {
		return imageCacheUploadCleanup{Skipped: []imageCacheUploadEntry{{Reason: "active_upload"}}}, nil
	}
	defer c.uploadBarrier.Unlock()
	result, err := c.collectStaleBlobUploads(now)
	if err != nil {
		return result, err
	}
	probe := c.uploadOpenFiles
	if probe == nil {
		probe = imageCacheOpenFiles
	}
	opened, err := probe()
	if err != nil {
		return result, fmt.Errorf("cannot establish upload inactivity: %w", err)
	}
	budget := c.uploadGCMaxBytes
	if budget <= 0 {
		budget = 1 << 30
	}
	groups := map[string][]imageCacheUploadEntry{}
	for _, entry := range result.Candidates {
		key := imageCacheUploadGroup(entry.Path)
		groups[key] = append(groups[key], entry)
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		entries := groups[key]
		size := int64(0)
		reason := ""
		for _, entry := range entries {
			size += entry.SizeBytes
			path := filepath.Join(c.storeDir, entry.Path)
			if opened[path] {
				reason = "active_upload"
				break
			}
			// Do not unlink a replaced or changed file from a stale observation.
			current, e := os.Lstat(path)
			if e != nil || !current.Mode().IsRegular() || !os.SameFile(entry.info, current) || current.Size() != entry.SizeBytes || !current.ModTime().Equal(entry.info.ModTime()) {
				reason = "upload_changed"
				break
			}
		}
		if reason == "" && size > budget {
			reason = "upload_budget_exhausted"
		}
		if reason != "" {
			for _, entry := range entries {
				entry.Reason = reason
				result.Skipped = append(result.Skipped, entry)
			}
			continue
		}
		// Data first, state last: a failed delete cannot orphan resumable data by
		// prematurely discarding the only state journal.
		sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
		for _, entry := range entries {
			if err := os.Remove(filepath.Join(c.storeDir, entry.Path)); err != nil {
				return result, fmt.Errorf("remove expired upload: %w", err)
			}
			result.Deleted = append(result.Deleted, entry)
			result.DeletedBytes += entry.SizeBytes
			budget -= entry.SizeBytes
		}
	}
	return result, nil
}

func (c *imageCache) collectStaleBlobUploads(now time.Time) (imageCacheUploadCleanup, error) {
	var result imageCacheUploadCleanup
	if c == nil || strings.TrimSpace(c.storeDir) == "" {
		return result, nil
	}
	ttl := c.uploadTTL
	if ttl <= 0 {
		ttl = defaultImageCacheUploadTTL
	}
	groups := map[string][]imageCacheUploadEntry{}
	for _, dir := range []string{"", imageCacheUploadDirName} {
		// Refuse symlinked upload directories rather than traversing outside store.
		full := filepath.Join(c.storeDir, dir)
		st, err := os.Lstat(full)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return result, err
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return result, errors.New("invalid upload directory")
		}
		entries, err := os.ReadDir(full)
		if err != nil {
			return result, err
		}
		for _, entry := range entries {
			name := entry.Name()
			known := dir == "" && strings.HasPrefix(name, "upload-")
			if dir != "" {
				known = (strings.HasSuffix(name, ".data") || strings.HasSuffix(name, ".json")) && cleanImageCacheUploadID(strings.TrimSuffix(name, filepath.Ext(name))) != ""
			}
			if !known || entry.IsDir() {
				continue
			}
			info, err := entry.Info()
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return result, err
			}
			if !info.Mode().IsRegular() {
				continue
			}
			relative := filepath.Join(dir, name)
			value := imageCacheUploadEntry{Path: relative, SizeBytes: info.Size(), ModifiedAt: info.ModTime().UTC().Format(time.RFC3339Nano), info: info}
			key := imageCacheUploadGroup(relative)
			groups[key] = append(groups[key], value)
			result.TotalBytes += info.Size()
		}
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		entries := groups[key]
		var activity time.Time
		var state *imageCacheBlobUploadState
		var dataSize int64
		reason := ""
		for _, entry := range entries {
			if entry.info.ModTime().After(activity) {
				activity = entry.info.ModTime()
			}
			if strings.HasSuffix(entry.Path, ".data") {
				dataSize = entry.SizeBytes
			}
			if filepath.Dir(entry.Path) == imageCacheUploadDirName && strings.HasSuffix(entry.Path, ".json") {
				raw, err := os.ReadFile(filepath.Join(c.storeDir, entry.Path))
				if err != nil {
					return result, err
				}
				var decoded imageCacheBlobUploadState
				if json.Unmarshal(raw, &decoded) != nil {
					reason = "upload_state_invalid"
					continue
				}
				state = &decoded
				if decoded.UpdatedAt.After(activity) {
					activity = decoded.UpdatedAt
				}
				if decoded.CreatedAt.After(activity) {
					activity = decoded.CreatedAt
				}
			}
		}
		if state != nil && state.SizeBytes != dataSize {
			result.StateMismatches++
		}
		if reason == "" && (activity.IsZero() || now.Sub(activity) < ttl) {
			reason = "recent_upload"
		}
		for _, entry := range entries {
			if reason != "" {
				entry.Reason = reason
				result.Skipped = append(result.Skipped, entry)
			} else {
				entry.Reason = "expired_upload"
				result.Candidates = append(result.Candidates, entry)
				result.CandidateBytes += entry.SizeBytes
			}
		}
	}
	return result, nil
}

func imageCacheUploadGroup(path string) string {
	if filepath.Dir(path) == imageCacheUploadDirName && (strings.HasSuffix(path, ".data") || strings.HasSuffix(path, ".json")) {
		return strings.TrimSuffix(path, filepath.Ext(path))
	}
	return path
}

// A single scan per pass avoids O(files*processes*descriptors). Failure to
// inspect a live process is unknown, not evidence that its uploads are idle.
func imageCacheOpenFiles() (map[string]bool, error) {
	if runtime.GOOS != "linux" {
		return nil, errors.New("upload GC requires Linux open-file observations")
	}
	processes, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	result := map[string]bool{}
	for _, process := range processes {
		if !process.IsDir() || strings.Trim(process.Name(), "0123456789") != "" {
			continue
		}
		dir := filepath.Join("/proc", process.Name(), "fd")
		fds, err := os.ReadDir(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, fd := range fds {
			target, err := os.Readlink(filepath.Join(dir, fd.Name()))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, err
			}
			result[target] = true
		}
	}
	return result, nil
}
