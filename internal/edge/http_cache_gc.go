package edge

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Response data is disposable. This collector never traverses the sibling
// route/LKG, WAL, certificate or request-body directories.
const defaultHTTPCacheTotalBytes int64 = 512 << 20
const httpCacheGCInterval = 5 * time.Minute

type edgeHTTPCacheDisk struct {
	mu                     sync.Mutex
	initialized            bool
	used, limit            int64
	files                  int
	removed                [3]uint64 // expired, capacity, abandoned temporary file
	removedBytes, failures uint64
	skipped                atomic.Uint64
	lastSuccess            time.Time
	snapshot               atomic.Pointer[httpCacheDiskStats]
}

type httpCacheDiskStats struct {
	used, limit            int64
	files                  int
	initialized            bool
	removed                [3]uint64
	removedBytes, failures uint64
	lastSuccess            time.Time
}

func (state *edgeHTTPCacheDisk) publish() {
	state.snapshot.Store(&httpCacheDiskStats{used: state.used, limit: state.limit, files: state.files, initialized: state.initialized, removed: state.removed, removedBytes: state.removedBytes, failures: state.failures, lastSuccess: state.lastSuccess})
}

type httpCacheDiskFile struct {
	path               string
	info               fs.FileInfo
	bytes              int64
	expired, temporary bool
}

func (s *Service) httpCacheDisk() *edgeHTTPCacheDisk {
	s.httpCacheOnce.Do(func() {
		limit := defaultHTTPCacheTotalBytes
		if value, err := strconv.ParseInt(strings.TrimSpace(os.Getenv("FUGUE_EDGE_ASSET_CACHE_TOTAL_MAX_BYTES")), 10, 64); err == nil && value > 0 {
			limit = value
		}
		s.httpCache = &edgeHTTPCacheDisk{limit: limit}
		s.httpCache.publish()
	})
	return s.httpCache
}

func cacheAllocatedBytes(info fs.FileInfo) int64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return st.Blocks * 512
	}
	return ((info.Size() + 4095) / 4096) * 4096
}

// Read only the bounded metadata prefix, never a multi-megabyte response body.
// Old entries lack stale_until; retain them for at least a day beyond expiry
// and respect any larger stale window in the currently loaded policy.
func cacheDiskExpiry(reader io.Reader, stale time.Duration) (time.Time, error) {
	dec := json.NewDecoder(io.LimitReader(reader, 4096))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return time.Time{}, errors.New("invalid cache metadata")
	}
	var expires, until time.Time
	var version int
	var namespace string
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return time.Time{}, err
		}
		if key == "namespace" {
			if err := dec.Decode(&namespace); err != nil {
				return time.Time{}, err
			}
			break
		}
		switch key {
		case "version":
			err = dec.Decode(&version)
		case "expires_at":
			err = dec.Decode(&expires)
		case "stale_until":
			err = dec.Decode(&until)
		default:
			var value json.RawMessage
			err = dec.Decode(&value)
		}
		if err != nil {
			return time.Time{}, err
		}
	}
	if version != 1 || namespace == "" || expires.IsZero() {
		return time.Time{}, errors.New("cache expiry missing")
	}
	if until.IsZero() && stale < 24*time.Hour {
		stale = 24 * time.Hour
	}
	if bound := expires.Add(stale); until.Before(bound) {
		until = bound
	}
	return until, nil
}

func (s *Service) cacheStaleWindow() time.Duration {
	var stale time.Duration
	if bundle, ok := s.Bundle(); ok {
		for _, p := range bundle.CachePolicies {
			if v := time.Duration(p.StaleWhileRevalidateSeconds) * time.Second; v > stale {
				stale = v
			}
		}
	}
	return stale
}

func (s *Service) runHTTPCacheGC(ctx context.Context) {
	if strings.TrimSpace(s.Config.AssetCachePath) == "" {
		return
	}
	ticker := time.NewTicker(httpCacheGCInterval)
	defer ticker.Stop()
	for {
		scanCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := s.collectHTTPCache(scanCtx, time.Now())
		cancel()
		if err != nil && ctx.Err() == nil && s.Logger != nil {
			s.Logger.Printf("edge HTTP cache collection failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) collectHTTPCache(ctx context.Context, now time.Time) error {
	state := s.httpCacheDisk()
	state.mu.Lock()
	defer state.mu.Unlock()
	defer state.publish()
	return s.collectHTTPCacheLocked(ctx, now, state)
}

func (s *Service) collectHTTPCacheLocked(ctx context.Context, now time.Time, state *edgeHTTPCacheDisk) error {
	root := strings.TrimSpace(s.Config.AssetCachePath)
	if root == "" {
		return nil
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		state.failures++
		return err
	}
	// OpenRoot confines all later opens/removes, including a concurrently swapped
	// namespace symlink. WalkDir itself does not follow symlinks.
	confined, err := os.OpenRoot(root)
	if err != nil {
		state.failures++
		return err
	}
	defer confined.Close()
	var entries []httpCacheDiskFile
	var directories []httpCacheDiskFile
	var used int64
	stale := s.cacheStaleWindow()
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			info, err := confined.Lstat(rel)
			if err != nil {
				return err
			}
			allocated := cacheAllocatedBytes(info)
			used += allocated
			directories = append(directories, httpCacheDiskFile{path: rel, info: info, bytes: allocated})
			return nil
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := confined.Lstat(rel)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		allocated := cacheAllocatedBytes(info)
		used += allocated
		item := httpCacheDiskFile{path: rel, info: info, bytes: allocated}
		switch {
		case strings.HasSuffix(rel, ".json"):
			f, err := confined.Open(rel)
			if err != nil {
				return err
			}
			until, readErr := cacheDiskExpiry(f, stale)
			_ = f.Close()
			if readErr != nil {
				state.failures++
				return nil // Unknown data is accounted, never deleted as a response.
			} else {
				item.expired = now.After(until)
			}
			entries = append(entries, item)
		case strings.HasSuffix(rel, ".tmp") || strings.HasPrefix(filepath.Base(rel), ".cache-"):
			item.temporary = true
			item.expired = now.Sub(info.ModTime()) > time.Hour
			entries = append(entries, item)
		}
		return nil
	})
	if err != nil {
		state.failures++
		state.initialized = false
		return err
	}
	// Expired responses first, then oldest writes (FIFO) to the low watermark.
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].expired != entries[j].expired {
			return entries[i].expired
		}
		if entries[i].info.ModTime().Equal(entries[j].info.ModTime()) {
			return entries[i].path < entries[j].path
		}
		return entries[i].info.ModTime().Before(entries[j].info.ModTime())
	})
	over := used > state.limit*9/10
	target := state.limit * 8 / 10
	remaining := len(entries)
	for _, item := range entries {
		if err := ctx.Err(); err != nil {
			state.initialized = false
			return err
		}
		if !item.expired && (!over || used <= target || item.temporary) {
			continue
		}
		latest, err := confined.Lstat(item.path)
		if errors.Is(err, fs.ErrNotExist) {
			used -= item.bytes
			remaining--
			continue
		}
		if err != nil {
			state.failures++
			continue
		}
		// Another worker may have replaced an entry since the inventory.
		if !os.SameFile(item.info, latest) || !latest.ModTime().Equal(item.info.ModTime()) || latest.Size() != item.info.Size() {
			state.initialized = false
			return errors.New("cache changed during collection")
		}
		if err := confined.Remove(item.path); err != nil {
			state.failures++
			continue
		}
		used -= item.bytes
		remaining--
		reason := 1
		if item.expired {
			reason = 0
		}
		if item.temporary {
			reason = 2
		}
		state.removed[reason]++
		state.removedBytes += uint64(item.bytes)
	}

	// Remove empty namespace directories as well; otherwise deployments with
	// tiny responses leak directory blocks forever even under a response cap.
	for i := len(directories) - 1; i >= 0; i-- {
		dir := directories[i]
		if dir.path == "." {
			continue
		}
		if err := ctx.Err(); err != nil {
			state.initialized = false
			return err
		}
		if err := confined.Remove(dir.path); err == nil {
			used -= dir.bytes
		} else if !errors.Is(err, syscall.ENOTEMPTY) && !errors.Is(err, syscall.EEXIST) && !errors.Is(err, fs.ErrNotExist) {
			state.failures++
		}
	}
	state.used = used
	state.files = remaining
	state.initialized = true
	state.lastSuccess = now
	return nil
}

func (s *Service) storeHTTPCacheFile(path string, data []byte) error {
	state := s.httpCacheDisk()
	if !state.mu.TryLock() {
		state.skipped.Add(1)
		return nil
	}
	defer state.mu.Unlock()
	defer state.publish()
	if !state.initialized {
		// Until the first inventory succeeds, bypass cache writes, not HTTP traffic.
		// A request must never pay for a filesystem-wide scan.
		state.skipped.Add(1)
		return nil
	}
	// Reserve temporary-file space as well as the old entry. Replacing an entry
	// must not temporarily double a full directory's disk use.
	reservation := ((int64(len(data)) + 4095) / 4096) * 4096
	directoryReserve := int64(strings.Count(filepath.Clean(path), string(os.PathSeparator))-strings.Count(filepath.Clean(s.Config.AssetCachePath), string(os.PathSeparator))+1) * 4096
	if reservation+directoryReserve > state.limit-state.used {
		state.skipped.Add(1)
		return nil
	}
	root, err := os.OpenRoot(s.Config.AssetCachePath)
	if err != nil {
		state.failures++
		return err
	}
	defer root.Close()
	rel, err := filepath.Rel(s.Config.AssetCachePath, path)
	if err != nil {
		return err
	}
	// Account for directory allocation changes, including an abandoned empty
	// directory after a failed write. OpenRoot confines each operation.
	dirs := []string{"."}
	for dir := filepath.Dir(rel); dir != "."; dir = filepath.Dir(dir) {
		if dir == ".." || filepath.IsAbs(dir) {
			return errors.New("cache path escapes root")
		}
		dirs = append(dirs, dir)
	}
	directoryBytes := func() int64 {
		var total int64
		for _, dir := range dirs {
			if info, err := root.Lstat(dir); err == nil && info.IsDir() {
				total += cacheAllocatedBytes(info)
			}
		}
		return total
	}
	beforeDirs := directoryBytes()
	defer func() { state.used += directoryBytes() - beforeDirs }()
	// Create and replace files only inside the confined cache root.
	if err := root.MkdirAll(filepath.Dir(rel), 0755); err != nil {
		state.failures++
		return err
	}
	tempRel := filepath.Join(filepath.Dir(rel), ".cache-"+rand.Text()+".tmp")
	temp, err := root.OpenFile(tempRel, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		state.failures++
		return err
	}
	defer root.Remove(tempRel)
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		state.failures++
		return err
	}
	info, statErr := temp.Stat()
	if statErr != nil {
		_ = temp.Close()
		state.failures++
		return statErr
	}
	allocated := cacheAllocatedBytes(info)
	if err := temp.Close(); err != nil {
		state.failures++
		return err
	}
	if allocated+state.used+directoryBytes()-beforeDirs > state.limit {
		state.skipped.Add(1)
		return nil
	}
	oldBytes := int64(0)
	oldExists := false
	if info, err := root.Lstat(rel); err == nil {
		oldBytes = cacheAllocatedBytes(info)
		oldExists = true
	} else if !errors.Is(err, fs.ErrNotExist) {
		state.failures++
		return err
	}
	if err := root.Rename(tempRel, rel); err != nil {
		state.failures++
		return err
	}
	state.used += allocated - oldBytes
	if !oldExists {
		state.files++
	}
	return nil
}

func (s *Service) writeHTTPCacheMetrics(w http.ResponseWriter) {
	disk := s.httpCacheDisk()
	state := disk.snapshot.Load()
	for _, m := range []struct {
		name  string
		value int64
	}{{"bytes", state.used}, {"limit_bytes", state.limit}, {"files", int64(state.files)}, {"inventory_ready", int64(boolGauge(state.initialized))}, {"last_success_timestamp_seconds", int64(unixSeconds(&state.lastSuccess))}} {
		fmt.Fprintf(w, "# TYPE fugue_edge_http_cache_%s gauge\nfugue_edge_http_cache_%s %d\n", m.name, m.name, m.value)
	}
	for i, reason := range []string{"expired", "capacity", "temporary"} {
		fmt.Fprintf(w, "fugue_edge_http_cache_removed_total{reason=%q} %d\n", reason, state.removed[i])
	}
	fmt.Fprintf(w, "fugue_edge_http_cache_removed_bytes_total %d\nfugue_edge_http_cache_errors_total %d\nfugue_edge_http_cache_write_skipped_total %d\n", state.removedBytes, state.failures, disk.skipped.Load())
}
