package edge

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/config"
)

func TestHTTPCacheGCExpiryAndStaleWindow(t *testing.T) {
	now := time.Now().UTC()
	s := NewService(config.EdgeConfig{AssetCachePath: t.TempDir()}, nil)
	for _, tc := range []struct {
		name           string
		expires, stale time.Time
		keep           bool
	}{
		{"expired", now.Add(-time.Hour), now.Add(-time.Minute), false},
		{"stale", now.Add(-time.Hour), now.Add(time.Hour), true},
		{"fresh", now.Add(time.Hour), now.Add(time.Hour), true},
		{"legacy-stale", now.Add(-time.Hour), time.Time{}, true},
		{"legacy-expired", now.Add(-48 * time.Hour), time.Time{}, false},
	} {
		data, _ := json.Marshal(edgeHTTPCacheEntry{Version: 1, ExpiresAt: tc.expires, StaleUntil: tc.stale, Namespace: "sample", Body: []byte("response")})
		path := filepath.Join(s.Config.AssetCachePath, tc.name+".json")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.collectHTTPCache(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"expired", "legacy-expired"} {
		if _, err := os.Stat(filepath.Join(s.Config.AssetCachePath, n+".json")); !os.IsNotExist(err) {
			t.Fatalf("expired file remains: %s", n)
		}
	}
	for _, n := range []string{"stale", "fresh", "legacy-stale"} {
		if _, err := os.Stat(filepath.Join(s.Config.AssetCachePath, n+".json")); err != nil {
			t.Fatal(err)
		}
	}
	if s.httpCacheDisk().removed[0] != 2 {
		t.Fatal("expired removals not counted")
	}
}

func TestHTTPCacheGCCapacityConcurrentWrites(t *testing.T) {
	s := NewService(config.EdgeConfig{AssetCachePath: t.TempDir()}, nil)
	state := s.httpCacheDisk()
	state.limit = 32 << 10
	if err := s.collectHTTPCache(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.storeHTTPCacheFile(filepath.Join(s.Config.AssetCachePath, "sample.json"), []byte(strings.Repeat("x", 7000))); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	rootInfo, _ := os.Stat(s.Config.AssetCachePath)
	allocated := cacheAllocatedBytes(rootInfo)
	es, _ := os.ReadDir(s.Config.AssetCachePath)
	for _, e := range es {
		info, _ := e.Info()
		allocated += cacheAllocatedBytes(info)
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatal("temp leaked")
		}
	}
	if allocated > state.limit || state.used != allocated {
		t.Fatalf("allocation mismatch: disk=%d accounting=%d limit=%d", allocated, state.used, state.limit)
	}
	// A startup inventory over budget evicts old entries down to the low watermark.
	for _, name := range []string{"old.json", "new.json"} {
		if err := os.WriteFile(filepath.Join(s.Config.AssetCachePath, name), gcTestBody(t, 20000), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.collectHTTPCache(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if state.used > state.limit*8/10 || state.removed[1] == 0 {
		t.Fatalf("capacity did not converge: %+v", state)
	}
}

func TestHTTPCacheGCConfinesFilesAndBypassesWithoutInventory(t *testing.T) {
	root := t.TempDir()
	cache := filepath.Join(root, "http-cache")
	outside := filepath.Join(root, "routes-cache.json")
	if err := os.WriteFile(outside, []byte("LKG"), 0600); err != nil {
		t.Fatal(err)
	}
	s := NewService(config.EdgeConfig{AssetCachePath: cache}, nil)
	if err := s.storeHTTPCacheFile(filepath.Join(cache, "pre-init.json"), []byte("skip")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cache); !os.IsNotExist(err) {
		t.Fatal("uninitialized write touched disk")
	}
	if err := s.collectHTTPCache(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(root, filepath.Join(cache, "escape")); err != nil {
		t.Fatal(err)
	}
	if err := s.storeHTTPCacheFile(filepath.Join(cache, "escape", "routes-cache.json"), []byte("bad")); err == nil {
		t.Fatal("write followed escaping symlink")
	}
	if err := s.collectHTTPCache(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(outside); string(data) != "LKG" {
		t.Fatal("LKG changed")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.collectHTTPCache(cancelled, time.Now()); err == nil || s.httpCacheDisk().initialized {
		t.Fatal("failed inventory accepted")
	}
}

func gcTestBody(t *testing.T, size int) []byte {
	t.Helper()
	data, err := json.Marshal(edgeHTTPCacheEntry{Version: 1, Namespace: "sample", ExpiresAt: time.Now().Add(time.Hour), Body: []byte(strings.Repeat("x", size))})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestHTTPCacheGCDoesNotEvictUnknownData(t *testing.T) {
	s := NewService(config.EdgeConfig{AssetCachePath: t.TempDir()}, nil)
	s.httpCacheDisk().limit = 4096
	path := filepath.Join(s.Config.AssetCachePath, "routes-cache.json")
	data := []byte(`{"version":2,"bundle":"` + strings.Repeat("LKG", 4096) + `"}`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.collectHTTPCache(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != string(data) {
		t.Fatal("unrecognized state deleted")
	}
}

func TestHTTPCacheGCRemovesEmptyNamespacesAndDoesNotBlockTraffic(t *testing.T) {
	s := NewService(config.EdgeConfig{AssetCachePath: t.TempDir()}, nil)
	empty := filepath.Join(s.Config.AssetCachePath, "obsolete", "nested")
	if err := os.MkdirAll(empty, 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.collectHTTPCache(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Dir(empty)); !os.IsNotExist(err) {
		t.Fatal("empty namespace remains")
	}
	state := s.httpCacheDisk()
	state.mu.Lock()
	defer state.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		s.writeHTTPCacheMetrics(httptest.NewRecorder())
		done <- s.storeHTTPCacheFile(filepath.Join(s.Config.AssetCachePath, "busy.json"), []byte("bypass"))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("metrics or traffic waits for collector")
	}
}
