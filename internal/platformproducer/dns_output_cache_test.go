package platformproducer

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
)

func TestDNSOutputValidationCacheBoundedAndConcurrent(t *testing.T) {
	var cache dnsOutputValidationCache
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := sha256.Sum256([]byte(fmt.Sprintf("%d/%d", i, j)))
				cache.remember(key)
				cache.contains(key)
			}
		}(i)
	}
	wg.Wait()
	cache.Lock()
	defer cache.Unlock()
	if len(cache.keys) != 64 {
		t.Fatal("unbounded validation fingerprints", len(cache.keys))
	}
}
