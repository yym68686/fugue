package managedappstatus

import (
	"strings"
	"sync"
	"time"
)

const (
	DefaultCacheTTL       = 15 * time.Second
	DefaultRefreshTimeout = 5 * time.Second
)

// Cache owns published observations and coordinates their refresh. Its zero
// value is ready to use. A Cache must not be copied after first use.
type Cache struct {
	ttl            time.Duration
	refreshTimeout time.Duration
	mu             sync.RWMutex
	byApp          map[string]AppSnapshot
	inventory      InventorySnapshot
	refresh        Refresh[AppSnapshot, InventorySnapshot]
}

func NewCache(ttl, refreshTimeout time.Duration) Cache {
	return Cache{ttl: ttl, refreshTimeout: refreshTimeout}
}

func (c *Cache) TTL() time.Duration {
	if c.ttl <= 0 {
		return DefaultCacheTTL
	}
	return c.ttl
}

func (c *Cache) RefreshTimeout() time.Duration {
	if c.refreshTimeout <= 0 {
		return DefaultRefreshTimeout
	}
	return c.refreshTimeout
}

func (c *Cache) App(key string) (AppSnapshot, bool, bool) {
	c.mu.RLock()
	entry, ok := c.byApp[key]
	c.mu.RUnlock()
	if !ok || !entry.Ok {
		return AppSnapshot{}, false, false
	}
	expired := time.Now().After(entry.ExpiresAt) || strings.TrimSpace(entry.ClusterID) == ""
	return entry.clone(), true, expired
}

func (c *Cache) PublishApp(key string, entry AppSnapshot) {
	if strings.TrimSpace(key) == "" {
		return
	}
	entry = entry.clone()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byApp == nil {
		c.byApp = make(map[string]AppSnapshot)
	}
	c.byApp[key] = entry
}

func (c *Cache) Inventory() (InventorySnapshot, bool, bool) {
	c.mu.RLock()
	entry := c.inventory
	c.mu.RUnlock()
	if !entry.Ok {
		return InventorySnapshot{}, false, false
	}
	return entry, true, time.Now().After(entry.ExpiresAt)
}

// PublishInventory only accepts observations tied to a confirmed cluster.
// A plain inventory read for backing services cannot replace live evidence.
func (c *Cache) PublishInventory(entry InventorySnapshot) {
	if !entry.Ok || strings.TrimSpace(entry.ClusterID) == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.byApp {
		if !entry.Has(key) {
			c.byApp[key] = entry.app(key)
		}
	}
	if c.byApp == nil {
		c.byApp = make(map[string]AppSnapshot, entry.Len())
	}
	for key := range entry.items {
		c.byApp[key] = entry.app(key)
	}
	c.inventory = entry
}

func (c *Cache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byApp = nil
	c.inventory = InventorySnapshot{}
	c.refresh.ResetBackoff()
}

func (c *Cache) NextSequence() uint64 {
	return c.refresh.NextSequence()
}

func (c *Cache) RefreshApp(key string, fetch func() (AppSnapshot, error)) (AppSnapshot, error) {
	entry, err := c.refresh.App(key, fetch)
	return entry.clone(), err
}

func (c *Cache) ReadInventory(fetch func() (InventorySnapshot, error)) (InventorySnapshot, error) {
	return c.refresh.Inventory(fetch)
}

func (c *Cache) RefreshInventory(fetch func() (InventorySnapshot, error)) (InventorySnapshot, error) {
	return c.refresh.List(fetch)
}

func (c *Cache) RefreshInventoryAsync(fetch func() (InventorySnapshot, error), onError func(error)) {
	c.refresh.ListAsync(fetch, onError)
}

func (c *Cache) CanRefreshInventory(now time.Time) bool {
	return c.refresh.ListAllowed(now)
}
