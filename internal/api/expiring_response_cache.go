package api

import (
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

type expiringResponseCache[T any] struct {
	ttl   time.Duration
	mu    sync.RWMutex
	byKey map[string]expiringResponseCacheEntry[T]
	group singleflight.Group
}

type expiringResponseCacheEntry[T any] struct {
	value     T
	expiresAt time.Time
	ok        bool
}

func newExpiringResponseCache[T any](ttl time.Duration) expiringResponseCache[T] {
	return expiringResponseCache[T]{
		ttl:   ttl,
		byKey: make(map[string]expiringResponseCacheEntry[T]),
	}
}

func (c *expiringResponseCache[T]) getEntry(key string) (expiringResponseCacheEntry[T], bool) {
	if c == nil {
		return expiringResponseCacheEntry[T]{}, false
	}

	c.mu.RLock()
	entry, ok := c.byKey[key]
	c.mu.RUnlock()
	if !ok || !entry.ok {
		return expiringResponseCacheEntry[T]{}, false
	}
	return entry, true
}

func (c *expiringResponseCache[T]) get(key string) (T, bool) {
	var zero T
	entry, ok := c.getEntry(key)
	if !ok || time.Now().After(entry.expiresAt) {
		return zero, false
	}
	return entry.value, true
}

func (c *expiringResponseCache[T]) set(key string, value T) {
	if c == nil || key == "" || c.ttl <= 0 {
		return
	}

	c.mu.Lock()
	if c.byKey == nil {
		c.byKey = make(map[string]expiringResponseCacheEntry[T])
	}
	c.byKey[key] = expiringResponseCacheEntry[T]{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
		ok:        true,
	}
	c.mu.Unlock()
}

func (c *expiringResponseCache[T]) clear(key string) {
	if c == nil || key == "" {
		return
	}

	c.mu.Lock()
	delete(c.byKey, key)
	c.mu.Unlock()
}

func (c *expiringResponseCache[T]) do(key string, load func() (T, error)) (T, error) {
	if value, ok := c.get(key); ok {
		return value, nil
	}

	raw, err, _ := c.group.Do(key, func() (any, error) {
		if value, ok := c.get(key); ok {
			return value, nil
		}

		value, err := load()
		if err != nil {
			var zero T
			return zero, err
		}

		c.set(key, value)
		return value, nil
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return raw.(T), nil
}
