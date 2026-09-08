package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

type cached struct {
	snapshot Snapshot
	fetched  time.Time
	used     uint64
}
type Store struct {
	provider Provider
	mu       sync.Mutex
	entries  map[string]cached
	serial   uint64
	group    singleflight.Group
	slots    chan struct{}
}

func NewStore(provider Provider) *Store {
	return &Store{provider: provider, entries: map[string]cached{}, slots: make(chan struct{}, 4)}
}

// Fetch coalesces equal requests and bounds network work across all panels.
// Snapshots are copied at the boundary so rendering never mutates the cache.
func (s *Store) Fetch(ctx context.Context, request Request, ttl time.Duration, force bool) (Snapshot, error) {
	key := request.Key()
	if !force {
		s.mu.Lock()
		entry, ok := s.entries[key]
		s.serial++
		entry.used = s.serial
		if ok {
			s.entries[key] = entry
		}
		s.mu.Unlock()
		if ok && time.Since(entry.fetched) < ttl {
			return cloneSnapshot(entry.snapshot)
		}
	}
	result := s.group.DoChan(key, func() (any, error) {
		select {
		case s.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		defer func() { <-s.slots }()
		fetchCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		snapshot, err := s.provider.Load(fetchCtx, request)
		if err != nil {
			return nil, err
		}
		snapshot = sanitizeSnapshot(snapshot)
		s.mu.Lock()
		s.serial++
		s.entries[key] = cached{snapshot: snapshot, fetched: time.Now(), used: s.serial}
		if len(s.entries) > 48 {
			oldest := ""
			var serial uint64 = ^uint64(0)
			for k, v := range s.entries {
				if v.used < serial {
					oldest, serial = k, v.used
				}
			}
			delete(s.entries, oldest)
		}
		s.mu.Unlock()
		return snapshot, nil
	})
	select {
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	case value := <-result:
		if value.Err != nil {
			return Snapshot{}, value.Err
		}
		snapshot, ok := value.Val.(Snapshot)
		if !ok {
			return Snapshot{}, fmt.Errorf("invalid dashboard snapshot")
		}
		return cloneSnapshot(snapshot)
	}
}
func cloneSnapshot(snapshot Snapshot) (Snapshot, error) {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return Snapshot{}, err
	}
	var copy Snapshot
	err = json.Unmarshal(data, &copy)
	return copy, err
}
