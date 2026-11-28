package managedappstatus

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

const DefaultRefreshBackoff = 15 * time.Second

var (
	ErrRefreshBackoff = errors.New("managed app status refresh backoff active")
	ErrEmptyAppKey    = errors.New("managed app cache key is empty")
)

// Refresh owns observation ordering, duplicate suppression, and the retry
// window for list observations. Its zero value is ready to use.
type Refresh[App, List any] struct {
	group     singleflight.Group
	sequence  atomic.Uint64
	mu        sync.RWMutex
	notBefore time.Time
}

func (r *Refresh[App, List]) NextSequence() uint64 {
	return r.sequence.Add(1)
}

func (r *Refresh[App, List]) ListAllowed(now time.Time) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return !now.Before(r.notBefore)
}

func (r *Refresh[App, List]) ResetBackoff() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notBefore = time.Time{}
}

func (r *Refresh[App, List]) App(key string, fetch func() (App, error)) (App, error) {
	if key == "" {
		var zero App
		return zero, ErrEmptyAppKey
	}
	value, err, _ := r.group.Do("app:"+key, func() (any, error) { return fetch() })
	if err != nil {
		var zero App
		return zero, err
	}
	entry, _ := value.(App)
	return entry, nil
}

func (r *Refresh[App, List]) Inventory(fetch func() (List, error)) (List, error) {
	value, err, _ := r.group.Do("inventory", func() (any, error) { return fetch() })
	if err != nil {
		var zero List
		return zero, err
	}
	entry, _ := value.(List)
	return entry, nil
}

func (r *Refresh[App, List]) List(fetch func() (List, error)) (List, error) {
	if !r.ListAllowed(time.Now()) {
		var zero List
		return zero, ErrRefreshBackoff
	}
	value, err, _ := r.group.Do("list", func() (any, error) { return r.fetchList(fetch) })
	if err != nil {
		var zero List
		return zero, err
	}
	entry, _ := value.(List)
	return entry, nil
}

func (r *Refresh[App, List]) ListAsync(fetch func() (List, error), onError func(error)) {
	if !r.ListAllowed(time.Now()) {
		return
	}
	r.group.DoChan("list", func() (any, error) {
		value, err := r.fetchList(fetch)
		if err != nil && onError != nil {
			onError(err)
		}
		return value, err
	})
}

func (r *Refresh[App, List]) fetchList(fetch func() (List, error)) (List, error) {
	if !r.ListAllowed(time.Now()) {
		var zero List
		return zero, ErrRefreshBackoff
	}
	value, err := fetch()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notBefore = time.Time{}
	if err != nil {
		r.notBefore = time.Now().Add(DefaultRefreshBackoff)
	}
	return value, err
}
