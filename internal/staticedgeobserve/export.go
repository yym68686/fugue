package staticedgeobserve

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// AsyncSink gives the serving process a bounded non-blocking channel. A slow
// collector consumes at most one exporter worker and never a request goroutine.
type AsyncSink struct {
	queue   chan Record
	client  *http.Client
	dropped atomic.Uint64
	errors  atomic.Uint64
	closed  atomic.Bool
}

func NewAsyncSink(socket string, capacity int) (*AsyncSink, error) {
	if capacity < 1 || capacity > 1024 {
		capacity = 128
	}
	c, e := UnixClient(socket, 2*time.Second)
	if e != nil {
		return nil, e
	}
	return &AsyncSink{queue: make(chan Record, capacity), client: c}, nil
}
func (s *AsyncSink) Submit(r Record) bool {
	if s.closed.Load() {
		s.dropped.Add(1)
		return false
	}
	// Snapshot owns the event slice; no serving buffers or payload are retained.
	select {
	case s.queue <- r:
		return true
	default:
		s.dropped.Add(1)
		return false
	}
}

// Drop records capacity loss when no per-request observer was allocated.
func (s *AsyncSink) Drop() { s.dropped.Add(1) }
func (s *AsyncSink) Run(ctx context.Context) {
	defer s.closed.Store(true)
	defer s.client.CloseIdleConnections()
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-s.queue:
			r.ExporterDroppedTotal = s.dropped.Load() + s.errors.Load()
			raw, e := json.Marshal(r)
			if e != nil || len(raw) > MaxRecordBytes {
				s.errors.Add(1)
				continue
			}
			req, e := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/records", bytes.NewReader(raw))
			if e != nil {
				s.errors.Add(1)
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			resp, e := s.client.Do(req)
			if e != nil {
				s.errors.Add(1)
				continue
			}
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
			resp.Body.Close()
			if resp.StatusCode != http.StatusAccepted {
				s.errors.Add(1)
			}
		}
	}
}

// Registry uses one timer for all live spans. Requests never wait for snapshot
// export. Capacity loss is explicit; body counters remain local to the request.
type Registry struct {
	mu       sync.Mutex
	active   map[*Observer]struct{}
	capacity int
	dropped  atomic.Uint64
}

func NewRegistry(capacity int) *Registry {
	if capacity < 1 || capacity > 2048 {
		capacity = 256
	}
	return &Registry{active: make(map[*Observer]struct{}), capacity: capacity}
}
func (r *Registry) Add(o *Observer) bool {
	if !r.mu.TryLock() {
		r.dropped.Add(1)
		o.dropped.Add(1)
		return false
	}
	defer r.mu.Unlock()
	if len(r.active) >= r.capacity {
		r.dropped.Add(1)
		o.dropped.Add(1)
		return false
	}
	r.active[o] = struct{}{}
	return true
}
func (r *Registry) Remove(o *Observer) { r.mu.Lock(); delete(r.active, o); r.mu.Unlock() }
func (r *Registry) Run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.mu.Lock()
			os := make([]*Observer, 0, len(r.active))
			for o := range r.active {
				os = append(os, o)
			}
			r.mu.Unlock()
			for _, o := range os {
				o.Emit()
			}
		}
	}
}
