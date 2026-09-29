package main

import (
	"context"
	"errors"
	"sync"
	"time"

	o "fugue/internal/staticedgeobserve"
)

// A configuration owns workers, but its active requests lease them through
// Caddy's asynchronous drain. Cleanup is not a request-completion barrier.
type observationWorkers struct {
	sink         *o.AsyncSink
	registry     *o.Registry
	stopRegistry context.CancelFunc
	stopSink     context.CancelFunc
	registryDone chan struct{}
	done         chan struct{}
	once         sync.Once
}

func (h *Observation) newWorkers() (*observationWorkers, error) {
	sink, err := o.NewAsyncSink(h.Socket, 128)
	if err != nil {
		return nil, err
	}
	registryCtx, stopRegistry := context.WithCancel(context.Background())
	sinkCtx, stopSink := context.WithCancel(context.Background())
	w := &observationWorkers{sink: sink, registry: o.NewRegistry(h.Capacity),
		stopRegistry: stopRegistry, stopSink: stopSink,
		registryDone: make(chan struct{}), done: make(chan struct{})}
	go w.sink.Run(sinkCtx)
	go func() { defer close(w.registryDone); w.registry.Run(registryCtx) }()
	return w, nil
}

func (w *observationWorkers) drain() {
	w.once.Do(func() {
		// Module cleanup and request goroutines never wait for collector I/O.
		go func() {
			defer close(w.done)
			w.stopRegistry()
			<-w.registryDone
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = w.sink.Drain(ctx)
			w.stopSink()
			<-w.sink.Done()
		}()
	})
}

func (h *Observation) acquireWorkers() (*observationWorkers, error) {
	h.workMu.Lock()
	defer h.workMu.Unlock()
	if h.workers == nil {
		if h.retiringWorkers != nil {
			select {
			case <-h.retiringWorkers.done:
				h.retiringWorkers = nil
			default:
				// Keep one worker pair per module even if many late-scheduled
				// requests arrive while a failed collector is draining.
				h.retiringWorkers.sink.Drop()
				return nil, errors.New("observation workers still retiring")
			}
		}
		// A handler scheduled before Caddy closed its listener may enter after
		// cleanup. It may acquire new workers after the previous pair exits.
		var err error
		h.workers, err = h.newWorkers()
		if err != nil {
			return nil, err
		}
	}
	h.active++
	return h.workers, nil
}

func (h *Observation) releaseWorkers(w *observationWorkers) {
	h.workMu.Lock()
	h.active--
	retire := h.retired && h.active == 0
	if retire {
		h.workers = nil
		h.retiringWorkers = w
	}
	h.workMu.Unlock()
	if retire {
		w.drain()
	}
}

// A transport attempt can own an upgraded response body beyond its caller's
// stack frame. Its lease ends only when that body completes or closes.
func (h *Observation) retainWorkers(w *observationWorkers) bool {
	h.workMu.Lock()
	defer h.workMu.Unlock()
	if w == nil || h.workers != w {
		return false
	}
	h.active++
	return true
}
