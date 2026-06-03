package cli

import (
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	runtimeFollowOutputBuffer       = 512
	runtimeFollowOutputDrainTimeout = 250 * time.Millisecond
)

type runtimeFollowTextOutput struct {
	warnf func(string, ...any)
	lines chan string
	done  chan struct{}
	w     io.Writer

	mu       sync.Mutex
	firstErr error
	dropped  int
	warned   bool
}

func newRuntimeFollowTextOutput(w io.Writer, warnf func(string, ...any)) *runtimeFollowTextOutput {
	out := &runtimeFollowTextOutput{
		w:     w,
		warnf: warnf,
		lines: make(chan string, runtimeFollowOutputBuffer),
		done:  make(chan struct{}),
	}
	go out.run()
	return out
}

func (o *runtimeFollowTextOutput) run() {
	defer close(o.done)
	for line := range o.lines {
		if _, err := fmt.Fprintln(o.w, line); err != nil {
			o.recordErr(err)
			return
		}
	}
}

func (o *runtimeFollowTextOutput) enqueue(line string) error {
	if err := o.err(); err != nil {
		return err
	}
	select {
	case o.lines <- line:
		return nil
	default:
	}

	dropped := 0
	select {
	case <-o.lines:
		dropped++
	default:
	}
	select {
	case o.lines <- line:
	default:
		dropped++
	}
	if dropped > 0 {
		o.recordDrop(dropped)
	}
	return o.err()
}

func (o *runtimeFollowTextOutput) close() error {
	close(o.lines)
	drained := true
	select {
	case <-o.done:
	case <-time.After(runtimeFollowOutputDrainTimeout):
		drained = false
	}

	dropped := o.dropCount()
	if dropped > 0 {
		o.warnf("warning=runtime log output dropped_queued_lines=%d reason=slow_consumer", dropped)
	}
	if !drained {
		o.warnf("warning=runtime log output did not drain before command exit; some queued lines may not have been printed")
	}
	return o.err()
}

func (o *runtimeFollowTextOutput) recordErr(err error) {
	if err == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.firstErr == nil {
		o.firstErr = err
	}
}

func (o *runtimeFollowTextOutput) err() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.firstErr
}

func (o *runtimeFollowTextOutput) recordDrop(count int) {
	if count <= 0 {
		return
	}
	warnNow := false
	o.mu.Lock()
	o.dropped += count
	if !o.warned {
		o.warned = true
		warnNow = true
	}
	o.mu.Unlock()
	if warnNow {
		o.warnf("warning=runtime log output is slower than incoming logs; dropping older queued lines to keep --follow near real time")
	}
}

func (o *runtimeFollowTextOutput) dropCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.dropped
}
