package terminal

import (
	"context"
	"time"
)

const DefaultProbeTimeout = 100 * time.Millisecond

type ProbeResult struct {
	CursorRow           int
	CursorColumn        int
	DefaultForeground   string
	DefaultBackground   string
	KeyboardEnhancement bool
	TimedOut            bool
	Err                 error
}

type ProbeFunc func(context.Context) ProbeResult

func RunBoundedProbe(ctx context.Context, timeout time.Duration, fn ProbeFunc) ProbeResult {
	if timeout <= 0 {
		timeout = DefaultProbeTimeout
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if fn == nil {
		return ProbeResult{}
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resultCh := make(chan ProbeResult, 1)
	go func() {
		resultCh <- fn(probeCtx)
	}()

	select {
	case result := <-resultCh:
		return result
	case <-probeCtx.Done():
		return ProbeResult{TimedOut: true, Err: probeCtx.Err()}
	}
}
