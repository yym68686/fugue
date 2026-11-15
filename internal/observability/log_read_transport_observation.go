package observability

import (
	"context"
	"io"
	"net/http/httptrace"
	"sync"
	"time"
)

// One fixed-size receipt for the failing HTTP stream, not a request history.
// None of the trace callback arguments (hosts, addresses, headers or errors)
// are retained. Booleans describe observed milestones, not their root cause.
type logReadTransportObservation struct {
	Phase             string `json:"phase"`
	Attempts          uint32 `json:"attempts"`
	ConnectionReady   bool   `json:"connection_ready"`
	ConnectionReused  bool   `json:"connection_reused"`
	RequestWritten    bool   `json:"request_written"`
	FirstResponseByte bool   `json:"first_response_byte"`
	HeadersReturned   bool   `json:"headers_returned"`
	HeaderMillis      int64  `json:"header_ms"`
	BodyBytes         int64  `json:"body_bytes"`
	BodyReadError     bool   `json:"body_read_error"`
}

type logReadTransportKey struct{}
type logReadTransportAttempt struct {
	mu    sync.Mutex
	fact  logReadTransportObservation
	owner *logReadTransportTrace
}
type logReadTransportTrace struct {
	mu             sync.Mutex
	latest, failed *logReadTransportAttempt
}

func (t *logReadTransportTrace) snapshot() *logReadTransportObservation {
	t.mu.Lock()
	attempt := t.failed
	if attempt == nil {
		attempt = t.latest
	}
	t.mu.Unlock()
	if attempt == nil {
		return nil
	}
	attempt.mu.Lock()
	defer attempt.mu.Unlock()
	copy := attempt.fact
	return &copy
}

func (a *logReadTransportAttempt) failure() {
	a.owner.mu.Lock()
	a.owner.failed = a
	a.owner.mu.Unlock()
}

func observedLogStream(ctx context.Context, phase string, open func(context.Context) (io.ReadCloser, error)) (io.ReadCloser, error) {
	t, _ := ctx.Value(logReadTransportKey{}).(*logReadTransportTrace)
	if t == nil {
		return open(ctx)
	}
	// Only internal, fixed purposes can enter diagnostic state.
	switch phase {
	case "pod_log", "rotation_inventory_before", "rotation_inventory_after", "rotation_current", "rotation_retained":
	default:
		phase = "other"
	}
	a := &logReadTransportAttempt{fact: logReadTransportObservation{Phase: phase}, owner: t}
	t.mu.Lock()
	t.latest = a
	// A new operation can be an intentional recovery (for example fetching a
	// compressed file after its uncompressed name disappeared). Do not retain
	// that recovered open error as the cause of a later, unrelated failure.
	// An older outstanding body can still mark itself failed when read later.
	t.failed = nil
	t.mu.Unlock()
	trace := &httptrace.ClientTrace{
		GetConn: func(string) {
			a.mu.Lock()
			a.fact.Attempts = min(a.fact.Attempts+1, uint32(65535))
			a.fact.ConnectionReady, a.fact.ConnectionReused = false, false
			a.fact.RequestWritten, a.fact.FirstResponseByte = false, false
			a.mu.Unlock()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			a.mu.Lock()
			a.fact.ConnectionReady, a.fact.ConnectionReused = true, info.Reused
			a.mu.Unlock()
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			a.mu.Lock()
			a.fact.RequestWritten = info.Err == nil
			a.mu.Unlock()
		},
		GotFirstResponseByte: func() {
			a.mu.Lock()
			a.fact.FirstResponseByte = true
			a.mu.Unlock()
		},
	}
	started := time.Now()
	body, err := open(httptrace.WithClientTrace(ctx, trace))
	a.mu.Lock()
	a.fact.HeaderMillis = time.Since(started).Milliseconds()
	a.fact.HeadersReturned = err == nil && body != nil
	a.mu.Unlock()
	if err != nil {
		a.failure()
		return body, err
	}
	if body == nil {
		return body, nil
	}
	return &observedLogBody{ReadCloser: body, attempt: a}, nil
}

type observedLogBody struct {
	io.ReadCloser
	attempt *logReadTransportAttempt
}

func (b *observedLogBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.attempt.mu.Lock()
	b.attempt.fact.BodyBytes += int64(n)
	b.attempt.fact.BodyReadError = b.attempt.fact.BodyReadError || err != nil && err != io.EOF
	b.attempt.mu.Unlock()
	if err != nil && err != io.EOF {
		b.attempt.failure()
	}
	return n, err
}
