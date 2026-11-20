package observability

import (
	"context"
	"io"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"
)

// One fixed-size receipt for the failing HTTP stream, not a request history.
// None of the trace callback arguments (hosts, addresses or errors) are
// retained. Timings describe caller-observed waits, not server CPU or a
// network root cause. Chain totals include recovered attempts and older bodies.
type logReadTransportObservation struct {
	Phase             string                     `json:"phase"`
	Attempts          uint32                     `json:"attempts"`
	ConnectionReady   bool                       `json:"connection_ready"`
	ConnectionReused  bool                       `json:"connection_reused"`
	RequestWritten    bool                       `json:"request_written"`
	FirstResponseByte bool                       `json:"first_response_byte"`
	StreamOpened      bool                       `json:"stream_opened"`
	OpenMillis        int64                      `json:"open_ms"`
	BodyBytes         int64                      `json:"body_bytes"`
	BodyReadError     bool                       `json:"body_read_error"`
	StartOffsetMillis int64                      `json:"start_offset_ms"`
	DeadlineMillis    int64                      `json:"deadline_remaining_ms"`
	ConnectionMillis  int64                      `json:"connection_ready_ms"`
	RequestMillis     int64                      `json:"request_written_ms"`
	FirstByteMillis   int64                      `json:"first_response_byte_ms"`
	BodyReadMillis    int64                      `json:"body_read_ms"`
	ChainStreams      uint32                     `json:"chain_streams"`
	ChainOpenMillis   int64                      `json:"chain_open_ms"`
	ChainBodyBytes    int64                      `json:"chain_body_bytes"`
	ChainReadMillis   int64                      `json:"chain_body_read_ms"`
	HTTPExchange      logHTTPExchangeObservation `json:"http_exchange,omitzero"`
}

// The last client transport call in this stream, not a unique wire request.
// Replace the whole value; readers can copy it without sharing mutable state.
type logHTTPExchangeObservation struct {
	RoundTrips uint32 `json:"round_trips"`
	Status     int    `json:"status,omitempty"`
	AuditID    string `json:"audit_id,omitempty"`
	AuditState string `json:"audit_id_state"`
}

// A caller-side milestone for the selected stream, not proof that any server
// or proxy received a request. Empty means insufficient trace coverage.
func (r *logReadTransportObservation) boundary() string {
	switch {
	case r == nil:
		return ""
	case r.StreamOpened && r.BodyReadError:
		return "body_read_error"
	case r.HTTPExchange.Status != 0:
		return "http_response_observed"
	case r.FirstResponseByte:
		return "first_byte_observed"
	case r.RequestWritten:
		return "client_write_completed"
	case r.ConnectionReady:
		return "connection_ready"
	case r.Attempts > 0:
		return "connection_requested"
	default:
		return ""
	}
}

type logHTTPAttemptKey struct{}
type observedLogTransport struct{ http.RoundTripper }

func observeLogHTTPResponse(next http.RoundTripper) http.RoundTripper {
	return observedLogTransport{next}
}

func (t observedLogTransport) WrappedRoundTripper() http.RoundTripper {
	return t.RoundTripper
}

func (t observedLogTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	a, _ := r.Context().Value(logHTTPAttemptKey{}).(*logReadTransportAttempt)
	if a == nil || a.fact.Phase == "other" || r.Method != http.MethodGet {
		return t.RoundTripper.RoundTrip(r)
	}
	a.mu.Lock()
	a.httpCalls++
	call := a.httpCalls
	fact := logHTTPExchangeObservation{RoundTrips: uint32(min(call, 65535)), AuditState: "no_response"}
	a.fact.HTTPExchange = fact
	a.mu.Unlock()
	resp, err := t.RoundTripper.RoundTrip(r)
	if resp != nil {
		if resp.StatusCode >= 100 && resp.StatusCode <= 599 {
			fact.Status = resp.StatusCode
		}
		switch values := resp.Header.Values("Audit-ID"); {
		case len(values) == 0:
			fact.AuditState = "missing"
		case len(values) == 1 && validLogAuditID(values[0]):
			fact.AuditState, fact.AuditID = "present", values[0]
		default:
			fact.AuditState = "invalid_or_ambiguous"
		}
		a.mu.Lock()
		if a.httpCalls == call {
			a.fact.HTTPExchange = fact
		}
		a.mu.Unlock()
	}
	return resp, err
}

// Do not copy arbitrary header values into the source diagnostics; only
// canonical lowercase UUIDs may be joined to existing server audit records.
func validLogAuditID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i := range len(id) {
		b := id[i]
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if b != '-' {
				return false
			}
		} else if !(b >= '0' && b <= '9' || b >= 'a' && b <= 'f') {
			return false
		}
	}
	return true
}

type logReadTransportKey struct{}
type logReadTransportAttempt struct {
	mu        sync.Mutex
	fact      logReadTransportObservation
	owner     *logReadTransportTrace
	started   time.Time
	readTime  time.Duration
	httpCalls uint64
}
type logReadTransportTrace struct {
	mu             sync.Mutex
	latest, failed *logReadTransportAttempt
	started        time.Time
	streams        uint32
	openTime       time.Duration
	readTime       time.Duration
	bodyBytes      int64
	clock          func() time.Time
}

func (t *logReadTransportTrace) now() time.Time {
	if t.clock != nil {
		return t.clock()
	}
	return time.Now()
}

func (t *logReadTransportTrace) snapshot() *logReadTransportObservation {
	t.mu.Lock()
	attempt := t.failed
	if attempt == nil {
		attempt = t.latest
	}
	streams, openTime, readTime, bodyBytes := t.streams, t.openTime, t.readTime, t.bodyBytes
	t.mu.Unlock()
	if attempt == nil {
		return nil
	}
	attempt.mu.Lock()
	defer attempt.mu.Unlock()
	copy := attempt.fact
	copy.BodyReadMillis = attempt.readTime.Milliseconds()
	copy.ChainStreams, copy.ChainOpenMillis = streams, openTime.Milliseconds()
	copy.ChainReadMillis, copy.ChainBodyBytes = readTime.Milliseconds(), bodyBytes
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
	started := t.now()
	a := &logReadTransportAttempt{fact: logReadTransportObservation{Phase: phase, DeadlineMillis: -1}, owner: t, started: started}
	if deadline, ok := ctx.Deadline(); ok {
		a.fact.DeadlineMillis = max(int64(0), deadline.Sub(started).Milliseconds())
	}
	t.mu.Lock()
	if t.started.IsZero() {
		t.started = started
	}
	a.fact.StartOffsetMillis = max(int64(0), started.Sub(t.started).Milliseconds())
	t.streams = min(t.streams+1, uint32(65535))
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
			a.fact.ConnectionMillis, a.fact.RequestMillis, a.fact.FirstByteMillis = 0, 0, 0
			a.mu.Unlock()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			a.mu.Lock()
			a.fact.ConnectionReady, a.fact.ConnectionReused = true, info.Reused
			a.fact.ConnectionMillis = max(int64(0), t.now().Sub(a.started).Milliseconds())
			a.mu.Unlock()
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			a.mu.Lock()
			a.fact.RequestWritten = info.Err == nil
			a.fact.RequestMillis = 0
			if info.Err == nil {
				a.fact.RequestMillis = max(int64(0), t.now().Sub(a.started).Milliseconds())
			}
			a.mu.Unlock()
		},
		GotFirstResponseByte: func() {
			a.mu.Lock()
			a.fact.FirstResponseByte = true
			a.fact.FirstByteMillis = max(int64(0), t.now().Sub(a.started).Milliseconds())
			a.mu.Unlock()
		},
	}
	ctx = context.WithValue(ctx, logHTTPAttemptKey{}, a)
	body, err := open(httptrace.WithClientTrace(ctx, trace))
	openTime := max(time.Duration(0), t.now().Sub(started))
	a.mu.Lock()
	a.fact.OpenMillis = openTime.Milliseconds()
	a.fact.StreamOpened = err == nil && body != nil
	a.mu.Unlock()
	t.mu.Lock()
	t.openTime += openTime
	t.mu.Unlock()
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
	started := b.attempt.owner.now()
	n, err := b.ReadCloser.Read(p)
	readTime := max(time.Duration(0), b.attempt.owner.now().Sub(started))
	b.attempt.mu.Lock()
	b.attempt.fact.BodyBytes += int64(n)
	b.attempt.readTime += readTime
	b.attempt.fact.BodyReadError = b.attempt.fact.BodyReadError || err != nil && err != io.EOF
	b.attempt.mu.Unlock()
	b.attempt.owner.mu.Lock()
	b.attempt.owner.readTime += readTime
	b.attempt.owner.bodyBytes += int64(n)
	b.attempt.owner.mu.Unlock()
	if err != nil && err != io.EOF {
		b.attempt.failure()
	}
	return n, err
}
