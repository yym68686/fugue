package runtimeobservation

import (
	"context"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

type HTTPRequestClass struct {
	Client        string `json:"client"`
	Method        string `json:"method"`
	Resource      string `json:"resource"`
	Namespaced    bool   `json:"namespaced"`
	SingleObject  bool   `json:"single_object"`
	Watch         bool   `json:"watch"`
	CacheMode     string `json:"cache_mode"`
	LabelSelector bool   `json:"label_selector"`
	FieldSelector bool   `json:"field_selector"`
}

type HTTPRequestFact struct {
	HTTPRequestClass
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	StatusCode    int       `json:"status_code"`
	HeaderMillis  float64   `json:"header_ms"`
	TotalMillis   float64   `json:"total_ms"`
	BytesRead     int64     `json:"bytes_read"`
	ReadError     bool      `json:"read_error"`
	CompletedBody bool      `json:"completed_body"`
}

// HTTPRecorder stores bounded facts, never headers, URLs, queries or bodies.
// Classifiers must return low-cardinality metadata suitable for observation.
type HTTPRecorder struct {
	mu        sync.Mutex
	rows      []HTTPRequestFact
	next      int
	completed uint64
	inFlight  int64
}

func (r *HTTPRecorder) Wrap(base http.RoundTripper, classify func(*http.Request) HTTPRequestClass) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &observedTransport{base: base, recorder: r, classify: classify}
}

func (r *HTTPRecorder) Snapshot(ctx context.Context) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	rows := append([]HTTPRequestFact(nil), r.rows...)
	count, inFlight := r.completed, r.inFlight
	r.mu.Unlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].StartedAt.Before(rows[j].StartedAt) })
	return map[string]any{"schema": "fugue.http-client.observation.v1", "observed_at": time.Now().UTC(), "requests": rows, "completed_total": count, "in_flight": inFlight, "retained_limit": 2048, "overwritten": count > uint64(len(rows)), "note": "request duration includes caller body consumption; watch duration includes stream lifetime"}, nil
}

type observedTransport struct {
	base     http.RoundTripper
	recorder *HTTPRecorder
	classify func(*http.Request) HTTPRequestClass
}

func (t *observedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	started := time.Now()
	fact := HTTPRequestFact{HTTPRequestClass: t.classify(req), StartedAt: started.UTC()}
	r := t.recorder
	r.mu.Lock()
	r.inFlight++
	r.mu.Unlock()
	resp, err := t.base.RoundTrip(req)
	fact.HeaderMillis = float64(time.Since(started)) / float64(time.Millisecond)
	if err != nil || resp == nil {
		fact.ReadError = true
		r.finish(fact, started)
		return resp, err
	}
	fact.StatusCode = resp.StatusCode
	if resp.Body == nil || resp.Body == http.NoBody {
		fact.CompletedBody = true
		r.finish(fact, started)
		return resp, nil
	}
	resp.Body = &observedBody{ReadCloser: resp.Body, recorder: r, fact: fact, started: started}
	return resp, nil
}

func (t *observedTransport) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

func (r *HTTPRecorder) finish(fact HTTPRequestFact, started time.Time) {
	fact.FinishedAt = time.Now().UTC()
	fact.TotalMillis = float64(time.Since(started)) / float64(time.Millisecond)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inFlight--
	r.completed++
	if len(r.rows) < 2048 {
		r.rows = append(r.rows, fact)
	} else {
		r.rows[r.next] = fact
		r.next = (r.next + 1) % 2048
	}
}

type observedBody struct {
	io.ReadCloser
	mu       sync.Mutex
	recorder *HTTPRecorder
	fact     HTTPRequestFact
	started  time.Time
	finished bool
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.finished {
		b.fact.BytesRead += int64(n)
		if err != nil {
			b.fact.CompletedBody = err == io.EOF
			b.fact.ReadError = err != io.EOF
			b.finish()
		}
	}
	return n, err
}

func (b *observedBody) Close() error {
	err := b.ReadCloser.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.finished {
		b.fact.ReadError = b.fact.ReadError || err != nil
		b.finish()
	}
	return err
}

func (b *observedBody) finish() { b.finished = true; b.recorder.finish(b.fact, b.started) }
