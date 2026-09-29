// Package staticedgeclient offers opt-in metadata-only client instrumentation.
// Records remain client-reported evidence, not trusted server observations.
// It never sends telemetry over the network or reads a body ahead of transport.
package staticedgeclient

import (
	o "fugue/internal/staticedgeobserve"
	"io"
	"net/http"
	"sync"
)

type Record = o.Record
type Evidence struct {
	Observation     Record `json:"observation"`
	ServerRequestID string `json:"server_request_id,omitempty"`
	Provenance      string `json:"provenance"`
}

// Transport requires a bounded non-blocking Submit function. Use a select with
// a default branch; callers retain ownership of destination and retention.
type Transport struct {
	Base   http.RoundTripper
	Submit func(Evidence) bool
}
type sink struct {
	submit   func(Evidence) bool
	serverID string
}

func (s *sink) Submit(r Record) bool {
	return s.submit(Evidence{r, s.serverID, "client_reported_unattested"})
}

var process = o.ID()

func (t *Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	if t.Submit == nil {
		return base.RoundTrip(r)
	}
	s := &sink{submit: t.Submit}
	obs := o.New(Record{NodeID: "client", ProcessID: process, RequestID: o.ID(), Hop: "client_send", Protocol: "HTTP/1.1", Build: "client-v1", ConfigDigest: "local", Correlation: "untrusted_client", Coverage: o.Coverage{Client: true}}, s)
	req := r.Clone(obs.Trace(r.Context()))
	req.Body = obs.Body(r.Body)
	resp, e := base.RoundTrip(req)
	if resp != nil && o.ValidID(resp.Header.Get("X-Fugue-Observation-ID")) {
		s.serverID = resp.Header.Get("X-Fugue-Observation-ID")
	}
	var once sync.Once
	finish := func() {
		once.Do(func() {
			status := 0
			if resp != nil {
				status = resp.StatusCode
			}
			obs.Finish(status, r.Context().Err() != nil, "")
		})
	}
	if e != nil || resp == nil || resp.Body == nil {
		finish()
	} else {
		original := resp.Body
		wrapped := &responseBody{ReadCloser: original, finish: finish}
		if duplex, ok := original.(io.ReadWriteCloser); ok {
			resp.Body = &duplexBody{responseBody: wrapped, Writer: duplex}
		} else {
			resp.Body = wrapped
		}
	}
	return resp, e
}

type responseBody struct {
	io.ReadCloser
	finish func()
}
type duplexBody struct {
	*responseBody
	io.Writer
}

func (b *responseBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if e != nil {
		b.finish()
	}
	return n, e
}
func (b *responseBody) Close() error { e := b.ReadCloser.Close(); b.finish(); return e }

// Queue is a ready-to-use bounded sink; consumers export or store independently.
type Queue struct {
	C       chan Evidence
	mu      sync.Mutex
	dropped uint64
}

func NewQueue(size int) *Queue {
	if size < 1 || size > 256 {
		size = 64
	}
	return &Queue{C: make(chan Evidence, size)}
}
func (q *Queue) Submit(e Evidence) bool {
	select {
	case q.C <- e:
		return true
	default:
		q.mu.Lock()
		q.dropped++
		q.mu.Unlock()
		return false
	}
}
func (q *Queue) Dropped() uint64 { q.mu.Lock(); defer q.mu.Unlock(); return q.dropped }
