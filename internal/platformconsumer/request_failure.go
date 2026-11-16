package platformconsumer

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http/httptrace"
	"sync"
	"time"
)

// RequestFailureObservation retains only fixed classes and transport milestones.
// It never retains callback arguments, URLs, credentials, response bodies or the
// original error. Consumers may copy it into their bounded private diagnostics.
type RequestFailureObservation struct {
	StartedAt         time.Time `json:"started_at"`
	Operation         string    `json:"operation"`
	Phase             string    `json:"phase"`
	Class             string    `json:"class"`
	HTTPStatus        int       `json:"http_status,omitempty"`
	DurationMS        int64     `json:"duration_ms"`
	DNSStarted        bool      `json:"dns_started"`
	DNSCompleted      bool      `json:"dns_completed"`
	ConnectStarted    bool      `json:"connect_started"`
	ConnectCompleted  bool      `json:"connect_completed"`
	TLSStarted        bool      `json:"tls_started"`
	TLSCompleted      bool      `json:"tls_completed"`
	ConnectionReady   bool      `json:"connection_ready"`
	ConnectionReused  bool      `json:"connection_reused"`
	RequestWritten    bool      `json:"request_written"`
	FirstResponseByte bool      `json:"first_response_byte"`
}

type requestFailureError struct{ observation RequestFailureObservation }

func (e *requestFailureError) Error() string { return ErrUnavailable.Error() }
func (e *requestFailureError) Unwrap() error { return ErrUnavailable }

// RequestFailure returns an independent copy without changing outage semantics.
func RequestFailure(err error) *RequestFailureObservation {
	var transport *requestFailureError
	if errors.As(err, &transport) {
		copy := transport.observation
		return &copy
	}
	var status *responseStatusError
	if errors.As(err, &status) && !status.observation.StartedAt.IsZero() {
		copy := status.observation
		return &copy
	}
	return nil
}

type requestFailureTrace struct {
	mu          sync.Mutex
	finished    bool
	started     time.Time
	observation RequestFailureObservation
}

func newRequestFailureTrace(path string) *requestFailureTrace {
	operation := "other"
	switch path {
	case "/v1/platform-state/consumers/identity":
		operation = "identity"
	case "/v1/platform-state/consumers/assignment":
		operation = "assignment"
	case "/v1/platform-state/consumers/trusted-heartbeat":
		operation = "trusted_heartbeat"
	case "/v1/platform-state/consumers/shadow-heartbeat":
		operation = "shadow_heartbeat"
	}
	now := time.Now()
	return &requestFailureTrace{started: now, observation: RequestFailureObservation{StartedAt: now.UTC(), Operation: operation}}
}

func (t *requestFailureTrace) update(f func(*RequestFailureObservation)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.finished {
		f(&t.observation)
	}
}

func (t *requestFailureTrace) trace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		DNSStart:     func(httptrace.DNSStartInfo) { t.update(func(o *RequestFailureObservation) { o.DNSStarted = true }) },
		DNSDone:      func(httptrace.DNSDoneInfo) { t.update(func(o *RequestFailureObservation) { o.DNSCompleted = true }) },
		ConnectStart: func(string, string) { t.update(func(o *RequestFailureObservation) { o.ConnectStarted = true }) },
		ConnectDone: func(string, string, error) {
			t.update(func(o *RequestFailureObservation) { o.ConnectCompleted = true })
		},
		TLSHandshakeStart: func() { t.update(func(o *RequestFailureObservation) { o.TLSStarted = true }) },
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			t.update(func(o *RequestFailureObservation) { o.TLSCompleted = true })
		},
		GotConn: func(info httptrace.GotConnInfo) {
			t.update(func(o *RequestFailureObservation) {
				o.ConnectionReady, o.ConnectionReused = true, info.Reused
				o.RequestWritten, o.FirstResponseByte = false, false
			})
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			t.update(func(o *RequestFailureObservation) { o.RequestWritten = info.Err == nil })
		},
		GotFirstResponseByte: func() { t.update(func(o *RequestFailureObservation) { o.FirstResponseByte = true }) },
	}
}

func (t *requestFailureTrace) finish(phase string, status int, err error) RequestFailureObservation {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return t.observation
	}
	t.finished = true
	o := t.observation
	o.Phase, o.HTTPStatus, o.DurationMS = phase, status, time.Since(t.started).Milliseconds()
	o.Class = "transport"
	var dns *net.DNSError
	var network net.Error
	switch {
	case status != 0 && phase == "status":
		o.Class = "http_status"
	case errors.Is(err, context.Canceled):
		o.Class = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		o.Class = "deadline"
	case errors.As(err, &dns):
		o.Class = "dns"
	case errors.As(err, &network) && network.Timeout():
		o.Class = "deadline"
	case phase == "decode":
		o.Class = "body_read"
	}
	t.observation = o
	return o
}
