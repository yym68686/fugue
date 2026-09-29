package main

import (
	o "fugue/internal/staticedgeobserve"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"golang.org/x/net/http2"
	"io"
	"net/http"
	"sync"
	"time"
)

// Embed the existing transport without changing any of its options, pool,
// retry decisions or optional TLS/health-check interfaces.
type ObservedTransport struct{ reverseproxy.HTTPTransport }

func init() { caddy.RegisterModule(ObservedTransport{}) }
func (ObservedTransport) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{ID: "http.reverse_proxy.transport.fugue_observed_http", New: func() caddy.Module { return new(ObservedTransport) }}
}

type attemptContextKey struct{}
type attemptContext struct {
	handler *Observation
	parent  *o.Observer
}

func (t *ObservedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	a, ok := r.Context().Value(attemptContextKey{}).(attemptContext)
	if !ok {
		return t.HTTPTransport.RoundTrip(r)
	}
	obs := a.parent.ForwardAttempt()
	a.handler.registry.Add(obs)
	req := r.Clone(http2.WithFugueSendObserver(obs.Trace(r.Context()), obs))
	req.Body = obs.Body(r.Body)
	req.Header.Del(o.CorrelationHeader)
	if a.handler.ForwardCorrelation {
		parent, err := o.SignParent(o.Parent{RequestID: obs.RequestID(), SpanID: obs.SpanID(), NodeID: a.handler.NodeID, Issued: time.Now().Unix()}, a.handler.key)
		if err == nil {
			req.Header.Set(o.CorrelationHeader, parent)
		}
	}
	resp, err := t.HTTPTransport.RoundTrip(req)
	var once sync.Once
	finish := func() {
		once.Do(func() {
			status := 0
			appID := ""
			if resp != nil {
				status = resp.StatusCode
				appID = resp.Header.Get("X-Request-ID")
			}
			obs.Finish(status, req.Context().Err() != nil, appID)
			a.handler.registry.Remove(obs)
		})
	}
	if err != nil || resp == nil || resp.Body == nil {
		finish()
	} else {
		original := resp.Body
		wrapped := &attemptResponse{ReadCloser: original, finish: finish}
		if duplex, ok := original.(io.ReadWriteCloser); ok {
			resp.Body = &attemptDuplex{attemptResponse: wrapped, Writer: duplex}
		} else {
			resp.Body = wrapped
		}
	}
	return resp, err
}

type attemptResponse struct {
	io.ReadCloser
	finish func()
}
type attemptDuplex struct {
	*attemptResponse
	io.Writer
}

func (r *attemptResponse) Read(p []byte) (int, error) {
	n, e := r.ReadCloser.Read(p)
	if e != nil {
		r.finish()
	}
	return n, e
}
func (r *attemptResponse) Close() error { e := r.ReadCloser.Close(); r.finish(); return e }

// Keep Caddy's ResponseController unwrapping and optional interfaces. The
// Write timer measures an API call, never claims on-wire delivery or CPU time.
type observedWriter struct {
	*caddyhttp.ResponseWriterWrapper
	observer *o.Observer
}

func (w *observedWriter) Write(p []byte) (int, error) {
	start := time.Now()
	n, e := w.ResponseWriter.Write(p)
	w.observer.ResponseWritten(n, time.Since(start))
	return n, e
}
func (w *observedWriter) ReadFrom(r io.Reader) (int64, error) {
	start := time.Now()
	n, e := w.ResponseWriterWrapper.ReadFrom(r)
	w.observer.ResponseCopied(int(n), time.Since(start))
	return n, e
}
func (w *observedWriter) Flush() { _ = w.FlushError() }
func (w *observedWriter) FlushError() error {
	start := time.Now()
	err := http.NewResponseController(w.ResponseWriter).Flush()
	w.observer.ResponseWritten(0, time.Since(start))
	return err
}
