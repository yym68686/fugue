package api

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type serverTimingContextKey struct{}

type serverTimingMetric struct {
	duration time.Duration
	name     string
}

type serverTimingRecorder struct {
	metrics []serverTimingMetric
	mu      sync.Mutex
}

type serverTimingResponseWriter struct {
	http.ResponseWriter
	recorder    *serverTimingRecorder
	wroteHeader bool
}

func newServerTimingRecorder() *serverTimingRecorder {
	return &serverTimingRecorder{}
}

func withServerTiming(r *http.Request) (*http.Request, *serverTimingRecorder) {
	recorder := newServerTimingRecorder()
	ctx := context.WithValue(r.Context(), serverTimingContextKey{}, recorder)
	return r.WithContext(ctx), recorder
}

func serverTimingFromContext(ctx context.Context) *serverTimingRecorder {
	recorder, _ := ctx.Value(serverTimingContextKey{}).(*serverTimingRecorder)
	return recorder
}

func (r *serverTimingRecorder) Add(name string, duration time.Duration) {
	if r == nil {
		return
	}

	normalizedName := normalizeServerTimingMetricName(name)
	if normalizedName == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics = append(r.metrics, serverTimingMetric{
		duration: duration,
		name:     normalizedName,
	})
}

func (r *serverTimingRecorder) headerValue() string {
	if r == nil {
		return ""
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.metrics) == 0 {
		return ""
	}

	parts := make([]string, 0, len(r.metrics))
	for _, metric := range r.metrics {
		parts = append(parts, fmt.Sprintf("%s;dur=%.1f", metric.name, float64(metric.duration)/float64(time.Millisecond)))
	}
	return strings.Join(parts, ", ")
}

func normalizeServerTimingMetricName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	var builder strings.Builder
	builder.Grow(len(value))
	for _, char := range value {
		switch {
		case char >= 'a' && char <= 'z':
			builder.WriteRune(char)
		case char >= 'A' && char <= 'Z':
			builder.WriteRune(char + ('a' - 'A'))
		case char >= '0' && char <= '9':
			builder.WriteRune(char)
		case char == '-' || char == '_':
			builder.WriteRune(char)
		default:
			builder.WriteRune('_')
		}
	}

	return builder.String()
}

func newServerTimingResponseWriter(w http.ResponseWriter, recorder *serverTimingRecorder) *serverTimingResponseWriter {
	return &serverTimingResponseWriter{
		ResponseWriter: w,
		recorder:       recorder,
	}
}

func (w *serverTimingResponseWriter) ensureServerTimingHeader() {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true

	if value := w.recorder.headerValue(); value != "" {
		w.ResponseWriter.Header().Set("Server-Timing", value)
	}
}

func (w *serverTimingResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *serverTimingResponseWriter) WriteHeader(statusCode int) {
	w.ensureServerTimingHeader()
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *serverTimingResponseWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (w *serverTimingResponseWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *serverTimingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

func (w *serverTimingResponseWriter) Push(target string, opts *http.PushOptions) error {
	pusher, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, opts)
}

func (w *serverTimingResponseWriter) ReadFrom(reader io.Reader) (int64, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if readerFrom, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		return readerFrom.ReadFrom(reader)
	}
	return io.Copy(w.ResponseWriter, reader)
}

func (w *serverTimingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
