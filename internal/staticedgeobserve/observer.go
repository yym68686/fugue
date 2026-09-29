package staticedgeobserve

import (
	"context"
	"crypto/tls"
	"fugue/internal/tcpdiag"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"
)

type Sink interface{ Submit(Record) bool }
type Observer struct {
	mu                                      sync.Mutex
	r                                       Record
	start                                   time.Time
	sink                                    Sink
	dropped                                 atomic.Uint64
	protocolSnapshot                        func() (*HTTP2, bool)
	connection                              net.Conn
	firstByte, lastByte, readEnd, pendingAt float64
}

func New(r Record, sink Sink) *Observer {
	now := time.Now()
	r.Schema = Schema
	r.StartedAt = now.UTC()
	r.ObservedAt = r.StartedAt
	r.SpanID = ID()
	r.AttemptID = ID()
	r.Events = make([]Event, 0, MaxEvents)
	return &Observer{r: r, start: now, sink: sink}
}

func (o *Observer) SpanID() string    { return o.r.SpanID }
func (o *Observer) RequestID() string { return o.r.RequestID }

// ForwardAttempt creates a separate monotonic clock and identity for each
// RoundTrip. A retry must never overwrite or extend a previous attempt.
func (o *Observer) ForwardAttempt() *Observer {
	// Read only immutable identity fields; other fields change concurrently.
	return New(Record{NodeID: o.r.NodeID, ProcessID: o.r.ProcessID, RequestID: o.r.RequestID,
		ParentSpanID: o.r.SpanID, Hop: "forward_attempt", Protocol: "HTTP/1.1",
		Build: o.r.Build, ConfigDigest: o.r.ConfigDigest, Correlation: "local_parent"}, o.sink)
}
func (o *Observer) at() float64 { return float64(time.Since(o.start)) / float64(time.Millisecond) }
func (o *Observer) update(fn func(*Record)) {
	if !o.mu.TryLock() {
		o.dropped.Add(1)
		return
	}
	defer o.mu.Unlock()
	fn(&o.r)
}
func (o *Observer) event(r *Record, kind string, bytes, value int64) {
	if len(r.Events) == MaxEvents {
		r.EventsDropped++
		return
	}
	r.Events = append(r.Events, Event{Kind: kind, AtMS: o.at(), Bytes: bytes, Value: value})
}
func (o *Observer) Event(kind string, bytes, value int64) {
	if !eventKinds[kind] {
		return
	}
	o.update(func(r *Record) { o.event(r, kind, bytes, value) })
}
func (o *Observer) Snapshot() (Record, bool) {
	var h2 *HTTP2
	if o.protocolSnapshot != nil {
		if v, ok := o.protocolSnapshot(); ok {
			h2 = v
		} else {
			o.dropped.Add(1)
		}
	}
	if !o.mu.TryLock() {
		o.dropped.Add(1)
		return Record{}, false
	}
	defer o.mu.Unlock()
	o.r.Sequence++
	o.r.ElapsedMS = o.at()
	o.r.ObservedAt = time.Now().UTC()
	o.r.EventsDropped += o.dropped.Swap(0)
	r := o.r
	// Snapshot numbers own their storage; per-Read updates reuse the observer's
	// slots instead of allocating two objects for every received chunk.
	r.Body.FirstByteMS = copyNumber(r.Body.FirstByteMS)
	r.Body.LastByteMS = copyNumber(r.Body.LastByteMS)
	r.Body.ReadEndMS = copyNumber(r.Body.ReadEndMS)
	r.Body.ReadPendingSinceMS = copyNumber(r.Body.ReadPendingSinceMS)
	r.HTTP2 = h2
	r.Events = append([]Event(nil), o.r.Events...)
	r.TCP = append([]TCP(nil), o.r.TCP...)
	return r, true
}
func copyNumber(v *float64) *float64 {
	if v == nil {
		return nil
	}
	n := *v
	return &n
}

// SetProtocolSnapshot is called before the observer is registered or used.
func (o *Observer) SetProtocolSnapshot(fn func() (*HTTP2, bool)) { o.protocolSnapshot = fn }

func (o *Observer) HTTP2SendWait(begin, granted bool, stream uint32, streamCredit, connectionCredit int32) {
	kind := "h2_window_wait_begin"
	if !begin {
		if granted {
			kind = "h2_window_wait_end"
		} else {
			return
		}
	}
	o.update(func(r *Record) {
		r.Coverage.HTTP2SendWait = true
		o.event(r, kind, int64(stream), int64(streamCredit))
		o.event(r, "h2_connection_window", 0, int64(connectionCredit))
	})
}
func (o *Observer) Emit() {
	r, ok := o.Snapshot()
	if !ok || o.sink == nil {
		return
	}
	if !o.sink.Submit(r) {
		o.update(func(r *Record) { r.SnapshotsDropped++ })
	}
}
func (o *Observer) Finish(status int, canceled bool, appID string) {
	// At most two user-space TCP_INFO reads per span; no probe, packet
	// capture, addresses, or stream-level retransmission attribution.
	o.mu.Lock()
	conn := o.connection
	o.mu.Unlock()
	if conn != nil {
		o.sampleTCP(conn)
	}
	o.update(func(r *Record) {
		r.Finished = true
		r.Status = status
		r.Canceled = canceled
		if ValidID(appID) {
			r.ApplicationRequestID = appID
		}
		o.event(r, "request_finished", 0, 0)
	})
	o.Emit()
}
func (o *Observer) Body(body io.ReadCloser) io.ReadCloser {
	if body == nil || body == http.NoBody {
		return body
	}
	o.update(func(r *Record) { r.Coverage.Body = true })
	return &observedBody{ReadCloser: body, o: o}
}

type observedBody struct {
	io.ReadCloser
	o *Observer
}

func (b *observedBody) Read(p []byte) (int, error) {
	start := time.Now()
	at := b.o.at()
	b.o.update(func(r *Record) { b.o.pendingAt = at; r.Body.ReadPendingSinceMS = &b.o.pendingAt })
	n, err := b.ReadCloser.Read(p)
	end := b.o.at()
	elapsed := float64(time.Since(start)) / float64(time.Millisecond)
	b.o.update(func(r *Record) {
		r.Body.ReadCalls++
		r.Body.ReadBlockMS += elapsed
		if elapsed > r.Body.MaxReadBlockMS {
			r.Body.MaxReadBlockMS = elapsed
		}
		r.Body.ReadPendingSinceMS = nil
		if n > 0 {
			r.Body.Bytes += int64(n)
			if r.Body.FirstByteMS == nil {
				b.o.firstByte = end
				r.Body.FirstByteMS = &b.o.firstByte
			}
			b.o.lastByte = end
			r.Body.LastByteMS = &b.o.lastByte
		}
		if err != nil {
			b.o.readEnd = end
			r.Body.ReadEndMS = &b.o.readEnd
			r.Body.EOF = err == io.EOF
			r.Body.Error = err != io.EOF
		}
		// Counters cover every Read. Retain individual intervals only for slow
		// calls, leaving space for protocol and lifecycle evidence on large bodies.
		if elapsed >= 50 && len(r.Events) < MaxEvents-32 {
			r.Events = append(r.Events, Event{Kind: "body_read_begin", AtMS: at}, Event{Kind: "body_read_end", AtMS: end, Bytes: int64(n)})
		}
	})
	return n, err
}
func (b *observedBody) Close() error {
	err := b.ReadCloser.Close()
	b.o.Event("body_close", 0, 0)
	return err
}

// Trace hooks compose with any existing httptrace hooks through Go's context
// API. They record transitions, never alter transport options or deadlines.
func (o *Observer) Trace(ctx context.Context) context.Context {
	o.update(func(r *Record) { r.Coverage.HTTPTrace = true })
	t := &httptrace.ClientTrace{
		GetConn: func(string) { o.Event("get_connection", 0, 0) },
		GotConn: func(info httptrace.GotConnInfo) {
			o.ObserveConnection(info.Conn)
			o.update(func(r *Record) {
				var reused int64
				if info.Reused {
					reused = 1
				}
				o.event(r, "got_connection", 0, reused)
				if c, ok := info.Conn.(*tls.Conn); ok && c.ConnectionState().NegotiatedProtocol == "h2" {
					r.Protocol = "HTTP/2.0"
				}
			})
		},
		DNSStart:             func(httptrace.DNSStartInfo) { o.Event("dns_begin", 0, 0) },
		DNSDone:              func(httptrace.DNSDoneInfo) { o.Event("dns_end", 0, 0) },
		ConnectStart:         func(string, string) { o.Event("connect_begin", 0, 0) },
		ConnectDone:          func(string, string, error) { o.Event("connect_end", 0, 0) },
		TLSHandshakeStart:    func() { o.Event("tls_begin", 0, 0) },
		TLSHandshakeDone:     func(tls.ConnectionState, error) { o.Event("tls_end", 0, 0) },
		WroteHeaders:         func() { o.Event("request_headers_written", 0, 0) },
		WroteRequest:         func(httptrace.WroteRequestInfo) { o.Event("request_written", 0, 0) },
		GotFirstResponseByte: func() { o.Event("response_first_byte", 0, 0) },
	}
	return httptrace.WithClientTrace(ctx, t)
}

// ResponseRecorder is deliberately not an http.ResponseWriter wrapper: callers
// use their server's established optional-interface-preserving recorder.
func (o *Observer) ResponseWritten(n int, d time.Duration) {
	o.update(func(r *Record) {
		if n > 0 {
			r.ResponseBytes += int64(n)
		}
		r.ResponseWriteMS += float64(d) / float64(time.Millisecond)
	})
}
func (o *Observer) ResponseCopied(n int, d time.Duration) {
	o.update(func(r *Record) {
		if n > 0 {
			r.ResponseBytes += int64(n)
		}
		r.ResponseCopyMS += float64(d) / float64(time.Millisecond)
	})
}
func (o *Observer) ObserveConnection(conn net.Conn) {
	if tlsConn, ok := conn.(*tls.Conn); ok {
		conn = tlsConn.NetConn()
	}
	o.update(func(r *Record) { o.connection = conn })
	o.sampleTCP(conn)
}
func (o *Observer) sampleTCP(conn net.Conn) {
	v := tcpdiag.SnapshotFromConn(conn)
	o.update(func(r *Record) {
		if len(r.TCP) >= 2 {
			return
		}
		r.TCP = append(r.TCP, TCP{AtMS: o.at(), Available: v.Available, Scope: "connection_only", RTTUS: v.RTTUsec, RTOUS: v.RTOUsec, Unacked: v.Unacked, TotalRetrans: v.TotalRetrans, BytesReceived: v.BytesReceived, BytesAcked: v.BytesAcked, BytesSent: v.BytesSent})
	})
}

func (o *Observer) AttachProtocol(conn string, stream uint64, frames, sendWait bool) {
	o.update(func(r *Record) {
		if ValidID(conn) {
			r.ConnectionID = conn
		}
		r.StreamID = stream
		r.Coverage.HTTP2Frames = frames
		r.Coverage.HTTP2SendWait = sendWait
	})
}

// CloneRequest applies body and transport observations without eager reading.
func (o *Observer) CloneRequest(r *http.Request) *http.Request {
	out := r.Clone(r.Context())
	out.Body = o.Body(r.Body)
	o.Event("handler_enter", 0, 0)
	return out
}
