package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	o "fugue/internal/staticedgeobserve"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"golang.org/x/net/http2"
)

var processID = o.ID()

type ingressConnectionKey struct{}

func init() { caddy.RegisterModule(Observation{}) }

// Observation is a pass-through handler. The dedicated collector socket and
// correlation key are local resources independent of the Fugue API.
type Observation struct {
	NodeID             string   `json:"node_id"`
	Hop                string   `json:"hop"`
	Build              string   `json:"build"`
	ConfigDigest       string   `json:"config_digest"`
	Socket             string   `json:"socket"`
	CorrelationKeyFile string   `json:"correlation_key_file"`
	Entry              bool     `json:"entry"`
	TrustLoopback      bool     `json:"trust_loopback,omitempty"`
	ClientFingerprints []string `json:"client_fingerprints,omitempty"`
	Capacity           int      `json:"capacity,omitempty"`
	ForwardCorrelation bool     `json:"forward_correlation,omitempty"`
	key                []byte
	workMu             sync.Mutex
	workers            *observationWorkers
	retiringWorkers    *observationWorkers
	active             int
	retired            bool
	slots              chan struct{}
}

func (Observation) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{ID: "http.handlers.fugue_observation", New: func() caddy.Module { return new(Observation) }}
}
func (h *Observation) Provision(ctx caddy.Context) error {
	if !o.ValidID(h.NodeID) || !o.ValidID(h.Hop) || !o.ValidID(h.Build) || !o.ValidID(h.ConfigDigest) || processID == "" {
		return errors.New("valid observation identity and provenance required")
	}
	if h.Capacity < 0 || h.Capacity > 2048 {
		return errors.New("observation capacity exceeds bound")
	}
	f, e := os.Open(h.CorrelationKeyFile)
	if e != nil {
		return errors.New("correlation key unavailable")
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0007 != 0 {
		return errors.New("correlation key must be a restricted regular file")
	}
	h.key, e = io.ReadAll(io.LimitReader(f, 65))
	if e != nil || len(h.key) != 32 {
		return errors.New("correlation key must contain 32 bytes")
	}
	for _, fp := range h.ClientFingerprints {
		v, e := hex.DecodeString(fp)
		if e != nil || len(v) != 32 {
			return errors.New("invalid authorized peer fingerprint")
		}
	}
	h.workers, e = h.newWorkers()
	if e != nil {
		return e
	}
	capacity := h.Capacity
	if capacity == 0 {
		capacity = 256
	}
	h.slots = make(chan struct{}, capacity)
	if server, ok := ctx.Value(caddyhttp.ServerCtxKey).(*caddyhttp.Server); ok {
		server.RegisterConnContext(func(ctx context.Context, conn net.Conn) context.Context {
			return context.WithValue(ctx, ingressConnectionKey{}, conn)
		})
	}
	return nil
}
func (h *Observation) Cleanup() error {
	h.workMu.Lock()
	h.retired = true
	w := h.workers
	retire := h.active == 0 && w != nil
	if retire {
		h.workers = nil
		h.retiringWorkers = w
	}
	h.workMu.Unlock()
	if retire {
		w.drain()
	}
	return nil
}
func (h *Observation) authorized(r *http.Request) bool {
	if h.TrustLoopback {
		host, _, e := net.SplitHostPort(r.RemoteAddr)
		if e == nil && net.ParseIP(host).IsLoopback() {
			return true
		}
	}
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		return false
	}
	sum := sha256.Sum256(r.TLS.PeerCertificates[0].Raw)
	fp := hex.EncodeToString(sum[:])
	for _, allowed := range h.ClientFingerprints {
		if allowed == fp {
			return true
		}
	}
	return false
}

func (h *Observation) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	workers, err := h.acquireWorkers()
	if err != nil {
		clone := r.Clone(r.Context())
		clone.Header.Del(o.CorrelationHeader)
		return next.ServeHTTP(w, clone)
	}
	defer h.releaseWorkers(workers)
	// Bound request observation memory before allocating event buffers. The
	// telemetry capacity is not a business concurrency or admission limit.
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		workers.sink.Drop()
		clone := r.Clone(r.Context())
		clone.Header.Del(o.CorrelationHeader)
		return next.ServeHTTP(w, clone)
	}
	rec := o.Record{NodeID: h.NodeID, ProcessID: processID, RequestID: o.ID(), Hop: h.Hop, Protocol: r.Proto, Build: h.Build, ConfigDigest: h.ConfigDigest, Correlation: "entry"}
	if !h.Entry {
		p, e := o.VerifyParent(r.Header.Get(o.CorrelationHeader), h.key, h.authorized(r), time.Now())
		if e == nil {
			rec.RequestID = p.RequestID
			rec.ParentSpanID = p.SpanID
			rec.Correlation = "authenticated_parent"
			rec.Coverage.Peer = true
		} else {
			rec.Correlation = "missing_parent"
		}
	}
	// Failure to create an identity loses observation, never rejects business.
	if rec.RequestID == "" {
		return next.ServeHTTP(w, r)
	}
	obs := o.New(rec, workers.sink)
	if conn, ok := r.Context().Value(ingressConnectionKey{}).(net.Conn); ok {
		obs.ObserveConnection(conn)
	}
	if protocol := http2.FugueRequestObservation(r.Context()); protocol != nil {
		if p, ok := protocol.Snapshot(); ok {
			obs.AttachProtocol("h2-"+strconv.FormatUint(p.ConnectionID, 10), uint64(p.StreamID), true, false)
		}
		obs.SetProtocolSnapshot(func() (*o.HTTP2, bool) {
			p, ok := protocol.Snapshot()
			if !ok {
				return nil, false
			}
			return &o.HTTP2{StartedAt: p.StartedAt, ElapsedMS: p.ElapsedMS, DataBytes: p.DataBytes, ConsumedBytes: p.ConsumedBytes, DataFrames: p.DataFrames, FirstDataMS: p.FirstDataMS, LastDataMS: p.LastDataMS, MaxUnreadBytes: p.MaxUnreadBytes, StreamCredit: p.StreamCredit, ConnectionCredit: p.ConnectionCredit, WindowUpdatesQueued: p.WindowUpdatesQueued, Dropped: p.Dropped}, true
		})
	}
	workers.registry.Add(obs)
	defer workers.registry.Remove(obs)
	r = obs.CloneRequest(r)
	r = r.WithContext(context.WithValue(r.Context(), attemptContextKey{}, attemptContext{handler: h, workers: workers, parent: obs}))
	r.Header.Del(o.CorrelationHeader)
	if h.Entry {
		w.Header().Set("X-Fugue-Observation-ID", obs.RequestID())
	}
	rr := caddyhttp.NewResponseRecorder(&observedWriter{ResponseWriterWrapper: &caddyhttp.ResponseWriterWrapper{ResponseWriter: w}, observer: obs}, nil, nil)
	err = next.ServeHTTP(rr, r)
	status := rr.Status()
	if status == 0 && err == nil {
		status = 200
	}
	obs.Finish(status, r.Context().Err() != nil, rr.Header().Get("X-Request-ID"))
	return err
}

var _ caddy.Provisioner = (*Observation)(nil)
var _ caddy.CleanerUpper = (*Observation)(nil)
var _ caddyhttp.MiddlewareHandler = (*Observation)(nil)
