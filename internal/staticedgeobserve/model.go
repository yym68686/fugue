// Package staticedgeobserve records bounded, payload-free facts. It has no
// dependency on the control plane, route inventory, or business configuration.
package staticedgeobserve

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math"
	"regexp"
	"time"
)

const Schema = "fugue.static-edge.observation/v1"
const MaxRecordBytes = 48 << 10
const MaxEvents = 128

var identifier = regexp.MustCompile(`^[a-zA-Z0-9_.:-]{1,128}$`)

func ID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Never substitute a timestamp, which can silently merge requests.
		return ""
	}
	return hex.EncodeToString(b[:])
}

func ValidID(s string) bool { return identifier.MatchString(s) }

// Event is a state boundary, not raw HTTP material. Begin/end pairs can
// overlap; consumers must not sum them into an end-to-end duration.
type Event struct {
	Kind  string  `json:"kind"`
	AtMS  float64 `json:"at_ms"`
	Bytes int64   `json:"bytes,omitempty"`
	Value int64   `json:"value,omitempty"`
}

var eventKinds = map[string]bool{
	"handler_enter": true, "body_read_begin": true, "body_read_end": true,
	"body_close": true, "response_write_begin": true, "response_write_end": true,
	"get_connection": true, "got_connection": true, "dns_begin": true, "dns_end": true,
	"connect_begin": true, "connect_end": true, "tls_begin": true, "tls_end": true,
	"request_headers_written": true, "request_written": true, "response_first_byte": true,
	"h2_headers": true, "h2_data_received": true, "h2_body_consumed": true,
	"h2_stream_window": true, "h2_connection_window": true,
	"h2_window_wait_begin": true, "h2_window_wait_end": true,
	"h2_data_sent": true, "h2_reset": true, "h2_goaway": true,
	"client_body_produce_begin": true, "client_body_produce_end": true,
	"request_finished": true,
}

type Coverage struct {
	Body          bool `json:"body"`
	HTTPTrace     bool `json:"http_trace"`
	HTTP2Frames   bool `json:"http2_frames"`
	HTTP2SendWait bool `json:"http2_send_wait"`
	HTTP3Frames   bool `json:"http3_frames"`
	Peer          bool `json:"peer"`
	Client        bool `json:"client"`
}

type Body struct {
	Bytes              int64    `json:"bytes"`
	ReadCalls          uint64   `json:"read_calls"`
	ReadBlockMS        float64  `json:"read_block_ms"`
	MaxReadBlockMS     float64  `json:"max_read_block_ms"`
	FirstByteMS        *float64 `json:"first_byte_ms,omitempty"`
	LastByteMS         *float64 `json:"last_byte_ms,omitempty"`
	ReadEndMS          *float64 `json:"read_end_ms,omitempty"`
	EOF                bool     `json:"eof"`
	Error              bool     `json:"error"`
	ReadPendingSinceMS *float64 `json:"read_pending_since_ms,omitempty"`
}

// HTTP2 is a receiver-side snapshot. Credit is local protocol accounting,
// not proof the sender received a WINDOW_UPDATE or had bytes ready to send.
type HTTP2 struct {
	StartedAt           time.Time `json:"started_at"`
	ElapsedMS           float64   `json:"elapsed_ms"`
	DataBytes           int64     `json:"data_bytes"`
	ConsumedBytes       int64     `json:"consumed_bytes"`
	DataFrames          uint64    `json:"data_frames"`
	FirstDataMS         *float64  `json:"first_data_ms,omitempty"`
	LastDataMS          *float64  `json:"last_data_ms,omitempty"`
	MaxUnreadBytes      int64     `json:"max_unread_bytes"`
	StreamCredit        int32     `json:"stream_credit"`
	ConnectionCredit    int32     `json:"connection_credit"`
	WindowUpdatesQueued uint64    `json:"window_updates_queued"`
	Dropped             uint64    `json:"dropped"`
}

type Record struct {
	Schema               string    `json:"schema"`
	NodeID               string    `json:"node_id"`
	ProcessID            string    `json:"process_id"`
	RequestID            string    `json:"request_id"`
	SpanID               string    `json:"span_id"`
	ParentSpanID         string    `json:"parent_span_id,omitempty"`
	ApplicationRequestID string    `json:"application_request_id,omitempty"`
	AttemptID            string    `json:"attempt_id"`
	Hop                  string    `json:"hop"`
	Protocol             string    `json:"protocol"`
	ConnectionID         string    `json:"connection_id,omitempty"`
	StreamID             uint64    `json:"stream_id,omitempty"`
	Build                string    `json:"build"`
	ConfigDigest         string    `json:"config_digest"`
	StartedAt            time.Time `json:"started_at"`
	ObservedAt           time.Time `json:"observed_at"`
	ElapsedMS            float64   `json:"elapsed_ms"`
	Sequence             uint64    `json:"sequence"`
	Finished             bool      `json:"finished"`
	Canceled             bool      `json:"canceled"`
	Status               int       `json:"status,omitempty"`
	Correlation          string    `json:"correlation"`
	Coverage             Coverage  `json:"coverage"`
	HTTP2                *HTTP2    `json:"http2,omitempty"`
	Body                 Body      `json:"body"`
	ResponseBytes        int64     `json:"response_bytes"`
	ResponseWriteMS      float64   `json:"response_write_ms"`
	ResponseCopyMS       float64   `json:"response_copy_ms,omitempty"`
	TCP                  []TCP     `json:"tcp,omitempty"`
	Events               []Event   `json:"events"`
	EventsDropped        uint64    `json:"events_dropped"`
	SnapshotsDropped     uint64    `json:"snapshots_dropped"`
	ExporterDroppedTotal uint64    `json:"exporter_dropped_total,omitempty"`
}
type TCP struct {
	AtMS          float64 `json:"at_ms"`
	Available     bool    `json:"available"`
	Scope         string  `json:"scope"`
	RTTUS         uint32  `json:"rtt_us"`
	RTOUS         uint32  `json:"rto_us"`
	Unacked       uint32  `json:"unacked"`
	TotalRetrans  uint32  `json:"total_retrans"`
	BytesReceived uint64  `json:"bytes_received"`
	BytesAcked    uint64  `json:"bytes_acked"`
	BytesSent     uint64  `json:"bytes_sent"`
}

func (r Record) Validate() error {
	if r.Schema != Schema || !ValidID(r.NodeID) || !ValidID(r.ProcessID) || !ValidID(r.RequestID) || !ValidID(r.SpanID) || !ValidID(r.AttemptID) {
		return errors.New("invalid observation identity")
	}
	for _, v := range []string{r.ParentSpanID, r.ApplicationRequestID, r.ConnectionID} {
		if v != "" && !ValidID(v) {
			return errors.New("invalid optional observation identity")
		}
	}
	if !ValidID(r.Hop) || !ValidID(r.Build) || !ValidID(r.ConfigDigest) || len(r.Events) > MaxEvents {
		return errors.New("invalid observation provenance or event count")
	}
	switch r.Protocol {
	case "HTTP/1.0", "HTTP/1.1", "HTTP/2.0", "HTTP/3.0":
	default:
		return errors.New("unsupported protocol identifier")
	}
	switch r.Correlation {
	case "entry", "authenticated_parent", "local_parent", "untrusted_client", "missing_parent":
	default:
		return errors.New("invalid correlation provenance")
	}
	if r.StartedAt.IsZero() || r.ObservedAt.Before(r.StartedAt) || r.ElapsedMS < 0 || !finite(r.ElapsedMS) || r.Status < 0 || r.Status > 599 || r.Body.Bytes < 0 || r.ResponseBytes < 0 {
		return errors.New("invalid observation boundary")
	}
	for _, v := range []float64{r.Body.ReadBlockMS, r.Body.MaxReadBlockMS, r.ResponseWriteMS, r.ResponseCopyMS} {
		if v < 0 || !finite(v) {
			return errors.New("invalid observation duration")
		}
	}
	if len(r.TCP) > 2 {
		return errors.New("too many connection samples")
	}
	for _, v := range r.TCP {
		if v.Scope != "connection_only" || !finite(v.AtMS) || v.AtMS < 0 || v.AtMS > r.ElapsedMS+1 {
			return errors.New("invalid connection evidence")
		}
	}
	for _, v := range []*float64{r.Body.FirstByteMS, r.Body.LastByteMS, r.Body.ReadEndMS, r.Body.ReadPendingSinceMS} {
		if v != nil && (*v < 0 || !finite(*v) || *v > r.ElapsedMS+1) {
			return errors.New("invalid observation timestamp")
		}
	}
	for _, e := range r.Events {
		if !eventKinds[e.Kind] || e.AtMS < 0 || e.AtMS > r.ElapsedMS+1 || !finite(e.AtMS) || e.Bytes < 0 {
			return errors.New("invalid observation event")
		}
	}
	if h := r.HTTP2; h != nil {
		if h.StartedAt.IsZero() || h.ElapsedMS < 0 || !finite(h.ElapsedMS) || h.DataBytes < 0 || h.ConsumedBytes < 0 || h.MaxUnreadBytes < 0 {
			return errors.New("invalid HTTP/2 metadata")
		}
		for _, v := range []*float64{h.FirstDataMS, h.LastDataMS} {
			if v != nil && (*v < 0 || *v > h.ElapsedMS+1 || !finite(*v)) {
				return errors.New("invalid HTTP/2 data boundary")
			}
		}
	}
	return nil
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

type Finding struct {
	SpanID     string   `json:"span_id"`
	State      string   `json:"state"`
	Mechanism  string   `json:"mechanism"`
	DurationMS float64  `json:"duration_ms,omitempty"`
	Evidence   []string `json:"evidence"`
	Missing    []string `json:"missing,omitempty"`
}

type Query struct {
	RequestID string    `json:"request_id,omitempty"`
	Since     time.Time `json:"since"`
	Until     time.Time `json:"until"`
	MinReadMS float64   `json:"min_read_ms,omitempty"`
	Limit     int       `json:"limit"`
}

func (q Query) Validate() error {
	if q.RequestID != "" && !ValidID(q.RequestID) {
		return errors.New("invalid lookup id")
	}
	if q.Since.IsZero() || q.Until.Before(q.Since) || q.Until.Sub(q.Since) > 7*24*time.Hour || q.Limit < 1 || q.Limit > 200 || q.MinReadMS < 0 || !finite(q.MinReadMS) {
		return errors.New("bounded query requires up to seven days and 1..200 records")
	}
	return nil
}

type Result struct {
	Schema           string     `json:"schema"`
	NodeID           string     `json:"node_id"`
	Records          []Record   `json:"records"`
	Findings         []Finding  `json:"findings"`
	Truncated        bool       `json:"truncated"`
	RecordsDropped   uint64     `json:"records_dropped"`
	RetentionSeconds int64      `json:"retention_seconds"`
	OldestRetained   *time.Time `json:"oldest_retained,omitempty"`
	Status           string     `json:"status"`
	MemoryEvictions  uint64     `json:"memory_evictions"`
	DiskErrors       uint64     `json:"disk_errors"`
	DiskScanComplete bool       `json:"disk_scan_complete"`
}

// Explain never infers client inactivity, CPU, a network hop or a software bug
// from a Read duration. The mechanism boundary and ultimate cause are distinct.
func Explain(r Record) []Finding {
	f := Finding{SpanID: r.SpanID, State: "bounded", Mechanism: "application_body_read_wait", DurationMS: r.Body.ReadBlockMS, Evidence: []string{"elapsed time inside request Body.Read calls"}, Missing: []string{"client production and sender state", "peer receive/consume stages", "scheduler attribution"}}
	if !r.Coverage.Body {
		f.State = "unknown"
		f.Mechanism = "body_not_observed"
		f.DurationMS = 0
	}
	if r.EventsDropped+r.SnapshotsDropped > 0 {
		f.Missing = append(f.Missing, "observation events were dropped")
	}
	if !r.Finished {
		f.Missing = append(f.Missing, "request has no observed terminal yet")
	}
	if !r.Coverage.HTTP2Frames && r.Protocol == "HTTP/2.0" {
		f.Missing = append(f.Missing, "HTTP/2 per-stream frame and window evidence")
	}
	if !r.Coverage.HTTP3Frames && r.Protocol == "HTTP/3.0" {
		f.Missing = append(f.Missing, "HTTP/3 per-stream frame and window evidence")
	}
	out := []Finding{f}
	// A complete explicit protocol wait pair proves that mechanism, not why the
	// peer withheld credit. Lost events invalidate the pair's completeness.
	var begin *Event
	for _, e := range r.Events {
		if e.Kind == "h2_window_wait_begin" {
			v := e
			begin = &v
		}
		if e.Kind == "h2_window_wait_end" && begin != nil {
			if r.Coverage.HTTP2SendWait && r.EventsDropped == 0 && e.Bytes == begin.Bytes && e.AtMS >= begin.AtMS {
				out = append(out, Finding{SpanID: r.SpanID, State: "confirmed", Mechanism: "http2_sender_waited_for_credit", DurationMS: e.AtMS - begin.AtMS, Evidence: []string{"protocol sender had pending DATA and waited for window credit"}, Missing: []string{"reason for peer withholding credit"}})
			}
			begin = nil
		}
	}
	return out
}
