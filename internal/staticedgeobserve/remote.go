package staticedgeobserve

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// RemoteConfig is optional. A separate worker exports scalar summaries into
// the existing Fugue structured telemetry receiver (or a compatible sink).
// TLS authentication and node authorization belong to that receiver. An API
// outage cannot invalidate locally retained observations or serving config.
type RemoteConfig struct {
	URL             string `json:"url"`
	CAFile          string `json:"ca_file,omitempty"`
	CertificateFile string `json:"certificate_file,omitempty"`
	KeyFile         string `json:"key_file,omitempty"`
	TokenFile       string `json:"token_file,omitempty"`
}
type RemoteSink struct {
	queue                 chan Record
	client                *http.Client
	url, token            string
	dropped, failed, sent atomic.Uint64
}

func NewRemoteSink(c RemoteConfig) (*RemoteSink, error) {
	u, e := url.Parse(c.URL)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("telemetry destination requires a credential-free HTTPS URL")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		pem, e := os.ReadFile(c.CAFile)
		if e != nil {
			return nil, errors.New("telemetry CA unavailable")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("invalid telemetry CA")
		}
		tlsConfig.RootCAs = pool
	}
	if c.CertificateFile != "" || c.KeyFile != "" {
		pair, e := tls.LoadX509KeyPair(c.CertificateFile, c.KeyFile)
		if e != nil {
			return nil, errors.New("telemetry client identity unavailable")
		}
		tlsConfig.Certificates = []tls.Certificate{pair}
	}
	s := &RemoteSink{queue: make(chan Record, 64), url: c.URL}
	if c.TokenFile != "" {
		f, e := os.Open(c.TokenFile)
		if e != nil {
			return nil, errors.New("telemetry credential unavailable")
		}
		defer f.Close()
		info, e := f.Stat()
		if e != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return nil, errors.New("telemetry credential must be private")
		}
		raw, e := io.ReadAll(io.LimitReader(f, 4097))
		if e != nil || len(raw) > 4096 {
			return nil, errors.New("invalid telemetry credential")
		}
		s.token = strings.TrimSpace(string(raw))
		if strings.ContainsAny(s.token, "\r\n") {
			return nil, errors.New("invalid telemetry credential")
		}
	}
	if len(tlsConfig.Certificates) == 0 && s.token == "" {
		return nil, errors.New("explicit telemetry authentication required")
	}
	s.client = &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: tlsConfig, MaxIdleConns: 1, MaxIdleConnsPerHost: 1, IdleConnTimeout: 30 * time.Second}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return s, nil
}
func (s *RemoteSink) Submit(r Record) bool {
	// Live snapshots stay local; final summaries are enough for central search.
	if !r.Finished {
		return true
	}
	select {
	case s.queue <- r:
		return true
	default:
		s.dropped.Add(1)
		return false
	}
}
func Summary(r Record) map[string]any {
	return map[string]any{"event_type": "static_edge_observation", "kind": "log", "service": "static-edge", "stage": "request-body", "request_id": r.RequestID, "span_id": r.SpanID, "node_id": r.NodeID, "process_id": r.ProcessID, "parent_span_id": r.ParentSpanID, "application_request_id": r.ApplicationRequestID, "hop": r.Hop, "protocol": r.Protocol, "build": r.Build, "config_digest": r.ConfigDigest, "elapsed_ms": r.ElapsedMS, "read_block_ms": r.Body.ReadBlockMS, "max_read_block_ms": r.Body.MaxReadBlockMS, "body_bytes": r.Body.Bytes, "first_byte_ms": r.Body.FirstByteMS, "events_dropped": r.EventsDropped, "snapshots_dropped": r.SnapshotsDropped, "finished": r.Finished, "full_evidence_source": "independent_collector"}
}
func (s *RemoteSink) Run(ctx context.Context) {
	defer s.client.CloseIdleConnections()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-s.queue:
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			raw, _ := json.Marshal(Summary(r))
			req, e := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(raw))
			if e != nil {
				s.failed.Add(1)
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			if s.token != "" {
				req.Header.Set("Authorization", "Bearer "+s.token)
			}
			resp, e := s.client.Do(req)
			if e != nil {
				s.failed.Add(1)
				continue
			}
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
			resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				s.failed.Add(1)
			} else {
				s.sent.Add(1)
			}
		}
	}
}
func (s *RemoteSink) Status() map[string]any {
	return map[string]any{"sent": s.sent.Load(), "failed": s.failed.Load(), "dropped": s.dropped.Load(), "queue_depth": len(s.queue)}
}
