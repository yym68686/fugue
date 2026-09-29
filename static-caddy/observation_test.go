package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	o "fugue/internal/staticedgeobserve"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"golang.org/x/net/http2"
)

func collector(t *testing.T) (*o.Store, string) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "fso-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	os.Chmod(root, 0700)
	socket := filepath.Join(root, "o.sock")
	s, e := o.NewStore(o.StoreConfig{NodeID: "edge-a", Directory: root})
	if e != nil {
		t.Fatal(e)
	}
	ln, e := net.Listen("unix", socket)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	srv := &http.Server{Handler: s.Handler()}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close(); cancel(); <-done })
	return s, socket
}
func observation(t *testing.T, socket string, entry bool) *Observation {
	t.Helper()
	key := filepath.Join(t.TempDir(), "key")
	os.WriteFile(key, bytes.Repeat([]byte{9}, 32), 0600)
	h := &Observation{NodeID: "edge-a", Hop: "entry", Build: "fixture", ConfigDigest: "sha256:fixture", Socket: socket, CorrelationKeyFile: key, Entry: entry, Capacity: 32}
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	t.Cleanup(cancel)
	if e := h.Provision(ctx); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { h.Cleanup() })
	return h
}
func records(t *testing.T, s *o.Store, id string) []o.Record {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		v, e := s.Query(o.Query{RequestID: id, Since: time.Now().Add(-time.Minute), Until: time.Now(), Limit: 20})
		if e != nil {
			t.Fatal(e)
		}
		for _, r := range v.Records {
			if r.Finished {
				return v.Records
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("completed observation not collected")
	return nil
}

func TestHTTP2DelayedBodyHasProtocolEvidenceWithoutPayload(t *testing.T) {
	s, socket := collector(t)
	h := observation(t, socket, true)
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		if r.ProtoMajor != 2 {
			t.Error("protocol changed")
		}
		b, e := io.ReadAll(r.Body)
		if e != nil {
			return e
		}
		w.Header().Set("X-Request-ID", "app-fixture")
		w.Write(b)
		return nil
	})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if e := h.ServeHTTP(w, r, next); e != nil {
			t.Error(e)
		}
	}))
	server.EnableHTTP2 = true
	if e := http2.ConfigureServer(server.Config, &http2.Server{FugueObserveRequests: true}); e != nil {
		t.Fatal(e)
	}
	server.StartTLS()
	defer server.Close()
	pr, pw := io.Pipe()
	payload := bytes.Repeat([]byte("payload-not-telemetry"), 4096)
	go func() { time.Sleep(45 * time.Millisecond); pw.Write(payload); pw.Close() }()
	req, _ := http.NewRequest(http.MethodPost, server.URL, pr)
	req.Header.Set(o.CorrelationHeader, "forged-untrusted-value")
	resp, e := server.Client().Do(req)
	if e != nil {
		t.Fatal(e)
	}
	got, e := io.ReadAll(resp.Body)
	resp.Body.Close()
	if e != nil || !bytes.Equal(got, payload) {
		t.Fatal("body changed", e)
	}
	id := resp.Header.Get("X-Fugue-Observation-ID")
	if !o.ValidID(id) {
		t.Fatal("missing ingress identity")
	}
	v := records(t, s, id)[0]
	if !v.Coverage.HTTP2Frames || v.HTTP2 == nil || v.HTTP2.DataBytes != int64(len(payload)) || v.HTTP2.ConsumedBytes != int64(len(payload)) || v.HTTP2.FirstDataMS == nil || *v.HTTP2.FirstDataMS < 20 {
		t.Fatalf("protocol evidence missing %+v", v.HTTP2)
	}
	if v.Body.FirstByteMS == nil || *v.Body.FirstByteMS < 20 || v.Correlation != "entry" || v.ApplicationRequestID != "app-fixture" {
		t.Fatal(v)
	}
}

type waitCapture struct {
	mu             sync.Mutex
	begins, grants int
}

func (w *waitCapture) HTTP2SendWait(begin, granted bool, _ uint32, _, _ int32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if begin {
		w.begins++
	}
	if granted {
		w.grants++
	}
}

func TestHTTP2SenderWaitHookSeesRealWindowBlock(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(204)
	}))
	server.EnableHTTP2 = true
	if e := http2.ConfigureServer(server.Config, &http2.Server{MaxUploadBufferPerStream: 65535, MaxUploadBufferPerConnection: 65535}); e != nil {
		t.Fatal(e)
	}
	server.StartTLS()
	defer server.Close()
	transport := &http2.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	defer transport.CloseIdleConnections()
	waits := &waitCapture{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(http2.WithFugueSendObserver(ctx, waits), http.MethodPost, server.URL, strings.NewReader(strings.Repeat("x", 512<<10)))
	result := make(chan error, 1)
	go func() {
		r, e := transport.RoundTrip(req)
		if e == nil {
			r.Body.Close()
		}
		result <- e
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		close(release)
		t.Fatal("handler never started")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		waits.mu.Lock()
		n := waits.begins
		waits.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	if e := <-result; e != nil {
		t.Fatal(e)
	}
	waits.mu.Lock()
	defer waits.mu.Unlock()
	if waits.begins == 0 || waits.grants == 0 {
		t.Fatalf("no measured sender block: %+v", waits)
	}
}

func TestHTTP1SSEFlushAndCancellationPreserved(t *testing.T) {
	_, socket := collector(t)
	h := observation(t, socket, true)
	cancelled := make(chan struct{})
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: first\n\n")
		if e := http.NewResponseController(w).Flush(); e != nil {
			return e
		}
		<-r.Context().Done()
		close(cancelled)
		return nil
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(w, r, next) }))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	b := make([]byte, len("data: first\n\n"))
	if _, e = io.ReadFull(resp.Body, b); e != nil {
		t.Fatal(e)
	}
	cancel()
	resp.Body.Close()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("cancellation not forwarded")
	}
}
