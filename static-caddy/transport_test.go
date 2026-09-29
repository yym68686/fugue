package main

import (
	"context"
	"encoding/json"
	o "fugue/internal/staticedgeobserve"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestForwardAttemptsJoinWithoutLeakingCarrierToApplication(t *testing.T) {
	store, socket := collector(t)
	entry := observation(t, socket, true)
	entry.ForwardCorrelation = true
	origin := observation(t, socket, false)
	origin.TrustLoopback = true
	origin.Hop = "origin"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := origin.ServeHTTP(w, r, caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
			if r.Header.Get(o.CorrelationHeader) != "" {
				t.Error("internal carrier leaked to application")
			}
			b, e := io.ReadAll(r.Body)
			if e != nil {
				return e
			}
			w.Header().Set("X-Request-ID", "application-fixture")
			_, e = w.Write(b)
			return e
		}))
		if err != nil {
			t.Error(err)
		}
	}))
	defer backend.Close()
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	transport := new(ObservedTransport)
	if e := transport.Provision(ctx); e != nil {
		t.Fatal(e)
	}
	defer transport.Cleanup()
	target, _ := url.Parse(backend.URL)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e := entry.ServeHTTP(w, r, caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
			r.URL.Scheme = target.Scheme
			r.URL.Host = target.Host
			r.RequestURI = ""
			resp, e := transport.RoundTrip(r)
			if e != nil {
				return e
			}
			defer resp.Body.Close()
			w.Header().Set("X-Request-ID", resp.Header.Get("X-Request-ID"))
			_, e = io.Copy(w, resp.Body)
			return e
		}))
		if e != nil {
			t.Error(e)
		}
	}))
	defer front.Close()
	resp, e := http.Post(front.URL, "application/octet-stream", strings.NewReader("safe-fixture"))
	if e != nil {
		t.Fatal(e)
	}
	body, e := io.ReadAll(resp.Body)
	resp.Body.Close()
	if e != nil || string(body) != "safe-fixture" {
		t.Fatal(string(body), e)
	}
	id := resp.Header.Get("X-Fugue-Observation-ID")
	var result o.Result
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		result, e = store.Query(o.Query{RequestID: id, Since: time.Now().Add(-time.Minute), Until: time.Now(), Limit: 10})
		if e != nil {
			t.Fatal(e)
		}
		if len(result.Records) == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(result.Records) != 3 {
		t.Fatalf("want ingress, attempt, origin; got %d", len(result.Records))
	}
	chain := o.Join([]o.Result{result})
	if len(chain.Links) != 2 {
		t.Fatalf("missing authenticated chain: %+v", chain)
	}
	var attempt o.Record
	for _, r := range result.Records {
		if r.Hop == "forward_attempt" {
			attempt = r
		}
	}
	if !attempt.Coverage.HTTPTrace || attempt.Body.Bytes != 12 || attempt.ParentSpanID == "" {
		t.Fatal(attempt)
	}
	data, _ := json.Marshal(result)
	if strings.Contains(string(data), "safe-fixture") {
		t.Fatal("body leaked")
	}
}

func TestObservedTransportPreservesHTTPUpgradeDuplex(t *testing.T) {
	_, socket := collector(t)
	h := observation(t, socket, true)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, rw, e := http.NewResponseController(w).Hijack()
		if e != nil {
			t.Error(e)
			return
		}
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: fixture\r\n\r\n")
		rw.Flush()
		buf := make([]byte, 4)
		if _, e = io.ReadFull(rw, buf); e == nil {
			conn.Write(buf)
		}
	}))
	defer backend.Close()
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	transport := new(ObservedTransport)
	if e := transport.Provision(ctx); e != nil {
		t.Fatal(e)
	}
	defer transport.Cleanup()
	parent := o.New(o.Record{NodeID: "edge-a", ProcessID: processID, RequestID: o.ID(), Hop: "entry", Protocol: "HTTP/1.1", Build: "fixture", ConfigDigest: "fixture", Correlation: "entry"}, h.sink)
	req, _ := http.NewRequestWithContext(context.WithValue(context.Background(), attemptContextKey{}, attemptContext{handler: h, parent: parent}), http.MethodGet, backend.URL, nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "fixture")
	resp, e := transport.RoundTrip(req)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	duplex, ok := resp.Body.(io.ReadWriteCloser)
	if !ok || resp.StatusCode != 101 {
		t.Fatal("HTTP upgrade lost duplex interface")
	}
	if _, e = duplex.Write([]byte("ping")); e != nil {
		t.Fatal(e)
	}
	buf := make([]byte, 4)
	if _, e = io.ReadFull(duplex, buf); e != nil || string(buf) != "ping" {
		t.Fatal(string(buf), e)
	}
}
