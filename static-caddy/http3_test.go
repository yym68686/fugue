package main

import (
	"crypto/tls"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/quic-go/quic-go/http3"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTP3RemainsHTTP3WithExplicitMissingFrameCoverage(t *testing.T) {
	store, socket := collector(t)
	h := observation(t, socket, true)
	certSource := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer certSource.Close()
	ln, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	srv := &http3.Server{TLSConfig: certSource.TLS.Clone(), Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e := h.ServeHTTP(w, r, caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
			if r.ProtoMajor != 3 {
				t.Error("HTTP/3 downgraded")
			}
			_, e := io.Copy(w, r.Body)
			return e
		}))
		if e != nil {
			t.Error(e)
		}
	})}
	go srv.Serve(ln)
	defer srv.Close()
	tr := &http3.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	defer tr.Close() // isolated generated certificate
	client := &http.Client{Transport: tr}
	resp, e := client.Post("https://"+ln.LocalAddr().String(), "application/octet-stream", strings.NewReader("synthetic-quic-body"))
	if e != nil {
		t.Fatal(e)
	}
	raw, e := io.ReadAll(resp.Body)
	resp.Body.Close()
	if e != nil || string(raw) != "synthetic-quic-body" || resp.ProtoMajor != 3 {
		t.Fatal(e, string(raw), resp.Proto)
	}
	r := records(t, store, resp.Header.Get("X-Fugue-Observation-ID"))[0]
	if r.Protocol != "HTTP/3.0" || r.Coverage.HTTP3Frames || !r.Coverage.Body {
		t.Fatal(r)
	}
}
