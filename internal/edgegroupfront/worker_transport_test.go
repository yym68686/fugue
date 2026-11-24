package edgegroupfront

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func transportFixture() (Config, workerTransportDeclaration) {
	pin := func(slot, id string) workerImagePin {
		return workerImagePin{slot, strings.Repeat(id, 40), "sha256:" + strings.Repeat(id, 64)}
	}
	cfg := Config{NodeHost: "127.0.0.1", EdgeID: "member-one", EdgeGroupID: testActivationGroup, ActiveSlotFile: "/unused", RequireActivationState: true, DefaultSlot: "a", DialTimeout: time.Second,
		Slots: map[string]SlotTargets{"a": {"127.0.0.1:18080", "127.0.0.1:18443"}, "b": {"127.0.0.1:28080", "127.0.0.1:28443"}}}
	return cfg, workerTransportDeclaration{"fugue.worker-transport/v1", testActivationGroup, "member-one", []workerImagePin{pin("a", "a"), pin("b", "b")}, pin("a", "c"), 38080, 38443}
}

func TestDeclaredWorkerTransportRejectsAmbiguousOrOffNodeInputs(t *testing.T) {
	for name, change := range map[string]func(*Config, *workerTransportDeclaration){
		"foreign-node":       func(_ *Config, v *workerTransportDeclaration) { v.Node = "other" },
		"foreign-group":      func(_ *Config, v *workerTransportDeclaration) { v.Group = "other" },
		"unsigned-slot-file": func(c *Config, _ *workerTransportDeclaration) { c.RequireActivationState = false },
		"nonliteral-node":    func(c *Config, _ *workerTransportDeclaration) { c.NodeHost = "other.example.test" },
		"slot-collision":     func(_ *Config, v *workerTransportDeclaration) { v.HTTP = 28080 },
		"port-alias":         func(_ *Config, v *workerTransportDeclaration) { v.HTTP = v.HTTPS },
		"other-node-port": func(c *Config, _ *workerTransportDeclaration) {
			c.Slots["b"] = SlotTargets{"192.0.2.2:28080", "192.0.2.2:28443"}
		},
		"unbound-image":         func(_ *Config, v *workerTransportDeclaration) { v.Candidate.Image = "latest" },
		"ambiguous-incarnation": func(_ *Config, v *workerTransportDeclaration) { v.Candidate = v.Retained[0] },
		"missing-retained-slot": func(_ *Config, v *workerTransportDeclaration) { v.Retained = v.Retained[:1] },
	} {
		t.Run(name, func(t *testing.T) {
			c, v := transportFixture()
			change(&c, &v)
			b, _ := json.Marshal(v)
			c.WorkerTransport = string(b)
			if c.prepareWorkerTransport() == nil {
				t.Fatal("accepted invalid transport")
			}
		})
	}
	c, v := transportFixture()
	b, _ := json.Marshal(v)
	c.WorkerTransport = string(b)
	if e := c.prepareWorkerTransport(); e != nil {
		t.Fatal("registered production port pair was not accepted", e)
	}
	wrong := v
	wrong.HTTPS = 6443
	unregistered, _ := json.Marshal(wrong)
	c.WorkerTransport = string(unregistered)
	if c.prepareWorkerTransport() == nil {
		t.Fatal("selected an unregistered node port")
	}
	c.WorkerTransport = strings.Replace(string(b), "{", "{\"node\":\"wrong\",", 1)
	if c.prepareWorkerTransport() == nil {
		t.Fatal("duplicate field accepted")
	}
}

func TestNodeOverlapReusesLogicalSlotWithoutDroppingEitherRetainedTLSStream(t *testing.T) {
	c, v := transportFixture()
	c.ActiveSlotFile = filepath.Join(t.TempDir(), "activation.json")
	roots := x509.NewCertPool()
	worker := func(label string) SlotTargets {
		h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, label) })
		a, b := httptest.NewServer(h), httptest.NewTLSServer(h)
		t.Cleanup(a.Close)
		t.Cleanup(b.Close)
		roots.AddCert(b.Certificate())
		return SlotTargets{a.Listener.Addr().String(), b.Listener.Addr().String()}
	}
	c.Slots["a"], c.Slots["b"] = worker("retained-A"), worker("retained-B")
	next := worker("new-A")
	v.HTTP = netip.MustParseAddrPort(next.HTTPAddress).Port()
	v.HTTPS = netip.MustParseAddrPort(next.HTTPSAddress).Port()
	raw, _ := json.Marshal(v)
	c.WorkerTransport = string(raw)
	if e := c.bindWorkerTransport(v.HTTP, v.HTTPS); e != nil {
		t.Fatal(e)
	}
	front := NewService(c, log.New(io.Discard, "", 0))
	ln, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	go func() {
		defer close(done)
		for {
			conn, e := ln.Accept()
			if e != nil {
				return
			}
			wg.Go(func() { front.handleTCPConnection(c, ProtocolHTTPS, conn) })
		}
	}()
	t.Cleanup(func() { ln.Close(); <-done; wg.Wait() })
	change := func(op string, n uint64, from string, to workerImagePin) {
		r := activationRequest(op, n, from, to.Slot, "release."+to.Slot, 0)
		r.WorkerImageDigest, r.WorkerSourceCommit = to.Image, to.Source
		switch op {
		case ActivationOperationPromote:
			if _, e := PreparePublication(c.ActiveSlotFile, r, time.Now().UTC()); e != nil {
				t.Fatal(e)
			}
			r.RequirePublicationPreparation = true
		case ActivationOperationRollback:
			r.RollbackOfGeneration = n
		}
		if _, e := ApplyActivationCAS(c.ActiveSlotFile, r, time.Now().UTC()); e != nil {
			t.Fatal(e)
		}
	}
	type stream struct {
		net.Conn
		reader *bufio.Reader
	}
	dial := func() *stream {
		conn, e := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp4", ln.Addr().String(), &tls.Config{RootCAs: roots, ServerName: "example.com", MinVersion: tls.VersionTLS12})
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { conn.Close() })
		return &stream{conn, bufio.NewReader(conn)}
	}
	prove := func(s *stream, want string) {
		t.Helper()
		s.SetDeadline(time.Now().Add(2 * time.Second))
		if _, e := io.WriteString(s, "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"); e != nil {
			t.Fatal(e)
		}
		r, e := http.ReadResponse(s.reader, nil)
		if e != nil {
			t.Fatal(e)
		}
		b, e := io.ReadAll(r.Body)
		r.Body.Close()
		if e != nil || string(b) != want {
			t.Fatalf("same TLS stream: got %q, want %q, error %v", b, want, e)
		}
	}
	change(ActivationOperationInit, 0, "a", v.Retained[0])
	a := dial()
	prove(a, "retained-A")
	change(ActivationOperationPromote, 1, "a", v.Retained[1])
	b := dial()
	prove(b, "retained-B")
	change(ActivationOperationPromote, 2, "b", v.Candidate)
	candidate := dial()
	prove(candidate, "new-A")
	prove(a, "retained-A")
	prove(b, "retained-B")
	change(ActivationOperationRollback, 3, "a", v.Retained[1])
	prove(dial(), "retained-B")
	prove(candidate, "new-A")
	prove(a, "retained-A")
	// An unknown incarnation of A must not fall back to either listening
	// port. Existing connections need neither reauthentication nor a redial.
	foreign := v.Candidate
	foreign.Source = strings.Repeat("d", 40)
	change(ActivationOperationPromote, 4, "b", foreign)
	if _, _, _, ok := front.selectedWorker(c); ok {
		t.Fatal("mapped a foreign release")
	}
	if conn, e := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp4", ln.Addr().String(), &tls.Config{RootCAs: roots, ServerName: "example.com"}); e == nil {
		conn.Close()
		t.Fatal("unknown release served new connection")
	}
	prove(a, "retained-A")
	prove(b, "retained-B")
	prove(candidate, "new-A")
}
