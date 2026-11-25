package edgegroupfront

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// A Front code replacement closes its sockets; an activation update need not.
// Exercise real TCP streams across promotion and rollback instead of inferring
// connection continuity from the activation file or health status alone.
func TestFrontActivationPreservesEstablishedWorkerStreams(t *testing.T) {
	testFrontWorkerStreams(t, false)
}

func TestFrontBoundedDrainClosesOldTCPButNotNewTCP(t *testing.T) {
	testFrontWorkerStreams(t, true)
}

func testFrontWorkerStreams(t *testing.T, bounded bool) {
	path := filepath.Join(t.TempDir(), "activation.json")
	now := time.Now().UTC()
	change := func(operation string, generation uint64, from, to string, rollbackOf uint64) ActivationState {
		t.Helper()
		req := activationRequest(operation, generation, from, to, "bundle-"+to, rollbackOf)
		if to == "b" {
			req.WorkerImageDigest = "sha256:" + strings.Repeat("b", 64)
		}
		r, err := ApplyActivationCAS(path, req, now.Add(time.Duration(generation)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		return r.Current
	}
	previous := change(ActivationOperationInit, 0, "a", "a", 0)
	worker := func(slot string) string {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprint(w, slot+":"+r.URL.Path)
		}))
		t.Cleanup(server.Close)
		return server.Listener.Addr().String()
	}
	cfg := Config{ActiveSlotFile: path, EdgeGroupID: testActivationGroup, RequireActivationState: true,
		DialTimeout: time.Second, Slots: map[string]SlotTargets{"a": {HTTPAddress: worker("a")}, "b": {HTTPAddress: worker("b")}}}
	if bounded {
		cfg.EdgeID = "fixture-node"
		cfg.WorkerDrainFrontUID = "01234567-89ab-cdef-0123-456789abcdef"
		cfg.WorkerDrainGrantPath = filepath.Join(filepath.Dir(path), "drain.json")
		cfg.WorkerTransport = "fixture-pinned-transport"
		cfg.workerTargets = make(map[workerImagePin]SlotTargets)
		for _, slot := range []string{"a", "b"} {
			req := activationRequest(ActivationOperationInit, 0, slot, slot, "bundle-"+slot, 0)
			if slot == "b" {
				req.WorkerImageDigest = "sha256:" + strings.Repeat("b", 64)
			}
			target := cfg.Slots[slot]
			target.HTTPSAddress = target.HTTPAddress
			cfg.workerTargets[workerImagePin{slot, req.WorkerSourceCommit, req.WorkerImageDigest}] = target
		}
	}
	front := NewService(cfg, log.New(io.Discard, "", 0))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var handlers sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			handlers.Go(func() { front.handleTCPConnection(cfg, ProtocolHTTP, conn) })
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-acceptDone
		handlers.Wait()
	})
	dial := func() (net.Conn, *bufio.Reader) {
		t.Helper()
		conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn, bufio.NewReader(conn)
	}
	roundTrip := func(conn net.Conn, reader *bufio.Reader, name, wantSlot string) {
		t.Helper()
		if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprintf(conn, "GET /%s HTTP/1.1\r\nHost: synthetic.example.test\r\n\r\n", name); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != wantSlot+":/"+name {
			t.Fatalf("stream %s: want Worker %s; got HTTP %d, body %q, error %v", name, wantSlot, response.StatusCode, body, err)
		}
	}

	old, oldReader := dial()
	roundTrip(old, oldReader, "before", "a")
	serving := change(ActivationOperationPromote, 1, "a", "b", 0)
	promoted, promotedReader := dial()
	roundTrip(promoted, promotedReader, "new-after-promotion", "b")
	roundTrip(old, oldReader, "existing-after-promotion", "a")
	if bounded {
		g := WorkerDrainGrant{
			Schema: WorkerDrainSchema, FrontUID: cfg.WorkerDrainFrontUID, Node: cfg.EdgeID,
			Previous: previous, Serving: serving, Start: serving.UpdatedAt,
			Deadline: serving.UpdatedAt.Add(WorkerDrainLimit), HealthyAt: serving.UpdatedAt,
		}
		writeDrainFixture(t, cfg, g)
		if err := front.reconcileWorkerDrain(cfg, g.Deadline.Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
		roundTrip(old, oldReader, "existing-before-deadline", "a")
		g.HealthyAt = g.Deadline
		writeDrainFixture(t, cfg, g)
		if err := front.reconcileWorkerDrain(cfg, g.Deadline); err != nil {
			t.Fatal(err)
		}
		if err := old.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := oldReader.ReadByte(); err != io.EOF {
			t.Fatalf("old TCP session didn't close: %v", err)
		}
		roundTrip(promoted, promotedReader, "new-connection-survives-drain", "b")
		return
	}
	change(ActivationOperationRollback, 2, "b", "a", 2)
	restored, restoredReader := dial()
	roundTrip(restored, restoredReader, "new-after-rollback", "a")
	roundTrip(old, oldReader, "existing-after-rollback", "a")
	roundTrip(promoted, promotedReader, "promoted-session-after-rollback", "b")
}
