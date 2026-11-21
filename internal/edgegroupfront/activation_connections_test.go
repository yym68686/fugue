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
	path := filepath.Join(t.TempDir(), "activation.json")
	change := func(operation string, generation uint64, from, to string, rollbackOf uint64) {
		t.Helper()
		if _, err := ApplyActivationCAS(path, activationRequest(operation, generation, from, to, "bundle-"+to, rollbackOf), time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	change(ActivationOperationInit, 0, "a", "a", 0)
	initial, _, err := ReadActivationState(path)
	if err != nil {
		t.Fatal(err)
	}
	worker := func(slot string) string {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprint(w, slot+":"+r.URL.Path)
		}))
		t.Cleanup(server.Close)
		return server.Listener.Addr().String()
	}
	cfg := Config{ActiveSlotFile: path, EdgeGroupID: testActivationGroup, RequireActivationState: true,
		DialTimeout: time.Second, Slots: map[string]SlotTargets{"a": {HTTPAddress: worker("a")}, "b": {HTTPAddress: worker("b")}}}
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
	change(ActivationOperationPromote, 1, "a", "b", 0)
	promoted, promotedReader := dial()
	roundTrip(promoted, promotedReader, "new-after-promotion", "b")
	roundTrip(old, oldReader, "existing-after-promotion", "a")
	active, _, err := ReadActivationState(path)
	if err != nil {
		t.Fatal(err)
	}
	identity := func(state ActivationState) ActivationRepairRelease {
		return ActivationRepairRelease{Slot: state.ActiveSlot, BundleGeneration: state.BundleGeneration,
			WorkerSourceCommit: state.WorkerSourceCommit, WorkerImageDigest: state.WorkerImageDigest}
	}
	repair := ActivationRepairCASRequest{JournalDigest: "sha256:" + strings.Repeat("c", 64), CommittedGeneration: 2,
		Expected: active, Committed: identity(active), Previous: identity(initial)}
	if _, err := ApplyActivationRepairCAS(path, repair); err != nil {
		t.Fatal(err)
	}
	rebased, rebasedReader := dial()
	roundTrip(rebased, rebasedReader, "new-after-same-slot-repair", "b")
	roundTrip(promoted, promotedReader, "existing-after-same-slot-repair", "b")
	roundTrip(old, oldReader, "prior-slot-existing-after-same-slot-repair", "a")
	change(ActivationOperationRollback, 4, "b", "a", 4)
	restored, restoredReader := dial()
	roundTrip(restored, restoredReader, "new-after-rollback", "a")
	roundTrip(old, oldReader, "existing-after-rollback", "a")
	roundTrip(promoted, promotedReader, "promoted-session-after-rollback", "b")
	diverged, _, err := ReadActivationState(path)
	if err != nil {
		t.Fatal(err)
	}
	repair.Expected, repair.CommittedGeneration = diverged, 4
	if _, err := PreparePublication(path, activationRequest(ActivationOperationPromote, 5, "a", "b", "bundle-b", 0), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyActivationRepairCAS(path, repair); err != nil {
		t.Fatal(err)
	}
	repaired, repairedReader := dial()
	roundTrip(repaired, repairedReader, "new-after-rollback-repair", "b")
	roundTrip(restored, restoredReader, "previously-rolled-back-session", "a")
	roundTrip(rebased, rebasedReader, "previously-rebased-session", "b")
}
