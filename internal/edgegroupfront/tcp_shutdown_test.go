package edgegroupfront

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)

type closeWitnessListener struct {
	net.Listener
	closed chan struct{}
}

func (w closeWitnessListener) Close() error { err := w.Listener.Close(); close(w.closed); return err }

func TestFrontTCPShutdownStopsAcceptButCompletesTheSameStream(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		name := "natural_eof"
		if timeout {
			name = "deadline_is_not_a_drain_receipt"
		}
		t.Run(name, func(t *testing.T) {
			ln, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			up, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer up.Close()
			proxy := closeWitnessListener{ln, make(chan struct{})}
			cfg := Config{DefaultSlot: "a", Slots: map[string]SlotTargets{"a": {HTTPSAddress: up.Addr().String()}}, DialTimeout: time.Second}
			s := NewService(cfg, log.New(io.Discard, "", 0))
			var wg sync.WaitGroup
			stop := s.serveTCPProxy(cfg, ProtocolHTTPS, proxy, &wg)
			client, err := net.DialTimeout("tcp4", ln.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			client.SetDeadline(time.Now().Add(5 * time.Second))
			up.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second))
			worker, err := up.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			worker.SetDeadline(time.Now().Add(5 * time.Second))
			check := func(w io.Writer, r io.Reader, letter string) {
				t.Helper()
				if _, e := io.WriteString(w, letter); e != nil {
					t.Fatal(e)
				}
				var b [1]byte
				if _, e := io.ReadFull(r, b[:]); e != nil || string(b[:]) != letter {
					t.Fatal("same stream lost", e)
				}
			}
			check(client, worker, "1")
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- stop(ctx) }()
			processDrained := make(chan struct{})
			go func() { wg.Wait(); close(processDrained) }()
			select {
			case <-proxy.closed:
			case <-ctx.Done():
				t.Fatal("listener didn't close")
			}
			if late, e := net.DialTimeout("tcp4", ln.Addr().String(), time.Second); e == nil {
				late.Close()
				t.Fatal("listener still accepts new streams")
			}
			check(worker, client, "2")
			select {
			case <-done:
				t.Fatal("reported drain with unfinished stream")
			default:
			}
			if timeout {
				cancel()
				if e := <-done; !errors.Is(e, context.Canceled) {
					t.Fatal("deadline reported success", e)
				}
				select {
				case <-processDrained:
					t.Fatal("Run's process lifetime ended before the stream did")
				case <-time.After(25 * time.Millisecond):
				}
				check(client, worker, "3")
			}
			client.Close()
			worker.Close()
			if !timeout {
				select {
				case e := <-done:
					if e != nil {
						t.Fatal(e)
					}
				case <-ctx.Done():
					t.Fatal("natural drain was not observed")
				}
			}
			select {
			case <-processDrained:
			case <-time.After(3 * time.Second):
				t.Fatal("process did not finish after the last stream closed")
			}
		})
	}
}
