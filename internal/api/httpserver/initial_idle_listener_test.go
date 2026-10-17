package httpserver

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testHeaderTimeout = 120 * time.Millisecond
const testIdleTimeout = 1200 * time.Millisecond

func initialIdleTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.Config.ReadHeaderTimeout = testHeaderTimeout
	server.Config.IdleTimeout = testIdleTimeout
	server.Listener = WithInitialIdleTimeout(server.Listener, server.Config.IdleTimeout)
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func idleTestDial(t *testing.T, server *httptest.Server) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
	return conn
}

func readIdleTestResponse(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%q error=%v", response.StatusCode, body, err)
	}
	return string(body)
}

func TestInitialIdleAllowsUnusedConnectionAndSubsequentReuse(t *testing.T) {
	t.Parallel()
	var posts atomic.Int32
	server := initialIdleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		}
		body, _ := io.ReadAll(r.Body)
		_, _ = io.WriteString(w, r.RemoteAddr+":"+string(body))
	}))
	conn := idleTestDial(t, server)
	reader := bufio.NewReader(conn)
	for i := 0; i < 2; i++ {
		// First wait is on a never-used connection; second is keep-alive.
		time.Sleep(2 * testHeaderTimeout)
		_, err := io.WriteString(conn, "POST / HTTP/1.1\r\nHost: fixture\r\nContent-Length: 7\r\n\r\npayload")
		if err != nil {
			t.Fatal(err)
		}
		if got := readIdleTestResponse(t, reader); got != conn.LocalAddr().String()+":payload" {
			t.Fatalf("unexpected response %q", got)
		}
	}
	if posts.Load() != 2 {
		t.Fatalf("POSTs=%d, want exactly two distinct requests", posts.Load())
	}
}

func TestInitialIdleStillBoundsEmptyConnections(t *testing.T) {
	t.Parallel()
	server := initialIdleTestServer(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("empty connection invoked handler")
	}))
	conn := idleTestDial(t, server)
	started := time.Now()
	_, err := conn.Read(make([]byte, 1))
	if !errors.Is(err, io.EOF) || time.Since(started) < 3*testHeaderTimeout || time.Since(started) > 3*time.Second {
		t.Fatalf("empty connection closed after %s with %v", time.Since(started), err)
	}
}

func TestInitialIdleHeaderBudgetDoesNotResetOnDrippedBytes(t *testing.T) {
	for _, reused := range []bool{false, true} {
		t.Run(map[bool]string{false: "first", true: "keepalive"}[reused], func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			server := initialIdleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = io.WriteString(w, "ok")
			}))
			conn := idleTestDial(t, server)
			reader := bufio.NewReader(conn)
			if reused {
				_, _ = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: fixture\r\n\r\n")
				readIdleTestResponse(t, reader)
			}
			// More than four bytes also starts net/http's next-request parser.
			_, _ = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: ")
			started := time.Now()
			stop, writerDone := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(writerDone)
				ticker := time.NewTicker(20 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						if _, err := io.WriteString(conn, "x"); err != nil {
							return
						}
					}
				}
			}()
			_, err := reader.ReadByte()
			close(stop)
			<-writerDone
			wantCalls := int32(0)
			if reused {
				wantCalls = 1
			}
			if err == nil || time.Since(started) > 5*testHeaderTimeout || calls.Load() != wantCalls {
				t.Fatalf("slow header elapsed=%s err=%v calls=%d", time.Since(started), err, calls.Load())
			}
		})
	}
}

func TestInitialIdleDoesNotLimitBodiesOrActiveHandlers(t *testing.T) {
	t.Parallel()
	server := initialIdleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("body read: %v", err)
			return
		}
		// Neither the initial idle deadline nor a header deadline owns this.
		time.Sleep(testIdleTimeout + 100*time.Millisecond)
		_, _ = w.Write(body)
	}))
	conn := idleTestDial(t, server)
	_, _ = io.WriteString(conn, "POST / HTTP/1.1\r\nHost: fixture\r\nContent-Length: 7\r\n\r\n")
	time.Sleep(2 * testHeaderTimeout)
	_, _ = io.WriteString(conn, "payload")
	if body := readIdleTestResponse(t, bufio.NewReader(conn)); body != "payload" {
		t.Fatalf("body=%q", body)
	}
}

func TestInitialIdleSpeculativeTransportDialRemainsUsable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	warmEntered, warmRelease := make(chan struct{}), make(chan struct{})
	var posts atomic.Int32
	server := initialIdleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/warm" {
			close(warmEntered)
			select {
			case <-warmRelease:
			case <-ctx.Done():
			}
		}
		if r.Method == http.MethodPost {
			posts.Add(1)
			body, _ := io.ReadAll(r.Body)
			if string(body) != "payload" {
				t.Errorf("body=%q", body)
			}
		}
		_, _ = io.WriteString(w, "ok")
	}))
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.IdleConnTimeout = time.Second
	defer transport.CloseIdleConnections()
	dialStarted, returnDial := make(chan struct{}), make(chan struct{})
	second := make(chan net.Conn, 1)
	var dials atomic.Int32
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		n := dials.Add(1)
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil || n != 2 {
			return conn, err
		}
		second <- conn
		close(dialStarted)
		select {
		case <-returnDial:
			return conn, nil
		case <-ctx.Done():
			_ = conn.Close()
			return nil, ctx.Err()
		}
	}
	do := func(method, path string, trace *httptrace.ClientTrace) error {
		request, _ := http.NewRequestWithContext(ctx, method, server.URL+path, strings.NewReader("payload"))
		request.GetBody = nil
		if trace != nil {
			request = request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
		}
		response, err := transport.RoundTrip(request)
		if err == nil {
			_, err = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
		return err
	}
	wait := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	warmDone, nextDone := make(chan error, 1), make(chan error, 1)
	go func() { warmDone <- do(http.MethodGet, "/warm", nil) }()
	wait(warmEntered)
	go func() { nextDone <- do(http.MethodGet, "/next", nil) }()
	wait(dialStarted)
	close(warmRelease)
	for _, done := range []chan error{warmDone, nextDone} {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	unused := <-second
	close(returnDial)
	time.Sleep(2 * testHeaderTimeout)
	var used net.Conn
	if err := do(http.MethodPost, "/mutation", &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { used = info.Conn }}); err != nil {
		t.Fatal(err)
	}
	if used != unused || posts.Load() != 1 || dials.Load() != 2 {
		t.Fatalf("unused connection reused=%t posts=%d dials=%d", used == unused, posts.Load(), dials.Load())
	}
}

func TestInitialIdleShutdownStillClosesUnusedConnections(t *testing.T) {
	t.Parallel()
	server := initialIdleTestServer(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	conn := idleTestDial(t, server)
	// Let Serve accept and track the connection before starting shutdown.
	time.Sleep(30 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := server.Config.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("unused connection after shutdown: %v", err)
	}
}

func TestInitialIdleExplicitDeadlineOverridesWaiting(t *testing.T) {
	for _, wholeConnection := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "whole_connection"}[wholeConnection], func(t *testing.T) {
			t.Parallel()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			client, err := net.Dial("tcp", listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			conn, err := WithInitialIdleTimeout(listener, testIdleTimeout).Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(250 * time.Millisecond)
			if wholeConnection {
				err = conn.SetDeadline(deadline)
			} else {
				err = conn.SetReadDeadline(deadline)
			}
			if err != nil {
				t.Fatal(err)
			}
			// Receiving the first byte must not reinstate the deferred header
			// budget after a newer explicit deadline has replaced it.
			_, _ = client.Write([]byte("x"))
			if _, err := conn.Read(make([]byte, 1)); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			_, err = conn.Read(make([]byte, 1))
			var timeout net.Error
			if !errors.As(err, &timeout) || !timeout.Timeout() || time.Since(started) > 800*time.Millisecond {
				t.Fatalf("explicit deadline elapsed=%s error=%v", time.Since(started), err)
			}
		})
	}
}
