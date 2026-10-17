package edge

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
)

func TestHeartbeatSuccessReusesConnectionWithoutRepeatingPost(t *testing.T) {
	for _, lengthKnown := range []bool{false, true} {
		t.Run(strconv.FormatBool(lengthKnown), func(t *testing.T) {
			var connections, posts atomic.Int32
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/edge/heartbeat" {
					t.Errorf("unexpected heartbeat request: %s %s", r.Method, r.URL.Path)
				}
				posts.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				// Larger than the server's automatic small-response buffer; exercise
				// chunked responses as well as bodies with a known Content-Length.
				body := `{"accepted":true,"padding":"` + strings.Repeat("x", 8192) + `"}`
				if lengthKnown {
					w.Header().Set("Content-Length", strconv.Itoa(len(body)))
				}
				_, _ = io.WriteString(w, body)
			}))
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
			}
			server.Start()
			defer server.Close()
			transport := http.DefaultTransport.(*http.Transport).Clone()
			defer transport.CloseIdleConnections()
			service := NewService(config.EdgeConfig{APIURL: server.URL, EdgeToken: "test-token", EdgeID: "edge-test", EdgeGroupID: "group-test", EdgeSlot: "b", EdgeInstanceUID: "instance-test", EdgeReleaseEpoch: "release-test"}, log.New(io.Discard, "", 0))
			service.HTTPClient = &http.Client{Transport: transport, Timeout: time.Second}
			for range 2 {
				if err := service.HeartbeatOnce(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if posts.Load() != 2 {
				t.Fatalf("heartbeat POST count = %d, want exactly 2", posts.Load())
			}
			if connections.Load() != 1 {
				t.Fatalf("successful heartbeats opened %d connections, want one reused connection", connections.Load())
			}
		})
	}
}

type heartbeatDrainTestBody struct {
	read   int
	closed bool
	err    error
}

func (b *heartbeatDrainTestBody) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	for i := range p {
		p[i] = 'x'
	}
	b.read += len(p)
	return len(p), nil
}
func (b *heartbeatDrainTestBody) Close() error { b.closed = true; return nil }

func TestHeartbeatAcknowledgmentSurvivesBoundedResponseDiscard(t *testing.T) {
	for _, tc := range []struct {
		name     string
		length   int64
		err      error
		wantRead int
	}{
		{"known oversized", heartbeatResponseDrainLimit + 100, nil, 0},
		{"unknown endless", -1, nil, heartbeatResponseDrainLimit + 1},
		{"body read error", -1, errors.New("broken optional body"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &heartbeatDrainTestBody{err: tc.err}
			posts := 0
			service := NewService(config.EdgeConfig{APIURL: "http://api.example.test", EdgeToken: "test-token", EdgeID: "edge-test", EdgeGroupID: "group-test", EdgeSlot: "b", EdgeInstanceUID: "instance-test", EdgeReleaseEpoch: "release-test"}, log.New(io.Discard, "", 0))
			service.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				posts++
				return &http.Response{StatusCode: http.StatusOK, Body: body, ContentLength: tc.length, Header: make(http.Header), Request: r}, nil
			})}
			if err := service.HeartbeatOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if posts != 1 || body.read != tc.wantRead || !body.closed {
				t.Fatalf("posts=%d bytes=%d closed=%v", posts, body.read, body.closed)
			}
		})
	}
}

func TestHeartbeatSuccessDoesNotWaitForStalledOptionalBody(t *testing.T) {
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	service := NewService(config.EdgeConfig{APIURL: server.URL, EdgeToken: "test-token", EdgeID: "edge-test", EdgeGroupID: "group-test", EdgeSlot: "b", EdgeInstanceUID: "instance-test", EdgeReleaseEpoch: "release-test"}, log.New(io.Discard, "", 0))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if err := service.HeartbeatOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("optional body held heartbeat for %s", elapsed)
	}
	if posts.Load() != 1 {
		t.Fatalf("POST replayed: %d", posts.Load())
	}
}
