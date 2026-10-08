package frontnetwork

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestFrontNetworkReadRejectsMismatchedResponses(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "front-client-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	for _, test := range []struct {
		name string
		edit func(*Response)
	}{
		{"nonce", func(response *Response) { response.Nonce = "wrong" }},
		{"edge", func(response *Response) { response.EdgeID = "other" }},
		{"group", func(response *Response) { response.GroupID = "other" }},
		{"slot", func(response *Response) { response.Sample.Slot = "a" }},
		{"scope", func(response *Response) { response.Sample.Scope = "tcp_peer:198.51.100.0/24" }},
		{"stale", func(response *Response) { response.Sample.ObservedAt = time.Now().Add(-time.Minute) }},
		{"future", func(response *Response) { response.Sample.ObservedAt = time.Now().Add(time.Minute) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(directory, test.name+".sock")
			listener, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var query Request
				if json.NewDecoder(request.Body).Decode(&query) != nil {
					t.Error("bad request")
					return
				}
				now := time.Now().UTC()
				response := Response{Schema: Schema, Nonce: query.Nonce, EdgeID: query.EdgeID, GroupID: query.GroupID, Sample: model.EdgeClientNetworkSample{ConnectionID: "connection-a", Slot: "b", Scope: "tcp_peer:203.0.113.0/24", StartedAt: now.Add(-time.Hour), ObservedAt: now}}
				test.edit(&response)
				json.NewEncoder(writer).Encode(response)
			})}
			go server.Serve(listener)
			defer server.Close()
			if _, err := Read(context.Background(), path, "edge-a", "group-a", "b", "203.0.113.1:41000"); err == nil {
				t.Fatal("accepted unbound observation")
			}
		})
	}
}

func TestFrontNetworkPublicPeerBoundary(t *testing.T) {
	for _, remote := range []string{"127.0.0.1:80", "10.0.0.1:80", "[::1]:80", "[::ffff:203.0.113.1]:80", "203.0.113.1:0", "example.test:443", "[fe80::1%eth0]:80"} {
		if _, err := ParseRemote(remote); err == nil {
			t.Fatal("accepted non-public peer", remote)
		}
	}
	for _, remote := range []string{"203.0.113.1:443", "[2001:db8::1]:443"} {
		if _, err := ParseRemote(remote); err != nil {
			t.Fatal(remote, err)
		}
	}
}

func TestFrontNetworkFailureReasonsAreBounded(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "front-errors-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	for _, test := range []struct {
		status int
		reason string
	}{
		{http.StatusBadRequest, "request_rejected"},
		{http.StatusNotFound, "connection_missing"},
		{http.StatusTooManyRequests, "rate_limited"},
		{http.StatusServiceUnavailable, "unavailable"},
		{http.StatusForbidden, "http_rejected"},
		{http.StatusOK, "response_invalid"},
	} {
		t.Run(test.reason, func(t *testing.T) {
			path := filepath.Join(directory, test.reason+".sock")
			listener, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(test.status)
				writer.Write([]byte("sensitive response must not become a metric label"))
			})}
			go server.Serve(listener)
			defer server.Close()
			_, err = Read(context.Background(), path, "edge-a", "group-a", "b", "203.0.113.1:41000")
			if err == nil || FailureReason(err) != test.reason {
				t.Fatal("unbounded or missing rejection reason", err)
			}
		})
	}
}
