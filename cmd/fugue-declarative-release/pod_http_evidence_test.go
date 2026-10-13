package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPodHTTPFailurePreservesBoundedEvidenceWithoutBody(t *testing.T) {
	for _, test := range []struct {
		name     string
		status   int
		body     string
		phase    string
		complete bool
	}{
		{name: "unhealthy", status: http.StatusServiceUnavailable, body: "private-upstream-detail", phase: "status", complete: true},
		{name: "oversized", status: http.StatusOK, body: strings.Repeat("x", (1<<20)+9), phase: "body_limit"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				response.WriteHeader(test.status)
				_, _ = response.Write([]byte(test.body))
			}))
			defer server.Close()
			host, portValue, _ := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
			port, _ := strconv.Atoi(portValue)
			endpoint := podHTTPEndpoint{Name: "worker-one", UID: "worker-uid-one", IP: host, Port: port}
			before := time.Now().UTC()
			body, err := readPodHTTP(context.Background(), endpoint, "/healthz")
			var failure *podHTTPFailure
			if body != nil || !errors.As(err, &failure) {
				t.Fatalf("missing structured failure: body=%v error=%v", body != nil, err)
			}
			captured := test.body[:min(len(test.body), (1<<20)+1)]
			if failure.PodName != endpoint.Name || failure.PodUID != endpoint.UID || failure.Path != "/healthz" ||
				failure.Status != test.status || failure.Phase != test.phase || failure.BodyComplete != test.complete ||
				failure.CapturedBytes != len(captured) || failure.CapturedBodyDigest != fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(captured))) ||
				failure.ObservedAt.Before(before) || len(err.Error()) > 1024 || strings.Contains(err.Error(), "private-upstream-detail") {
				t.Fatalf("incorrect or unbounded evidence: %v", err)
			}
		})
	}
}

func TestPodHTTPFailurePreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := readPodHTTP(ctx, podHTTPEndpoint{Name: "worker-one", UID: "worker-uid-one", IP: "127.0.0.1", Port: 1}, "/healthz")
	var failure *podHTTPFailure
	if !errors.Is(err, context.Canceled) || !errors.As(err, &failure) || failure.ErrorClass != "canceled" || failure.Phase != "request" || failure.BodyComplete {
		t.Fatalf("cancellation evidence lost: %v", err)
	}
}
