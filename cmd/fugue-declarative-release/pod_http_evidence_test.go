package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
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

func TestPodProbeKeepsEvidenceThroughPredecessorClassification(t *testing.T) {
	for _, test := range []struct {
		name, phase, errorClass string
		status                  int
		body                    string
		complete                bool
		cause                   error
	}{
		{name: "status", status: 503, body: "private-upstream-detail", phase: "status", complete: true},
		{name: "marker", status: 200, body: "private-upstream-detail", phase: "expected_marker", complete: true},
		{name: "oversized", status: 200, body: strings.Repeat("x", (1<<20)+9), phase: "body_limit"},
		{name: "partial_body", status: 200, body: "private-upstream-detail", phase: "body_read", errorClass: "transport_or_read_failure", cause: io.ErrUnexpectedEOF},
		{name: "canceled", phase: "request", errorClass: "canceled", cause: context.Canceled},
		{name: "deadline", phase: "request", errorClass: "deadline_exceeded", cause: context.DeadlineExceeded},
		{name: "healthy", status: 200, body: `{"healthy":true}`, complete: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if test.name == "partial_body" {
					w.Header().Set("Content-Length", "1000")
				}
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			host, portValue, _ := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
			port, _ := strconv.Atoi(portValue)
			pods, err := json.Marshal(map[string]any{"items": []any{map[string]any{
				"metadata": map[string]any{"name": "worker-one", "uid": "worker-uid-one"},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "worker", "ports": []any{map[string]any{"name": "http", "containerPort": port}}}}},
				"status":   map[string]any{"podIP": host, "conditions": []any{map[string]any{"type": "Ready", "status": "True"}}},
			}}})
			if err != nil {
				t.Fatal(err)
			}
			reads := 0
			cluster := &kubectlCluster{readCommand: func(_ context.Context, _ []byte, _ string, args ...string) ([]byte, error) {
				reads++
				if len(args) < 2 || args[0] != "get" {
					t.Fatalf("probe attempted a non-read command: %v", args)
				}
				if args[1] == "deployment" {
					return []byte(`{"spec":{"selector":{"matchLabels":{"app":"worker"}}}}`), nil
				}
				if args[1] == "pods" {
					return pods, nil
				}
				t.Fatalf("unexpected read: %v", args)
				return nil, nil
			}}
			revision, digest := strings.Repeat("1", 40), "sha256:"+strings.Repeat("a", 64)
			release := declarativerelease.PlanRelease{
				Artifact:                declarativerelease.Artifact{Repository: "ghcr.io/example/worker"},
				Workload:                declarativerelease.Workload{Kind: "Deployment", Namespace: "system", Name: "worker", Container: "worker"},
				Health:                  []declarativerelease.HealthProbe{{Type: "pod-http", Name: "worker", Port: "http", Path: "/healthz", Expected: `"healthy":true`}},
				ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: revision, ExpectedPreviousManifestSHA: revision,
				ExpectedPreviousOCIRevision: revision, ExpectedPreviousImageDigest: digest,
			}
			ctx := declarativerelease.WithPrewritePredecessorHealthWait(context.Background())
			if test.cause == context.Canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			} else if test.cause == context.DeadlineExceeded {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
			}
			target := declarativerelease.TargetIdentity{Present: true, ImageRef: release.Artifact.Repository + "@" + digest, ConfigSHA: revision, ManifestSHA: revision, OCIRevision: revision}
			proof, probeErr := cluster.verifyProbes(ctx, release, target, nil, declarativerelease.Observation{})
			if reads != 2 {
				t.Fatalf("unexpected read count: %d", reads)
			}
			if test.name == "healthy" {
				if probeErr != nil || proof == "" {
					t.Fatalf("healthy probe changed: proof=%q error=%v", proof, probeErr)
				}
				return
			}
			typed := typedPrewritePredecessorHealth(ctx, release, target, probeErr)
			var failure *podHTTPFailure
			if proof != "" || !errors.Is(typed, errPodHTTPHealth) || !errors.Is(typed, declarativerelease.ErrDegradedPredecessorHealth) || !errors.As(typed, &failure) {
				t.Fatalf("probe evidence or classification lost: %v", typed)
			}
			if failure.PodUID != "worker-uid-one" || failure.Phase != test.phase || failure.BodyComplete != test.complete || failure.ErrorClass != test.errorClass ||
				strings.Contains(typed.Error(), "private-upstream-detail") || len(typed.Error()) > 1500 {
				t.Fatalf("incorrect or unbounded propagated evidence: %v", typed)
			}
			if test.cause != nil && !errors.Is(typed, test.cause) {
				t.Fatalf("underlying cause lost: %v", typed)
			}
		})
	}
}
