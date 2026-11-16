package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"
)

type drainEvidenceTransport func(*http.Request) (*http.Response, error)

func (f drainEvidenceTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPodDrainProxyFailureRetainsSafeCauseAndBudget(t *testing.T) {
	for _, tc := range []struct {
		name, cause string
		err         error
		context     func() (context.Context, context.CancelFunc)
	}{
		{"canceled", "context_canceled", context.Canceled, func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, cancel
		}},
		{"deadline", "context_deadline", context.DeadlineExceeded, func() (context.Context, context.CancelFunc) {
			return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		}},
		{"refused", "connection_refused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, nil},
		{"reset", "connection_reset", syscall.ECONNRESET, nil},
		{"eof", "eof", io.EOF, nil},
		{"unknown", "transport_error", errors.New("private server body bearer-secret"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			if tc.context != nil {
				cancel()
				ctx, cancel = tc.context()
			}
			defer cancel()
			client := &kubeClient{baseURL: "https://user:password@cluster.invalid", bearerToken: "bearer-secret",
				client: &http.Client{Transport: drainEvidenceTransport(func(*http.Request) (*http.Response, error) {
					return nil, tc.err
				})}}
			_, err := client.observeReleasePodDrain(ctx, "tenant-test", releaseDrainPod{Name: "pod-test", Port: 19090, AppPorts: []int{8080}})
			if !errors.Is(err, tc.err) {
				t.Fatalf("underlying cause lost: %v", err)
			}
			for _, want := range []string{"cause=" + tc.cause, "stage=request", "namespace=\"tenant-test\"", "pod=\"pod-test\"", "elapsed_ms=", "budget_ms=", "remaining_ms=", "context="} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("missing %s in %v", want, err)
				}
			}
			for _, secret := range []string{"password", "bearer-secret", "cluster.invalid", "nonce="} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("diagnostic exposed %q: %v", secret, err)
				}
			}
		})
	}
}

type drainEvidenceBrokenBody struct{}

func (drainEvidenceBrokenBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (drainEvidenceBrokenBody) Close() error             { return nil }

func TestPodDrainProxyBodyFailureRetainsSafeCause(t *testing.T) {
	client := &kubeClient{baseURL: "https://cluster.invalid", client: &http.Client{Transport: drainEvidenceTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: drainEvidenceBrokenBody{}}, nil
	})}}
	_, err := client.observeReleasePodDrain(context.Background(), "tenant-test", releaseDrainPod{Name: "pod-test", Port: 19090, AppPorts: []int{8080}})
	if !errors.Is(err, io.ErrUnexpectedEOF) || !strings.Contains(err.Error(), "stage=body") || !strings.Contains(err.Error(), "cause=unexpected_eof") {
		t.Fatalf("body cause lost: %v", err)
	}
}

func TestPodDrainObservationsRetainFailuresApartFromSuccessfulPolls(t *testing.T) {
	failure := podDrainObservation{Namespace: "tenant-observation", Pod: "pod-failed", StartedAt: time.Now(),
		FinishedAt: time.Now(), Stage: "request", Cause: "context_deadline", ActiveConnections: -1}
	recordPodDrainObservation(failure)
	for i := 0; i < 300; i++ {
		recordPodDrainObservation(podDrainObservation{Namespace: "tenant-observation", Pod: fmt.Sprintf("pod-%d", i),
			StartedAt: time.Now(), FinishedAt: time.Now(), Stage: "complete", ActiveConnections: 43})
	}
	raw, err := ReleaseDrainObservationSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := raw.(map[string]any)
	latest := snapshot["latest"].([]podDrainObservation)
	failures := snapshot["recent_failures"].([]podDrainObservation)
	if len(latest) > 256 || len(failures) > 64 {
		t.Fatalf("unbounded evidence: latest=%d failures=%d", len(latest), len(failures))
	}
	found := false
	for _, item := range failures {
		found = found || item.Pod == failure.Pod && item.Cause == failure.Cause && item.ActiveConnections == -1
	}
	if !found {
		t.Fatal("successful polls displaced failure evidence")
	}
	if _, err := json.Marshal(snapshot); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReleaseDrainObservationSnapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("snapshot ignored caller cancellation: %v", err)
	}
}

func TestPodDrainBusyObservationRemainsPositiveAndReadOnly(t *testing.T) {
	var requests int
	client := &kubeClient{baseURL: "https://cluster.invalid", client: &http.Client{Transport: drainEvidenceTransport(func(r *http.Request) (*http.Response, error) {
		requests++
		if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/proxy/drain/observe") {
			t.Errorf("unexpected mutating request: %s %s", r.Method, r.URL.Path)
		}
		body := fmt.Sprintf(`{"api_version":"fugue.drain-observation/v1","nonce":%q,"pod":"pod-busy","namespace":"tenant-test","app_ports":[8080],"active_connections":43}`, r.URL.Query().Get("nonce"))
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}}
	result, err := client.observeReleasePodDrain(context.Background(), "tenant-test", releaseDrainPod{Name: "pod-busy", Port: 19090, AppPorts: []int{8080}})
	if err != nil || result.ActiveConnections == nil || *result.ActiveConnections != 43 || requests != 1 {
		t.Fatalf("busy observation changed behavior: %+v err=%v requests=%d", result, err, requests)
	}
}

func TestPodDrainInvalidResponsesStayUnknownAndDoNotExposeBodies(t *testing.T) {
	for _, tc := range []struct {
		name, body, cause string
		status            int
	}{
		{"status", "private-response-body", "http_status", http.StatusForbidden},
		{"json", "private-response-body", "invalid_json", http.StatusOK},
		{"oversized", strings.Repeat("private-response-body", 300), "response_too_large", http.StatusOK},
		{"identity", `{"active_connections":0}`, "incomplete_receipt", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &kubeClient{baseURL: "https://cluster.invalid", client: &http.Client{Transport: drainEvidenceTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})}}
			_, err := client.observeReleasePodDrain(context.Background(), "tenant-test", releaseDrainPod{Name: "pod-invalid", Port: 19090, AppPorts: []int{8080}})
			var failure *podDrainProxyError
			if !errors.As(err, &failure) || failure.observation.ActiveConnections != -1 || failure.observation.Cause != tc.cause {
				t.Fatalf("invalid response became a drain proof: %v", err)
			}
			if strings.Contains(err.Error(), "private-response-body") {
				t.Fatalf("response body leaked: %v", err)
			}
		})
	}
}
