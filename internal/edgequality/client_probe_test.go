package edgequality

import (
	"context"
	"errors"
	"net/http/httptrace"
	"testing"
	"time"

	"fugue/internal/routeprobe"
)

func TestClientProbeSeparatesTCPFromTLSAndBusinessWait(t *testing.T) {
	for _, test := range []struct {
		name       string
		connectErr error
		proofErr   error
		proofEdge  string
	}{
		{"verified", nil, nil, "edge-a"},
		{"tcp_failed", errors.New("connection refused"), errors.New("dial failed"), ""},
		{"tls_failed", nil, errors.New("certificate invalid"), ""},
		{"wrong_physical_edge", nil, nil, "edge-b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
				if host != "app.example.test" || path != "/" || address != "8.8.8.8" || state != "" || timeout != time.Second {
					t.Fatal("changed controlled probe destination")
				}
				trace := httptrace.ContextClientTrace(ctx)
				trace.ConnectStart("tcp", "8.8.8.8:443")
				trace.ConnectDone("tcp", "8.8.8.8:443", test.connectErr)
				return routeprobe.Proof{EdgeID: test.proofEdge}, test.proofErr
			}
			result := measureClientPath(context.Background(), "app.example.test", "/", ClientProbeTarget{EdgeID: "edge-a", Address: "8.8.8.8"}, time.Second, probe)
			if !result.TCPAttempted || result.TCPConnectFailure == nil || *result.TCPConnectFailure != (test.connectErr != nil) || result.TCPEstablished != (test.connectErr == nil) {
				t.Fatal("mixed TCP failure with proof failure", result)
			}
			if (result.TCPConnectMS != nil) != (test.connectErr == nil) || result.ProofVerified != (test.name == "verified") {
				t.Fatal(result)
			}
		})
	}
}

func TestClientProbeUnavailableTimingStaysUnknown(t *testing.T) {
	probe := func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		return routeprobe.Proof{EdgeID: "edge-a"}, nil
	}
	result := measureClientPath(context.Background(), "app.example.test", "/", ClientProbeTarget{EdgeID: "edge-a", Address: "8.8.8.8"}, time.Second, probe)
	if result.TCPConnectMS != nil || result.TCPConnectFailure != nil || result.TCPEstablished || result.TCPAttempted || !result.ProofVerified {
		t.Fatal("fabricated successful zero-cost network evidence", result)
	}
}

func TestClientProbeRejectsUnboundedPrivateOrAmbiguousTargets(t *testing.T) {
	for _, targets := range [][]ClientProbeTarget{
		nil,
		{{EdgeID: "edge-a", Address: "127.0.0.1"}},
		{{EdgeID: "edge-a", Address: "10.0.0.1"}},
		{{EdgeID: "edge-a", Address: "8.8.8.8"}, {EdgeID: "edge-a", Address: "9.9.9.9"}},
	} {
		if _, err := CaptureClientPath(context.Background(), "app.example.test", "/", "observer", targets, 1, time.Minute, time.Second); err == nil {
			t.Fatal("accepted unsafe probe targets", targets)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := CaptureClientPath(ctx, "app.example.test", "/", "observer", []ClientProbeTarget{{EdgeID: "edge-a", Address: "8.8.8.8"}}, 1, time.Minute, time.Second)
	if !errors.Is(err, context.Canceled) || len(result.Observations) != 0 || result.RoutingAuthorized || result.Scope != "observer_local" || result.FinishedAt.IsZero() {
		t.Fatal("probe ignored cancellation or gained selection authority", result, err)
	}
}
