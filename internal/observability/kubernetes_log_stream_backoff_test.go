package observability

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type logStreamTransport func(*http.Request) (*http.Response, error)

func (f logStreamTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type logStreamReader func([]byte) (int, error)

func (f logStreamReader) Read(p []byte) (int, error) { return f(p) }

func streamBackoffCollector(t *testing.T, body func(*http.Request) io.Reader) (*kubernetesLogCollector, *Pipeline) {
	t.Helper()
	transport := logStreamTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(body(r)), Request: r}, nil
	})
	client, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: "http://unused.invalid", QPS: 1000, Burst: 1000}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(Config{Enabled: true, QueueSize: 32, KubernetesLogPollInterval: 15 * time.Second}, nil)
	return newKubernetesLogCollectorWithClient(p, client), p
}

func collectStreamTarget(c *kubernetesLogCollector, ctx context.Context, target kubernetesLogTarget) {
	var budget atomic.Int64
	budget.Store(10)
	c.collectCursorTarget(ctx, target, &budget)
}

func TestCursorBodyTransportFailureBacksOffNodeAndPreservesAdmittedProgress(t *testing.T) {
	for _, failure := range []struct {
		name string
		err  error
	}{{"deadline", context.DeadlineExceeded}, {"unexpected_eof", io.ErrUnexpectedEOF}} {
		t.Run(failure.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			at := now.Add(-time.Minute)
			c, p := streamBackoffCollector(t, func(*http.Request) io.Reader {
				return io.MultiReader(strings.NewReader(at.Format(time.RFC3339Nano)+" admitted\n"), logStreamReader(func([]byte) (int, error) { return 0, failure.err }))
			})
			c.clock = func() time.Time { return now }
			target := kubernetesLogTarget{pod: runningLogPod("first"), container: "app"}
			collectStreamTarget(c, t.Context(), target)
			cur := c.cursors[logTargetKey(target)]
			state := c.nodeRetries["node"]
			if p.kubernetesLogErrors.Load() != 1 || p.kubernetesLogLines.Load() != 1 || cur.Failures != 1 ||
				!cur.Time.Equal(at) || cur.PendingAt.IsZero() || state.Failures != 1 || state.Probe || !state.NextAttempt.After(now) {
				t.Fatalf("body failure lost cursor or node backoff: cursor=%+v node=%+v errors=%d lines=%d", cur, state, p.kubernetesLogErrors.Load(), p.kubernetesLogLines.Load())
			}
			if _, _, ok := c.beginLogNodeRead("node", now); ok {
				t.Fatal("sibling bypassed failed body node circuit")
			}
			if _, _, ok := c.beginLogNodeRead("healthy", now); !ok {
				t.Fatal("unrelated node was quarantined")
			}
			// Both source and node delays expire; recovery resumes at the last
			// admitted record, rather than resetting to a new five-minute cursor.
			now = maxLogTime(cur.NextAttempt, state.NextAttempt).Add(time.Second)
			recovered, _ := streamBackoffCollector(t, func(r *http.Request) io.Reader {
				if got := r.URL.Query().Get("sinceTime"); got != at.Format(time.RFC3339Nano) {
					t.Errorf("recovery skipped progress: sinceTime=%q", got)
				}
				return strings.NewReader(at.Format(time.RFC3339Nano) + " admitted\n" + at.Add(time.Second).Format(time.RFC3339Nano) + " recovered\n")
			})
			c.client = recovered.client
			collectStreamTarget(c, t.Context(), target)
			if p.kubernetesLogErrors.Load() != 1 || p.kubernetesLogLines.Load() != 2 || c.nodeRetries["node"].Failures != 0 || !c.cursors[logTargetKey(target)].PendingAt.IsZero() {
				t.Fatal("successful recovery failed to drain the retained cursor or close circuit")
			}
		})
	}
}

func TestCursorHalfOpenProbeWaitsForBodyAndDistinguishesCancellation(t *testing.T) {
	for _, test := range []struct {
		name        string
		err         error
		cancel      bool
		wantFailure uint32
		wantErrors  uint64
	}{{"success", io.EOF, false, 0, 0}, {"transport_failure", io.ErrUnexpectedEOF, false, 3, 1},
		{"source_failure", bufio.ErrTooLong, false, 0, 1}, {"parent_canceled", context.DeadlineExceeded, true, 2, 0}} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC()
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			var c *kubernetesLogCollector
			c, p := streamBackoffCollector(t, func(*http.Request) io.Reader {
				return logStreamReader(func([]byte) (int, error) {
					state := c.nodeRetries["node"]
					if !state.Probe || state.Failures != 2 {
						t.Errorf("headers prematurely closed half-open probe: %+v", state)
					}
					if _, _, ok := c.beginLogNodeRead("node", now); ok {
						t.Error("sibling entered while probe body remained unread")
					}
					if test.cancel {
						cancel()
					}
					return 0, test.err
				})
			})
			c.clock = func() time.Time { return now }
			c.nodeRetries = map[string]kubernetesLogNodeRetry{"node": {Failures: 2, NextAttempt: now, Epoch: 7}}
			collectStreamTarget(c, parent, kubernetesLogTarget{pod: runningLogPod("probe"), container: "app"})
			state := c.nodeRetries["node"]
			if state.Probe || state.Failures != test.wantFailure || p.kubernetesLogErrors.Load() != test.wantErrors {
				t.Fatalf("incorrect terminal classification: node=%+v errors=%d", state, p.kubernetesLogErrors.Load())
			}
		})
	}
}

func TestCursorRotationTransportFailureOpensNodeCircuit(t *testing.T) {
	now := time.Now().UTC()
	c, p := streamBackoffCollector(t, func(r *http.Request) io.Reader {
		if strings.HasSuffix(r.URL.Path, "/log") {
			return strings.NewReader("")
		}
		return logStreamReader(func([]byte) (int, error) { return 0, context.DeadlineExceeded })
	})
	c.clock = func() time.Time { return now }
	pod := runningLogPod("rotated")
	pod.UID = types.UID("uid-a")
	target := kubernetesLogTarget{pod: pod, container: "app"}
	c.cursors = map[string]kubernetesLogCursor{logTargetKey(target): {Time: now.Add(-time.Minute), RecoveringRotation: true}}
	collectStreamTarget(c, t.Context(), target)
	if c.nodeRetries["node"].Failures != 1 || p.kubernetesLogErrors.Load() != 1 || !c.cursors[logTargetKey(target)].RecoveringRotation {
		t.Fatalf("rotation body failure bypassed node circuit: node=%+v errors=%d", c.nodeRetries["node"], p.kubernetesLogErrors.Load())
	}
}
