package observability

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestRotationIncompleteCRIIsSourceLocal(t *testing.T) {
	at := "2026-01-01T00:00:00Z"
	for name, body := range map[string]string{
		"physical_tail": at + " stdout F incomplete",
		"logical_tail":  at + " stdout P incomplete\n",
	} {
		t.Run(name, func(t *testing.T) {
			for _, retained := range []bool{false, true} {
				t.Run(fmt.Sprintf("retained-%t", retained), func(t *testing.T) {
					old, current := "", at+" stdout F complete\n"+body
					if retained {
						old, current = current, ""
					}
					reader := rawRotationReader(t, old, current, 128)
					got, err := io.ReadAll(reader)
					if string(got) != at+" complete\n" || !errors.Is(err, errIncompleteKubernetesLogRecord) || logReadErrorClass(err) != "record_incomplete" || logNodeUnavailable(err) {
						t.Fatalf("source truncation became a node transport failure: prefix=%q class=%s node_failure=%t err=%v", got, logReadErrorClass(err), logNodeUnavailable(err), err)
					}
				})
			}
		})
	}
}

func TestRotationIncompleteCRIDoesNotMaskGzipTruncation(t *testing.T) {
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	if _, err := io.WriteString(z, "2026-01-01T00:00:00Z stdout F incomplete"); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	r := &rotationLogReader{
		files:   []rotationLogFile{{"0.log.20260101-000001", "0.log.20260101-000001.gz"}, {"0.log", "0.log"}},
		current: io.NopCloser(strings.NewReader("")),
		get: func(string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(compressed.Bytes()[:compressed.Len()-8])), nil
		},
		maxLine: 128, partial: map[string]*rotationCRIRecord{},
	}
	t.Cleanup(func() { r.Close() })
	_, err := io.ReadAll(r)
	if !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errIncompleteKubernetesLogRecord) || !logNodeUnavailable(err) {
		t.Fatalf("CRI parser masked gzip truncation: class=%s node_failure=%t err=%v", logReadErrorClass(err), logNodeUnavailable(err), err)
	}
}

func TestRotationIncompleteCRIDoesNotMaskBodyTransportFailure(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled, io.ErrUnexpectedEOF} {
		reader := &rotationLogReader{
			files: []rotationLogFile{{"0.log", "0.log"}},
			current: io.NopCloser(io.MultiReader(strings.NewReader("2026-01-01T00:00:00Z stdout F incomplete"),
				rotationErrorReader{cause})),
			maxLine: 128, partial: map[string]*rotationCRIRecord{},
		}
		_, err := io.ReadAll(reader)
		reader.Close()
		if !errors.Is(err, cause) || errors.Is(err, errIncompleteKubernetesLogRecord) {
			t.Fatalf("CRI parser masked body transport failure: got=%v want=%v", err, cause)
		}
	}
}

// A successful HTTP body can end before the runtime finishes a CRI write.
// Keep this source pending, but do not quarantine healthy siblings on its node.
func TestCursorRetainedIncompleteCRIRetriesWithoutQuarantiningNode(t *testing.T) {
	for _, physical := range []bool{false, true} {
		t.Run(fmt.Sprintf("physical-%t", physical), func(t *testing.T) {
			c, target, _, _ := rotationFixture(t, 10, "", "")
			now := time.Now().UTC()
			at := now.Add(-time.Minute).Truncate(time.Second)
			key := logTargetKey(target)
			c.clock = func() time.Time { return now }
			c.cursors = map[string]kubernetesLogCursor{key: {Time: at, Boundary: map[[32]byte]int{}, RecoveringRotation: true}}
			directory, _, _ := rotationLogDirectory(target)
			completed := false
			transport := logStreamTransport(func(r *http.Request) (*http.Response, error) {
				var body string
				switch r.URL.Path {
				case directory:
					body = `<a href="0.log">current</a>`
				case directory + "0.log":
					body = at.Format(time.RFC3339Nano) + " stdout F before\n"
					if completed {
						body += at.Add(time.Second).Format(time.RFC3339Nano) + " stdout F partial completed\n"
					} else if physical {
						body += at.Add(time.Second).Format(time.RFC3339Nano) + " stdout F partial"
					} else {
						body += at.Add(time.Second).Format(time.RFC3339Nano) + " stdout P partial\n"
					}
				default:
					t.Fatalf("unexpected endpoint: %s", r.URL.Path)
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			client, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: "http://unused.invalid", QPS: 1000, Burst: 1000}, &http.Client{Transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			c.client = client
			rotationPoll(t, c, target)
			cur, observation := c.cursors[key], c.pipeline.sourceObservations[key]
			if got := rotationMessages(t, c.pipeline); !reflect.DeepEqual(got, []string{"before"}) {
				t.Fatalf("partial record admitted or safe prefix lost: %v", got)
			}
			if observation.ErrorClass != "record_incomplete" || observation.LastErrorRead.BodyReadError || !cur.Time.Equal(at) || !cur.RecoveringRotation || cur.PendingAt.IsZero() || cur.NextAttempt.IsZero() || !cur.DrainedThrough.IsZero() || c.pipeline.kubernetesLogCursorGaps.Load() != 0 || c.pipeline.kubernetesLogErrors.Load() != 1 {
				t.Fatalf("source retry lost safety/evidence: cursor=%+v observation=%+v", cur, observation)
			}
			if _, _, allowed := c.beginLogNodeRead(target.pod.Spec.NodeName, now); !allowed {
				t.Fatal("a successful transport with an unfinished CRI record quarantined healthy siblings")
			}
			completed = true
			now = cur.NextAttempt.Add(time.Second)
			rotationPoll(t, c, target)
			if got := rotationMessages(t, c.pipeline); !reflect.DeepEqual(got, []string{"partial completed"}) {
				t.Fatalf("completion lost or duplicated: %v", got)
			}
			cur = c.cursors[key]
			if cur.RecoveringRotation || cur.DrainedThrough.IsZero() || !cur.PendingAt.IsZero() || c.pipeline.kubernetesLogCursorGaps.Load() != 0 || c.nodeRetries[target.pod.Spec.NodeName].Failures != 0 {
				t.Fatalf("completion failed to recover safely: %+v", cur)
			}
		})
	}
}
