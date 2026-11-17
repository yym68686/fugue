package observability

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
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

// A restored or interrupted replay already requires the retained CRI stream.
// A failure of the unused pod-log endpoint must not prevent that recovery.
func TestCursorPendingRotationDoesNotRequirePodLogOpen(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		t.Run(fmt.Sprintf("checkpoint-%t", checkpoint), func(t *testing.T) {
			c, target, _, _ := rotationFixture(t, 10, "", "")
			now := time.Now().UTC()
			at := now.Add(-time.Minute).Truncate(time.Second)
			c.clock = func() time.Time { return now }
			key := logTargetKey(target)
			hash := sha256.Sum256([]byte("before"))
			if checkpoint {
				c.restored = map[string]logCheckpointPosition{logCheckpointKey(key): {
					Time: at.UnixNano(), Visited: now.Unix(),
					Boundary: map[string]int{base64.RawURLEncoding.EncodeToString(hash[:]): 1},
				}}
			} else {
				c.cursors = map[string]kubernetesLogCursor{key: {
					Time: at, Boundary: map[[32]byte]int{hash: 1}, RecoveringRotation: true,
				}}
			}
			directory, _, _ := rotationLogDirectory(target)
			podOpens, inventories, files := 0, 0, 0
			transport := logStreamTransport(func(r *http.Request) (*http.Response, error) {
				var body string
				switch r.URL.Path {
				case "/api/v1/namespaces/ns/pods/pod-a/log":
					podOpens++
					return nil, context.DeadlineExceeded
				case directory:
					inventories++
					body = `<a href="0.log.20260101-010101">retained</a><a href="0.log">current</a>`
				case directory + "0.log.20260101-010101":
					files++
					body = fmt.Sprintf("%s stdout F before\n%s stdout F retained\n", at.Format(time.RFC3339Nano), at.Add(time.Second).Format(time.RFC3339Nano))
				case directory + "0.log":
					files++
					body = at.Add(2*time.Second).Format(time.RFC3339Nano) + " stdout F after\n"
				default:
					t.Fatalf("unexpected request: %s", r.URL.Path)
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			client, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: "http://unused.invalid", QPS: 1000, Burst: 1000}, &http.Client{Transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			c.client = client
			rotationPoll(t, c, target)
			want := []string{"retained", "after"}
			if checkpoint {
				// A checkpoint witness is not an export acknowledgement.
				want = append([]string{"before"}, want...)
			}
			if got := rotationMessages(t, c.pipeline); !reflect.DeepEqual(got, want) {
				t.Fatalf("unused pod-log failure blocked retained recovery: messages=%v want=%v pod_opens=%d inventories=%d files=%d", got, want, podOpens, inventories, files)
			}
			cur := c.cursors[key]
			if podOpens != 0 || inventories != 2 || files != 2 || cur.RecoveringRotation || !cur.Time.Equal(at.Add(2*time.Second)) || cur.DrainedThrough.IsZero() || c.pipeline.kubernetesLogErrors.Load() != 0 || c.pipeline.kubernetesLogCursorGaps.Load() != 0 || c.nodeRetries[target.pod.Spec.NodeName].Failures != 0 {
				t.Fatalf("recovery changed safety or opened unused stream: cursor=%+v pod_opens=%d inventories=%d files=%d", cur, podOpens, inventories, files)
			}
		})
	}
}

func TestCursorDirectRotationFailureKeepsRetryAndNodeCircuit(t *testing.T) {
	for _, phase := range []string{"inventory", "current", "retained"} {
		t.Run(phase, func(t *testing.T) {
			c, target, _, _ := rotationFixture(t, 10, "", "")
			now := time.Now().UTC()
			at := now.Add(-time.Minute)
			c.clock = func() time.Time { return now }
			key := logTargetKey(target)
			c.cursors = map[string]kubernetesLogCursor{key: {Time: at, RecoveringRotation: true}}
			directory, _, _ := rotationLogDirectory(target)
			transport := logStreamTransport(func(r *http.Request) (*http.Response, error) {
				var body string
				switch r.URL.Path {
				case directory:
					if phase == "inventory" {
						return nil, context.DeadlineExceeded
					}
					body = `<a href="0.log.20260101-010101">retained</a><a href="0.log">current</a>`
				case directory + "0.log":
					if phase == "current" {
						return nil, context.DeadlineExceeded
					}
				case directory + "0.log.20260101-010101":
					return nil, context.DeadlineExceeded
				default:
					t.Fatalf("direct recovery opened unused endpoint: %s", r.URL.Path)
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			client, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: "http://unused.invalid", QPS: 1000, Burst: 1000}, &http.Client{Transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			c.client = client
			rotationPoll(t, c, target)
			cur := c.cursors[key]
			row := c.pipeline.sourceObservations[key]
			if !cur.Time.Equal(at) || !cur.RecoveringRotation || cur.PendingAt.IsZero() || cur.NextAttempt.IsZero() || !cur.DrainedThrough.IsZero() || row.ErrorClass != "deadline" || c.pipeline.kubernetesLogErrors.Load() != 1 || c.pipeline.kubernetesLogCursorGaps.Load() != 0 || c.nodeRetries[target.pod.Spec.NodeName].Failures != 1 {
				t.Fatalf("direct recovery lost failure or advanced coverage: cursor=%+v observation=%+v", cur, row)
			}
		})
	}
}
