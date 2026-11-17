package observability

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestKubernetesLogScannerRequiresCompleteTimestampedRecords(t *testing.T) {
	for _, trial := range []struct {
		name, body string
		want       []string
		wantErr    error
	}{
		{"empty", "", nil, nil},
		{"lf", "2026-01-01T00:00:00Z complete\n", []string{"2026-01-01T00:00:00Z complete"}, nil},
		{"crlf", "2026-01-01T00:00:00Z complete\r\n", []string{"2026-01-01T00:00:00Z complete"}, nil},
		{"tail", "2026-01-01T00:00:00Z incomplete", nil, errIncompleteKubernetesLogRecord},
		{"prefix-tail", "2026-01-01T00:00:00Z complete\n2026-01-01T00:00:01Z incomplete", []string{"2026-01-01T00:00:00Z complete"}, errIncompleteKubernetesLogRecord},
		{"unavailable", "unable to retrieve container logs for containerd://synthetic", []string{"unable to retrieve container logs for containerd://synthetic"}, nil},
		{"invalid", "invalid timestamp", []string{"invalid timestamp"}, nil},
	} {
		t.Run(trial.name, func(t *testing.T) {
			scanner := bufio.NewScanner(strings.NewReader(trial.body))
			scanner.Split(scanKubernetesLogLine)
			var got []string
			for scanner.Scan() {
				got = append(got, scanner.Text())
			}
			if !reflect.DeepEqual(got, trial.want) || !errors.Is(scanner.Err(), trial.wantErr) {
				t.Fatalf("records=%v err=%v; want=%v err=%v", got, scanner.Err(), trial.want, trial.wantErr)
			}
		})
	}
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled, io.ErrUnexpectedEOF} {
		scanner := bufio.NewScanner(io.MultiReader(strings.NewReader("2026-01-01T00:00:00Z incomplete"), rotationErrorReader{cause}))
		scanner.Split(scanKubernetesLogLine)
		if scanner.Scan() || !errors.Is(scanner.Err(), cause) {
			t.Fatalf("incomplete record masked underlying read failure: %v; want %v", scanner.Err(), cause)
		}
	}
	if logNodeUnavailable(errIncompleteKubernetesLogRecord) || logReadErrorClass(errIncompleteKubernetesLogRecord) != "record_incomplete" {
		t.Fatal("source-local incomplete write became a node transport failure")
	}
}

// A non-following kubelet read can end successfully halfway through the CRI
// record currently being written. A later read returns the whole record at the
// same timestamp; the fragment must never become an inclusive boundary witness.
func TestCursorRetriesIncompleteResponseTailWithoutInventingBoundary(t *testing.T) {
	for _, preceding := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete-prefix-%t", preceding), func(t *testing.T) {
			at := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
			var phase atomic.Int64
			var retainedReads atomic.Int64
			c, target, _, _ := rotationFixture(t, 10, "", "")
			now := time.Now().UTC()
			c.clock = func() time.Time { return now }
			directory, _, _ := rotationLogDirectory(target)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, directory) {
					retainedReads.Add(1)
					switch r.URL.Path {
					case directory:
						fmt.Fprint(w, `<a href="0.log.20260101-010101">retained</a><a href="0.log">current</a>`)
					case directory + "0.log.20260101-010101":
						if preceding {
							fmt.Fprintf(w, "%s stdout F before\n", at.Add(-time.Second).Format(time.RFC3339Nano))
						}
						fmt.Fprintf(w, "%s stdout F partial completed\n", at.Format(time.RFC3339Nano))
					case directory + "0.log":
						fmt.Fprintf(w, "%s stdout F after\n", at.Add(time.Second).Format(time.RFC3339Nano))
					}
					return
				}
				if phase.Load() == 2 {
					fmt.Fprintf(w, "%s after\n", at.Add(time.Second).Format(time.RFC3339Nano))
					return
				}
				if preceding {
					fmt.Fprintf(w, "%s before\n", at.Add(-time.Second).Format(time.RFC3339Nano))
				}
				fmt.Fprintf(w, "%s partial", at.Format(time.RFC3339Nano))
				if phase.Load() == 1 {
					fmt.Fprint(w, " completed\r\n")
				}
			}))
			defer server.Close()
			client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
			if err != nil {
				t.Fatal(err)
			}
			c.client = client
			rotationPoll(t, c, target)
			key := logTargetKey(target)
			cur := c.cursors[key]
			var want []string
			if preceding {
				want = append(want, "before")
			}
			if got := rotationMessages(t, c.pipeline); !reflect.DeepEqual(got, want) {
				t.Fatalf("incomplete tail was admitted: got=%v want=%v", got, want)
			}
			if !cur.Time.Before(at) || cur.Complete || cur.PendingAt.IsZero() || !cur.DrainedThrough.IsZero() || cur.Boundary[sha256.Sum256([]byte("partial"))] != 0 {
				t.Fatalf("incomplete tail advanced completeness or boundary: %+v", cur)
			}
			observation := c.pipeline.sourceObservations[key]
			if observation.Outcome != "scan_error" || observation.ErrorClass != "record_incomplete" || c.pipeline.kubernetesLogErrors.Load() != 1 {
				t.Fatalf("incomplete response failure lost: %+v", observation)
			}
			if c.nodeRetries[target.pod.Spec.NodeName].Failures != 0 {
				t.Fatal("a source's incomplete write backed off the entire node")
			}
			// Retry after source backoff; no wall-clock wait.
			now = now.Add(time.Minute)
			phase.Store(1)
			rotationPoll(t, c, target)
			if got := rotationMessages(t, c.pipeline); !reflect.DeepEqual(got, []string{"partial completed"}) {
				t.Fatalf("completion was lost or duplicated: %v", got)
			}
			phase.Store(2)
			rotationPoll(t, c, target)
			if got := rotationMessages(t, c.pipeline); !reflect.DeepEqual(got, []string{"after"}) {
				t.Fatalf("retained recovery replayed the complete boundary: %v", got)
			}
			if retainedReads.Load() == 0 || c.pipeline.kubernetesLogCursorGaps.Load() != 0 || c.pipeline.kubernetesLogErrors.Load() != 1 {
				t.Fatalf("fragment invented rotation loss: reads=%d gaps=%d errors=%d", retainedReads.Load(), c.pipeline.kubernetesLogCursorGaps.Load(), c.pipeline.kubernetesLogErrors.Load())
			}
		})
	}
}
