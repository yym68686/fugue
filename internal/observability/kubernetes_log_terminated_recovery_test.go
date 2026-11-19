package observability

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// A terminated Pod may no longer have a pod-log endpoint even while its
// UID/restart-bound CRI files remain available. Failure must preserve the
// original receipt and cursor, then use the existing bounded recovery path.
func TestCursorTerminatedOpenFailureSchedulesRetainedRecovery(t *testing.T) {
	for _, kind := range []string{"current", "previous", "unknown finish"} {
		previous := kind == "previous"
		for _, missing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/missing-%t", kind, missing), func(t *testing.T) {
				c, target, _, _ := rotationFixture(t, 10, "", "")
				now := time.Now().UTC()
				at := now.Add(-time.Minute).Truncate(time.Second)
				c.clock = func() time.Time { return now }
				terminated := &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(at.Add(2 * time.Second))}
				if kind == "unknown finish" {
					terminated = &corev1.ContainerStateTerminated{Reason: "ContainerStatusUnknown"}
					target.pod.Status.ContainerStatuses[0].ContainerID = ""
				}
				if previous {
					target.previous = true
					target.pod.Status.ContainerStatuses[0].RestartCount = 1
					target.pod.Status.ContainerStatuses[0].LastTerminationState.Terminated = terminated
				} else {
					target.pod.Status.ContainerStatuses[0].State.Terminated = terminated
				}
				key := logTargetKey(target)
				hash := sha256.Sum256([]byte("before"))
				c.cursors = map[string]kubernetesLogCursor{key: {Time: at, Boundary: map[[32]byte]int{hash: 1}}}
				directory, _, eligible := rotationLogDirectory(target)
				if !eligible {
					t.Fatal("fixture lacks retained identity")
				}
				podOpens, inventories := 0, 0
				transport := logStreamTransport(func(r *http.Request) (*http.Response, error) {
					code, body := http.StatusOK, ""
					switch r.URL.Path {
					case "/api/v1/namespaces/ns/pods/pod-a/log":
						podOpens++
						code = http.StatusBadRequest
						raw, _ := json.Marshal(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonBadRequest, Code: int32(code), Message: `container "app" in pod "pod-a" is terminated`})
						body = string(raw)
					case directory:
						inventories++
						if missing {
							code, body = http.StatusNotFound, "not found"
						} else {
							body = `<a href="0.log.20260101-010101">retained</a><a href="0.log">current</a>`
						}
					case directory + "0.log.20260101-010101":
						body = at.Format(time.RFC3339Nano) + " stdout F before\n" + at.Add(time.Second).Format(time.RFC3339Nano) + " stdout F retained\n"
					case directory + "0.log":
						body = at.Add(2*time.Second).Format(time.RFC3339Nano) + " stdout F after\n"
					default:
						t.Fatalf("unexpected recovery request: %s", r.URL.Path)
					}
					header := make(http.Header)
					if code == http.StatusBadRequest {
						header.Set("Content-Type", "application/json")
					}
					return &http.Response{StatusCode: code, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
				})
				client, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: "http://unused.invalid", QPS: 1000, Burst: 1000}, &http.Client{Transport: transport})
				if err != nil {
					t.Fatal(err)
				}
				c.client = client
				rotationPoll(t, c, target)
				cur, row := c.cursors[key], c.pipeline.sourceObservations[key]
				if !cur.RecoveringRotation || !cur.Time.Equal(at) || !reflect.DeepEqual(cur.Boundary, map[[32]byte]int{hash: 1}) || cur.PendingAt.IsZero() || cur.NextAttempt.IsZero() || cur.Complete || !cur.DrainedThrough.IsZero() || row.Outcome != "open_error" || row.HTTPStatus != 400 || row.Errors != 1 || inventories != 0 {
					t.Fatalf("terminated open did not preserve failure and schedule bounded recovery: cursor=%+v row=%+v inventories=%d", cur, row, inventories)
				}
				now = cur.NextAttempt.Add(time.Second)
				rotationPoll(t, c, target)
				cur, row = c.cursors[key], c.pipeline.sourceObservations[key]
				if podOpens != 1 {
					t.Fatal("retried unavailable pod log instead of retained recovery")
				}
				if missing {
					if inventories != 1 || cur.Complete || !cur.RecoveringRotation || !cur.Time.Equal(at) || !cur.DrainedThrough.IsZero() || cur.PendingAt.IsZero() || cur.NextAttempt.IsZero() || row.Outcome != "rotation_recovery_error" || row.HTTPStatus != 404 || row.Errors != 2 || c.pipeline.kubernetesLogLines.Load() != 0 {
						t.Fatalf("missing retained logs were hidden or marked delivered: cursor=%+v row=%+v", cur, row)
					}
					if row.LastErrorTarget.ContainerState != "terminated" ||
						row.LastErrorTarget.StatusUnknown != (kind == "unknown finish") ||
						row.LastErrorTarget.FinishedAtPresent != (kind != "unknown finish") {
						t.Fatalf("missing inventory lacked exact listed termination evidence: %+v", row.LastErrorTarget)
					}
				} else if got := rotationMessages(t, c.pipeline); !reflect.DeepEqual(got, []string{"retained", "after"}) || inventories != 2 || cur.Complete != (kind != "unknown finish") || cur.RecoveringRotation || !cur.PendingAt.IsZero() || !cur.Time.Equal(at.Add(2*time.Second)) || row.Errors != 1 || c.pipeline.kubernetesLogCursorGaps.Load() != 0 {
					t.Fatalf("retained recovery lost ordering, coverage or failure evidence: messages=%v cursor=%+v row=%+v", got, cur, row)
				}
			})
		}
	}
}

func TestCursorRetainedRecoveryRequiresTerminalRejectionAndSourceIdentity(t *testing.T) {
	for _, scenario := range []struct {
		name, message string
		code          int
		invalidUID    bool
	}{
		{"other bad request", "invalid request", 400, false},
		{"forbidden", "container is terminated", 403, false},
		{"server failure", "container is terminated", 500, false},
		{"missing identity", "container is terminated", 400, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			c, target, _, _ := rotationFixture(t, 10, "", "")
			if scenario.invalidUID {
				target.pod.UID = ""
			}
			transport := logStreamTransport(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/api/v1/namespaces/ns/pods/pod-a/log" {
					t.Fatalf("unexpected recovery request: %s", r.URL.Path)
				}
				raw, _ := json.Marshal(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: int32(scenario.code), Message: scenario.message})
				header := make(http.Header)
				header.Set("Content-Type", "application/json")
				return &http.Response{StatusCode: scenario.code, Header: header, Body: io.NopCloser(strings.NewReader(string(raw))), Request: r}, nil
			})
			client, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: "http://unused.invalid", QPS: 1000, Burst: 1000}, &http.Client{Transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			c.client = client
			rotationPoll(t, c, target)
			cur, row := c.cursors[logTargetKey(target)], c.pipeline.sourceObservations[logTargetKey(target)]
			if cur.RecoveringRotation || cur.Complete || !cur.DrainedThrough.IsZero() || cur.NextAttempt.IsZero() || row.Outcome != "open_error" || row.HTTPStatus != scenario.code {
				t.Fatalf("nonterminal or unfenced request changed recovery behavior: cursor=%+v row=%+v", cur, row)
			}
		})
	}
}
