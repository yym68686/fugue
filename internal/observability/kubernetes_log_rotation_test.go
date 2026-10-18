package observability

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// The source rotates after the first poll. Its last unread record remains
// available in the authenticated node's retained CRI file.
func TestCursorRecoversUnreadRotationTail(t *testing.T) {
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	rotatedName := "0.log." + at.Add(2*time.Second).Format("20060102-150405") + ".gz"
	directory := "/api/v1/nodes/node-a/proxy/logs/pods/ns_pod-a_uid-a/app/"
	var rotated atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/namespaces/ns/pods/pod-a/log":
			if rotated.Load() {
				fmt.Fprintf(w, "%s after\n", at.Add(3*time.Second).Format(time.RFC3339Nano))
			} else {
				fmt.Fprintf(w, "%s before\n", at.Format(time.RFC3339Nano))
			}
		case directory:
			fmt.Fprintf(w, "<pre><a href=\"0.log\">0.log</a><a href=\"%s\">%s</a></pre>", rotatedName, rotatedName)
		case directory + "0.log":
			fmt.Fprintf(w, "%s stdout F after\n", at.Add(3*time.Second).Format(time.RFC3339Nano))
		case directory + rotatedName:
			w.Header().Set("Content-Type", "application/octet-stream")
			z := gzip.NewWriter(w)
			fmt.Fprintf(z, "%s stdout F before\n%s stdout F retained\n", at.Format(time.RFC3339Nano), at.Add(time.Second).Format(time.RFC3339Nano))
			if err := z.Close(); err != nil {
				t.Error(err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(Config{Enabled: true, QueueSize: 32, MemoryLimitBytes: 1 << 20, KubernetesLogTailLines: 10, KubernetesLogMaxLinesPerCycle: 10, KubernetesLogPollInterval: time.Second}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod-a", UID: types.UID("uid-a")},
		Spec:       corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "app"}}},
		Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "app", ContainerID: "containerd://container-a", RestartCount: 0}}},
	}, container: "app"}
	var budget atomic.Int64
	budget.Store(10)
	c.collectCursorTarget(t.Context(), target, &budget)
	rotated.Store(true)
	budget.Store(10)
	c.collectCursorTarget(t.Context(), target, &budget)
	if got := p.kubernetesLogLines.Load(); got != 3 {
		t.Fatalf("rotation lost unread retained record: got %d lines; want before, retained, after", got)
	}
	var messages []string
	for len(p.queue) > 0 {
		var event Event
		if err := json.Unmarshal((<-p.queue).payload, &event); err != nil {
			t.Fatal(err)
		}
		messages = append(messages, event.Message)
	}
	if !reflect.DeepEqual(messages, []string{"before", "retained", "after"}) {
		t.Fatalf("rotation duplicated or reordered records: %v", messages)
	}
}

func TestRotationEmptyRetainedSourceDoesNotClaimCompleteRecovery(t *testing.T) {
	c, target, rotated, _ := rotationFixture(t, 10, "", "")
	rotationPoll(t, c, target)
	rotated.Store(true)
	cur := c.cursors[logTargetKey(target)]
	cur.RecoveringRotation = true
	c.cursors[logTargetKey(target)] = cur
	rotationPoll(t, c, target)
	if c.pipeline.kubernetesLogCursorGaps.Load() != 1 || c.pipeline.sourceObservations[logTargetKey(target)].Outcome != "rotation_gap" {
		t.Fatal("empty retained source claimed recovery")
	}
}

func TestRotationInventoryChangeCannotReturnMixedFiles(t *testing.T) {
	c, target, _, _ := rotationFixture(t, 10, "", "")
	var listings atomic.Int64
	directory, prefix, ok := rotationLogDirectory(target)
	if !ok {
		t.Fatal("fixture identity")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case directory:
			fmt.Fprint(w, "<a href=\"0.log\">current</a><a href=\"0.log.20260101-010101\">retained</a>")
			if listings.Add(1) > 1 {
				fmt.Fprint(w, "<a href=\"0.log.20260101-020202\">new rotation</a>")
			}
		case directory + prefix:
			fmt.Fprint(w, "2026-01-01T00:00:00Z stdout F current\n")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	c.client = client
	reader, err := c.openRotationLogs(t.Context(), target)
	if reader != nil {
		reader.Close()
	}
	if err == nil || reader != nil {
		t.Fatal("mixed rotation inventory accepted")
	}
}

type rotationCloseProbe struct {
	io.Reader
	closes int
}

func (r *rotationCloseProbe) Close() error { r.closes++; return nil }

func TestRotationCloseReleasesUnconsumedCurrentAndRetainedFiles(t *testing.T) {
	current := &rotationCloseProbe{Reader: strings.NewReader("2026-01-01T00:00:01Z stdout F current\n")}
	old := &rotationCloseProbe{Reader: strings.NewReader("2026-01-01T00:00:00Z stdout F old\n")}
	r := &rotationLogReader{files: []rotationLogFile{{"0.log.20260101-010101", "0.log.20260101-010101"}, {"0.log", "0.log"}}, current: current, get: func(string) (io.ReadCloser, error) { return old, nil }, maxLine: 128, partial: map[string]*rotationCRIRecord{}}
	buf := make([]byte, 1)
	if n, err := r.Read(buf); n != 1 || err != nil {
		t.Fatal(n, err)
	}
	r.Close()
	r.Close()
	if current.closes != 1 || old.closes != 1 {
		t.Fatalf("source reader leaked or double-closed: current=%d old=%d", current.closes, old.closes)
	}
}

func rotationFixture(t *testing.T, tail int64, retained, current string) (*kubernetesLogCollector, kubernetesLogTarget, *atomic.Bool, *atomic.Bool) {
	t.Helper()
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	expand := func(s string) string {
		for i, token := range []string{"A", "B", "C"} {
			s = strings.ReplaceAll(s, token+" ", at.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano)+" ")
		}
		return s
	}
	retained, current = expand(retained), expand(current)
	name := "0.log." + at.Add(2*time.Second).Format("20060102-150405") + ".gz"
	dir := "/api/v1/nodes/node-a/proxy/logs/pods/ns_pod-a_uid-a/app/"
	rotated, denied := &atomic.Bool{}, &atomic.Bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/namespaces/ns/pods/pod-a/log":
			if rotated.Load() {
				fmt.Fprint(w, strings.ReplaceAll(current, " stdout F ", " "))
			} else {
				fmt.Fprintf(w, "%s before\n", at.Format(time.RFC3339Nano))
			}
		case dir:
			if denied.Load() {
				http.Error(w, "unavailable", http.StatusForbidden)
				return
			}
			fmt.Fprintf(w, "<a href=\"%s\">retained</a><a href=\"0.log\">current</a>", name)
		case dir + "0.log":
			fmt.Fprint(w, current)
		case dir + name:
			z := gzip.NewWriter(w)
			fmt.Fprint(z, retained)
			if err := z.Close(); err != nil {
				t.Error(err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(Config{Enabled: true, QueueSize: 64, MemoryLimitBytes: 1 << 20, KubernetesLogTailLines: tail, KubernetesLogMaxLinesPerCycle: 64, KubernetesLogPollInterval: time.Second}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod-a", UID: types.UID("uid-a")},
		Spec:       corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "app"}}},
		Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "app", ContainerID: "containerd://container-a"}}},
	}, container: "app"}
	return c, target, rotated, denied
}
func rotationPoll(t *testing.T, c *kubernetesLogCollector, target kubernetesLogTarget) {
	t.Helper()
	var budget atomic.Int64
	budget.Store(64)
	c.collectCursorTarget(t.Context(), target, &budget)
}
func rotationMessages(t *testing.T, p *Pipeline) []string {
	t.Helper()
	var out []string
	for len(p.queue) > 0 {
		var event Event
		if err := json.Unmarshal((<-p.queue).payload, &event); err != nil {
			t.Fatal(err)
		}
		out = append(out, event.Message)
	}
	return out
}
func TestRotationReplayResumesBudgetedEqualTimestampOccurrences(t *testing.T) {
	c, target, rotated, _ := rotationFixture(t, 1, "A stdout F before\nB stdout F same\nB stdout F same\n", "B stdout F same\nC stdout F after\n")
	rotationPoll(t, c, target)
	rotated.Store(true)
	for i := 0; i < 5; i++ {
		rotationPoll(t, c, target)
	}
	if got := rotationMessages(t, c.pipeline); !reflect.DeepEqual(got, []string{"before", "same", "same", "same", "after"}) {
		t.Fatalf("budgeted replay lost/duplicated records: %v", got)
	}
	if c.cursors[logTargetKey(target)].RecoveringRotation || c.pipeline.kubernetesLogCursorGaps.Load() != 0 {
		t.Fatal("completed replay retained pending state or reported a gap")
	}
}
func TestRotationReadFailurePreservesCursorAndCanRetry(t *testing.T) {
	c, target, rotated, denied := rotationFixture(t, 10, "A stdout F before\nB stdout F retained\n", "C stdout F after\n")
	now := time.Now().UTC()
	c.clock = func() time.Time { return now }
	rotationPoll(t, c, target)
	before := c.cursors[logTargetKey(target)]
	rotated.Store(true)
	denied.Store(true)
	rotationPoll(t, c, target)
	failed := c.cursors[logTargetKey(target)]
	if !failed.Time.Equal(before.Time) || !failed.RecoveringRotation || failed.NextAttempt.IsZero() || c.pipeline.kubernetesLogLines.Load() != 1 {
		t.Fatalf("failed replay advanced or dropped retry state: %+v", failed)
	}
	denied.Store(false)
	now = failed.NextAttempt.Add(time.Second)
	rotationPoll(t, c, target)
	if got := rotationMessages(t, c.pipeline); !reflect.DeepEqual(got, []string{"before", "retained", "after"}) {
		t.Fatal(got)
	}
}
func TestRotationMissingRetainedBoundaryIsReported(t *testing.T) {
	c, target, rotated, _ := rotationFixture(t, 10, "B stdout F retained\n", "C stdout F after\n")
	rotationPoll(t, c, target)
	rotated.Store(true)
	rotationPoll(t, c, target)
	if c.pipeline.kubernetesLogCursorGaps.Load() != 1 {
		t.Fatal("retention loss silently declared recovered")
	}
	if c.pipeline.sourceObservations[logTargetKey(target)].Outcome != "rotation_gap" {
		t.Fatal("missing gap outcome")
	}
}
func TestRotationDirectoryIgnoresForeignPathsAndRestarts(t *testing.T) {
	body := "<a href=\"0.log\">current</a><a href=\"0.log.20260101-010101.gz\">old</a>" +
		"<a href=\"../0.log.20260101-020202\">escape</a><a href=\"https://example.test/0.log\">remote</a>" +
		"<a href=\"1.log.20260101-010101\">foreign</a><a href=\"0.log.20261399-999999\">bad date</a>" +
		"<a href=\"0.log.20260101-010101\">plain equivalent</a><a href=\"0.log.20260101-010101.tmp\">temporary</a>"
	got, err := parseRotationLogFiles(strings.NewReader(body), "0.log")
	want := []rotationLogFile{{"0.log.20260101-010101", "0.log.20260101-010101"}, {"0.log", "0.log"}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal(got, err)
	}
	if _, err = parseRotationLogFiles(strings.NewReader(strings.Repeat("x", 65537)), "0.log"); err == nil {
		t.Fatal("unbounded directory accepted")
	}
	many := "<a href=\"0.log\">current</a>"
	for i := 0; i < 17; i++ {
		many += fmt.Sprintf("<a href=\"0.log.20260101-0101%02d\">old</a>", i)
	}
	if _, err = parseRotationLogFiles(strings.NewReader(many), "0.log"); err == nil {
		t.Fatal("unbounded inventory accepted")
	}
}
func TestRotationDirectoryUsesExactPodAndPreviousContainer(t *testing.T) {
	_, target, _, _ := rotationFixture(t, 10, "", "")
	target.pod.Status.ContainerStatuses[0].RestartCount = 2
	target.pod.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{ContainerID: "containerd://old"}
	target.previous = true
	directory, prefix, ok := rotationLogDirectory(target)
	if !ok || prefix != "1.log" || directory != "/api/v1/nodes/node-a/proxy/logs/pods/ns_pod-a_uid-a/app/" {
		t.Fatal(directory, prefix, ok)
	}
	target.pod.UID = "../foreign"
	if _, _, ok = rotationLogDirectory(target); ok {
		t.Fatal("path traversal accepted")
	}
}
func rawRotationReader(t *testing.T, old, current string, maxLine int) *rotationLogReader {
	t.Helper()
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	fmt.Fprint(z, old)
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	r := &rotationLogReader{
		files:   []rotationLogFile{{"0.log.20260101-000001", "0.log.20260101-000001.gz"}, {"0.log", "0.log"}},
		current: io.NopCloser(strings.NewReader(current)),
		get:     func(string) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(compressed.Bytes())), nil },
		maxLine: maxLine, partial: map[string]*rotationCRIRecord{},
	}
	t.Cleanup(func() { r.Close() })
	return r
}
func TestRotationCRIFragmentsSurviveFilesAndStreamInterleaving(t *testing.T) {
	at := "2026-01-01T00:00:00Z"
	r := rawRotationReader(t, at+" stdout P hel\r\n"+at+" stderr F note\n", at+" stdout F lo\n", 128)
	got, err := io.ReadAll(r)
	if err != nil || string(got) != at+" hel\rlo\n"+at+" note\n" {
		t.Fatalf("fragment reconstruction: %q %v", got, err)
	}
}
func TestRotationCRIRejectsTruncatedMalformedAndOversizedSources(t *testing.T) {
	at := "2026-01-01T00:00:00Z"
	for name, input := range map[string]string{
		"partial_record": at + " stdout P incomplete\n", "partial_file": at + " stdout F cut",
		"invalid_stream": at + " other F message\n", "invalid_timestamp": "yesterday stdout F message\n",
		"large_record": at + " stdout F " + strings.Repeat("x", 129) + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			r := rawRotationReader(t, "", input, 128)
			if _, err := io.ReadAll(r); err == nil {
				t.Fatal("invalid CRI source accepted")
			}
		})
	}
	r := rawRotationReader(t, "", at+" stdout F bounded\n", 128)
	r.decodedBytes = rotationReadLimit
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("decoded-byte bound ignored")
	}
	r = rawRotationReader(t, "", at+" stdout F complete\n"+at+" stdout F cut", 128)
	got, err := io.ReadAll(r)
	if !errors.Is(err, io.ErrUnexpectedEOF) || string(got) != at+" complete\n" {
		t.Fatalf("safe prefix/truncation lost: %q %v", got, err)
	}
}
