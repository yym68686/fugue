package diagnosticprobe

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/livediagnostics"
)

func TestProcessStatRetainsIdentityWithSpacesAndParentheses(t *testing.T) {
	line := "42 (worker (child)) S 1 2 3 4 5 6 7 8 9 10 111 222 13 14 15 16 7 18 9000 20"
	got, err := parseProcessStat(42, line)
	if err != nil {
		t.Fatal(err)
	}
	if got.Command != "worker (child)" || got.StartTicks != "9000" || got.UserTicks != 111 || got.SystemTicks != 222 || got.Threads != "7" || got.MinorFaults != 7 || got.MajorFaults != 9 {
		t.Fatalf("bad identity/counters: %+v", got)
	}
}
func TestObjectProjectionDoesNotExportEnvironmentOrManagedFields(t *testing.T) {
	input := map[string]any{"kind": "Pod", "metadata": map[string]any{"name": "sample", "uid": "uid-sample", "managedFields": "internal-payload", "annotations": map[string]any{"credential": "do-not-export"}}, "spec": map[string]any{"nodeName": "node-test", "containers": []any{map[string]any{"env": []any{map[string]any{"name": "PASSWORD", "value": "secret-value"}}}}}, "status": map[string]any{"phase": "Running"}}
	out := projectObject(input, []string{"spec", "spec.nodeName", "spec.containers", "status"})
	raw, _ := json.Marshal(out)
	if strings.Contains(string(raw), "secret-value") || strings.Contains(string(raw), "do-not-export") || strings.Contains(string(raw), "internal-payload") {
		t.Fatalf("sensitive object fields leaked: %s", raw)
	}
	if out["spec.nodeName"] != "node-test" {
		t.Fatal("lost scheduling evidence")
	}
}
func TestObservationPaginationAndAuthorization(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("missing scoped read identity")
		}
		calls++
		if r.URL.Query().Get("limit") != "100" {
			t.Error("missing pagination bound")
		}
		if r.URL.Query().Get("continue") == "" {
			fmt.Fprint(w, `{"metadata":{"resourceVersion":"21","continue":"next"},"items":[{"metadata":{"name":"one","uid":"one"},"status":{"phase":"Pending"}}]}`)
		} else {
			fmt.Fprint(w, `{"metadata":{"resourceVersion":"21"},"items":[{"metadata":{"name":"two","uid":"two"},"status":{"phase":"Running"}}]}`)
		}
	}))
	defer server.Close()
	reader := &kubeReader{client: server.Client(), publicClient: server.Client(), token: "fixture-token", base: server.URL}
	value, err := reader.objects(context.Background(), livediagnostics.ProbeRequest{}, Collector{Resource: "pods", Fields: []string{"status"}})
	if err != nil {
		t.Fatal(err)
	}
	result := value.(map[string]any)
	if calls != 2 || len(result["items"].([]any)) != 2 || result["truncated"] != false {
		t.Fatalf("incomplete pagination evidence: %+v", result)
	}
	if _, err := reader.objects(context.Background(), livediagnostics.ProbeRequest{}, Collector{Resource: "secrets"}); err == nil {
		t.Fatal("credential resource was accepted")
	}
}
func TestCollectorProtocolRejectsUnboundedRequestsAndReportsCancellation(t *testing.T) {
	request := livediagnostics.ProbeRequest{Protocol: livediagnostics.CatalogProtocol, DurationSeconds: 5, MaxOutputBytes: 64 << 10, Config: json.RawMessage(`{"collectors":[{"name":"test","kind":"future-source"}]}`)}
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := Collect(parent, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Quality.Status != "degraded" || !strings.Contains(strings.Join(result.Quality.Gaps, ";"), "session canceled") || !strings.Contains(strings.Join(result.Quality.Gaps, ";"), "not sampled") {
		t.Fatalf("cancellation was presented as complete: %+v", result.Quality)
	}
	request.DurationSeconds = 361
	if _, err := Collect(context.Background(), request); err == nil {
		t.Fatal("accepted excessive duration")
	}
}
func TestReaderDoesNotRetryPastDeadline(t *testing.T) {
	reader := &kubeReader{client: http.DefaultClient, publicClient: http.DefaultClient, lastRequest: time.Now()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reader.get(ctx, "http://unused.invalid", true); err == nil {
		t.Fatal("ignored cancellation")
	}
	if reader.requests != 0 {
		t.Fatal("canceled request reached the transport")
	}
}
