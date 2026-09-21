package diagnosticprobe

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

func TestLoopbackMetricsFixedReadAndMissingFamilies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" || r.Method != "GET" || r.Header.Get("Authorization") != "" {
			t.Error("metrics read exceeded fixed scope")
		}
		fmt.Fprint(w, "# TYPE work_seconds histogram\nwork_seconds_bucket{le=\"0.1\"} 3\nwork_seconds_sum 0.15\nwork_seconds_count 3\nother 9007199254740993\n")
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	port, _ := strconv.Atoi(u.Port())
	raw, err := LoopbackMetrics(context.Background(), port)
	if err != nil {
		t.Fatal(err)
	}
	v, err := selectedMetrics(raw, []string{"work_seconds", "missing"}, port)
	if err != nil {
		t.Fatal(err)
	}
	p, ok := v.(partialValue)
	if !ok || len(p.Gaps) != 1 {
		t.Fatalf("missing family not explicit: %+v", v)
	}
	m := p.Value.(map[string]any)["metrics"].(map[string][]string)
	if len(m["work_seconds"]) != 3 || m["other"] != nil {
		t.Fatal("metric selection ignored")
	}
	if _, err := selectedMetrics(raw, []string{"invalid/path"}, port); err == nil {
		t.Fatal("invalid metric family")
	}
	if _, err := LoopbackMetrics(context.Background(), 80); err == nil {
		t.Fatal("invalid port")
	}
}
