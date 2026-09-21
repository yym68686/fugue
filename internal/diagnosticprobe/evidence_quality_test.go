package diagnosticprobe

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/livediagnostics"
)

func TestRequiredMetricsDistinguishAbsentFromZero(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		missing bool
	}{
		{`{"resultType":"vector","result":[]}`, true},
		{`{"resultType":"vector","result":[{"metric":{},"value":[1,"0"]}]}`, false},
		{`{"resultType":"vector","result":[{"metric":{},"value":[1,"NaN"]}]}`, true},
		{`{"resultType":"matrix","result":[{"metric":{},"values":[]}]}`, true},
		{`{"resultType":"matrix","result":[{"metric":{},"values":[[1,"NaN"]]}]}`, true},
		{`{"resultType":"matrix","result":[{"metric":{},"values":[[1,"0"]]}]}`, false},
	} {
		var value any
		if err := json.Unmarshal([]byte(tc.raw), &value); err != nil {
			t.Fatal(err)
		}
		if (missingMetricResult(value) != "") != tc.missing {
			t.Fatalf("wrong availability for %s", tc.raw)
		}
	}
}

func TestPrometheusMissingRequiredSeriesDegradesReport(t *testing.T) {
	var endpoint string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/services/") {
			u, _ := url.Parse(endpoint)
			port, _ := strconv.Atoi(u.Port())
			fmt.Fprintf(w, `{"spec":{"clusterIP":"127.0.0.1","ports":[{"name":"http","port":%d}]}}`, port)
			return
		}
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	}))
	defer server.Close()
	endpoint = server.URL
	k := &kubeReader{client: server.Client(), publicClient: server.Client(), base: endpoint}
	value, err := k.prometheus(context.Background(), livediagnostics.ProbeRequest{}, Collector{Service: &Service{Namespace: "system-test", Name: "metrics-test", Port: "http"}, Queries: map[string]string{"required": "absent_metric"}, RequiredQueries: []string{"required"}})
	if err != nil {
		t.Fatal(err)
	}
	partial, ok := value.(partialValue)
	if !ok || !strings.Contains(strings.Join(partial.Gaps, ";"), "unavailable") {
		t.Fatalf("missing evidence marked complete: %+v", value)
	}
}

func TestProcessSnapshotDeclaresMissingPermissionsAndDisabledAccounting(t *testing.T) {
	root := t.TempDir()
	for name, value := range map[string]string{
		"42/stat":   "42 (sample) S 1 2 3 4 5 6 7 8 9 10 111 222 13 14 15 16 7 18 9000 20",
		"42/cgroup": "0::/sample", "42/schedstat": "100 0 2", "sys/kernel/sched_schedstats": "0",
	} {
		filename := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	value, err := processSchedulingAt(context.Background(), livediagnostics.ProbeRequest{}, root)
	if err != nil {
		t.Fatal(err)
	}
	partial, ok := value.(partialValue)
	if !ok || !strings.Contains(strings.Join(partial.Gaps, ";"), "accounting is disabled") || !strings.Contains(strings.Join(partial.Gaps, ";"), "io unavailable") {
		t.Fatalf("unavailable counters were treated as zero: %+v", value)
	}
}

func TestCPUWindowRejectsRebootsAndCountsStealWithoutGuestDuplication(t *testing.T) {
	now := time.Now()
	makeEvidence := func(at time.Time, boot, stat string) livediagnostics.Evidence {
		data, _ := json.Marshal(nodeWindow{Sources: map[string]string{"sys/kernel/random/boot_id": boot, "stat": stat}})
		return livediagnostics.Evidence{Data: data, ObservedAt: at}
	}
	a := makeEvidence(now, "boot-test", "cpu  100 0 20 100 0 0 0 10 50 0\ncpu0 0")
	b := makeEvidence(now.Add(time.Second), "boot-test", "cpu  150 0 30 130 0 0 0 20 80 0\ncpu0 0")
	summary, err := summarizeNodeWindow(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if summary["cpu_tick_delta"] != uint64(100) || summary["cpu_time_percent"].(map[string]float64)["steal"] != 10 {
		t.Fatalf("wrong node window: %+v", summary)
	}
	b = makeEvidence(now.Add(time.Second), "different-boot", "cpu  150 0 30 130 0 0 0 20 80 0\ncpu0 0")
	if _, err := summarizeNodeWindow(a, b); err == nil {
		t.Fatal("mixed counters from different boot identities")
	}
}

func TestAnnotationProjectionIsExplicitAndExcludesCredentials(t *testing.T) {
	object := map[string]any{"metadata": map[string]any{"annotations": map[string]any{"observation.example/state": "initializing", "api-token": "private", "kubectl.kubernetes.io/last-applied-configuration": "private-env"}}}
	result := projectConfiguredObject(object, Collector{AnnotationKeys: []string{"observation.example/state", "api-token", "kubectl.kubernetes.io/last-applied-configuration"}})
	raw, _ := json.Marshal(result)
	if strings.Contains(string(raw), "private") || !strings.Contains(string(raw), "initializing") {
		t.Fatalf("wrong observation projection: %s", raw)
	}
}
