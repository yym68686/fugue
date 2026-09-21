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

func TestProcessFaultWindowExcludesReusedAndRegressedCounters(t *testing.T) {
	start := time.Unix(100, 0)
	encode := func(source string, at time.Time, value any) livediagnostics.Evidence {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return livediagnostics.Evidence{Source: source, ObservedAt: at, Data: data}
	}
	before := []processFact{
		{PID: 1, StartTicks: "10", UserTicks: 10, MinorFaults: 100, MajorFaults: 20},
		{PID: 2, StartTicks: "20", UserTicks: 10, MinorFaults: 100, MajorFaults: 20},
		{PID: 3, StartTicks: "30", UserTicks: 10, MinorFaults: 100, MajorFaults: 20},
	}
	after := []processFact{
		{PID: 1, StartTicks: "10", UserTicks: 20, MinorFaults: 150, MajorFaults: 23},
		{PID: 2, StartTicks: "21", UserTicks: 30, MinorFaults: 150, MajorFaults: 23},
		{PID: 3, StartTicks: "30", UserTicks: 30, MinorFaults: 150, MajorFaults: 19},
		{PID: 4, StartTicks: "40", UserTicks: 30, MinorFaults: 150, MajorFaults: 23},
	}
	report := livediagnostics.ProbeReport{Evidence: []livediagnostics.Evidence{
		encode("node-snapshot", start, nodeWindow{Sources: map[string]string{"sys/kernel/random/boot_id": "same", "stat": "cpu 100 0 0 100 0 0 0 0\ncpu0 0\n"}}),
		encode("process-scheduling", start, processWindow{Processes: before}),
		encode("node-snapshot", start.Add(time.Second), nodeWindow{Sources: map[string]string{"sys/kernel/random/boot_id": "same", "stat": "cpu 120 0 0 180 0 0 0 0\ncpu0 0\n"}}),
		encode("process-scheduling", start.Add(time.Second), processWindow{Processes: after}),
	}}
	appendWindowSummary(&report)
	var result struct {
		Processes []processDelta `json:"top_process_cpu"`
	}
	if err := json.Unmarshal(report.Evidence[len(report.Evidence)-1].Data, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Processes) != 1 || result.Processes[0].PID != 1 || result.Processes[0].MinorFaults != 50 || result.Processes[0].MajorFaults != 3 {
		t.Fatalf("invalid process fault deltas: %+v", result.Processes)
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
