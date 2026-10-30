package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestClusterResourcesCommandLimitsAfterSortingAndKeepsFullTotals(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/cluster/resources" || r.URL.Query().Get("node") != "node-a" || r.URL.Query().Get("window_hours") != "24" {
			t.Errorf("unexpected request: %s", r.URL)
		}
		json.NewEncoder(w).Encode(model.ClusterResourceReport{Node: "node-a", TotalServices: 2, TotalPods: 3, CPURequestMilliCores: 300, Services: []model.ClusterServiceResources{{Name: "small", CPURequestMilliCores: 100}, {Name: "large", CPURequestMilliCores: 200}}})
	}))
	defer server.Close()
	for _, limit := range []string{"1", "0"} {
		var out, stderr bytes.Buffer
		err := runWithStreams([]string{"--base-url", server.URL, "--token", "token", "admin", "cluster", "resources", "--node", "node-a", "--limit", limit, "--window-hours", "24", "--json"}, &out, &stderr)
		if err != nil {
			t.Fatal(err)
		}
		var report model.ClusterResourceReport
		if err := json.Unmarshal(out.Bytes(), &report); err != nil {
			t.Fatal(err)
		}
		want := 1
		if limit == "0" {
			want = 2
		}
		if len(report.Services) != want || report.Services[0].Name != "large" || report.TotalServices != 2 || report.CPURequestMilliCores != 300 {
			t.Fatalf("unexpected limited report: %+v", report)
		}
	}
	for _, flags := range [][]string{{"--limit", "-1"}, {"--sort", "invalid"}, {"--order", "invalid"}, {"--min-samples", "0"}} {
		var out, stderr bytes.Buffer
		args := append([]string{"admin", "cluster", "resources", "--node", "node-a"}, flags...)
		if err := runWithStreams(args, &out, &stderr); err == nil {
			t.Fatalf("expected invalid options rejected: %v", flags)
		}
	}
}

func TestSortClusterServiceResourcesSortsUnknownLastAndUsesStableTieBreaks(t *testing.T) {
	usage := 10.0
	rows := []model.ClusterServiceResources{
		{Name: "b", Namespace: "ns", CPURequestMilliCores: 100, CPUUsageMilliCores: &usage},
		{Name: "a", Namespace: "ns", CPURequestMilliCores: 200},
		{Name: "c", Namespace: "ns", CPURequestMilliCores: 100},
	}
	if err := sortClusterServiceResources(rows, "cpu-request", "desc"); err != nil {
		t.Fatal(err)
	}
	if rows[0].Name != "a" || rows[1].Name != "b" || rows[2].Name != "c" {
		t.Fatalf("unexpected descending order: %+v", rows)
	}
	if err := sortClusterServiceResources(rows, "cpu-usage", "desc"); err != nil {
		t.Fatal(err)
	}
	if rows[0].Name != "b" || rows[1].Name != "a" || rows[2].Name != "c" {
		t.Fatalf("unknown metrics should sort last: %+v", rows)
	}
	if err := sortClusterServiceResources(rows, "missing", "desc"); err == nil {
		t.Fatal("expected invalid sort field error")
	}
}

func TestWriteClusterResourceTableIncludesActualAndRecommendationColumns(t *testing.T) {
	var out bytes.Buffer
	usage, recommendation, capacity, memoryUsage, memoryRecommendation := 12.5, int64(45), int64(80), int64(16*1024*1024), int64(32*1024*1024)
	err := writeClusterResourceTable(&out, model.ClusterResourceReport{
		Node: "worker-a", TotalServices: 1, TotalPods: 2, WindowHours: 168, MinSamples: 12,
		Services: []model.ClusterServiceResources{{Namespace: "app", Name: "web", Kind: "app", PodCount: 2, MeasuredPods: 2, CPURequestMilliCores: 100, CPUUsageMilliCores: &usage, CPURecommendedMilliCores: &recommendation, CPUCapacityMilliCores: &capacity, MemoryUsageBytes: &memoryUsage, MemoryRecommendedBytes: &memoryRecommendation}},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"CPU-USED(m)", "CPU-REC(m)", "MEM-USED(MiB)", "MEM-REC(MiB)", "12.50", "45", "80"} {
		if !strings.Contains(out.String(), value) {
			t.Fatalf("expected output to contain %q, got %s", value, out.String())
		}
	}
}
