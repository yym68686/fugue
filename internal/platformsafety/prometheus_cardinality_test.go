package platformsafety

import (
	"os"
	"strings"
	"testing"
)

func TestProductionPrometheusSurfacesExcludeHighCardinalityIdentity(t *testing.T) {
	t.Parallel()
	worker := readFunctionSource(t, "../../internal/edge/service.go", "func (s *Service) handleMetrics(")
	front := readFunctionSource(t, "../../internal/edgegroupfront/service.go", "func (s *Service) handleMetrics(")
	pipeline := readFunctionSource(t, "../../internal/observability/pipeline.go", "func (p *Pipeline) PrometheusMetrics(")
	prometheus := readTextFile(t, "../../deploy/helm/fugue/templates/observability-prometheus-configmap.yaml")
	alertmanager := readTextFile(t, "../../deploy/helm/fugue/templates/observability-alertmanager-configmap.yaml")
	surfaces := map[string]string{
		"edge worker exposition": worker,
		"edge front exposition":  front,
		"telemetry exposition":   pipeline,
		"prometheus rules":       prometheus,
		"alertmanager routing":   alertmanager,
	}
	for name, source := range surfaces {
		for _, forbidden := range []string{
			"tenant_id", "project_id", "app_id", "edge_id", "edge_group_id", "node_host",
			"hostname", "path_prefix", "route_kind", "client_country", "client_region", "client_asn",
			"bundle_version", "decision_id",
		} {
			if strings.Contains(source, forbidden) {
				t.Fatalf("%s retained forbidden high-cardinality Prometheus identity %q", name, forbidden)
			}
		}
	}
	if !strings.Contains(worker, `component=\"worker\",group=\"%s\",slot=\"%s\",release_epoch=\"%s\",status_class=\"%s\",platform_error_class=\"%s\"`) {
		t.Fatal("edge worker release metric does not expose the exact low-cardinality platform labels")
	}
	clickHouse := readTextFile(t, "../../deploy/helm/fugue/templates/observability-clickhouse-configmap.yaml")
	for _, field := range []string{"tenant_id", "app_id", "hostname", "path_template", "decision_id", "route_generation"} {
		if !strings.Contains(clickHouse, field) {
			t.Fatalf("ClickHouse drilldown lost high-cardinality field %q", field)
		}
	}
}

func readFunctionSource(t *testing.T, path, signature string) string {
	t.Helper()
	text := readTextFile(t, path)
	start := strings.Index(text, signature)
	if start < 0 {
		t.Fatalf("%s does not contain %s", path, signature)
	}
	rest := text[start+len(signature):]
	end := strings.Index(rest, "\n}\n\nfunc ")
	if end < 0 {
		t.Fatalf("%s function %s is unterminated", path, signature)
	}
	return rest[:end]
}

func readTextFile(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
