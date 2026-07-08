package fuguechart_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestNodeJanitorDefaultsToSystemNodeCritical(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	if !strings.Contains(manifest, "name: fugue-fugue-node-janitor") {
		t.Fatalf("rendered manifest missing node-janitor daemonset:\n%s", manifest)
	}
	if !strings.Contains(manifest, "priorityClassName: \"system-node-critical\"") {
		t.Fatalf("node-janitor should render with system-node-critical priority:\n%s", manifest)
	}
	if !strings.Contains(manifest, "hostPID: true") {
		t.Fatalf("node-janitor should render with host PID access for host maintenance:\n%s", manifest)
	}
}

func TestMaintenanceDaemonSetsDefaultToInternalNodes(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "--set", "imagePrePull.images[0]=busybox:latest")
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	for _, name := range []string{
		"fugue-fugue-node-janitor",
		"fugue-fugue-topology-labeler",
		"fugue-fugue-image-prepull",
	} {
		doc := manifestDocumentForKindAndName(manifest, "DaemonSet", name)
		if doc == "" {
			t.Fatalf("rendered manifest missing %s:\n%s", name, manifest)
		}
		for _, want := range []string{
			"affinity:",
			"nodeAffinity:",
			"node-role.kubernetes.io/control-plane",
			"fugue.io/shared-pool",
			"- internal",
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s manifest missing internal-node affinity fragment %q:\n%s", name, want, doc)
			}
		}
	}
}

func TestMaintenanceDaemonSetImagesCanBePreservedIndependently(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set-string", "controller.image.repository=ghcr.io/example/fugue-controller",
		"--set-string", "controller.image.tag=new-controller",
		"--set-string", "nodeJanitor.image.repository=ghcr.io/example/fugue-controller",
		"--set-string", "nodeJanitor.image.tag=old-maintenance",
		"--set-string", "topologyLabeler.image.repository=ghcr.io/example/fugue-controller",
		"--set-string", "topologyLabeler.image.tag=old-maintenance",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	for _, name := range []string{
		"fugue-fugue-node-janitor",
		"fugue-fugue-topology-labeler",
	} {
		doc := manifestDocumentForKindAndName(manifest, "DaemonSet", name)
		if doc == "" {
			t.Fatalf("rendered manifest missing %s:\n%s", name, manifest)
		}
		if !strings.Contains(doc, `image: "ghcr.io/example/fugue-controller:old-maintenance"`) {
			t.Fatalf("%s should preserve maintenance image independently from controller:\n%s", name, doc)
		}
		if strings.Contains(doc, "new-controller") {
			t.Fatalf("%s should not inherit the new controller image when maintenance image is set:\n%s", name, doc)
		}
	}
}

func TestTopologyLabelerUsesNarrowInternalTolerations(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-topology-labeler")
	if doc == "" {
		t.Fatalf("rendered manifest missing topology-labeler daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		"tolerations:",
		"key: node-role.kubernetes.io/control-plane",
		"key: node-role.kubernetes.io/master",
		"key: fugue.io/dedicated",
		"key: fugue.io/schedulable",
		"value: internal",
		`- "true"`,
		"operator: Equal",
		"effect: NoSchedule",
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("topology-labeler manifest missing narrow toleration fragment %q:\n%s", want, doc)
		}
	}
	tolerationsBlock := manifestTolerationsBlock(doc)
	if tolerationsBlock == "" {
		t.Fatalf("topology-labeler manifest missing tolerations block:\n%s", doc)
	}
	for _, unwanted := range []string{
		"operator: Exists",
		"node.kubernetes.io/disk-pressure",
	} {
		if strings.Contains(tolerationsBlock, unwanted) {
			t.Fatalf("topology-labeler tolerations should not contain %q:\n%s", unwanted, tolerationsBlock)
		}
	}

	nodeJanitorDoc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-node-janitor")
	if nodeJanitorDoc == "" {
		t.Fatalf("rendered manifest missing node-janitor daemonset:\n%s", manifest)
	}
	if !strings.Contains(nodeJanitorDoc, "- operator: Exists") {
		t.Fatalf("node-janitor should keep broad abnormal-node cleanup toleration:\n%s", nodeJanitorDoc)
	}
}

func TestAPIAndControllerEvictQuicklyOnNodeFailure(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	for _, tc := range []struct {
		kind string
		name string
	}{
		{kind: "Deployment", name: "fugue-fugue-api"},
		{kind: "Deployment", name: "fugue-fugue-controller"},
	} {
		doc := manifestDocumentForKindAndName(manifest, tc.kind, tc.name)
		if doc == "" {
			t.Fatalf("rendered manifest missing %s %s:\n%s", tc.kind, tc.name, manifest)
		}
		for _, want := range []string{
			"key: node.kubernetes.io/not-ready",
			"key: node.kubernetes.io/unreachable",
			"effect: NoExecute",
			"tolerationSeconds: 30",
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s should evict quickly on node failure; missing %q:\n%s", tc.name, want, doc)
			}
		}
	}
}

func TestDefaultControlPlaneResourceEnvelopeKeepsK3SHeadroom(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	apiDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-api")
	if apiDoc == "" {
		t.Fatalf("rendered manifest missing fugue-fugue-api deployment:\n%s", manifest)
	}
	for _, want := range []string{
		"replicas: 2",
		"maxSurge: 1",
		"cpu: 250m",
		"memory: 768Mi",
		`cpu: "1"`,
		"memory: 1536Mi",
	} {
		if !strings.Contains(apiDoc, want) {
			t.Fatalf("api deployment should keep conservative single-node defaults; missing %q:\n%s", want, apiDoc)
		}
	}

	controllerDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-controller")
	if controllerDoc == "" {
		t.Fatalf("rendered manifest missing fugue-fugue-controller deployment:\n%s", manifest)
	}
	for _, want := range []string{
		"replicas: 2",
		"cpu: 100m",
		"memory: 256Mi",
		`cpu: "1"`,
		"memory: 512Mi",
	} {
		if !strings.Contains(controllerDoc, want) {
			t.Fatalf("controller deployment should have resource boundaries; missing %q:\n%s", want, controllerDoc)
		}
	}
}

func TestAPIDefaultStartupProbeCoversDatabaseBootstrapLockWait(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	apiDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-api")
	if apiDoc == "" {
		t.Fatalf("rendered manifest missing fugue-fugue-api deployment:\n%s", manifest)
	}
	for _, want := range []string{
		"startupProbe:",
		"path: /healthz",
		"failureThreshold: 180",
		"periodSeconds: 2",
	} {
		if !strings.Contains(apiDoc, want) {
			t.Fatalf("api deployment startup probe should cover database bootstrap lock waits; missing %q:\n%s", want, apiDoc)
		}
	}
}

func TestAPIAndControllerReceivePublicAPIDomain(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "--set", "api.apiPublicDomain=api.example.com")
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	for _, name := range []string{
		"fugue-fugue-api",
		"fugue-fugue-controller",
	} {
		doc := manifestDocumentForKindAndName(manifest, "Deployment", name)
		if doc == "" {
			t.Fatalf("rendered manifest missing Deployment %s:\n%s", name, manifest)
		}
		for _, want := range []string{
			"name: FUGUE_API_PUBLIC_DOMAIN",
			"value: \"api.example.com\"",
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s should receive API public domain; missing %q:\n%s", name, want, doc)
			}
		}
	}
}

func TestTelemetryAgentIsDisabledByDefaultAndAPIReceivesObservabilityDefaults(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	if doc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-telemetry-agent"); doc != "" {
		t.Fatalf("telemetry agent should not render by default:\n%s", doc)
	}
	apiDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-api")
	if apiDoc == "" {
		t.Fatalf("rendered manifest missing fugue-fugue-api deployment:\n%s", manifest)
	}
	controllerDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-controller")
	if controllerDoc == "" {
		t.Fatalf("rendered manifest missing fugue-fugue-controller deployment:\n%s", manifest)
	}
	for _, want := range []string{
		"name: FUGUE_OBSERVABILITY_ENABLED",
		"value: \"false\"",
		"name: FUGUE_OBSERVABILITY_RETENTION",
		"value: \"24h\"",
	} {
		if !strings.Contains(apiDoc, want) {
			t.Fatalf("api deployment missing observability default %q:\n%s", want, apiDoc)
		}
	}
	if strings.Contains(apiDoc, "FUGUE_OBSERVABILITY_LOKI_URL") {
		t.Fatalf("api deployment should not render exporter secret envs by default:\n%s", apiDoc)
	}
	if strings.Contains(controllerDoc, "FUGUE_APP_OBSERVABILITY_ENDPOINT") {
		t.Fatalf("controller deployment should not inject app observability endpoint by default:\n%s", controllerDoc)
	}
	for _, want := range []string{
		"name: FUGUE_OBSERVABILITY_ENABLED",
		"value: \"false\"",
		"name: FUGUE_OBSERVABILITY_RETENTION",
		"value: \"24h\"",
	} {
		if !strings.Contains(controllerDoc, want) {
			t.Fatalf("controller deployment missing observability default %q:\n%s", want, controllerDoc)
		}
	}

	cmd = exec.Command("helm", "template", "fugue", chartDir, "--set", "observability.enabled=true")
	cmd.Dir = chartDir
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	manifest = string(output)
	controllerDoc = manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-controller")
	if controllerDoc == "" {
		t.Fatalf("rendered manifest missing fugue-fugue-controller deployment:\n%s", manifest)
	}
	if strings.Contains(controllerDoc, "FUGUE_APP_OBSERVABILITY_ENDPOINT") {
		t.Fatalf("controller deployment should not inject app observability endpoint without telemetry-agent:\n%s", controllerDoc)
	}
	for _, want := range []string{
		"name: FUGUE_OBSERVABILITY_ENABLED",
		"value: \"true\"",
	} {
		if !strings.Contains(controllerDoc, want) {
			t.Fatalf("controller deployment missing observability enabled setting %q:\n%s", want, controllerDoc)
		}
	}
}

func TestTelemetryAgentCanBeRenderedExplicitly(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "observability.enabled=true",
		"--set", "observability.agent.enabled=true",
		"--set-string", "observability.agent.image.repository=ghcr.io/example/fugue-telemetry-agent",
		"--set-string", "observability.agent.image.tag=agent-test",
		"--set-string", "observability.exporterSecret.existingSecretName=fugue-observability-exporters",
		"--set-string", "observability.identity.tenantID=tenant_test",
		"--set-string", "observability.identity.projectID=project_test",
		"--set-string", "observability.identity.appID=app_test",
		"--set-string", "observability.identity.runtimeID=runtime_test",
		"--set-string", "observability.identity.component=telemetry-agent",
		"--set-string", "observability.agent.runtimeLogPaths=/var/log/pods/app.log",
		"--set-string", "observability.agent.prometheusScrapeURLs=http://127.0.0.1:9100/metrics",
		"--set-string", `observability.agent.kubernetesLogs.namespaces=fugue-system\,fg-tenant`,
		"--set-string", "observability.agent.kubernetesLogs.namespacePrefixes=fg-",
		"--set-string", "observability.agent.kubernetesLogs.labelSelector=app.kubernetes.io/managed-by=fugue",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-telemetry-agent")
	if doc == "" {
		t.Fatalf("rendered manifest missing telemetry-agent deployment:\n%s", manifest)
	}
	for _, want := range []string{
		`image: "ghcr.io/example/fugue-telemetry-agent:agent-test"`,
		"name: FUGUE_OBSERVABILITY_ENABLED",
		"value: \"true\"",
		"name: FUGUE_OBSERVABILITY_RETENTION",
		"value: \"24h\"",
		"name: FUGUE_OBSERVABILITY_RUNTIME_LOG_PATHS",
		"value: \"/var/log/pods/app.log\"",
		"name: FUGUE_OBSERVABILITY_PROMETHEUS_SCRAPE_URLS",
		"value: \"http://127.0.0.1:9100/metrics\"",
		"name: FUGUE_OBSERVABILITY_KUBERNETES_LOGS_ENABLED",
		"value: \"true\"",
		"name: FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACES",
		"value: \"fugue-system,fg-tenant\"",
		"name: FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACE_PREFIXES",
		"value: \"fg-\"",
		"name: FUGUE_OBSERVABILITY_KUBERNETES_LOG_LABEL_SELECTOR",
		"value: \"app.kubernetes.io/managed-by=fugue\"",
		"name: FUGUE_OBSERVABILITY_KUBERNETES_LOG_TAIL_LINES",
		"value: \"2000\"",
		"name: FUGUE_OBSERVABILITY_KUBERNETES_LOG_MAX_LINES_PER_CYCLE",
		"value: \"20000\"",
		"name: FUGUE_OBSERVABILITY_QUEUE_SIZE",
		"value: \"32768\"",
		"name: FUGUE_OBSERVABILITY_BATCH_SIZE",
		"value: \"512\"",
		"name: FUGUE_OBSERVABILITY_MAX_PAYLOAD_BYTES",
		"value: \"1048576\"",
		"name: FUGUE_OBSERVABILITY_MEMORY_LIMIT_BYTES",
		"value: \"134217728\"",
		"name: FUGUE_OBSERVABILITY_TENANT_ID",
		"value: \"tenant_test\"",
		"name: FUGUE_OBSERVABILITY_LOKI_URL",
		"name: \"fugue-observability-exporters\"",
		"key: \"FUGUE_OBSERVABILITY_LOKI_URL\"",
		"name: FUGUE_OBSERVABILITY_METRICS_QUERY_URL",
		"key: \"FUGUE_OBSERVABILITY_METRICS_QUERY_URL\"",
		"path: /readyz",
		"path: /healthz",
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("telemetry agent deployment missing %q:\n%s", want, doc)
		}
	}
	apiDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-api")
	controllerDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-controller")
	if controllerDoc == "" {
		t.Fatalf("rendered manifest missing fugue-fugue-controller deployment:\n%s", manifest)
	}
	for _, want := range []string{
		"name: FUGUE_OBSERVABILITY_METRICS_QUERY_URL",
		"key: \"FUGUE_OBSERVABILITY_METRICS_QUERY_URL\"",
		"name: FUGUE_OBSERVABILITY_CLICKHOUSE_DSN",
		"name: \"fugue-observability-exporters\"",
		"key: \"FUGUE_OBSERVABILITY_CLICKHOUSE_DSN\"",
	} {
		if !strings.Contains(apiDoc, want) {
			t.Fatalf("api deployment missing exporter secret env %q:\n%s", want, apiDoc)
		}
	}
	for _, want := range []string{
		"name: FUGUE_APP_OBSERVABILITY_ENDPOINT",
		"value: \"http://fugue-fugue-telemetry-agent.default.svc.cluster.local:7834\"",
		"name: FUGUE_OBSERVABILITY_METRICS_QUERY_URL",
		"key: \"FUGUE_OBSERVABILITY_METRICS_QUERY_URL\"",
		"name: FUGUE_OBSERVABILITY_LOKI_URL",
		"key: \"FUGUE_OBSERVABILITY_LOKI_URL\"",
		"name: FUGUE_OBSERVABILITY_CLICKHOUSE_DSN",
		"key: \"FUGUE_OBSERVABILITY_CLICKHOUSE_DSN\"",
	} {
		if !strings.Contains(controllerDoc, want) {
			t.Fatalf("controller deployment missing app observability endpoint %q:\n%s", want, controllerDoc)
		}
	}
}

func TestControllerReceivesInternalObservabilityQueryEnv(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "observability.enabled=true",
		"--set", "observability.metrics.enabled=true",
		"--set", "observability.logs.enabled=true",
		"--set", "observability.analytics.enabled=true",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	controllerDoc := manifestDocumentForKindAndName(string(output), "Deployment", "fugue-fugue-controller")
	if controllerDoc == "" {
		t.Fatalf("rendered manifest missing fugue-fugue-controller deployment:\n%s", output)
	}
	for _, want := range []string{
		"name: FUGUE_OBSERVABILITY_METRICS_QUERY_URL",
		"value: \"http://fugue-fugue-observability-prometheus:9090/api/v1/query\"",
		"name: FUGUE_OBSERVABILITY_LOKI_URL",
		"value: \"http://fugue-fugue-observability-loki:3100/loki/api/v1/push\"",
		"name: FUGUE_OBSERVABILITY_CLICKHOUSE_DSN",
		"value: \"http://fugue-fugue-observability-clickhouse:8123?database=fugue_observability\"",
	} {
		if !strings.Contains(controllerDoc, want) {
			t.Fatalf("controller deployment missing internal observability query env %q:\n%s", want, controllerDoc)
		}
	}
}

func TestObservabilityPrometheusIsDisabledByDefaultAndCanRender(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	manifest := string(output)
	for _, tc := range []struct {
		kind string
		name string
	}{
		{"Deployment", "fugue-fugue-observability-prometheus"},
		{"Service", "fugue-fugue-observability-prometheus"},
		{"ConfigMap", "fugue-fugue-observability-prometheus"},
	} {
		if doc := manifestDocumentForKindAndName(manifest, tc.kind, tc.name); doc != "" {
			t.Fatalf("%s/%s should not render by default:\n%s", tc.kind, tc.name, doc)
		}
	}

	cmd = exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "observability.metrics.enabled=true",
		"--set", "observability.agent.enabled=true",
		"--set-string", "observability.metrics.image.repository=prom/prometheus",
		"--set-string", "observability.metrics.image.tag=test",
	)
	cmd.Dir = chartDir
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	manifest = string(output)
	deploymentDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-observability-prometheus")
	serviceDoc := manifestDocumentForKindAndName(manifest, "Service", "fugue-fugue-observability-prometheus")
	configDoc := manifestDocumentForKindAndName(manifest, "ConfigMap", "fugue-fugue-observability-prometheus")
	agentServiceDoc := manifestDocumentForKindAndName(manifest, "Service", "fugue-fugue-telemetry-agent")
	for name, doc := range map[string]string{
		"prometheus deployment": deploymentDoc,
		"prometheus service":    serviceDoc,
		"prometheus config":     configDoc,
		"agent service":         agentServiceDoc,
	} {
		if doc == "" {
			t.Fatalf("expected %s to render:\n%s", name, manifest)
		}
	}
	for _, want := range []string{
		`image: "prom/prometheus:test"`,
		"--storage.tsdb.retention.time=24h",
		"--web.enable-remote-write-receiver",
		"checksum/prometheus-config:",
		"path: /-/ready",
		"path: /-/healthy",
	} {
		if !strings.Contains(deploymentDoc, want) {
			t.Fatalf("prometheus deployment missing %q:\n%s", want, deploymentDoc)
		}
	}
	for _, want := range []string{
		"rule_files:",
		"/etc/prometheus/fugue-alerts.yml",
		"job_name: fugue-observability-prometheus",
		"job_name: fugue-telemetry-agent",
		"job_name: fugue-control-plane-pods",
		"job_name: fugue-kubernetes-nodes",
		"job_name: fugue-kubernetes-cadvisor",
		"job_name: fugue-managed-postgres-pods",
		"kubernetes_sd_configs:",
		"regex: \"api|controller|edge|dns|telemetry-agent|observability-prometheus|.*-front\"",
		"regex: \"api;http\"",
		"action: drop",
		"regex: \"http|metrics|health\"",
		"replacement: /api/v1/nodes/$1/proxy/metrics",
		"replacement: /api/v1/nodes/$1/proxy/metrics/cadvisor",
		"target_label: postgres_cluster",
		"fugue-fugue-telemetry-agent:7834",
		"FugueAppNoReadyPods",
		"FugueAppHighErrorRate",
		"FugueEdgeFrontHighClientTCPRetransmits",
		"FugueEdgeNodeTCPRetransmitRateHigh",
		"FugueEdgeNodeTCPMetricsUnavailable",
		"FugueRobustnessBundlePublishRejected",
		"FugueRobustnessNodeGenerationDrift",
		"FugueRobustnessLKGServing",
		"FugueRobustnessBackupStale",
		"fugue_registry_pvc_usage_ratio",
		"FugueRegistryMaintenanceJobMissing",
		"FugueRegistryPVCUsageHigh",
		"FugueRegistryUnreferencedBlobsHigh",
		"FugueRegistryGCOverdue",
	} {
		if !strings.Contains(configDoc, want) {
			t.Fatalf("prometheus config missing %q:\n%s", want, configDoc)
		}
	}
}

func TestControlPlaneMetricsPortsRender(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	apiDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-api")
	controllerDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-controller")
	for name, doc := range map[string]string{
		"api":        apiDoc,
		"controller": controllerDoc,
	} {
		if doc == "" {
			t.Fatalf("rendered manifest missing %s deployment:\n%s", name, manifest)
		}
	}
	for _, want := range []string{
		"name: metrics",
		"containerPort: 9090",
		"name: FUGUE_API_METRICS_BIND_ADDR",
		"value: \":9090\"",
	} {
		if !strings.Contains(apiDoc, want) {
			t.Fatalf("api deployment missing metrics fragment %q:\n%s", want, apiDoc)
		}
	}
	for _, want := range []string{
		"name: metrics",
		"containerPort: 9090",
		"name: FUGUE_CONTROLLER_METRICS_BIND_ADDR",
		"value: \":9090\"",
	} {
		if !strings.Contains(controllerDoc, want) {
			t.Fatalf("controller deployment missing metrics fragment %q:\n%s", want, controllerDoc)
		}
	}
}

func TestObservabilityInternalBackendsInjectLocalEndpoints(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "observability.enabled=true",
		"--set", "observability.agent.enabled=true",
		"--set-string", "observability.agent.image.repository=ghcr.io/example/fugue-telemetry-agent",
		"--set-string", "observability.agent.image.tag=agent-test",
		"--set", "observability.metrics.enabled=true",
		"--set", "observability.logs.enabled=true",
		"--set", "observability.analytics.enabled=true",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	apiDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-api")
	agentDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-telemetry-agent")
	for name, doc := range map[string]string{
		"api":             apiDoc,
		"telemetry-agent": agentDoc,
	} {
		if doc == "" {
			t.Fatalf("rendered manifest missing %s deployment:\n%s", name, manifest)
		}
	}
	for _, want := range []string{
		"name: FUGUE_OBSERVABILITY_METRICS_QUERY_URL",
		"value: \"http://fugue-fugue-observability-prometheus:9090/api/v1/query\"",
		"name: FUGUE_OBSERVABILITY_LOKI_URL",
		"value: \"http://fugue-fugue-observability-loki:3100/loki/api/v1/push\"",
		"name: FUGUE_OBSERVABILITY_CLICKHOUSE_DSN",
		"value: \"http://fugue-fugue-observability-clickhouse:8123?database=fugue_observability\"",
	} {
		if !strings.Contains(apiDoc, want) {
			t.Fatalf("api deployment missing local observability endpoint %q:\n%s", want, apiDoc)
		}
	}
	for _, want := range []string{
		"name: FUGUE_OBSERVABILITY_METRICS_REMOTE_WRITE_URL",
		"value: \"http://fugue-fugue-observability-prometheus:9090/api/v1/write\"",
		"name: FUGUE_OBSERVABILITY_METRICS_QUERY_URL",
		"value: \"http://fugue-fugue-observability-prometheus:9090/api/v1/query\"",
		"name: FUGUE_OBSERVABILITY_LOKI_URL",
		"value: \"http://fugue-fugue-observability-loki:3100/loki/api/v1/push\"",
		"name: FUGUE_OBSERVABILITY_CLICKHOUSE_DSN",
		"value: \"http://fugue-fugue-observability-clickhouse:8123?database=fugue_observability\"",
	} {
		if !strings.Contains(agentDoc, want) {
			t.Fatalf("telemetry agent deployment missing local observability endpoint %q:\n%s", want, agentDoc)
		}
	}
}

func TestObservabilityAlertmanagerIsDisabledByDefaultAndCanRender(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	manifest := string(output)
	for _, tc := range []struct {
		kind string
		name string
	}{
		{"Deployment", "fugue-fugue-observability-alertmanager"},
		{"Service", "fugue-fugue-observability-alertmanager"},
		{"ConfigMap", "fugue-fugue-observability-alertmanager"},
	} {
		if doc := manifestDocumentForKindAndName(manifest, tc.kind, tc.name); doc != "" {
			t.Fatalf("%s/%s should not render by default:\n%s", tc.kind, tc.name, doc)
		}
	}

	cmd = exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "observability.metrics.enabled=true",
		"--set", "observability.alerts.enabled=true",
		"--set-string", "observability.alerts.image.repository=prom/alertmanager",
		"--set-string", "observability.alerts.image.tag=test",
		"--set-string", "observability.alerts.webhookURL=https://alerts.example.test/hook",
	)
	cmd.Dir = chartDir
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	manifest = string(output)
	deploymentDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-observability-alertmanager")
	serviceDoc := manifestDocumentForKindAndName(manifest, "Service", "fugue-fugue-observability-alertmanager")
	configDoc := manifestDocumentForKindAndName(manifest, "ConfigMap", "fugue-fugue-observability-alertmanager")
	for name, doc := range map[string]string{
		"alertmanager deployment": deploymentDoc,
		"alertmanager service":    serviceDoc,
		"alertmanager config":     configDoc,
	} {
		if doc == "" {
			t.Fatalf("expected %s to render:\n%s", name, manifest)
		}
	}
	for _, want := range []string{
		`image: "prom/alertmanager:test"`,
		"--config.file=/etc/alertmanager/alertmanager.yml",
		"path: /-/ready",
		"path: /-/healthy",
	} {
		if !strings.Contains(deploymentDoc, want) {
			t.Fatalf("alertmanager deployment missing %q:\n%s", want, deploymentDoc)
		}
	}
	for _, want := range []string{
		"receiver: default",
		"group_by: [\"tenant_id\", \"project_id\", \"app_id\", \"alertname\"]",
		"url: \"https://alerts.example.test/hook\"",
		"send_resolved: true",
	} {
		if !strings.Contains(configDoc, want) {
			t.Fatalf("alertmanager config missing %q:\n%s", want, configDoc)
		}
	}
}

func TestObservabilityLokiIsDisabledByDefaultAndCanRender(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	manifest := string(output)
	for _, tc := range []struct {
		kind string
		name string
	}{
		{"Deployment", "fugue-fugue-observability-loki"},
		{"Service", "fugue-fugue-observability-loki"},
		{"Service", "fugue-loki"},
		{"ConfigMap", "fugue-fugue-observability-loki"},
	} {
		if doc := manifestDocumentForKindAndName(manifest, tc.kind, tc.name); doc != "" {
			t.Fatalf("%s/%s should not render by default:\n%s", tc.kind, tc.name, doc)
		}
	}

	cmd = exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "observability.logs.enabled=true",
		"--set-string", "observability.logs.image.repository=grafana/loki",
		"--set-string", "observability.logs.image.tag=test",
	)
	cmd.Dir = chartDir
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	manifest = string(output)
	deploymentDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-observability-loki")
	serviceDoc := manifestDocumentForKindAndName(manifest, "Service", "fugue-fugue-observability-loki")
	openEBSAliasServiceDoc := manifestDocumentForKindAndName(manifest, "Service", "fugue-loki")
	configDoc := manifestDocumentForKindAndName(manifest, "ConfigMap", "fugue-fugue-observability-loki")
	for name, doc := range map[string]string{
		"loki deployment":            deploymentDoc,
		"loki service":               serviceDoc,
		"openebs loki alias service": openEBSAliasServiceDoc,
		"loki config":                configDoc,
	} {
		if doc == "" {
			t.Fatalf("expected %s to render:\n%s", name, manifest)
		}
	}
	for _, want := range []string{
		`image: "grafana/loki:test"`,
		"- -config.file=/etc/loki/loki.yml",
		"checksum/loki-config:",
		"path: /ready",
	} {
		if !strings.Contains(deploymentDoc, want) {
			t.Fatalf("loki deployment missing %q:\n%s", want, deploymentDoc)
		}
	}
	for _, want := range []string{
		"auth_enabled: false",
		"retention_period: 24h",
		"max_label_names_per_series: 20",
		"delete_request_store: filesystem",
		"schema: v13",
	} {
		if !strings.Contains(configDoc, want) {
			t.Fatalf("loki config missing %q:\n%s", want, configDoc)
		}
	}
	for _, want := range []string{
		"name: fugue-loki",
		"app.kubernetes.io/component: observability-loki-alias",
		"app.kubernetes.io/component: observability-loki",
		"port: 3100",
		"targetPort: http",
	} {
		if !strings.Contains(openEBSAliasServiceDoc, want) {
			t.Fatalf("openebs loki alias service missing %q:\n%s", want, openEBSAliasServiceDoc)
		}
	}

	cmd = exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "observability.logs.enabled=true",
		"--set", "openebs.alloy.enabled=false",
	)
	cmd.Dir = chartDir
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	if doc := manifestDocumentForKindAndName(string(output), "Service", "fugue-loki"); doc != "" {
		t.Fatalf("openebs loki alias service should not render when openebs alloy is disabled:\n%s", doc)
	}
}

func TestObservabilityClickHouseIsDisabledByDefaultAndCanRender(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	manifest := string(output)
	for _, tc := range []struct {
		kind string
		name string
	}{
		{"Deployment", "fugue-fugue-observability-clickhouse"},
		{"Service", "fugue-fugue-observability-clickhouse"},
		{"ConfigMap", "fugue-fugue-observability-clickhouse"},
	} {
		if doc := manifestDocumentForKindAndName(manifest, tc.kind, tc.name); doc != "" {
			t.Fatalf("%s/%s should not render by default:\n%s", tc.kind, tc.name, doc)
		}
	}

	cmd = exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "observability.analytics.enabled=true",
		"--set-string", "observability.analytics.image.repository=clickhouse/clickhouse-server",
		"--set-string", "observability.analytics.image.tag=test",
	)
	cmd.Dir = chartDir
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	manifest = string(output)
	deploymentDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-observability-clickhouse")
	serviceDoc := manifestDocumentForKindAndName(manifest, "Service", "fugue-fugue-observability-clickhouse")
	configDoc := manifestDocumentForKindAndName(manifest, "ConfigMap", "fugue-fugue-observability-clickhouse")
	for name, doc := range map[string]string{
		"clickhouse deployment": deploymentDoc,
		"clickhouse service":    serviceDoc,
		"clickhouse config":     configDoc,
	} {
		if doc == "" {
			t.Fatalf("expected %s to render:\n%s", name, manifest)
		}
	}
	for _, want := range []string{
		`image: "clickhouse/clickhouse-server:test"`,
		"name: CLICKHOUSE_DB",
		"value: fugue_observability",
		"path: /ping",
		"name: native",
		"containerPort: 9000",
		"mountPath: /docker-entrypoint-initdb.d/init-observability.sql",
		"checksum/clickhouse-config:",
	} {
		if !strings.Contains(deploymentDoc, want) {
			t.Fatalf("clickhouse deployment missing %q:\n%s", want, deploymentDoc)
		}
	}
	for _, want := range []string{
		"<clickhouse>",
		"<console>true</console>",
		"CREATE TABLE IF NOT EXISTS fugue_observability.request_facts",
		"CREATE TABLE IF NOT EXISTS fugue_observability.request_spans",
		"CREATE TABLE IF NOT EXISTS fugue_observability.app_events",
		"CREATE TABLE IF NOT EXISTS fugue_observability.diagnosis_windows_1m",
		"CREATE TABLE IF NOT EXISTS fugue_observability.release_gate_rollups_1m",
		"CREATE MATERIALIZED VIEW IF NOT EXISTS fugue_observability.release_gate_rollups_1m_mv",
		"TTL ts + INTERVAL 1 DAY DELETE",
	} {
		if !strings.Contains(configDoc, want) {
			t.Fatalf("clickhouse config missing %q:\n%s", want, configDoc)
		}
	}
	if !strings.Contains(serviceDoc, "port: 8123") || !strings.Contains(serviceDoc, "port: 9000") {
		t.Fatalf("clickhouse service missing expected ports:\n%s", serviceDoc)
	}
}

func TestStatelessControlPlaneTopologySpreadAllowsFailover(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	for _, tc := range []struct {
		kind     string
		name     string
		maxSurge string
	}{
		{kind: "Deployment", name: "fugue-fugue-api", maxSurge: "maxSurge: 1"},
		{kind: "Deployment", name: "fugue-fugue-controller", maxSurge: "maxSurge: 2"},
	} {
		doc := manifestDocumentForKindAndName(manifest, tc.kind, tc.name)
		if doc == "" {
			t.Fatalf("rendered manifest missing %s %s:\n%s", tc.kind, tc.name, manifest)
		}
		if !strings.Contains(doc, "topologySpreadConstraints:") {
			t.Fatalf("%s should keep topology spread preference:\n%s", tc.name, doc)
		}
		if !strings.Contains(doc, "whenUnsatisfiable: ScheduleAnyway") {
			t.Fatalf("%s should allow temporary co-location after a control-plane node failure:\n%s", tc.name, doc)
		}
		if strings.Contains(doc, "whenUnsatisfiable: DoNotSchedule") {
			t.Fatalf("%s should not hard-block failover scheduling:\n%s", tc.name, doc)
		}
		for _, want := range []string{
			"maxUnavailable: 0",
			tc.maxSurge,
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s should allow surge-first HA rollouts; missing %q:\n%s", tc.name, want, doc)
			}
		}
	}
}

func TestControllerRendersStrictDrainConfiguration(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set-string", "runtime.strictDrain.agent.image.repository=ghcr.io/example/fugue-drain-agent",
		"--set-string", "runtime.strictDrain.agent.image.tag=test-sha",
		"--set", "runtime.strictDrain.minReadySeconds=12",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	doc := manifestDocumentForKindAndName(string(output), "Deployment", "fugue-fugue-controller")
	if doc == "" {
		t.Fatalf("rendered manifest missing controller deployment:\n%s", output)
	}
	for _, want := range []string{
		"name: FUGUE_STRICT_DRAIN_MODE",
		"value: \"connection-aware\"",
		"name: FUGUE_STRICT_DRAIN_MIN_READY_SECONDS",
		"value: \"12\"",
		"name: FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY",
		"value: \"ghcr.io/example/fugue-drain-agent\"",
		"name: FUGUE_DRAIN_AGENT_IMAGE_TAG",
		"value: \"test-sha\"",
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("controller deployment missing strict drain fragment %q:\n%s", want, doc)
		}
	}
}

func TestControlPlaneSingletonSelectorOverridesPrimaryNodeSelector(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	valuesPath := filepath.Join(t.TempDir(), "values.yaml")
	values := `
registry:
  enabled: true
  nodeSelector:
    fugue.install/role: primary
  controlPlaneSingletonNodeSelector:
    fugue.io/control-plane-singleton: "true"
headscale:
  enabled: true
  nodeSelector:
    fugue.install/role: primary
  controlPlaneSingletonNodeSelector:
    fugue.io/control-plane-singleton: "true"
postgres:
  enabled: true
  nodeSelector:
    fugue.install/role: primary
  controlPlaneSingletonNodeSelector:
    fugue.io/control-plane-singleton: "true"
sharedWorkspaceStorage:
  enabled: true
  server:
    clusterIP: 10.43.253.99
    nodeSelector:
      fugue.install/role: primary
    controlPlaneSingletonNodeSelector:
      fugue.io/control-plane-singleton: "true"
  provisioner:
    nodeSelector:
      node-role.kubernetes.io/control-plane: "true"
    controlPlaneSingletonNodeSelector:
      fugue.io/control-plane-singleton: "true"
`
	if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
		t.Fatalf("write temp values: %v", err)
	}

	cmd := exec.Command("helm", "template", "fugue", chartDir, "-f", valuesPath)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	for _, name := range []string{
		"fugue-fugue-registry",
		"fugue-fugue-headscale",
		"fugue-fugue-postgres",
		"fugue-fugue-shared-workspace-nfs",
		"fugue-fugue-shared-workspace-provisioner",
	} {
		doc := manifestDocumentForKindAndName(manifest, "Deployment", name)
		if doc == "" {
			t.Fatalf("rendered manifest missing %s:\n%s", name, manifest)
		}
		if !strings.Contains(doc, "fugue.io/control-plane-singleton: \"true\"") {
			t.Fatalf("%s should render singleton anchor selector:\n%s", name, doc)
		}
		if strings.Contains(doc, "fugue.install/role: primary") || strings.Contains(doc, "node-role.kubernetes.io/control-plane: \"true\"") {
			t.Fatalf("%s should not keep legacy primary/control-plane selector with singleton anchor:\n%s", name, doc)
		}
	}
}

func TestSharedWorkspaceStorageRequiresClusterIP(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	valuesPath := filepath.Join(t.TempDir(), "values.yaml")
	values := `
sharedWorkspaceStorage:
  enabled: true
`
	if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
		t.Fatalf("write temp values: %v", err)
	}

	cmd := exec.Command("helm", "template", "fugue", chartDir, "-f", valuesPath)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected helm template to fail without shared workspace NFS clusterIP:\n%s", output)
	}
	if !strings.Contains(string(output), "sharedWorkspaceStorage.server.clusterIP is required") {
		t.Fatalf("expected clusterIP requirement failure, got:\n%s", output)
	}
}

func TestMovableRWOStorageDefaultsToOpenEBSWorkspaceClass(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	storageClassDoc := manifestDocumentForKindAndName(manifest, "StorageClass", "fugue-workspace-rwo")
	if storageClassDoc == "" {
		t.Fatalf("rendered manifest missing workspace RWO StorageClass:\n%s", manifest)
	}
	for _, want := range []string{
		`provisioner: "local.csi.openebs.io"`,
		"allowVolumeExpansion: true",
		"storage: lvm",
		"volgroup: fugue-vg",
	} {
		if !strings.Contains(storageClassDoc, want) {
			t.Fatalf("workspace RWO StorageClass missing %q:\n%s", want, storageClassDoc)
		}
	}

	legacyStorageClassDoc := manifestDocumentForKindAndName(manifest, "StorageClass", "fugue-local-rwo")
	if legacyStorageClassDoc == "" {
		t.Fatalf("rendered manifest should retain legacy local RWO StorageClass:\n%s", manifest)
	}
	for _, want := range []string{
		`provisioner: "rancher.io/local-path"`,
		"allowVolumeExpansion: false",
	} {
		if !strings.Contains(legacyStorageClassDoc, want) {
			t.Fatalf("legacy local RWO StorageClass missing %q:\n%s", want, legacyStorageClassDoc)
		}
	}
}

func TestRegistryDefaultsToPVCStorage(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	pvcDoc := manifestDocumentForKindAndName(manifest, "PersistentVolumeClaim", "fugue-fugue-registry-data")
	if pvcDoc == "" {
		t.Fatalf("rendered manifest missing registry PVC:\n%s", manifest)
	}
	for _, want := range []string{
		`storageClassName: "fugue-workspace-rwo"`,
		"storage: 200Gi",
	} {
		if !strings.Contains(pvcDoc, want) {
			t.Fatalf("registry PVC missing %q:\n%s", want, pvcDoc)
		}
	}

	deploymentDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-registry")
	if deploymentDoc == "" {
		t.Fatalf("rendered manifest missing registry deployment:\n%s", manifest)
	}
	if !strings.Contains(deploymentDoc, "persistentVolumeClaim:") || !strings.Contains(deploymentDoc, `claimName: "fugue-fugue-registry-data"`) {
		t.Fatalf("registry deployment should mount the registry PVC:\n%s", deploymentDoc)
	}
	if strings.Contains(deploymentDoc, "hostPath:") {
		t.Fatalf("registry deployment should not render hostPath by default:\n%s", deploymentDoc)
	}
}

func TestRegistryMaintenanceJobsRenderByDefault(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	janitor := manifestDocumentForKindAndName(manifest, "CronJob", "fugue-fugue-registry-janitor")
	if janitor == "" {
		t.Fatalf("rendered manifest missing registry janitor:\n%s", manifest)
	}
	for _, want := range []string{
		"kubectl get deployments,statefulsets,daemonsets --all-namespaces",
		"controller retention owns these manifests",
		"fugue.pro/registry-gc-requested-at",
		"protected registry GC is running",
	} {
		if !strings.Contains(janitor, want) {
			t.Fatalf("registry janitor missing %q:\n%s", want, janitor)
		}
	}

	gc := manifestDocumentForKindAndName(manifest, "CronJob", "fugue-fugue-registry-gc")
	if gc == "" {
		t.Fatalf("rendered manifest missing registry GC:\n%s", manifest)
	}
	for _, want := range []string{
		"fugue-registry-maintenance active-imports",
		"fugue-registry-maintenance scan",
		"registry garbage-collect /etc/docker/registry/config.yml",
		"kubectl get deployments,statefulsets,daemonsets --all-namespaces",
		"active_retention_jobs",
		"registry pod did not terminate within 180 seconds",
		"skipped-empty-registry",
		"FUGUE_REGISTRY_PUSH_BASE",
		"abort GC after quiesce",
		"persistentVolumeClaim:",
		`claimName: "fugue-fugue-registry-data"`,
	} {
		if !strings.Contains(gc, want) {
			t.Fatalf("registry GC missing %q:\n%s", want, gc)
		}
	}
	if strings.Contains(gc, "--delete-untagged") {
		t.Fatalf("protected registry GC must not delete untagged digest-pinned manifests:\n%s", gc)
	}
	if lease := manifestDocumentForKindAndName(manifest, "Lease", "fugue-fugue-registry-gc"); lease == "" {
		t.Fatalf("rendered manifest missing registry GC coordination lease:\n%s", manifest)
	}
	api := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-api")
	for _, want := range []string{
		"name: FUGUE_REGISTRY_GC_LEASE_NAME",
		`value: "fugue-fugue-registry-gc"`,
	} {
		if !strings.Contains(api, want) {
			t.Fatalf("API deployment missing registry GC coordination value %q:\n%s", want, api)
		}
	}
}

func TestRegistryMaintenanceJobsCanBeDisabled(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "registryJanitor.enabled=false",
		"--set", "registryGC.enabled=false",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	manifest := string(output)
	if doc := manifestDocumentForKindAndName(manifest, "CronJob", "fugue-fugue-registry-janitor"); doc != "" {
		t.Fatalf("registry janitor should be disabled:\n%s", doc)
	}
	if doc := manifestDocumentForKindAndName(manifest, "CronJob", "fugue-fugue-registry-gc"); doc != "" {
		t.Fatalf("registry GC should be disabled:\n%s", doc)
	}
	if doc := manifestDocumentForKindAndName(manifest, "Lease", "fugue-fugue-registry-gc"); doc != "" {
		t.Fatalf("registry GC coordination lease should be disabled:\n%s", doc)
	}
}

func TestRegistryHostPathRequiresUnsafeOptIn(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "--set", "registry.persistence.mode=hostPath")
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected unsafe hostPath registry render to fail:\n%s", output)
	}
	if !strings.Contains(string(output), "registry.persistence.mode=hostPath is unsafe") {
		t.Fatalf("hostPath failure should explain the unsafe mode:\n%s", output)
	}

	cmd = exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"registry.persistence.mode=hostPath",
		"--set",
		"registry.unsafeHostPath.enabled=true",
		"--set",
		"registry.unsafeHostPath.reason=single-node development",
	)
	cmd.Dir = chartDir
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("unsafe hostPath opt-in should render: %v\n%s", err, output)
	}
	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-registry")
	if doc == "" {
		t.Fatalf("rendered manifest missing registry deployment:\n%s", manifest)
	}
	if !strings.Contains(doc, "hostPath:") || !strings.Contains(doc, `path: "/var/lib/fugue/registry"`) {
		t.Fatalf("unsafe hostPath opt-in should render hostPath volume:\n%s", doc)
	}
}

func TestHeadscaleDefaultsToPVCStorage(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "--set", "headscale.enabled=true", "--set-string", "headscale.domain=mesh.example.com")
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	pvcDoc := manifestDocumentForKindAndName(manifest, "PersistentVolumeClaim", "fugue-fugue-headscale-data")
	if pvcDoc == "" {
		t.Fatalf("rendered manifest missing headscale PVC:\n%s", manifest)
	}
	for _, want := range []string{
		`storageClassName: "fugue-workspace-rwo"`,
		"storage: 1Gi",
	} {
		if !strings.Contains(pvcDoc, want) {
			t.Fatalf("headscale PVC missing %q:\n%s", want, pvcDoc)
		}
	}

	deploymentDoc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-headscale")
	if deploymentDoc == "" {
		t.Fatalf("rendered manifest missing headscale deployment:\n%s", manifest)
	}
	if !strings.Contains(deploymentDoc, "persistentVolumeClaim:") || !strings.Contains(deploymentDoc, `claimName: "fugue-fugue-headscale-data"`) {
		t.Fatalf("headscale deployment should mount the headscale PVC:\n%s", deploymentDoc)
	}
	if strings.Contains(deploymentDoc, "hostPath:") {
		t.Fatalf("headscale deployment should not render hostPath by default:\n%s", deploymentDoc)
	}
}

func TestHeadscaleHostPathRequiresStableNodeSelector(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"headscale.enabled=true",
		"--set-string",
		"headscale.domain=mesh.example.com",
		"--set",
		"headscale.persistence.mode=hostPath",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected headscale hostPath render to fail without selector:\n%s", output)
	}
	if !strings.Contains(string(output), "headscale.persistence.mode=hostPath requires") {
		t.Fatalf("hostPath failure should explain the missing selector:\n%s", output)
	}

	cmd = exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"headscale.enabled=true",
		"--set-string",
		"headscale.domain=mesh.example.com",
		"--set",
		"headscale.persistence.mode=hostPath",
		"--set-string",
		"headscale.nodeSelector.kubernetes\\.io/hostname=control-1",
	)
	cmd.Dir = chartDir
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("headscale hostPath with selector should render: %v\n%s", err, output)
	}
	doc := manifestDocumentForKindAndName(string(output), "Deployment", "fugue-fugue-headscale")
	if doc == "" {
		t.Fatalf("rendered manifest missing headscale deployment:\n%s", output)
	}
	for _, want := range []string{
		"hostPath:",
		`path: "/var/lib/fugue/headscale"`,
		"kubernetes.io/hostname: control-1",
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("headscale hostPath deployment missing %q:\n%s", want, doc)
		}
	}
}

func TestEdgeShadowDaemonSetDefaultsToNoPublicTraffic(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge")
	if doc == "" {
		t.Fatalf("rendered manifest missing edge daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`image: "fugue-edge:latest"`,
		`fugue.io/role.edge: "true"`,
		`fugue.io/schedulable: "true"`,
		`key: fugue.io/tenant`,
		`operator: Exists`,
		`path: "/var/lib/fugue/edge"`,
		`key: FUGUE_EDGE_TLS_ASK_TOKEN`,
		`path: /healthz`,
		`containerPort: 7832`,
		`value: "http://fugue-fugue:80"`,
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("edge daemonset missing %q:\n%s", want, doc)
		}
	}
	for _, unwanted := range []string{
		"hostNetwork: true",
		"hostPort:",
		"containerPort: 80",
		"containerPort: 443",
		"caddy",
	} {
		if strings.Contains(doc, unwanted) {
			t.Fatalf("edge daemonset should not contain %q in shadow mode:\n%s", unwanted, doc)
		}
	}
}

func TestSSHFrontDaemonSetDefaultsToHostNetwork(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge-ssh-front")
	if doc == "" {
		t.Fatalf("rendered manifest missing ssh-front daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`name: ssh-front`,
		`image: "fugue-edge:latest"`,
		`- /usr/local/bin/fugue-ssh-front`,
		`hostNetwork: true`,
		`dnsPolicy: ClusterFirstWithHostNet`,
		`app.kubernetes.io/component: fugue-ssh-front`,
		`fugue.io/rollout-subsystem: public-ssh-data-plane`,
		`type: OnDelete`,
		`name: FUGUE_SSH_PUBLIC_PORT_START`,
		`value: "22000"`,
		`name: FUGUE_SSH_PUBLIC_PORT_END`,
		`value: "32000"`,
		`name: FUGUE_SSH_FRONT_ROUTES_CACHE_PATH`,
		`value: "/var/lib/fugue/edge/ssh-routes-cache.json"`,
		`name: FUGUE_SSH_FRONT_MAX_CONNECTIONS_PER_IP`,
		`value: "0"`,
		`name: FUGUE_SSH_FRONT_MAX_CONNECTION_ATTEMPTS_PER_IP_PER_MINUTE`,
		`value: "0"`,
		`path: "/var/lib/fugue/edge"`,
		`fugue.io/role.edge: "true"`,
		`key: fugue.io/tenant`,
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("ssh-front daemonset missing %q:\n%s", want, doc)
		}
	}
}

func TestSSHFrontDaemonSetCanBeDisabled(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "--set", "edge.sshFront.enabled=false")
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	if doc := manifestDocumentForKindAndName(string(output), "DaemonSet", "fugue-fugue-edge-ssh-front"); doc != "" {
		t.Fatalf("ssh-front daemonset should not render when disabled:\n%s", doc)
	}
}

func TestProductionHAValuesKeepSSHFrontDisabled(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "-f", "values-production-ha.yaml")
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	if doc := manifestDocumentForKindAndName(string(output), "DaemonSet", "fugue-fugue-edge-ssh-front"); doc != "" {
		t.Fatalf("ssh-front daemonset should not render from production HA values before canary rollout:\n%s", doc)
	}
}

func TestEdgeDaemonSetRendersPublicIdentityEnv(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set-string",
		"edge.region=north-america",
		"--set-string",
		"edge.country=us",
		"--set-string",
		"edge.publicHostname=edge-us.fugue.pro",
		"--set-string",
		"edge.publicIPv4=15.204.94.71",
		"--set-string",
		"edge.publicIPv6=2001:db8::15",
		"--set-string",
		"edge.meshIP=100.64.0.15",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge")
	if doc == "" {
		t.Fatalf("rendered manifest missing edge daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`name: FUGUE_EDGE_REGION`,
		`value: "north-america"`,
		`name: FUGUE_EDGE_COUNTRY`,
		`value: "us"`,
		`name: FUGUE_EDGE_PUBLIC_HOSTNAME`,
		`value: "edge-us.fugue.pro"`,
		`name: FUGUE_EDGE_PUBLIC_IPV4`,
		`value: "15.204.94.71"`,
		`name: FUGUE_EDGE_PUBLIC_IPV6`,
		`value: "2001:db8::15"`,
		`name: FUGUE_EDGE_MESH_IP`,
		`value: "100.64.0.15"`,
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("edge daemonset missing public identity env %q:\n%s", want, doc)
		}
	}
}

func TestEdgeCaddyShadowCanBeEnabledWithoutPublicPorts(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"edge.caddy.enabled=true",
		"--set-string",
		"edge.edgeGroupID=edge-group-country-us",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge")
	if doc == "" {
		t.Fatalf("rendered manifest missing edge daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`name: caddy`,
		`image: "caddy:2.10.2-alpine"`,
		`name: FUGUE_EDGE_CADDY_ENABLED`,
		`value: "true"`,
		`name: FUGUE_EDGE_GROUP_ID`,
		`value: "edge-group-country-us"`,
		`name: FUGUE_EDGE_CADDY_ADMIN_URL`,
		`value: "http://127.0.0.1:2019"`,
		`name: FUGUE_EDGE_CADDY_LISTEN_ADDR`,
		`value: "127.0.0.1:18080"`,
		`name: FUGUE_EDGE_CADDY_TLS_MODE`,
		`value: "off"`,
		`name: FUGUE_EDGE_PROXY_LISTEN_ADDR`,
		`value: "127.0.0.1:7833"`,
		`name: FUGUE_EDGE_CADDY_DATA_DIR`,
		`value: "/data/caddy"`,
		`name: FUGUE_EDGE_CADDY_SHARED_TLS_ENABLED`,
		`value: "true"`,
		`admin 127.0.0.1:2019`,
		`name: caddy-config`,
		`name: caddy-data`,
		`path: "/var/lib/fugue/edge/caddy-data"`,
		`mountPath: "/data"`,
		`type: DirectoryOrCreate`,
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("caddy-enabled edge daemonset missing %q:\n%s", want, doc)
		}
	}
	for _, unwanted := range []string{
		"hostNetwork: true",
		"hostPort:",
		"containerPort: 80",
		"containerPort: 443",
	} {
		if strings.Contains(doc, unwanted) {
			t.Fatalf("caddy-enabled edge daemonset should not expose public traffic with %q:\n%s", unwanted, doc)
		}
	}
}

func TestPublicIngressDefaultsKeepDNSProbeAndHeadroom(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"edge.caddy.enabled=true",
		"--set-string",
		"edge.edgeGroupID=edge-group-country-us",
		"--set",
		"dns.enabled=true",
		"--set-string",
		"dns.zone=fugue.pro",
		"--set",
		"dns.answerIPs[0]=15.204.94.71",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	edgeDoc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge")
	if edgeDoc == "" {
		t.Fatalf("rendered manifest missing edge daemonset:\n%s", manifest)
	}
	if got := strings.Count(edgeDoc, "memory: 1Gi"); got < 2 {
		t.Fatalf("edge and caddy containers should both render 1Gi memory limits, got %d:\n%s", got, edgeDoc)
	}

	dnsDoc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-dns")
	if dnsDoc == "" {
		t.Fatalf("rendered manifest missing dns daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`name: FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED`,
		`value: "true"`,
		`name: FUGUE_DNS_EDGE_HEALTH_PROBE_PORT`,
		`value: "443"`,
	} {
		if !strings.Contains(dnsDoc, want) {
			t.Fatalf("dns daemonset missing edge health probe default %q:\n%s", want, dnsDoc)
		}
	}
}

func TestEdgeCaddyShadowRequiresEdgeGroupID(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "--set", "edge.caddy.enabled=true")
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("helm template should reject caddy edge without edge group:\n%s", output)
	}
	if !strings.Contains(string(output), "edge.edgeGroupID is required when edge.caddy.enabled=true") {
		t.Fatalf("expected edge group validation error, got: %v\n%s", err, output)
	}
}

func TestEdgeCaddyInternalTLSCanaryDoesNotExposePublicPorts(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"edge.caddy.enabled=true",
		"--set-string",
		"edge.edgeGroupID=edge-group-country-us",
		"--set",
		"edge.caddy.listenAddr=:18443",
		"--set",
		"edge.caddy.tlsMode=internal",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge")
	if doc == "" {
		t.Fatalf("rendered manifest missing edge daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`name: FUGUE_EDGE_CADDY_LISTEN_ADDR`,
		`value: ":18443"`,
		`name: FUGUE_EDGE_CADDY_TLS_MODE`,
		`value: "internal"`,
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("internal-tls canary edge daemonset missing %q:\n%s", want, doc)
		}
	}
	for _, unwanted := range []string{
		"hostNetwork: true",
		"hostPort:",
		"containerPort: 80",
		"containerPort: 443",
	} {
		if strings.Contains(doc, unwanted) {
			t.Fatalf("internal-tls canary edge daemonset should not expose public traffic with %q:\n%s", unwanted, doc)
		}
	}
}

func TestEdgeCaddyPublicHostPortsRequireExplicitEnable(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"edge.caddy.enabled=true",
		"--set-string",
		"edge.edgeGroupID=edge-group-country-us",
		"--set",
		"edge.caddy.publicHostPorts.enabled=true",
		"--set",
		"edge.caddy.listenAddr=:443",
		"--set",
		"edge.caddy.tlsMode=public-on-demand",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge")
	if doc == "" {
		t.Fatalf("rendered manifest missing edge daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`name: FUGUE_EDGE_CADDY_LISTEN_ADDR`,
		`value: ":443"`,
		`name: FUGUE_EDGE_CADDY_TLS_MODE`,
		`value: "public-on-demand"`,
		`name: http-canary`,
		`containerPort: 80`,
		`hostPort: 80`,
		`name: https-canary`,
		`containerPort: 443`,
		`hostPort: 443`,
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("public-hostport canary edge daemonset missing %q:\n%s", want, doc)
		}
	}
	if strings.Contains(doc, "hostNetwork: true") {
		t.Fatalf("public-hostport canary edge daemonset should not use hostNetwork:\n%s", doc)
	}
}

func TestEdgeBlueGreenRendersFrontAndWorkerSlots(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"edge.caddy.enabled=true",
		"--set-string",
		"edge.edgeGroupID=edge-group-country-us",
		"--set",
		"edge.blueGreen.enabled=true",
		"--set",
		"edge.caddy.tlsMode=public-on-demand",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	if doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge"); doc != "" {
		t.Fatalf("legacy edge daemonset should not render in blue/green mode:\n%s", doc)
	}
	frontDoc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge-front")
	if frontDoc == "" {
		t.Fatalf("rendered manifest missing edge front daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`- /usr/local/bin/fugue-edge-front`,
		`fugue.io/rollout-mode: node-local-blue-green-front`,
		`name: http-public`,
		`hostPort: 80`,
		`name: https-public`,
		`hostPort: 443`,
		`name: FUGUE_EDGE_FRONT_ACTIVE_SLOT_FILE`,
		`value: "/var/lib/fugue/edge-blue-green/active-slot"`,
		`name: FUGUE_EDGE_FRONT_SLOT_A_HTTPS_PORT`,
		`value: "18443"`,
		`name: FUGUE_EDGE_FRONT_SLOT_B_HTTPS_PORT`,
		`value: "28443"`,
		`type: OnDelete`,
	} {
		if !strings.Contains(frontDoc, want) {
			t.Fatalf("edge front daemonset missing %q:\n%s", want, frontDoc)
		}
	}
	for _, tc := range []struct {
		name     string
		slot     string
		hostPort string
	}{
		{name: "fugue-fugue-edge-worker-a", slot: `"a"`, hostPort: "18443"},
		{name: "fugue-fugue-edge-worker-b", slot: `"b"`, hostPort: "28443"},
	} {
		doc := manifestDocumentForKindAndName(manifest, "DaemonSet", tc.name)
		if doc == "" {
			t.Fatalf("rendered manifest missing %s:\n%s", tc.name, manifest)
		}
		for _, want := range []string{
			`fugue.io/rollout-mode: node-local-blue-green-worker`,
			`fugue.io/edge-slot: ` + tc.slot,
			`type: OnDelete`,
			`name: https-worker`,
			`hostPort: ` + tc.hostPort,
			`name: FUGUE_EDGE_CADDY_LISTEN_ADDR`,
			`value: ":` + tc.hostPort + `"`,
			`path: "/var/lib/fugue/edge/slot-` + strings.Trim(tc.slot, `"`) + `"`,
			`path: "/var/lib/fugue/edge/caddy-data/slot-` + strings.Trim(tc.slot, `"`) + `"`,
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s missing %q:\n%s", tc.name, want, doc)
			}
		}
		for _, unwanted := range []string{
			`hostPort: 80`,
			`hostPort: 443`,
		} {
			if strings.Contains(doc, unwanted) {
				t.Fatalf("%s worker should not own public hostPort with %q:\n%s", tc.name, unwanted, doc)
			}
		}
	}
}

func TestEdgeBlueGreenRendersDynamicWorkload(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"edge.caddy.enabled=true",
		"--set-string",
		"edge.edgeGroupID=edge-group-country-us",
		"--set",
		"edge.blueGreen.enabled=true",
		"--set",
		"edge.dynamic.enabled=true",
		"--set",
		"edge.caddy.tlsMode=public-on-demand",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	frontDoc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge-dynamic-front")
	if frontDoc == "" {
		t.Fatalf("rendered manifest missing dynamic edge front daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`app.kubernetes.io/component: edge-dynamic-front`,
		`fugue.io/edge-workload: dynamic`,
		`name: FUGUE_EDGE_FRONT_NODE_ENV_FILE`,
		`value: "/etc/fugue/edge-node.env"`,
		`mountPath: "/etc/fugue/edge-node.env"`,
		`type: FileOrCreate`,
		`hostPort: 80`,
		`hostPort: 443`,
		`fugue.io/role.edge: "true"`,
		`fugue.io/schedulable: "true"`,
	} {
		if !strings.Contains(frontDoc, want) {
			t.Fatalf("dynamic edge front daemonset missing %q:\n%s", want, frontDoc)
		}
	}
	if strings.Contains(frontDoc, `name: FUGUE_EDGE_FRONT_EDGE_GROUP_ID`) {
		t.Fatalf("dynamic edge front should read edge group from node env, not static env:\n%s", frontDoc)
	}

	for _, tc := range []struct {
		name     string
		slot     string
		hostPort string
	}{
		{name: "fugue-fugue-edge-dynamic-worker-a", slot: `"a"`, hostPort: "18443"},
		{name: "fugue-fugue-edge-dynamic-worker-b", slot: `"b"`, hostPort: "28443"},
	} {
		doc := manifestDocumentForKindAndName(manifest, "DaemonSet", tc.name)
		if doc == "" {
			t.Fatalf("rendered manifest missing %s:\n%s", tc.name, manifest)
		}
		for _, want := range []string{
			`app.kubernetes.io/component: edge-dynamic-worker-` + strings.Trim(tc.slot, `"`),
			`fugue.io/edge-workload: dynamic`,
			`name: FUGUE_EDGE_NODE_ENV_FILE`,
			`value: "/etc/fugue/edge-node.env"`,
			`name: FUGUE_EDGE_WORKLOAD_MODE`,
			`value: "dynamic"`,
			`mountPath: "/etc/fugue/edge-node.env"`,
			`type: FileOrCreate`,
			`hostPort: ` + tc.hostPort,
			`memory: 512Mi`,
			`memory: 384Mi`,
			`fugue.io/edge-workload: dynamic`,
			`fugue.io/role.edge: "true"`,
			`fugue.io/schedulable: "true"`,
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s missing %q:\n%s", tc.name, want, doc)
			}
		}
		for _, unwanted := range []string{
			`name: FUGUE_EDGE_TOKEN`,
			`name: FUGUE_EDGE_GROUP_ID`,
			`hostPort: 80`,
			`hostPort: 443`,
		} {
			if strings.Contains(doc, unwanted) {
				t.Fatalf("%s should not contain %q:\n%s", tc.name, unwanted, doc)
			}
		}
	}

	staticFrontDoc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge-front")
	if staticFrontDoc == "" {
		t.Fatalf("rendered manifest missing static edge front daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`key: fugue.io/edge-workload`,
		`operator: NotIn`,
		`- dynamic`,
	} {
		if !strings.Contains(staticFrontDoc, want) {
			t.Fatalf("static edge front should avoid dynamic nodes with %q:\n%s", want, staticFrontDoc)
		}
	}
	if strings.Contains(manifest, "fugue-fugue-edge-dynamic-dns") {
		t.Fatalf("dynamic edge workload must not render a DNS daemonset:\n%s", manifest)
	}
}

func TestEdgeBlueGreenSeparatesPrimaryAndRegionalDocuments(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	valuesPath := filepath.Join(t.TempDir(), "values.yaml")
	values := `
edge:
  edgeGroupID: edge-group-country-us
  caddy:
    enabled: true
    tlsMode: public-on-demand
  blueGreen:
    enabled: true
  groups:
    - name: country-de
      edgeGroupID: edge-group-country-de
      nodeSelector:
        fugue.io/role.edge: "true"
        fugue.io/schedulable: "true"
        fugue.io/location-country-code: de
`
	if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
		t.Fatalf("write values: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "-f", valuesPath)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	primaryWorkerB := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge-worker-b")
	if primaryWorkerB == "" {
		t.Fatalf("rendered manifest missing primary worker-b daemonset:\n%s", manifest)
	}
	if strings.Contains(primaryWorkerB, "fugue-fugue-edge-country-de-front") {
		t.Fatalf("primary worker-b must be a separate YAML document from regional front:\n%s", primaryWorkerB)
	}
	regionalFront := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge-country-de-front")
	if regionalFront == "" {
		t.Fatalf("rendered manifest missing regional front daemonset:\n%s", manifest)
	}
	if strings.Contains(regionalFront, "fugue-fugue-edge-worker-b") {
		t.Fatalf("regional front must be a separate YAML document from primary worker-b:\n%s", regionalFront)
	}
}

func TestEdgeBlueGreenMigrationCanPrewarmWithoutPublicFrontHostPorts(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"edge.caddy.enabled=true",
		"--set",
		"edge.caddy.publicHostPorts.enabled=true",
		"--set-string",
		"edge.edgeGroupID=edge-group-country-us",
		"--set",
		"edge.blueGreen.enabled=true",
		"--set",
		"edge.blueGreen.migration.keepLegacyDirect=true",
		"--set",
		"edge.blueGreen.front.publicHostPorts.enabled=false",
		"--set",
		"edge.caddy.tlsMode=public-on-demand",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	legacyDoc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge")
	if legacyDoc == "" {
		t.Fatalf("migration prewarm should keep legacy direct edge daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`fugue.io/rollout-mode: direct-ondelete-protected`,
		`hostPort: 80`,
		`hostPort: 443`,
	} {
		if !strings.Contains(legacyDoc, want) {
			t.Fatalf("legacy direct edge daemonset missing %q during migration prewarm:\n%s", want, legacyDoc)
		}
	}

	frontDoc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge-front")
	if frontDoc == "" {
		t.Fatalf("migration prewarm should render edge front daemonset:\n%s", manifest)
	}
	if strings.Contains(frontDoc, `hostPort: 80`) || strings.Contains(frontDoc, `hostPort: 443`) {
		t.Fatalf("prewarm front daemonset should not bind public hostPorts while legacy direct owns them:\n%s", frontDoc)
	}
}

func TestEdgeCaddyStaticTLSSecretMountsPrimaryAndGroups(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	valuesPath := t.TempDir() + "/values.yaml"
	values := `
edge:
  edgeGroupID: edge-group-country-us
  caddy:
    enabled: true
    listenAddr: :443
    tlsMode: public-on-demand
    staticTLS:
      enabled: true
      secretName: fugue-app-wildcard-tls
      mountPath: /etc/caddy/static-tls
      certificateKey: tls.crt
      privateKeyKey: tls.key
  groups:
    - name: country-us
      edgeGroupID: edge-group-country-us
      tokenSecret:
        name: fugue-edge-us-scoped-token
        key: FUGUE_EDGE_TOKEN
      nodeSelector:
        fugue.io/role.edge: "true"
        fugue.io/schedulable: "true"
        fugue.io/location-country-code: us
    - name: country-de
      edgeGroupID: edge-group-country-de
      tokenSecret:
        name: fugue-edge-de-scoped-token
        key: FUGUE_EDGE_TOKEN
      nodeSelector:
        fugue.io/role.edge: "true"
        fugue.io/schedulable: "true"
        fugue.io/location-country-code: de
`
	if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
		t.Fatalf("write values: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "-f", valuesPath)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	for _, name := range []string{"fugue-fugue-edge", "fugue-fugue-edge-country-de"} {
		doc := manifestDocumentForKindAndName(manifest, "DaemonSet", name)
		if doc == "" {
			t.Fatalf("rendered manifest missing %s:\n%s", name, manifest)
		}
		for _, want := range []string{
			`name: FUGUE_EDGE_CADDY_STATIC_TLS_CERT_FILE`,
			`value: "/etc/caddy/static-tls/tls.crt"`,
			`name: FUGUE_EDGE_CADDY_STATIC_TLS_KEY_FILE`,
			`value: "/etc/caddy/static-tls/tls.key"`,
			`name: caddy-static-tls`,
			`mountPath: "/etc/caddy/static-tls"`,
			`readOnly: true`,
			`secretName: "fugue-app-wildcard-tls"`,
			`key: "tls.crt"`,
			`path: "tls.crt"`,
			`key: "tls.key"`,
			`path: "tls.key"`,
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s static TLS manifest missing %q:\n%s", name, want, doc)
			}
		}
	}
}

func TestDNSShadowDaemonSetDisabledByDefault(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-dns")
	if doc != "" {
		t.Fatalf("dns daemonset should be disabled by default:\n%s", doc)
	}
	if doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-mesh-recovery"); doc != "" {
		t.Fatalf("mesh recovery daemonset should be disabled by default:\n%s", doc)
	}
}

func TestMeshRecoveryDaemonSetCanBeEnabled(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"meshRecovery.enabled=true",
		"--set",
		"meshRecovery.tokenSecret.name=fugue-mesh-recovery-secret",
		"--set",
		"meshRecovery.signingKeySecret.name=fugue-mesh-recovery-secret",
		"--set",
		"meshRecovery.generation=meshgen-20260525",
		"--set",
		"meshRecovery.loginServer=https://mesh.fugue.pro",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-mesh-recovery")
	if doc == "" {
		t.Fatalf("rendered manifest missing mesh recovery daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`image: "fugue-edge:latest"`,
		`- /usr/local/bin/fugue-mesh-recovery`,
		`name: FUGUE_MESH_RECOVERY_GENERATION`,
		`value: "meshgen-20260525"`,
		`name: FUGUE_MESH_RECOVERY_LOGIN_SERVER`,
		`value: "https://mesh.fugue.pro"`,
		`name: FUGUE_MESH_RECOVERY_TOKEN`,
		`name: "fugue-mesh-recovery-secret"`,
		`key: "FUGUE_MESH_RECOVERY_TOKEN"`,
		`name: FUGUE_MESH_RECOVERY_SIGNING_KEY`,
		`key: "FUGUE_MESH_RECOVERY_SIGNING_KEY"`,
		`path: "/var/lib/fugue/mesh-recovery"`,
		`path: /healthz`,
		`containerPort: 7840`,
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("mesh recovery daemonset missing %q:\n%s", want, doc)
		}
	}
}

func TestMeshRecoveryDaemonSetRequiresExplicitSecrets(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"meshRecovery.enabled=true",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected helm template to fail without mesh recovery secrets:\n%s", output)
	}
	if !strings.Contains(string(output), "meshRecovery.tokenSecret.name is required") {
		t.Fatalf("unexpected helm error:\n%s", output)
	}
}

func TestAPIStaticDNSRecordsEnvRenders(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	valuesPath := t.TempDir() + "/values.yaml"
	values := `
api:
  dnsStaticRecordsJSON: '[{"name":"fugue.pro","type":"MX","values":["10 mail.fugue.pro"],"ttl":300,"record_kind":"protected","status":"active","record_generation":"dnsgen_test"}]'
  platformRoutesJSON: '{"routes":[{"hostname":"api.fugue.pro","kind":"control-plane-api","upstream_url":"http://fugue-fugue.fugue-system.svc.cluster.local:80","edge_group_mode":"region_aware"}]}'
dns:
  nameservers:
    - ns1.dns.fugue.pro
    - ns2.dns.fugue.pro
`
	if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
		t.Fatalf("write values: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "-f", valuesPath)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-api")
	if doc == "" {
		t.Fatalf("rendered manifest missing api deployment:\n%s", manifest)
	}
	for _, want := range []string{
		`name: FUGUE_DNS_STATIC_RECORDS_JSON`,
		`name: FUGUE_PLATFORM_ROUTES_JSON`,
		`name: FUGUE_DNS_NAMESERVERS`,
		`ns1.dns.fugue.pro,ns2.dns.fugue.pro`,
		`api.fugue.pro`,
		`control-plane-api`,
		`fugue.pro`,
		`mail.fugue.pro`,
		`protected`,
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("api deployment missing %q:\n%s", want, doc)
		}
	}
}

func TestDNSShadowDaemonSetCanBeEnabledWithoutPublicPorts(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"dns.enabled=true",
		"--set",
		"dns.answerIPs[0]=203.0.113.10",
		"--set",
		"dns.routeAAnswerIPs[0]=136.112.185.40",
		"--set",
		"dns.nameservers[0]=ns1.dns.fugue.pro",
		"--set",
		"dns.extraZones[0]=oaix.cc",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-dns")
	if doc == "" {
		t.Fatalf("rendered manifest missing dns daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`image: "fugue-edge:latest"`,
		`command:`,
		`- /usr/local/bin/fugue-dns`,
		`fugue.io/role.dns: "true"`,
		`fugue.io/schedulable: "true"`,
		`key: fugue.io/tenant`,
		`operator: Exists`,
		`path: "/var/lib/fugue/dns"`,
		`key: FUGUE_EDGE_TLS_ASK_TOKEN`,
		`name: FUGUE_DNS_ANSWER_IPS`,
		`value: "203.0.113.10"`,
		`name: FUGUE_DNS_ROUTE_A_ANSWER_IPS`,
		`value: "136.112.185.40"`,
		`name: FUGUE_DNS_UDP_ADDR`,
		`value: "127.0.0.1:5353"`,
		`name: FUGUE_DNS_TCP_ADDR`,
		`value: "127.0.0.1:5353"`,
		`name: FUGUE_DNS_NAMESERVERS`,
		`value: "ns1.dns.fugue.pro"`,
		`name: FUGUE_DNS_EXTRA_ZONES`,
		`value: "oaix.cc"`,
		`path: /healthz`,
		`containerPort: 7834`,
		`value: "http://fugue-fugue:80"`,
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("dns daemonset missing %q:\n%s", want, doc)
		}
	}
	for _, unwanted := range []string{
		"hostNetwork: true",
		"hostPort:",
		"containerPort: 53",
	} {
		if strings.Contains(doc, unwanted) {
			t.Fatalf("dns daemonset should not expose public DNS traffic with %q:\n%s", unwanted, doc)
		}
	}
}

func TestDNSPublicHostPortsRequireExplicitEnable(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set",
		"dns.enabled=true",
		"--set",
		"dns.answerIPs[0]=203.0.113.10",
		"--set",
		"dns.publicHostPorts.enabled=true",
		"--set-string",
		"dns.udpAddr=:53",
		"--set-string",
		"dns.tcpAddr=:53",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-dns")
	if doc == "" {
		t.Fatalf("rendered manifest missing dns daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`name: dns-udp`,
		`containerPort: 53`,
		`hostPort: 53`,
		`protocol: UDP`,
		`name: dns-tcp`,
		`protocol: TCP`,
		`name: FUGUE_DNS_UDP_ADDR`,
		`value: ":53"`,
		`name: FUGUE_DNS_TCP_ADDR`,
		`value: ":53"`,
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("public-hostport dns daemonset missing %q:\n%s", want, doc)
		}
	}
	if strings.Contains(doc, "hostNetwork: true") {
		t.Fatalf("public-hostport dns daemonset should not use hostNetwork:\n%s", doc)
	}
}

func TestRegionalEdgeAndDNSGroupsRenderSeparateDaemonSets(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	valuesPath := t.TempDir() + "/values.yaml"
	values := `
edge:
  edgeGroupID: edge-group-country-us
  caddy:
    enabled: true
    listenAddr: :443
    tlsMode: public-on-demand
    publicHostPorts:
      enabled: true
  groups:
    - name: country-us
      edgeGroupID: edge-group-country-us
      tokenSecret:
        name: fugue-edge-us-scoped-token
        key: FUGUE_EDGE_TOKEN
      nodeSelector:
        fugue.io/role.edge: "true"
        fugue.io/schedulable: "true"
        fugue.io/location-country-code: us
    - name: country-de
      edgeGroupID: edge-group-country-de
      tokenSecret:
        name: fugue-edge-de-scoped-token
        key: FUGUE_EDGE_TOKEN
      nodeSelector:
        fugue.io/role.edge: "true"
        fugue.io/schedulable: "true"
        fugue.io/location-country-code: de
dns:
  enabled: true
  extraZones:
    - oaix.cc
  answerIPs:
    - 15.204.94.71
  routeAAnswerIPs:
    - 136.112.185.40
  publicHostPorts:
    enabled: true
  udpAddr: :53
  tcpAddr: :53
  groups:
    - name: country-us
      edgeGroupID: edge-group-country-us
      answerIPs:
        - 15.204.94.71
      tokenSecret:
        name: fugue-edge-us-scoped-token
        key: FUGUE_EDGE_TOKEN
      nodeSelector:
        fugue.io/role.dns: "true"
        fugue.io/schedulable: "true"
        fugue.io/location-country-code: us
    - name: country-de
      edgeGroupID: edge-group-country-de
      answerIPs:
        - 51.38.126.103
      tokenSecret:
        name: fugue-edge-de-scoped-token
        key: FUGUE_EDGE_TOKEN
      nodeSelector:
        fugue.io/role.dns: "true"
        fugue.io/schedulable: "true"
        fugue.io/location-country-code: de
`
	if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
		t.Fatalf("write values: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "-f", valuesPath)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	if strings.Contains(manifest, "Exists---") {
		t.Fatalf("rendered manifest has a malformed group document separator:\n%s", manifest)
	}
	for _, name := range []string{"fugue-fugue-edge-country-us", "fugue-fugue-dns-country-us"} {
		if doc := manifestDocumentForKindAndName(manifest, "DaemonSet", name); doc == "" {
			t.Fatalf("rendered manifest missing %s:\n%s", name, manifest)
		}
	}

	edgeDoc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-edge-country-de")
	if edgeDoc == "" {
		t.Fatalf("rendered manifest missing country-de edge daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`app.kubernetes.io/component: edge-country-de`,
		`fugue.io/location-country-code: de`,
		`name: fugue-edge-de-scoped-token`,
		`key: FUGUE_EDGE_TOKEN`,
		`name: FUGUE_EDGE_GROUP_ID`,
		`value: "edge-group-country-de"`,
		`name: https-canary`,
		`hostPort: 443`,
		`path: "/var/lib/fugue/edge/caddy-data"`,
		`mountPath: "/data"`,
		`name: FUGUE_EDGE_CADDY_DATA_DIR`,
		`value: "/data/caddy"`,
		`name: FUGUE_EDGE_CADDY_SHARED_TLS_ENABLED`,
		`value: "true"`,
	} {
		if !strings.Contains(edgeDoc, want) {
			t.Fatalf("country-de edge daemonset missing %q:\n%s", want, edgeDoc)
		}
	}

	dnsDoc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-dns-country-de")
	if dnsDoc == "" {
		t.Fatalf("rendered manifest missing country-de dns daemonset:\n%s", manifest)
	}
	for _, want := range []string{
		`app.kubernetes.io/component: dns-country-de`,
		`fugue.io/location-country-code: de`,
		`name: fugue-edge-de-scoped-token`,
		`key: FUGUE_EDGE_TOKEN`,
		`name: FUGUE_EDGE_GROUP_ID`,
		`value: "edge-group-country-de"`,
		`name: FUGUE_DNS_ANSWER_IPS`,
		`value: "51.38.126.103"`,
		`name: FUGUE_DNS_EXTRA_ZONES`,
		`value: "oaix.cc"`,
		`name: FUGUE_DNS_ROUTE_A_ANSWER_IPS`,
		`value: "136.112.185.40"`,
		`name: dns-udp`,
		`hostPort: 53`,
	} {
		if !strings.Contains(dnsDoc, want) {
			t.Fatalf("country-de dns daemonset missing %q:\n%s", want, dnsDoc)
		}
	}
}

func TestPublicDataPlaneDaemonSetsUseHostPortCompatibleRollouts(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	valuesPath := t.TempDir() + "/values.yaml"
	values := `
edge:
  edgeGroupID: edge-group-country-us
  caddy:
    enabled: true
    listenAddr: :443
    tlsMode: public-on-demand
    publicHostPorts:
      enabled: true
  groups:
    - name: country-de
      edgeGroupID: edge-group-country-de
      nodeSelector:
        fugue.io/role.edge: "true"
        fugue.io/schedulable: "true"
        fugue.io/location-country-code: de
dns:
  enabled: true
  answerIPs:
    - 15.204.94.71
  publicHostPorts:
    enabled: true
  groups:
    - name: country-de
      edgeGroupID: edge-group-country-de
      answerIPs:
        - 51.38.126.103
      nodeSelector:
        fugue.io/role.dns: "true"
        fugue.io/schedulable: "true"
        fugue.io/location-country-code: de
`
	if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
		t.Fatalf("write values: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "-f", valuesPath)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	for _, name := range []string{
		"fugue-fugue-edge",
		"fugue-fugue-edge-country-de",
	} {
		doc := manifestDocumentForKindAndName(manifest, "DaemonSet", name)
		if doc == "" {
			t.Fatalf("rendered manifest missing daemonset %s:\n%s", name, manifest)
		}
		for _, want := range []string{
			"updateStrategy:",
			"type: OnDelete",
			"fugue.io/rollout-mode: direct-ondelete-protected",
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s missing protected edge rollout fragment %q:\n%s", name, want, doc)
			}
		}
		for _, unwanted := range []string{
			"type: RollingUpdate",
			"maxUnavailable: 1",
			"maxSurge: 0",
		} {
			if strings.Contains(doc, unwanted) {
				t.Fatalf("%s edge daemonset must not use rolling update fragment %q:\n%s", name, unwanted, doc)
			}
		}
	}
	for _, name := range []string{
		"fugue-fugue-dns",
		"fugue-fugue-dns-country-de",
	} {
		doc := manifestDocumentForKindAndName(manifest, "DaemonSet", name)
		if doc == "" {
			t.Fatalf("rendered manifest missing daemonset %s:\n%s", name, manifest)
		}
		for _, want := range []string{
			"updateStrategy:",
			"type: OnDelete",
			"fugue.io/rollout-mode: direct-ondelete-protected",
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s missing protected dns rollout fragment %q:\n%s", name, want, doc)
			}
		}
		for _, unwanted := range []string{
			"type: RollingUpdate",
			"maxUnavailable: 1",
			"maxSurge: 0",
		} {
			if strings.Contains(doc, unwanted) {
				t.Fatalf("%s dns daemonset must not use rolling update fragment %q:\n%s", name, unwanted, doc)
			}
		}
	}
	doc := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-image-cache")
	if doc == "" {
		t.Fatalf("rendered manifest missing daemonset fugue-fugue-image-cache:\n%s", manifest)
	}
	for _, want := range []string{
		"updateStrategy:",
		"type: RollingUpdate",
		"maxUnavailable: 1",
		"maxSurge: 0",
		"fugue.io/rollout-mode: bounded-rolling-restart",
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("fugue-fugue-image-cache missing hostPort-compatible rollout fragment %q:\n%s", want, doc)
		}
	}
}

func TestSingletonDependenciesDeclareIsolatedRolloutSemantics(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	valuesPath := t.TempDir() + "/values.yaml"
	values := `
headscale:
  enabled: true
  domain: mesh.example.com
sharedWorkspaceStorage:
  enabled: true
  server:
    clusterIP: 10.43.253.99
`
	if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
		t.Fatalf("write values: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "-f", valuesPath)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	for _, tc := range []struct {
		name          string
		rolloutMode   string
		downtimeClass string
	}{
		{name: "fugue-fugue-registry", rolloutMode: "isolated-singleton", downtimeClass: "build-deploy-gate"},
		{name: "fugue-fugue-headscale", rolloutMode: "isolated-singleton", downtimeClass: "join-plane-gate"},
		{name: "fugue-fugue-shared-workspace-nfs", rolloutMode: "isolated-singleton", downtimeClass: "downtime-required"},
		{name: "fugue-fugue-shared-workspace-provisioner", rolloutMode: "isolated-singleton", downtimeClass: "provisioner-gate"},
	} {
		doc := manifestDocumentForKindAndName(manifest, "Deployment", tc.name)
		if doc == "" {
			t.Fatalf("rendered manifest missing deployment %s:\n%s", tc.name, manifest)
		}
		for _, want := range []string{
			"strategy:",
			"type: Recreate",
			"fugue.io/rollout-mode: " + tc.rolloutMode,
			"fugue.io/downtime-class: " + tc.downtimeClass,
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s missing singleton rollout fragment %q:\n%s", tc.name, want, doc)
			}
		}
	}

	releaseSafety := manifestDocumentForKindAndName(manifest, "ConfigMap", "fugue-fugue-release-safety")
	if releaseSafety == "" {
		t.Fatalf("rendered manifest missing release safety catalog:\n%s", manifest)
	}
	for _, want := range []string{
		`public-data-plane: "isolated; node-local front plus worker a/b blue-green required for edge image/template changes"`,
		`shared-workspace-storage: "downtime-required; single NFS writer"`,
	} {
		if !strings.Contains(releaseSafety, want) {
			t.Fatalf("release safety catalog missing %q:\n%s", want, releaseSafety)
		}
	}
}

func manifestDocumentForKindAndName(manifest string, kind string, name string) string {
	for _, doc := range strings.Split(manifest, "\n---") {
		hasKind := strings.Contains(doc, "\nkind: "+kind+"\n") || strings.Contains(doc, "kind: "+kind+"\n")
		hasName := strings.Contains(doc, "\n  name: "+name+"\n") ||
			strings.Contains(doc, "\n  name: \""+name+"\"\n") ||
			strings.Contains(doc, "\nname: "+name+"\n") ||
			strings.Contains(doc, "\nname: \""+name+"\"\n")
		if hasKind && hasName {
			return doc
		}
	}
	return ""
}

func manifestTolerationsBlock(doc string) string {
	index := strings.LastIndex(doc, "\ntolerations:")
	if index == -1 {
		index = strings.LastIndex(doc, "\n              tolerations:")
	}
	if index == -1 {
		index = strings.LastIndex(doc, "\n      tolerations:")
	}
	if index == -1 {
		return ""
	}
	return doc[index:]
}

func TestControlPlanePostgresCNPGCanDriveAPI(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm",
		"template",
		"fugue",
		chartDir,
		"--set", "controlPlanePostgres.enabled=true",
		"--set", "controlPlanePostgres.useForAPI=true",
		"--set", "controlPlanePostgres.password=test-password",
		"--set", "postgres.enabled=false",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	cluster := manifestDocumentForKindAndName(manifest, "Cluster", "fugue-fugue-control-plane-postgres")
	if cluster == "" {
		t.Fatalf("rendered manifest missing control-plane CNPG cluster:\n%s", manifest)
	}
	for _, want := range []string{
		"instances: 3",
		"kind: Cluster",
		"app.kubernetes.io/component: control-plane-postgres",
		`storageClass: "fugue-postgres-rwo"`,
		"name: fugue-fugue-control-plane-postgres-app",
		"node-role.kubernetes.io/control-plane",
		"podAntiAffinityType: \"preferred\"",
	} {
		if !strings.Contains(cluster, want) {
			t.Fatalf("control-plane CNPG cluster missing %q:\n%s", want, cluster)
		}
	}
	if !strings.Contains(manifest, "FUGUE_DATABASE_URL: \"postgres://fugue:") ||
		!strings.Contains(manifest, "@fugue-fugue-control-plane-postgres-rw.default.svc.cluster.local:5432/fugue?sslmode=disable\"") {
		t.Fatalf("config secret should point API at control-plane CNPG rw service:\n%s", manifest)
	}
	if strings.Contains(manifest, "name: fugue-fugue-postgres\n") {
		t.Fatalf("legacy postgres deployment should not render when postgres.enabled=false:\n%s", manifest)
	}
}

func TestControlPlanePostgresBackupAndDrillsRender(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	valuesPath := filepath.Join(t.TempDir(), "values.yaml")
	values := `
controlPlanePostgres:
  enabled: true
  password: test-password
  backup:
    enabled: true
    destinationPath: s3://fugue-control-plane-pitr
    endpointURL: https://s3.example.test
    s3Credentials:
      existingSecretName: pitr-secret
    scheduled:
      enabled: true
      schedule: "0 0 3 * * *"
  restoreDrill:
    enabled: true
    restoreManifestJSON: '{"dump_ref":"s3://fugue-control-plane-pitr/latest","owner":"restore-drill"}'
platformFailureDrill:
  enabled: true
  target: random-ready-control-plane-node
`
	if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
		t.Fatalf("write temp values: %v", err)
	}

	cmd := exec.Command("helm", "template", "fugue", chartDir, "-f", valuesPath)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	cluster := manifestDocumentForKindAndName(manifest, "Cluster", "fugue-fugue-control-plane-postgres")
	if cluster == "" {
		t.Fatalf("rendered manifest missing control-plane CNPG cluster:\n%s", manifest)
	}
	for _, want := range []string{
		"barmanObjectStore:",
		"destinationPath: \"s3://fugue-control-plane-pitr\"",
		"endpointURL: \"https://s3.example.test\"",
		"compression: \"gzip\"",
		"name: \"pitr-secret\"",
	} {
		if !strings.Contains(cluster, want) {
			t.Fatalf("control-plane CNPG cluster missing PITR fragment %q:\n%s", want, cluster)
		}
	}
	if doc := manifestDocumentForKindAndName(manifest, "ScheduledBackup", "fugue-fugue-control-plane-postgres-backup"); doc == "" ||
		!strings.Contains(doc, "schedule: \"0 0 3 * * *\"") {
		t.Fatalf("rendered manifest missing scheduled backup:\n%s", manifest)
	}
	if doc := manifestDocumentForKindAndName(manifest, "CronJob", "fugue-fugue-control-plane-restore-drill"); doc == "" ||
		!strings.Contains(doc, "/v1/admin/control-plane/store/promote") ||
		!strings.Contains(doc, "FUGUE_RESTORE_MANIFEST_JSON") {
		t.Fatalf("rendered manifest missing control-plane restore drill:\n%s", manifest)
	}
	if doc := manifestDocumentForKindAndName(manifest, "CronJob", "fugue-fugue-platform-failure-drill"); doc == "" ||
		!strings.Contains(doc, "/v1/admin/platform/failure-drills") ||
		!strings.Contains(doc, "random-ready-control-plane-node") {
		t.Fatalf("rendered manifest missing platform failure drill:\n%s", manifest)
	}
}

func TestControlPlaneRBACCoversDiagnosableWorkloads(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	for _, want := range []string{
		`resources: ["deployments", "replicasets", "daemonsets", "statefulsets"]`,
		`resources: ["networkpolicies"]`,
	} {
		if !strings.Contains(manifest, want) {
			t.Fatalf("control plane RBAC should cover managed app workload resource %s:\n%s", want, manifest)
		}
	}
}

func TestCloudNativePGOperatorHasHAAndResources(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	for _, want := range []string{
		"name: fugue-cloudnative-pg",
		"replicas: 2",
		"priorityClassName: system-cluster-critical",
		"node-role.kubernetes.io/control-plane: \"true\"",
		"cpu: 100m",
		"memory: 128Mi",
		"app.kubernetes.io/name: cloudnative-pg",
	} {
		if !strings.Contains(manifest, want) {
			t.Fatalf("cloudnative-pg operator manifest missing %q:\n%s", want, manifest)
		}
	}
}
