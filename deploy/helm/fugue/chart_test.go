package fuguechart_test

import (
	"os"
	"os/exec"
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
		`admin 127.0.0.1:2019`,
		`name: caddy-config`,
		`name: caddy-data`,
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
		`name: FUGUE_DNS_UDP_ADDR`,
		`value: "127.0.0.1:5353"`,
		`name: FUGUE_DNS_TCP_ADDR`,
		`value: "127.0.0.1:5353"`,
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
  answerIPs:
    - 15.204.94.71
  publicHostPorts:
    enabled: true
  udpAddr: :53
  tcpAddr: :53
  groups:
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
		`name: dns-udp`,
		`hostPort: 53`,
	} {
		if !strings.Contains(dnsDoc, want) {
			t.Fatalf("country-de dns daemonset missing %q:\n%s", want, dnsDoc)
		}
	}
}

func manifestDocumentForKindAndName(manifest string, kind string, name string) string {
	for _, doc := range strings.Split(manifest, "\n---") {
		hasKind := strings.Contains(doc, "\nkind: "+kind+"\n") || strings.Contains(doc, "kind: "+kind+"\n")
		hasName := strings.Contains(doc, "\n  name: "+name+"\n") || strings.Contains(doc, "\nname: "+name+"\n")
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
