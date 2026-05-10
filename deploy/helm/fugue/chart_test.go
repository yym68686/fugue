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
