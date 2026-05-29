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
			"maxSurge: 2",
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s should allow surge-first HA rollouts; missing %q:\n%s", tc.name, want, doc)
			}
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
		`storageClassName: "fugue-local-rwo"`,
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
		`storageClassName: "fugue-local-rwo"`,
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
		"fugue-fugue-dns",
		"fugue-fugue-dns-country-de",
		"fugue-fugue-image-cache",
	} {
		doc := manifestDocumentForKindAndName(manifest, "DaemonSet", name)
		if doc == "" {
			t.Fatalf("rendered manifest missing daemonset %s:\n%s", name, manifest)
		}
		for _, want := range []string{
			"updateStrategy:",
			"type: RollingUpdate",
			"maxUnavailable: 1",
			"maxSurge: 0",
			"fugue.io/rollout-mode: bounded-rolling-restart",
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s missing hostPort-compatible rollout fragment %q:\n%s", name, want, doc)
			}
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
		"name: fugue-fugue-control-plane-postgres-app",
		"node-role.kubernetes.io/control-plane",
		"podAntiAffinityType: \"required\"",
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
