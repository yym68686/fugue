package fuguechart_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

func TestDedicatedControlPlaneServiceAccountIsolatesSecretRBAC(t *testing.T) {
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
	serviceAccount := manifestDocumentForKindAndName(manifest, "ServiceAccount", "fugue-fugue-control-plane-sa")
	if serviceAccount == "" {
		t.Fatalf("rendered manifest missing dedicated control-plane service account:\n%s", manifest)
	}
	role := manifestDocumentForKindAndName(manifest, "ClusterRole", "fugue-fugue-control-plane")
	if role == "" {
		t.Fatalf("rendered manifest missing dedicated control-plane role:\n%s", manifest)
	}
	for _, want := range []string{
		`resources: ["namespaces", "services", "secrets", "pods", "endpoints", "persistentvolumeclaims", "persistentvolumes"]`,
		`verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]`,
	} {
		if !strings.Contains(role, want) {
			t.Fatalf("dedicated control-plane role missing %q:\n%s", want, role)
		}
	}
	sharedRole := manifestDocumentForKindAndName(manifest, "ClusterRole", "fugue-fugue")
	sharedRulesOffset := strings.Index(sharedRole, "rules:")
	controlPlaneRulesOffset := strings.Index(role, "rules:")
	if sharedRulesOffset < 0 || controlPlaneRulesOffset < 0 {
		t.Fatalf("rendered roles must both contain rules:\nshared:\n%s\ncontrol-plane:\n%s", sharedRole, role)
	}
	sharedRules := strings.TrimSpace(sharedRole[sharedRulesOffset:])
	controlPlaneRules := strings.TrimSpace(role[controlPlaneRulesOffset:])
	if strings.Contains(sharedRules, `"secrets"`) {
		t.Fatalf("shared platform role must not grant Kubernetes Secret API access:\n%s", sharedRole)
	}
	wantSharedRules := strings.Replace(controlPlaneRules, `"secrets", `, "", 1)
	if sharedRules != wantSharedRules {
		t.Fatalf("shared and control-plane roles must differ only by Secret API access:\nshared:\n%s\ncontrol-plane:\n%s", sharedRole, role)
	}
	binding := manifestDocumentForKindAndName(manifest, "ClusterRoleBinding", "fugue-fugue-control-plane")
	if binding == "" {
		t.Fatalf("rendered manifest missing dedicated control-plane role binding:\n%s", manifest)
	}
	for _, want := range []string{
		"kind: ClusterRole",
		"name: fugue-fugue",
		"kind: ServiceAccount",
		"name: fugue-fugue-control-plane-sa",
	} {
		if !strings.Contains(binding, want) {
			t.Fatalf("dedicated control-plane binding missing %q:\n%s", want, binding)
		}
	}
	isolatedBinding := manifestDocumentForKindAndName(manifest, "ClusterRoleBinding", "fugue-fugue-control-plane-isolated")
	if isolatedBinding == "" {
		t.Fatalf("rendered manifest missing isolated control-plane role binding:\n%s", manifest)
	}
	for _, want := range []string{
		"kind: ClusterRole",
		"name: fugue-fugue-control-plane",
		"kind: ServiceAccount",
		"name: fugue-fugue-control-plane-sa",
	} {
		if !strings.Contains(isolatedBinding, want) {
			t.Fatalf("isolated control-plane binding missing %q:\n%s", want, isolatedBinding)
		}
	}
	api := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-api")
	if !strings.Contains(api, "serviceAccountName: fugue-fugue-control-plane-sa") {
		t.Fatalf("API should canary the dedicated control-plane service account:\n%s", api)
	}
	controller := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-controller")
	if !strings.Contains(controller, "serviceAccountName: fugue-fugue-control-plane-sa") {
		t.Fatalf("controller should use the dedicated control-plane service account after the API canary:\n%s", controller)
	}
}

func TestPlatformComponentIdentitySecretIsIndependentAndRetained(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set-string", "platformComponentIdentity.signingKey=component-current-secret",
		"--set-string", "platformComponentIdentity.signingKeyID=component-current",
		"--set-string", "platformComponentIdentity.previousSigningKey=component-previous-secret",
		"--set-string", "platformComponentIdentity.previousSigningKeyID=component-previous",
		"--set-string", "platformComponentIdentity.revokedKeyIDs=component-revoked",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	secret := manifestDocumentForKindAndName(manifest, "Secret", "fugue-fugue-platform-component-identity")
	if secret == "" {
		t.Fatalf("rendered manifest missing platform component identity secret:\n%s", manifest)
	}
	for _, want := range []string{
		"helm.sh/resource-policy: keep",
		`FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY: "component-current-secret"`,
		`FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID: "component-current"`,
		`FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY: "component-previous-secret"`,
		`FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID: "component-previous"`,
		`FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS: "component-revoked"`,
	} {
		if !strings.Contains(secret, want) {
			t.Fatalf("platform component identity secret missing %q:\n%s", want, secret)
		}
	}
	for _, forbidden := range []string{
		"FUGUE_BOOTSTRAP_ADMIN_KEY",
		"FUGUE_WORKLOAD_IDENTITY_SIGNING_KEY",
		"FUGUE_BUNDLE_SIGNING_KEY",
	} {
		if strings.Contains(secret, forbidden) {
			t.Fatalf("platform component identity secret must not contain %q:\n%s", forbidden, secret)
		}
	}
}

func TestPlatformComponentIdentityMissingManagedSecretBootstrapsOneKey(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set-string", "workloadIdentity.signingKey=workload-test-key",
		"--set-string", "bundle.signingKey=bundle-test-key",
		"--set-string", "postgres.password=postgres-test-password",
		"--set-string", "api.edgeTLSAskToken=edge-test-token",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	secret := manifestDocumentForKindAndName(string(output), "Secret", "fugue-fugue-platform-component-identity")
	if secret == "" {
		t.Fatalf("rendered manifest missing managed platform component identity secret:\n%s", output)
	}
	signingKey, ok := manifestStringDataValue(secret, "FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY")
	if !ok || len(signingKey) != 48 {
		t.Fatalf("missing managed secret must bootstrap one 48-character signing key, got %q:\n%s", signingKey, secret)
	}
	signingKeyID, ok := manifestStringDataValue(secret, "FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID")
	if !ok {
		t.Fatalf("managed component identity secret missing signing key id:\n%s", secret)
	}
	digest := sha256.Sum256([]byte(signingKey))
	wantKeyID := "pci-" + hex.EncodeToString(digest[:])[:16]
	if signingKeyID != wantKeyID {
		t.Fatalf("managed component identity key id = %q, want %q", signingKeyID, wantKeyID)
	}
	for _, key := range []string{
		"FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY",
		"FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID",
		"FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS",
	} {
		if value, ok := manifestStringDataValue(secret, key); !ok || value != "" {
			t.Fatalf("new managed component identity secret %s = %q, want empty", key, value)
		}
	}
}

func TestPlatformComponentIdentityManagedSecretUpgradeAndRollbackPreserveKeys(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	testChartDir := copyChartWithPlatformComponentIdentityProbe(t, chartDir)
	v1Settings := []string{
		"platformComponentIdentityResolutionTest.existingSigningKey=component-current-v1",
		"platformComponentIdentityResolutionTest.existingSigningKeyID=component-v1",
		"platformComponentIdentityResolutionTest.existingPreviousSigningKey=component-previous-v0",
		"platformComponentIdentityResolutionTest.existingPreviousSigningKeyID=component-v0",
		"platformComponentIdentityResolutionTest.existingRevokedKeyIDs=component-revoked-v0",
	}
	firstUpgrade := renderPlatformComponentIdentityResolution(t, testChartDir, v1Settings...)
	secondUpgrade := renderPlatformComponentIdentityResolution(t, testChartDir, v1Settings...)
	if firstUpgrade != secondUpgrade {
		t.Fatalf("unchanged upgrade generated different component identity data:\nfirst:\n%s\nsecond:\n%s", firstUpgrade, secondUpgrade)
	}
	for _, want := range []string{
		`signingKey: "component-current-v1"`,
		`signingKeyID: "component-v1"`,
		`previousSigningKey: "component-previous-v0"`,
		`previousSigningKeyID: "component-v0"`,
		`revokedKeyIDs: "component-revoked-v0"`,
	} {
		if !strings.Contains(firstUpgrade, want) {
			t.Fatalf("unchanged upgrade did not retain %q:\n%s", want, firstUpgrade)
		}
	}

	rotated := renderPlatformComponentIdentityResolution(t, testChartDir, append(v1Settings,
		"platformComponentIdentity.signingKey=component-current-v2",
		"platformComponentIdentity.signingKeyID=component-v2",
	)...)
	for _, want := range []string{
		`signingKey: "component-current-v2"`,
		`signingKeyID: "component-v2"`,
		`previousSigningKey: "component-current-v1"`,
		`previousSigningKeyID: "component-v1"`,
	} {
		if !strings.Contains(rotated, want) {
			t.Fatalf("explicit rotation did not produce %q:\n%s", want, rotated)
		}
	}

	rollbackSettings := []string{
		"platformComponentIdentityResolutionTest.existingSigningKey=component-current-v2",
		"platformComponentIdentityResolutionTest.existingSigningKeyID=component-v2",
		"platformComponentIdentityResolutionTest.existingPreviousSigningKey=component-current-v1",
		"platformComponentIdentityResolutionTest.existingPreviousSigningKeyID=component-v1",
		"platformComponentIdentityResolutionTest.existingRevokedKeyIDs=component-revoked-v0",
	}
	rollback := renderPlatformComponentIdentityResolution(t, testChartDir, rollbackSettings...)
	for _, want := range []string{
		`signingKey: "component-current-v2"`,
		`signingKeyID: "component-v2"`,
		`previousSigningKey: "component-current-v1"`,
		`previousSigningKeyID: "component-v1"`,
		`revokedKeyIDs: "component-revoked-v0"`,
	} {
		if !strings.Contains(rollback, want) {
			t.Fatalf("rollback with historical empty values rotated retained key %q:\n%s", want, rollback)
		}
	}
}

func TestPlatformComponentIdentityMissingExternalSecretFailsClosed(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set-string", "platformComponentIdentity.existingSecretName=external-component-identity",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	if secret := manifestDocumentForKindAndName(manifest, "Secret", "fugue-fugue-platform-component-identity"); secret != "" {
		t.Fatalf("generated component identity secret must be omitted when an external secret is configured:\n%s", secret)
	}
	if secret := manifestDocumentForKindAndName(manifest, "Secret", "external-component-identity"); secret != "" {
		t.Fatalf("chart must not take ownership of the external component identity secret:\n%s", secret)
	}
	api := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-api")
	for _, want := range []string{
		"checksum/platform-component-identity-secret:",
		"name: FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY",
		"key: FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY",
		"name: FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID",
		"key: FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID",
		"name: FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY",
		"key: FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY",
		"name: FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID",
		"key: FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID",
		"name: FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS",
		"key: FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS",
		"name: external-component-identity",
	} {
		if !strings.Contains(api, want) {
			t.Fatalf("API component identity canary missing %q:\n%s", want, api)
		}
	}
	for _, requiredEnv := range []string{
		"FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY",
		"FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID",
	} {
		block := manifestEnvBlock(api, requiredEnv)
		if block == "" || strings.Contains(block, "optional: true") {
			t.Fatalf("API %s reference must fail closed when the external secret or key is absent:\n%s", requiredEnv, block)
		}
	}
	controller := manifestDocumentForKindAndName(manifest, "Deployment", "fugue-fugue-controller")
	for _, want := range []string{
		"checksum/platform-component-identity-secret:",
		"name: FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY",
		"key: FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY",
		"name: FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID",
		"key: FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID",
		"name: FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY",
		"key: FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY",
		"name: FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID",
		"key: FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID",
		"name: FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS",
		"key: FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS",
		"name: external-component-identity",
	} {
		if !strings.Contains(controller, want) {
			t.Fatalf("controller component identity canary missing %q:\n%s", want, controller)
		}
	}
	for _, requiredEnv := range []string{
		"FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY",
		"FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID",
	} {
		block := manifestEnvBlock(controller, requiredEnv)
		if block == "" || strings.Contains(block, "optional: true") {
			t.Fatalf("controller %s reference must fail closed when the external secret or key is absent:\n%s", requiredEnv, block)
		}
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

func TestAPIReceivesImageStoreMinimumReplicas(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir, "--set", "imageStore.minReplicas=3")
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
		"name: FUGUE_IMAGE_STORE_MIN_REPLICAS",
		`value: "3"`,
	} {
		if !strings.Contains(apiDoc, want) {
			t.Fatalf("api deployment should receive image-store minimum replicas; missing %q:\n%s", want, apiDoc)
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
		"name: FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME",
		`value: "fugue-fugue-control-plane-db-backup"`,
		"name: FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE",
		"name: FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_DURATION_SECONDS",
		`value: "120"`,
		"name: FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_RENEW_SECONDS",
		`value: "30"`,
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

func TestNodeLocalDNSDefaultsDisabled(t *testing.T) {
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
	for _, kind := range []string{"DaemonSet", "PriorityClass"} {
		if doc := manifestDocumentForKindAndName(manifest, kind, "fugue-fugue-node-local-dns"); doc != "" {
			t.Fatalf("node-local DNS %s must require an explicit production opt-in:\n%s", kind, doc)
		}
	}
}

func TestNodeLocalDNSShadowModeIsObservableWithoutInterceptingPodDNS(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "nodeLocalDNS.enabled=true",
		"--set-string", "nodeLocalDNS.kubeDNSServiceIP=10.43.0.10",
		"--set-string", `nodeLocalDNS.targetNodes[0]=vps-84c8f0a9`,
		"--set-string", `nodeLocalDNS.targetNodes[1]=vps-d6d20fa1`,
		"--set", "observability.metrics.enabled=true",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	name := "fugue-fugue-node-local-dns"
	for _, resource := range []struct {
		kind string
		name string
	}{
		{kind: "ServiceAccount", name: name},
		{kind: "ConfigMap", name: name},
		{kind: "Service", name: name},
		{kind: "Service", name: "fugue-fugue-dns-upstream"},
		{kind: "DaemonSet", name: name},
	} {
		doc := manifestDocumentForKindAndName(manifest, resource.kind, resource.name)
		if doc == "" {
			t.Fatalf("rendered manifest missing %s %s:\n%s", resource.kind, resource.name, manifest)
		}
		if !strings.Contains(doc, "namespace: kube-system") {
			t.Fatalf("%s %s must be explicitly owned in kube-system:\n%s", resource.kind, resource.name, doc)
		}
	}
	priorityClass := manifestDocumentForKindAndName(manifest, "PriorityClass", name)
	for _, want := range []string{"value: 1000000000", "globalDefault: false", "preemptionPolicy: Never"} {
		if !strings.Contains(priorityClass, want) {
			t.Fatalf("node-local DNS priority class missing %q:\n%s", want, priorityClass)
		}
	}

	config := manifestDocumentForKindAndName(manifest, "ConfigMap", name)
	shadowStart := strings.Index(config, "  Corefile.shadow: |")
	ipTablesStart := strings.Index(config, "  Corefile.iptables: |")
	legacyStart := strings.Index(config, "  Corefile: |")
	if shadowStart < 0 || ipTablesStart <= shadowStart || legacyStart <= ipTablesStart {
		t.Fatalf("node-local DNS config must contain stable shadow, iptables, and compatibility Corefiles:\n%s", config)
	}
	shadowCorefile := config[shadowStart:ipTablesStart]
	ipTablesCorefile := config[ipTablesStart:legacyStart]
	legacyCorefile := config[legacyStart:]
	for _, want := range []string{
		"cluster.local:53 {",
		"bind 169.254.20.10",
		"forward . __PILLAR__CLUSTER__DNS__ {",
		"force_tcp",
		"health 169.254.20.10:8080",
	} {
		if !strings.Contains(shadowCorefile, want) {
			t.Fatalf("shadow node-local DNS config missing %q:\n%s", want, shadowCorefile)
		}
	}
	for entryName, corefile := range map[string]string{
		"shadow":        shadowCorefile,
		"iptables":      ipTablesCorefile,
		"compatibility": legacyCorefile,
	} {
		if got := strings.Count(corefile, "forward . __PILLAR__CLUSTER__DNS__ {"); got != 4 {
			t.Fatalf("every %s node-local DNS zone must retain central CoreDNS as its fallback; got %d forwards:\n%s", entryName, got, corefile)
		}
	}
	for _, unwanted := range []string{
		"bind 169.254.20.10 10.43.0.10",
		"__PILLAR__UPSTREAM__SERVERS__",
		"/etc/resolv.conf",
	} {
		if strings.Contains(shadowCorefile, unwanted) || strings.Contains(legacyCorefile, unwanted) {
			t.Fatalf("shadow and selected compatibility Corefiles must not contain %q:\n%s", unwanted, config)
		}
	}
	if got := strings.Count(ipTablesCorefile, "bind 169.254.20.10 10.43.0.10"); got != 4 {
		t.Fatalf("stable iptables Corefile must bind both addresses in every zone; got %d:\n%s", got, ipTablesCorefile)
	}

	upstream := manifestDocumentForKindAndName(manifest, "Service", "fugue-fugue-dns-upstream")
	for _, want := range []string{
		"k8s-app: kube-dns",
		"protocol: UDP",
		"protocol: TCP",
	} {
		if !strings.Contains(upstream, want) {
			t.Fatalf("central CoreDNS upstream service missing %q:\n%s", want, upstream)
		}
	}
	metricsService := manifestDocumentForKindAndName(manifest, "Service", name)
	for _, want := range []string{`fugue.io/node-local-dns-cohort: "active"`, "targetPort: 9253", "targetPort: 9353"} {
		if !strings.Contains(metricsService, want) {
			t.Fatalf("node-local DNS metrics Service missing %q:\n%s", want, metricsService)
		}
	}
	prometheusConfig := manifestDocumentForKindAndName(manifest, "ConfigMap", "fugue-fugue-observability-prometheus")
	for _, want := range []string{
		"job_name: fugue-node-local-dns",
		"role: endpoints",
		"__meta_kubernetes_service_label_app_kubernetes_io_component",
		"__meta_kubernetes_endpoint_port_name",
		`regex: "metrics|setup-metrics"`,
	} {
		if !strings.Contains(prometheusConfig, want) {
			t.Fatalf("node-local DNS Prometheus endpoint discovery missing %q after removing container ports:\n%s", want, prometheusConfig)
		}
	}
	nodeLocalJob := prometheusConfig[strings.Index(prometheusConfig, "job_name: fugue-node-local-dns"):]
	if nextJob := strings.Index(nodeLocalJob[len("job_name: fugue-node-local-dns"):], "- job_name:"); nextJob >= 0 {
		nodeLocalJob = nodeLocalJob[:len("job_name: fugue-node-local-dns")+nextJob]
	}
	if strings.Contains(nodeLocalJob, "__meta_kubernetes_pod_container_port_name") {
		t.Fatalf("node-local DNS Prometheus job must not depend on removed pod container ports:\n%s", nodeLocalJob)
	}
	daemonSet := manifestDocumentForKindAndName(manifest, "DaemonSet", name)
	for _, want := range []string{
		`fugue.io/node-local-dns-mode: "shadow"`,
		"priorityClassName: fugue-fugue-node-local-dns",
		"automountServiceAccountToken: false",
		"hostNetwork: true",
		"dnsPolicy: Default",
		"type: OnDelete",
		`image: "registry.k8s.io/dns/k8s-dns-node-cache@sha256:bc6e64e2c85956af2fcc0aa720086410d41b4f31f378c9a92646fecc85cd4739"`,
		`- "169.254.20.10"`,
		`- "fugue-fugue-dns-upstream"`,
		"- NET_ADMIN",
		"path: /run/xtables.lock",
		"key: Corefile.shadow",
		"kubernetes.io/os: linux",
		"key: kubernetes.io/hostname",
		"operator: In",
		`- "vps-84c8f0a9"`,
		`- "vps-d6d20fa1"`,
		"effect: NoSchedule",
		"effect: NoExecute",
	} {
		if !strings.Contains(daemonSet, want) {
			t.Fatalf("node-local DNS daemonset missing %q:\n%s", want, daemonSet)
		}
	}
	for _, unwanted := range []string{"containerPort:", "hostPort:", "rollingUpdate:", "maxUnavailable:", `- "169.254.20.10,10.43.0.10"`} {
		if strings.Contains(daemonSet, unwanted) {
			t.Fatalf("shadow node-local DNS daemonset must not contain %q:\n%s", unwanted, daemonSet)
		}
	}
}

func TestNodeLocalDNSIPTablesModeInterceptsTheExistingKubeDNSServiceIP(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "nodeLocalDNS.enabled=true",
		"--set", "nodeLocalDNS.mode=iptables",
		"--set-string", "nodeLocalDNS.kubeDNSServiceIP=10.44.0.10",
		"--set-string", `nodeLocalDNS.targetNodes[0]=node-a`,
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	config := manifestDocumentForKindAndName(manifest, "ConfigMap", "fugue-fugue-node-local-dns")
	ipTablesStart := strings.Index(config, "  Corefile.iptables: |")
	legacyStart := strings.Index(config, "  Corefile: |")
	if ipTablesStart < 0 || legacyStart <= ipTablesStart {
		t.Fatalf("iptables node-local DNS config must contain stable and compatibility Corefiles:\n%s", config)
	}
	if got := strings.Count(config[ipTablesStart:legacyStart], "bind 169.254.20.10 10.44.0.10"); got != 4 {
		t.Fatalf("stable iptables Corefile must bind the local and existing kube-dns service IP in every zone; got %d:\n%s", got, config)
	}
	if got := strings.Count(config[legacyStart:], "bind 169.254.20.10 10.44.0.10"); got != 4 {
		t.Fatalf("selected compatibility Corefile must match iptables mode in every zone; got %d:\n%s", got, config)
	}
	daemonSet := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-node-local-dns")
	for _, want := range []string{
		`fugue.io/node-local-dns-mode: "iptables"`,
		`- "169.254.20.10,10.44.0.10"`,
		"type: OnDelete",
		"key: Corefile.iptables",
		"minReadySeconds: 10",
	} {
		if !strings.Contains(daemonSet, want) {
			t.Fatalf("iptables node-local DNS daemonset missing %q:\n%s", want, daemonSet)
		}
	}
}

func TestNodeLocalDNSLegacyCorefileCanRemainIPTablesWhileNewPodsUseShadow(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "nodeLocalDNS.enabled=true",
		"--set", "nodeLocalDNS.mode=shadow",
		"--set", "nodeLocalDNS.legacyMode=iptables",
		"--set-string", "nodeLocalDNS.kubeDNSServiceIP=10.44.0.10",
		"--set-string", `nodeLocalDNS.targetNodes[0]=node-a`,
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	config := manifestDocumentForKindAndName(manifest, "ConfigMap", "fugue-fugue-node-local-dns")
	ipTablesStart := strings.Index(config, "  Corefile.iptables: |")
	legacyStart := strings.Index(config, "  Corefile: |")
	if ipTablesStart < 0 || legacyStart <= ipTablesStart {
		t.Fatalf("node-local DNS config must contain stable iptables and compatibility Corefiles:\n%s", config)
	}
	if got := strings.Count(config[legacyStart:], "bind 169.254.20.10 10.44.0.10"); got != 4 {
		t.Fatalf("legacy Corefile must stay in iptables mode for every zone; got %d:\n%s", got, config)
	}

	daemonSet := manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-node-local-dns")
	for _, want := range []string{
		`fugue.io/node-local-dns-mode: "shadow"`,
		`- "169.254.20.10"`,
		"key: Corefile.shadow",
	} {
		if !strings.Contains(daemonSet, want) {
			t.Fatalf("new shadow Pod template missing %q:\n%s", want, daemonSet)
		}
	}
	for _, unwanted := range []string{`- "169.254.20.10,10.44.0.10"`, "key: Corefile.iptables"} {
		if strings.Contains(daemonSet, unwanted) {
			t.Fatalf("new shadow Pod template must not contain %q:\n%s", unwanted, daemonSet)
		}
	}
}

func TestNodeLocalDNSSplitCohortsPreserveIPTablesWhileActiveUsesShadow(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "nodeLocalDNS.enabled=true",
		"--set", "nodeLocalDNS.mode=shadow",
		"--set", "nodeLocalDNS.legacyMode=iptables",
		"--set-string", "nodeLocalDNS.kubeDNSServiceIP=10.44.0.10",
		"--set-string", `nodeLocalDNS.targetNodes[0]=online-a`,
		"--set-string", `nodeLocalDNS.targetNodes[1]=online-b`,
		"--set-string", `nodeLocalDNS.preservedOfflineNodes[0]=dmit`,
		"--set-string", `nodeLocalDNS.nodeSelector.kubernetes\.io/hostname=`,
		"--set", "observability.metrics.enabled=true",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	const preservedName = "fugue-fugue-node-local-dns"
	const activeName = "fugue-fugue-node-local-dns-active"
	preservedDaemonSet := manifestDocumentForKindAndName(manifest, "DaemonSet", preservedName)
	activeDaemonSet := manifestDocumentForKindAndName(manifest, "DaemonSet", activeName)
	if preservedDaemonSet == "" || activeDaemonSet == "" {
		t.Fatalf("split node-local DNS render must contain distinct preserved and active DaemonSets:\npreserved:\n%s\nactive:\n%s\nmanifest:\n%s", preservedDaemonSet, activeDaemonSet, manifest)
	}
	for name, daemonSet := range map[string]string{
		"preserved": preservedDaemonSet,
		"active":    activeDaemonSet,
	} {
		const canonicalSelector = "      nodeSelector:\n        kubernetes.io/os: linux\n      affinity:\n"
		if !strings.Contains(daemonSet, canonicalSelector) {
			t.Fatalf("%s DaemonSet must discard the empty legacy hostname selector and render only the canonical OS selector:\n%s", name, daemonSet)
		}
	}
	nodeLocalDaemonSetCount := 0
	for _, document := range strings.Split(manifest, "\n---") {
		if !strings.Contains(document, "\nkind: DaemonSet\n") && !strings.Contains(document, "kind: DaemonSet\n") {
			continue
		}
		if strings.Contains(document, "app.kubernetes.io/component: node-local-dns\n") || strings.Contains(document, "app.kubernetes.io/component: node-local-dns-active\n") {
			nodeLocalDaemonSetCount++
		}
	}
	if nodeLocalDaemonSetCount != 2 {
		t.Fatalf("split node-local DNS render must contain exactly two cohort DaemonSets, got %d", nodeLocalDaemonSetCount)
	}
	if got := strings.Count(preservedDaemonSet, "app.kubernetes.io/component: node-local-dns\n"); got != 3 {
		t.Fatalf("preserved DaemonSet metadata, selector, and Pod template must share one component source; got %d occurrences:\n%s", got, preservedDaemonSet)
	}
	if got := strings.Count(activeDaemonSet, "app.kubernetes.io/component: node-local-dns-active\n"); got != 3 {
		t.Fatalf("active DaemonSet metadata, selector, and Pod template must share one component source; got %d occurrences:\n%s", got, activeDaemonSet)
	}
	for _, want := range []string{
		"app.kubernetes.io/component: node-local-dns",
		`fugue.io/node-local-dns-cohort: "preserved"`,
		`fugue.io/node-local-dns-mode: "iptables"`,
		"type: OnDelete",
		`- "169.254.20.10,10.44.0.10"`,
		"key: Corefile.iptables",
		"key: kubernetes.io/hostname",
		"operator: In",
		`- "dmit"`,
	} {
		if !strings.Contains(preservedDaemonSet, want) {
			t.Fatalf("preserved node-local DNS DaemonSet missing %q:\n%s", want, preservedDaemonSet)
		}
	}
	for _, unwanted := range []string{
		"app.kubernetes.io/component: node-local-dns-active",
		`fugue.io/node-local-dns-mode: "shadow"`,
		"key: Corefile.shadow",
		`- "online-a"`,
		`- "online-b"`,
	} {
		if strings.Contains(preservedDaemonSet, unwanted) {
			t.Fatalf("preserved node-local DNS DaemonSet must not contain %q:\n%s", unwanted, preservedDaemonSet)
		}
	}
	for _, want := range []string{
		"app.kubernetes.io/component: node-local-dns-active",
		`fugue.io/node-local-dns-cohort: "active"`,
		`fugue.io/node-local-dns-mode: "shadow"`,
		"type: OnDelete",
		`- "169.254.20.10"`,
		"key: Corefile.shadow",
		"key: kubernetes.io/hostname",
		"operator: In",
		`- "online-a"`,
		`- "online-b"`,
	} {
		if !strings.Contains(activeDaemonSet, want) {
			t.Fatalf("active node-local DNS DaemonSet missing %q:\n%s", want, activeDaemonSet)
		}
	}
	for _, unwanted := range []string{
		`fugue.io/node-local-dns-cohort: "preserved"`,
		`fugue.io/node-local-dns-mode: "iptables"`,
		`- "169.254.20.10,10.44.0.10"`,
		"key: Corefile.iptables",
		`- "dmit"`,
	} {
		if strings.Contains(activeDaemonSet, unwanted) {
			t.Fatalf("active node-local DNS DaemonSet must not contain %q:\n%s", unwanted, activeDaemonSet)
		}
	}

	preservedService := manifestDocumentForKindAndName(manifest, "Service", preservedName)
	activeService := manifestDocumentForKindAndName(manifest, "Service", activeName)
	if preservedService == "" || activeService == "" {
		t.Fatalf("split node-local DNS render must contain distinct preserved and active metrics Services:\npreserved:\n%s\nactive:\n%s", preservedService, activeService)
	}
	for _, test := range []struct {
		name      string
		document  string
		component string
		cohort    string
	}{
		{name: "preserved", document: preservedService, component: "node-local-dns", cohort: "preserved"},
		{name: "active", document: activeService, component: "node-local-dns-active", cohort: "active"},
	} {
		if got := strings.Count(test.document, "app.kubernetes.io/component: "+test.component); got != 2 {
			t.Fatalf("%s metrics Service metadata and selector must share one component source; got %d occurrences:\n%s", test.name, got, test.document)
		}
		for _, want := range []string{
			"app.kubernetes.io/component: " + test.component,
			`fugue.io/node-local-dns-cohort: "` + test.cohort + `"`,
			"targetPort: 9253",
			"targetPort: 9353",
		} {
			if !strings.Contains(test.document, want) {
				t.Fatalf("%s node-local DNS metrics Service missing %q:\n%s", test.name, want, test.document)
			}
		}
	}

	config := manifestDocumentForKindAndName(manifest, "ConfigMap", preservedName)
	shadowStart := strings.Index(config, "  Corefile.shadow: |")
	ipTablesStart := strings.Index(config, "  Corefile.iptables: |")
	legacyStart := strings.Index(config, "  Corefile: |")
	if shadowStart < 0 || ipTablesStart <= shadowStart || legacyStart <= ipTablesStart {
		t.Fatalf("split node-local DNS config must contain stable shadow, iptables, and compatibility Corefiles:\n%s", config)
	}
	if got := strings.Count(config[shadowStart:ipTablesStart], "bind 169.254.20.10"); got != 4 {
		t.Fatalf("active stable shadow Corefile must bind only the local address in every zone; got %d:\n%s", got, config[shadowStart:ipTablesStart])
	}
	if strings.Contains(config[shadowStart:ipTablesStart], "10.44.0.10") {
		t.Fatalf("active stable shadow Corefile must not bind the kube-dns ServiceIP:\n%s", config[shadowStart:ipTablesStart])
	}
	for entryName, corefile := range map[string]string{
		"stable iptables": config[ipTablesStart:legacyStart],
		"legacy":          config[legacyStart:],
	} {
		if got := strings.Count(corefile, "bind 169.254.20.10 10.44.0.10"); got != 4 {
			t.Fatalf("%s Corefile must preserve iptables binding in every zone; got %d:\n%s", entryName, got, corefile)
		}
	}

	prometheusConfig := manifestDocumentForKindAndName(manifest, "ConfigMap", "fugue-fugue-observability-prometheus")
	for _, want := range []string{
		`regex: "node-local-dns|node-local-dns-active"`,
		"__meta_kubernetes_service_label_fugue_io_node_local_dns_cohort",
		`expr: up{job="fugue-node-local-dns",cohort="active"} == 0 or absent(up{job="fugue-node-local-dns",cohort="active"})`,
	} {
		if !strings.Contains(prometheusConfig, want) {
			t.Fatalf("split node-local DNS Prometheus config missing %q:\n%s", want, prometheusConfig)
		}
	}
}

func TestNodeLocalDNSSplitCohortNamesRemainDistinctForLongReleaseNames(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	releaseName := strings.Repeat("r", 53)
	cmd := exec.Command(
		"helm", "template", releaseName, chartDir,
		"--set", "nodeLocalDNS.enabled=true",
		"--set", "nodeLocalDNS.mode=shadow",
		"--set", "nodeLocalDNS.legacyMode=iptables",
		"--set-string", "nodeLocalDNS.kubeDNSServiceIP=10.44.0.10",
		"--set-string", `nodeLocalDNS.targetNodes[0]=online-a`,
		"--set-string", `nodeLocalDNS.preservedOfflineNodes[0]=dmit`,
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	// This intentionally mirrors both Helm's fugue.fullname/name derivation and
	// node_local_dns_configure_cohort_names in the release script. The active
	// suffix is reserved before it is appended, so it cannot truncate back to
	// the preserved name at Kubernetes' 63-character DNS-label boundary.
	truncateAndTrim := func(value string, limit int) string {
		if len(value) > limit {
			value = value[:limit]
		}
		return strings.TrimSuffix(value, "-")
	}
	fullName := truncateAndTrim(releaseName+"-fugue", 63)
	preservedName := truncateAndTrim(fullName+"-node-local-dns", 63)
	activeName := truncateAndTrim(preservedName, 56) + "-active"
	if preservedName == activeName || len(preservedName) > 63 || len(activeName) > 63 {
		t.Fatalf("test name derivation is not a valid split cohort: preserved=%q active=%q", preservedName, activeName)
	}

	manifest := string(output)
	for _, resource := range []struct {
		kind string
		name string
	}{
		{kind: "DaemonSet", name: preservedName},
		{kind: "DaemonSet", name: activeName},
		{kind: "Service", name: preservedName},
		{kind: "Service", name: activeName},
	} {
		if doc := manifestDocumentForKindAndName(manifest, resource.kind, resource.name); doc == "" {
			t.Fatalf("long release name render missing %s %q; expected script/Helm split names preserved=%q active=%q:\n%s", resource.kind, resource.name, preservedName, activeName, manifest)
		}
	}
}

func TestNodeLocalDNSSplitCohortsRejectUnsafeMembershipAndStrategy(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "overlapping active and preserved membership",
			args: []string{
				"--set", "nodeLocalDNS.legacyMode=iptables",
				"--set-string", `nodeLocalDNS.preservedOfflineNodes[0]=online-a`,
			},
			want: "nodeLocalDNS.targetNodes and nodeLocalDNS.preservedOfflineNodes must be disjoint",
		},
		{
			name: "duplicate active membership",
			args: []string{
				"--set-string", `nodeLocalDNS.targetNodes[1]=online-a`,
			},
			want: "nodeLocalDNS.targetNodes must not contain duplicate hostnames",
		},
		{
			name: "duplicate preserved membership",
			args: []string{
				"--set", "nodeLocalDNS.legacyMode=iptables",
				"--set-string", `nodeLocalDNS.preservedOfflineNodes[0]=dmit`,
				"--set-string", `nodeLocalDNS.preservedOfflineNodes[1]=dmit`,
			},
			want: "nodeLocalDNS.preservedOfflineNodes must not contain duplicate hostnames",
		},
		{
			name: "missing explicit legacy mode",
			args: []string{
				"--set-string", `nodeLocalDNS.preservedOfflineNodes[0]=dmit`,
			},
			want: "nodeLocalDNS.legacyMode must be explicit when preservedOfflineNodes is non-empty",
		},
		{
			name: "rolling update with preserved cohort",
			args: []string{
				"--set", "nodeLocalDNS.legacyMode=iptables",
				"--set-string", `nodeLocalDNS.preservedOfflineNodes[0]=dmit`,
				"--set", "nodeLocalDNS.updateStrategy.type=RollingUpdate",
				"--set", "nodeLocalDNS.updateStrategy.maxUnavailable=1",
			},
			want: "nodeLocalDNS.updateStrategy.type must be OnDelete when preservedOfflineNodes is non-empty",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := []string{
				"template", "fugue", chartDir,
				"--set", "nodeLocalDNS.enabled=true",
				"--set", "nodeLocalDNS.mode=shadow",
				"--set-string", "nodeLocalDNS.kubeDNSServiceIP=10.44.0.10",
				"--set-string", `nodeLocalDNS.targetNodes[0]=online-a`,
			}
			args = append(args, test.args...)
			cmd := exec.Command("helm", args...)
			cmd.Dir = chartDir
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("expected helm template to reject unsafe split node-local DNS values:\n%s", output)
			}
			if !strings.Contains(string(output), test.want) {
				t.Fatalf("unexpected helm error; want %q:\n%s", test.want, output)
			}
		})
	}
}

func TestNodeLocalDNSRollingUpdateMustBeExplicit(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "nodeLocalDNS.enabled=true",
		"--set-string", "nodeLocalDNS.kubeDNSServiceIP=10.43.0.10",
		"--set-string", `nodeLocalDNS.targetNodes[0]=node-a`,
		"--set", "nodeLocalDNS.updateStrategy.type=RollingUpdate",
		"--set", "nodeLocalDNS.updateStrategy.maxUnavailable=1",
	)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	daemonSet := manifestDocumentForKindAndName(string(output), "DaemonSet", "fugue-fugue-node-local-dns")
	for _, want := range []string{"type: RollingUpdate", "rollingUpdate:", "maxUnavailable: 1"} {
		if !strings.Contains(daemonSet, want) {
			t.Fatalf("explicit rolling update strategy missing %q:\n%s", want, daemonSet)
		}
	}
}

func TestNodeLocalDNSChecksumTracksOnlyTheSelectedCorefile(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	render := func(t *testing.T, mode string, serviceIP string) (string, string) {
		t.Helper()
		cmd := exec.Command(
			"helm", "template", "fugue", chartDir,
			"--set", "nodeLocalDNS.enabled=true",
			"--set", "nodeLocalDNS.mode="+mode,
			"--set-string", "nodeLocalDNS.kubeDNSServiceIP="+serviceIP,
			"--set-string", `nodeLocalDNS.targetNodes[0]=node-a`,
		)
		cmd.Dir = chartDir
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("helm template failed: %v\n%s", err, output)
		}
		manifest := string(output)
		return manifestDocumentForKindAndName(manifest, "DaemonSet", "fugue-fugue-node-local-dns"),
			manifestDocumentForKindAndName(manifest, "ConfigMap", "fugue-fugue-node-local-dns")
	}
	checksum := func(t *testing.T, daemonSet string) string {
		t.Helper()
		const prefix = "checksum/node-local-dns-config:"
		for _, line := range strings.Split(daemonSet, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, prefix) {
				return strings.TrimSpace(strings.TrimPrefix(line, prefix))
			}
		}
		t.Fatalf("node-local DNS daemonset is missing the selected Corefile checksum:\n%s", daemonSet)
		return ""
	}

	shadowA, configA := render(t, "shadow", "10.43.0.10")
	shadowB, configB := render(t, "shadow", "10.44.0.10")
	if configA == configB {
		t.Fatal("test setup did not change the unselected stable iptables Corefile")
	}
	if gotA, gotB := checksum(t, shadowA), checksum(t, shadowB); gotA != gotB {
		t.Fatalf("shadow rollout checksum changed with only the unselected iptables Corefile: %s != %s", gotA, gotB)
	}

	ipTablesA, _ := render(t, "iptables", "10.43.0.10")
	ipTablesB, _ := render(t, "iptables", "10.44.0.10")
	if gotA, gotB := checksum(t, ipTablesA), checksum(t, ipTablesB); gotA == gotB {
		t.Fatalf("iptables rollout checksum did not change with its selected Corefile: %s", gotA)
	}
}

func TestNodeLocalDNSRejectsUnsafeAddressingAndModes(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	missingTargets := exec.Command(
		"helm", "template", "fugue", chartDir,
		"--set", "nodeLocalDNS.enabled=true",
		"--set-string", "nodeLocalDNS.kubeDNSServiceIP=10.43.0.10",
	)
	missingTargets.Dir = chartDir
	missingTargetOutput, missingTargetErr := missingTargets.CombinedOutput()
	if missingTargetErr == nil {
		t.Fatalf("expected helm template to reject an empty node-local DNS target cohort:\n%s", missingTargetOutput)
	}
	if want := "nodeLocalDNS.targetNodes must be a non-empty hostname list"; !strings.Contains(string(missingTargetOutput), want) {
		t.Fatalf("unexpected empty target cohort error; want %q:\n%s", want, missingTargetOutput)
	}
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "unknown mode",
			args: []string{"--set", "nodeLocalDNS.mode=active"},
			want: "nodeLocalDNS.mode must be shadow or iptables",
		},
		{
			name: "unknown legacy mode",
			args: []string{"--set", "nodeLocalDNS.legacyMode=active"},
			want: "nodeLocalDNS.legacyMode must be shadow or iptables",
		},
		{
			name: "non link-local cache address",
			args: []string{"--set-string", "nodeLocalDNS.localIP=10.44.0.11"},
			want: "nodeLocalDNS.localIP must be in the IPv4 link-local 169.254.0.0/16 range",
		},
		{
			name: "invalid kube-dns service address",
			args: []string{"--set-string", "nodeLocalDNS.kubeDNSServiceIP=10.44.999.10"},
			want: "nodeLocalDNS.kubeDNSServiceIP must be a valid IPv4 address",
		},
		{
			name: "same cache and service address",
			args: []string{
				"--set-string", "nodeLocalDNS.localIP=169.254.20.10",
				"--set-string", "nodeLocalDNS.kubeDNSServiceIP=169.254.20.10",
			},
			want: "nodeLocalDNS.localIP and nodeLocalDNS.kubeDNSServiceIP must be different",
		},
		{
			name: "unknown update strategy",
			args: []string{"--set", "nodeLocalDNS.updateStrategy.type=Recreate"},
			want: "nodeLocalDNS.updateStrategy.type must be OnDelete or RollingUpdate",
		},
		{
			name: "invalid rolling update availability",
			args: []string{
				"--set", "nodeLocalDNS.updateStrategy.type=RollingUpdate",
				"--set", "nodeLocalDNS.updateStrategy.maxUnavailable=0",
			},
			want: "nodeLocalDNS.updateStrategy.maxUnavailable must be an integer >= 1",
		},
		{
			name: "legacy hostname node selector",
			args: []string{"--set-string", `nodeLocalDNS.nodeSelector.kubernetes\.io/hostname=node-a`},
			want: "nodeLocalDNS.nodeSelector must not set kubernetes.io/hostname; use nodeLocalDNS.targetNodes",
		},
		{
			name: "nonempty legacy hostname migration sentinel",
			args: []string{"--set-string", `nodeLocalDNS.nodeSelector.kubernetes\.io/hostname= `},
			want: "nodeLocalDNS.nodeSelector must not set kubernetes.io/hostname; use nodeLocalDNS.targetNodes",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := []string{
				"template", "fugue", chartDir,
				"--set", "nodeLocalDNS.enabled=true",
				"--set-string", "nodeLocalDNS.kubeDNSServiceIP=10.43.0.10",
				"--set-string", `nodeLocalDNS.targetNodes[0]=node-a`,
			}
			args = append(args, test.args...)
			cmd := exec.Command("helm", args...)
			cmd.Dir = chartDir
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("expected helm template to reject unsafe node-local DNS values:\n%s", output)
			}
			if !strings.Contains(string(output), test.want) {
				t.Fatalf("unexpected helm error; want %q:\n%s", test.want, output)
			}
		})
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
		"dns.publicHostPorts.hostIP=203.0.113.10",
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
		`hostIP: "203.0.113.10"`,
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

func TestDNSPublicHostPortsRequirePerDaemonSetHostIP(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	tests := []struct {
		name   string
		values string
		want   string
	}{
		{
			name: "primary",
			values: `
dns:
  enabled: true
  answerIPs: [203.0.113.10]
  publicHostPorts:
    enabled: true
`,
			want: "dns.publicHostPorts.hostIP is required when public DNS host ports are enabled",
		},
		{
			name: "regional group",
			values: `
dns:
  enabled: true
  answerIPs: [203.0.113.10]
  publicHostPorts:
    enabled: true
    hostIP: 203.0.113.10
  groups:
    - name: country-de
      edgeGroupID: edge-group-country-de
      answerIPs: [198.51.100.20]
      nodeSelector:
        fugue.io/role.dns: "true"
      publicHostPorts:
        enabled: true
`,
			want: "dns.groups[country-de].publicHostPorts.hostIP is required when public DNS host ports are enabled",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			valuesPath := t.TempDir() + "/values.yaml"
			if err := os.WriteFile(valuesPath, []byte(test.values), 0o600); err != nil {
				t.Fatalf("write values: %v", err)
			}
			cmd := exec.Command("helm", "template", "fugue", chartDir, "-f", valuesPath)
			cmd.Dir = chartDir
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("expected Helm template to reject missing hostIP:\n%s", output)
			}
			if !strings.Contains(string(output), test.want) {
				t.Fatalf("unexpected Helm error; want %q:\n%s", test.want, output)
			}
		})
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
    hostIP: 15.204.94.71
  udpAddr: :53
  tcpAddr: :53
  groups:
    - name: country-us
      edgeGroupID: edge-group-country-us
      answerIPs:
        - 15.204.94.71
      publicHostPorts:
        hostIP: 15.204.94.71
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
      publicHostPorts:
        hostIP: 51.38.126.103
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
		`hostIP: "51.38.126.103"`,
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
    hostIP: 15.204.94.71
  groups:
    - name: country-de
      edgeGroupID: edge-group-country-de
      answerIPs:
        - 51.38.126.103
      publicHostPorts:
        hostIP: 51.38.126.103
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

func TestImageCacheOnDeleteStrategyPreservesPodTemplate(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	render := func(strategy string) string {
		args := []string{"template", "fugue", chartDir, "--show-only", "templates/image-cache-daemonset.yaml"}
		if strategy != "" {
			args = append(args, "--set-string", "imageCache.updateStrategy.type="+strategy)
		}
		cmd := exec.Command("helm", args...)
		cmd.Dir = chartDir
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("helm template image-cache strategy %q failed: %v\n%s", strategy, err, output)
		}
		return string(output)
	}
	withoutUpdateStrategy := func(manifest string) string {
		lines := strings.Split(manifest, "\n")
		result := make([]string, 0, len(lines))
		skipping := false
		for _, line := range lines {
			if line == "  updateStrategy:" {
				if skipping {
					t.Fatal("nested image-cache updateStrategy block")
				}
				skipping = true
				result = append(result, "  updateStrategy: <normalized>")
				continue
			}
			if skipping {
				indent := len(line) - len(strings.TrimLeft(line, " "))
				if strings.TrimSpace(line) != "" && indent <= 2 {
					skipping = false
					result = append(result, line)
				}
				continue
			}
			result = append(result, line)
		}
		if skipping {
			t.Fatal("unterminated image-cache updateStrategy block")
		}
		return strings.Join(result, "\n")
	}

	rolling := render("")
	onDelete := render("OnDelete")
	rollingDoc := manifestDocumentForKindAndName(rolling, "DaemonSet", "fugue-fugue-image-cache")
	onDeleteDoc := manifestDocumentForKindAndName(onDelete, "DaemonSet", "fugue-fugue-image-cache")
	for _, want := range []string{"type: RollingUpdate", "maxUnavailable: 1", "maxSurge: 0"} {
		if !strings.Contains(rollingDoc, want) {
			t.Fatalf("default image-cache strategy missing %q:\n%s", want, rollingDoc)
		}
	}
	if !strings.Contains(onDeleteDoc, "type: OnDelete") {
		t.Fatalf("OnDelete image-cache render missing strategy:\n%s", onDeleteDoc)
	}
	for _, unwanted := range []string{"type: RollingUpdate", "rollingUpdate:", "maxUnavailable:", "maxSurge:"} {
		if strings.Contains(onDeleteDoc, unwanted) {
			t.Fatalf("OnDelete image-cache render contains %q:\n%s", unwanted, onDeleteDoc)
		}
	}
	if withoutUpdateStrategy(rolling) != withoutUpdateStrategy(onDelete) {
		t.Fatalf("image-cache OnDelete render changed fields outside spec.updateStrategy\nrolling:\n%s\nonDelete:\n%s", rolling, onDelete)
	}

	invalid := exec.Command("helm", "template", "fugue", chartDir, "--show-only", "templates/image-cache-daemonset.yaml", "--set-string", "imageCache.updateStrategy.type=Recreate")
	invalid.Dir = chartDir
	if output, err := invalid.CombinedOutput(); err == nil || !strings.Contains(string(output), "must be OnDelete or RollingUpdate") {
		t.Fatalf("invalid image-cache strategy must fail closed: err=%v\n%s", err, output)
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

func manifestStringDataValue(manifest string, key string) (string, bool) {
	prefix := key + ":"
	for _, line := range strings.Split(manifest, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, prefix) {
			continue
		}
		raw := strings.TrimSpace(strings.TrimPrefix(trimmed, prefix))
		value, err := strconv.Unquote(raw)
		if err != nil {
			return "", false
		}
		return value, true
	}
	return "", false
}

func manifestEnvBlock(manifest string, envName string) string {
	lines := strings.Split(manifest, "\n")
	want := "- name: " + envName
	for start, line := range lines {
		if strings.TrimSpace(line) != want {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " \t"))
		end := len(lines)
		for i := start + 1; i < len(lines); i++ {
			candidate := lines[i]
			candidateIndent := len(candidate) - len(strings.TrimLeft(candidate, " \t"))
			if candidateIndent == indent && strings.HasPrefix(strings.TrimSpace(candidate), "- name:") {
				end = i
				break
			}
		}
		return strings.Join(lines[start:end], "\n")
	}
	return ""
}

func copyChartWithPlatformComponentIdentityProbe(t *testing.T, sourceDir string) string {
	t.Helper()
	destinationDir := filepath.Join(t.TempDir(), "fugue")
	if err := os.CopyFS(destinationDir, os.DirFS(sourceDir)); err != nil {
		t.Fatalf("copy chart for component identity lifecycle test: %v", err)
	}
	const probeTemplate = `{{- $test := .Values.platformComponentIdentityResolutionTest -}}
{{- $existingData := dict
  "FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY" ((default "" $test.existingSigningKey) | b64enc)
  "FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID" ((default "" $test.existingSigningKeyID) | b64enc)
  "FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY" ((default "" $test.existingPreviousSigningKey) | b64enc)
  "FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID" ((default "" $test.existingPreviousSigningKeyID) | b64enc)
  "FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS" ((default "" $test.existingRevokedKeyIDs) | b64enc)
-}}
{{- $resolved := include "fugue.resolvePlatformComponentIdentityData" (dict "root" . "existingData" $existingData) | fromJson -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: fugue-component-identity-resolution-probe
data:
  signingKey: {{ index $resolved "signingKey" | quote }}
  signingKeyID: {{ index $resolved "signingKeyID" | quote }}
  previousSigningKey: {{ index $resolved "previousSigningKey" | quote }}
  previousSigningKeyID: {{ index $resolved "previousSigningKeyID" | quote }}
  revokedKeyIDs: {{ index $resolved "revokedKeyIDs" | quote }}
`
	probePath := filepath.Join(destinationDir, "templates", "platform-component-identity-resolution-probe.yaml")
	if err := os.WriteFile(probePath, []byte(probeTemplate), 0o600); err != nil {
		t.Fatalf("write component identity lifecycle probe template: %v", err)
	}
	return destinationDir
}

func renderPlatformComponentIdentityResolution(t *testing.T, chartDir string, settings ...string) string {
	t.Helper()
	args := []string{
		"template", "fugue", chartDir,
		"--set-string", "workloadIdentity.signingKey=workload-test-key",
		"--set-string", "bundle.signingKey=bundle-test-key",
		"--set-string", "postgres.password=postgres-test-password",
		"--set-string", "api.edgeTLSAskToken=edge-test-token",
	}
	for _, setting := range settings {
		args = append(args, "--set-string", setting)
	}
	cmd := exec.Command("helm", args...)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template component identity lifecycle probe failed: %v\n%s", err, output)
	}
	probe := manifestDocumentForKindAndName(string(output), "ConfigMap", "fugue-component-identity-resolution-probe")
	if probe == "" {
		t.Fatalf("rendered manifest missing component identity lifecycle probe:\n%s", output)
	}
	return probe
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
