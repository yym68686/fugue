package backupmaterializerchart_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializeridentity"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	testDigest     = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testCell       = "backup/app-database/0123456789abcdef"
	testRun        = "run-1"
	testName       = "fugue-backup-materializer-app-database-0123456789abcdef"
	testSecretName = "fugue-backup-observer-app-database-0123456789abcdef-input"
)

func TestBackupMaterializerChartDefaultsToNoResources(t *testing.T) {
	requireHelm(t)
	output, err := renderChart(t)
	if err != nil {
		t.Fatalf("helm template default chart: %v\n%s", err, output)
	}
	if strings.TrimSpace(string(output)) != "" {
		t.Fatalf("disabled chart must render zero resources:\n%s", output)
	}
}

func TestEnabledBackupMaterializerChartRendersOneCellScopedReadOnlyBoundary(t *testing.T) {
	requireHelm(t)
	if got := backupmaterializeridentity.ServiceAccountNameForCell(testCell); got != testName {
		t.Fatalf("chart ServiceAccount fixture = %q, identity policy requires %q", testName, got)
	}
	secretIdentity, err := materialization.SecretIdentityForCell(testCell)
	if err != nil || secretIdentity.Namespace != "fugue-system" || secretIdentity.SecretName != testSecretName {
		t.Fatalf("chart Secret fixture drifted from materialization contract: identity=%+v err=%v", secretIdentity, err)
	}
	output, err := renderChart(t, validEnabledArgs()...)
	if err != nil {
		t.Fatalf("helm template enabled chart: %v\n%s", err, output)
	}
	objects := decodeObjects(t, output)
	if got, want := objectKinds(objects), []string{
		"ServiceAccount/" + testName,
		"Role/" + testName,
		"RoleBinding/" + testName,
		"Deployment/" + testName,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("enabled chart objects = %v, want %v\n%s", got, want, output)
	}
	for _, object := range objects {
		if object.GetNamespace() != "fugue-system" {
			t.Fatalf("%s escaped fixed namespace: %q", object.GetKind(), object.GetNamespace())
		}
		for key, value := range map[string]string{
			"fugue.io/release-lane":        "backup",
			"fugue.io/ownership-mode":      "shadow",
			"fugue.io/production-mutation": "forbidden",
		} {
			if object.GetLabels()[key] != value {
				t.Fatalf("%s label %s = %q, want %q", object.GetKind(), key, object.GetLabels()[key], value)
			}
		}
	}

	var serviceAccount corev1.ServiceAccount
	decodeObject(t, objects[0], &serviceAccount)
	if serviceAccount.Name != testName || serviceAccount.AutomountServiceAccountToken == nil ||
		*serviceAccount.AutomountServiceAccountToken {
		t.Fatalf("ServiceAccount identity/default-token boundary drifted: %+v", serviceAccount)
	}

	var role rbacv1.Role
	decodeObject(t, objects[1], &role)
	wantRule := rbacv1.PolicyRule{
		APIGroups:     []string{""},
		Resources:     []string{"secrets"},
		ResourceNames: []string{testSecretName},
		Verbs:         []string{"get"},
	}
	if len(role.Rules) != 1 || !reflect.DeepEqual(role.Rules[0], wantRule) {
		t.Fatalf("Role must permit only GET of one cell Secret: %+v", role.Rules)
	}

	var binding rbacv1.RoleBinding
	decodeObject(t, objects[2], &binding)
	wantRoleRef := rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: testName}
	wantSubjects := []rbacv1.Subject{{Kind: "ServiceAccount", Name: testName, Namespace: "fugue-system"}}
	if !reflect.DeepEqual(binding.RoleRef, wantRoleRef) || !reflect.DeepEqual(binding.Subjects, wantSubjects) {
		t.Fatalf("RoleBinding escaped exact cell identity: roleRef=%+v subjects=%+v", binding.RoleRef, binding.Subjects)
	}

	var deployment appsv1.Deployment
	decodeObject(t, objects[3], &deployment)
	assertDeployment(t, deployment)
}

func assertDeployment(t *testing.T, deployment appsv1.Deployment) {
	t.Helper()
	wantAnnotations := map[string]string{
		"fugue.io/backup-cell-key":           testCell,
		"fugue.io/backup-run-id":             testRun,
		"fugue.io/backup-secret-name":        testSecretName,
		"fugue.io/input-token-audience":      "fugue-backup-materializer.fugue.dev",
		"fugue.io/kubernetes-token-audience": "cluster-default",
		"fugue.io/production-mutation":       "forbidden",
	}
	if deployment.Name != testName || !reflect.DeepEqual(deployment.Annotations, wantAnnotations) ||
		!reflect.DeepEqual(deployment.Spec.Template.Annotations, wantAnnotations) {
		t.Fatalf("Deployment cell/run identity drifted: name=%q object=%#v pod=%#v", deployment.Name, deployment.Annotations, deployment.Spec.Template.Annotations)
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 ||
		deployment.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType ||
		deployment.Spec.RevisionHistoryLimit == nil || *deployment.Spec.RevisionHistoryLimit != 2 {
		t.Fatalf("unsafe singleton deployment strategy: %+v", deployment.Spec)
	}

	pod := deployment.Spec.Template.Spec
	if pod.ServiceAccountName != testName || pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken ||
		pod.EnableServiceLinks == nil || *pod.EnableServiceLinks || pod.HostNetwork || pod.HostPID || pod.HostIPC ||
		pod.DNSPolicy != corev1.DNSClusterFirst || pod.RestartPolicy != corev1.RestartPolicyAlways ||
		pod.TerminationGracePeriodSeconds == nil || *pod.TerminationGracePeriodSeconds != 15 {
		t.Fatalf("Pod isolation boundary drifted: %+v", pod)
	}
	if pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot ||
		pod.SecurityContext.RunAsUser == nil || *pod.SecurityContext.RunAsUser != 65532 ||
		pod.SecurityContext.RunAsGroup == nil || *pod.SecurityContext.RunAsGroup != 65532 ||
		pod.SecurityContext.FSGroup == nil || *pod.SecurityContext.FSGroup != 65532 ||
		pod.SecurityContext.FSGroupChangePolicy == nil || *pod.SecurityContext.FSGroupChangePolicy != corev1.FSGroupChangeOnRootMismatch ||
		pod.SecurityContext.SeccompProfile == nil || pod.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("Pod security boundary drifted: %+v", pod.SecurityContext)
	}
	if len(pod.Containers) != 1 || len(pod.InitContainers) != 0 || len(pod.EphemeralContainers) != 0 {
		t.Fatalf("chart must render exactly one process container: %+v", pod)
	}
	container := pod.Containers[0]
	if container.Name != "backup-materializer" ||
		container.Image != "registry.example.test/fugue/fugue-backup-materializer@"+testDigest ||
		container.ImagePullPolicy != corev1.PullIfNotPresent || len(container.Ports) != 0 {
		t.Fatalf("container identity/network boundary drifted: %+v", container)
	}
	security := container.SecurityContext
	if security == nil || security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation ||
		security.Privileged == nil || *security.Privileged || security.ReadOnlyRootFilesystem == nil || !*security.ReadOnlyRootFilesystem ||
		security.RunAsNonRoot == nil || !*security.RunAsNonRoot || security.RunAsUser == nil || *security.RunAsUser != 65532 ||
		security.RunAsGroup == nil || *security.RunAsGroup != 65532 || security.Capabilities == nil ||
		!reflect.DeepEqual(security.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Fatalf("container security boundary drifted: %+v", security)
	}
	if container.Resources.Requests.Cpu().IsZero() || container.Resources.Requests.Memory().IsZero() ||
		container.Resources.Requests.StorageEphemeral().IsZero() || container.Resources.Limits.Cpu().IsZero() ||
		container.Resources.Limits.Memory().IsZero() || container.Resources.Limits.StorageEphemeral().IsZero() {
		t.Fatalf("container resources must be bounded: %+v", container.Resources)
	}

	wantEnv := map[string]string{
		"FUGUE_BACKUP_MATERIALIZER_ENABLED":                    "true",
		"FUGUE_BACKUP_MATERIALIZER_BIND_ADDR":                  "127.0.0.1:8093",
		"FUGUE_BACKUP_MATERIALIZER_CELL_KEY":                   testCell,
		"FUGUE_BACKUP_MATERIALIZER_RUN_ID":                     testRun,
		"FUGUE_BACKUP_MATERIALIZER_INPUT_API_BASE_URL":         "https://api.fugue-system.svc.cluster.local:8443",
		"FUGUE_BACKUP_MATERIALIZER_INPUT_PROJECTION_ROOT":      "/run/fugue/backup-materializer/input-api",
		"FUGUE_BACKUP_MATERIALIZER_KUBERNETES_API_URL":         "https://kubernetes.default.svc",
		"FUGUE_BACKUP_MATERIALIZER_KUBERNETES_PROJECTION_ROOT": "/run/fugue/backup-materializer/kubernetes-api",
		"FUGUE_BACKUP_MATERIALIZER_RECONCILE_INTERVAL":         "30s",
		"FUGUE_BACKUP_MATERIALIZER_ATTEMPT_TIMEOUT":            "20s",
		"FUGUE_BACKUP_MATERIALIZER_INPUT_REQUEST_TIMEOUT":      "5s",
		"FUGUE_BACKUP_MATERIALIZER_INPUT_HANDSHAKE_TIMEOUT":    "5s",
		"FUGUE_BACKUP_MATERIALIZER_SECRET_REQUEST_TIMEOUT":     "5s",
		"FUGUE_BACKUP_MATERIALIZER_SECRET_HANDSHAKE_TIMEOUT":   "5s",
		"FUGUE_BACKUP_MATERIALIZER_SHUTDOWN_TIMEOUT":           "10s",
		"FUGUE_BACKUP_MATERIALIZER_INPUT_MAX_RESPONSE_BYTES":   "65536",
		"FUGUE_BACKUP_MATERIALIZER_SECRET_MAX_RESPONSE_BYTES":  "131072",
	}
	gotEnv := make(map[string]string, len(container.Env))
	for _, variable := range container.Env {
		if variable.ValueFrom != nil {
			t.Fatalf("environment must not source a Secret or field: %+v", variable)
		}
		gotEnv[variable.Name] = variable.Value
	}
	if !reflect.DeepEqual(gotEnv, wantEnv) {
		t.Fatalf("environment = %#v, want %#v", gotEnv, wantEnv)
	}
	assertExecProbe(t, "startup", container.StartupProbe, "health")
	assertExecProbe(t, "liveness", container.LivenessProbe, "health")
	assertExecProbe(t, "readiness", container.ReadinessProbe, "ready")

	if len(pod.Volumes) != 2 || len(container.VolumeMounts) != 2 {
		t.Fatalf("expected two separate identity projections: volumes=%+v mounts=%+v", pod.Volumes, container.VolumeMounts)
	}
	assertIdentityProjection(t, pod.Volumes[0], "input-api-identity", "fugue-backup-materializer.fugue.dev", "fugue-input-api-ca", "api-ca.pem")
	assertIdentityProjection(t, pod.Volumes[1], "kubernetes-api-identity", "", "kube-root-ca.crt", "ca.crt")
	wantMounts := map[string]corev1.VolumeMount{
		"input-api-identity":      {Name: "input-api-identity", MountPath: "/run/fugue/backup-materializer/input-api", ReadOnly: true},
		"kubernetes-api-identity": {Name: "kubernetes-api-identity", MountPath: "/run/fugue/backup-materializer/kubernetes-api", ReadOnly: true},
	}
	for _, mount := range container.VolumeMounts {
		if want, ok := wantMounts[mount.Name]; !ok || !reflect.DeepEqual(mount, want) {
			t.Fatalf("identity mount escaped exact boundary: %+v", mount)
		}
		delete(wantMounts, mount.Name)
	}
	if len(wantMounts) != 0 {
		t.Fatalf("missing identity mounts: %+v", wantMounts)
	}
}

func TestBackupMaterializerResourceNamesAreReleaseIndependentAndCellDistinct(t *testing.T) {
	requireHelm(t)
	first, err := renderChartWithReleaseAndNamespace(t, "backup-materializer", "fugue-system", validEnabledArgs()...)
	if err != nil {
		t.Fatalf("first release render: %v\n%s", err, first)
	}
	second, err := renderChartWithReleaseAndNamespace(t, "other-release", "fugue-system", validEnabledArgs()...)
	if err != nil {
		t.Fatalf("second release render: %v\n%s", err, second)
	}
	if firstNames, secondNames := objectNames(decodeObjects(t, first)), objectNames(decodeObjects(t, second)); !reflect.DeepEqual(firstNames, secondNames) {
		t.Fatalf("cell resource names changed with release name: first=%v second=%v", firstNames, secondNames)
	}

	names := make(map[string]bool)
	for _, kind := range []string{"control-plane-db", "app-database", "persistent-storage", "data-workspace", "registry", "platform-component"} {
		cell := "backup/" + kind + "/0123456789abcdef"
		args := append(validEnabledArgs(), "--set-string", "cell.key="+cell)
		output, err := renderChart(t, args...)
		if err != nil {
			t.Fatalf("render canonical %s cell: %v\n%s", kind, err, output)
		}
		objects := decodeObjects(t, output)
		want := "fugue-backup-materializer-" + kind + "-0123456789abcdef"
		if len(objects) != 4 || len(want) > 63 || names[want] {
			t.Fatalf("canonical %s cell identity invalid: objects=%v want=%q", kind, objectKinds(objects), want)
		}
		for _, object := range objects {
			if object.GetName() != want {
				t.Fatalf("canonical %s resource name = %q, want %q", kind, object.GetName(), want)
			}
		}
		names[want] = true
	}
}

func TestEnabledBackupMaterializerChartRejectsUnsafeValues(t *testing.T) {
	requireHelm(t)
	tests := []struct {
		name      string
		namespace string
		args      []string
		want      string
	}{
		{name: "wrong namespace", namespace: "default", want: "only in namespace fugue-system"},
		{name: "string false enablement", args: []string{"--set-string", "enabled=false"}, want: "enabled must be exactly a boolean"},
		{name: "zero replicas", args: []string{"--set-string", "replicaCount=0"}, want: "replicaCount must be exactly 1"},
		{name: "multiple replicas", args: []string{"--set-string", "replicaCount=2"}, want: "replicaCount must be exactly 1"},
		{name: "missing repository", args: []string{"--set-string", "image.repository="}, want: "image.repository is required"},
		{name: "tagged repository", args: []string{"--set-string", "image.repository=registry.example.test/fugue/fugue-backup-materializer:latest"}, want: "without a tag or digest"},
		{name: "legacy artifact", args: []string{"--set-string", "image.repository=registry.example.test/fugue/fugue-api"}, want: "dedicated fugue-backup-materializer artifact"},
		{name: "missing digest", args: []string{"--set-string", "image.digest="}, want: "image.digest is required"},
		{name: "uppercase digest", args: []string{"--set-string", "image.digest=sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}, want: "exact lowercase sha256"},
		{name: "missing cell", args: []string{"--set-string", "cell.key="}, want: "cell.key is required"},
		{name: "wrong cell", args: []string{"--set-string", "cell.key=backup/all/0123456789abcdef"}, want: "canonical backup"},
		{name: "uppercase cell", args: []string{"--set-string", "cell.key=backup/app-database/0123456789ABCDEF"}, want: "canonical backup"},
		{name: "missing run", args: []string{"--set-string", "cell.runID="}, want: "cell.runID is required"},
		{name: "uppercase run", args: []string{"--set-string", "cell.runID=Run-1"}, want: "canonical backup run"},
		{name: "run path", args: []string{"--set-string", "cell.runID=run/1"}, want: "canonical backup run"},
		{name: "long run", args: []string{"--set-string", "cell.runID=" + strings.Repeat("a", 129)}, want: "canonical backup run"},
		{name: "plaintext input API", args: []string{"--set-string", "inputAPI.baseURL=http://api.example.test"}, want: "canonical lowercase absolute HTTPS authority"},
		{name: "input API path", args: []string{"--set-string", "inputAPI.baseURL=https://api.example.test/v1"}, want: "canonical lowercase absolute HTTPS authority"},
		{name: "input API query", args: []string{"--set-string", "inputAPI.baseURL=https://api.example.test?token=x"}, want: "canonical lowercase absolute HTTPS authority"},
		{name: "input API credentials", args: []string{"--set-string", "inputAPI.baseURL=https://user:pass@api.example.test"}, want: "canonical lowercase absolute HTTPS authority"},
		{name: "uppercase input API", args: []string{"--set-string", "inputAPI.baseURL=https://API.example.test"}, want: "canonical lowercase absolute HTTPS authority"},
		{name: "empty DNS label", args: []string{"--set-string", "inputAPI.baseURL=https://api..example.test"}, want: "canonical lowercase DNS labels"},
		{name: "leading label hyphen", args: []string{"--set-string", "inputAPI.baseURL=https://-api.example.test"}, want: "canonical lowercase DNS labels"},
		{name: "trailing label hyphen", args: []string{"--set-string", "inputAPI.baseURL=https://api-.example.test"}, want: "canonical lowercase DNS labels"},
		{name: "long DNS label", args: []string{"--set-string", "inputAPI.baseURL=https://" + strings.Repeat("a", 64) + ".example.test"}, want: "canonical lowercase DNS labels"},
		{name: "invalid input API port", args: []string{"--set-string", "inputAPI.baseURL=https://api.example.test:65536"}, want: "port must be between 1 and 65535"},
		{name: "missing input CA", args: []string{"--set-string", "inputAPI.ca.existingConfigMap.name="}, want: "inputAPI.ca.existingConfigMap.name"},
		{name: "uppercase input CA", args: []string{"--set-string", "inputAPI.ca.existingConfigMap.name=Input-CA"}, want: "canonical lowercase DNS label"},
		{name: "input CA path key", args: []string{"--set-string", "inputAPI.ca.existingConfigMap.key=../ca.crt"}, want: "canonical ConfigMap key"},
		{name: "fractional interval", args: []string{"--set-string", "runtime.reconcileInterval=500ms"}, want: "positive whole-second duration"},
		{name: "long interval", args: []string{"--set-string", "runtime.reconcileInterval=601s"}, want: "between 1s and 600s"},
		{name: "long attempt", args: []string{"--set-string", "runtime.attemptTimeout=61s"}, want: "between 1s and 60s"},
		{name: "input handshake above request", args: []string{"--set-string", "runtime.inputHandshakeTimeout=6s"}, want: "must not exceed runtime.inputRequestTimeout"},
		{name: "secret handshake above request", args: []string{"--set-string", "runtime.secretHandshakeTimeout=6s"}, want: "must not exceed runtime.secretRequestTimeout"},
		{name: "input request above attempt", args: []string{"--set-string", "runtime.inputRequestTimeout=21s"}, want: "must not exceed runtime.attemptTimeout"},
		{name: "secret request above attempt", args: []string{"--set-string", "runtime.secretRequestTimeout=21s"}, want: "must not exceed runtime.attemptTimeout"},
		{name: "long shutdown", args: []string{"--set-string", "runtime.shutdownTimeout=61s"}, want: "between 1s and 60s"},
		{name: "small input response", args: []string{"--set-string", "runtime.inputMaxResponseBytes=1023"}, want: "between 1024 and 65536"},
		{name: "large input response", args: []string{"--set-string", "runtime.inputMaxResponseBytes=65537"}, want: "between 1024 and 65536"},
		{name: "small Secret response", args: []string{"--set-string", "runtime.secretMaxResponseBytes=4095"}, want: "between 4096 and 262144"},
		{name: "large Secret response", args: []string{"--set-string", "runtime.secretMaxResponseBytes=262145"}, want: "between 4096 and 262144"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			namespace := test.namespace
			if namespace == "" {
				namespace = "fugue-system"
			}
			args := append(validEnabledArgs(), test.args...)
			output, err := renderChartWithReleaseAndNamespace(t, "backup-materializer", namespace, args...)
			if err == nil {
				t.Fatalf("helm template unexpectedly accepted unsafe values:\n%s", output)
			}
			if !strings.Contains(string(output), test.want) {
				t.Fatalf("helm error = %q, want substring %q", output, test.want)
			}
		})
	}
}

func validEnabledArgs() []string {
	return []string{
		"--set", "enabled=true",
		"--set-string", "image.repository=registry.example.test/fugue/fugue-backup-materializer",
		"--set-string", "image.digest=" + testDigest,
		"--set-string", "inputAPI.baseURL=https://api.fugue-system.svc.cluster.local:8443",
		"--set-string", "inputAPI.ca.existingConfigMap.name=fugue-input-api-ca",
		"--set-string", "inputAPI.ca.existingConfigMap.key=api-ca.pem",
		"--set-string", "cell.key=" + testCell,
		"--set-string", "cell.runID=" + testRun,
	}
}

func renderChart(t *testing.T, extraArgs ...string) ([]byte, error) {
	return renderChartWithReleaseAndNamespace(t, "backup-materializer", "fugue-system", extraArgs...)
}

func renderChartWithReleaseAndNamespace(t *testing.T, release, namespace string, extraArgs ...string) ([]byte, error) {
	t.Helper()
	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get chart directory: %v", err)
	}
	args := append([]string{"template", release, chartDir, "--namespace", namespace}, extraArgs...)
	command := exec.Command("helm", args...)
	command.Dir = chartDir
	return command.CombinedOutput()
}

func decodeObjects(t *testing.T, manifest []byte) []*unstructured.Unstructured {
	t.Helper()
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifest), 4096)
	var objects []*unstructured.Unstructured
	for {
		object := &unstructured.Unstructured{}
		err := decoder.Decode(object)
		if errors.Is(err, io.EOF) {
			return objects
		}
		if err != nil {
			t.Fatalf("decode rendered object: %v\n%s", err, manifest)
		}
		if len(object.Object) != 0 {
			objects = append(objects, object)
		}
	}
}

func decodeObject(t *testing.T, object *unstructured.Unstructured, target any) {
	t.Helper()
	encoded, err := json.Marshal(object.Object)
	if err != nil {
		t.Fatalf("marshal rendered %s: %v", object.GetKind(), err)
	}
	if err := json.Unmarshal(encoded, target); err != nil {
		t.Fatalf("decode rendered %s: %v", object.GetKind(), err)
	}
}

func objectKinds(objects []*unstructured.Unstructured) []string {
	result := make([]string, 0, len(objects))
	for _, object := range objects {
		result = append(result, object.GetKind()+"/"+object.GetName())
	}
	return result
}

func objectNames(objects []*unstructured.Unstructured) []string {
	result := make([]string, 0, len(objects))
	for _, object := range objects {
		result = append(result, object.GetName())
	}
	return result
}

func assertExecProbe(t *testing.T, name string, probe *corev1.Probe, target string) {
	t.Helper()
	want := []string{"/usr/local/bin/fugue-backup-materializer", "probe", target}
	if probe == nil || probe.Exec == nil || !reflect.DeepEqual(probe.Exec.Command, want) {
		t.Fatalf("%s probe = %+v, want exec %v", name, probe, want)
	}
}

func assertIdentityProjection(t *testing.T, volume corev1.Volume, name, audience, configMapName, configMapKey string) {
	t.Helper()
	projection := volume.Projected
	if volume.Name != name || projection == nil || projection.DefaultMode == nil || *projection.DefaultMode != 0o440 ||
		len(projection.Sources) != 2 {
		t.Fatalf("unsafe %s projection shell: %+v", name, volume)
	}
	token := projection.Sources[0].ServiceAccountToken
	if token == nil || token.Path != "token" || token.Audience != audience ||
		token.ExpirationSeconds == nil || *token.ExpirationSeconds != 600 {
		t.Fatalf("unsafe %s token projection: %+v", name, token)
	}
	configMap := projection.Sources[1].ConfigMap
	if configMap == nil || configMap.Name != configMapName || configMap.Optional == nil || *configMap.Optional ||
		len(configMap.Items) != 1 || configMap.Items[0].Key != configMapKey || configMap.Items[0].Path != "ca.crt" ||
		configMap.Items[0].Mode == nil || *configMap.Items[0].Mode != 0o440 {
		t.Fatalf("unsafe %s CA projection: %+v", name, configMap)
	}
}

func requireHelm(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
}
