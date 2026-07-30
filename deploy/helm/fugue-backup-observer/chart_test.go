package backupobserverchart_test

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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	testDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testCell   = "backup/app-database/0123456789abcdef"
)

func TestBackupObserverChartDefaultsToNoResources(t *testing.T) {
	requireHelm(t)
	output, err := renderChart(t)
	if err != nil {
		t.Fatalf("helm template default chart: %v\n%s", err, output)
	}
	if strings.TrimSpace(string(output)) != "" {
		t.Fatalf("disabled chart must render zero resources:\n%s", output)
	}
}

func TestEnabledBackupObserverChartRendersOneCellScopedHardenedDeployment(t *testing.T) {
	requireHelm(t)
	output, err := renderChart(t, validEnabledArgs()...)
	if err != nil {
		t.Fatalf("helm template enabled chart: %v\n%s", err, output)
	}
	objects := decodeObjects(t, output)
	if len(objects) != 1 || objects[0].GetKind() != "Deployment" {
		t.Fatalf("enabled chart objects = %v, want exactly one Deployment\n%s", objectKinds(objects), output)
	}

	encoded, err := json.Marshal(objects[0].Object)
	if err != nil {
		t.Fatalf("marshal rendered Deployment: %v", err)
	}
	var deployment appsv1.Deployment
	if err := json.Unmarshal(encoded, &deployment); err != nil {
		t.Fatalf("decode rendered Deployment: %v", err)
	}
	wantName := "fugue-backup-observer-app-database-0123456789abcdef"
	if deployment.Name != wantName || deployment.Namespace != "fugue-system" {
		t.Fatalf("cell-scoped identity = %s/%s, want fugue-system/%s", deployment.Namespace, deployment.Name, wantName)
	}
	if deployment.Annotations["fugue.io/backup-cell-key"] != testCell ||
		deployment.Annotations["fugue.io/production-mutation"] != "forbidden" {
		t.Fatalf("deployment annotations drifted: %#v", deployment.Annotations)
	}
	for key, value := range map[string]string{
		"fugue.io/release-lane":        "backup",
		"fugue.io/ownership-mode":      "shadow",
		"fugue.io/production-mutation": "forbidden",
	} {
		if deployment.Labels[key] != value || deployment.Spec.Template.Labels[key] != value {
			t.Fatalf("deployment label %s is not bound to %q: object=%#v pod=%#v", key, value, deployment.Labels, deployment.Spec.Template.Labels)
		}
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 ||
		deployment.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType ||
		deployment.Spec.RevisionHistoryLimit == nil || *deployment.Spec.RevisionHistoryLimit != 2 {
		t.Fatalf("unsafe singleton deployment strategy: %+v", deployment.Spec)
	}

	pod := deployment.Spec.Template.Spec
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken ||
		pod.EnableServiceLinks == nil || *pod.EnableServiceLinks || pod.ServiceAccountName != "" ||
		pod.HostNetwork || pod.HostPID || pod.HostIPC || pod.DNSPolicy != corev1.DNSClusterFirst ||
		pod.RestartPolicy != corev1.RestartPolicyAlways || pod.TerminationGracePeriodSeconds == nil ||
		*pod.TerminationGracePeriodSeconds != 15 {
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
	if len(pod.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(pod.Containers))
	}
	container := pod.Containers[0]
	if container.Name != "backup-observer" ||
		container.Image != "registry.example.test/fugue/fugue-backup-observer@"+testDigest ||
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
	ephemeralLimit := container.Resources.Limits[corev1.ResourceEphemeralStorage]
	if container.Resources.Requests.Cpu().IsZero() || container.Resources.Requests.Memory().IsZero() ||
		container.Resources.Limits.Cpu().IsZero() || container.Resources.Limits.Memory().IsZero() || ephemeralLimit.IsZero() {
		t.Fatalf("container resources must be bounded: %+v", container.Resources)
	}

	wantEnv := map[string]string{
		"FUGUE_BACKUP_OBSERVER_ENABLED":            "true",
		"FUGUE_BACKUP_OBSERVER_BIND_ADDR":          "127.0.0.1:8092",
		"FUGUE_BACKUP_OBSERVER_CELL_KEY":           testCell,
		"FUGUE_BACKUP_OBSERVER_SPEC_FILE":          "/run/fugue/backup-observer/spec/spec.json",
		"FUGUE_BACKUP_OBSERVER_TOKEN_FILE":         "/run/fugue/backup-observer/token/token",
		"FUGUE_BACKUP_OBSERVER_API_BASE_URL":       "https://api.fugue-system.svc.cluster.local:8443",
		"FUGUE_BACKUP_OBSERVER_RECONCILE_INTERVAL": "30s",
		"FUGUE_BACKUP_OBSERVER_ATTEMPT_TIMEOUT":    "20s",
		"FUGUE_BACKUP_OBSERVER_REQUEST_TIMEOUT":    "10s",
		"FUGUE_BACKUP_OBSERVER_SHUTDOWN_TIMEOUT":   "10s",
		"FUGUE_BACKUP_OBSERVER_MAX_RESPONSE_BYTES": "65536",
	}
	gotEnv := make(map[string]string, len(container.Env))
	for _, variable := range container.Env {
		if variable.ValueFrom != nil {
			t.Fatalf("environment must not source a Secret: %+v", variable)
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
		t.Fatalf("expected exactly two external projected volumes: volumes=%+v mounts=%+v", pod.Volumes, container.VolumeMounts)
	}
	assertConfigMapVolume(t, pod.Volumes[0], "desired-spec", "backup-app-database-spec", "desired.json", "spec.json")
	assertSecretVolume(t, pod.Volumes[1], "observer-token", "backup-app-database-token", "observer-token", "token")
	for _, mount := range container.VolumeMounts {
		if !mount.ReadOnly || mount.SubPath != "" || mount.SubPathExpr != "" {
			t.Fatalf("projected mount must be read-only and cannot use subPath: %+v", mount)
		}
	}
	if pod.InitContainers != nil || pod.EphemeralContainers != nil {
		t.Fatalf("chart must render one process container only: init=%+v ephemeral=%+v", pod.InitContainers, pod.EphemeralContainers)
	}
}

func TestBackupObserverChartResourceNameIsIndependentOfReleaseName(t *testing.T) {
	requireHelm(t)
	first, err := renderChartWithRelease(t, "backup-observer", validEnabledArgs()...)
	if err != nil {
		t.Fatalf("first release render: %v\n%s", err, first)
	}
	second, err := renderChartWithRelease(t, "another-release", validEnabledArgs()...)
	if err != nil {
		t.Fatalf("second release render: %v\n%s", err, second)
	}
	firstObjects := decodeObjects(t, first)
	secondObjects := decodeObjects(t, second)
	if len(firstObjects) != 1 || len(secondObjects) != 1 || firstObjects[0].GetName() != secondObjects[0].GetName() {
		t.Fatalf("cell resource name changed with release name: first=%v second=%v", objectKinds(firstObjects), objectKinds(secondObjects))
	}
}

func TestCanonicalBackupCellKindsRenderDistinctDNSNames(t *testing.T) {
	requireHelm(t)
	names := make(map[string]bool)
	for _, kind := range []string{"control-plane-db", "app-database", "persistent-storage", "data-workspace", "registry", "platform-component"} {
		cell := "backup/" + kind + "/0123456789abcdef"
		args := append(validEnabledArgs(), "--set-string", "cell.key="+cell)
		output, err := renderChart(t, args...)
		if err != nil {
			t.Fatalf("render canonical %s cell: %v\n%s", kind, err, output)
		}
		objects := decodeObjects(t, output)
		if len(objects) != 1 {
			t.Fatalf("canonical %s cell rendered %v", kind, objectKinds(objects))
		}
		want := "fugue-backup-observer-" + kind + "-0123456789abcdef"
		if got := objects[0].GetName(); got != want || len(got) > 63 || names[got] {
			t.Fatalf("canonical cell resource name = %q, want unique %q", got, want)
		}
		names[want] = true
	}
}

func TestEnabledBackupObserverChartRejectsUnsafeValues(t *testing.T) {
	requireHelm(t)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "string false enablement", args: []string{"--set-string", "enabled=false"}, want: "enabled must be exactly a boolean"},
		{name: "zero replicas", args: []string{"--set-string", "replicaCount=0"}, want: "replicaCount must be exactly 1"},
		{name: "multiple replicas", args: []string{"--set-string", "replicaCount=2"}, want: "replicaCount must be exactly 1"},
		{name: "missing repository", args: []string{"--set-string", "image.repository="}, want: "image.repository is required"},
		{name: "tagged repository", args: []string{"--set-string", "image.repository=registry.example.test/fugue/fugue-backup-observer:latest"}, want: "without a tag or digest"},
		{name: "legacy artifact", args: []string{"--set-string", "image.repository=registry.example.test/fugue/fugue-api"}, want: "dedicated fugue-backup-observer artifact"},
		{name: "missing digest", args: []string{"--set-string", "image.digest="}, want: "image.digest is required"},
		{name: "uppercase digest", args: []string{"--set-string", "image.digest=sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}, want: "exact lowercase sha256"},
		{name: "missing cell", args: []string{"--set-string", "cell.key="}, want: "cell.key is required"},
		{name: "wrong cell", args: []string{"--set-string", "cell.key=backup/all/0123456789abcdef"}, want: "canonical backup"},
		{name: "uppercase cell", args: []string{"--set-string", "cell.key=backup/app-database/0123456789ABCDEF"}, want: "canonical backup"},
		{name: "plaintext API", args: []string{"--set-string", "api.baseURL=http://api.example.test"}, want: "canonical absolute HTTPS"},
		{name: "API credentials", args: []string{"--set-string", "api.baseURL=https://user:pass@api.example.test"}, want: "canonical absolute HTTPS"},
		{name: "API query", args: []string{"--set-string", "api.baseURL=https://api.example.test?token=x"}, want: "canonical absolute HTTPS"},
		{name: "API traversal", args: []string{"--set-string", "api.baseURL=https://api.example.test/../v1"}, want: "canonical absolute HTTPS"},
		{name: "API invalid port", args: []string{"--set-string", "api.baseURL=https://api.example.test:65536"}, want: "port must be between 1 and 65535"},
		{name: "missing spec ConfigMap", args: []string{"--set-string", "spec.existingConfigMap.name="}, want: "spec.existingConfigMap.name"},
		{name: "invalid spec key", args: []string{"--set-string", "spec.existingConfigMap.key=../spec"}, want: "canonical ConfigMap/Secret key"},
		{name: "invalid token Secret", args: []string{"--set-string", "token.existingSecret.name=Token"}, want: "canonical lowercase DNS label"},
		{name: "invalid token key", args: []string{"--set-string", "token.existingSecret.key=token/path"}, want: "canonical ConfigMap/Secret key"},
		{name: "fractional interval", args: []string{"--set-string", "runtime.reconcileInterval=500ms"}, want: "positive whole-second duration"},
		{name: "long interval", args: []string{"--set-string", "runtime.reconcileInterval=601s"}, want: "between 1s and 600s"},
		{name: "request not below attempt", args: []string{"--set-string", "runtime.requestTimeout=20s"}, want: "less than runtime.attemptTimeout"},
		{name: "long request", args: []string{"--set-string", "runtime.requestTimeout=31s"}, want: "between 1s and 30s"},
		{name: "long shutdown", args: []string{"--set-string", "runtime.shutdownTimeout=61s"}, want: "between 1s and 60s"},
		{name: "oversized response", args: []string{"--set-string", "runtime.maxResponseBytes=1048577"}, want: "no greater than 1048576"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := append(validEnabledArgs(), test.args...)
			output, err := renderChart(t, args...)
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
		"--set-string", "image.repository=registry.example.test/fugue/fugue-backup-observer",
		"--set-string", "image.digest=" + testDigest,
		"--set-string", "api.baseURL=https://api.fugue-system.svc.cluster.local:8443",
		"--set-string", "cell.key=" + testCell,
		"--set-string", "spec.existingConfigMap.name=backup-app-database-spec",
		"--set-string", "spec.existingConfigMap.key=desired.json",
		"--set-string", "token.existingSecret.name=backup-app-database-token",
		"--set-string", "token.existingSecret.key=observer-token",
	}
}

func renderChart(t *testing.T, extraArgs ...string) ([]byte, error) {
	return renderChartWithRelease(t, "backup-observer", extraArgs...)
}

func renderChartWithRelease(t *testing.T, release string, extraArgs ...string) ([]byte, error) {
	t.Helper()
	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get chart directory: %v", err)
	}
	args := append([]string{"template", release, chartDir, "--namespace", "fugue-system"}, extraArgs...)
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

func objectKinds(objects []*unstructured.Unstructured) []string {
	kinds := make([]string, 0, len(objects))
	for _, object := range objects {
		kinds = append(kinds, object.GetKind()+"/"+object.GetName())
	}
	return kinds
}

func assertExecProbe(t *testing.T, name string, probe *corev1.Probe, target string) {
	t.Helper()
	want := []string{"/usr/local/bin/fugue-backup-observer", "probe", target}
	if probe == nil || probe.Exec == nil || !reflect.DeepEqual(probe.Exec.Command, want) {
		t.Fatalf("%s probe = %+v, want exec %v", name, probe, want)
	}
}

func assertConfigMapVolume(t *testing.T, volume corev1.Volume, volumeName, configMapName, key, path string) {
	t.Helper()
	configMap := volume.ConfigMap
	if volume.Name != volumeName || configMap == nil || configMap.Name != configMapName || configMap.Optional == nil || *configMap.Optional ||
		configMap.DefaultMode == nil || *configMap.DefaultMode != 0o444 || len(configMap.Items) != 1 ||
		configMap.Items[0].Key != key || configMap.Items[0].Path != path {
		t.Fatalf("unsafe external ConfigMap volume: %+v", volume)
	}
}

func assertSecretVolume(t *testing.T, volume corev1.Volume, volumeName, secretName, key, path string) {
	t.Helper()
	secret := volume.Secret
	if volume.Name != volumeName || secret == nil || secret.SecretName != secretName || secret.Optional == nil || *secret.Optional ||
		secret.DefaultMode == nil || *secret.DefaultMode != 0o440 || len(secret.Items) != 1 ||
		secret.Items[0].Key != key || secret.Items[0].Path != path {
		t.Fatalf("unsafe external Secret volume: %+v", volume)
	}
}

func requireHelm(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skipf("helm is required for chart tests: %v", err)
	}
}
