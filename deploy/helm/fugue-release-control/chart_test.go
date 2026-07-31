package releasecontrolchart_test

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

const testDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestReleaseControlChartDefaultsToNoResources(t *testing.T) {
	requireHelm(t)
	output, err := renderChart(t)
	if err != nil {
		t.Fatalf("helm template default chart: %v\n%s", err, output)
	}
	if strings.TrimSpace(string(output)) != "" {
		t.Fatalf("disabled chart must render zero resources:\n%s", output)
	}
}

func TestEnabledReleaseControlChartRendersOneHardenedDeployment(t *testing.T) {
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
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 ||
		deployment.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType ||
		deployment.Spec.RevisionHistoryLimit == nil || *deployment.Spec.RevisionHistoryLimit != 2 {
		t.Fatalf("unsafe single-reconciler deployment strategy: %+v", deployment.Spec)
	}
	pod := deployment.Spec.Template.Spec
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken ||
		pod.EnableServiceLinks == nil || *pod.EnableServiceLinks || pod.ServiceAccountName != "" ||
		pod.TerminationGracePeriodSeconds == nil || *pod.TerminationGracePeriodSeconds != 20 {
		t.Fatalf("Pod must not receive Kubernetes API credentials or service links: %+v", pod)
	}
	if pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot ||
		pod.SecurityContext.RunAsUser == nil || *pod.SecurityContext.RunAsUser != 65532 ||
		pod.SecurityContext.RunAsGroup == nil || *pod.SecurityContext.RunAsGroup != 65532 ||
		pod.SecurityContext.FSGroup == nil || *pod.SecurityContext.FSGroup != 65532 ||
		pod.SecurityContext.SeccompProfile == nil || pod.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("Pod security boundary drifted: %+v", pod.SecurityContext)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(pod.Containers))
	}
	container := pod.Containers[0]
	if container.Image != "registry.example.test/fugue/release-control@"+testDigest ||
		container.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Fatalf("container image is not immutable: %q (%q)", container.Image, container.ImagePullPolicy)
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
		container.Resources.Limits.Cpu().IsZero() || container.Resources.Limits.Memory().IsZero() {
		t.Fatalf("container resources must be bounded: %+v", container.Resources)
	}

	wantEnv := map[string]string{
		"FUGUE_RELEASE_CONTROL_ENABLED":            "true",
		"FUGUE_RELEASE_CONTROL_BIND_ADDR":          "0.0.0.0:8091",
		"FUGUE_RELEASE_CONTROL_API_BASE_URL":       "https://api.fugue-system.svc.cluster.local:8443",
		"FUGUE_RELEASE_CONTROL_SPEC_FILE":          "/run/fugue/component-plan.json",
		"FUGUE_RELEASE_CONTROL_TOKEN_FILE":         "/run/secrets/release-control/token",
		"FUGUE_RELEASE_CONTROL_RECONCILE_INTERVAL": "30s",
		"FUGUE_RELEASE_CONTROL_ATTEMPT_TIMEOUT":    "45s",
		"FUGUE_RELEASE_CONTROL_REQUEST_TIMEOUT":    "10s",
		"FUGUE_RELEASE_CONTROL_SHUTDOWN_TIMEOUT":   "10s",
		"FUGUE_RELEASE_CONTROL_MAX_RESPONSE_BYTES": "2097152",
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
	assertHTTPProbe(t, "startup", container.StartupProbe, "/healthz")
	assertHTTPProbe(t, "liveness", container.LivenessProbe, "/healthz")
	assertHTTPProbe(t, "readiness", container.ReadinessProbe, "/readyz")

	if len(pod.Volumes) != 2 || len(container.VolumeMounts) != 2 {
		t.Fatalf("expected exactly two external Secret volumes: volumes=%+v mounts=%+v", pod.Volumes, container.VolumeMounts)
	}
	assertSecretVolume(t, pod.Volumes[0], "desired-spec", "release-control-plan", "component-plan.json", "component-plan.json")
	assertSecretVolume(t, pod.Volumes[1], "observer-token", "release-control-token", "observer-token", "token")
	for _, mount := range container.VolumeMounts {
		if !mount.ReadOnly {
			t.Fatalf("Secret mount %q must be read-only", mount.Name)
		}
	}
}

func TestReleaseControlTerminationGraceIncludesShutdownBudget(t *testing.T) {
	requireHelm(t)
	args := append(validEnabledArgs(), "--set-string", "runtime.shutdownTimeout=30s")
	output, err := renderChart(t, args...)
	if err != nil {
		t.Fatalf("helm template custom shutdown budget: %v\n%s", err, output)
	}
	objects := decodeObjects(t, output)
	encoded, err := json.Marshal(objects[0].Object)
	if err != nil {
		t.Fatalf("marshal rendered Deployment: %v", err)
	}
	var deployment appsv1.Deployment
	if err := json.Unmarshal(encoded, &deployment); err != nil {
		t.Fatalf("decode rendered Deployment: %v", err)
	}
	grace := deployment.Spec.Template.Spec.TerminationGracePeriodSeconds
	if grace == nil || *grace != 40 {
		t.Fatalf("termination grace = %v, want shutdown timeout plus 10 seconds", grace)
	}
}

func TestEnabledReleaseControlChartRejectsUnsafeValues(t *testing.T) {
	requireHelm(t)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "zero replicas", args: []string{"--set-string", "replicaCount=0"}, want: "replicaCount must be exactly 1"},
		{name: "multiple replicas", args: []string{"--set-string", "replicaCount=2"}, want: "replicaCount must be exactly 1"},
		{name: "missing repository", args: []string{"--set-string", "image.repository="}, want: "image.repository is required"},
		{name: "tagged repository", args: []string{"--set-string", "image.repository=registry.example.test/fugue/release-control:latest"}, want: "without a tag or digest"},
		{name: "missing digest", args: []string{"--set-string", "image.digest="}, want: "image.digest is required"},
		{name: "noncanonical digest", args: []string{"--set-string", "image.digest=sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}, want: "exact lowercase sha256 digest"},
		{name: "non HTTP API", args: []string{"--set-string", "api.baseURL=ftp://api.example.test"}, want: "absolute HTTP(S) URL"},
		{name: "API credentials", args: []string{"--set-string", "api.baseURL=https://user@api.example.test"}, want: "without credentials"},
		{name: "missing spec secret", args: []string{"--set-string", "spec.existingSecret.name="}, want: "spec.existingSecret.name is required"},
		{name: "missing token key", args: []string{"--set-string", "token.existingSecret.key="}, want: "token.existingSecret.key is required"},
		{name: "shared credential secret", args: []string{"--set-string", "token.existingSecret.name=release-control-plan"}, want: "different externally owned Secrets"},
		{name: "short interval", args: []string{"--set-string", "runtime.reconcileInterval=500ms"}, want: "positive whole-second duration"},
		{name: "long interval", args: []string{"--set-string", "runtime.reconcileInterval=601s"}, want: "must not exceed 600s"},
		{name: "long attempt", args: []string{"--set-string", "runtime.attemptTimeout=121s"}, want: "must not exceed 120s"},
		{name: "long request", args: []string{"--set-string", "runtime.requestTimeout=31s"}, want: "must not exceed 30s"},
		{name: "oversized response", args: []string{"--set-string", "runtime.maxResponseBytes=8388609"}, want: "no greater than 8388608"},
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
		"--set-string", "image.repository=registry.example.test/fugue/release-control",
		"--set-string", "image.digest=" + testDigest,
		"--set-string", "api.baseURL=https://api.fugue-system.svc.cluster.local:8443",
		"--set-string", "spec.existingSecret.name=release-control-plan",
		"--set-string", "spec.existingSecret.key=component-plan.json",
		"--set-string", "token.existingSecret.name=release-control-token",
		"--set-string", "token.existingSecret.key=observer-token",
	}
}

func renderChart(t *testing.T, extraArgs ...string) ([]byte, error) {
	t.Helper()
	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	args := append([]string{"template", "release-control", chartDir}, extraArgs...)
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

func assertHTTPProbe(t *testing.T, name string, probe *corev1.Probe, path string) {
	t.Helper()
	if probe == nil || probe.HTTPGet == nil || probe.HTTPGet.Path != path || probe.HTTPGet.Port.StrVal != "health" {
		t.Fatalf("%s probe must GET %s on named health port: %+v", name, path, probe)
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
		t.Skip("helm not installed")
	}
}
