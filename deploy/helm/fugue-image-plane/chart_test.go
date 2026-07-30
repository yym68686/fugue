package imageplanechart_test

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

const imagePlaneTestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestImagePlaneChartDefaultsToNoResources(t *testing.T) {
	requireImagePlaneHelm(t)
	output, err := renderImagePlaneChart(t)
	if err != nil {
		t.Fatalf("helm template default chart: %v\n%s", err, output)
	}
	if strings.TrimSpace(string(output)) != "" {
		t.Fatalf("disabled image-plane chart must render zero resources:\n%s", output)
	}
}

func TestEnabledImagePlaneChartRendersOneIsolatedShadowDaemonSet(t *testing.T) {
	requireImagePlaneHelm(t)
	output, err := renderImagePlaneChart(t, validImagePlaneArgs()...)
	if err != nil {
		t.Fatalf("helm template enabled chart: %v\n%s", err, output)
	}
	objects := decodeImagePlaneObjects(t, output)
	if len(objects) != 1 || objects[0].GetKind() != "DaemonSet" {
		t.Fatalf("enabled image-plane objects=%v, want exactly one DaemonSet\n%s", imagePlaneObjectKinds(objects), output)
	}
	encoded, err := json.Marshal(objects[0].Object)
	if err != nil {
		t.Fatalf("marshal rendered DaemonSet: %v", err)
	}
	var daemonSet appsv1.DaemonSet
	if err := json.Unmarshal(encoded, &daemonSet); err != nil {
		t.Fatalf("decode rendered DaemonSet: %v", err)
	}
	if daemonSet.Spec.UpdateStrategy.Type != appsv1.OnDeleteDaemonSetStrategyType ||
		daemonSet.Spec.RevisionHistoryLimit == nil || *daemonSet.Spec.RevisionHistoryLimit != 2 {
		t.Fatalf("shadow DaemonSet rollout boundary drifted: %+v", daemonSet.Spec)
	}
	for key, want := range map[string]string{
		"fugue.io/release-lane":        "image-plane",
		"fugue.io/ownership-mode":      "shadow",
		"fugue.io/production-mutation": "forbidden",
	} {
		if got := daemonSet.Labels[key]; got != want || daemonSet.Spec.Template.Labels[key] != want {
			t.Fatalf("shadow label %s drifted: metadata=%q pod=%q", key, got, daemonSet.Spec.Template.Labels[key])
		}
	}
	pod := daemonSet.Spec.Template.Spec
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken ||
		pod.EnableServiceLinks == nil || *pod.EnableServiceLinks || pod.ServiceAccountName != "" || pod.HostNetwork ||
		pod.DNSPolicy != corev1.DNSClusterFirst || pod.PriorityClassName != "" ||
		pod.TerminationGracePeriodSeconds == nil || *pod.TerminationGracePeriodSeconds != 35 ||
		len(pod.InitContainers) != 0 {
		t.Fatalf("shadow Pod identity/network/lifecycle boundary drifted: %+v", pod)
	}
	if !reflect.DeepEqual(pod.NodeSelector, map[string]string{"fugue.io/image-plane-shadow": "true"}) {
		t.Fatalf("shadow Pod escaped its opt-in node selector: %+v", pod.NodeSelector)
	}
	if pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot ||
		pod.SecurityContext.RunAsUser == nil || *pod.SecurityContext.RunAsUser != 65532 ||
		pod.SecurityContext.RunAsGroup == nil || *pod.SecurityContext.RunAsGroup != 65532 ||
		pod.SecurityContext.FSGroup == nil || *pod.SecurityContext.FSGroup != 65532 ||
		pod.SecurityContext.SeccompProfile == nil || pod.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("shadow Pod security context drifted: %+v", pod.SecurityContext)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("containers=%d, want one", len(pod.Containers))
	}
	container := pod.Containers[0]
	if container.Name != "image-plane-shadow" ||
		container.Image != "registry.example.test/fugue/image-cache@"+imagePlaneTestDigest ||
		container.ImagePullPolicy != corev1.PullIfNotPresent || len(container.Ports) != 0 || len(container.Command) != 0 ||
		!reflect.DeepEqual(container.Args, []string{"platform-plan-shadow"}) {
		t.Fatalf("shadow container artifact or network boundary drifted: %+v", container)
	}
	security := container.SecurityContext
	if security == nil || security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation ||
		security.Privileged == nil || *security.Privileged || security.ReadOnlyRootFilesystem == nil || !*security.ReadOnlyRootFilesystem ||
		security.RunAsNonRoot == nil || !*security.RunAsNonRoot || security.RunAsUser == nil || *security.RunAsUser != 65532 ||
		security.RunAsGroup == nil || *security.RunAsGroup != 65532 || security.Capabilities == nil ||
		!reflect.DeepEqual(security.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Fatalf("shadow container security boundary drifted: %+v", security)
	}
	if container.Resources.Requests.Cpu().IsZero() || container.Resources.Requests.Memory().IsZero() ||
		container.Resources.Limits.Cpu().IsZero() || container.Resources.Limits.Memory().IsZero() {
		t.Fatalf("shadow resources must be bounded: %+v", container.Resources)
	}

	wantEnv := map[string]string{
		"FUGUE_IMAGE_CACHE_LISTEN_ADDR":                      "127.0.0.1:5001",
		"FUGUE_IMAGE_CACHE_PLATFORM_PLAN_SHADOW_ENABLED":     "true",
		"FUGUE_API_BASE":                                     "https://api.fugue-system.svc.cluster.local:8443",
		"FUGUE_IMAGE_CACHE_PLATFORM_CREDENTIAL_FILE":         "/run/fugue/image-cache/platform-component-credential.json",
		"FUGUE_IMAGE_CACHE_REPLICATION_PLAN_PATH":            "/var/lib/fugue/image-cache/replication-plan.json",
		"FUGUE_IMAGE_CACHE_PLATFORM_PLAN_LONG_POLL":          "30s",
		"FUGUE_IMAGE_CACHE_PLATFORM_PLAN_REQUEST_TIMEOUT":    "40s",
		"FUGUE_IMAGE_CACHE_PLATFORM_PLAN_RETRY_MIN":          "2s",
		"FUGUE_IMAGE_CACHE_PLATFORM_PLAN_RETRY_MAX":          "60s",
		"FUGUE_IMAGE_CACHE_PLATFORM_PLAN_NO_PLAN_RETRY":      "15s",
		"FUGUE_IMAGE_CACHE_PLATFORM_CREDENTIAL_MIN_VALIDITY": "30s",
		"FUGUE_IMAGE_CACHE_PLATFORM_PLAN_ARCHIVE_LIMIT":      "5",
	}
	gotEnv := map[string]string{}
	for _, variable := range container.Env {
		if variable.Name == "FUGUE_IMAGE_CACHE_CLUSTER_NODE_NAME" {
			if variable.ValueFrom == nil || variable.ValueFrom.FieldRef == nil || variable.ValueFrom.FieldRef.FieldPath != "spec.nodeName" {
				t.Fatalf("node identity must come only from spec.nodeName: %+v", variable)
			}
			continue
		}
		if variable.ValueFrom != nil {
			t.Fatalf("unexpected environment valueFrom: %+v", variable)
		}
		gotEnv[variable.Name] = variable.Value
	}
	if !reflect.DeepEqual(gotEnv, wantEnv) {
		t.Fatalf("environment=%#v, want %#v", gotEnv, wantEnv)
	}
	for _, forbidden := range []string{
		"FUGUE_API_KEY",
		"FUGUE_NODE_UPDATER_TOKEN",
		"FUGUE_IMAGE_CACHE_MANAGEMENT_TOKEN",
		"FUGUE_IMAGE_CACHE_PLATFORM_PLAN_ALLOW_INSECURE_HTTP",
		"FUGUE_IMAGE_CACHE_STORE_DIR",
		"FUGUE_IMAGE_CACHE_DISK_LIMIT_ENABLED",
		"FUGUE_IMAGE_CACHE_REGISTRY_BASE",
		"FUGUE_IMAGE_CACHE_UPSTREAM_BASE",
	} {
		if _, exists := gotEnv[forbidden]; exists {
			t.Fatalf("shadow chart injected forbidden broad or insecure credential setting %s", forbidden)
		}
	}
	assertImagePlaneExecProbe(t, "startup", container.StartupProbe, "http://127.0.0.1:5001/healthz")
	assertImagePlaneExecProbe(t, "liveness", container.LivenessProbe, "http://127.0.0.1:5001/healthz")
	assertImagePlaneExecProbe(t, "readiness", container.ReadinessProbe, "http://127.0.0.1:5001/fugue/cache/v1/platform-plan/readyz")

	if len(pod.Volumes) != 2 || len(container.VolumeMounts) != 2 {
		t.Fatalf("expected exactly state and identity volumes: volumes=%+v mounts=%+v", pod.Volumes, container.VolumeMounts)
	}
	assertImagePlaneHostPath(t, pod.Volumes[0], "shadow-state", "/var/lib/fugue/image-plane-shadow")
	assertImagePlaneHostPath(t, pod.Volumes[1], "component-identity", "/run/fugue/image-cache")
	mounts := map[string]corev1.VolumeMount{}
	for _, mount := range container.VolumeMounts {
		mounts[mount.Name] = mount
	}
	if mounts["shadow-state"].ReadOnly || mounts["shadow-state"].MountPath != "/var/lib/fugue/image-cache" ||
		!mounts["component-identity"].ReadOnly || mounts["component-identity"].MountPath != "/run/fugue/image-cache" {
		t.Fatalf("shadow state/credential mount boundary drifted: %+v", mounts)
	}
}

func TestEnabledImagePlaneChartRejectsUnsafeOrUnboundedValues(t *testing.T) {
	requireImagePlaneHelm(t)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing repository", args: []string{"--set-string", "image.repository="}, want: "image.repository is required"},
		{name: "tagged repository", args: []string{"--set-string", "image.repository=registry.example.test/fugue/image-cache:latest"}, want: "without a tag or digest"},
		{name: "missing digest", args: []string{"--set-string", "image.digest="}, want: "image.digest is required"},
		{name: "uppercase digest", args: []string{"--set-string", "image.digest=sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}, want: "exact lowercase sha256"},
		{name: "plaintext API", args: []string{"--set-string", "api.baseURL=http://api.example.test"}, want: "absolute HTTPS"},
		{name: "API credentials", args: []string{"--set-string", "api.baseURL=https://user@api.example.test"}, want: "without credentials"},
		{name: "rolling update", args: []string{"--set-string", "updateStrategy.type=RollingUpdate"}, want: "exactly OnDelete"},
		{name: "priority preemption", args: []string{"--set-string", "priorityClassName=system-node-critical"}, want: "priorityClassName is forbidden"},
		{name: "empty selector", args: []string{"--set", "nodeSelector={}"}, want: "nodeSelector must contain only"},
		{name: "wrong selector", args: []string{"--set-string", "nodeSelector.fugue\\.io/image-plane-shadow=false"}, want: "nodeSelector must contain only"},
		{name: "broad selector", args: []string{"--set-string", "nodeSelector.extra=value"}, want: "nodeSelector must contain only"},
		{name: "relative credential path", args: []string{"--set-string", "credential.hostPath=run/fugue/image-cache"}, want: "absolute canonical host path"},
		{name: "credential traversal", args: []string{"--set-string", "credential.hostPath=/run/fugue/../secrets"}, want: "absolute canonical host path"},
		{name: "alternate credential path", args: []string{"--set-string", "credential.hostPath=/run/fugue/image-plane"}, want: "must remain exactly /run/fugue/image-cache"},
		{name: "relative state path", args: []string{"--set-string", "state.hostPath=var/lib/fugue/image-plane"}, want: "absolute canonical host path"},
		{name: "alternate state path", args: []string{"--set-string", "state.hostPath=/var/lib/fugue/another-shadow"}, want: "must remain exactly /var/lib/fugue/image-plane-shadow"},
		{name: "legacy state path", args: []string{"--set-string", "state.hostPath=/var/lib/fugue/image-cache"}, want: "isolated from state.legacyHostPath"},
		{name: "nested legacy state path", args: []string{"--set-string", "state.hostPath=/var/lib/fugue/image-cache/shadow"}, want: "isolated from state.legacyHostPath"},
		{name: "credential state overlap", args: []string{"--set-string", "state.hostPath=/run/fugue/image-cache/state"}, want: "state and credential host paths"},
		{name: "alternate legacy path", args: []string{"--set-string", "state.legacyHostPath=/var/lib/fugue/legacy-cache"}, want: "must remain exactly /var/lib/fugue/image-cache"},
		{name: "privileged port", args: []string{"--set-string", "runtime.port=443"}, want: "between 1024 and 65535"},
		{name: "oversized port", args: []string{"--set-string", "runtime.port=65536"}, want: "between 1024 and 65535"},
		{name: "fractional long poll", args: []string{"--set-string", "runtime.longPoll=500ms"}, want: "positive whole-second"},
		{name: "long poll over server bound", args: []string{"--set-string", "runtime.longPoll=31s"}, want: "between 1s and 30s"},
		{name: "request not above poll", args: []string{"--set-string", "runtime.requestTimeout=30s"}, want: "greater than runtime.longPoll"},
		{name: "retry inversion", args: []string{"--set-string", "runtime.retryMin=60s", "--set-string", "runtime.retryMax=30s"}, want: "greater than or equal"},
		{name: "small archive", args: []string{"--set-string", "runtime.archiveLimit=2"}, want: "between 3 and 32"},
		{name: "large archive", args: []string{"--set-string", "runtime.archiveLimit=33"}, want: "between 3 and 32"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := append(validImagePlaneArgs(), test.args...)
			output, err := renderImagePlaneChart(t, args...)
			if err == nil {
				t.Fatalf("helm template accepted unsafe values:\n%s", output)
			}
			if !strings.Contains(string(output), test.want) {
				t.Fatalf("helm error=%q, want substring %q", output, test.want)
			}
		})
	}
}

func validImagePlaneArgs() []string {
	return []string{
		"--set", "enabled=true",
		"--set-string", "image.repository=registry.example.test/fugue/image-cache",
		"--set-string", "image.digest=" + imagePlaneTestDigest,
		"--set-string", "api.baseURL=https://api.fugue-system.svc.cluster.local:8443",
	}
}

func renderImagePlaneChart(t *testing.T, extraArgs ...string) ([]byte, error) {
	t.Helper()
	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get chart directory: %v", err)
	}
	args := append([]string{"template", "image-plane-shadow", chartDir}, extraArgs...)
	command := exec.Command("helm", args...)
	command.Dir = chartDir
	return command.CombinedOutput()
}

func decodeImagePlaneObjects(t *testing.T, manifest []byte) []*unstructured.Unstructured {
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

func imagePlaneObjectKinds(objects []*unstructured.Unstructured) []string {
	kinds := make([]string, 0, len(objects))
	for _, object := range objects {
		kinds = append(kinds, object.GetKind()+"/"+object.GetName())
	}
	return kinds
}

func assertImagePlaneExecProbe(t *testing.T, name string, probe *corev1.Probe, endpoint string) {
	t.Helper()
	want := []string{"/usr/bin/wget", "-q", "-T", "2", "-Y", "off", "--spider", endpoint}
	if probe == nil || probe.Exec == nil || !reflect.DeepEqual(probe.Exec.Command, want) || probe.HTTPGet != nil || probe.TCPSocket != nil {
		t.Fatalf("%s probe must be loopback-only exec probe: %+v", name, probe)
	}
}

func assertImagePlaneHostPath(t *testing.T, volume corev1.Volume, name, path string) {
	t.Helper()
	if volume.Name != name || volume.HostPath == nil || volume.HostPath.Path != path ||
		volume.HostPath.Type == nil || *volume.HostPath.Type != corev1.HostPathDirectory {
		t.Fatalf("unsafe image-plane hostPath volume: %+v", volume)
	}
}

func requireImagePlaneHelm(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}
}
