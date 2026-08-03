package edgecontrolchart_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	testDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testSource = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestChartDefaultsToZeroObjects(t *testing.T) {
	requireHelm(t)
	output, err := render(t)
	if err != nil {
		t.Fatalf("render disabled chart: %v\n%s", err, output)
	}
	if strings.TrimSpace(string(output)) != "" {
		t.Fatalf("disabled chart rendered objects:\n%s", output)
	}
}

func TestEnabledBoundaryIsHardenedAndNonAuthoritative(t *testing.T) {
	requireHelm(t)
	output, err := render(t, enabledArgs()...)
	if err != nil {
		t.Fatalf("render enabled chart: %v\n%s", err, output)
	}
	objects := decodeObjects(t, output)
	wantKinds := []string{
		"NetworkPolicy/edge-control-fugue-edge-control",
		"PodDisruptionBudget/edge-control-fugue-edge-control",
		"ServiceAccount/edge-control-fugue-edge-control",
		"Service/edge-control-fugue-edge-control",
		"Deployment/edge-control-fugue-edge-control",
	}
	if got := objectKinds(objects); !reflect.DeepEqual(got, wantKinds) {
		t.Fatalf("rendered objects = %v, want %v\n%s", got, wantKinds, output)
	}

	var deployment appsv1.Deployment
	decodeTyped(t, objectByKind(t, objects, "Deployment"), &deployment)
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 ||
		deployment.Spec.Strategy.Type != appsv1.RollingUpdateDeploymentStrategyType ||
		deployment.Spec.Strategy.RollingUpdate == nil ||
		deployment.Spec.Strategy.RollingUpdate.MaxUnavailable.IntValue() != 0 ||
		deployment.Spec.Strategy.RollingUpdate.MaxSurge.IntValue() != 1 {
		t.Fatalf("unsafe deployment strategy: %+v", deployment.Spec)
	}
	pod := deployment.Spec.Template.Spec
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken ||
		pod.EnableServiceLinks == nil || *pod.EnableServiceLinks ||
		pod.ServiceAccountName != "edge-control-fugue-edge-control" || len(pod.Volumes) != 0 {
		t.Fatalf("unexpected Pod capability: %+v", pod)
	}
	if pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot ||
		pod.SecurityContext.RunAsUser == nil || *pod.SecurityContext.RunAsUser != 65532 ||
		pod.SecurityContext.RunAsGroup == nil || *pod.SecurityContext.RunAsGroup != 65532 ||
		pod.SecurityContext.SeccompProfile == nil || pod.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("unsafe Pod security context: %+v", pod.SecurityContext)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(pod.Containers))
	}
	container := pod.Containers[0]
	if container.Name != "edge-control" || container.Image != "registry.example.test/fugue/edge-control@"+testDigest ||
		container.ImagePullPolicy != corev1.PullIfNotPresent || len(container.EnvFrom) != 0 || len(container.VolumeMounts) != 0 {
		t.Fatalf("unexpected container identity/capability: %+v", container)
	}
	wantEnv := map[string]string{
		"FUGUE_EDGE_CONTROL_ENABLED":          "true",
		"FUGUE_EDGE_CONTROL_BIND_ADDR":        "0.0.0.0:8092",
		"FUGUE_EDGE_CONTROL_SHUTDOWN_TIMEOUT": "10s",
	}
	gotEnv := make(map[string]string, len(container.Env))
	for _, variable := range container.Env {
		if variable.ValueFrom != nil {
			t.Fatalf("edge-control boundary must not project credentials: %+v", variable)
		}
		gotEnv[variable.Name] = variable.Value
	}
	if !reflect.DeepEqual(gotEnv, wantEnv) {
		t.Fatalf("environment = %#v, want %#v", gotEnv, wantEnv)
	}
	security := container.SecurityContext
	if security == nil || security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation ||
		security.Privileged == nil || *security.Privileged || security.ReadOnlyRootFilesystem == nil || !*security.ReadOnlyRootFilesystem ||
		security.Capabilities == nil || !reflect.DeepEqual(security.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Fatalf("unsafe container security context: %+v", security)
	}
	if container.Resources.Requests.Cpu().IsZero() || container.Resources.Requests.Memory().IsZero() ||
		container.Resources.Limits.Cpu().IsZero() || container.Resources.Limits.Memory().IsZero() {
		t.Fatalf("resources are not bounded: %+v", container.Resources)
	}
	for name, probe := range map[string]*corev1.Probe{"startup": container.StartupProbe, "liveness": container.LivenessProbe, "readiness": container.ReadinessProbe} {
		if probe == nil || probe.HTTPGet == nil || probe.HTTPGet.Port.StrVal != "health" {
			t.Fatalf("%s probe is not local HTTP: %+v", name, probe)
		}
	}
	if got := deployment.Spec.Template.Annotations["fugue.pro/source-commit"]; got != testSource {
		t.Fatalf("source annotation = %q, want %q", got, testSource)
	}
	if got := deployment.Spec.Template.Annotations["fugue.pro/edge-control-authority"]; got != "none" {
		t.Fatalf("authority annotation = %q, want none", got)
	}

	var service corev1.Service
	decodeTyped(t, objectByKind(t, objects, "Service"), &service)
	if service.Spec.Type != corev1.ServiceTypeClusterIP || len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 8092 {
		t.Fatalf("service escaped cluster-local boundary: %+v", service.Spec)
	}
	var pdb policyv1.PodDisruptionBudget
	decodeTyped(t, objectByKind(t, objects, "PodDisruptionBudget"), &pdb)
	if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.IntValue() != 1 {
		t.Fatalf("PDB does not preserve the only shadow replica: %+v", pdb.Spec)
	}
	var networkPolicy networkingv1.NetworkPolicy
	decodeTyped(t, objectByKind(t, objects, "NetworkPolicy"), &networkPolicy)
	if !reflect.DeepEqual(networkPolicy.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress}) ||
		len(networkPolicy.Spec.Egress) != 0 || len(networkPolicy.Spec.Ingress) != 1 {
		t.Fatalf("network policy is not default-deny egress/restricted ingress: %+v", networkPolicy.Spec)
	}
	peer := networkPolicy.Spec.Ingress[0].From
	if len(peer) != 1 || peer[0].NamespaceSelector == nil || peer[0].PodSelector == nil ||
		peer[0].PodSelector.MatchLabels["fugue.io/edge-control-client"] != "true" {
		t.Fatalf("network policy ingress is not explicit: %+v", networkPolicy.Spec.Ingress)
	}
}

func TestEnabledChartRejectsMutableOrAmbiguousValues(t *testing.T) {
	requireHelm(t)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "zero replicas", args: []string{"--set-string", "replicaCount=0"}, want: "replicaCount must be exactly 1"},
		{name: "two replicas", args: []string{"--set-string", "replicaCount=2"}, want: "replicaCount must be exactly 1"},
		{name: "missing repository", args: []string{"--set-string", "image.repository="}, want: "image.repository is required"},
		{name: "tagged repository", args: []string{"--set-string", "image.repository=registry.example.test/fugue/edge-control:latest"}, want: "without a tag or digest"},
		{name: "missing digest", args: []string{"--set-string", "image.digest="}, want: "image.digest is required"},
		{name: "uppercase digest", args: []string{"--set-string", "image.digest=sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}, want: "exact lowercase sha256"},
		{name: "missing source", args: []string{"--set-string", "image.sourceCommit="}, want: "image.sourceCommit is required"},
		{name: "short source", args: []string{"--set-string", "image.sourceCommit=abc"}, want: "exact lowercase 40-hex"},
		{name: "privileged port", args: []string{"--set-string", "service.port=443"}, want: "between 1024 and 65535"},
		{name: "zero shutdown", args: []string{"--set-string", "runtime.shutdownTimeout=0s"}, want: "positive whole-second"},
		{name: "long shutdown", args: []string{"--set-string", "runtime.shutdownTimeout=121s"}, want: "must not exceed 120s"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := append(enabledArgs(), test.args...)
			output, err := render(t, args...)
			if err == nil || !strings.Contains(string(output), test.want) {
				t.Fatalf("render error=%v output=%q, want %q", err, output, test.want)
			}
		})
	}
}

func enabledArgs() []string {
	return []string{
		"--set", "enabled=true",
		"--set-string", "image.repository=registry.example.test/fugue/edge-control",
		"--set-string", "image.digest=" + testDigest,
		"--set-string", "image.sourceCommit=" + testSource,
	}
}

func render(t *testing.T, extra ...string) ([]byte, error) {
	t.Helper()
	args := append([]string{"template", "edge-control", "."}, extra...)
	command := exec.Command("helm", args...)
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
			t.Fatalf("decode manifest: %v\n%s", err, manifest)
		}
		if len(object.Object) != 0 {
			objects = append(objects, object)
		}
	}
}

func objectKinds(objects []*unstructured.Unstructured) []string {
	out := make([]string, 0, len(objects))
	for _, object := range objects {
		out = append(out, object.GetKind()+"/"+object.GetName())
	}
	return out
}

func objectByKind(t *testing.T, objects []*unstructured.Unstructured, kind string) *unstructured.Unstructured {
	t.Helper()
	for _, object := range objects {
		if object.GetKind() == kind {
			return object
		}
	}
	t.Fatalf("missing object kind %s", kind)
	return nil
}

func decodeTyped(t *testing.T, object *unstructured.Unstructured, target any) {
	t.Helper()
	encoded, err := json.Marshal(object.Object)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, target); err != nil {
		t.Fatal(err)
	}
}

func requireHelm(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}
}
