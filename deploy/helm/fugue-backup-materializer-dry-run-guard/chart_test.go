package backupmaterializerdryrunguardchart_test

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

	"fugue/internal/backupmaterializer/dryrunguard"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const testCellKey = "backup/app-database/0123456789abcdef"

func TestDryRunGuardChartDefaultsToNoResourcesOnUnsupportedClusters(t *testing.T) {
	requireHelm(t)
	for name, args := range map[string][]string{
		"default capabilities": nil,
		"old Kubernetes":       {"--kube-version", "1.20.15"},
		"explicit false":       {"--kube-version", "1.20.15", "--set", "enabled=false"},
	} {
		t.Run(name, func(t *testing.T) {
			output, err := renderChartWithReleaseAndNamespace(t, "guard", "default", args...)
			if err != nil {
				t.Fatalf("render disabled chart: %v\n%s", err, output)
			}
			if strings.TrimSpace(string(output)) != "" {
				t.Fatalf("disabled guard chart rendered resources:\n%s", output)
			}
		})
	}
}

func TestEnabledDryRunGuardChartExactlyRendersContractPolicyAndBinding(t *testing.T) {
	requireHelm(t)
	guard, err := dryrunguard.Build(testCellKey)
	if err != nil {
		t.Fatalf("build guard contract: %v", err)
	}
	output, err := renderChart(t, validEnabledArgs()...)
	if err != nil {
		t.Fatalf("render enabled guard chart: %v\n%s", err, output)
	}
	objects := decodeObjects(t, output)
	if got, want := objectKinds(objects), []string{
		"ValidatingAdmissionPolicy/" + guard.AdmissionPolicyName,
		"ValidatingAdmissionPolicyBinding/" + guard.AdmissionBindingName,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("guard objects=%v, want %v\n%s", got, want, output)
	}

	wantLabels := map[string]string{
		"app.kubernetes.io/name":       "fugue-backup-materializer-dry-run-guard",
		"app.kubernetes.io/component":  "backup-materializer-dry-run-guard",
		"app.kubernetes.io/part-of":    "fugue",
		"app.kubernetes.io/managed-by": "Helm",
		"helm.sh/chart":                "fugue-backup-materializer-dry-run-guard-0.1.0",
		"fugue.io/release-lane":        "backup",
		"fugue.io/ownership-mode":      "shadow",
		"fugue.io/production-mutation": "forbidden",
		"fugue.io/backup-cell-id":      guard.CellID,
	}
	wantAnnotations := map[string]string{
		"fugue.io/backup-cell-key":            guard.CellKey,
		"fugue.io/backup-secret-name":         guard.SecretName,
		"fugue.io/gateway-service-account":    guard.ServiceAccountName,
		"fugue.io/guard-contract":             "backup-materializer-dry-run-guard@v1",
		"fugue.io/minimum-kubernetes-version": guard.MinimumKubernetesVersion,
		"fugue.io/production-mutation":        "forbidden",
	}
	for _, object := range objects {
		if object.GetNamespace() != "" || object.GetGenerateName() != "" || object.GetUID() != "" ||
			object.GetResourceVersion() != "" || len(object.GetOwnerReferences()) != 0 || len(object.GetFinalizers()) != 0 ||
			!reflect.DeepEqual(object.GetLabels(), wantLabels) || !reflect.DeepEqual(object.GetAnnotations(), wantAnnotations) {
			t.Fatalf("unsafe cluster-scoped metadata on %s: %#v", object.GetKind(), object.Object["metadata"])
		}
	}

	var policy admissionv1.ValidatingAdmissionPolicy
	decodeObject(t, objects[0], &policy)
	if policy.Name != guard.AdmissionPolicyName || policy.APIVersion != guard.AdmissionAPIVersion ||
		policy.Kind != guard.AdmissionPolicyKind || policy.Spec.FailurePolicy == nil ||
		string(*policy.Spec.FailurePolicy) != guard.FailurePolicy || policy.Spec.ParamKind != nil ||
		len(policy.Spec.Variables) != 0 || len(policy.Spec.AuditAnnotations) != 0 || !reflect.DeepEqual(policy.Status, admissionv1.ValidatingAdmissionPolicyStatus{}) {
		t.Fatalf("policy shell drifted from contract: %+v", policy)
	}
	assertMatchResources(t, "policy", policy.Spec.MatchConstraints, guard)
	if len(policy.Spec.MatchConditions) != len(guard.MatchConditions) {
		t.Fatalf("policy match conditions=%+v, contract=%+v", policy.Spec.MatchConditions, guard.MatchConditions)
	}
	for index, expected := range guard.MatchConditions {
		actual := policy.Spec.MatchConditions[index]
		if actual.Name != expected.Name || actual.Expression != expected.Expression {
			t.Fatalf("match condition %d=%+v, contract=%+v", index, actual, expected)
		}
	}
	if len(policy.Spec.Validations) != len(guard.Validations) {
		t.Fatalf("policy validations=%+v, contract=%+v", policy.Spec.Validations, guard.Validations)
	}
	for index, expected := range guard.Validations {
		actual := policy.Spec.Validations[index]
		if actual.Expression != expected.Expression || actual.Message != expected.Message || actual.Reason == nil ||
			string(*actual.Reason) != expected.Reason || actual.MessageExpression != "" {
			t.Fatalf("validation %d=%+v, contract=%+v", index, actual, expected)
		}
	}

	var binding admissionv1.ValidatingAdmissionPolicyBinding
	decodeObject(t, objects[1], &binding)
	if binding.Name != guard.AdmissionBindingName || binding.APIVersion != guard.AdmissionAPIVersion ||
		binding.Kind != guard.AdmissionBindingKind || binding.Spec.PolicyName != guard.AdmissionPolicyName ||
		binding.Spec.ParamRef != nil || !reflect.DeepEqual(binding.Spec.ValidationActions, []admissionv1.ValidationAction{admissionv1.Deny}) {
		t.Fatalf("binding shell drifted from contract: %+v", binding)
	}
	assertMatchResources(t, "binding", binding.Spec.MatchResources, guard)
}

func TestDryRunGuardResourcesAreReleaseIndependentAndCellDistinct(t *testing.T) {
	requireHelm(t)
	first, err := renderChartWithReleaseAndNamespace(t, "guard", "fugue-system", validEnabledArgs()...)
	if err != nil {
		t.Fatalf("render first release: %v\n%s", err, first)
	}
	second, err := renderChartWithReleaseAndNamespace(t, "other-release", "fugue-system", validEnabledArgs()...)
	if err != nil {
		t.Fatalf("render second release: %v\n%s", err, second)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("cluster guard render changed with Helm release identity:\nfirst:\n%s\nsecond:\n%s", first, second)
	}

	names := make(map[string]bool)
	for _, kind := range []string{"control-plane-db", "app-database", "persistent-storage", "data-workspace", "registry", "platform-component"} {
		cellKey := "backup/" + kind + "/0123456789abcdef"
		guard, err := dryrunguard.Build(cellKey)
		if err != nil {
			t.Fatalf("build %s guard: %v", kind, err)
		}
		args := append(validCapabilityArgs(), "--set", "enabled=true", "--set-string", "cell.key="+cellKey)
		output, err := renderChart(t, args...)
		if err != nil {
			t.Fatalf("render %s cell: %v\n%s", kind, err, output)
		}
		objects := decodeObjects(t, output)
		if len(objects) != 2 || names[guard.ServiceAccountName] || len(guard.ServiceAccountName) > 63 {
			t.Fatalf("invalid or colliding %s guard identity: objects=%v guard=%+v", kind, objectKinds(objects), guard)
		}
		for _, object := range objects {
			if object.GetName() != guard.ServiceAccountName {
				t.Fatalf("%s object name=%q, contract=%q", kind, object.GetName(), guard.ServiceAccountName)
			}
		}
		names[guard.ServiceAccountName] = true
	}
}

func TestEnabledDryRunGuardChartRejectsUnsafeOrUnprovenCapabilities(t *testing.T) {
	requireHelm(t)
	tests := []struct {
		name      string
		namespace string
		args      []string
		want      string
	}{
		{name: "wrong namespace", namespace: "default", args: validEnabledArgs(), want: "only in namespace fugue-system"},
		{name: "string false", args: append(validCapabilityArgs(), "--set-string", "enabled=false"), want: "enabled must be exactly a boolean"},
		{name: "numeric zero", args: append(validCapabilityArgs(), "--set", "enabled=0"), want: "enabled must be exactly a boolean"},
		{name: "missing cell", args: append(validCapabilityArgs(), "--set", "enabled=true"), want: "cell.key is required"},
		{name: "unknown kind", args: append(validCapabilityArgs(), "--set", "enabled=true", "--set-string", "cell.key=backup/all/0123456789abcdef"), want: "canonical backup cell key"},
		{name: "uppercase cell", args: append(validCapabilityArgs(), "--set", "enabled=true", "--set-string", "cell.key=backup/app-database/0123456789ABCDEF"), want: "canonical backup cell key"},
		{name: "cell whitespace", args: append(validCapabilityArgs(), "--set", "enabled=true", "--set-string", "cell.key= backup/app-database/0123456789abcdef"), want: "canonical backup cell key"},
		{name: "old Kubernetes", args: append(validAPIArgs(), "--kube-version", "1.29.9", "--set", "enabled=true", "--set-string", "cell.key="+testCellKey), want: "Kubernetes 1.30 or newer"},
		{name: "missing policy discovery", args: []string{"--kube-version", "1.35.4", "--set", "enabled=true", "--set-string", "cell.key=" + testCellKey}, want: "ValidatingAdmissionPolicy discovery"},
		{name: "missing binding discovery", args: []string{"--kube-version", "1.35.4", "--api-versions", "admissionregistration.k8s.io/v1/ValidatingAdmissionPolicy", "--set", "enabled=true", "--set-string", "cell.key=" + testCellKey}, want: "ValidatingAdmissionPolicyBinding discovery"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			namespace := test.namespace
			if namespace == "" {
				namespace = "fugue-system"
			}
			output, err := renderChartWithReleaseAndNamespace(t, "guard", namespace, test.args...)
			if err == nil {
				t.Fatalf("unsafe guard values unexpectedly rendered:\n%s", output)
			}
			if !strings.Contains(string(output), test.want) {
				t.Fatalf("Helm error=%q, want substring %q", output, test.want)
			}
		})
	}
}

func assertMatchResources(t *testing.T, owner string, resources *admissionv1.MatchResources, guard dryrunguard.Guard) {
	t.Helper()
	if resources == nil || resources.MatchPolicy == nil || string(*resources.MatchPolicy) != guard.MatchPolicy ||
		resources.NamespaceSelector == nil || !reflect.DeepEqual(resources.NamespaceSelector.MatchLabels, map[string]string{guard.NamespaceSelectorKey: guard.NamespaceSelectorValue}) ||
		len(resources.NamespaceSelector.MatchExpressions) != 0 || resources.ObjectSelector != nil || len(resources.ExcludeResourceRules) != 0 ||
		len(resources.ResourceRules) != 1 {
		t.Fatalf("%s match resources drifted: %+v", owner, resources)
	}
	rule := resources.ResourceRules[0]
	if len(rule.ResourceNames) != 0 || !reflect.DeepEqual(rule.APIGroups, guard.ResourceRule.APIGroups) ||
		!reflect.DeepEqual(rule.APIVersions, guard.ResourceRule.APIVersions) ||
		!reflect.DeepEqual(rule.Resources, guard.ResourceRule.Resources) || rule.Scope == nil || string(*rule.Scope) != guard.ResourceRule.Scope {
		t.Fatalf("%s resource rule drifted: %+v", owner, rule)
	}
	operations := make([]string, len(rule.Operations))
	for index, operation := range rule.Operations {
		operations[index] = string(operation)
	}
	if !reflect.DeepEqual(operations, guard.ResourceRule.Operations) {
		t.Fatalf("%s operations=%v, contract=%v", owner, operations, guard.ResourceRule.Operations)
	}
}

func validEnabledArgs() []string {
	return append(validCapabilityArgs(), "--set", "enabled=true", "--set-string", "cell.key="+testCellKey)
}

func validCapabilityArgs() []string {
	return append([]string{"--kube-version", "1.35.4"}, validAPIArgs()...)
}

func validAPIArgs() []string {
	return []string{
		"--api-versions", "admissionregistration.k8s.io/v1/ValidatingAdmissionPolicy",
		"--api-versions", "admissionregistration.k8s.io/v1/ValidatingAdmissionPolicyBinding",
	}
}

func renderChart(t *testing.T, extraArgs ...string) ([]byte, error) {
	return renderChartWithReleaseAndNamespace(t, "guard", "fugue-system", extraArgs...)
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
	document, err := json.Marshal(object.Object)
	if err != nil {
		t.Fatalf("marshal rendered %s: %v", object.GetKind(), err)
	}
	if err := json.Unmarshal(document, target); err != nil {
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

func requireHelm(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
}
