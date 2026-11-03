package controller

import "testing"

func TestNodeLossIdentityPreservesInfiniteAndExplicitZeroTimeouts(t *testing.T) {
	wait := map[string]any{"key": "node.kubernetes.io/unreachable", "operator": "Exists", "value": "", "effect": "NoExecute"}
	actual := map[string]any{"key": "node.kubernetes.io/unreachable", "operator": "Exists", "effect": "NoExecute"}
	if !revisionAppliedFieldsMatch("spec.template.spec.tolerations", actual, wait) {
		t.Fatal("Kubernetes omitted an empty Exists value")
	}
	actual["tolerationSeconds"] = float64(300)
	if revisionAppliedFieldsMatch("spec.template.spec.tolerations", actual, wait) {
		t.Fatal("finite eviction timeout accepted for wait")
	}
	wait["tolerationSeconds"] = float64(0)
	delete(actual, "tolerationSeconds")
	if revisionAppliedFieldsMatch("spec.template.spec.tolerations", actual, wait) {
		t.Fatal("infinity accepted for explicitly immediate eviction")
	}
	actual["tolerationSeconds"] = float64(0)
	if !revisionAppliedFieldsMatch("spec.template.spec.tolerations", actual, wait) {
		t.Fatal("explicit zero timeout rejected")
	}
}
