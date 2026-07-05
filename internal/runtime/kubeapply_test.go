package runtime

import "testing"

func TestObjectAPIPathSupportsNetworkPolicy(t *testing.T) {
	path, err := ObjectAPIPath("tenant-demo", map[string]any{
		"apiVersion": KubernetesNetworkPolicyAPIVersion,
		"kind":       "NetworkPolicy",
		"metadata": map[string]any{
			"name": "app-demo-net",
		},
	})
	if err != nil {
		t.Fatalf("network policy object API path: %v", err)
	}

	want := "/apis/networking.k8s.io/v1/namespaces/tenant-demo/networkpolicies/app-demo-net"
	if path != want {
		t.Fatalf("unexpected network policy API path %q, want %q", path, want)
	}
}

func TestObjectAPIPathSupportsRoleAndRoleBinding(t *testing.T) {
	rolePath, err := ObjectAPIPath("tenant-demo", map[string]any{
		"apiVersion": KubernetesRBACAPIVersion,
		"kind":       "Role",
		"metadata": map[string]any{
			"name": "fugue-drain-agent-event-recorder",
		},
	})
	if err != nil {
		t.Fatalf("role object API path: %v", err)
	}
	if want := "/apis/rbac.authorization.k8s.io/v1/namespaces/tenant-demo/roles/fugue-drain-agent-event-recorder"; rolePath != want {
		t.Fatalf("unexpected role API path %q, want %q", rolePath, want)
	}

	roleBindingPath, err := ObjectAPIPath("tenant-demo", map[string]any{
		"apiVersion": KubernetesRBACAPIVersion,
		"kind":       "RoleBinding",
		"metadata": map[string]any{
			"name": "fugue-drain-agent-event-recorder",
		},
	})
	if err != nil {
		t.Fatalf("rolebinding object API path: %v", err)
	}
	if want := "/apis/rbac.authorization.k8s.io/v1/namespaces/tenant-demo/rolebindings/fugue-drain-agent-event-recorder"; roleBindingPath != want {
		t.Fatalf("unexpected rolebinding API path %q, want %q", roleBindingPath, want)
	}
}
