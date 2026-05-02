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
