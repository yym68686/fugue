package api

import (
	"os/exec"
	"testing"
)

func TestNodeDNSProbeWireObservations(t *testing.T) {
	// Python is a required runtime dependency of the shipped node updater.
	if out, err := exec.Command("python3", "-B", "node_dns_probe_test.py").CombinedOutput(); err != nil {
		t.Fatalf("DNS probe: %v\n%s", err, out)
	}
}

func TestNodeResourcePolicyExecution(t *testing.T) {
	if out, err := exec.Command("python3", "-B", "node_resource_policy_test.py").CombinedOutput(); err != nil {
		t.Fatalf("node resource policy: %v\n%s", err, out)
	}
}

func TestNodeServiceProbeTargetSelection(t *testing.T) {
	if out, err := exec.Command("python3", "-B", "node_service_probe_test.py").CombinedOutput(); err != nil {
		t.Fatalf("Service probe: %v\n%s", err, out)
	}
}
