package sshdeployment

import (
	"encoding/json"
	"fugue/internal/managedssh"
	"strings"
	"testing"
)

func TestManagedSSHDeploymentLimitsRuntimeAuthority(test *testing.T) {
	plan := DeploymentPlan{Name: "ssh-candidate", Namespace: "platform", Image: "registry.example/managed-ssh@sha256:" + strings.Repeat("a", 64), HelperDigest: "sha256:" + strings.Repeat("b", 64), ArtifactSecret: "ssh-config", TrustSecret: "ssh-trust", HostKeySecret: "ssh-identity", StateClaim: "ssh-state", APIServerCIDR: "192.0.2.10/32", APIServerPort: 6443, IngressNamespace: "platform", IngressPodLabels: map[string]string{"component": "ssh-front"}, Target: managedssh.Target{TenantID: "tenant", AppID: "app", Namespace: "workloads", PodName: "workspace", PodUID: "pod-uid", ContainerID: "container-id"}}
	_, role, binding, err := RuntimePermissions(plan)
	if err != nil {
		test.Fatal(err)
	}
	if role.Namespace != plan.Target.Namespace || len(role.Rules) != 2 || binding.Subjects[0].Namespace != plan.Namespace {
		test.Fatal("permissions not bound to target namespace")
	}
	for _, rule := range role.Rules {
		if len(rule.ResourceNames) != 1 || rule.ResourceNames[0] != plan.Target.PodName {
			test.Fatal("runtime credentials can target another pod")
		}
		for _, resource := range rule.Resources {
			if resource != "pods" && resource != "pods/exec" {
				test.Fatal("unexpected runtime authority", resource)
			}
		}
	}
	objects, err := DeploymentObjects(plan)
	if err != nil {
		test.Fatal(err)
	}
	raw, err := json.Marshal(objects)
	if err != nil {
		test.Fatal(err)
	}
	for _, forbidden := range []string{"hostPath", "ClusterRole", "subPath", "private_key", "0.0.0.0/0", "fugue.pro/app-id", "fugue.pro/owner-app-id"} {
		if strings.Contains(string(raw), forbidden) {
			test.Fatal("unsafe deployment field", forbidden)
		}
	}
	first, err := json.Marshal(objects[0])
	if err != nil || !strings.Contains(string(first), "NetworkPolicy") {
		test.Fatal("SSH workload could start before its network policy", err)
	}
	plan.APIServerCIDR = "0.0.0.0/0"
	if _, err := DeploymentObjects(plan); err == nil {
		test.Fatal("unbounded API egress accepted")
	}
}
