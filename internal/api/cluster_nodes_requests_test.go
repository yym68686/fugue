package api

import "testing"

func TestClusterNodeSinglePodRequestsCountsRestartableInitSidecars(t *testing.T) {
	pod := clusterNodePod{}
	pod.Spec.Containers = []clusterNodeContainer{{Resources: clusterNodeResourceRequirements{Requests: map[string]string{"cpu": "100m", "memory": "128Mi"}}}}
	pod.Spec.InitContainers = []clusterNodeContainer{
		{RestartPolicy: "Always", Resources: clusterNodeResourceRequirements{Requests: map[string]string{"cpu": "5m", "memory": "16Mi"}}},
		{Resources: clusterNodeResourceRequirements{Requests: map[string]string{"cpu": "25m", "memory": "64Mi"}}},
	}

	got := clusterNodeSinglePodRequests(pod)
	if got.cpuMilliCores != 105 {
		t.Fatalf("cpu request = %d, want 105m", got.cpuMilliCores)
	}
	wantMemory := int64((128 + 16) * 1024 * 1024)
	if got.memoryBytes != wantMemory {
		t.Fatalf("memory request = %d, want %d", got.memoryBytes, wantMemory)
	}
}
