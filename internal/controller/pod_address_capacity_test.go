package controller

import "testing"

func TestPodAddressCapacityIsIndependentOfArtificialPodCountLimit(t *testing.T) {
	var node kubeNode
	node.Metadata.Name = "worker-a"
	node.Spec.PodCIDR = "10.20.1.0/24"
	var pods []kubePod
	for i := 0; i < 253; i++ {
		var p kubePod
		p.Spec.NodeName = node.Metadata.Name
		p.Status.Phase = "Running"
		pods = append(pods, p)
	}
	if n, known := availablePodAddresses(node, pods[:110]); !known || n != 143 {
		t.Fatalf("110 should not exhaust the network: %d %t", n, known)
	}
	if n, _ := availablePodAddresses(node, pods); n != 0 {
		t.Fatalf("actual network exhaustion not detected: %d", n)
	}
	pods[0].Spec.HostNetwork = true
	pods[1].Status.Phase = "Succeeded"
	pods[2].Spec.NodeName = "another-worker"
	if n, _ := availablePodAddresses(node, pods); n != 3 {
		t.Fatalf("host-network/finished/foreign pods consume no addresses: %d", n)
	}
	if rolloutCapacityCandidateFits(rolloutCapacityCandidate{remainingMemoryBytes: 1 << 30, podAddressesKnown: true}, managedSharedNodeRequests{memoryBytes: 64 << 20}) {
		t.Fatal("accepted surge with no Pod IP address")
	}
}
