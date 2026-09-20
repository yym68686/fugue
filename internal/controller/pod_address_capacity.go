package controller

import "net/netip"

// Flannel's host-local allocator cannot use the subnet, broadcast or gateway
// address. Removing max-pods is not permission to schedule beyond this physical
// resource. Unknown/non-IPv4 addressing remains the CNI's responsibility.
func availablePodAddresses(node kubeNode, pods []kubePod) (int64, bool) {
	prefix, err := netip.ParsePrefix(node.Spec.PodCIDR)
	if err != nil || !prefix.Addr().Is4() || prefix.Bits() > 29 {
		return 0, false
	}
	capacity := (int64(1) << (32 - prefix.Bits())) - 3
	for _, pod := range pods {
		if pod.Spec.NodeName == node.Metadata.Name && !pod.Spec.HostNetwork && !managedPostgresPodFinished(pod) {
			capacity--
		}
	}
	return capacity, true
}
