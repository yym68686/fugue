package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"reflect"
	"strconv"
	"strings"

	"fugue/internal/edgegroupfront"
	"fugue/internal/model/edgecohort"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
)

const nodeWorkerListLimit = 256

type nodeWorkerFrontHealth struct {
	observedFront
	Target string `json:"https_target"`
}

type nodeWorkerProcess struct {
	pod             corev1.Pod
	edgeID, caddyID string
	target          string
}

type nodeWorkerRuntime struct {
	node, nodeRV string
	service      corev1.Service
	slices       []discoveryv1.EndpointSlice
	probe        corev1.Service
	probeSlices  []discoveryv1.EndpointSlice
	front        corev1.Pod
	prior, next  nodeWorkerProcess
}

func (a *frontAuthorityActivator) observeNodeWorkerRuntime(ctx context.Context, j edgegroupfront.NodeWorkerJournal, requireCandidate, requirePublicReady bool) (nodeWorkerRuntime, error) {
	var found nodeWorkerRuntime
	ns, nodeName, group := a.config.Namespace, j.Recipient.Node, a.config.GroupID
	if j.Recipient.Group != group {
		return found, errors.New("node Worker journal is for a different release group")
	}
	n, err := a.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil || string(n.UID) != j.NodeUID || !repairNodeReady(*n) ||
		n.Labels["fugue.io/edge-group-id"] != group || n.Labels["fugue.io/role.edge"] != "true" ||
		n.Labels["fugue.io/schedulable"] == "false" || n.Labels[edgecohort.PhaseLabel] == edgecohort.Preparing {
		return found, errors.Join(errors.New("node Worker does not have its exact ready Node"), err)
	}
	if held, err := edgecohort.ReleaseHeld(*n, group); err != nil || held {
		return found, errors.Join(errors.New("node Worker is excluded from releases by Node policy"), err)
	}
	found.node, found.nodeRV = n.Name, n.ResourceVersion
	if found.nodeRV == "" {
		return found, errors.New("node Worker Node lacks its observation revision")
	}
	// Check all Services for an external address conflict, not just the
	// managed Service. A second writer can claim ports without the label.
	services, err := a.client.CoreV1().Services(ns).List(ctx, metav1.ListOptions{Limit: nodeWorkerListLimit})
	if err != nil || services.Continue != "" || len(services.Items) > nodeWorkerListLimit {
		return found, errors.Join(errors.New("node Worker Service inventory is incomplete"), err)
	}
	service, publicIP, err := selectNodeWorkerService(services.Items, j, ns)
	if err != nil {
		return found, err
	}
	found.service = service

	pods := a.client.CoreV1().Pods(ns)
	selected, err := pods.List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set(service.Spec.Selector)).String(), Limit: nodeWorkerListLimit,
	})
	if err != nil || selected.Continue != "" || len(selected.Items) != 1 {
		return found, errors.Join(errors.New("public Service selector must match one exact Front across the namespace"), err)
	}
	front := selected.Items[0]
	if string(front.UID) != j.Recipient.PodUID || front.Spec.NodeName != nodeName || front.Labels["fugue.io/edge-group-id"] != group ||
		front.Labels["fugue.io/edge-node"] != nodeName || front.ResourceVersion == "" || !frontMountsNodeWorkerFence(front, group) {
		return found, errors.New("public Service selected a different Front or its journal projection is absent")
	}
	if j.ProbeServiceUID != "" && !frontMountsNodeWorkerDrain(front, group) {
		return found, errors.New("pinned Front does not have the exact read-only Worker drain projection")
	}
	if requirePublicReady {
		if _, err := repairPodProcess(front, ns, "edge-front"); err != nil {
			return found, err
		}
	} else if !nodeWorkerFrontRunning(front, ns) {
		return found, errors.New("pinned Front process is not reachable for recovery")
	}
	hostIP, err := netip.ParseAddr(front.Status.HostIP)
	if err != nil || hostIP.Zone() != "" || hostIP.IsUnspecified() || hostIP.IsLoopback() || hostIP.IsMulticast() {
		return found, errors.New("node Worker Front node address is not valid")
	}
	if hostIP.Unmap() != publicIP.Unmap() {
		return found, errors.New("node Worker plan did not identify this node's public listener")
	}
	slices, err := a.client.DiscoveryV1().EndpointSlices(ns).List(ctx, metav1.ListOptions{
		LabelSelector: discoveryv1.LabelServiceName + "=" + service.Name, Limit: nodeWorkerListLimit,
	})
	if err != nil || slices.Continue != "" || len(slices.Items) > nodeWorkerListLimit || !nodeWorkerEndpointsMatch(service, front, slices.Items, requirePublicReady) {
		return found, errors.Join(errors.New("public Service EndpointSlices do not exclusively select the pinned Front"), err)
	}
	found.front, found.slices = front, slices.Items
	probe, err := selectNodeWorkerProbe(services.Items, service, publicIP, j.ProbeServiceUID)
	if err != nil {
		return found, err
	}
	if probe.UID != "" {
		slices, err := a.client.DiscoveryV1().EndpointSlices(ns).List(ctx, metav1.ListOptions{
			LabelSelector: discoveryv1.LabelServiceName + "=" + probe.Name, Limit: nodeWorkerListLimit,
		})
		if err != nil || slices.Continue != "" || len(slices.Items) > nodeWorkerListLimit || !nodeWorkerEndpointsMatch(probe, front, slices.Items, requirePublicReady) {
			return found, errors.Join(errors.New("probe EndpointSlices do not exclusively select the pinned Front"), err)
		}
		found.probe, found.probeSlices = probe, slices.Items
	}

	local, err := pods.List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", nodeName).String(), Limit: nodeWorkerListLimit,
	})
	if err != nil || local.Continue != "" || len(local.Items) > nodeWorkerListLimit {
		return found, errors.Join(errors.New("node Worker local Pod inventory is incomplete"), err)
	}
	if j.ProbeServiceUID != "" && requireCandidate {
		if err := requireExclusiveNodeWorkerIngress(services.Items, local.Items, found); err != nil {
			return found, err
		}
	}
	prior := edgegroupfront.ActivationRepairRelease{
		Slot: j.Expected.ActiveSlot, BundleGeneration: j.Expected.BundleGeneration,
		WorkerSourceCommit: j.Expected.WorkerSourceCommit, WorkerImageDigest: j.Expected.WorkerImageDigest,
	}
	port := uint16(18443)
	if prior.Slot == "b" {
		port = 28443
	}
	if found.prior, err = observeNodeWorkerProcess(local.Items, front, j.PredecessorWorkerUID, prior, hostIP, port); err != nil {
		return found, fmt.Errorf("retained Worker: %w", err)
	}
	if edgegroupfront.NodeWorkerProcessDigest(found.prior.edgeID, found.prior.caddyID) != j.PredecessorProcess {
		return found, errors.New("retained Worker process changed after the journal was claimed")
	}
	found.next, err = observeNodeWorkerProcess(local.Items, front, j.CandidateWorkerUID, j.Candidate, hostIP, j.CandidateHTTPSPort())
	if err != nil && requireCandidate {
		return found, fmt.Errorf("candidate Worker: %w", err)
	}
	if requireCandidate && edgegroupfront.NodeWorkerProcessDigest(found.next.edgeID, found.next.caddyID) != j.CandidateProcess {
		return found, errors.New("candidate Worker process changed after the journal was claimed")
	}
	return found, nil
}

// A failed selected Worker can make the Front's /readyz and EndpointSlice
// readiness drop. Do not require that readiness as a prerequisite to moving
// away from the failed listener. The same Front must still be running and
// match the exact on-disk activation and native recipient.
func nodeWorkerFrontRunning(front corev1.Pod, namespace string) bool {
	ip, err := netip.ParseAddr(front.Status.PodIP)
	if err != nil || ip.Zone() != "" || ip.IsUnspecified() || ip.IsMulticast() || ip.IsLoopback() ||
		front.Namespace != namespace || front.DeletionTimestamp != nil || front.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range front.Status.ContainerStatuses {
		if c.Name == "edge-front" && c.State.Running != nil && frontRepairIdentity(c.ContainerID) {
			return true
		}
	}
	return false
}

func selectNodeWorkerService(services []corev1.Service, j edgegroupfront.NodeWorkerJournal, ns string) (corev1.Service, netip.Addr, error) {
	var service corev1.Service
	var ip netip.Addr
	for _, s := range services {
		if string(s.UID) != j.ServiceUID {
			continue
		}
		if service.UID != "" || s.Namespace != ns || s.Name == "" || s.ResourceVersion == "" || s.DeletionTimestamp != nil ||
			s.Labels["app.kubernetes.io/managed-by"] != "fugue-front-serving-transport" ||
			s.Spec.Type != corev1.ServiceTypeClusterIP || s.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyLocal ||
			len(s.Spec.ExternalIPs) != 1 || len(s.Spec.Selector) == 0 || s.Spec.PublishNotReadyAddresses ||
			len(s.Spec.Ports) != 2 {
			return service, ip, errors.New("node Worker public Service has unexpected identity, listener or policy")
		}
		if _, err := labels.ValidatedSelectorFromSet(labels.Set(s.Spec.Selector)); err != nil {
			return service, ip, err
		}
		var err error
		ip, err = netip.ParseAddr(s.Spec.ExternalIPs[0])
		if err != nil || ip.Zone() != "" || ip.IsUnspecified() || ip.IsLoopback() || ip.IsMulticast() {
			return service, ip, errors.New("invalid node Worker public address")
		}
		ports := map[string]int32{}
		for _, p := range s.Spec.Ports {
			if p.Protocol != corev1.ProtocolTCP || p.NodePort != 0 ||
				(p.Name != "http" || p.Port != 80 || p.TargetPort.IntVal != 80 || p.TargetPort.StrVal != "") &&
					(p.Name != "https" || p.Port != 443 || p.TargetPort.IntVal != 443 || p.TargetPort.StrVal != "") {
				return service, ip, errors.New("unexpected node Worker public Service port")
			}
			ports[p.Name] = p.Port
		}
		if len(ports) != 2 {
			return service, ip, errors.New("duplicate node Worker public Service port")
		}
		service = s
	}
	if service.UID == "" {
		return service, ip, errors.New("exact node Worker public Service is absent")
	}
	for _, other := range services {
		if other.UID == service.UID {
			continue
		}
		addresses := append([]string{}, other.Spec.ExternalIPs...)
		addresses = append(addresses, other.Spec.LoadBalancerIP)
		for _, ingress := range other.Status.LoadBalancer.Ingress {
			addresses = append(addresses, ingress.IP)
		}
		for _, address := range addresses {
			parsed, _ := netip.ParseAddr(address)
			if parsed.Unmap() != ip.Unmap() {
				continue
			}
			for _, p := range other.Spec.Ports {
				if (p.Port == 80 || p.Port == 443) && (p.Protocol == "" || p.Protocol == corev1.ProtocolTCP) {
					return service, ip, errors.New("another Service claims the node Worker public address")
				}
			}
		}
	}
	return service, ip, nil
}

func selectNodeWorkerProbe(services []corev1.Service, public corev1.Service, ip netip.Addr, uid string) (corev1.Service, error) {
	var probe corev1.Service
	for _, svc := range services {
		external := append([]string{}, svc.Spec.ExternalIPs...)
		external = append(external, svc.Spec.LoadBalancerIP)
		for _, ingress := range svc.Status.LoadBalancer.Ingress {
			external = append(external, ingress.IP)
		}
		claimsIP := false
		for _, addr := range external {
			parsed, err := netip.ParseAddr(addr)
			claimsIP = claimsIP || err == nil && parsed.Unmap() == ip.Unmap()
		}
		if svc.UID == public.UID {
			continue
		}
		isProbe := svc.Labels["app.kubernetes.io/managed-by"] == "fugue-front-probe-transport"
		if string(svc.UID) != uid {
			for _, p := range svc.Spec.Ports {
				if claimsIP && (p.Protocol == "" || p.Protocol == corev1.ProtocolTCP) &&
					(isProbe || p.TargetPort.IntVal == 443 || p.Port == 15443) {
					return probe, errors.New("unbound Service can send new connections to a Front HTTPS listener")
				}
			}
			continue
		}
		spec := svc.Spec
		generation, genErr := strconv.ParseUint(svc.Annotations["transport.fugue.dev/generation"], 10, 64)
		if uid == "" || probe.UID != "" || !claimsIP || !isProbe || svc.Namespace != public.Namespace ||
			svc.Name == "" || svc.ResourceVersion == "" || svc.DeletionTimestamp != nil || genErr != nil || generation == 0 ||
			svc.Annotations["transport.fugue.dev/follows-serving-uid"] != string(public.UID) ||
			spec.Type != corev1.ServiceTypeClusterIP || spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyLocal ||
			spec.InternalTrafficPolicy == nil || *spec.InternalTrafficPolicy != corev1.ServiceInternalTrafficPolicyLocal ||
			spec.PublishNotReadyAddresses || len(spec.ExternalIPs) != 1 || spec.ExternalIPs[0] != ip.String() ||
			!reflect.DeepEqual(spec.Selector, public.Spec.Selector) || len(spec.Ports) != 1 {
			return probe, errors.New("pinned node Worker probe does not follow the exact serving Front")
		}
		p := spec.Ports[0]
		if p.Name != "https-probe" || p.Port != 15443 || p.Protocol != corev1.ProtocolTCP ||
			p.NodePort != 0 || p.AppProtocol != nil || p.TargetPort.IntVal != 443 || p.TargetPort.StrVal != "" {
			return probe, errors.New("node Worker probe listener has unexpected ports")
		}
		wire, err := json.Marshal(map[string]any{
			"type": spec.Type, "externalIPs": spec.ExternalIPs, "externalTrafficPolicy": spec.ExternalTrafficPolicy,
			"internalTrafficPolicy": spec.InternalTrafficPolicy, "publishNotReadyAddresses": spec.PublishNotReadyAddresses,
			"selector": spec.Selector,
			"ports":    []map[string]any{{"name": p.Name, "protocol": p.Protocol, "port": p.Port, "targetPort": p.TargetPort.IntVal}},
		})
		if err != nil || fmt.Sprintf("sha256:%x", sha256.Sum256(wire)) != svc.Annotations["transport.fugue.dev/digest"] {
			return probe, errors.New("node Worker probe differs from its transport declaration")
		}
		probe = svc
	}
	if uid != "" && probe.UID == "" {
		return probe, errors.New("pinned node Worker probe Service is missing")
	}
	return probe, nil
}

// An unselected Front may keep its established sockets. But a second
// reachable ingress bound to the same activation cannot silently be left
// outside this grant's proof and Front UID scope.
func requireExclusiveNodeWorkerIngress(services []corev1.Service, pods []corev1.Pod, r nodeWorkerRuntime) error {
	mount := repairHostActivationMount(r.front, "edge-front", true)
	if mount == "" {
		return errors.New("expected Front activation mount is absent")
	}
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded ||
			repairHostActivationMount(pod, "edge-front", false) != mount {
			continue
		}
		portNames := map[string]bool{}
		for _, c := range pod.Spec.Containers {
			for _, p := range c.Ports {
				if p.Protocol != "" && p.Protocol != corev1.ProtocolTCP {
					continue
				}
				if p.HostPort == 80 || p.HostPort == 443 || pod.Spec.HostNetwork && (p.ContainerPort == 80 || p.ContainerPort == 443) {
					return fmt.Errorf("Front %s has a separate host ingress outside the Worker drain witness", pod.Name)
				}
				if p.ContainerPort == 80 || p.ContainerPort == 443 {
					portNames[p.Name] = true
				}
			}
		}
		for _, svc := range services {
			if svc.UID == r.service.UID || svc.UID == r.probe.UID || len(svc.Spec.Selector) == 0 ||
				!labels.SelectorFromSet(labels.Set(svc.Spec.Selector)).Matches(labels.Set(pod.Labels)) {
				continue
			}
			for _, p := range svc.Spec.Ports {
				if (p.Protocol == "" || p.Protocol == corev1.ProtocolTCP) &&
					(p.TargetPort.IntVal == 80 || p.TargetPort.IntVal == 443 || p.TargetPort.StrVal != "" && portNames[p.TargetPort.StrVal] ||
						p.TargetPort.IntVal == 0 && p.TargetPort.StrVal == "" && (p.Port == 80 || p.Port == 443)) {
					return fmt.Errorf("Service %s has a Front ingress outside the Worker drain witness", svc.Name)
				}
			}
		}
	}
	return nil
}

func frontMountsNodeWorkerFence(front corev1.Pod, group string) bool {
	if front.Spec.HostNetwork || front.Spec.ShareProcessNamespace != nil && *front.Spec.ShareProcessNamespace ||
		repairHostActivationMount(front, "edge-front", true) != "/var/lib/fugue/edge-owned/"+group+"/front" {
		return false
	}
	for _, c := range front.Spec.Containers {
		if c.Name != "edge-front" {
			continue
		}
		for _, m := range c.VolumeMounts {
			if m.MountPath != "/var/run/fugue-worker-transaction" || !m.ReadOnly || m.SubPath != "" || m.SubPathExpr != "" {
				continue
			}
			for _, v := range front.Spec.Volumes {
				if v.Name != m.Name || v.ConfigMap == nil || v.ConfigMap.Name != releaseguardian.NodeWorkerProjectionName(group, front.Spec.NodeName) ||
					v.ConfigMap.Optional == nil || !*v.ConfigMap.Optional || len(v.ConfigMap.Items) != 1 {
					continue
				}
				item := v.ConfigMap.Items[0]
				return item.Key == "journal.json" && item.Path == "journal.json"
			}
		}
	}
	return false
}

func frontMountsNodeWorkerDrain(front corev1.Pod, group string) bool {
	const path = "/var/run/fugue-worker-drain"
	for _, c := range front.Spec.Containers {
		if c.Name != "edge-front" {
			continue
		}
		var uid, file bool
		for _, e := range c.Env {
			switch e.Name {
			case "FUGUE_EDGE_FRONT_WORKER_DRAIN_FRONT_UID":
				uid = e.Value == "" && e.ValueFrom != nil && e.ValueFrom.FieldRef != nil && e.ValueFrom.FieldRef.FieldPath == "metadata.uid"
			case "FUGUE_EDGE_FRONT_WORKER_DRAIN_GRANT_PATH":
				file = e.ValueFrom == nil && e.Value == path+"/grant.json"
			}
		}
		for _, m := range c.VolumeMounts {
			if m.MountPath != path || !m.ReadOnly || m.SubPath != "" || m.SubPathExpr != "" || m.MountPropagation != nil {
				continue
			}
			for _, v := range front.Spec.Volumes {
				cm := v.ConfigMap
				if v.Name == m.Name && cm != nil && cm.Name == releaseguardian.NodeWorkerDrainName(group, front.Spec.NodeName) &&
					cm.Optional != nil && *cm.Optional && len(cm.Items) == 1 && cm.Items[0].Key == "grant.json" && cm.Items[0].Path == "grant.json" {
					return uid && file
				}
			}
		}
	}
	return false
}

func nodeWorkerEndpointsMatch(s corev1.Service, front corev1.Pod, slices []discoveryv1.EndpointSlice, requireReady bool) bool {
	podIP, err := netip.ParseAddr(front.Status.PodIP)
	if err != nil || podIP.Zone() != "" {
		return false
	}
	total := 0
	for _, slice := range slices {
		if slice.Namespace != s.Namespace || slice.DeletionTimestamp != nil || slice.ResourceVersion == "" ||
			slice.Labels[discoveryv1.LabelServiceName] != s.Name || len(slice.Ports) != len(s.Spec.Ports) || len(slice.Ports) == 0 {
			return false
		}
		owner := metav1.GetControllerOf(&slice)
		if owner == nil || owner.Kind != "Service" || owner.Name != s.Name || owner.UID != s.UID {
			return false
		}
		ports := map[string]int32{}
		for _, p := range slice.Ports {
			if p.Name == nil || p.Protocol == nil || *p.Protocol != corev1.ProtocolTCP || p.Port == nil {
				return false
			}
			ports[*p.Name] = *p.Port
		}
		for _, p := range s.Spec.Ports {
			if p.TargetPort.IntVal == 0 || ports[p.Name] != p.TargetPort.IntVal {
				return false
			}
		}
		for _, e := range slice.Endpoints {
			total++
			ref := e.TargetRef
			if total > 1 || e.NodeName == nil || *e.NodeName != front.Spec.NodeName || ref == nil || ref.Kind != "Pod" ||
				ref.Name != front.Name || ref.Namespace != front.Namespace || ref.UID != front.UID || len(e.Addresses) != 1 ||
				e.Addresses[0] != front.Status.PodIP || requireReady && (e.Conditions.Ready == nil || !*e.Conditions.Ready ||
				e.Conditions.Serving != nil && !*e.Conditions.Serving) ||
				e.Conditions.Terminating != nil && *e.Conditions.Terminating ||
				podIP.Is4() && slice.AddressType != discoveryv1.AddressTypeIPv4 || podIP.Is6() && slice.AddressType != discoveryv1.AddressTypeIPv6 {
				return false
			}
		}
	}
	return total == 1
}

func observeNodeWorkerProcess(pods []corev1.Pod, front corev1.Pod, wantedUID string, pin edgegroupfront.ActivationRepairRelease, host netip.Addr, port uint16) (nodeWorkerProcess, error) {
	var out nodeWorkerProcess
	var owner *corev1.Pod
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName != front.Spec.NodeName || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			continue
		}
		for _, c := range pod.Spec.Containers {
			for _, p := range c.Ports {
				if p.HostPort != int32(port) || p.Protocol != "" && p.Protocol != corev1.ProtocolTCP {
					continue
				}
				if p.HostIP != "" && p.HostIP != host.String() {
					bind, err := netip.ParseAddr(p.HostIP)
					if err != nil {
						return out, errors.New("invalid Worker host binding")
					}
					if !bind.IsUnspecified() || bind.Is4() != host.Is4() {
						continue
					}
				}
				if owner != nil || c.Name != "caddy" || p.Name != "https-slot" || p.ContainerPort != p.HostPort {
					return out, errors.New("host listener is ambiguous or not the Caddy HTTPS slot")
				}
				owner = pod
			}
		}
	}
	if owner == nil || string(owner.UID) != wantedUID || owner.ResourceVersion == "" ||
		owner.Status.HostIP != front.Status.HostIP || owner.Labels["fugue.io/edge-slot"] != pin.Slot ||
		owner.Labels["fugue.io/edge-group-id"] != front.Labels["fugue.io/edge-group-id"] ||
		owner.Annotations["fugue.pro/source-commit"] != pin.WorkerSourceCommit ||
		repairHostActivationMount(*owner, "edge", false) != repairHostActivationMount(front, "edge-front", true) {
		return out, errors.New("host listener does not belong to the exact Worker release and activation mount")
	}
	var declared string
	for _, c := range owner.Spec.Containers {
		if c.Name == "edge" && strings.HasSuffix(c.Image, "@"+pin.WorkerImageDigest) {
			declared = c.Image
		}
	}
	if declared == "" || containerRuntimeDigest(*owner, "edge") != pin.WorkerImageDigest {
		return out, errors.New("declared and actual Worker image digests must match the release")
	}
	e, err := repairPodProcess(*owner, front.Namespace, "edge")
	c, caddyErr := repairPodProcess(*owner, front.Namespace, "caddy")
	if err != nil || caddyErr != nil {
		return out, errors.Join(err, caddyErr)
	}
	return nodeWorkerProcess{pod: *owner, edgeID: e, caddyID: c, target: netip.AddrPortFrom(host, port).String()}, nil
}

func (r nodeWorkerRuntime) sameBinding(other nodeWorkerRuntime, needCandidate bool) bool {
	return r.node == other.node && r.nodeRV == other.nodeRV && reflect.DeepEqual(r.service, other.service) && reflect.DeepEqual(r.slices, other.slices) &&
		reflect.DeepEqual(r.probe, other.probe) && reflect.DeepEqual(r.probeSlices, other.probeSlices) &&
		r.front.UID == other.front.UID && r.front.ResourceVersion == other.front.ResourceVersion &&
		r.prior.pod.UID == other.prior.pod.UID && r.prior.pod.ResourceVersion == other.prior.pod.ResourceVersion && r.prior.edgeID == other.prior.edgeID && r.prior.caddyID == other.prior.caddyID &&
		(!needCandidate || r.next.pod.UID == other.next.pod.UID && r.next.pod.ResourceVersion == other.next.pod.ResourceVersion && r.next.edgeID == other.next.edgeID && r.next.caddyID == other.next.caddyID)
}
