package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/netip"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const dnsTransportManager = "fugue-dns-transport"

// The physical process owner remains node-scoped; neutral consumer receipts
// additionally bind their explicit cell. Only the uniquely selected public
// backend may write its projection; a candidate can load/probe locally before
// selection without overwriting the serving instance's receipt. This is an
// observation of Kubernetes transport, never a source of DNS serving config.
func (s *Server) validateDNSHeartbeatBackend(ctx context.Context, claims platformcontrol.PlatformComponentIdentityClaims, h platformcontrol.PlatformConsumerHeartbeatEnvelope, sets ...model.PlatformExpectedConsumerSet) int {
	status, _ := s.validateDNSHeartbeatBackendSelection(ctx, claims, h, sets...)
	return status
}

func (s *Server) validateDNSHeartbeatBackendSelection(ctx context.Context, claims platformcontrol.PlatformComponentIdentityClaims, h platformcontrol.PlatformConsumerHeartbeatEnvelope, sets ...model.PlatformExpectedConsumerSet) (int, bool) {
	notSelected := false
	private, status := s.privateDNSReceiptMember(claims, h, sets)
	if status != http.StatusOK {
		return status, false
	}
	status = s.inspectDNSBackendTransport(ctx, claims, h, nil, private, &notSelected)
	return status, notSelected
}

// A reader runs only after transport identity validation and before the final
// metadata recheck. It must not retain the client or use Pod data as configuration.
func (s *Server) inspectDNSBackend(ctx context.Context, claims platformcontrol.PlatformComponentIdentityClaims, h platformcontrol.PlatformConsumerHeartbeatEnvelope, read func(context.Context, *clusterNodeClient, corev1.Pod, corev1.Service, bool) int) int {
	return s.inspectDNSBackendTransport(ctx, claims, h, read, false)
}

func (s *Server) inspectDNSBackendTransport(ctx context.Context, claims platformcontrol.PlatformComponentIdentityClaims, h platformcontrol.PlatformConsumerHeartbeatEnvelope, read func(context.Context, *clusterNodeClient, corev1.Pod, corev1.Service, bool) int, allowPrivate bool, notSelected ...*bool) int {
	if claims.Component != model.PlatformConsumerComponentDNSServer {
		return http.StatusOK
	}
	if !strings.HasPrefix(claims.CredentialID, "kubernetes:") {
		if claims.AuthorityID != "" {
			return http.StatusForbidden
		}
		return http.StatusOK
	}
	identity := strings.Split(claims.CredentialID, ":")
	if len(identity) != 4 || identity[1] == "" || identity[1] != s.controlPlaneNamespace || identity[2] == "" || identity[3] == "" || claims.NodeID == "" {
		return http.StatusForbidden
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client, err := s.newClusterNodeClient()
	if err != nil {
		return http.StatusServiceUnavailable
	}
	defer client.closeIdleConnections()
	base := "/api/v1/namespaces/" + url.PathEscape(identity[1])
	var pods corev1.PodList
	query := url.Values{"fieldSelector": {"spec.nodeName=" + claims.NodeID}, "limit": {"256"}}
	if !allowPrivate {
		// A public observation can only authorize the claimed service account.
		// Private candidate validation also inspects the public backend, which
		// may use another account, so that path retains the full node inventory.
		query.Set("fieldSelector", query.Get("fieldSelector")+",spec.serviceAccountName="+identity[2])
	}
	if client.doJSON(ctx, http.MethodGet, base+"/pods?"+query.Encode(), &pods) != nil || pods.Continue != "" {
		return http.StatusServiceUnavailable
	}
	var pod *corev1.Pod
	for i := range pods.Items {
		if string(pods.Items[i].UID) == identity[3] {
			if pod != nil {
				return http.StatusForbidden
			}
			pod = &pods.Items[i]
		}
	}
	if pod == nil || pod.Name == "" || pod.ResourceVersion == "" || pod.Namespace != identity[1] || pod.Spec.NodeName != claims.NodeID || pod.Spec.ServiceAccountName != identity[2] || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return http.StatusForbidden
	}
	policy, valid := decodePlatformConsumerIdentityPolicy(pod.Annotations[platformConsumerIdentityAnnotation])
	if !valid || policy.Component != claims.Component || policy.ScopeKey != claims.ScopeKey || policy.AuthorityID != claims.AuthorityID ||
		(claims.AuthorityID != "" && pod.Labels["fugue.io/edge-group-id"] != claims.AuthorityID) || len(policy.ArtifactKinds) != len(claims.ArtifactKinds) {
		return http.StatusForbidden
	}
	for _, kind := range claims.ArtifactKinds {
		if !slices.Contains(policy.ArtifactKinds, kind) {
			return http.StatusForbidden
		}
	}
	positive := strings.EqualFold(strings.TrimSpace(h.ApplyStatus), model.PlatformConsumerApplyStatusApplied) && strings.EqualFold(strings.TrimSpace(h.ProbeStatus), model.PlatformConsumerProbeStatusPassed)
	if positive && !dnsBackendPodReady(*pod) {
		return http.StatusConflict
	}
	var node corev1.Node
	if client.doJSON(ctx, http.MethodGet, "/api/v1/nodes/"+url.PathEscape(claims.NodeID), &node) != nil {
		return http.StatusServiceUnavailable
	}
	if node.Name != claims.NodeID || node.UID == "" || node.DeletionTimestamp != nil {
		return http.StatusConflict
	}
	var services corev1.ServiceList
	query = url.Values{"labelSelector": {"app.kubernetes.io/managed-by=" + dnsTransportManager}, "limit": {"256"}}
	if client.doJSON(ctx, http.MethodGet, base+"/services?"+query.Encode(), &services) != nil || services.Continue != "" {
		return http.StatusServiceUnavailable
	}
	selected, transportStatus := publicDNSTransportForNode(services.Items, node, identity[1])
	if transportStatus != http.StatusOK {
		return transportStatus
	}
	var privateGuard *dnsPrivatePublicGuard
	if selected == nil || !dnsServiceSelectsPod(*selected, *pod) {
		if !allowPrivate {
			// Distinguish a verified standby from malformed/missing transport.
			// This remains a rejection; never accept or persist a serving fact.
			if selected != nil && len(notSelected) == 1 && notSelected[0] != nil {
				var currentPod corev1.Pod
				var currentService corev1.Service
				if client.doJSON(ctx, http.MethodGet, base+"/pods/"+url.PathEscape(pod.Name), &currentPod) != nil || client.doJSON(ctx, http.MethodGet, base+"/services/"+url.PathEscape(selected.Name), &currentService) != nil {
					return http.StatusServiceUnavailable
				}
				if currentPod.UID == pod.UID && currentPod.ResourceVersion == pod.ResourceVersion && currentService.UID == selected.UID && currentService.ResourceVersion == selected.ResourceVersion {
					*notSelected[0] = true
				}
			}
			return http.StatusConflict
		}
		var status int
		selected, privateGuard, status = selectPrivateDNSValidationService(ctx, client, claims, *pod, node, pods.Items, selected)
		if status != http.StatusOK {
			return status
		}
	}
	slicePath := "/apis/discovery.k8s.io/v1/namespaces/" + url.PathEscape(identity[1]) + "/endpointslices?" + url.Values{"labelSelector": {discoveryv1.LabelServiceName + "=" + selected.Name}, "limit": {"256"}}.Encode()
	var endpoints discoveryv1.EndpointSliceList
	if client.doJSON(ctx, http.MethodGet, slicePath, &endpoints) != nil || endpoints.Continue != "" {
		return http.StatusServiceUnavailable
	}
	if !dnsBackendEndpointsMatch(*selected, *pod, endpoints.Items, positive) {
		return http.StatusConflict
	}
	if read != nil {
		ready := dnsBackendPodReady(*pod) && dnsBackendEndpointsMatch(*selected, *pod, endpoints.Items, true)
		if status := read(ctx, client, *pod, *selected, ready); status != http.StatusOK {
			return status
		}
	}
	// Fail on an observed replacement or readiness/selector change during the
	// lookup. Resource versions fence observations, not a lease on serving.
	var currentPod corev1.Pod
	var currentService corev1.Service
	var currentEndpoints discoveryv1.EndpointSliceList
	if client.doJSON(ctx, http.MethodGet, base+"/pods/"+url.PathEscape(pod.Name), &currentPod) != nil ||
		client.doJSON(ctx, http.MethodGet, base+"/services/"+url.PathEscape(selected.Name), &currentService) != nil ||
		client.doJSON(ctx, http.MethodGet, slicePath, &currentEndpoints) != nil || currentEndpoints.Continue != "" {
		return http.StatusServiceUnavailable
	}
	if currentPod.UID != pod.UID || currentPod.ResourceVersion != pod.ResourceVersion || currentService.UID != selected.UID || currentService.ResourceVersion != selected.ResourceVersion || !reflect.DeepEqual(endpoints.Items, currentEndpoints.Items) {
		return http.StatusConflict
	}
	if privateGuard != nil {
		return privateGuard.recheck(ctx, client)
	}
	return http.StatusOK
}

func dnsBackendPodReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// Match the fields owned by the independent transport reconciler. An unrelated
// live edit must not silently become new heartbeat authority.
func validDNSPublicTransport(svc corev1.Service, namespace string) bool {
	generation, err := strconv.ParseInt(svc.Annotations["transport.fugue.dev/generation"], 10, 64)
	if err != nil || generation <= 0 || svc.Name == "" || svc.UID == "" || svc.ResourceVersion == "" || svc.Namespace != namespace || svc.DeletionTimestamp != nil || svc.Labels["app.kubernetes.io/managed-by"] != dnsTransportManager || svc.Spec.Type != corev1.ServiceTypeClusterIP || svc.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyLocal || svc.Spec.InternalTrafficPolicy == nil || *svc.Spec.InternalTrafficPolicy != corev1.ServiceInternalTrafficPolicyLocal || svc.Spec.PublishNotReadyAddresses || len(svc.Spec.Selector) == 0 || len(svc.Spec.ExternalIPs) != 1 || len(svc.Spec.Ports) != 2 {
		return false
	}
	ports := make([]map[string]any, 0, 2)
	protocols := map[corev1.Protocol]bool{}
	for _, port := range svc.Spec.Ports {
		if port.Port != 53 || port.TargetPort.Type != intstr.Int || port.TargetPort.IntVal <= 0 || port.TargetPort.IntVal > 65535 || port.Name == "" || (port.Protocol != corev1.ProtocolTCP && port.Protocol != corev1.ProtocolUDP) || protocols[port.Protocol] {
			return false
		}
		protocols[port.Protocol] = true
		ports = append(ports, map[string]any{"name": port.Name, "protocol": port.Protocol, "port": port.Port, "targetPort": port.TargetPort.IntVal})
	}
	spec := map[string]any{"type": svc.Spec.Type, "externalIPs": svc.Spec.ExternalIPs, "externalTrafficPolicy": svc.Spec.ExternalTrafficPolicy, "internalTrafficPolicy": *svc.Spec.InternalTrafficPolicy, "publishNotReadyAddresses": false, "selector": svc.Spec.Selector, "ports": ports}
	raw, err := json.Marshal(spec)
	digest := sha256.Sum256(raw)
	return err == nil && svc.Annotations["transport.fugue.dev/digest"] == "sha256:"+hex.EncodeToString(digest[:])
}

func dnsBackendEndpointsMatch(svc corev1.Service, pod corev1.Pod, slices []discoveryv1.EndpointSlice, positive bool) bool {
	addresses := map[string]bool{}
	for _, ip := range pod.Status.PodIPs {
		addresses[ip.IP] = true
	}
	addresses[pod.Status.PodIP] = true
	seen := map[string]bool{}
	matched := false
	for _, slice := range slices {
		owner := metav1.GetControllerOf(&slice)
		if slice.UID == "" || slice.Namespace != svc.Namespace || slice.DeletionTimestamp != nil || slice.Labels[discoveryv1.LabelServiceName] != svc.Name || owner == nil || owner.APIVersion != "v1" || owner.Kind != "Service" || owner.Name != svc.Name || owner.UID != svc.UID || len(slice.Ports) != len(svc.Spec.Ports) {
			return false
		}
		for _, required := range svc.Spec.Ports {
			found := 0
			for _, port := range slice.Ports {
				if port.Name != nil && *port.Name == required.Name && port.Protocol != nil && *port.Protocol == required.Protocol && port.Port != nil && *port.Port == required.TargetPort.IntVal {
					found++
				}
			}
			if found != 1 {
				return false
			}
		}
		for _, endpoint := range slice.Endpoints {
			if endpoint.NodeName == nil {
				return false
			}
			if *endpoint.NodeName != pod.Spec.NodeName {
				continue // Local traffic never selects a backend on another node.
			}
			ref := endpoint.TargetRef
			if ref == nil || ref.Kind != "Pod" || (ref.APIVersion != "" && ref.APIVersion != "v1") || ref.Namespace != pod.Namespace || ref.Name != pod.Name || ref.UID != pod.UID || len(endpoint.Addresses) == 0 || endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating {
				return false
			}
			if positive && (endpoint.Conditions.Ready == nil || !*endpoint.Conditions.Ready || endpoint.Conditions.Serving != nil && !*endpoint.Conditions.Serving) {
				return false
			}
			for _, address := range endpoint.Addresses {
				ip, err := netip.ParseAddr(address)
				if err != nil || !addresses[address] || seen[address] || (slice.AddressType == discoveryv1.AddressTypeIPv4) != ip.Is4() || (slice.AddressType != discoveryv1.AddressTypeIPv4 && slice.AddressType != discoveryv1.AddressTypeIPv6) {
					return false
				}
				seen[address] = true
			}
			matched = true
		}
	}
	return matched
}
