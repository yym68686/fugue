package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strconv"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const dnsPrivateTransportManager = "fugue-dns-validation"

// Only an exact, signed DNS-only execution member can report local candidate
// configuration. Missing or foreign declarations never relax public selection.
func (s *Server) privateDNSReceiptMember(claims platformcontrol.PlatformComponentIdentityClaims, h platformcontrol.PlatformConsumerHeartbeatEnvelope, sets []model.PlatformExpectedConsumerSet) (bool, int) {
	if claims.Component != model.PlatformConsumerComponentDNSServer || claims.AuthorityID == "" || claims.ScopeKey != platformconfig.AuthorityCellScope(claims.AuthorityID) || len(sets) != 1 {
		return false, http.StatusOK
	}
	set := sets[0]
	parent, err := s.store.GetPlatformArtifact(set.ReleaseSetID)
	if err != nil || parent.Content["publication_role"] != platformconfig.PublicationRoleCellDNS {
		return false, http.StatusOK
	}
	if parent.ID != set.ReleaseSetID || parent.Status != model.PlatformArtifactStatusValidated || parent.ScopeKey != claims.ScopeKey || set.ScopeKey != claims.ScopeKey || set.ArtifactReleaseID == "" || set.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || h.ExpectedConsumerSetID != set.ID || h.ReleaseSetID != parent.ID || s.store.VerifyPlatformArtifactIntegrity(parent) != nil || platformcontrol.ValidateDeclaredTrafficConsumerSet(parent, set) != nil {
		return false, http.StatusConflict
	}
	binding := s.platformConvergenceBinding(set)
	if binding == nil || h.FencingToken != binding.FencingToken || h.GenerationSequence != binding.GenerationSequence || !platformcontrol.TrafficCanaryConsumerAllowed(set, claims.ConsumerID(), binding) {
		return false, http.StatusConflict
	}
	for _, member := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
		if member.Required && platformcontrol.ExpectedConsumerIdentityMatches(member, claims) {
			return true, http.StatusOK
		}
	}
	return false, http.StatusForbidden
}

func dnsServiceSelectsPod(svc corev1.Service, pod corev1.Pod) bool {
	if len(svc.Spec.Selector) == 0 {
		return false
	}
	for key, value := range svc.Spec.Selector {
		if pod.Labels[key] != value {
			return false
		}
	}
	return true
}

func publicDNSTransportForNode(services []corev1.Service, node corev1.Node, namespace string) (*corev1.Service, int) {
	var selected *corev1.Service
	for i := range services {
		svc := &services[i]
		public, local := false, false
		for _, port := range svc.Spec.Ports {
			public = public || port.Port == 53
		}
		for _, address := range node.Status.Addresses {
			local = local || slices.Contains(svc.Spec.ExternalIPs, address.Address)
		}
		if !public || !local {
			continue
		}
		if selected != nil || !validDNSPublicTransport(*svc, namespace) {
			return nil, http.StatusConflict
		}
		selected = svc
	}
	return selected, http.StatusOK
}

type dnsPrivatePublicGuard struct {
	namespace string
	authority string
	node      corev1.Node
	service   *corev1.Service
	pod       *corev1.Pod
	private   *corev1.Service
}

func selectPrivateDNSValidationService(ctx context.Context, client *clusterNodeClient, claims platformcontrol.PlatformComponentIdentityClaims, pod corev1.Pod, node corev1.Node, pods []corev1.Pod, public *corev1.Service) (*corev1.Service, *dnsPrivatePublicGuard, int) {
	fail := func(code int) (*corev1.Service, *dnsPrivatePublicGuard, int) { return nil, nil, code }
	if node.ResourceVersion == "" || pod.Annotations["fugue.pro/authority-transition-role"] != "isolated-candidate" || pod.Spec.HostNetwork || pod.Spec.HostPID || pod.Spec.HostIPC {
		return fail(http.StatusConflict)
	}
	for _, container := range append(append([]corev1.Container{}, pod.Spec.Containers...), pod.Spec.InitContainers...) {
		for _, port := range container.Ports {
			if port.HostPort != 0 {
				return fail(http.StatusConflict)
			}
		}
	}
	guard := &dnsPrivatePublicGuard{namespace: pod.Namespace, authority: claims.AuthorityID, node: node, service: public}
	if public != nil {
		for i := range pods {
			p := &pods[i]
			if p.Spec.NodeName != claims.NodeID || !dnsServiceSelectsPod(*public, *p) {
				continue
			}
			policy, valid := decodePlatformConsumerIdentityPolicy(p.Annotations[platformConsumerIdentityAnnotation])
			if guard.pod != nil || !valid || policy.Component != model.PlatformConsumerComponentDNSServer || p.UID == "" || p.ResourceVersion == "" || p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodRunning || policy.AuthorityID == claims.AuthorityID || policy.ScopeKey == claims.ScopeKey {
				return fail(http.StatusConflict)
			}
			guard.pod = p
		}
		if guard.pod == nil {
			return fail(http.StatusConflict)
		}
	}
	query := url.Values{"labelSelector": {"app.kubernetes.io/managed-by=" + dnsPrivateTransportManager}, "limit": {"256"}}
	var services corev1.ServiceList
	if client.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+url.PathEscape(pod.Namespace)+"/services?"+query.Encode(), &services) != nil || services.Continue != "" {
		return fail(http.StatusServiceUnavailable)
	}
	selected := privateDNSTransportForMember(services.Items, pod.Namespace, claims.AuthorityID, claims.NodeID)
	if selected == nil || !dnsServiceSelectsPod(*selected, pod) {
		return fail(http.StatusConflict)
	}
	if _, ok := dnsBackendObservationPort(pod, *selected); !ok {
		return fail(http.StatusConflict)
	}
	guard.private = selected
	return selected, guard, http.StatusOK
}

func privateDNSTransportForMember(services []corev1.Service, namespace, authority, node string) *corev1.Service {
	var selected *corev1.Service
	for i := range services {
		svc := &services[i]
		if svc.Annotations["transport.fugue.dev/authority-id"] != authority || svc.Annotations["transport.fugue.dev/node-id"] != node {
			continue
		}
		if selected != nil || !validDNSPrivateTransport(*svc, namespace, authority, node) {
			return nil
		}
		selected = svc
	}
	return selected
}

func validDNSPrivateTransport(svc corev1.Service, namespace, authority, node string) bool {
	generation, err := strconv.ParseInt(svc.Annotations["transport.fugue.dev/generation"], 10, 64)
	if err != nil || generation <= 0 || svc.Name == "" || svc.UID == "" || svc.ResourceVersion == "" || svc.Namespace != namespace || svc.DeletionTimestamp != nil || svc.Labels["app.kubernetes.io/managed-by"] != dnsPrivateTransportManager || svc.Annotations["transport.fugue.dev/authority-id"] != authority || svc.Annotations["transport.fugue.dev/node-id"] != node || svc.Spec.Type != corev1.ServiceTypeClusterIP || len(svc.Spec.ExternalIPs) != 0 || svc.Spec.ExternalName != "" || svc.Spec.ExternalTrafficPolicy != "" || svc.Spec.LoadBalancerIP != "" || svc.Spec.HealthCheckNodePort != 0 || svc.Spec.LoadBalancerClass != nil || len(svc.Spec.LoadBalancerSourceRanges) != 0 || svc.Spec.InternalTrafficPolicy == nil || *svc.Spec.InternalTrafficPolicy != corev1.ServiceInternalTrafficPolicyLocal || svc.Spec.PublishNotReadyAddresses || len(svc.Spec.Selector) == 0 || len(svc.Spec.Ports) != 2 {
		return false
	}
	ports := make([]map[string]any, 0, 2)
	protocols := map[corev1.Protocol]bool{}
	for _, port := range svc.Spec.Ports {
		if port.Port < 1 || port.Port > 65535 || port.NodePort != 0 || port.TargetPort.Type != intstr.Int || port.TargetPort.IntVal < 1 || port.TargetPort.IntVal > 65535 || port.Name == "" || (port.Protocol != corev1.ProtocolTCP && port.Protocol != corev1.ProtocolUDP) || protocols[port.Protocol] {
			return false
		}
		protocols[port.Protocol] = true
		ports = append(ports, map[string]any{"name": port.Name, "protocol": port.Protocol, "port": port.Port, "targetPort": port.TargetPort.IntVal})
	}
	spec := map[string]any{"type": svc.Spec.Type, "internalTrafficPolicy": *svc.Spec.InternalTrafficPolicy, "publishNotReadyAddresses": false, "selector": svc.Spec.Selector, "ports": ports}
	raw, err := json.Marshal(map[string]any{"authority_id": authority, "node_id": node, "spec": spec})
	digest := sha256.Sum256(raw)
	return err == nil && svc.Annotations["transport.fugue.dev/digest"] == "sha256:"+hex.EncodeToString(digest[:])
}

func (g *dnsPrivatePublicGuard) recheck(ctx context.Context, client *clusterNodeClient) int {
	base := "/api/v1/namespaces/" + url.PathEscape(g.namespace)
	privateQuery := url.Values{"labelSelector": {"app.kubernetes.io/managed-by=" + dnsPrivateTransportManager}, "limit": {"256"}}
	var privateServices corev1.ServiceList
	if client.doJSON(ctx, http.MethodGet, base+"/services?"+privateQuery.Encode(), &privateServices) != nil || privateServices.Continue != "" {
		return http.StatusServiceUnavailable
	}
	private := privateDNSTransportForMember(privateServices.Items, g.namespace, g.authority, g.node.Name)
	if private == nil || private.UID != g.private.UID || private.ResourceVersion != g.private.ResourceVersion {
		return http.StatusConflict
	}
	var currentNode corev1.Node
	if client.doJSON(ctx, http.MethodGet, "/api/v1/nodes/"+url.PathEscape(g.node.Name), &currentNode) != nil {
		return http.StatusServiceUnavailable
	}
	if currentNode.UID != g.node.UID || currentNode.ResourceVersion != g.node.ResourceVersion {
		return http.StatusConflict
	}
	query := url.Values{"labelSelector": {"app.kubernetes.io/managed-by=" + dnsTransportManager}, "limit": {"256"}}
	var services corev1.ServiceList
	if client.doJSON(ctx, http.MethodGet, base+"/services?"+query.Encode(), &services) != nil || services.Continue != "" {
		return http.StatusServiceUnavailable
	}
	current, status := publicDNSTransportForNode(services.Items, currentNode, g.namespace)
	if status != http.StatusOK {
		return status
	}
	if g.service == nil {
		if current != nil {
			return http.StatusConflict
		}
		return http.StatusOK
	}
	if current == nil || current.UID != g.service.UID || current.ResourceVersion != g.service.ResourceVersion {
		return http.StatusConflict
	}
	var pod corev1.Pod
	if client.doJSON(ctx, http.MethodGet, base+"/pods/"+url.PathEscape(g.pod.Name), &pod) != nil {
		return http.StatusServiceUnavailable
	}
	if pod.UID != g.pod.UID || pod.ResourceVersion != g.pod.ResourceVersion {
		return http.StatusConflict
	}
	return http.StatusOK
}
