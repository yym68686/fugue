package api

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"fugue/internal/frontnetwork"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func (s *Server) handleReadEdgePublicNetworkObservation(writer http.ResponseWriter, request *http.Request) {
	authority, ok := s.authorizeEdgeRequest(writer, request)
	if !ok {
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, 2048)
	var query frontnetwork.Request
	if httpx.DecodeJSON(request, &query) != nil || frontnetwork.ValidateRequest(query) != nil {
		httpx.WriteError(writer, http.StatusBadRequest, "invalid bounded Front observation request")
		return
	}
	if !authority.Scoped || query.EdgeID != authority.EdgeID || query.GroupID != authority.EdgeGroupID {
		httpx.WriteError(writer, http.StatusForbidden, "exact node-scoped Edge identity required")
		return
	}
	if !s.allowPublicNetworkObservation(query.EdgeID, time.Now()) {
		httpx.WriteError(writer, http.StatusTooManyRequests, "Front observation budget exhausted")
		return
	}
	node, _, err := s.store.GetEdgeNode(query.EdgeID)
	if err != nil || node.EdgeGroupID != query.GroupID {
		httpx.WriteError(writer, http.StatusServiceUnavailable, "Front node identity unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	response, err := s.readPublicFrontNetwork(ctx, node, query)
	if err != nil {
		httpx.WriteError(writer, http.StatusServiceUnavailable, "selected public Front connection evidence unavailable")
		return
	}
	writer.Header().Set("Cache-Control", "private, no-store")
	httpx.WriteJSON(writer, http.StatusOK, response)
}

func (s *Server) allowPublicNetworkObservation(edgeID string, now time.Time) bool {
	if !s.frontNetworkObservationMu.TryLock() {
		return false
	}
	defer s.frontNetworkObservationMu.Unlock()
	if s.frontNetworkObservationLast == nil {
		s.frontNetworkObservationLast = map[string]time.Time{}
	}
	if previous, found := s.frontNetworkObservationLast[edgeID]; found && now.Sub(previous) < time.Second {
		return false
	}
	for identity, previous := range s.frontNetworkObservationLast {
		if now.Sub(previous) > time.Minute {
			delete(s.frontNetworkObservationLast, identity)
		}
	}
	if len(s.frontNetworkObservationLast) >= 256 {
		return false
	}
	s.frontNetworkObservationLast[edgeID] = now
	return true
}

func publicFrontService(services []corev1.Service, node model.EdgeNode, namespace string) (*corev1.Service, error) {
	var selected *corev1.Service
	for index := range services {
		service := &services[index]
		public := false
		for _, address := range service.Spec.ExternalIPs {
			public = public || address != "" && (address == node.PublicIPv4 || address == node.PublicIPv6)
		}
		https := false
		for _, port := range service.Spec.Ports {
			https = https || port.Port == 443 && port.Protocol == corev1.ProtocolTCP
		}
		if !public || !https {
			continue
		}
		if selected != nil || service.Namespace != namespace || service.UID == "" || service.ResourceVersion == "" || service.DeletionTimestamp != nil || service.Spec.Type != corev1.ServiceTypeClusterIP || service.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyLocal || service.Spec.PublishNotReadyAddresses || len(service.Spec.Selector) == 0 || service.Labels["app.kubernetes.io/managed-by"] != "fugue-front-serving-transport" || service.Annotations["transport.fugue.dev/phase"] != "serving" {
			return nil, errors.New("ambiguous or invalid public Front transport")
		}
		generation, err := strconv.ParseUint(service.Annotations["transport.fugue.dev/generation"], 10, 64)
		if err != nil || generation == 0 {
			return nil, errors.New("public Front transport generation unavailable")
		}
		ports := []map[string]any{}
		for _, port := range service.Spec.Ports {
			if port.TargetPort.Type != intstr.Int || port.TargetPort.IntVal != port.Port || port.Protocol != corev1.ProtocolTCP || port.Name == "" || (port.Port != 80 && port.Port != 443) || port.NodePort != 0 || port.AppProtocol != nil {
				return nil, errors.New("public Front transport ports invalid")
			}
			ports = append(ports, map[string]any{"name": port.Name, "protocol": port.Protocol, "port": port.Port, "targetPort": port.TargetPort.IntVal})
		}
		projection := map[string]any{"type": service.Spec.Type, "selector": service.Spec.Selector, "ports": ports, "externalIPs": service.Spec.ExternalIPs, "externalTrafficPolicy": service.Spec.ExternalTrafficPolicy, "publishNotReadyAddresses": false}
		digest, err := platformconfig.Digest(projection)
		if err != nil || digest != service.Annotations["transport.fugue.dev/digest"] {
			return nil, errors.New("public Front transport declaration mismatch")
		}
		selected = service
	}
	if selected == nil {
		return nil, errors.New("public Front transport unavailable")
	}
	return selected, nil
}

func publicFrontPod(service corev1.Service, pods []corev1.Pod, node model.EdgeNode) (*corev1.Pod, int, error) {
	var selected *corev1.Pod
	port := 0
	for index := range pods {
		pod := &pods[index]
		if pod.Spec.NodeName != node.ID || !dnsServiceSelectsPod(service, *pod) {
			continue
		}
		if selected != nil || pod.DeletionTimestamp != nil || pod.UID == "" || pod.ResourceVersion == "" || pod.Namespace != service.Namespace || !dnsBackendPodReady(*pod) || pod.Labels["fugue.io/edge-group-id"] != node.EdgeGroupID {
			return nil, 0, errors.New("ambiguous or unready public Front pod")
		}
		for _, container := range pod.Spec.Containers {
			if container.Name != "edge-front" || len(container.Command) != 1 || container.Command[0] != "/usr/local/bin/fugue-edge-front" || !strings.Contains(container.Image, "@sha256:") {
				continue
			}
			for _, status := range pod.Status.ContainerStatuses {
				if status.Name == container.Name && status.Ready && status.State.Running != nil && strings.TrimPrefix(status.ImageID, "docker-pullable://") == container.Image {
					for _, listener := range container.Ports {
						if listener.Name == "health" && listener.Protocol == corev1.ProtocolTCP && listener.ContainerPort > 0 && listener.ContainerPort <= 65535 {
							port = int(listener.ContainerPort)
						}
					}
				}
			}
		}
		selected = pod
	}
	if selected == nil || port == 0 {
		return nil, 0, errors.New("public Front process identity unavailable")
	}
	return selected, port, nil
}

func (s *Server) readPublicFrontNetwork(ctx context.Context, node model.EdgeNode, query frontnetwork.Request) (frontnetwork.Response, error) {
	client, err := s.newClusterNodeClient()
	if err != nil {
		return frontnetwork.Response{}, err
	}
	defer client.closeIdleConnections()
	fail := func() (frontnetwork.Response, error) {
		return frontnetwork.Response{}, errors.New("public Front runtime binding failed")
	}
	base := "/api/v1/namespaces/" + url.PathEscape(s.controlPlaneNamespace)
	var services corev1.ServiceList
	var pods corev1.PodList
	podPath := base + "/pods?" + url.Values{"fieldSelector": {"spec.nodeName=" + node.ID}, "limit": {"256"}}.Encode()
	if client.doJSON(ctx, http.MethodGet, base+"/services?limit=256", &services) != nil || services.Continue != "" || client.doJSON(ctx, http.MethodGet, podPath, &pods) != nil || pods.Continue != "" {
		return fail()
	}
	service, err := publicFrontService(services.Items, node, s.controlPlaneNamespace)
	if err != nil {
		return fail()
	}
	pod, port, err := publicFrontPod(*service, pods.Items, node)
	if err != nil {
		return fail()
	}
	slicePath := "/apis/discovery.k8s.io/v1/namespaces/" + url.PathEscape(s.controlPlaneNamespace) + "/endpointslices?" + url.Values{"labelSelector": {discoveryv1.LabelServiceName + "=" + service.Name}, "limit": {"256"}}.Encode()
	var endpoints discoveryv1.EndpointSliceList
	if client.doJSON(ctx, http.MethodGet, slicePath, &endpoints) != nil || endpoints.Continue != "" || !dnsBackendEndpointsMatch(*service, *pod, endpoints.Items, true) {
		return fail()
	}
	var connections frontnetwork.LiveConnections
	observed := time.Now().UTC()
	path := base + "/pods/" + url.PathEscape(pod.Name+":"+strconv.Itoa(port)) + "/proxy/edge/tcp-connections"
	if readDNSPodObservation(ctx, client, path, &connections, 2<<20) != nil {
		return fail()
	}
	sample, err := frontnetwork.SampleFromLiveConnections(query, connections, observed)
	if err != nil {
		return fail()
	}
	var currentServices corev1.ServiceList
	var currentPod corev1.Pod
	var currentEndpoints discoveryv1.EndpointSliceList
	if client.doJSON(ctx, http.MethodGet, base+"/services?limit=256", &currentServices) != nil || currentServices.Continue != "" || client.doJSON(ctx, http.MethodGet, base+"/pods/"+url.PathEscape(pod.Name), &currentPod) != nil || client.doJSON(ctx, http.MethodGet, slicePath, &currentEndpoints) != nil || currentEndpoints.Continue != "" {
		return fail()
	}
	current, err := publicFrontService(currentServices.Items, node, s.controlPlaneNamespace)
	if err != nil || current.UID != service.UID || current.ResourceVersion != service.ResourceVersion || currentPod.UID != pod.UID || currentPod.ResourceVersion != pod.ResourceVersion || !reflect.DeepEqual(currentEndpoints.Items, endpoints.Items) {
		return fail()
	}
	digest, err := platformconfig.Digest(endpoints.Items)
	if err != nil {
		return fail()
	}
	sample.Backend = &model.EdgeClientNetworkBackend{Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), PodVersion: pod.ResourceVersion, ServiceName: service.Name, ServiceUID: string(service.UID), ServiceVersion: service.ResourceVersion, EndpointsDigest: digest}
	if model.ValidateEdgeClientNetworkSample(&sample) != nil {
		return fail()
	}
	return frontnetwork.Response{Schema: frontnetwork.Schema, Nonce: query.Nonce, EdgeID: node.ID, GroupID: node.EdgeGroupID, Sample: sample}, nil
}
