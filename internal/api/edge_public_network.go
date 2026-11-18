package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"fugue/internal/frontnetwork"
	"fugue/internal/httpx"
	"fugue/internal/kubeauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var errPublicFrontConnectionMissing = errors.New("connection_missing")

func classifyPublicFrontObservation(response *http.Response) error {
	if response.StatusCode != http.StatusNotFound {
		return nil
	}
	// The Kubernetes pod proxy also emits 404s for missing Pods. Only accept
	// the exact, bounded response from the indexed Front handler.
	body, err := io.ReadAll(io.LimitReader(response.Body, 128))
	if err == nil {
		switch string(body) {
		case "exact indexed connection unavailable\n", "indexed connection changed during observation\n":
			return errPublicFrontConnectionMissing
		}
	}
	return nil
}

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
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	if !s.waitPublicNetworkObservation(ctx, query.EdgeID) {
		if s.log != nil {
			s.log.Printf("public Front network observation rejected; edge_id=%s slot=%s reason=rate_limited", query.EdgeID, query.Slot)
		}
		writer.Header().Set("Retry-After", "1")
		httpx.WriteError(writer, http.StatusTooManyRequests, "Front observation budget exhausted")
		return
	}
	node, _, err := s.store.GetEdgeNode(query.EdgeID)
	if err != nil || node.EdgeGroupID != query.GroupID {
		httpx.WriteError(writer, http.StatusServiceUnavailable, "Front node identity unavailable")
		return
	}
	response, err := s.readPublicFrontNetwork(ctx, node, query)
	if err != nil {
		if s.log != nil {
			s.log.Printf("public Front network observation rejected; edge_id=%s slot=%s reason=%s", query.EdgeID, query.Slot, err.Error())
		}
		if errors.Is(err, errPublicFrontConnectionMissing) {
			httpx.WriteError(writer, http.StatusNotFound, "selected public Front connection is no longer present")
		} else {
			httpx.WriteError(writer, http.StatusServiceUnavailable, "selected public Front connection evidence unavailable")
		}
		return
	}
	writer.Header().Set("Cache-Control", "private, no-store")
	httpx.WriteJSON(writer, http.StatusOK, response)
}

func (s *Server) waitPublicNetworkObservation(ctx context.Context, edgeID string) bool {
	delay, ok := s.reservePublicNetworkObservation(edgeID, time.Now())
	if !ok || ctx.Err() != nil {
		return false
	}
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

func (s *Server) reservePublicNetworkObservation(edgeID string, now time.Time) (time.Duration, bool) {
	if !s.frontNetworkObservationMu.TryLock() {
		return 0, false
	}
	defer s.frontNetworkObservationMu.Unlock()
	if s.frontNetworkObservationLast == nil {
		s.frontNetworkObservationLast = map[string]time.Time{}
	}
	delay := time.Duration(0)
	if previous, found := s.frontNetworkObservationLast[edgeID]; found {
		// Reserve only the next slot. A future reservation already belongs to
		// one waiter; further callers cannot build a queue or consume its slot.
		if previous.After(now) {
			return 0, false
		}
		delay = max(0, previous.Add(time.Second).Sub(now))
	}
	for identity, previous := range s.frontNetworkObservationLast {
		if now.Sub(previous) > time.Minute {
			delete(s.frontNetworkObservationLast, identity)
		}
	}
	if len(s.frontNetworkObservationLast) >= 256 {
		return 0, false
	}
	s.frontNetworkObservationLast[edgeID] = now.Add(delay)
	return delay, true
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

type publicFrontBinding struct {
	service                    *corev1.Service
	pod                        *corev1.Pod
	endpoints                  []discoveryv1.EndpointSlice
	base, slicePath, proxyPath string
}

func publicFrontBindingError(reason string) error {
	return errors.New("public Front runtime binding failed: " + reason)
}

func (s *Server) resolvePublicFrontBinding(ctx context.Context, client *clusterNodeClient, node model.EdgeNode) (publicFrontBinding, error) {
	base := "/api/v1/namespaces/" + url.PathEscape(s.controlPlaneNamespace)
	var services corev1.ServiceList
	if client.doJSON(ctx, http.MethodGet, base+"/services?limit=256", &services) != nil || services.Continue != "" {
		return publicFrontBinding{}, publicFrontBindingError("runtime_inventory_unavailable")
	}
	service, err := publicFrontService(services.Items, node, s.controlPlaneNamespace)
	if err != nil {
		return publicFrontBinding{}, publicFrontBindingError("serving_service_identity")
	}
	// Only narrow discovery to possible matches; keep the complete Service
	// inventory above to detect conflicting owners of an external IP/port.
	// Preserve the local matcher's treatment of absent, empty-valued labels.
	nonempty := make(labels.Set, len(service.Spec.Selector))
	for key, value := range service.Spec.Selector {
		if value != "" {
			nonempty[key] = value
		}
	}
	podQuery := url.Values{"fieldSelector": {"spec.nodeName=" + node.ID}, "limit": {"256"}}
	if len(nonempty) != 0 {
		podQuery.Set("labelSelector", labels.SelectorFromSet(nonempty).String())
	}
	var pods corev1.PodList
	if client.doJSON(ctx, http.MethodGet, base+"/pods?"+podQuery.Encode(), &pods) != nil || pods.Continue != "" {
		return publicFrontBinding{}, publicFrontBindingError("runtime_inventory_unavailable")
	}
	pod, port, err := publicFrontPod(*service, pods.Items, node)
	if err != nil {
		return publicFrontBinding{}, publicFrontBindingError("serving_pod_identity")
	}
	slicePath := "/apis/discovery.k8s.io/v1/namespaces/" + url.PathEscape(s.controlPlaneNamespace) + "/endpointslices?" + url.Values{"labelSelector": {discoveryv1.LabelServiceName + "=" + service.Name}, "limit": {"256"}}.Encode()
	var endpoints discoveryv1.EndpointSliceList
	if client.doJSON(ctx, http.MethodGet, slicePath, &endpoints) != nil || endpoints.Continue != "" || !dnsBackendEndpointsMatch(*service, *pod, endpoints.Items, true) {
		return publicFrontBinding{}, publicFrontBindingError("serving_endpoints_identity")
	}
	path := base + "/pods/" + url.PathEscape(pod.Name+":"+strconv.Itoa(port)) + "/proxy"
	return publicFrontBinding{service, pod, endpoints.Items, base, slicePath, path}, nil
}

func (b publicFrontBinding) verifiedBackend(ctx context.Context, client *clusterNodeClient, node model.EdgeNode) (*model.EdgeClientNetworkBackend, error) {
	var currentServices corev1.ServiceList
	var currentPod corev1.Pod
	var currentEndpoints discoveryv1.EndpointSliceList
	if client.doJSON(ctx, http.MethodGet, b.base+"/services?limit=256", &currentServices) != nil || currentServices.Continue != "" || client.doJSON(ctx, http.MethodGet, b.base+"/pods/"+url.PathEscape(b.pod.Name), &currentPod) != nil || client.doJSON(ctx, http.MethodGet, b.slicePath, &currentEndpoints) != nil || currentEndpoints.Continue != "" {
		return nil, publicFrontBindingError("runtime_recheck_unavailable")
	}
	current, err := publicFrontService(currentServices.Items, node, b.service.Namespace)
	if err != nil || current.UID != b.service.UID || current.ResourceVersion != b.service.ResourceVersion || currentPod.UID != b.pod.UID || currentPod.ResourceVersion != b.pod.ResourceVersion || !reflect.DeepEqual(currentEndpoints.Items, b.endpoints) {
		return nil, publicFrontBindingError("runtime_changed_during_observation")
	}
	digest, err := platformconfig.Digest(b.endpoints)
	if err != nil {
		return nil, publicFrontBindingError("endpoints_digest_invalid")
	}
	return &model.EdgeClientNetworkBackend{Namespace: b.pod.Namespace, PodName: b.pod.Name, PodUID: string(b.pod.UID), PodVersion: b.pod.ResourceVersion,
		ServiceName: b.service.Name, ServiceUID: string(b.service.UID), ServiceVersion: b.service.ResourceVersion, EndpointsDigest: digest}, nil
}

func (s *Server) readPublicFrontNetwork(ctx context.Context, node model.EdgeNode, query frontnetwork.Request) (frontnetwork.Response, error) {
	ctx = kubeauth.WithObservationSite(ctx, kubeauth.ObservePublicFront)
	client, err := s.newClusterNodeClient()
	if err != nil {
		return frontnetwork.Response{}, err
	}
	defer client.closeIdleConnections()
	fail := func(reason string) (frontnetwork.Response, error) {
		return frontnetwork.Response{}, publicFrontBindingError(reason)
	}
	binding, err := s.resolvePublicFrontBinding(ctx, client, node)
	if err != nil {
		return frontnetwork.Response{}, err
	}
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return fail("socket_query_invalid")
	}
	var observation frontnetwork.Response
	path := binding.proxyPath + "/edge/tcp-connections?" + url.Values{"request": {string(queryJSON)}}.Encode()
	if err := readPodObservation(ctx, client, path, &observation, 4096, classifyPublicFrontObservation); err != nil {
		if errors.Is(err, errPublicFrontConnectionMissing) {
			return frontnetwork.Response{}, err
		}
		return fail("socket_inventory_unavailable")
	}
	peer, err := frontnetwork.ParseRemote(query.RemoteAddr)
	now := time.Now().UTC()
	if err != nil || observation.Schema != frontnetwork.Schema || observation.Nonce != query.Nonce || observation.EdgeID != query.EdgeID || observation.GroupID != query.GroupID ||
		observation.Sample.Slot != query.Slot || observation.Sample.Scope != frontnetwork.Scope(peer.Addr()) || observation.Sample.ObservedAt.After(now.Add(5*time.Second)) ||
		now.Sub(observation.Sample.ObservedAt) > 5*time.Second || model.ValidateEdgeClientNetworkSample(&observation.Sample) != nil {
		return fail("socket_observation_binding")
	}
	sample := observation.Sample
	sample.Backend, err = binding.verifiedBackend(ctx, client, node)
	if err != nil {
		return frontnetwork.Response{}, err
	}
	if model.ValidateEdgeClientNetworkSample(&sample) != nil {
		return fail("sample_validation_failed")
	}
	return frontnetwork.Response{Schema: frontnetwork.Schema, Nonce: query.Nonce, EdgeID: node.ID, GroupID: node.EdgeGroupID, Sample: sample}, nil
}
