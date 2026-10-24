package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/model"
	"fugue/internal/platformproducer"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
)

type capacityFrontState struct {
	Status    string `json:"status"`
	Slot      string `json:"active_slot"`
	Target    string `json:"https_target"`
	Fence     uint64 `json:"activation_generation"`
	Bundle    string `json:"bundle_generation"`
	Source    string `json:"worker_source_commit"`
	Image     string `json:"worker_image_digest"`
	Authority string `json:"route_authority"`
}

// This read never changes a node, a code-release hold, or a serving artifact.
// The caller captures it on both sides of a signed public transfer. Missing,
// changing, or ambiguous physical ownership keeps the member quarantined.
func (s *Server) readCapacityCode(parent context.Context, edge model.EdgeNode) (platformproducer.CapacityCodeIdentity, error) {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	var result platformproducer.CapacityCodeIdentity
	fail := func() (platformproducer.CapacityCodeIdentity, error) {
		return result, errors.New("current physical Edge code unavailable")
	}
	client, err := s.edgeEnrollmentClusterClient()
	if err != nil {
		return result, err
	}
	defer client.closeIdleConnections()
	base := "/api/v1/namespaces/" + url.PathEscape(s.controlPlaneNamespace)
	var node corev1.Node
	var authority corev1.ConfigMap
	var journals corev1.ConfigMapList
	var services corev1.ServiceList
	var pods corev1.PodList
	read := func(path string, out any) bool { return readCapacityMetadata(ctx, client, path, out) == nil }
	if !read("/api/v1/nodes/"+url.PathEscape(edge.ID), &node) || node.Name != edge.ID || node.UID == "" || node.DeletionTimestamp != nil ||
		!read(base+"/configmaps/"+url.PathEscape("fugue-current-authority-"+edge.EdgeGroupID), &authority) || authority.UID == "" || authority.DeletionTimestamp != nil || authority.Labels["fugue.pro/authority-store"] != "true" ||
		!read(base+"/configmaps?"+url.Values{"labelSelector": {"fugue.pro/group=" + edge.EdgeGroupID + ",fugue.pro/authority-kind=transition-journal"}, "limit": {"1"}}.Encode(), &journals) || journals.Continue != "" || len(journals.Items) != 0 ||
		!read(base+"/services?limit=256", &services) || services.Continue != "" ||
		!read(base+"/pods?"+url.Values{"fieldSelector": {"spec.nodeName=" + edge.ID}, "limit": {"256"}}.Encode(), &pods) || pods.Continue != "" {
		return fail()
	}
	var current edgeEnrollmentAuthority
	if json.Unmarshal([]byte(authority.Data["authority.json"]), &current) != nil || current.GroupID != edge.EdgeGroupID {
		return fail()
	}
	service, err := publicFrontService(services.Items, edge, s.controlPlaneNamespace)
	if err != nil {
		return fail()
	}
	front, port, err := publicFrontPod(*service, pods.Items, edge)
	if err != nil {
		return fail()
	}
	var endpoints discoveryv1.EndpointSliceList
	if !read("/apis/discovery.k8s.io/v1/namespaces/"+url.PathEscape(s.controlPlaneNamespace)+"/endpointslices?"+url.Values{"labelSelector": {discoveryv1.LabelServiceName + "=" + service.Name}, "limit": {"256"}}.Encode(), &endpoints) || endpoints.Continue != "" || !dnsBackendEndpointsMatch(*service, *front, endpoints.Items, true) {
		return fail()
	}
	var health capacityFrontState
	if readDNSPodObservation(ctx, client, base+"/pods/"+url.PathEscape(front.Name+":"+strconv.Itoa(port))+"/proxy/healthz", &health, 32<<10) != nil ||
		health.Status != "ok" || health.Authority != edgegroupfront.ActivationAuthority || health.Slot != current.Slot || health.Fence != current.Fence || health.Bundle != current.Bundle || health.Source != current.Source || health.Image != current.Image {
		return fail()
	}
	worker, err := capacityTargetWorker(*front, pods.Items, edge, health)
	if err != nil {
		return fail()
	}
	frontImage := ""
	for _, c := range front.Spec.Containers {
		if c.Name == "edge-front" {
			_, frontImage, _ = strings.Cut(c.Image, "@")
		}
	}
	result = platformproducer.CapacityCodeIdentity{NodeUID: string(node.UID), FrontPodUID: string(front.UID), WorkerPodUID: string(worker.UID), ServiceUID: string(service.UID), AuthorityUID: string(authority.UID), AuthorityRecordDigest: current.Record, WorkerSourceSHA: current.Source, WorkerImageDigest: current.Image, FrontImageDigest: frontImage, ProxyImageDigest: runningCapacityImage(*worker, "caddy"), Slot: current.Slot, Fence: current.Fence, Bundle: current.Bundle}
	if !result.Valid() {
		return fail()
	}
	return result, nil
}

func capacityTargetWorker(front corev1.Pod, pods []corev1.Pod, edge model.EdgeNode, health capacityFrontState) (*corev1.Pod, error) {
	host, portText, err := net.SplitHostPort(health.Target)
	port, portErr := strconv.Atoi(portText)
	if err != nil || portErr != nil || port < 1 || port > 65535 || net.ParseIP(host) == nil {
		return nil, errors.New("literal Front target required")
	}
	var selected *corev1.Pod
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName != edge.ID || pod.Namespace != front.Namespace {
			continue
		}
		addressMatches := pod.Status.PodIP == host
		addressMatches = addressMatches || net.ParseIP(host).IsLoopback() && front.Spec.HostNetwork && pod.Spec.HostNetwork
		if !addressMatches {
			continue
		}
		// Caddy, not the Edge control process, owns the Worker HTTPS socket.
		targetMatches := false
		for _, c := range pod.Spec.Containers {
			if c.Name != "caddy" {
				continue
			}
			for _, p := range c.Ports {
				targetMatches = targetMatches || p.Name == "https-slot" && p.ContainerPort == int32(port) && p.Protocol == corev1.ProtocolTCP
			}
		}
		if !targetMatches {
			continue
		}
		if selected != nil || pod.UID == "" || pod.DeletionTimestamp != nil || !dnsBackendPodReady(*pod) || pod.Labels["fugue.io/edge-group-id"] != edge.EdgeGroupID || pod.Labels["fugue.io/edge-slot"] != health.Slot || pod.Annotations["fugue.pro/source-commit"] != health.Source {
			return nil, errors.New("ambiguous or outdated Front target")
		}
		workerImage, proxyImage := runningCapacityImage(*pod, "edge"), runningCapacityImage(*pod, "caddy")
		if workerImage != health.Image || !platformproducer.ValidDigest(proxyImage) {
			return nil, errors.New("running Worker or Caddy image not verified")
		}
		selected = pod
	}
	if selected == nil {
		return nil, errors.New("Front target has no verified physical Worker")
	}
	return selected, nil
}

func runningCapacityImage(pod corev1.Pod, containerName string) string {
	for _, c := range pod.Spec.Containers {
		if c.Name != containerName {
			continue
		}
		_, digest, found := strings.Cut(c.Image, "@")
		if !found || !platformproducer.ValidDigest(digest) {
			return ""
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == c.Name && status.Ready && status.State.Running != nil && strings.TrimPrefix(status.ImageID, "docker-pullable://") == c.Image {
				return digest
			}
		}
	}
	return ""
}

// Kubernetes metadata is trusted for identity, but every response still has a
// memory bound. Unlike the exact Front health contract, Kubernetes objects may
// contain newer fields that this client does not consume.
func readCapacityMetadata(ctx context.Context, client *clusterNodeClient, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, client.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+client.bearerToken)
	req.Header.Set("Accept", "application/json")
	httpClient := *client.client
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("capacity metadata read unavailable")
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	if err != nil || len(raw) > 8<<20 {
		return errors.New("capacity metadata response exceeds bound")
	}
	return json.Unmarshal(raw, out)
}
