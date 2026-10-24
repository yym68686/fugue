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
	fail := func(reason, detail string) (platformproducer.CapacityCodeIdentity, error) {
		return result, codeObservationError(reason, detail)
	}
	client, err := s.edgeEnrollmentClusterClient()
	if err != nil {
		return fail("code_observation_unavailable", "cluster_client")
	}
	defer client.closeIdleConnections()
	base := "/api/v1/namespaces/" + url.PathEscape(s.controlPlaneNamespace)
	var node corev1.Node
	var authority corev1.ConfigMap
	var journals corev1.ConfigMapList
	var services corev1.ServiceList
	var pods corev1.PodList
	read := func(path string, out any) bool { return readCapacityMetadata(ctx, client, path, out) == nil }
	if !read("/api/v1/nodes/"+url.PathEscape(edge.ID), &node) {
		return fail("code_observation_unavailable", "node_metadata")
	}
	if node.Name != edge.ID || node.UID == "" || node.DeletionTimestamp != nil {
		return fail("code_identity_conflict", "node_metadata")
	}
	authorityPath := base + "/configmaps/" + url.PathEscape("fugue-current-authority-"+edge.EdgeGroupID)
	if !read(authorityPath, &authority) {
		return fail("code_observation_unavailable", "authority_metadata")
	}
	if authority.UID == "" || authority.DeletionTimestamp != nil || authority.Labels["fugue.pro/authority-store"] != "true" {
		return fail("code_identity_conflict", "authority_metadata")
	}
	if !read(base+"/configmaps?"+url.Values{"labelSelector": {"fugue.pro/group=" + edge.EdgeGroupID + ",fugue.pro/authority-kind=transition-journal"}, "limit": {"1"}}.Encode(), &journals) || journals.Continue != "" {
		return fail("code_observation_unavailable", "transition_journal")
	}
	if len(journals.Items) != 0 {
		return fail("code_transition_in_progress", "transition_journal")
	}
	if !read(base+"/services?limit=256", &services) || services.Continue != "" {
		return fail("code_observation_unavailable", "service_inventory")
	}
	if !read(base+"/pods?"+url.Values{"fieldSelector": {"spec.nodeName=" + edge.ID}, "limit": {"256"}}.Encode(), &pods) || pods.Continue != "" {
		return fail("code_observation_unavailable", "pod_inventory")
	}
	var current edgeEnrollmentAuthority
	if json.Unmarshal([]byte(authority.Data["authority.json"]), &current) != nil || current.GroupID != edge.EdgeGroupID {
		return fail("code_identity_conflict", "authority_metadata")
	}
	service, err := publicFrontService(services.Items, edge, s.controlPlaneNamespace)
	if err != nil {
		return fail("code_identity_conflict", "public_service")
	}
	front, port, err := publicFrontPod(*service, pods.Items, edge)
	if err != nil {
		return fail("code_observation_unavailable", "public_front")
	}
	var endpoints discoveryv1.EndpointSliceList
	if !read("/apis/discovery.k8s.io/v1/namespaces/"+url.PathEscape(s.controlPlaneNamespace)+"/endpointslices?"+url.Values{"labelSelector": {discoveryv1.LabelServiceName + "=" + service.Name}, "limit": {"256"}}.Encode(), &endpoints) || endpoints.Continue != "" {
		return fail("code_observation_unavailable", "endpoints")
	}
	if !dnsBackendEndpointsMatch(*service, *front, endpoints.Items, true) {
		return fail("code_identity_conflict", "endpoints")
	}
	var health capacityFrontState
	if readDNSPodObservation(ctx, client, base+"/pods/"+url.PathEscape(front.Name+":"+strconv.Itoa(port))+"/proxy/healthz", &health, 32<<10) != nil {
		return fail("code_observation_unavailable", "front_health")
	}
	if health.Status != "ok" {
		return fail("code_observation_unavailable", "front_health")
	}
	if health.Authority != edgegroupfront.ActivationAuthority {
		return fail("code_identity_conflict", "front_health")
	}
	worker, err := capacityTargetWorker(*front, pods.Items, edge, node, health)
	if err != nil {
		return fail("code_identity_conflict", "worker_target")
	}
	frontImage := ""
	for _, c := range front.Spec.Containers {
		if c.Name == "edge-front" {
			_, frontImage, _ = strings.Cut(c.Image, "@")
		}
	}
	// Bind the observed code to the actual selected Worker first. A stale
	// Front field without a matching running process cannot drive a recovery plan.
	result = platformproducer.CapacityCodeIdentity{NodeUID: string(node.UID), FrontPodUID: string(front.UID), WorkerPodUID: string(worker.UID), ServiceUID: string(service.UID), AuthorityUID: string(authority.UID), AuthorityRecordDigest: current.Record, WorkerSourceSHA: health.Source, WorkerImageDigest: health.Image, FrontImageDigest: frontImage, ProxyImageDigest: runningCapacityImage(*worker, "caddy"), Slot: health.Slot, Fence: health.Fence, Bundle: health.Bundle}
	if !result.Valid() {
		return fail("code_identity_conflict", "code_binding")
	}
	var latest corev1.ConfigMap
	if !read(authorityPath, &latest) {
		return fail("code_observation_unavailable", "authority_metadata")
	}
	if latest.UID != authority.UID || latest.ResourceVersion != authority.ResourceVersion || latest.Data["authority.json"] != authority.Data["authority.json"] {
		return fail("code_identity_conflict", "authority_changed")
	}
	observed := model.EdgeCodeRecoveryTarget{Slot: health.Slot, FrontGeneration: health.Fence, BundleGeneration: health.Bundle, WorkerSourceSHA: health.Source, WorkerImageDigest: health.Image}
	target := model.EdgeCodeRecoveryTarget{Slot: current.Slot, FrontGeneration: current.Fence, BundleGeneration: current.Bundle, WorkerSourceSHA: current.Source, WorkerImageDigest: current.Image}
	var previous *model.EdgeCodeRecoveryTarget
	if platformproducer.ValidDigest(current.PreviousRecord) && current.PreviousRecord != current.Record {
		previous = &model.EdgeCodeRecoveryTarget{Slot: current.PreviousSlot, FrontGeneration: current.PreviousFence, BundleGeneration: current.PreviousBundle, WorkerSourceSHA: current.PreviousSource, WorkerImageDigest: current.PreviousImage}
	}
	plan, planErr := model.PlanEdgeCodeRecovery(current.Record, observed, target, previous)
	if planErr != nil {
		if target.Valid() && observed.FrontGeneration < target.FrontGeneration {
			return fail("code_not_current", "recovery_history")
		}
		return fail("code_identity_conflict", "code_binding")
	}
	if len(plan.Stages) > 0 {
		return result, &capacityCodeError{reason: "code_not_current", detail: "code_binding", plan: &plan}
	}

	return result, nil
}

func capacityTargetWorker(front corev1.Pod, pods []corev1.Pod, edge model.EdgeNode, node corev1.Node, health capacityFrontState) (*corev1.Pod, error) {
	host, portText, err := net.SplitHostPort(health.Target)
	port, portErr := strconv.Atoi(portText)
	if err != nil || portErr != nil || port < 1 || port > 65535 || net.ParseIP(host) == nil {
		return nil, errors.New("literal Front target required")
	}
	nodeAddress := false
	for _, a := range node.Status.Addresses {
		nodeAddress = nodeAddress || (a.Type == corev1.NodeInternalIP || a.Type == corev1.NodeExternalIP) && a.Address == host
	}
	var selected *corev1.Pod
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName != edge.ID || pod.Namespace != front.Namespace {
			continue
		}
		addressMatches := pod.Status.PodIP == host
		addressMatches = addressMatches || net.ParseIP(host).IsLoopback() && front.Spec.HostNetwork && pod.Spec.HostNetwork
		// Caddy, not the Edge control process, owns the Worker HTTPS socket.
		targetMatches := false
		for _, c := range pod.Spec.Containers {
			if c.Name != "caddy" {
				continue
			}
			for _, p := range c.Ports {
				hostPortMatches := nodeAddress && p.HostPort == int32(port) && (p.HostIP == "" || p.HostIP == host || p.HostIP == "0.0.0.0" && net.ParseIP(host).To4() != nil || p.HostIP == "::" && net.ParseIP(host).To4() == nil)
				targetMatches = targetMatches || p.Name == "https-slot" && p.Protocol == corev1.ProtocolTCP && (addressMatches && p.ContainerPort == int32(port) || hostPortMatches)
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
