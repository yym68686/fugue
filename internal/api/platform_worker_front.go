package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/kubeauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Enrollment is an API release input, never a field supplied by a reporter.
// Pins survive a selector handoff on the same Service but not its replacement.
type WorkerFrontBindings map[string]workerFrontPin
type workerFrontPin struct {
	uid   string
	reads chan struct{}
}

var workerServiceUID = regexp.MustCompile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
var workerSource = regexp.MustCompile("^[0-9a-f]{40}$")
var workerDigest = regexp.MustCompile("^sha256:[0-9a-f]{64}$")

func ParseWorkerFrontBindings(raw string) (WorkerFrontBindings, error) {
	if raw == "" {
		return nil, nil // no overlap enrollment in this release
	}
	bad := errors.New("invalid FUGUE_WORKER_FRONT_BINDINGS: expected unique node and public Service UID pairs")
	var list []struct {
		Node       string `json:"node"`
		ServiceUID string `json:"service_uid"`
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if len(raw) > 16<<10 || dec.Decode(&list) != nil || dec.Decode(&struct{}{}) != io.EOF || len(list) == 0 || len(list) > 64 {
		return nil, bad
	}
	out, seen := WorkerFrontBindings{}, map[string]bool{}
	for _, pin := range list {
		if len(validation.IsDNS1123Subdomain(pin.Node)) != 0 || !workerServiceUID.MatchString(pin.ServiceUID) || out[pin.Node].uid != "" || seen[pin.ServiceUID] {
			return nil, bad
		}
		seen[pin.ServiceUID] = true
		out[pin.Node] = workerFrontPin{pin.ServiceUID, make(chan struct{}, 2)}
	}
	return out, nil
}

type workerFrontHealth struct {
	Status     string `json:"status"`
	ActiveSlot string `json:"active_slot"`
	Target     string `json:"https_target"`
	Generation uint64 `json:"activation_generation"`
	Bundle     string `json:"bundle_generation"`
	Source     string `json:"worker_source_commit"`
	Image      string `json:"worker_image_digest"`
	Authority  string `json:"route_authority"`
}

// Compare the Front's address with a declared host listener; never dial an
// address from the observation. A logical slot or image alone is not an
// instance identity. Even a failed receipt could invalidate the active one.
func physicalWorkerOwner(front corev1.Pod, h workerFrontHealth, pods []corev1.Pod, who platformcontrol.PlatformComponentIdentityClaims, positive bool) (*corev1.Pod, int) {
	target, err := netip.ParseAddrPort(h.Target)
	if err != nil || target.Port() == 0 || target.Addr().Zone() != "" || target.Addr().IsUnspecified() || target.Addr().IsMulticast() ||
		target.Addr().String() != front.Status.HostIP || h.Status != "ok" || h.Generation == 0 || h.Bundle == "" || h.Authority != "edge-control" ||
		(h.ActiveSlot != "a" && h.ActiveSlot != "b") || !workerSource.MatchString(h.Source) || !workerDigest.MatchString(h.Image) {
		return nil, http.StatusServiceUnavailable
	}
	group := front.Labels["fugue.io/edge-group-id"]
	if group == "" || who.NodeID != front.Spec.NodeName || who.ScopeKey != "global" || who.Component != model.PlatformConsumerComponentEdgeWorker || who.AuthorityID != "" && who.AuthorityID != group {
		return nil, http.StatusForbidden
	}
	var owner *corev1.Pod
	for i := range pods {
		p := &pods[i]
		if p.Spec.NodeName != who.NodeID {
			continue
		}
		for _, c := range p.Spec.Containers {
			for _, port := range c.Ports {
				if port.Protocol != corev1.ProtocolTCP || int32(target.Port()) != port.HostPort {
					continue
				}
				if port.HostIP != "" && port.HostIP != target.Addr().String() {
					ip, e := netip.ParseAddr(port.HostIP)
					if e != nil {
						return nil, http.StatusServiceUnavailable
					}
					if !ip.IsUnspecified() || ip.Is4() != target.Addr().Is4() {
						continue
					}
				}
				if owner != nil || c.Name != "caddy" || port.Name != "https-slot" || port.ContainerPort != port.HostPort {
					return nil, http.StatusServiceUnavailable
				}
				owner = p
			}
		}
	}
	if owner == nil || owner.Namespace != front.Namespace || owner.Name == "" || owner.UID == "" || owner.ResourceVersion == "" || owner.Spec.ServiceAccountName == "" ||
		owner.Status.Phase != corev1.PodRunning || owner.DeletionTimestamp != nil || owner.Status.HostIP != front.Status.HostIP ||
		owner.Labels["fugue.io/edge-slot"] != h.ActiveSlot || owner.Labels["fugue.io/edge-group-id"] != group || owner.Annotations["fugue.pro/source-commit"] != h.Source {
		return nil, http.StatusServiceUnavailable
	}
	if who.CredentialID != "kubernetes:"+owner.Namespace+":"+owner.Spec.ServiceAccountName+":"+string(owner.UID) {
		return nil, http.StatusConflict
	}
	policy, ok := decodePlatformConsumerIdentityPolicy(owner.Annotations[platformConsumerIdentityAnnotation])
	if !ok || policy.Component != who.Component || policy.AuthorityID != who.AuthorityID || policy.ScopeKey != who.ScopeKey || len(policy.ArtifactKinds) != len(who.ArtifactKinds) {
		return nil, http.StatusForbidden
	}
	for _, k := range who.ArtifactKinds {
		if !slices.Contains(policy.ArtifactKinds, k) {
			return nil, http.StatusForbidden
		}
	}
	if positive && !dnsBackendPodReady(*owner) {
		return nil, http.StatusServiceUnavailable
	}
	for _, c := range owner.Spec.Containers {
		if c.Name != "edge" || !strings.HasSuffix(c.Image, "@"+h.Image) {
			continue
		}
		for _, st := range owner.Status.ContainerStatuses {
			id := strings.TrimPrefix(st.ImageID, "docker-pullable://")
			if st.Name == c.Name && st.State.Running != nil && (!positive || st.Ready) && (id == c.Image || id == h.Image || id == "containerd://"+h.Image) {
				return owner, http.StatusOK
			}
		}
	}
	return nil, http.StatusServiceUnavailable
}

type selectedWorkerRuntimeBinding struct {
	ServiceUID        string `json:"service_uid"`
	ServiceGeneration string `json:"service_generation"`
	ServiceDigest     string `json:"service_digest"`
	FrontUID          string `json:"front_uid"`
	WorkerUID         string `json:"worker_uid"`
	ActiveSlot        string `json:"active_slot"`
	Generation        uint64 `json:"activation_generation"`
	Bundle            string `json:"bundle_generation"`
	Source            string `json:"worker_source_commit"`
	Image             string `json:"worker_image_digest"`
	Target            string `json:"https_target"`
}

func (s *Server) admitPhysicalWorker(w http.ResponseWriter, r *http.Request, who platformcontrol.PlatformComponentIdentityClaims, positive bool) (context.Context, bool) {
	pin, enabled := s.workerFrontBindings[who.NodeID]
	if !enabled || who.Component != model.PlatformConsumerComponentEdgeWorker || who.ScopeKey != "global" {
		return r.Context(), true
	}
	status := http.StatusServiceUnavailable
	admissionCtx := r.Context()
	if !strings.HasPrefix(who.CredentialID, "kubernetes:") {
		status = http.StatusForbidden
	} else {
		select {
		case pin.reads <- struct{}{}:
			defer func() { <-pin.reads }()
			status, admissionCtx = s.checkPhysicalWorker(r.Context(), who, pin.uid, positive)
		default:
		}
	}
	if status == http.StatusOK {
		return admissionCtx, true
	}
	if status == http.StatusServiceUnavailable {
		writeWorkerTransportUnavailable(w)
	} else {
		httpx.WriteError(w, status, "reporter is not the selected physical Worker")
	}
	return r.Context(), false
}

func writeWorkerTransportUnavailable(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	httpx.WriteJSON(w, http.StatusServiceUnavailable, httpx.ErrorResponse{Error: "selected Worker transport could not be verified", Code: "worker_transport_unavailable", Category: "unavailable", Retryable: true})
}

func (s *Server) checkPhysicalWorker(request context.Context, who platformcontrol.PlatformComponentIdentityClaims, serviceUID string, positive bool) (int, context.Context) {
	ctx, cancel := context.WithTimeout(kubeauth.WithObservationSite(request, kubeauth.ObservePublicFront), 4*time.Second)
	defer cancel()
	node, _, err := s.store.GetEdgeNode(who.NodeID)
	if err != nil || node.ID != who.NodeID {
		return http.StatusServiceUnavailable, ctx
	}
	predecessor, err := s.store.ReadWorkerReporterPredecessor(ctx, who.NodeID)
	if err != nil {
		return http.StatusServiceUnavailable, ctx
	}
	_, binding, status := s.inspectPhysicalWorker(ctx, node, who, serviceUID, positive)
	if status != http.StatusOK {
		return status, ctx
	}
	digest, err := platformconfig.Digest(binding)
	if err != nil {
		return http.StatusServiceUnavailable, ctx
	}
	return status, predecessor.Bind(request, digest, who.CredentialID)
}

func (s *Server) inspectPhysicalWorker(ctx context.Context, node model.EdgeNode, who platformcontrol.PlatformComponentIdentityClaims, serviceUID string, positive bool) (*corev1.Pod, selectedWorkerRuntimeBinding, int) {
	var empty selectedWorkerRuntimeBinding
	client, err := s.newClusterNodeClient()
	if err != nil {
		return nil, empty, http.StatusServiceUnavailable
	}
	defer client.closeIdleConnections()
	b, err := s.resolvePublicFrontBinding(ctx, client, node)
	if err != nil || string(b.service.UID) != serviceUID {
		return nil, empty, http.StatusServiceUnavailable
	}
	var first, last workerFrontHealth
	health := b.proxyPath + "/healthz"
	if readPodObservation(ctx, client, health, &first, 4096, nil) != nil {
		return nil, empty, http.StatusServiceUnavailable
	}
	var pods corev1.PodList
	q := url.Values{"fieldSelector": {"spec.nodeName=" + node.ID}, "limit": {"256"}}
	if client.doJSON(ctx, http.MethodGet, b.base+"/pods?"+q.Encode(), &pods) != nil || pods.Continue != "" {
		return nil, empty, http.StatusServiceUnavailable
	}
	worker, status := physicalWorkerOwner(*b.pod, first, pods.Items, who, positive)
	if status != http.StatusOK {
		return nil, empty, status
	}
	if _, err := b.verifiedBackend(ctx, client, node); err != nil {
		return nil, empty, http.StatusServiceUnavailable
	}
	var rev kubernetesObjectRevision
	if client.getKubernetesObjectRevision(ctx, b.base+"/pods/"+url.PathEscape(worker.Name), "Pod", &rev) != nil || !rev.matches(worker) ||
		readPodObservation(ctx, client, health, &last, 4096, nil) != nil || first != last {
		return nil, empty, http.StatusServiceUnavailable
	}
	binding := selectedWorkerRuntimeBinding{
		ServiceUID: string(b.service.UID), ServiceGeneration: b.service.Annotations["transport.fugue.dev/generation"],
		ServiceDigest: b.service.Annotations["transport.fugue.dev/digest"], FrontUID: string(b.pod.UID),
		WorkerUID: string(worker.UID), ActiveSlot: first.ActiveSlot, Generation: first.Generation,
		Bundle: first.Bundle, Source: first.Source, Image: first.Image, Target: first.Target,
	}
	return worker, binding, http.StatusOK
}
