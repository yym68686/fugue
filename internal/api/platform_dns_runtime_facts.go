package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/dnsfacts"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
)

var errDNSRuntimeFacts = errors.New("current DNS backend observations unavailable")

type dnsRuntimeBackend struct {
	Namespace   string `json:"namespace"`
	PodName     string `json:"pod_name"`
	PodUID      string `json:"pod_uid"`
	ServiceName string `json:"service_name"`
	ServiceUID  string `json:"service_uid"`
}

type platformDNSRuntimeFactsResponse struct {
	Backend       dnsRuntimeBackend `json:"backend"`
	Snapshot      dnsfacts.Snapshot `json:"snapshot"`
	EvaluatedAt   time.Time         `json:"evaluated_at"`
	Ready         bool              `json:"ready"`
	ReadyProbeIDs []string          `json:"ready_probe_ids"`
}

type dnsFactSource struct {
	consumer       model.PlatformConsumerInstance
	claims         platformcontrol.PlatformComponentIdentityClaims
	parent         model.PlatformArtifact
	lookup         consumerArtifactLookup
	payload        platformDNSArtifactPayload
	trafficBinding *model.TrafficReleaseBinding
	group, routeID string
}

func (s *Server) handleGetPlatformDNSRuntimeFacts(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() || !p.HasScope("artifact.read") {
		httpx.WriteError(w, http.StatusForbidden, "platform admin with artifact.read scope required")
		return
	}
	node := r.PathValue("node_id")
	if len(validation.IsDNS1123Subdomain(node)) != 0 {
		httpx.WriteError(w, http.StatusBadRequest, "canonical Kubernetes node identity required")
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	result, err := s.readPlatformDNSRuntimeFacts(r.Context(), node)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, errDNSRuntimeFacts.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, result)
}

func (s *Server) currentDNSFactSource(node string) (dnsFactSource, error) {
	fail := func() (dnsFactSource, error) { return dnsFactSource{}, errDNSRuntimeFacts }
	consumers, err := s.store.ListPlatformConsumers(model.PlatformArtifactKindDNSAnswerBundle, "global")
	if err != nil {
		return fail()
	}
	var fact *model.PlatformConsumerInstance
	for i := range consumers {
		c := &consumers[i]
		if c.ConsumerID == model.PlatformConsumerComponentDNSServer+":"+node {
			if fact != nil {
				return fail()
			}
			fact = c
		}
	}
	if fact == nil || !fact.IdentityVerified || fact.NodeID != node || fact.Component != model.PlatformConsumerComponentDNSServer || !strings.HasPrefix(fact.CredentialID, "kubernetes:") {
		return fail()
	}
	claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: fact.CredentialID, Component: fact.Component, NodeID: node, ScopeKey: fact.ScopeKey, ArtifactKinds: fact.SupportedKinds}
	resolved, err := s.resolvePlatformConsumerAssignments(claims)
	if err != nil {
		return fail()
	}
	var source *dnsFactSource
	for _, item := range resolved {
		if item.Artifact.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || item.Release.ReleaseChannel == model.PlatformArtifactReleaseChannelShadow {
			continue
		}
		set, err := s.store.GetPlatformExpectedConsumerSet(item.Assignment.ExpectedConsumerSetID)
		if err != nil {
			return fail()
		}
		group, freshness := "", 0
		for _, expected := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
			if expected.ConsumerID == fact.ConsumerID {
				group, freshness = expected.Cohort, expected.HeartbeatFreshnessSeconds
			}
		}
		if group == "" || freshness <= 0 {
			return fail()
		}
		parent, release, found, err := s.selectTrafficRouteRelease(group)
		if err != nil {
			return fail()
		}
		if !found || release.ID != item.Release.ID {
			continue
		}
		now := time.Now().UTC()
		if source != nil || fact.ExpectedConsumerSetID != set.ID || fact.ReleaseSetID != parent.ID || fact.FencingToken != release.FencingToken || fact.GenerationSequence != item.Artifact.GenerationSequence || fact.LastHeartbeatAt.IsZero() || fact.LastHeartbeatAt.After(now) || !fact.LastHeartbeatAt.Add(time.Duration(freshness)*time.Second).After(now) {
			return fail()
		}
		payload, err := decodePlatformDNSArtifact(item.Artifact)
		if err != nil || payload.ReadinessPlan == nil || payload.Policy.DNSReadiness == nil {
			return fail()
		}
		viewFound := false
		for _, view := range payload.ConsumerViews {
			if view.NodeID == node {
				if view.EdgeGroupID != group {
					return fail()
				}
				viewFound = true
			}
		}
		if !viewFound {
			return fail()
		}
		projection, found, err := s.edgeRouteIntentSnapshotFromTrafficRelease(group)
		if err != nil || !found || projection.TrafficRelease == nil || projection.TrafficRelease.ReleaseSetID != parent.ID || projection.TrafficRelease.ReleaseID != release.ID || projection.TrafficRelease.FencingToken != release.FencingToken {
			return fail()
		}
		source = &dnsFactSource{consumer: *fact, claims: claims, parent: parent, lookup: item, payload: payload, group: group, routeID: projection.TrafficRelease.RouteArtifactID, trafficBinding: projection.TrafficRelease}
	}
	if source == nil {
		return fail()
	}
	return *source, nil
}

func (s *Server) readPlatformDNSRuntimeFacts(ctx context.Context, node string) (platformDNSRuntimeFactsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	source, err := s.currentDNSFactSource(node)
	if err != nil {
		return platformDNSRuntimeFactsResponse{}, errDNSRuntimeFacts
	}
	var response platformDNSRuntimeFactsResponse
	transportReady := false
	// Negative/expired observations are useful for diagnosis. Transport must be
	// uniquely selected and live, but a failing readiness probe must not hide it.
	status := s.inspectDNSBackend(ctx, source.claims, platformcontrol.PlatformConsumerHeartbeatEnvelope{}, func(ctx context.Context, client *clusterNodeClient, pod corev1.Pod, svc corev1.Service, ready bool) int {
		transportReady = ready
		port, ok := dnsBackendObservationPort(pod, svc)
		if !ok {
			return http.StatusServiceUnavailable
		}
		path := "/api/v1/namespaces/" + url.PathEscape(pod.Namespace) + "/pods/" + url.PathEscape(pod.Name+":"+strconv.Itoa(int(port))) + "/proxy/runtime-facts"
		if err := readDNSPodSnapshot(ctx, client, path, &response.Snapshot); err != nil {
			return http.StatusServiceUnavailable
		}
		response.Backend = dnsRuntimeBackend{Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), ServiceName: svc.Name, ServiceUID: string(svc.UID)}
		return http.StatusOK
	})
	if status != http.StatusOK {
		return platformDNSRuntimeFactsResponse{}, errDNSRuntimeFacts
	}
	// Re-resolve immutable inputs and current publication; do not reuse an
	// artifact-reader cache across the network request or renew heartbeat time.
	current, err := s.currentDNSFactSource(node)
	if err != nil || current.consumer.CredentialID != source.consumer.CredentialID || current.parent.ContentHash != source.parent.ContentHash || !reflect.DeepEqual(current.lookup.Assignment, source.lookup.Assignment) || current.group != source.group || ctx.Err() != nil {
		return platformDNSRuntimeFactsResponse{}, errDNSRuntimeFacts
	}
	response.EvaluatedAt = time.Now().UTC()
	response.ReadyProbeIDs, response.Ready, err = evaluateDNSRuntimeSnapshot(response.Snapshot, current, response.EvaluatedAt)
	response.Ready = response.Ready && transportReady
	if err != nil {
		return platformDNSRuntimeFactsResponse{}, errDNSRuntimeFacts
	}
	return response, nil
}

// Resolve the HTTP readiness port from the one container that owns all Service
// DNS target sockets. Names, container images and concrete port numbers are not
// inferred from projects or process environment.
func dnsBackendObservationPort(pod corev1.Pod, svc corev1.Service) (int32, bool) {
	var result int32
	matches := 0
	for _, container := range pod.Spec.Containers {
		owns := len(svc.Spec.Ports) > 0
		for _, required := range svc.Spec.Ports {
			count := 0
			for _, port := range container.Ports {
				if required.TargetPort.Type == intstr.Int && port.ContainerPort == required.TargetPort.IntVal && port.Protocol == required.Protocol {
					count++
				}
			}
			owns = owns && count == 1
		}
		if !owns {
			continue
		}
		matches++
		if container.ReadinessProbe == nil || container.ReadinessProbe.HTTPGet == nil {
			return 0, false
		}
		probe := container.ReadinessProbe.HTTPGet
		if probe.Host != "" || probe.Scheme != "" && probe.Scheme != corev1.URISchemeHTTP {
			return 0, false
		}
		if probe.Port.Type == intstr.Int {
			result = probe.Port.IntVal
		} else {
			count := 0
			for _, port := range container.Ports {
				if port.Name == probe.Port.StrVal && port.Protocol == corev1.ProtocolTCP {
					result = port.ContainerPort
					count++
				}
			}
			if count != 1 {
				return 0, false
			}
		}
	}
	return result, matches == 1 && result > 0 && result <= 65535
}

func readDNSPodSnapshot(ctx context.Context, client *clusterNodeClient, path string, out *dnsfacts.Snapshot) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, client.baseURL+path, nil)
	if err != nil {
		return errDNSRuntimeFacts
	}
	req.Header.Set("Authorization", "Bearer "+client.bearerToken)
	req.Header.Set("Accept", "application/json")
	reader := *client.client
	reader.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := reader.Do(req)
	if err != nil {
		return errDNSRuntimeFacts
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errDNSRuntimeFacts
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	if err != nil || len(raw) > 8<<20 {
		return errDNSRuntimeFacts
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(out) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return errDNSRuntimeFacts
	}
	return nil
}

func evaluateDNSRuntimeSnapshot(snapshot dnsfacts.Snapshot, source dnsFactSource, now time.Time) ([]string, bool, error) {
	fail := func() ([]string, bool, error) { return nil, false, errDNSRuntimeFacts }
	plan, policy := source.payload.ReadinessPlan, source.payload.Policy.DNSReadiness
	if plan == nil || policy == nil {
		return fail()
	}
	digest, err := platformconfig.Digest(plan)
	if err != nil || snapshot.Schema != dnsfacts.Schema || snapshot.NodeID != source.claims.NodeID || snapshot.EdgeGroupID != source.group || snapshot.ParentDigest != source.parent.ContentHash || snapshot.RouteArtifactID != source.routeID || snapshot.PlanDigest != digest || !reflect.DeepEqual(snapshot.Assignment, source.lookup.Assignment) || snapshot.ObservedAt.IsZero() || snapshot.EvaluatedAt.Before(snapshot.ObservedAt) || snapshot.EvaluatedAt.After(now) || snapshot.CheckpointValidUntil.IsZero() || snapshot.CheckpointValidUntil.After(snapshot.EvaluatedAt.Add(time.Duration(source.payload.Policy.MaxStaleSeconds)*time.Second)) || len(snapshot.Facts) > 4096 {
		return fail()
	}
	requirements := map[string]platformconfig.DNSReadinessProbe{}
	for _, p := range plan.Probes {
		requirements[p.ID] = p
	}
	seen, valid := map[string]bool{}, map[string]bool{}
	for _, fact := range snapshot.Facts {
		req, found := requirements[fact.ProbeID]
		if !found || seen[fact.ProbeID] {
			return fail()
		}
		seen[fact.ProbeID] = true
		if !fact.Ready {
			continue
		}
		p, b := fact.Proof, fact.Proof.TrafficRelease
		if p.Digest != req.RouteDigest || p.EdgeID != req.EdgeID || p.GroupID != req.EdgeGroupID || p.State != req.State || p.Version == "" || p.CheckedAt.IsZero() || p.CheckedAt.After(snapshot.EvaluatedAt) || p.ValidUntil.After(p.CheckedAt.Add(time.Duration(policy.FactFreshnessSeconds)*time.Second)) || b == nil || source.trafficBinding == nil || !reflect.DeepEqual(b, source.trafficBinding) {
			return fail()
		}
		valid[fact.ProbeID] = p.ValidUntil.After(now) && snapshot.CheckpointValidUntil.After(now)
	}
	ready := snapshot.Ready && snapshot.CheckpointValidUntil.After(now)
	for _, record := range plan.Records {
		edges, v4, v6 := map[string]bool{}, map[string]bool{}, map[string]bool{}
		for _, target := range record.Targets {
			pass := len(target.ProbeIDs) > 0
			for _, id := range target.ProbeIDs {
				pass = pass && valid[id]
			}
			if !pass {
				continue
			}
			edges[target.EdgeID] = true
			if target.Family == "A" {
				v4[target.EdgeID] = true
			} else if target.Family == "AAAA" {
				v6[target.EdgeID] = true
			}
		}
		ready = ready && len(edges) >= record.MinimumHealthyEdges && (!record.RequireDualStack || len(v4) >= record.MinimumHealthyEdges && len(v6) >= record.MinimumHealthyEdges)
	}
	ids := []string{}
	for id, pass := range valid {
		if pass {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, ready, nil
}
