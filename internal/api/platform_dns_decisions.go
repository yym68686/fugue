package api

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

type platformDNSDecisionResponse struct {
	Backend     dnsRuntimeBackend             `json:"backend"`
	Snapshot    dnsserver.DNSDecisionSnapshot `json:"snapshot"`
	EvaluatedAt time.Time                     `json:"evaluated_at"`
}

func (s *Server) handleGetPlatformDNSDecisions(writer http.ResponseWriter, request *http.Request) {
	principal := mustPrincipal(request)
	if !principal.IsPlatformAdmin() || !principal.HasScope("artifact.read") {
		httpx.WriteError(writer, http.StatusForbidden, "platform admin with artifact.read scope required")
		return
	}
	node := request.PathValue("node_id")
	query := request.URL.Query()
	limit := 5
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			httpx.WriteError(writer, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = parsed
	}
	if len(validation.IsDNS1123Subdomain(node)) != 0 || dnsserver.ValidateDNSDecisionFilter(query.Get("hostname"), query.Get("decision_id"), limit) != nil {
		httpx.WriteError(writer, http.StatusBadRequest, "invalid DNS decision filter or node")
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
	defer cancel()
	claims, err := s.dnsDecisionBackendIdentity(ctx, node)
	if err != nil {
		httpx.WriteError(writer, http.StatusServiceUnavailable, "selected DNS decision backend unavailable")
		return
	}
	query = url.Values{"limit": {strconv.Itoa(limit)}, "hostname": {query.Get("hostname")}, "decision_id": {query.Get("decision_id")}}
	var response platformDNSDecisionResponse
	status := s.inspectDNSBackend(ctx, claims, platformcontrol.PlatformConsumerHeartbeatEnvelope{}, func(ctx context.Context, client *clusterNodeClient, pod corev1.Pod, service corev1.Service, ready bool) int {
		port, ok := dnsBackendObservationPort(pod, service)
		if !ok {
			return http.StatusServiceUnavailable
		}
		path := "/api/v1/namespaces/" + url.PathEscape(pod.Namespace) + "/pods/" + url.PathEscape(pod.Name+":"+strconv.Itoa(int(port))) + "/proxy/decisions?" + query.Encode()
		if readDNSPodObservation(ctx, client, path, &response.Snapshot, 8<<20) != nil || response.Snapshot.NodeID != node || response.Snapshot.ProcessID == "" || len(response.Snapshot.Receipts) > limit {
			return http.StatusServiceUnavailable
		}
		for _, receipt := range response.Snapshot.Receipts {
			if receipt.NodeID != node || receipt.Schema != dnsserver.DNSDecisionSchema || len(receipt.ReplayInput) > dnsserver.DNSDecisionMaxBytes {
				return http.StatusServiceUnavailable
			}
		}
		response.Backend = dnsRuntimeBackend{Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), ServiceName: service.Name, ServiceUID: string(service.UID)}
		return http.StatusOK
	})
	if status != http.StatusOK {
		httpx.WriteError(writer, http.StatusServiceUnavailable, "selected DNS decision backend changed or unavailable")
		return
	}
	response.EvaluatedAt = time.Now().UTC()
	writer.Header().Set("Cache-Control", "private, no-store")
	httpx.WriteJSON(writer, http.StatusOK, response)
}

func (s *Server) dnsDecisionBackendIdentity(ctx context.Context, node string) (platformcontrol.PlatformComponentIdentityClaims, error) {
	consumers, err := s.store.ListPlatformAuthorityConsumers(model.PlatformArtifactKindDNSAnswerBundle, node)
	if err != nil {
		return platformcontrol.PlatformComponentIdentityClaims{}, err
	}
	var selected *platformcontrol.PlatformComponentIdentityClaims
	for _, consumer := range consumers {
		if !consumer.IdentityVerified || consumer.NodeID != node || consumer.Component != model.PlatformConsumerComponentDNSServer || !strings.HasPrefix(consumer.CredentialID, "kubernetes:") {
			continue
		}
		set, err := s.store.GetPlatformExpectedConsumerSet(consumer.ExpectedConsumerSetID)
		if err != nil {
			return platformcontrol.PlatformComponentIdentityClaims{}, err
		}
		for _, member := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
			if member.ConsumerID != consumer.ConsumerID {
				continue
			}
			claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: consumer.CredentialID, Component: consumer.Component, NodeID: node, ScopeKey: consumer.ScopeKey, ArtifactKinds: consumer.SupportedKinds, AuthorityID: member.AuthorityID}
			if !platformcontrol.ExpectedConsumerIdentityMatches(member, claims) || member.HeartbeatFreshnessSeconds <= 0 || !consumer.LastHeartbeatAt.Add(time.Duration(member.HeartbeatFreshnessSeconds)*time.Second).After(time.Now().UTC()) {
				continue
			}
			status := s.inspectDNSBackend(ctx, claims, platformcontrol.PlatformConsumerHeartbeatEnvelope{}, nil)
			if status == http.StatusForbidden || status == http.StatusConflict {
				continue
			}
			if status != http.StatusOK || selected != nil {
				return platformcontrol.PlatformComponentIdentityClaims{}, errors.New("DNS backend unavailable or ambiguous")
			}
			selected = &claims
		}
	}
	if selected == nil {
		return platformcontrol.PlatformComponentIdentityClaims{}, errors.New("DNS backend identity unavailable")
	}
	return *selected, nil
}
