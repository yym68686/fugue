package trafficepoch

import (
	"strings"
	"time"

	"fugue/internal/model"
)

const DefaultEdgeGroupID = "edge-group-default"

type RouteUpstreamFact struct {
	Kind         string
	Scope        string
	URL          string
	Status       string
	StatusReason string
}

type RouteBindingInput struct {
	Hostname                    string
	PathPrefix                  string
	RouteKind                   string
	AppID                       string
	TenantID                    string
	RuntimeID                   string
	RuntimeType                 string
	RuntimeEdgeGroupID          string
	RuntimeClusterNode          string
	Status                      string
	StatusReason                string
	Upstream                    RouteUpstreamFact
	ServicePort                 int
	TLSPolicy                   string
	CachePolicyID               string
	DeploymentGeneration        string
	RequestBodyPoliciesEnvelope string
	CreatedAt                   time.Time
	UpdatedAt                   time.Time
}

func CompileRouteBinding(input RouteBindingInput) model.EdgeRouteBinding {
	edgeGroupID := strings.TrimSpace(input.RuntimeEdgeGroupID)
	fallbackEdgeGroupID := ""
	if edgeGroupID != "" && edgeGroupID != DefaultEdgeGroupID {
		fallbackEdgeGroupID = DefaultEdgeGroupID
	}
	status, statusReason := input.Status, input.StatusReason
	if status == model.EdgeRouteStatusActive && input.Upstream.Status != model.EdgeRouteStatusActive {
		status = input.Upstream.Status
		statusReason = input.Upstream.StatusReason
	}

	binding := model.EdgeRouteBinding{
		Hostname:             NormalizeHostname(input.Hostname),
		PathPrefix:           model.NormalizeAppRoutePathPrefix(input.PathPrefix),
		RouteKind:            input.RouteKind,
		AppID:                input.AppID,
		TenantID:             input.TenantID,
		RuntimeID:            input.RuntimeID,
		RuntimeType:          strings.TrimSpace(input.RuntimeType),
		RuntimeEdgeGroupID:   edgeGroupID,
		RuntimeEdgeGroup:     edgeGroupID,
		RuntimeClusterNode:   strings.TrimSpace(input.RuntimeClusterNode),
		EdgeGroupID:          edgeGroupID,
		SelectedEdgeGroup:    edgeGroupID,
		FallbackEdgeGroupID:  fallbackEdgeGroupID,
		RoutePolicy:          model.EdgeRoutePolicyRouteAOnly,
		UpstreamKind:         input.Upstream.Kind,
		UpstreamScope:        input.Upstream.Scope,
		ServicePort:          input.ServicePort,
		TLSPolicy:            input.TLSPolicy,
		CachePolicyID:        input.CachePolicyID,
		DeploymentGeneration: input.DeploymentGeneration,
		Streaming:            true,
		Status:               status,
		StatusReason:         statusReason,
		CreatedAt:            input.CreatedAt,
		UpdatedAt:            input.UpdatedAt,
	}
	requestBodyPolicies, err := model.ParseEdgeRequestBodyPolicies(input.RequestBodyPoliciesEnvelope)
	if err != nil {
		binding.Status = model.EdgeRouteStatusUnavailable
		binding.StatusReason = "invalid app edge request body policy"
	} else {
		binding.RequestBodyPolicies = requestBodyPolicies
	}
	if binding.CachePolicyID != "" {
		binding.CacheNamespace = cacheNamespace(input.AppID, input.DeploymentGeneration)
	}
	if binding.Status == model.EdgeRouteStatusActive {
		binding.UpstreamURL = input.Upstream.URL
	}
	binding.RouteGeneration = EdgeRouteGeneration(binding)
	return binding
}

func NormalizeHostname(raw string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(raw)), ".")
}

func cacheNamespace(appID, deploymentGeneration string) string {
	appID = strings.TrimSpace(appID)
	deploymentGeneration = strings.TrimSpace(deploymentGeneration)
	switch {
	case appID != "" && deploymentGeneration != "":
		return appID + "_" + deploymentGeneration
	case appID != "":
		return appID
	default:
		return deploymentGeneration
	}
}
