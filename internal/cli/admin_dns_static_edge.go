package cli

import (
	"context"
	"fmt"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

type dnsStaticEdgeReader interface {
	GetApp(string) (model.App, error)
	GetStaticEdgeRegistration(string) (model.StaticEdgeRegistration, error)
}

// Desired configuration only authorizes an endpoint for a strict direct probe.
// It is never evidence of runtime readiness or a completed DNS publication.
func dnsAnswerStaticEdgeProbes(client dnsStaticEdgeReader, hostname string, explain model.RouteExplainResponse, managed map[string][]string) (map[string]platformconfig.DNSReadinessProbe, error) {
	out := map[string]platformconfig.DNSReadinessProbe{}
	hostname = normalizeDNSHostname(hostname)
	routes := append([]model.EdgeRouteBinding{}, explain.Routes...)
	if explain.Route != nil {
		routes = append(routes, *explain.Route)
	}
	var appID, tenantID string
	for _, route := range routes {
		if normalizeDNSHostname(route.Hostname) != hostname || model.NormalizeAppRoutePathPrefix(route.PathPrefix) != "/" || route.AppID == "" || route.TenantID == "" {
			return out, nil
		}
		if route.Status != model.EdgeRouteStatusActive || route.RoutePolicy != model.EdgeRoutePolicyEnabled {
			return nil, fmt.Errorf("app route is not active and enabled")
		}
		if appID != "" && (appID != route.AppID || tenantID != route.TenantID) {
			return out, fmt.Errorf("hostname has multiple route owners")
		}
		appID, tenantID = route.AppID, route.TenantID
	}
	if appID == "" {
		return out, nil
	}
	app, err := client.GetApp(appID)
	if err != nil {
		return nil, err
	}
	if app.ID != appID || app.TenantID != tenantID {
		return nil, fmt.Errorf("app route ownership differs from route explanation")
	}
	// A platform/custom domain may alias an app's primary route. Without an
	// explicit static DNS configuration there is no endpoint to authorize, and
	// differing primary hostnames are not a static-route ownership failure.
	if app.Route == nil || app.Route.DNSRouting == nil {
		return out, nil
	}
	if normalizeDNSHostname(app.Route.Hostname) != hostname || model.NormalizeAppRoutePathPrefix(app.Route.PathPrefix) != "/" {
		return nil, fmt.Errorf("app route ownership differs from route explanation")
	}
	if err := model.ValidateAppStaticEdgeRouting(app.Route.DNSRouting); err != nil {
		return nil, err
	}
	for _, candidate := range app.Route.DNSRouting.Candidates {
		registration, err := client.GetStaticEdgeRegistration(candidate.RegistrationID)
		if err != nil {
			return nil, err
		}
		if registration.ID != candidate.RegistrationID || registration.Status != model.StaticEdgeStatusReady || registration.TenantID != tenantID || registration.EdgeID != candidate.EdgeID {
			return nil, fmt.Errorf("static edge registration %s is not ready or differs from configured ownership", candidate.RegistrationID)
		}
		for _, address := range candidate.Addresses {
			if len(managed[address]) != 0 {
				return nil, fmt.Errorf("static endpoint %s overlaps managed edge inventory", address)
			}
			out[address] = platformconfig.DNSReadinessProbe{
				Hostname: hostname, Address: address, Path: candidate.ProbePath,
				EdgeID: candidate.EdgeID, EdgeGroupID: platformconfig.StaticEdgeDNSGroup(candidate.EdgeID),
				RouteDigest: candidate.RouteDigest, StaticEdge: &platformconfig.StaticEdgeDNSProbe{StatusCode: candidate.StatusCode},
			}
		}
	}
	return out, nil
}

func probeStaticDNSAnswerHost(ctx context.Context, requirement platformconfig.DNSReadinessProbe) dnsAnswerCheckHostProbe {
	out := dnsAnswerCheckHostProbe{Hostname: requirement.Hostname, IP: requirement.Address, StaticEdgeID: requirement.EdgeID, RouteDigest: requirement.RouteDigest}
	if _, err := routeprobe.ProbeStatic(ctx, requirement, 5*time.Second, 30*time.Second); err != nil {
		out.Message = err.Error()
		return out
	}
	out.TLSReady, out.Pass, out.StatusCode = true, true, requirement.StaticEdge.StatusCode
	return out
}

func probeDNSAnswerHostsWithStatic(hostname string, nodes []dnsAnswerCheckNode, requirements map[string]platformconfig.DNSReadinessProbe, probeStatic func(context.Context, platformconfig.DNSReadinessProbe) dnsAnswerCheckHostProbe) []dnsAnswerCheckHostProbe {
	return probeDNSAnswerHostsUsing(hostname, nodes, func(ctx context.Context, hostname, address string) dnsAnswerCheckHostProbe {
		if requirement, ok := requirements[address]; ok {
			return probeStatic(ctx, requirement)
		}
		return probeDNSAnswerHost(ctx, hostname, address)
	})
}
