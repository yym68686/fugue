package edge

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/tcpdiag"
)

func originProbeService(t *testing.T) *Service {
	t.Helper()
	service := NewService(config.EdgeConfig{EdgeID: "edge-a", EdgeGroupID: "group-a", CaddyEnabled: true,
		OriginNetworkProbeInterval: 30 * time.Second, OriginNetworkProbeHostnames: []string{"app.example.test"}}, nil)
	route := networkTestObservation().Route
	route.EdgeGroupID, route.Status, route.RoutePolicy = "group-a", model.EdgeRouteStatusActive, model.EdgeRoutePolicyEnabled
	bundle := model.EdgeRouteBundle{Version: "bundle-one", ValidUntil: time.Now().Add(time.Hour), Routes: []model.EdgeRouteBinding{route}}
	service.recordSyncSuccess(bundle, "", time.Now(), false)
	service.mu.Lock()
	service.snapshot.Healthy, service.snapshot.CaddyAppliedVersion = true, bundle.Version
	service.mu.Unlock()
	return service
}

type originProbeConn struct {
	net.Conn
	writes atomic.Int32
	closed atomic.Bool
}

func (connection *originProbeConn) Write(payload []byte) (int, error) {
	connection.writes.Add(1)
	return len(payload), nil
}

func (connection *originProbeConn) Close() error {
	connection.closed.Store(true)
	return nil
}

func (connection *originProbeConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("10.43.0.20"), Port: 3000}
}

func TestOriginProbeConnectsWithoutApplicationBytes(t *testing.T) {
	service := originProbeService(t)
	connection := &originProbeConn{}
	attempts := 0
	dial := func(ctx context.Context, network, target string) (net.Conn, error) {
		attempts++
		deadline, exists := ctx.Deadline()
		if !exists || time.Until(deadline) > 2*time.Second || network != "tcp" || target != "app.tenant.svc.cluster.local:3000" {
			t.Fatal("unbounded or foreign target", network, target, deadline)
		}
		return connection, nil
	}
	inspect := func(net.Conn) tcpdiag.Snapshot { return tcpdiag.Snapshot{Available: true, RTTUsec: 22000} }
	now := time.Now().UTC()
	service.probeOriginNetworkOnce(context.Background(), now, dial, inspect)
	service.probeOriginNetworkOnce(context.Background(), now.Add(time.Second), dial, inspect)
	samples := service.originNetworkSamples()
	if attempts != 1 || len(samples) != 1 || samples[0].Source != "service_endpoint_tcp_probe_v1" ||
		samples[0].ServiceConnectFailed == nil || *samples[0].ServiceConnectFailed || samples[0].ServiceRTTMS == nil || *samples[0].ServiceRTTMS != 22 ||
		connection.writes.Load() != 0 || !connection.closed.Load() {
		t.Fatal("probe sent business bytes, exceeded budget or lost raw outcome", attempts, samples)
	}
	if samples[0].BundleVersion != "bundle-one" || samples[0].TrafficClass != "streaming" || model.ValidateEdgeNetworkSample(samples[0]) != nil {
		t.Fatal("immutable route identity lost", samples)
	}
}

func TestOriginProbeFailuresAndUnavailableKernelRemainUnknown(t *testing.T) {
	for _, failed := range []bool{true, false} {
		service := originProbeService(t)
		dial := func(context.Context, string, string) (net.Conn, error) {
			if failed {
				return nil, errors.New("connection failed")
			}
			return &originProbeConn{}, nil
		}
		service.probeOriginNetworkOnce(context.Background(), time.Now(), dial, func(net.Conn) tcpdiag.Snapshot { return tcpdiag.Snapshot{} })
		samples := service.originNetworkSamples()
		if len(samples) != 1 || samples[0].ServiceRTTMS != nil || samples[0].ServiceConnectFailed == nil || *samples[0].ServiceConnectFailed != failed {
			t.Fatal("missing RTT fabricated zero or failure disappeared", samples)
		}
	}
}

func TestOriginProbeRejectsUnservedAndUnconfiguredState(t *testing.T) {
	for _, scenario := range []string{"disabled", "small_interval", "large_interval", "no_hosts", "other_host", "candidate", "unapplied", "unhealthy", "expired", "cancelled", "busy"} {
		t.Run(scenario, func(t *testing.T) {
			service := originProbeService(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch scenario {
			case "disabled":
				service.Config.OriginNetworkProbeInterval = 0
			case "small_interval":
				service.Config.OriginNetworkProbeInterval = time.Second
			case "large_interval":
				service.Config.OriginNetworkProbeInterval = time.Hour
			case "no_hosts":
				service.Config.OriginNetworkProbeHostnames = nil
			case "other_host":
				service.Config.OriginNetworkProbeHostnames = []string{"other.example.test"}
			case "candidate":
				service.currentRouteIndex().publication.Candidate = true
			case "unapplied":
				service.snapshot.CaddyAppliedVersion = "other-bundle"
			case "unhealthy":
				service.snapshot.Healthy = false
			case "expired":
				service.currentRouteIndex().validUntil = time.Now().Add(-time.Minute)
			case "cancelled":
				cancel()
			case "busy":
				service.originNetworkProbeMu.Lock()
				defer service.originNetworkProbeMu.Unlock()
			}
			service.probeOriginNetworkOnce(ctx, time.Now(), func(context.Context, string, string) (net.Conn, error) {
				t.Fatal("unserved state contacted origin")
				return nil, nil
			}, func(net.Conn) tcpdiag.Snapshot { return tcpdiag.Snapshot{} })
			if len(service.originNetworkSamples()) != 0 {
				t.Fatal("unserved evidence recorded")
			}
		})
	}
}

func TestOriginProbeDropsResultIfServingIndexChanged(t *testing.T) {
	service := originProbeService(t)
	connection := &originProbeConn{}
	service.probeOriginNetworkOnce(context.Background(), time.Now(), func(context.Context, string, string) (net.Conn, error) {
		service.routeIndex.Store(&edgeRouteIndex{})
		return connection, nil
	}, func(net.Conn) tcpdiag.Snapshot { return tcpdiag.Snapshot{Available: true, RTTUsec: 1} })
	if len(service.originNetworkSamples()) != 0 || !connection.closed.Load() {
		t.Fatal("changed serving artifact inherited an old measurement")
	}
}

func TestOriginProbeTargetRejectsExternalWeightedExcludedAndForeignRoutes(t *testing.T) {
	base := networkTestObservation().Route
	base.EdgeGroupID, base.Status, base.RoutePolicy = "group-a", model.EdgeRouteStatusActive, model.EdgeRoutePolicyEnabled
	for _, edit := range []func(*model.EdgeRouteBinding){
		func(route *model.EdgeRouteBinding) { route.UpstreamURL = "http://external.example.test" },
		func(route *model.EdgeRouteBinding) {
			route.UpstreamURL = "http://user:secret@app.tenant.svc.cluster.local"
		},
		func(route *model.EdgeRouteBinding) { route.UpstreamURL = "http://app.tenant.svc.cluster.local:0" },
		func(route *model.EdgeRouteBinding) { route.UpstreamScope = "mesh" },
		func(route *model.EdgeRouteBinding) {
			route.Upstreams = []model.EdgeRouteUpstream{{UpstreamURL: "http://canary"}}
		},
		func(route *model.EdgeRouteBinding) { route.ExcludedEdgeIDs = []string{"edge-a"} },
		func(route *model.EdgeRouteBinding) { route.ExcludedEdgeGroupIDs = []string{"group-a"} },
		func(route *model.EdgeRouteBinding) { route.Status = "disabled" },
		func(route *model.EdgeRouteBinding) { route.EdgeGroupID = "foreign" },
	} {
		route := base
		edit(&route)
		if sample, ok := originNetworkProbeTarget(route, "bundle-one", "edge-a", "group-a", time.Now()); ok {
			t.Fatal("unproved route admitted", sample)
		}
	}
	if sample, ok := originNetworkProbeTarget(base, "bundle-one", "edge-a", "group-a", time.Now()); !ok || !strings.HasPrefix(sample.RouteDigest, "sha256:") {
		t.Fatal("valid target lost", sample)
	}
}
