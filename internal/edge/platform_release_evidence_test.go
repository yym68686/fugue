package edge

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestEdgePlatformErrorClassMatrix(t *testing.T) {
	t.Parallel()
	active := model.EdgeRouteBinding{Status: model.EdgeRouteStatusActive, DecisionID: "decision_1", RouteGeneration: "route_1"}
	tests := []struct {
		name     string
		observed edgeProxyObservation
		want     string
	}{
		{name: "route-unavailable", observed: edgeProxyObservation{StatusCode: 503, Route: model.EdgeRouteBinding{Status: model.EdgeRouteStatusUnavailable, StatusReason: "route disabled"}}, want: model.PlatformErrorClassRouteUnavailable},
		{name: "no-healthy", observed: edgeProxyObservation{StatusCode: 503, Route: model.EdgeRouteBinding{Status: model.EdgeRouteStatusUnavailable, StatusReason: "no healthy edge groups"}}, want: model.PlatformErrorClassNoHealthy},
		{name: "signature", observed: edgeProxyObservation{StatusCode: 503, Route: model.EdgeRouteBinding{Status: model.EdgeRouteStatusUnavailable, StatusReason: "bundle signature verification failed"}}, want: model.PlatformErrorClassBundleSignature},
		{name: "invariant", observed: edgeProxyObservation{StatusCode: 503, Route: model.EdgeRouteBinding{Status: model.EdgeRouteStatusUnavailable, StatusReason: "runtime invariant violation: image_missing"}}, want: model.PlatformErrorClassInvariant},
		{name: "origin-dns", observed: edgeProxyObservation{StatusCode: 502, Route: active, Proxied: true, OriginDNSError: "lookup failed"}, want: model.PlatformErrorClassOriginDNS},
		{name: "origin-connect", observed: edgeProxyObservation{StatusCode: 502, Route: active, Proxied: true, OriginConnectError: "connection refused"}, want: model.PlatformErrorClassOriginConnect},
		{name: "origin-unavailable", observed: edgeProxyObservation{StatusCode: 502, Route: active, Proxied: true, UpstreamError: "EOF"}, want: model.PlatformErrorClassOriginUnavailable},
		{name: "origin-connected-application-5xx", observed: edgeProxyObservation{StatusCode: 500, Route: active, Proxied: true, OriginGotConn: true}, want: model.PlatformErrorClassOriginConnectedApp5xx},
		{name: "evidence-unknown", observed: edgeProxyObservation{StatusCode: 500, Route: active}, want: model.PlatformErrorClassEvidenceUnknown},
		{name: "latency", observed: edgeProxyObservation{StatusCode: 200, Route: active, Proxied: true, OriginGotConn: true, Duration: 6 * time.Second, TTFB: 6 * time.Second}, want: model.PlatformErrorClassLatencyRegression},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := edgePlatformErrorClass(testCase.observed); got != testCase.want {
				t.Fatalf("class=%q want=%q", got, testCase.want)
			}
		})
	}
}

func TestPlatformReleaseMetricIsLowCardinalityAndTyped(t *testing.T) {
	t.Parallel()
	service := NewService(config.EdgeConfig{EdgeGroupID: "group-de", EdgeSlot: "b", EdgeReleaseEpoch: "release-1"}, nil)
	service.recordProxyObservation(edgeProxyObservation{
		StatusCode:    http.StatusInternalServerError,
		Host:          "customer.example.test",
		Method:        http.MethodPost,
		Proxied:       true,
		OriginGotConn: true,
		Route: model.EdgeRouteBinding{
			Status: model.EdgeRouteStatusActive, EdgeGroupID: "group-de", TenantID: "tenant-secret", AppID: "app-secret",
		},
	})
	recorder := httptest.NewRecorder()
	service.handleMetrics(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	var metricLine string
	for _, line := range strings.Split(recorder.Body.String(), "\n") {
		if strings.HasPrefix(line, "fugue_edge_platform_release_requests_total{") {
			metricLine = line
			break
		}
	}
	if metricLine == "" || !strings.Contains(metricLine, `group="group-de",slot="b",release_epoch="release-1",status_class="5xx",platform_error_class="origin_connected_application_5xx"`) {
		t.Fatalf("typed platform release metric is missing: %s", metricLine)
	}
	for _, forbidden := range []string{"tenant", "project", "app=", "domain", "hostname", "path", "method", "client", "geo", "asn", "bundle", "reason"} {
		if strings.Contains(strings.ToLower(metricLine), forbidden) {
			t.Fatalf("platform release metric contains high-cardinality label %q: %s", forbidden, metricLine)
		}
	}
}

func TestEdgeRequestFactFailsClosedWhenDecisionLinkIsMissing(t *testing.T) {
	t.Parallel()
	fields := edgeProxyObservationRequestFactFields(edgeProxyObservation{
		StatusCode: http.StatusServiceUnavailable,
		Route: model.EdgeRouteBinding{
			Status:       model.EdgeRouteStatusUnavailable,
			StatusReason: "runtime invariant violation: image_missing",
		},
	}, config.EdgeConfig{EdgeGroupID: "group-de", EdgeSlot: "b", EdgeReleaseEpoch: "release-1"}, "")
	if fields["platform_error_class"] != model.PlatformErrorClassDecisionMissing {
		t.Fatalf("missing decision link class=%v", fields["platform_error_class"])
	}
	if fields["status_class"] != "5xx" || fields["edge_group_id"] != "group-de" || fields["slot"] != "b" || fields["release_epoch"] != "release-1" {
		t.Fatalf("typed low-cardinality fact is incomplete: %+v", fields)
	}
}
