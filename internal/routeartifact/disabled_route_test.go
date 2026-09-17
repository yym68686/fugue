package routeartifact

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDisabledRouteRetainsPolicyAndOriginFactsWithoutServing(t *testing.T) {
	for _, scenario := range []struct {
		name, observed, reason, intentStatus, intentReason, policy, wantStatus, wantReason, wantPolicy string
	}{
		{name: "scaled to zero", observed: "disabled", reason: "desired replicas is 0", policy: model.EdgeRoutePolicyEnabled, wantStatus: "disabled", wantReason: "desired replicas is 0", wantPolicy: model.EdgeRoutePolicyEnabled},
		{name: "runtime absent", observed: "unavailable", reason: "runtime object unavailable", policy: model.EdgeRoutePolicyEnabled, wantStatus: "unavailable", wantReason: "runtime object unavailable", wantPolicy: model.EdgeRoutePolicyEnabled},
		{name: "recovered origin", observed: "active", policy: model.EdgeRoutePolicyCanary, wantStatus: "disabled", wantPolicy: model.EdgeRoutePolicyCanary},
		{name: "explicit maintenance", observed: "unavailable", reason: "endpoint unavailable", intentStatus: "disabled", intentReason: "maintenance", policy: model.EdgeRoutePolicyEnabled, wantStatus: "disabled", wantReason: "maintenance", wantPolicy: model.EdgeRoutePolicyEnabled},
		{name: "legacy missing policy", observed: "active", wantStatus: "disabled", wantPolicy: model.EdgeRoutePolicyRouteAOnly},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			request := platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "disabled-intent", Routes: []platformconfig.RouteIntent{{
				Hostname: "disabled.example.test", UpstreamURL: "http://origin", Enabled: false, RuntimeID: "runtime", OriginRef: "origin",
				RoutePolicy: scenario.policy, Status: scenario.intentStatus, StatusReason: scenario.intentReason,
				Upstreams: []platformconfig.UpstreamIntent{{Role: "stable", Weight: 100, UpstreamURL: "http://weighted-origin"}},
			}}}, Policy: platformconfig.PolicySnapshot{Generation: "disabled-policy"}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{
				CapturedAt: &now, Origins: []platformconfig.OriginObservation{{Ref: "origin", RuntimeID: "runtime", ObservedAt: now,
					Status: scenario.observed, StatusReason: scenario.reason}},
			}}
			before, _ := json.Marshal(request)
			compiled, err := platformconfig.Compile(request)
			if err != nil {
				t.Fatal(err)
			}
			projected, err := Project(compiled.RouteArtifact)
			if err != nil || len(projected.Routes) != 1 {
				t.Fatal("projection failed", err)
			}
			route := projected.Routes[0]
			if route.OriginStatus != scenario.wantStatus || route.OriginStatusReason != scenario.wantReason || route.RoutePolicy != scenario.wantPolicy {
				t.Fatalf("disabled intent/policy/fact semantics lost: %+v", route)
			}
			if route.OriginStatus == model.EdgeRouteStatusActive || route.UpstreamURL != "" || len(route.Upstreams) != 0 {
				t.Fatal("disabled route regained a serving upstream")
			}
			after, _ := json.Marshal(request)
			if string(before) != string(after) {
				t.Fatal("compilation mutated intent or facts")
			}
			request.CreatedAt = now.Add(time.Hour)
			replay, err := platformconfig.Compile(request)
			if err != nil || !reflect.DeepEqual(compiled.RouteArtifact.Content, replay.RouteArtifact.Content) {
				t.Fatal("fixed input replay changed with wall clock", err)
			}
		})
	}
}
