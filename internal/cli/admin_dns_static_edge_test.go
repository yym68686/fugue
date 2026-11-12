package cli

import (
	"context"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

type dnsStaticEdgeFixture struct {
	app          model.App
	registration model.StaticEdgeRegistration
}

func (f dnsStaticEdgeFixture) GetApp(string) (model.App, error) { return f.app, nil }
func (f dnsStaticEdgeFixture) GetStaticEdgeRegistration(string) (model.StaticEdgeRegistration, error) {
	return f.registration, nil
}

func dnsStaticEdgeTestFixture() (dnsStaticEdgeFixture, model.RouteExplainResponse) {
	candidate := model.StaticEdgeDNSCandidate{RegistrationID: "registration-a", EdgeID: "static-a", Addresses: []string{"8.8.4.4"}, ProbePath: "/health", RouteDigest: "sha256:" + strings.Repeat("a", 64), StatusCode: 204}
	f := dnsStaticEdgeFixture{
		app: model.App{ID: "app-a", TenantID: "tenant-a", ProjectID: "project-a", Route: &model.AppRoute{Hostname: "app.example.test", PathPrefix: "/", DNSRouting: &model.AppStaticEdgeRouting{
			Version: model.StaticEdgeDNSVersion, Candidates: []model.StaticEdgeDNSCandidate{candidate},
			Availability: model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"static-a", "managed-a"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60},
		}}},
		registration: model.StaticEdgeRegistration{ID: "registration-a", TenantID: "tenant-a", ProjectID: "project-b", EdgeID: "static-a", Status: model.StaticEdgeStatusReady},
	}
	return f, model.RouteExplainResponse{Route: &model.EdgeRouteBinding{Hostname: "app.example.test", AppID: "app-a", TenantID: "tenant-a", PathPrefix: "/", Status: model.EdgeRouteStatusActive, RoutePolicy: model.EdgeRoutePolicyEnabled}}
}

func TestDNSAnswerStaticEdgeRequiresExplicitOwnedReadyRegistration(t *testing.T) {
	for _, name := range []string{"valid shared registration", "revoked", "foreign tenant", "wrong edge", "wrong app", "changed hostname", "overlap", "ambiguous owner", "disabled route", "unconfigured"} {
		t.Run(name, func(t *testing.T) {
			f, explain := dnsStaticEdgeTestFixture()
			managed := map[string][]string{}
			switch name {
			case "revoked":
				f.registration.Status = model.StaticEdgeStatusRevoked
			case "foreign tenant":
				f.registration.TenantID = "tenant-b"
			case "wrong edge":
				f.registration.EdgeID = "static-b"
			case "wrong app":
				f.app.ID = "app-b"
			case "changed hostname":
				f.app.Route.Hostname = "other.example.test"
			case "overlap":
				managed["8.8.4.4"] = []string{"managed-group"}
			case "ambiguous owner":
				explain.Routes = []model.EdgeRouteBinding{{Hostname: "app.example.test", AppID: "app-b", TenantID: "tenant-a", PathPrefix: "/"}}
			case "disabled route":
				explain.Route.RoutePolicy = "disabled"
			case "unconfigured":
				f.app.Route.DNSRouting = nil
			}
			probes, err := dnsAnswerStaticEdgeProbes(f, "app.example.test", explain, managed)
			if name == "valid shared registration" {
				probe, ok := probes["8.8.4.4"]
				if err != nil || !ok || len(probes) != 1 || probe.Path != "/health" || probe.StaticEdge.StatusCode != 204 || probe.RouteDigest != f.app.Route.DNSRouting.Candidates[0].RouteDigest {
					t.Fatal("configured endpoint lost strict probe authority", probes, err)
				}
				if _, ok := probes["8.8.8.8"]; ok {
					t.Fatal("unconfigured IP authorized")
				}
			} else if len(probes) != 0 || (name != "unconfigured" && err == nil) {
				t.Fatal("invalid registration or ownership authorized", probes, err)
			}
		})
	}
}

func TestDNSAnswerStaticEdgeProbeFailureCannotUseOrdinaryHostProbe(t *testing.T) {
	f, explain := dnsStaticEdgeTestFixture()
	requirements, err := dnsAnswerStaticEdgeProbes(f, "app.example.test", explain, nil)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	probes := probeDNSAnswerHostsWithStatic("app.example.test", []dnsAnswerCheckNode{{Answers: []string{"8.8.4.4"}}, {Answers: []string{"8.8.4.4"}}}, requirements, func(_ context.Context, p platformconfig.DNSReadinessProbe) dnsAnswerCheckHostProbe {
		calls++
		return dnsAnswerCheckHostProbe{IP: p.Address, StaticEdgeID: p.EdgeID, Message: "nonce mismatch"}
	})
	if calls != 1 || len(probes) != 1 || probes[0].Pass || probes[0].Message != "nonce mismatch" {
		t.Fatal("strict probe failure was discarded or endpoint probed repeatedly", probes, calls)
	}
}

func TestDNSAnswerStaticEdgeUnconfiguredAliasDoesNotAuthorizeEndpoints(t *testing.T) {
	for _, scenario := range []string{"different primary", "no primary route", "foreign app", "foreign tenant"} {
		t.Run(scenario, func(t *testing.T) {
			f, explain := dnsStaticEdgeTestFixture()
			f.app.Route.Hostname = "primary.example.test"
			f.app.Route.DNSRouting = nil
			switch scenario {
			case "no primary route":
				f.app.Route = nil
			case "foreign app":
				f.app.ID = "foreign"
			case "foreign tenant":
				f.app.TenantID = "foreign"
			}
			probes, err := dnsAnswerStaticEdgeProbes(f, "app.example.test", explain, nil)
			foreign := scenario == "foreign app" || scenario == "foreign tenant"
			if len(probes) != 0 || (err != nil) != foreign {
				t.Fatal("unconfigured alias changed static endpoint authority", probes, err)
			}
		})
	}
}
