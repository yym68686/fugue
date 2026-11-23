package platformconfig

import (
	"encoding/json"
	"slices"
	"testing"
)

func TestStagedHostedAliasPreservesOtherDependenciesAndCustody(t *testing.T) {
	for _, scenario := range []string{"old-implicit", "old-explicit", "changed-ttl", "policy", "tenant", "renamed-alias", "foreign-root", "invalid-current", "deleted-alias", "missing-other-host", "new-alias"} {
		t.Run(scenario, func(t *testing.T) {
			const h = "a.example.test"
			const other = "b.example.test"
			const alias = "edge.example.test"
			route := func(host, path, app string) RouteIntent {
				return RouteIntent{Hostname: host, PathPrefix: path, AppID: app, TenantID: "tenant-a"}
			}
			bind := func(host, path, app string) DNSRouteBinding {
				return DNSRouteBinding{Hostname: host, PathPrefix: path, AppID: app}
			}
			hostRoute, nearby := route(h, "/", "old"), route(other, "/", "old")
			record := DNSIntent{Hostname: alias, Type: "FUGUE_ROUTE", Values: []string{}, TTL: 60, AppID: "old", TenantID: "tenant-a", RecordKind: "custom-domain-target",
				Route: &DNSRouteIntent{Hostnames: []string{h, other}, DNSApplicationIntent: DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}
			if scenario == "old-explicit" {
				record.Route.Bindings = []DNSRouteBinding{bind(h, "/", "old"), bind(other, "/", "old")}
			}
			prior := PlatformIntent{DNS: []DNSIntent{record}, Routes: []RouteIntent{hostRoute, nearby}}
			current := record
			q := *record.Route
			q.Bindings = []DNSRouteBinding{bind(h, "/", "new"), bind(h, "/blue", "old"), bind(other, "/", "old"), bind(other, "/green", "third")}
			current.Route = &q
			desired := PlatformIntent{DNS: []DNSIntent{current}, Routes: []RouteIntent{route(h, "/", "new"), route(h, "/blue", "old"), nearby, route(other, "/green", "third")}}
			stage := PlatformIntent{Routes: []RouteIntent{hostRoute, nearby, route(other, "/green", "third")}}
			switch scenario {
			case "changed-ttl":
				desired.DNS[0].TTL++
			case "policy":
				q.TTLPolicy = "other"
			case "tenant":
				desired.DNS[0].TenantID = "another"
			case "renamed-alias":
				desired.DNS[0].Hostname = "moved.example.test"
			case "foreign-root":
				stage.Routes[0].AppID = "new"
			case "invalid-current":
				q.Bindings[1].AppID = "wrong"
			case "deleted-alias":
				desired.DNS = nil
			case "missing-other-host":
				desired.Routes = desired.Routes[:2]
			case "new-alias":
				prior.DNS = nil
			}
			stage.DNS = slices.Clone(desired.DNS)
			before, _ := json.Marshal([]PlatformIntent{prior, desired})
			e := stageHostedAliasBindings(prior, desired, stage, map[string]bool{h: true})
			after, _ := json.Marshal([]PlatformIntent{prior, desired})
			if string(before) != string(after) {
				t.Fatal("mutated input")
			}
			if scenario == "deleted-alias" {
				if e != nil || len(stage.DNS) != 0 {
					t.Fatal("revived a removed alias", e)
				}
				return
			}
			if scenario != "old-implicit" && scenario != "old-explicit" {
				if e == nil {
					t.Fatal("accepted unrelated change")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			want := []DNSRouteBinding{bind(other, "/", "old"), bind(other, "/green", "third"), bind(h, "/", "old")}
			if !slices.Equal(stage.DNS[0].Route.Bindings, want) {
				t.Fatal("rewrote unrelated bindings or did not restore the staged root")
			}
		})
	}
}
