package platformconfig

import (
	"testing"

	"fugue/internal/model"
)

func TestExternalPathChangesFindsOneAliasWithoutChangingSharedTargetOrPeer(t *testing.T) {
	const target = "target.example.test"
	before := PlatformIntent{Scope: GlobalScopeKey}
	after := PlatformIntent{Scope: GlobalScopeKey}
	shared := DNSIntent{Hostname: target, Type: "FUGUE_ROUTE", RecordKind: model.EdgeDNSRecordKindCustomDomainTarget,
		TTL: 60, AppID: "app", TenantID: "tenant", Route: &DNSRouteIntent{Hostnames: []string{"a.example.test", "b.example.test"}}}
	before.DNS, after.DNS = []DNSIntent{shared}, []DNSIntent{shared}
	for _, h := range []string{"a.example.test", "b.example.test"} {
		root := RouteIntent{Hostname: h, PathPrefix: "/", AppID: "app", TenantID: "tenant"}
		before.Routes, after.Routes = append(before.Routes, root), append(after.Routes, root)
		before.TLS = append(before.TLS, TLSIntent{Hostname: h, AppID: "app", TenantID: "tenant", DomainRef: "cert-" + h, Policy: "automatic"})
		after.TLS = append(after.TLS, before.TLS[len(before.TLS)-1])
	}
	for _, p := range []string{"/one", "/two", "/three"} {
		after.Routes = append(after.Routes, RouteIntent{Hostname: "a.example.test", PathPrefix: p, AppID: "app", TenantID: "tenant"})
	}
	changes := externalPathChanges(before, after)
	if len(changes) != 1 || changes["a.example.test"].target.Hostname != "a.example.test" || changes["a.example.test"].target.Target != target ||
		changes["a.example.test"].target.TenantID != "tenant" || changes["a.example.test"].target.AppID != "app" ||
		len(changes["a.example.test"].target.Members) != 2 ||
		changes["a.example.test"].before["/"] != "app" || len(changes["a.example.test"].after) != 4 || changes["b.example.test"].target.Hostname != "" {
		t.Fatalf("unexpected independent change set: %+v", changes)
	}

	after.TLS[0].DomainRef = "changed-owner"
	if got := ExternalChangedHosts(before, after); len(got) != 0 {
		t.Fatalf("certificate change was misclassified as path-only: %v", got)
	}
}
