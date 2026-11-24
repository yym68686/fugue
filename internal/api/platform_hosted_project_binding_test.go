package api

import (
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/store"
	"testing"
	"time"
)

func TestHostedProjectBindingUsesSavedIntentAndNotTenantUptime(t *testing.T) {
	const h = "site.example.test"
	stamp := time.Now().UTC().Truncate(time.Second)
	p := pc.HostedScopeProposal{Host: h}
	entries := func(paths map[string]string) pc.HostedDispatchRows {
		v := pc.HostedDispatchRows{Owner: model.EdgeTLSAllowlistEntry{Hostname: h, AppID: "owner", TenantID: "tenant"}}
		for path, id := range paths {
			v.Routes = append(v.Routes, model.EdgeRouteIntent{Hostname: h, PathPrefix: path, AppID: id, TenantID: "tenant"})
		}
		return v
	}
	p.Before = entries(map[string]string{"/": "owner"})
	p.Prepared = p.Before
	p.After = entries(map[string]string{"/": "site", "/api": "owner"})
	in := store.ProjectHostRouteInputs{Project: model.Project{ID: "project", TenantID: "tenant"},
		Domain: model.AppDomain{Hostname: h, AppID: "owner", TenantID: "tenant", CreatedAt: stamp, DNSMode: model.AppDomainDNSModeManaged, DNSZoneID: "zone", DNSRecordID: "record",
			Status: model.AppDomainStatusVerified, DNSStatus: model.AppDomainDNSStatusReady, TLSStatus: model.AppDomainTLSStatusReady},
		Zone: model.HostedZone{ID: "zone", TenantID: "tenant", ZoneName: "example.test", Status: model.HostedZoneStatusActive, CreatedAt: stamp},
		DNS: model.DNSRecord{ID: "record", ZoneID: "zone", TenantID: "tenant", FQDN: h, Type: model.DNSRecordTypeFUGUEAPP, Values: []string{"owner"}, TTL: 60, Source: model.DNSRecordSourceAppDomain,
			SourceRefType: model.DNSRecordSourceRefTypeAppDomain, SourceRefID: h, Status: model.DNSRecordStatusActive, CreatedAt: stamp}}
	for _, id := range []string{"owner", "site"} {
		// Zero replicas do not grant us permission to change the paths or
		// increase capacity. This is not a creation/availability check.
		in.Apps = append(in.Apps, model.App{ID: id, Name: id, TenantID: "tenant", ProjectID: "project", Spec: model.AppSpec{Ports: []int{8080}}})
	}
	base, err := matchHostedProjectIntent(in, p)
	if err != nil || base.Direction != "baseline" {
		t.Fatal("unapplied intent became forward", err)
	}
	in.Table.Domains = []model.ProjectRouteDomain{{Name: "primary", Hostname: h}, {Name: "another", Hostname: "other.example.test"}}
	in.Table.Entrypoints = []model.ProjectRouteEntrypoint{{Name: "desired", Domain: "primary", Routes: []model.ProjectRouteEntrypointRoute{{PathPrefix: "/", AppID: "site"}, {PathPrefix: "/api", AppID: "owner"}}},
		{Name: "not-in-scope", Domain: "another", Routes: []model.ProjectRouteEntrypointRoute{{PathPrefix: "/", AppID: "unavailable-foreign"}}}}
	forward, err := matchHostedProjectIntent(in, p)
	if err != nil || forward.Direction != "forward" || base.OwnerDigest != forward.OwnerDigest || base.PathDigest == forward.PathDigest {
		t.Fatal("wrong source or path scope", err)
	}
	in.Table.Entrypoints[1].Routes[0].AppID = "other-foreign"
	in.Table.UpdatedAt = stamp.Add(time.Hour)
	got, err := matchHostedProjectIntent(in, p)
	if err != nil || got != forward {
		t.Fatal("unrelated project activity changed scope", err)
	}
	t.Run("verified external target follows saved routes without granting zone ownership", func(t *testing.T) {
		x, q := in, p
		x.DNS, x.Zone = model.DNSRecord{}, model.HostedZone{}
		x.Domain.DNSMode, x.Domain.DNSRecordKind = model.AppDomainDNSModeExternal, model.AppDomainDNSRecordKindCNAME
		x.Domain.DNSZoneID, x.Domain.DNSRecordID, x.Domain.RouteTarget = "", "", "target.example.test"
		q.Before.Owner.Status = model.AppDomainStatusVerified
		q.Prepared.Owner, q.After.Owner = q.Before.Owner, q.Before.Owner
		q.ExternalPath = &pc.ExternalPathAnchor{Target: pc.ExternalPathTarget{Hostname: h, Target: x.Domain.RouteTarget, TenantID: x.Domain.TenantID, AppID: x.Domain.AppID, Members: []string{h}}}
		current, err := matchHostedProjectIntent(x, q)
		if err != nil || current.Direction != "forward" || current.PathDigest != forward.PathDigest || current.OwnerDigest == forward.OwnerDigest {
			t.Fatal("external route failed to bind independently", err)
		}
		for name, change := range map[string]func(*model.AppDomain){
			"target":            func(d *model.AppDomain) { d.RouteTarget = "other.example.test" },
			"owner":             func(d *model.AppDomain) { d.AppID = "other" },
			"delegation":        func(d *model.AppDomain) { d.DNSRecordID = "now-managed" },
			"tls pending":       func(d *model.AppDomain) { d.TLSStatus = model.AppDomainTLSStatusPending },
			"dns pending":       func(d *model.AppDomain) { d.DNSStatus = model.AppDomainDNSStatusPending },
			"ownership pending": func(d *model.AppDomain) { d.Status = model.AppDomainStatusPending },
		} {
			v := x
			change(&v.Domain)
			if _, err := matchHostedProjectIntent(v, q); err == nil {
				t.Errorf("accepted obsolete external binding after %s changed", name)
			}
		}
		if _, err := matchHostedProjectIntent(in, q); err == nil {
			t.Fatal("existing managed zone was treated as external")
		}
		if _, err := matchHostedProjectIntent(x, p); err == nil {
			t.Fatal("address-hold proposal was applied to an external CNAME")
		}
	})
	in.DNS.TTL++
	got, err = matchHostedProjectIntent(in, p)
	if err != nil || got.OwnerDigest == forward.OwnerDigest {
		t.Fatal("changed wire identity reused old binding", err)
	}
	in.Table.Entrypoints[0].Routes[1].StripPrefix = true
	if _, err := matchHostedProjectIntent(in, p); err == nil {
		t.Fatal("different dispatch transformation matched")
	}
}
