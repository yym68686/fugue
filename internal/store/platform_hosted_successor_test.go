package store

import (
	"fugue/internal/model"
	"testing"
)

func TestHostedBusinessRevisionIsPerOwnerNotPerProject(t *testing.T) {
	const a, b = "one.example.test", "two.example.test"
	in := ProjectHostRouteInputs{Project: model.Project{ID: "project", TenantID: "t"}, Domain: model.AppDomain{Hostname: a, TenantID: "t", AppID: "service"}, DNS: model.DNSRecord{TTL: 90}}
	in.Table.Domains = []model.ProjectRouteDomain{{Hostname: a, Name: "first"}, {Hostname: b, Name: "second"}}
	pin := func() string {
		d, e := hostedBusinessRevision(in)
		if e != nil {
			t.Fatal(e)
		}
		return d
	}
	before := pin()
	in.Table.Domains[1].Name = "changed-other-domain"
	if pin() != before {
		t.Fatal("an independent business update invalidated this phase")
	}
	in.DNS.TTL++
	if pin() == before {
		t.Fatal("changed TTL authorized by old revision")
	}
	in.DNS.TTL--
	in.Domain.TLSStatus = model.AppDomainTLSStatusPending
	if pin() == before {
		t.Fatal("changed TLS authorization accepted")
	}
}
