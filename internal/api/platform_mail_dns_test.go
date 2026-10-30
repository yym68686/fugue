package api

import (
	"slices"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

func mailDNSFixture() store.RouteBusinessSnapshot {
	return store.RouteBusinessSnapshot{
		MailPlatformConfig: &model.MailPlatformConfig{Enabled: true, DNSRecords: []model.AppMailDNSRecord{{Name: "@", Type: "MX", Values: []string{"10 mx.example.test."}, TTL: 300}, {Name: "@", Type: "TXT", Values: []string{"v=spf1 -all"}, TTL: 300}, {Name: "_dmarc", Type: "TXT", Values: []string{"v=DMARC1; p=reject; adkim=r; aspf=r"}, TTL: 300}}},
		Apps:               []model.App{{ID: "app", TenantID: "tenant", ProjectID: "project", Route: &model.AppRoute{Hostname: "app.apps.example.test"}}},
		AppMailBindings:    []model.AppMailBinding{{AppID: "app", TenantID: "tenant", ProjectID: "project", Hostname: "app.apps.example.test", State: "active"}},
	}
}
func TestAppMailDNSCoexistsWithWebAndPreservesManualRecords(t *testing.T) {
	b := mailDNSFixture()
	r := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{DNS: []platformconfig.DNSIntent{{Hostname: b.AppMailBindings[0].Hostname, Type: "A", Values: []string{"192.0.2.10"}, TTL: 60}, {Hostname: b.AppMailBindings[0].Hostname, Type: "TXT", Values: []string{"manual-verification=keep"}, TTL: 900}}}}
	if err := projectAppMailDNS(&r, b, "apps.example.test"); err != nil {
		t.Fatal(err)
	}
	if len(r.Intent.DNS) != 4 || len(r.Issues) != 0 {
		t.Fatal("mail/Web merge", r.Intent.DNS, r.Issues)
	}
	for _, v := range r.Intent.DNS {
		if v.Type == "TXT" && v.Hostname == b.AppMailBindings[0].Hostname && (!slices.Contains(v.Values, "manual-verification=keep") || !slices.Contains(v.Values, "v=spf1 -all") || v.TTL != 900) {
			t.Fatal("manual TXT replaced", v)
		}
	}
	if err := platformconfig.ValidateDNSIntents(r.Intent.DNS); err != nil {
		t.Fatal(err)
	}
	if err := projectAppMailDNS(&r, b, "apps.example.test"); err != nil || len(r.Intent.DNS) != 4 {
		t.Fatal("not idempotent", err)
	}
}
func TestAppMailDNSDoesNotReplaceManualMXOrSPF(t *testing.T) {
	b := mailDNSFixture()
	host := b.AppMailBindings[0].Hostname
	r := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{DNS: []platformconfig.DNSIntent{{Hostname: host, Type: "MX", Values: []string{"20 user-mx.example.test."}, TTL: 900}, {Hostname: host, Type: "TXT", Values: []string{"v=spf1 include:manual.example.test -all"}, TTL: 900}}}}
	if err := projectAppMailDNS(&r, b, "apps.example.test"); err != nil {
		t.Fatal(err)
	}
	if len(r.Issues) != 2 {
		t.Fatal("conflicts unreported", r.Issues)
	}
	for _, v := range r.Intent.DNS {
		if v.Hostname == host && (len(v.Values) != 1 || v.TTL != 900) {
			t.Fatal("manual mail policy changed", v)
		}
	}
}
func TestAppMailDNSRequiresFrozenApplicationIdentityAndActiveLifecycle(t *testing.T) {
	for _, kind := range []string{"revoked", "suspended", "transferred", "renamed", "deleted", "root", "missing"} {
		t.Run(kind, func(t *testing.T) {
			b := mailDNSFixture()
			switch kind {
			case "revoked", "suspended":
				b.AppMailBindings[0].State = kind
			case "transferred":
				b.Apps[0].ProjectID = "other"
			case "renamed":
				b.Apps[0].Route.Hostname = "other.apps.example.test"
			case "deleted":
				b.Apps[0].Status.Phase = "deleted"
			case "root":
				b.Apps[0].Route.Hostname = "apps.example.test"
				b.AppMailBindings[0].Hostname = "apps.example.test"
			case "missing":
				b.Apps = nil
			}
			r := platformIntentProjectionResponse{}
			if err := projectAppMailDNS(&r, b, "apps.example.test"); err != nil || len(r.Intent.DNS) != 0 {
				t.Fatal("ineligible application got mail DNS", r.Intent.DNS, err)
			}
		})
	}
}
func TestMailDNSPolicyCannotAliasWebOrEscapeApplicationNamespace(t *testing.T) {
	for _, record := range []model.AppMailDNSRecord{{Name: "@", Type: "CNAME", Values: []string{"mx.example.test."}, TTL: 300}, {Name: "evil.", Type: "TXT", Values: []string{"claim"}, TTL: 300}, {Name: "@", Type: "A", Values: []string{"192.0.2.10"}, TTL: 300}, {Name: "@", Type: "MX", Values: []string{"invalid"}, TTL: 300}} {
		if err := validateMailDNSPolicy([]model.AppMailDNSRecord{record}); err == nil {
			t.Fatal("invalid mail policy accepted", record)
		}
	}
}
