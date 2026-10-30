package api

import (
	"slices"
	"strings"
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

func TestMailRelayDNSIsSpecificToExactActiveApplication(t *testing.T) {
	b := mailDNSFixture()
	otherApp := b.Apps[0]
	otherApp.ID = "other"
	otherApp.TenantID, otherApp.ProjectID = "peer-tenant", "peer-project"
	otherApp.Route = &model.AppRoute{Hostname: "other.apps.example.test"}
	b.Apps = append(b.Apps, otherApp)
	otherBinding := b.AppMailBindings[0]
	otherBinding.AppID, otherBinding.Hostname = otherApp.ID, otherApp.Route.Hostname
	otherBinding.TenantID, otherBinding.ProjectID = otherApp.TenantID, otherApp.ProjectID
	b.AppMailBindings = append(b.AppMailBindings, otherBinding)
	b.MailPlatformConfig.AppDNSRecords = map[string][]model.AppMailDNSRecord{
		"app":     {{Name: "relay._domainkey", Type: "TXT", Values: []string{"v=DKIM1; p=domain-specific-public-key"}, TTL: 300}},
		"other":   {{Name: "relay._domainkey", Type: "TXT", Values: []string{"v=DKIM1; p=other-domain-public-key"}, TTL: 300}},
		"unbound": {{Name: "relay._domainkey", Type: "TXT", Values: []string{"v=DKIM1; p=unbound-public-key"}, TTL: 300}},
	}
	r := platformIntentProjectionResponse{}
	if err := projectAppMailDNS(&r, b, "apps.example.test"); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, record := range r.Intent.DNS {
		if strings.Contains(record.Hostname, "_domainkey") {
			count++
			validAppRecord := record.AppID == "app" && record.Hostname == "relay._domainkey.app.apps.example.test"
			validPeerRecord := record.AppID == "other" && record.Hostname == "relay._domainkey.other.apps.example.test"
			if !validAppRecord && !validPeerRecord {
				t.Fatal("domain-specific relay key leaked to another app", record)
			}
		}
	}
	if count != 2 {
		t.Fatal("missing app-specific DKIM records", r.Intent.DNS)
	}
	for _, lifecycle := range []string{"revoked", "deleted", "missing", "transferred"} {
		t.Run(lifecycle, func(t *testing.T) {
			changed := b
			changed.Apps = slices.Clone(b.Apps)
			changed.AppMailBindings = slices.Clone(b.AppMailBindings)
			switch lifecycle {
			case "revoked":
				changed.AppMailBindings[0].State = "revoked"
			case "deleted":
				changed.Apps[0].Status.Phase = "deleted"
			case "missing":
				changed.Apps = changed.Apps[1:]
			case "transferred":
				changed.Apps[0].TenantID = "new-tenant"
			}
			r := platformIntentProjectionResponse{}
			if err := projectAppMailDNS(&r, changed, "apps.example.test"); err != nil {
				t.Fatal("project application DNS", err)
			}
			for _, record := range r.Intent.DNS {
				if record.AppID != "other" || record.TenantID != "peer-tenant" {
					t.Fatal("ineligible application retained mail DNS", record)
				}
			}
			peerDKIM := slices.ContainsFunc(r.Intent.DNS, func(record platformconfig.DNSIntent) bool {
				return record.Hostname == "relay._domainkey.other.apps.example.test" && slices.Equal(record.Values, []string{"v=DKIM1; p=other-domain-public-key"})
			})
			if len(r.Intent.DNS) != 4 || !peerDKIM {
				t.Fatal("lifecycle cleanup changed another tenant's mail DNS", r.Intent.DNS)
			}
		})
	}
}

func TestMailDNSConfigRejectsUnboundedAndConflictingPerAppRules(t *testing.T) {
	config := *mailDNSFixture().MailPlatformConfig
	config.AppDNSRecords = map[string][]model.AppMailDNSRecord{"app": {{Name: "@", Type: "CNAME", Values: []string{"relay.example.test."}, TTL: 300}}}
	if err := validateMailDNSConfig(config); err == nil {
		t.Fatal("app-specific policy aliased the Web host")
	}
	config.AppDNSRecords = map[string][]model.AppMailDNSRecord{"bad/key": nil}
	if err := validateMailDNSConfig(config); err == nil {
		t.Fatal("invalid application identity accepted")
	}
	config.AppDNSRecords = map[string][]model.AppMailDNSRecord{"app": make([]model.AppMailDNSRecord, 12)}
	if err := validateMailDNSConfig(config); err == nil {
		t.Fatal("combined per-app record bound was bypassed")
	}
}
