package dnsserver

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	miekgdns "github.com/miekg/dns"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestServiceAnswersAuthoritativelyAndRefusesOutsideZone(t *testing.T) {
	t.Parallel()

	service := NewService(config.DNSConfig{
		Zone:        "dns.fugue.pro",
		TTL:         60,
		Nameservers: []string{"ns1.dns.fugue.pro"},
	}, log.New(ioDiscard{}, "", 0))
	service.setBundle(model.EdgeDNSBundle{
		Version: "dnsgen_test",
		Zone:    "dns.fugue.pro",
		Records: []model.EdgeDNSRecord{
			{
				Name:       "d-test.dns.fugue.pro",
				Type:       model.EdgeDNSRecordTypeA,
				Values:     []string{"203.0.113.10"},
				TTL:        60,
				RecordKind: model.EdgeDNSRecordKindProbe,
				Status:     model.EdgeRouteStatusActive,
			},
		},
	}, `"dnsgen_test"`, false, "")

	answer := dnsQuery(t, service, "d-test.dns.fugue.pro.", miekgdns.TypeA)
	if answer.Rcode != miekgdns.RcodeSuccess || !answer.Authoritative || answer.RecursionAvailable {
		t.Fatalf("unexpected authoritative answer header: rcode=%s aa=%t ra=%t", miekgdns.RcodeToString[answer.Rcode], answer.Authoritative, answer.RecursionAvailable)
	}
	if len(answer.Answer) != 1 {
		t.Fatalf("expected one A answer, got %+v", answer.Answer)
	}
	a, ok := answer.Answer[0].(*miekgdns.A)
	if !ok || a.A.String() != "203.0.113.10" || a.Hdr.Ttl != 60 {
		t.Fatalf("unexpected A answer: %+v", answer.Answer[0])
	}

	missing := dnsQuery(t, service, "missing.dns.fugue.pro.", miekgdns.TypeA)
	if missing.Rcode != miekgdns.RcodeNameError || missing.RecursionAvailable {
		t.Fatalf("expected in-zone missing name to be NXDOMAIN without recursion, got rcode=%s ra=%t", miekgdns.RcodeToString[missing.Rcode], missing.RecursionAvailable)
	}
	if len(missing.Ns) == 0 {
		t.Fatalf("expected NXDOMAIN response to include SOA authority: %+v", missing)
	}

	refused := dnsQuery(t, service, "example.com.", miekgdns.TypeA)
	if refused.Rcode != miekgdns.RcodeRefused || !refused.Authoritative || refused.RecursionAvailable {
		t.Fatalf("expected outside-zone query to be refused authoritatively without recursion, got rcode=%s aa=%t ra=%t", miekgdns.RcodeToString[refused.Rcode], refused.Authoritative, refused.RecursionAvailable)
	}
}

func TestServiceSuppressesUnhealthyEdgeAnswerWhenProbeEnabled(t *testing.T) {
	t.Parallel()

	service := NewService(config.DNSConfig{
		Zone:                   "dns.fugue.pro",
		TTL:                    60,
		Nameservers:            []string{"ns1.dns.fugue.pro"},
		EdgeHealthProbeEnabled: true,
		EdgeHealthProbeTimeout: 10 * time.Millisecond,
	}, log.New(ioDiscard{}, "", 0))
	service.edgeProbe = func(ctx context.Context, ip string) bool {
		return ip == "203.0.113.11"
	}
	service.setBundle(model.EdgeDNSBundle{
		Version: "dnsgen_probe",
		Zone:    "dns.fugue.pro",
		Records: []model.EdgeDNSRecord{
			{
				Name:       "app.dns.fugue.pro",
				Type:       model.EdgeDNSRecordTypeA,
				Values:     []string{"203.0.113.10", "203.0.113.11"},
				TTL:        60,
				RecordKind: model.EdgeDNSRecordKindPlatform,
				Status:     model.EdgeRouteStatusActive,
			},
		},
	}, `"dnsgen_probe"`, false, "")
	service.refreshEdgeHealth(context.Background())

	answer := dnsQuery(t, service, "app.dns.fugue.pro.", miekgdns.TypeA)
	if answer.Rcode != miekgdns.RcodeSuccess {
		t.Fatalf("expected success, got %s", miekgdns.RcodeToString[answer.Rcode])
	}
	if len(answer.Answer) != 1 {
		t.Fatalf("expected only one healthy A answer, got %+v", answer.Answer)
	}
	a, ok := answer.Answer[0].(*miekgdns.A)
	if !ok || a.A.String() != "203.0.113.11" {
		t.Fatalf("expected healthy fallback edge answer, got %+v", answer.Answer[0])
	}
}

func TestServiceFallsBackToNextLiveHealthyCandidateBeforeLimiting(t *testing.T) {
	t.Parallel()

	service := NewService(config.DNSConfig{
		Zone:                   "dns.fugue.pro",
		TTL:                    60,
		Nameservers:            []string{"ns1.dns.fugue.pro"},
		EdgeGroupID:            "edge-group-country-us",
		EdgeHealthProbeEnabled: true,
		EdgeHealthProbeTimeout: 10 * time.Millisecond,
	}, log.New(ioDiscard{}, "", 0))
	service.edgeProbe = func(ctx context.Context, ip string) bool {
		return ip == "51.38.126.103"
	}
	service.setBundle(model.EdgeDNSBundle{
		Version: "dnsgen_live_fallback",
		Zone:    "dns.fugue.pro",
		Records: []model.EdgeDNSRecord{
			{
				Name:       "app.dns.fugue.pro",
				Type:       model.EdgeDNSRecordTypeA,
				Values:     []string{"15.204.94.71", "51.38.126.103"},
				TTL:        60,
				RecordKind: model.EdgeDNSRecordKindPlatform,
				Status:     model.EdgeRouteStatusActive,
				AnswerPolicy: model.DNSAnswerPolicy{
					PolicyKind:         model.DNSAnswerPolicyKindGeo,
					ECSEnabled:         true,
					HealthRequired:     true,
					RouteReadyRequired: true,
				},
				Candidates: []model.EdgeDNSAnswerCandidate{
					{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", Country: "us", Priority: 0, Weight: 100, Healthy: true, RouteReady: true, TLSReady: true},
					{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", Country: "de", Priority: 50, Weight: 100, Healthy: true, RouteReady: true, TLSReady: true},
				},
			},
		},
	}, `"dnsgen_live_fallback"`, false, "")
	service.refreshEdgeHealth(context.Background())

	answer := dnsQuery(t, service, "app.dns.fugue.pro.", miekgdns.TypeA)
	if answer.Rcode != miekgdns.RcodeSuccess {
		t.Fatalf("expected success, got %s", miekgdns.RcodeToString[answer.Rcode])
	}
	if len(answer.Answer) != 1 {
		t.Fatalf("expected one live fallback answer, got %+v", answer.Answer)
	}
	a, ok := answer.Answer[0].(*miekgdns.A)
	if !ok || a.A.String() != "51.38.126.103" {
		t.Fatalf("expected live healthy fallback edge answer, got %+v", answer.Answer)
	}
}

func TestServiceOrdersGeoDNSCandidatesFromECS(t *testing.T) {
	t.Parallel()

	service := NewService(config.DNSConfig{
		Zone:        "dns.fugue.pro",
		TTL:         60,
		Nameservers: []string{"ns1.dns.fugue.pro"},
		GeoIPOverrides: []config.DNSGeoIPOverride{
			{CIDR: "198.51.100.0/24", Country: "hk", EdgeGroupID: "edge-group-country-hk"},
		},
	}, log.New(ioDiscard{}, "", 0))
	service.setBundle(model.EdgeDNSBundle{
		Version: "dnsgen_geo",
		Zone:    "dns.fugue.pro",
		Records: []model.EdgeDNSRecord{
			{
				Name:       "app.dns.fugue.pro",
				Type:       model.EdgeDNSRecordTypeA,
				Values:     []string{"15.204.94.71", "5.102.124.125"},
				TTL:        60,
				RecordKind: model.EdgeDNSRecordKindPlatform,
				Status:     model.EdgeRouteStatusActive,
				AnswerPolicy: model.DNSAnswerPolicy{
					PolicyKind:         model.DNSAnswerPolicyKindGeo,
					ECSEnabled:         true,
					HealthRequired:     true,
					RouteReadyRequired: true,
				},
				Candidates: []model.EdgeDNSAnswerCandidate{
					{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", Country: "us", Priority: 50, Weight: 100, Healthy: true, RouteReady: true, TLSReady: true},
					{IP: "5.102.124.125", EdgeGroupID: "edge-group-country-hk", Country: "hk", Priority: 50, Weight: 100, Healthy: true, RouteReady: true, TLSReady: true},
				},
			},
		},
	}, `"dnsgen_geo"`, false, "")

	req := new(miekgdns.Msg)
	req.SetQuestion("app.dns.fugue.pro.", miekgdns.TypeA)
	req.RecursionDesired = true
	opt := new(miekgdns.OPT)
	opt.Hdr.Name = "."
	opt.Hdr.Rrtype = miekgdns.TypeOPT
	opt.Option = append(opt.Option, &miekgdns.EDNS0_SUBNET{
		Code:          miekgdns.EDNS0SUBNET,
		Family:        1,
		SourceNetmask: 24,
		Address:       net.ParseIP("198.51.100.25").To4(),
	})
	req.Extra = append(req.Extra, opt)

	answer := dnsQueryMsg(t, service, req)
	if answer.Rcode != miekgdns.RcodeSuccess || len(answer.Answer) != 1 {
		t.Fatalf("expected one selected answer, got rcode=%s answers=%+v", miekgdns.RcodeToString[answer.Rcode], answer.Answer)
	}
	first, ok := answer.Answer[0].(*miekgdns.A)
	if !ok || first.A.String() != "5.102.124.125" {
		t.Fatalf("expected ECS HK hint to order HK edge first, got %+v", answer.Answer)
	}
	ecs := ecsSubnetOption(answer)
	if ecs == nil || ecs.SourceScope != 24 {
		t.Fatalf("expected ECS response scope to confirm /24 geographic scope, got %+v", ecs)
	}
}

func TestServiceUsesStableRoutePriorityWithoutGeoHint(t *testing.T) {
	t.Parallel()

	for _, edgeGroupID := range []string{"edge-group-country-us", "edge-group-country-de"} {
		service := NewService(config.DNSConfig{
			Zone:        "dns.fugue.pro",
			TTL:         60,
			Nameservers: []string{"ns1.dns.fugue.pro"},
			EdgeGroupID: edgeGroupID,
		}, log.New(ioDiscard{}, "", 0))
		service.setBundle(model.EdgeDNSBundle{
			Version: "dnsgen_stable_priority",
			Zone:    "dns.fugue.pro",
			Records: []model.EdgeDNSRecord{
				{
					Name:       "app.dns.fugue.pro",
					Type:       model.EdgeDNSRecordTypeA,
					Values:     []string{"15.204.94.71", "51.38.126.103"},
					TTL:        60,
					RecordKind: model.EdgeDNSRecordKindPlatform,
					Status:     model.EdgeRouteStatusActive,
					AnswerPolicy: model.DNSAnswerPolicy{
						PolicyKind:         model.DNSAnswerPolicyKindGeo,
						ECSEnabled:         true,
						HealthRequired:     true,
						RouteReadyRequired: true,
					},
					Candidates: []model.EdgeDNSAnswerCandidate{
						{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", Country: "de", Priority: 0, Weight: 100, Healthy: true, RouteReady: true, TLSReady: true},
						{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", Country: "us", Priority: 50, Weight: 100, Healthy: true, RouteReady: true, TLSReady: true},
					},
				},
			},
		}, `"dnsgen_stable_priority"`, false, "")

		answer := dnsQuery(t, service, "app.dns.fugue.pro.", miekgdns.TypeA)
		if answer.Rcode != miekgdns.RcodeSuccess || len(answer.Answer) != 1 {
			t.Fatalf("edge group %s: expected one selected answer, got rcode=%s answers=%+v", edgeGroupID, miekgdns.RcodeToString[answer.Rcode], answer.Answer)
		}
		first, ok := answer.Answer[0].(*miekgdns.A)
		if !ok || first.A.String() != "51.38.126.103" {
			t.Fatalf("edge group %s: expected stable route-priority answer, got %+v", edgeGroupID, answer.Answer)
		}
	}
}

func TestServiceIgnoresECSWithoutGeoOverride(t *testing.T) {
	t.Parallel()

	service := NewService(config.DNSConfig{
		Zone:        "dns.fugue.pro",
		TTL:         60,
		Nameservers: []string{"ns1.dns.fugue.pro"},
		EdgeGroupID: "edge-group-country-us",
	}, log.New(ioDiscard{}, "", 0))
	service.setBundle(model.EdgeDNSBundle{
		Version: "dnsgen_ecs_without_geo",
		Zone:    "dns.fugue.pro",
		Records: []model.EdgeDNSRecord{
			{
				Name:       "app.dns.fugue.pro",
				Type:       model.EdgeDNSRecordTypeA,
				Values:     []string{"15.204.94.71", "51.38.126.103"},
				TTL:        60,
				RecordKind: model.EdgeDNSRecordKindPlatform,
				Status:     model.EdgeRouteStatusActive,
				AnswerPolicy: model.DNSAnswerPolicy{
					PolicyKind:         model.DNSAnswerPolicyKindGeo,
					ECSEnabled:         true,
					HealthRequired:     true,
					RouteReadyRequired: true,
				},
				Candidates: []model.EdgeDNSAnswerCandidate{
					{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", Country: "de", Priority: 0, Weight: 100, Healthy: true, RouteReady: true, TLSReady: true},
					{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", Country: "us", Priority: 50, Weight: 100, Healthy: true, RouteReady: true, TLSReady: true},
				},
			},
		},
	}, `"dnsgen_ecs_without_geo"`, false, "")

	req := new(miekgdns.Msg)
	req.SetQuestion("app.dns.fugue.pro.", miekgdns.TypeA)
	req.RecursionDesired = true
	opt := new(miekgdns.OPT)
	opt.Hdr.Name = "."
	opt.Hdr.Rrtype = miekgdns.TypeOPT
	opt.Option = append(opt.Option, &miekgdns.EDNS0_SUBNET{
		Code:          miekgdns.EDNS0SUBNET,
		Family:        1,
		SourceNetmask: 24,
		Address:       net.ParseIP("198.51.100.25").To4(),
	})
	req.Extra = append(req.Extra, opt)

	answer := dnsQueryMsg(t, service, req)
	if answer.Rcode != miekgdns.RcodeSuccess || len(answer.Answer) != 1 {
		t.Fatalf("expected one selected answer, got rcode=%s answers=%+v", miekgdns.RcodeToString[answer.Rcode], answer.Answer)
	}
	first, ok := answer.Answer[0].(*miekgdns.A)
	if !ok || first.A.String() != "51.38.126.103" {
		t.Fatalf("expected ECS without GeoIP override to keep stable route-priority answer, got %+v", answer.Answer)
	}
	ecs := ecsSubnetOption(answer)
	if ecs == nil || ecs.SourceScope != 0 {
		t.Fatalf("expected ECS response scope 0 when server has no user-location signal, got %+v", ecs)
	}
}

func TestServiceUsesScopedLatencyCandidatesOnlyWithExplicitGeoHint(t *testing.T) {
	t.Parallel()

	service := NewService(config.DNSConfig{
		Zone:        "dns.fugue.pro",
		TTL:         60,
		Nameservers: []string{"ns1.dns.fugue.pro"},
		GeoIPOverrides: []config.DNSGeoIPOverride{
			{CIDR: "198.51.100.0/24", Country: "us", Region: "us-east", ASN: "as701"},
		},
	}, log.New(ioDiscard{}, "", 0))
	record := model.EdgeDNSRecord{
		Name:       "app.dns.fugue.pro",
		Type:       model.EdgeDNSRecordTypeA,
		Values:     []string{"51.38.126.103", "15.204.94.71"},
		TTL:        60,
		RecordKind: model.EdgeDNSRecordKindPlatform,
		Status:     model.EdgeRouteStatusActive,
		AnswerPolicy: model.DNSAnswerPolicy{
			PolicyKind:         model.DNSAnswerPolicyKindGeo,
			ECSEnabled:         true,
			HealthRequired:     true,
			RouteReadyRequired: true,
		},
		Candidates: []model.EdgeDNSAnswerCandidate{
			{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", Country: "de", Priority: 0, Weight: 100, Healthy: true, RouteReady: true, TLSReady: true},
			{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", Country: "us", Priority: 50, Weight: 100, Healthy: true, RouteReady: true, TLSReady: true},
		},
		ScopedCandidates: []model.EdgeDNSScopedAnswerCandidates{
			{
				ScopeKey:            "country:us",
				Country:             "us",
				PolicyKind:          model.DNSAnswerPolicyKindLatencyAware,
				Reason:              "latency_aware_stable_window_24h",
				SelectedEdgeGroupID: "edge-group-country-us",
				Candidates: []model.EdgeDNSAnswerCandidate{
					{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", Country: "us", Priority: 50, Weight: 200, Healthy: true, RouteReady: true, TLSReady: true},
					{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", Country: "de", Priority: 50, Weight: 20, Healthy: true, RouteReady: true, TLSReady: true},
				},
			},
		},
	}
	service.setBundle(model.EdgeDNSBundle{
		Version: "dnsgen_scoped_latency",
		Zone:    "dns.fugue.pro",
		Records: []model.EdgeDNSRecord{record},
	}, `"dnsgen_scoped_latency"`, false, "")

	global := dnsQuery(t, service, "app.dns.fugue.pro.", miekgdns.TypeA)
	if global.Rcode != miekgdns.RcodeSuccess || len(global.Answer) != 1 {
		t.Fatalf("expected one global answer, got rcode=%s answers=%+v", miekgdns.RcodeToString[global.Rcode], global.Answer)
	}
	globalA, ok := global.Answer[0].(*miekgdns.A)
	if !ok || globalA.A.String() != "51.38.126.103" {
		t.Fatalf("without ECS/Geo, expected stable global route-priority answer, got %+v", global.Answer)
	}

	req := new(miekgdns.Msg)
	req.SetQuestion("app.dns.fugue.pro.", miekgdns.TypeA)
	req.RecursionDesired = true
	opt := new(miekgdns.OPT)
	opt.Hdr.Name = "."
	opt.Hdr.Rrtype = miekgdns.TypeOPT
	opt.Option = append(opt.Option, &miekgdns.EDNS0_SUBNET{
		Code:          miekgdns.EDNS0SUBNET,
		Family:        1,
		SourceNetmask: 24,
		Address:       net.ParseIP("198.51.100.25").To4(),
	})
	req.Extra = append(req.Extra, opt)

	scoped := dnsQueryMsg(t, service, req)
	if scoped.Rcode != miekgdns.RcodeSuccess || len(scoped.Answer) != 1 {
		t.Fatalf("expected one scoped answer, got rcode=%s answers=%+v", miekgdns.RcodeToString[scoped.Rcode], scoped.Answer)
	}
	scopedA, ok := scoped.Answer[0].(*miekgdns.A)
	if !ok || scopedA.A.String() != "15.204.94.71" {
		t.Fatalf("with explicit ECS/Geo, expected scoped latency answer, got %+v", scoped.Answer)
	}
	ecs := ecsSubnetOption(scoped)
	if ecs == nil || ecs.SourceScope != 24 {
		t.Fatalf("expected ECS response scope to reflect scoped decision, got %+v", ecs)
	}
}

func TestEdgeDNSExplorationPromotesHealthyAlternativeDeterministically(t *testing.T) {
	t.Parallel()

	record := model.EdgeDNSRecord{
		Name: "app.dns.fugue.pro",
		Type: model.EdgeDNSRecordTypeA,
		AnswerPolicy: model.DNSAnswerPolicy{
			PolicyKind:         model.DNSAnswerPolicyKindLatencyAware,
			HealthRequired:     true,
			RouteReadyRequired: true,
			ExplorationPercent: 5,
		},
		Candidates: []model.EdgeDNSAnswerCandidate{
			{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", Country: "de", Priority: 50, Weight: 200, Healthy: true, RouteReady: true, TLSReady: true},
			{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", Country: "us", Priority: 50, Weight: 20, Healthy: true, RouteReady: true, TLSReady: true},
			{IP: "5.102.124.125", EdgeGroupID: "edge-group-country-hk", Country: "hk", Priority: 50, Weight: 240, Healthy: false, RouteReady: true, TLSReady: true},
		},
	}
	hint := dnsGeoHint{Country: "us", IP: "198.51.100.25", Source: "ecs"}
	foundExploration := false
	for bucket := int64(0); bucket < 2000; bucket++ {
		ordered := edgeDNSOrderedCandidates(record, hint, time.Unix(bucket*int64((10*time.Minute).Seconds()), 0).UTC())
		if len(ordered) == 0 {
			t.Fatal("expected eligible candidates")
		}
		if ordered[0].IP == "15.204.94.71" {
			foundExploration = true
			break
		}
		if ordered[0].IP == "5.102.124.125" {
			t.Fatalf("exploration must never promote unhealthy candidates: %+v", ordered)
		}
	}
	if !foundExploration {
		t.Fatal("expected 5% exploration to promote a healthy non-top candidate in at least one deterministic bucket")
	}
}

func TestEdgeDNSExplorationPromotesSameGroupNodeBeforeCrossGroup(t *testing.T) {
	t.Parallel()

	record := model.EdgeDNSRecord{
		Name: "app.dns.fugue.pro",
		Type: model.EdgeDNSRecordTypeA,
		AnswerPolicy: model.DNSAnswerPolicy{
			PolicyKind:         model.DNSAnswerPolicyKindWeighted,
			HealthRequired:     true,
			RouteReadyRequired: true,
			ExplorationPercent: 5,
		},
		Candidates: []model.EdgeDNSAnswerCandidate{
			{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", Country: "us", Priority: 50, Weight: 200, Healthy: true, RouteReady: true, TLSReady: true},
			{IP: "95.169.10.156", EdgeGroupID: "edge-group-country-us", Country: "us", Priority: 50, Weight: 200, Healthy: true, RouteReady: true, TLSReady: true},
			{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", Country: "de", Priority: 50, Weight: 200, Healthy: true, RouteReady: true, TLSReady: true},
		},
	}
	hint := dnsGeoHint{Country: "us", IP: "198.51.100.25", Source: "ecs"}
	foundNodeExploration := false
	for bucket := int64(0); bucket < 2000; bucket++ {
		ordered := edgeDNSOrderedCandidates(record, hint, time.Unix(bucket*int64((10*time.Minute).Seconds()), 0).UTC())
		if len(ordered) == 0 {
			t.Fatal("expected eligible candidates")
		}
		switch ordered[0].IP {
		case "95.169.10.156":
			foundNodeExploration = true
		case "15.204.94.71":
		default:
			t.Fatalf("same-group node exploration must not promote cross-group candidate first, got %+v", ordered)
		}
		if foundNodeExploration {
			break
		}
	}
	if !foundNodeExploration {
		t.Fatal("expected exploration to promote a same-group edge node in at least one deterministic bucket")
	}
}

func TestEdgeDNSCandidateEligibilityRequiresTLSReadyForWeightedPolicy(t *testing.T) {
	t.Parallel()

	record := model.EdgeDNSRecord{
		Name: "app.dns.fugue.pro",
		Type: model.EdgeDNSRecordTypeA,
		AnswerPolicy: model.DNSAnswerPolicy{
			PolicyKind:         model.DNSAnswerPolicyKindWeighted,
			HealthRequired:     true,
			RouteReadyRequired: true,
			ExplorationPercent: 50,
		},
		Candidates: []model.EdgeDNSAnswerCandidate{
			{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", Priority: 0, Weight: 200, Healthy: true, RouteReady: true, TLSReady: false},
			{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", Priority: 50, Weight: 20, Healthy: true, RouteReady: true, TLSReady: true},
			{IP: "5.102.124.125", EdgeGroupID: "edge-group-country-hk", Priority: 50, Weight: 240, Healthy: false, RouteReady: true, TLSReady: true},
		},
	}
	ordered := edgeDNSOrderedCandidates(record, dnsGeoHint{}, time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC))
	if len(ordered) != 1 || ordered[0].IP != "51.38.126.103" {
		t.Fatalf("expected only healthy route-ready TLS-ready candidate, got %+v", ordered)
	}
}

func TestServiceLatencyAwareSelectsMeasuredFastEdgeWhenHealthy(t *testing.T) {
	t.Parallel()

	service := NewService(config.DNSConfig{
		Zone:        "dns.fugue.pro",
		TTL:         60,
		Nameservers: []string{"ns1.dns.fugue.pro"},
	}, log.New(ioDiscard{}, "", 0))
	service.Config.EdgeGroupID = "edge-group-country-de"
	service.setBundle(model.EdgeDNSBundle{
		Version: "dnsgen_latency",
		Zone:    "dns.fugue.pro",
		Records: []model.EdgeDNSRecord{
			{
				Name:       "app.dns.fugue.pro",
				Type:       model.EdgeDNSRecordTypeA,
				Values:     []string{"51.38.126.103", "15.204.94.71", "5.102.124.125"},
				TTL:        60,
				Status:     model.EdgeRouteStatusActive,
				RecordKind: model.EdgeDNSRecordKindPlatform,
				AnswerPolicy: model.DNSAnswerPolicy{
					PolicyKind:         model.DNSAnswerPolicyKindLatencyAware,
					ECSEnabled:         true,
					HealthRequired:     true,
					RouteReadyRequired: true,
				},
				Candidates: []model.EdgeDNSAnswerCandidate{
					{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", Country: "de", Priority: 0, Weight: 20, Healthy: true, RouteReady: true, TLSReady: true},
					{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", Country: "us", Priority: 50, Weight: 200, Healthy: true, RouteReady: true, TLSReady: true},
					{IP: "5.102.124.125", EdgeGroupID: "edge-group-country-hk", Country: "hk", Priority: 50, Weight: 240, Healthy: false, RouteReady: true, TLSReady: true},
				},
			},
		},
	}, `"dnsgen_latency"`, false, "")

	answer := dnsQuery(t, service, "app.dns.fugue.pro.", miekgdns.TypeA)
	if answer.Rcode != miekgdns.RcodeSuccess {
		t.Fatalf("expected success, got %s", miekgdns.RcodeToString[answer.Rcode])
	}
	if len(answer.Answer) != 1 {
		t.Fatalf("expected one latency-selected answer, got %+v", answer.Answer)
	}
	first, ok := answer.Answer[0].(*miekgdns.A)
	if !ok || first.A.String() != "15.204.94.71" {
		t.Fatalf("expected measured fast healthy edge to beat local route priority, got %+v", answer.Answer)
	}
}

func TestServiceLatencyAwareSelectsTopAnswerWhenWeightsAreClose(t *testing.T) {
	t.Parallel()

	service := NewService(config.DNSConfig{
		Zone:        "dns.fugue.pro",
		TTL:         60,
		Nameservers: []string{"ns1.dns.fugue.pro"},
	}, log.New(ioDiscard{}, "", 0))
	service.setBundle(model.EdgeDNSBundle{
		Version: "dnsgen_latency_close",
		Zone:    "dns.fugue.pro",
		Records: []model.EdgeDNSRecord{
			{
				Name:       "app.dns.fugue.pro",
				Type:       model.EdgeDNSRecordTypeA,
				Values:     []string{"51.38.126.103", "15.204.94.71"},
				TTL:        60,
				Status:     model.EdgeRouteStatusActive,
				RecordKind: model.EdgeDNSRecordKindPlatform,
				AnswerPolicy: model.DNSAnswerPolicy{
					PolicyKind:         model.DNSAnswerPolicyKindLatencyAware,
					ECSEnabled:         true,
					HealthRequired:     true,
					RouteReadyRequired: true,
				},
				Candidates: []model.EdgeDNSAnswerCandidate{
					{IP: "51.38.126.103", EdgeGroupID: "edge-group-country-de", Country: "de", Priority: 50, Weight: 170, Healthy: true, RouteReady: true, TLSReady: true},
					{IP: "15.204.94.71", EdgeGroupID: "edge-group-country-us", Country: "us", Priority: 50, Weight: 190, Healthy: true, RouteReady: true, TLSReady: true},
				},
			},
		},
	}, `"dnsgen_latency_close"`, false, "")

	answer := dnsQuery(t, service, "app.dns.fugue.pro.", miekgdns.TypeA)
	if answer.Rcode != miekgdns.RcodeSuccess {
		t.Fatalf("expected success, got %s", miekgdns.RcodeToString[answer.Rcode])
	}
	if len(answer.Answer) != 1 {
		t.Fatalf("expected close latency candidates to keep one selected answer, got %+v", answer.Answer)
	}
	first, ok := answer.Answer[0].(*miekgdns.A)
	if !ok || first.A.String() != "15.204.94.71" {
		t.Fatalf("expected faster weighted edge first, got %+v", answer.Answer)
	}
}

func TestLoadCacheFallsBackToPreviousVerifiedDNSGeneration(t *testing.T) {
	t.Parallel()

	cachePath := filepath.Join(t.TempDir(), "dns-cache.json")
	service := NewService(config.DNSConfig{
		Zone:              "dns.fugue.pro",
		CachePath:         cachePath,
		CacheArchiveLimit: 3,
		MaxStale:          time.Hour,
	}, log.New(ioDiscard{}, "", 0))
	oldBundle := model.EdgeDNSBundle{
		Version: "dnsgen_old",
		Zone:    "dns.fugue.pro",
		Records: []model.EdgeDNSRecord{
			{Name: "old.dns.fugue.pro", Type: model.EdgeDNSRecordTypeA, Values: []string{"203.0.113.10"}, Status: model.EdgeRouteStatusActive},
		},
	}
	newBundle := oldBundle
	newBundle.Version = "dnsgen_new"
	if err := service.writeCache(cacheFile{Version: cacheFileVersion, ETag: `"dnsgen_old"`, CachedAt: time.Now().UTC(), Bundle: oldBundle}); err != nil {
		t.Fatalf("write old dns cache: %v", err)
	}
	if err := service.writeCache(cacheFile{Version: cacheFileVersion, ETag: `"dnsgen_new"`, CachedAt: time.Now().UTC(), Bundle: newBundle}); err != nil {
		t.Fatalf("write new dns cache: %v", err)
	}
	if err := os.WriteFile(cachePath, []byte("{corrupt"), 0o600); err != nil {
		t.Fatalf("corrupt current dns cache: %v", err)
	}

	if err := service.LoadCache(); err != nil {
		t.Fatalf("load dns cache with fallback: %v", err)
	}
	status := service.Status()
	if status.ServingGeneration != "dnsgen_old" || status.CacheCorruptGeneration != "unknown" {
		t.Fatalf("expected previous DNS LKG after corrupt current cache, got %+v", status)
	}
}

func TestServiceAnswersFullZoneRecordTypesAndWildcard(t *testing.T) {
	t.Parallel()

	service := NewService(config.DNSConfig{
		Zone:        "fugue.pro",
		TTL:         60,
		Nameservers: []string{"fallback-ns.fugue.pro"},
	}, log.New(ioDiscard{}, "", 0))
	service.setBundle(model.EdgeDNSBundle{
		Version: "dnsgen_full_zone",
		Zone:    "fugue.pro",
		Records: []model.EdgeDNSRecord{
			{Name: "fugue.pro", Type: model.EdgeDNSRecordTypeNS, Values: []string{"ns1.dns.fugue.pro", "ns2.dns.fugue.pro"}, TTL: 300, RecordKind: model.EdgeDNSRecordKindProtected, Status: model.EdgeRouteStatusActive},
			{Name: "fugue.pro", Type: model.EdgeDNSRecordTypeMX, Values: []string{"10 mail.fugue.pro"}, TTL: 300, RecordKind: model.EdgeDNSRecordKindProtected, Status: model.EdgeRouteStatusActive},
			{Name: "fugue.pro", Type: model.EdgeDNSRecordTypeTXT, Values: []string{"v=spf1 include:_spf.example.com -all"}, TTL: 300, RecordKind: model.EdgeDNSRecordKindProtected, Status: model.EdgeRouteStatusActive},
			{Name: "fugue.pro", Type: model.EdgeDNSRecordTypeCAA, Values: []string{"0 issue \"letsencrypt.org\""}, TTL: 300, RecordKind: model.EdgeDNSRecordKindProtected, Status: model.EdgeRouteStatusActive},
			{Name: "alias.fugue.pro", Type: model.EdgeDNSRecordTypeCNAME, Values: []string{"target.example.net"}, TTL: 120, RecordKind: model.EdgeDNSRecordKindProtected, Status: model.EdgeRouteStatusActive},
			{Name: "*.fugue.pro", Type: model.EdgeDNSRecordTypeA, Values: []string{"198.51.100.9"}, TTL: 60, RecordKind: model.EdgeDNSRecordKindProtected, Status: model.EdgeRouteStatusActive},
			{Name: "demo.fugue.pro", Type: model.EdgeDNSRecordTypeA, Values: []string{"198.51.100.7"}, TTL: 60, RecordKind: model.EdgeDNSRecordKindPlatform, Status: model.EdgeRouteStatusActive},
		},
	}, `"dnsgen_full_zone"`, false, "")

	nsAnswer := dnsQuery(t, service, "fugue.pro.", miekgdns.TypeNS)
	if len(nsAnswer.Answer) != 2 {
		t.Fatalf("expected static NS answers, got %+v", nsAnswer.Answer)
	}
	if ns, ok := nsAnswer.Answer[0].(*miekgdns.NS); !ok || ns.Ns == "fallback-ns.fugue.pro." {
		t.Fatalf("expected bundle NS answers to override fallback nameservers, got %+v", nsAnswer.Answer)
	}

	mxAnswer := dnsQuery(t, service, "fugue.pro.", miekgdns.TypeMX)
	if len(mxAnswer.Answer) != 1 {
		t.Fatalf("expected one MX answer, got %+v", mxAnswer.Answer)
	}
	if mx, ok := mxAnswer.Answer[0].(*miekgdns.MX); !ok || mx.Preference != 10 || mx.Mx != "mail.fugue.pro." {
		t.Fatalf("unexpected MX answer: %+v", mxAnswer.Answer[0])
	}

	txtAnswer := dnsQuery(t, service, "fugue.pro.", miekgdns.TypeTXT)
	if len(txtAnswer.Answer) != 1 {
		t.Fatalf("expected one TXT answer, got %+v", txtAnswer.Answer)
	}
	if txt, ok := txtAnswer.Answer[0].(*miekgdns.TXT); !ok || len(txt.Txt) == 0 || txt.Txt[0] != "v=spf1 include:_spf.example.com -all" {
		t.Fatalf("unexpected TXT answer: %+v", txtAnswer.Answer[0])
	}

	caaAnswer := dnsQuery(t, service, "fugue.pro.", miekgdns.TypeCAA)
	if len(caaAnswer.Answer) != 1 {
		t.Fatalf("expected one CAA answer, got %+v", caaAnswer.Answer)
	}
	if caa, ok := caaAnswer.Answer[0].(*miekgdns.CAA); !ok || caa.Flag != 0 || caa.Tag != "issue" || caa.Value != "letsencrypt.org" {
		t.Fatalf("unexpected CAA answer: %+v", caaAnswer.Answer[0])
	}

	cnameForA := dnsQuery(t, service, "alias.fugue.pro.", miekgdns.TypeA)
	if len(cnameForA.Answer) != 1 {
		t.Fatalf("expected CNAME answer for A query, got %+v", cnameForA.Answer)
	}
	if cname, ok := cnameForA.Answer[0].(*miekgdns.CNAME); !ok || cname.Target != "target.example.net." {
		t.Fatalf("unexpected CNAME answer: %+v", cnameForA.Answer[0])
	}

	exact := dnsQuery(t, service, "demo.fugue.pro.", miekgdns.TypeA)
	if len(exact.Answer) != 1 {
		t.Fatalf("expected exact A answer, got %+v", exact.Answer)
	}
	if a, ok := exact.Answer[0].(*miekgdns.A); !ok || a.A.String() != "198.51.100.7" {
		t.Fatalf("unexpected exact A answer: %+v", exact.Answer[0])
	}

	wildcard := dnsQuery(t, service, "other.fugue.pro.", miekgdns.TypeA)
	if len(wildcard.Answer) != 1 {
		t.Fatalf("expected wildcard A answer, got %+v", wildcard.Answer)
	}
	if a, ok := wildcard.Answer[0].(*miekgdns.A); !ok || a.A.String() != "198.51.100.9" || a.Hdr.Name != "other.fugue.pro." {
		t.Fatalf("unexpected wildcard A answer: %+v", wildcard.Answer[0])
	}
}

func TestServiceSyncWritesCacheLoadsCacheAndUsesNotModified(t *testing.T) {
	t.Parallel()

	cachePath := filepath.Join(t.TempDir(), "dns-cache.json")
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/v1/edge/dns" {
			t.Fatalf("unexpected request path %s", r.URL.Path)
		}
		query := r.URL.Query()
		if query.Get("token") != "edge-secret" {
			t.Fatalf("unexpected token %q", query.Get("token"))
		}
		if query.Get("dns_node_id") != "dns-node-1" {
			t.Fatalf("unexpected dns_node_id %q", query.Get("dns_node_id"))
		}
		if query.Get("answer_ip") != "203.0.113.10" {
			t.Fatalf("unexpected answer_ip %q", query.Get("answer_ip"))
		}
		if query.Get("route_a_answer_ip") != "136.112.185.40" {
			t.Fatalf("unexpected route_a_answer_ip %q", query.Get("route_a_answer_ip"))
		}
		if requests == 2 {
			if got := r.Header.Get("If-None-Match"); got != `"dnsgen_test"` {
				t.Fatalf("expected second sync to send If-None-Match, got %q", got)
			}
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"dnsgen_test"`)
		_ = json.NewEncoder(w).Encode(model.EdgeDNSBundle{
			Version:     "dnsgen_test",
			GeneratedAt: time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC),
			DNSNodeID:   query.Get("dns_node_id"),
			Zone:        query.Get("zone"),
			Records: []model.EdgeDNSRecord{
				{
					Name:       "d-test.dns.fugue.pro",
					Type:       model.EdgeDNSRecordTypeA,
					Values:     []string{"203.0.113.10"},
					TTL:        60,
					RecordKind: model.EdgeDNSRecordKindProbe,
					Status:     model.EdgeRouteStatusActive,
				},
			},
		})
	}))
	defer server.Close()

	cfg := config.DNSConfig{
		APIURL:          server.URL,
		EdgeToken:       "edge-secret",
		DNSNodeID:       "dns-node-1",
		Zone:            "dns.fugue.pro",
		AnswerIPs:       []string{"203.0.113.10"},
		RouteAAnswerIPs: []string{"136.112.185.40"},
		CachePath:       cachePath,
		ListenAddr:      "127.0.0.1:0",
		UDPAddr:         "127.0.0.1:0",
		SyncInterval:    time.Hour,
		HTTPTimeout:     time.Second,
		TTL:             60,
	}
	service := NewService(cfg, log.New(ioDiscard{}, "", 0))
	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatalf("first sync failed: %v", err)
	}
	if status := service.Status(); !status.Healthy || status.BundleVersion != "dnsgen_test" || status.RecordCount != 1 {
		t.Fatalf("unexpected healthy status after first sync: %+v", status)
	}
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("expected cache file to be written: %v", err)
	}

	reloaded := NewService(cfg, log.New(ioDiscard{}, "", 0))
	if err := reloaded.LoadCache(); err != nil {
		t.Fatalf("load cache failed: %v", err)
	}
	if status := reloaded.Status(); !status.Healthy || !status.StaleCache || status.BundleVersion != "dnsgen_test" {
		t.Fatalf("unexpected status after cache load: %+v", status)
	}

	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatalf("second sync failed: %v", err)
	}
	snapshot := service.metricSnapshot()
	if snapshot.Metrics.BundleSyncSuccess != 1 || snapshot.Metrics.BundleSyncNotModified != 1 || snapshot.Metrics.CacheWriteSuccess != 1 {
		t.Fatalf("unexpected sync/cache metrics: %+v", snapshot.Metrics)
	}
}

func TestHeartbeatOnceReportsDNSInventory(t *testing.T) {
	t.Parallel()

	var gotToken string
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/dns/heartbeat" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		gotToken = r.URL.Query().Get("token")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode heartbeat: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "node": gotBody})
	}))
	defer server.Close()

	service := NewService(config.DNSConfig{
		APIURL:            server.URL,
		EdgeToken:         "edge-secret",
		DNSNodeID:         "dns-us-1",
		EdgeGroupID:       "edge-group-country-us",
		PublicIPv4:        "203.0.113.10",
		MeshIP:            "100.64.0.10",
		Zone:              "dns.fugue.pro",
		AnswerIPs:         []string{"203.0.113.10"},
		ListenAddr:        "127.0.0.1:7834",
		UDPAddr:           ":53",
		TCPAddr:           ":53",
		HeartbeatInterval: time.Minute,
		TTL:               60,
	}, log.New(ioDiscard{}, "", 0))
	service.setBundle(model.EdgeDNSBundle{
		Version:     "dnsgen_heartbeat",
		GeneratedAt: time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC),
		DNSNodeID:   "dns-us-1",
		EdgeGroupID: "edge-group-country-us",
		Zone:        "dns.fugue.pro",
		Records: []model.EdgeDNSRecord{{
			Name:       "d-test.dns.fugue.pro",
			Type:       model.EdgeDNSRecordTypeA,
			Values:     []string{"203.0.113.10"},
			TTL:        60,
			RecordKind: model.EdgeDNSRecordKindProbe,
			Status:     model.EdgeRouteStatusActive,
		}},
	}, `"dnsgen_heartbeat"`, false, "")
	_ = dnsQuery(t, service, "d-test.dns.fugue.pro.", miekgdns.TypeA)
	_ = dnsQuery(t, service, "missing.dns.fugue.pro.", miekgdns.TypeA)
	service.recordCacheWrite(false)

	if err := service.HeartbeatOnce(context.Background()); err != nil {
		t.Fatalf("heartbeat once: %v", err)
	}
	if gotToken != "edge-secret" {
		t.Fatalf("expected edge token, got %q", gotToken)
	}
	for key, want := range map[string]any{
		"dns_node_id":        "dns-us-1",
		"edge_group_id":      "edge-group-country-us",
		"public_ipv4":        "203.0.113.10",
		"mesh_ip":            "100.64.0.10",
		"zone":               "dns.fugue.pro",
		"dns_bundle_version": "dnsgen_heartbeat",
		"cache_status":       "ready",
		"status":             model.EdgeHealthHealthy,
		"healthy":            true,
		"udp_listen":         true,
		"tcp_listen":         true,
	} {
		if got := gotBody[key]; got != want {
			t.Fatalf("heartbeat field %s: expected %#v, got %#v in %#v", key, want, got, gotBody)
		}
	}
	if got := gotBody["record_count"]; got != float64(1) {
		t.Fatalf("expected record_count 1, got %#v", got)
	}
	if got := gotBody["query_count"]; got != float64(2) {
		t.Fatalf("expected query_count 2, got %#v", got)
	}
	if got := gotBody["query_error_count"]; got != float64(1) {
		t.Fatalf("expected query_error_count 1, got %#v", got)
	}
	if got := gotBody["cache_write_errors"]; got != float64(1) {
		t.Fatalf("expected cache_write_errors 1, got %#v", got)
	}
}

func dnsQuery(t *testing.T, service *Service, name string, qtype uint16) *miekgdns.Msg {
	t.Helper()
	req := new(miekgdns.Msg)
	req.SetQuestion(name, qtype)
	req.RecursionDesired = true
	return dnsQueryMsg(t, service, req)
}

func dnsQueryMsg(t *testing.T, service *Service, req *miekgdns.Msg) *miekgdns.Msg {
	t.Helper()
	writer := &captureDNSResponseWriter{}
	service.ServeDNS(writer, req)
	if writer.msg == nil {
		t.Fatal("expected DNS response")
	}
	return writer.msg
}

func ecsSubnetOption(msg *miekgdns.Msg) *miekgdns.EDNS0_SUBNET {
	if msg == nil {
		return nil
	}
	opt := msg.IsEdns0()
	if opt == nil {
		return nil
	}
	for _, extra := range opt.Option {
		if subnet, ok := extra.(*miekgdns.EDNS0_SUBNET); ok {
			return subnet
		}
	}
	return nil
}

type captureDNSResponseWriter struct {
	msg *miekgdns.Msg
}

func (w *captureDNSResponseWriter) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 53}
}

func (w *captureDNSResponseWriter) RemoteAddr() net.Addr {
	return &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 53000}
}

func (w *captureDNSResponseWriter) WriteMsg(msg *miekgdns.Msg) error {
	w.msg = msg.Copy()
	return nil
}

func (w *captureDNSResponseWriter) Write(data []byte) (int, error) {
	msg := new(miekgdns.Msg)
	if err := msg.Unpack(data); err != nil {
		return 0, err
	}
	w.msg = msg
	return len(data), nil
}

func (w *captureDNSResponseWriter) Close() error {
	return nil
}

func (w *captureDNSResponseWriter) TsigStatus() error {
	return nil
}

func (w *captureDNSResponseWriter) TsigTimersOnly(bool) {}

func (w *captureDNSResponseWriter) Hijack() {}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}
