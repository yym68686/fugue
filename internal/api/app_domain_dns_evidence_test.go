package api

import (
	"context"
	"net"
	"net/http"
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestManualDomainDiagnosisUsesCurrentDNSEvidenceAfterRecordReplacement(t *testing.T) {
	state, server, key, _, app, resolver := setupAppDomainTestServerWithDomains(t, "example.test")
	host := "customer.test"
	zone := putHostedDNSZoneForEdgeDNSTest(t, state, app.TenantID, host)
	record, err := state.PutDNSRecord(zone, model.DNSRecord{Name: "@", Type: model.DNSRecordTypeFUGUEAPP,
		Values: []string{app.ID}, FlattenMode: model.DNSRecordFlattenModeApp, Source: model.DNSRecordSourceUser}, false)
	if err != nil {
		t.Fatal(err)
	}
	created := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", key,
		map[string]any{"hostname": host, "dns_mode": "manual"})
	if created.Code != http.StatusOK {
		t.Fatal(created.Body.String())
	}
	before, err := state.GetAppDomain(host)
	if err != nil || before.DNSRecordID != record.ID {
		t.Fatalf("initial manual association: %+v %v", before, err)
	}
	if _, err := state.DeleteDNSRecord(zone.ID, record.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := state.PutDNSRecord(zone, model.DNSRecord{Name: "@", Type: model.DNSRecordTypeA,
		Values: []string{"192.0.2.25"}, Source: model.DNSRecordSourceUser}, false); err != nil {
		t.Fatal(err)
	}
	target := server.primaryCustomDomainTarget(app)
	resolver.ip[host] = []net.IPAddr{{IP: net.ParseIP("192.0.2.25")}}
	resolver.ip[target] = resolver.ip[host]
	diagnosis := server.buildAppDomainDiagnosis(context.Background(), app, before)
	if !diagnosis.DNSObservation.Verified || diagnosis.DNSObservation.MatchedTarget != target ||
		!reflect.DeepEqual(diagnosis.DNSObservation.HostIPs, []string{"192.0.2.25"}) || diagnosis.Domain.DNSRecordID != "" {
		t.Fatalf("diagnosis reported historical record instead of current DNS: %+v", diagnosis)
	}
	unchanged, _ := state.GetAppDomain(host)
	if unchanged.DNSRecordID != before.DNSRecordID {
		t.Fatal("diagnosis mutated persisted intent")
	}
	verified := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains/verify", key, map[string]any{"hostname": host})
	after, _ := state.GetAppDomain(host)
	if verified.Code != http.StatusOK || after.DNSRecordID != "" || after.Status != model.AppDomainStatusVerified {
		t.Fatalf("verification retained removed DNS relationship: %+v, %s", after, verified.Body.String())
	}
	delete(resolver.ip, host)
	diagnosis = server.buildAppDomainDiagnosis(context.Background(), app, before)
	if diagnosis.DNSObservation.Verified || diagnosis.DNSObservation.MatchedTarget == record.ID || diagnosis.Domain.DNSRecordID != "" {
		t.Fatalf("failed verification retained stale evidence: %+v", diagnosis)
	}
}

func TestDomainDNSObservationIPsBelongToMatchedTarget(t *testing.T) {
	_, server, _, _, _, resolver := setupAppDomainTestServer(t)
	resolver.ip["customer.test"] = []net.IPAddr{{IP: net.ParseIP("192.0.2.25")}}
	resolver.ip["first.example.test"] = []net.IPAddr{{IP: net.ParseIP("192.0.2.10")}}
	resolver.ip["second.example.test"] = resolver.ip["customer.test"]
	observation, err := server.inspectCustomDomainDNS(context.Background(), "customer.test", []string{"first.example.test", "second.example.test"})
	if err != nil || !observation.Verified || observation.MatchedTarget != "second.example.test" ||
		!reflect.DeepEqual(observation.TargetIPs, []string{"192.0.2.25"}) {
		t.Fatalf("matched target and IP evidence disagree: %+v %v", observation, err)
	}
}
