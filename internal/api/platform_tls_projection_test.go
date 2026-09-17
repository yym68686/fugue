package api

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDomainTLSProjectionSeparatesIntentAndOriginalLifecycleFacts(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	verified := now.Add(-100 * 24 * time.Hour)
	checked := now.Add(-30 * 24 * time.Hour)
	domain := model.AppDomain{Hostname: "domain.example.test", AppID: "app", TenantID: "tenant", Status: "verified", TLSStatus: "ready", VerifiedAt: &verified, TLSReadyAt: &verified, TLSLastCheckedAt: &checked}
	entry := model.EdgeTLSAllowlistEntry{Hostname: domain.Hostname, AppID: domain.AppID, TenantID: domain.TenantID, Status: domain.Status, TLSStatus: domain.TLSStatus}
	newProjection := func() platformIntentProjectionResponse {
		return platformIntentProjectionResponse{CapturedAt: now, Intent: platformconfig.PlatformIntent{Generation: "initial", Routes: []platformconfig.RouteIntent{{Hostname: domain.Hostname, AppID: domain.AppID, TenantID: domain.TenantID, UpstreamURL: "http://origin", Enabled: true}}, TLS: []platformconfig.TLSIntent{{Hostname: domain.Hostname, Policy: "custom-domain"}}}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now}}
	}
	projection := newProjection()
	before, _ := json.Marshal(domain)
	if err := projectDomainTLSLifecycle(&projection, []model.EdgeTLSAllowlistEntry{entry}, []model.AppDomain{domain}); err != nil || len(projection.Issues) != 0 {
		t.Fatal("domain projection failed", err, projection.Issues)
	}
	if projection.Intent.TLS[0].DomainRef == "" || projection.RuntimeSnapshot.IntentGeneration != projection.Intent.Generation {
		t.Fatal("desired domain reference or generation missing")
	}
	fact := projection.RuntimeSnapshot.TLSDomains[0]
	if !reflect.DeepEqual(fact.VerifiedAt, domain.VerifiedAt) || !reflect.DeepEqual(fact.TLSLastCheckedAt, domain.TLSLastCheckedAt) || !reflect.DeepEqual(fact.TLSReadyAt, domain.TLSReadyAt) {
		t.Fatal("snapshot capture renewed original evidence")
	}
	after, _ := json.Marshal(domain)
	if string(before) != string(after) {
		t.Fatal("projection modified business snapshot")
	}
	updated := newProjection()
	domain.TLSStatus, entry.TLSStatus = "pending", "pending"
	domain.TLSLastCheckedAt = nil
	if err := projectDomainTLSLifecycle(&updated, []model.EdgeTLSAllowlistEntry{entry}, []model.AppDomain{domain}); err != nil {
		t.Fatal(err)
	}
	if updated.Intent.Generation != projection.Intent.Generation || updated.RuntimeSnapshot.TLSDomains[0].TLSStatus != "pending" || updated.RuntimeSnapshot.TLSDomains[0].TLSLastCheckedAt != nil {
		t.Fatal("runtime status changed intent or missing check time was invented")
	}
	broken := newProjection()
	domain.VerifiedAt = nil
	if err := projectDomainTLSLifecycle(&broken, []model.EdgeTLSAllowlistEntry{entry}, []model.AppDomain{domain}); err != nil || len(broken.Issues) != 1 || broken.Issues[0].Code != "tls_domain_lifecycle_evidence_requires_repair" {
		t.Fatal("missing verification evidence was not exposed", err, broken.Issues)
	}
	for _, domains := range [][]model.AppDomain{nil, {domain, domain}, {{Hostname: domain.Hostname, AppID: "other", TenantID: domain.TenantID, Status: domain.Status, TLSStatus: domain.TLSStatus}}} {
		broken := newProjection()
		if err := projectDomainTLSLifecycle(&broken, []model.EdgeTLSAllowlistEntry{entry}, domains); err == nil {
			t.Fatal("unbound domain source accepted")
		}
	}
}
