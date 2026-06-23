package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestPutAppDomainVerifiesWithCNAMEOnly(t *testing.T) {
	t.Parallel()

	s, server, apiKey, _, app, resolver := setupAppDomainTestServer(t)
	expectedTarget := server.primaryCustomDomainTarget(app)
	resolver.cname["www.example.com"] = expectedTarget + "."

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", apiKey, map[string]any{
		"hostname": "www.example.com",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var putResponse struct {
		Domain         model.AppDomain       `json:"domain"`
		Availability   appDomainAvailability `json:"availability"`
		AlreadyCurrent bool                  `json:"already_current"`
	}
	mustDecodeJSON(t, recorder, &putResponse)
	if putResponse.AlreadyCurrent {
		t.Fatal("expected new app domain to be created")
	}
	if got := putResponse.Domain.RouteTarget; got != expectedTarget {
		t.Fatalf("expected route target %q, got %q", expectedTarget, got)
	}
	if putResponse.Domain.VerificationTXTName != "" || putResponse.Domain.VerificationTXTValue != "" {
		t.Fatalf("expected CNAME-only verification, got %+v", putResponse.Domain)
	}
	if putResponse.Domain.Status != model.AppDomainStatusVerified {
		t.Fatalf("expected verified domain status, got %+v", putResponse.Domain)
	}
	if putResponse.Domain.TLSStatus != model.AppDomainTLSStatusPending {
		t.Fatalf("expected pending TLS status after verification, got %+v", putResponse.Domain)
	}
	if putResponse.Domain.TLSReadyAt != nil {
		t.Fatalf("expected TLS ready timestamp to be empty before edge report, got %+v", putResponse.Domain)
	}

	found, err := s.GetAppByHostname("www.example.com")
	if err != nil {
		t.Fatalf("lookup verified custom domain: %v", err)
	}
	if found.ID != app.ID {
		t.Fatalf("expected app %s, got %s", app.ID, found.ID)
	}
}

func TestListAppDomainsRefreshesStalePendingDomain(t *testing.T) {
	t.Parallel()

	s, server, apiKey, _, app, resolver := setupAppDomainTestServer(t)
	expectedTarget := server.primaryCustomDomainTarget(app)
	stale := time.Now().UTC().Add(-2 * appDomainReadVerifyMinInterval)
	if _, err := s.PutAppDomain(model.AppDomain{
		Hostname:         "www.example.com",
		AppID:            app.ID,
		TenantID:         app.TenantID,
		Status:           model.AppDomainStatusPending,
		DNSStatus:        model.AppDomainDNSStatusPending,
		RouteTarget:      expectedTarget,
		LastMessage:      "create a CNAME record for www.example.com pointing to " + expectedTarget,
		DNSLastMessage:   "create a CNAME record for www.example.com pointing to " + expectedTarget,
		LastCheckedAt:    &stale,
		DNSLastCheckedAt: &stale,
		CreatedAt:        stale,
		UpdatedAt:        stale,
	}); err != nil {
		t.Fatalf("put stale pending domain: %v", err)
	}
	resolver.cname["www.example.com"] = expectedTarget + "."

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/domains", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Domains []model.AppDomain `json:"domains"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.Domains) != 1 {
		t.Fatalf("expected one domain, got %+v", response.Domains)
	}
	domain := response.Domains[0]
	if domain.Status != model.AppDomainStatusVerified || domain.DNSStatus != model.AppDomainDNSStatusReady || domain.DNSRecordKind != model.AppDomainDNSRecordKindCNAME {
		t.Fatalf("expected stale pending domain to refresh to verified, got %+v", domain)
	}
	if domain.VerifiedAt == nil || domain.LastMessage != "" || domain.DNSLastMessage != "" {
		t.Fatalf("expected verification metadata to be refreshed, got %+v", domain)
	}
	persisted, err := s.GetAppDomain("www.example.com")
	if err != nil {
		t.Fatalf("get refreshed domain: %v", err)
	}
	if persisted.Status != model.AppDomainStatusVerified || persisted.DNSStatus != model.AppDomainDNSStatusReady {
		t.Fatalf("expected refreshed domain to persist, got %+v", persisted)
	}
}

func TestGetAppDomainDiagnosisRefreshesStalePendingDomain(t *testing.T) {
	t.Parallel()

	s, server, apiKey, _, app, resolver := setupAppDomainTestServer(t)
	expectedTarget := server.primaryCustomDomainTarget(app)
	stale := time.Now().UTC().Add(-2 * appDomainReadVerifyMinInterval)
	if _, err := s.PutAppDomain(model.AppDomain{
		Hostname:         "www.example.com",
		AppID:            app.ID,
		TenantID:         app.TenantID,
		Status:           model.AppDomainStatusPending,
		DNSStatus:        model.AppDomainDNSStatusPending,
		RouteTarget:      expectedTarget,
		LastMessage:      "CNAME for www.example.com points to www.example.com; expected " + expectedTarget,
		DNSLastMessage:   "CNAME for www.example.com points to www.example.com; expected " + expectedTarget,
		LastCheckedAt:    &stale,
		DNSLastCheckedAt: &stale,
		CreatedAt:        stale,
		UpdatedAt:        stale,
	}); err != nil {
		t.Fatalf("put stale pending domain: %v", err)
	}
	resolver.cname["www.example.com"] = expectedTarget + "."

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/domains/diagnosis?hostname=www.example.com", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Diagnosis appDomainDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &response)
	domain := response.Diagnosis.Domain
	if domain.Status != model.AppDomainStatusVerified || domain.DNSStatus != model.AppDomainDNSStatusReady || domain.DNSRecordKind != model.AppDomainDNSRecordKindCNAME {
		t.Fatalf("expected stale pending domain diagnosis to refresh to verified, got %+v", response.Diagnosis)
	}
	if response.Diagnosis.DNSObservation.Verified != true {
		t.Fatalf("expected refreshed diagnosis DNS observation to verify, got %+v", response.Diagnosis)
	}
	for _, check := range response.Diagnosis.Checks {
		if check.Name == "domain_verified" && check.Status != "pass" {
			t.Fatalf("expected domain_verified check to pass after read refresh, got %+v", response.Diagnosis.Checks)
		}
	}
	persisted, err := s.GetAppDomain("www.example.com")
	if err != nil {
		t.Fatalf("get refreshed domain: %v", err)
	}
	if persisted.Status != model.AppDomainStatusVerified || persisted.DNSStatus != model.AppDomainDNSStatusReady {
		t.Fatalf("expected refreshed diagnosis domain to persist, got %+v", persisted)
	}
}

func TestPutAppDomainVerifiesWithFlattenedTargetIPs(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, app, resolver := setupAppDomainTestServer(t)
	expectedTarget := server.primaryCustomDomainTarget(app)
	edgeIP := net.ParseIP("203.0.113.10")
	resolver.ip["example.com"] = []net.IPAddr{{IP: edgeIP}}
	resolver.ip[expectedTarget] = []net.IPAddr{{IP: edgeIP}}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", apiKey, map[string]any{
		"hostname": "example.com",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var putResponse struct {
		Domain model.AppDomain `json:"domain"`
	}
	mustDecodeJSON(t, recorder, &putResponse)
	if got := putResponse.Domain.RouteTarget; got != expectedTarget {
		t.Fatalf("expected route target %q, got %q", expectedTarget, got)
	}
	if putResponse.Domain.Status != model.AppDomainStatusVerified {
		t.Fatalf("expected verified domain status, got %+v", putResponse.Domain)
	}
	if putResponse.Domain.TLSStatus != model.AppDomainTLSStatusPending {
		t.Fatalf("expected pending TLS status after verification, got %+v", putResponse.Domain)
	}
}

func TestPutAppDomainCreatesPendingClaimBeforeDNSIsConfigured(t *testing.T) {
	t.Parallel()

	s, server, apiKey, _, app, _ := setupAppDomainTestServer(t)
	expectedTarget := server.primaryCustomDomainTarget(app)

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", apiKey, map[string]any{
		"hostname": "www.example.com",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var putResponse struct {
		Domain       model.AppDomain       `json:"domain"`
		Availability appDomainAvailability `json:"availability"`
	}
	mustDecodeJSON(t, recorder, &putResponse)
	if putResponse.Availability.Current {
		t.Fatalf("expected pending hostname to stay non-current, got %+v", putResponse.Availability)
	}
	if putResponse.Domain.Status != model.AppDomainStatusPending {
		t.Fatalf("expected pending domain status, got %+v", putResponse.Domain)
	}
	if putResponse.Domain.RouteTarget != expectedTarget {
		t.Fatalf("expected route target %q, got %+v", expectedTarget, putResponse.Domain)
	}
	if !strings.Contains(putResponse.Domain.LastMessage, "CNAME") || !strings.Contains(putResponse.Domain.LastMessage, expectedTarget) {
		t.Fatalf("expected CNAME guidance in pending domain message, got %+v", putResponse.Domain)
	}

	found, err := s.GetAppDomain("www.example.com")
	if err != nil {
		t.Fatalf("expected pending hostname to be claimed, got %v", err)
	}
	if found.Status != model.AppDomainStatusPending {
		t.Fatalf("expected stored domain to stay pending, got %+v", found)
	}
}

func TestCustomDomainTargetStaysStableWhenAppRouteChanges(t *testing.T) {
	t.Parallel()

	s, server, apiKey, _, app, resolver := setupAppDomainTestServer(t)
	expectedTarget := server.primaryCustomDomainTarget(app)
	updatedApp, err := s.UpdateAppRoute(app.ID, model.AppRoute{
		Hostname:    "renamed.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://renamed.apps.example.com",
		ServicePort: 8080,
	})
	if err != nil {
		t.Fatalf("update app route: %v", err)
	}
	if got := server.primaryCustomDomainTarget(updatedApp); got != expectedTarget {
		t.Fatalf("expected stable target %q after route change, got %q", expectedTarget, got)
	}
	resolver.cname["www.example.com"] = expectedTarget + "."

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+updatedApp.ID+"/domains", apiKey, map[string]any{
		"hostname": "www.example.com",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var putResponse struct {
		Domain model.AppDomain `json:"domain"`
	}
	mustDecodeJSON(t, recorder, &putResponse)
	if got := putResponse.Domain.RouteTarget; got != expectedTarget {
		t.Fatalf("expected stable route target %q, got %q", expectedTarget, got)
	}
}

func TestEdgeTLSAskAutoVerifiesPendingDomain(t *testing.T) {
	t.Parallel()

	s, server, _, _, app, resolver := setupAppDomainTestServer(t)
	if _, err := s.PutAppDomain(model.AppDomain{
		Hostname:    "www.example.com",
		AppID:       app.ID,
		TenantID:    app.TenantID,
		Status:      model.AppDomainStatusPending,
		RouteTarget: "demo.apps.example.com",
	}); err != nil {
		t.Fatalf("put pending app domain: %v", err)
	}
	resolver.cname["www.example.com"] = "demo.apps.example.com."

	req := httptest.NewRequest(http.MethodGet, "/v1/edge/tls/ask?token=edge-secret&domain=www.example.com", nil)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	domain, err := s.GetAppDomain("www.example.com")
	if err != nil {
		t.Fatalf("get app domain after ask: %v", err)
	}
	if domain.Status != model.AppDomainStatusVerified {
		t.Fatalf("expected app domain to be auto-verified, got %+v", domain)
	}
}

func TestPutAppDomainAllowsPlatformAdminToClaimPlatformRoot(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, app, resolver := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	expectedTarget := server.primaryCustomDomainTarget(app)
	resolver.cname["fugue.pro"] = expectedTarget + "."

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", platformAdminKey, map[string]any{
		"hostname": "fugue.pro",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var putResponse struct {
		Domain model.AppDomain `json:"domain"`
	}
	mustDecodeJSON(t, recorder, &putResponse)
	if got := putResponse.Domain.Hostname; got != "fugue.pro" {
		t.Fatalf("expected hostname %q, got %q", "fugue.pro", got)
	}
	if got := putResponse.Domain.RouteTarget; got != expectedTarget {
		t.Fatalf("expected route target %q, got %q", expectedTarget, got)
	}
	if putResponse.Domain.Status != model.AppDomainStatusVerified {
		t.Fatalf("expected verified domain status, got %+v", putResponse.Domain)
	}
}

func TestPutAppDomainRejectsPlatformRootForTenantAdmin(t *testing.T) {
	t.Parallel()

	s, server, apiKey, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", apiKey, map[string]any{
		"hostname": "fugue.pro",
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "platform-managed hostnames") {
		t.Fatalf("expected platform-managed hostname error, got body=%s", recorder.Body.String())
	}
	if _, err := s.GetAppDomain("fugue.pro"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected platform root to remain unclaimed, got %v", err)
	}
}

func TestGetAppDomainAvailabilityAllowsOnlyPlatformAdminForPlatformRoot(t *testing.T) {
	t.Parallel()

	_, server, apiKey, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")

	tenantRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/domains/availability?hostname=fugue.pro", apiKey, nil)
	if tenantRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, tenantRecorder.Code, tenantRecorder.Body.String())
	}
	var tenantResponse struct {
		Availability appDomainAvailability `json:"availability"`
	}
	mustDecodeJSON(t, tenantRecorder, &tenantResponse)
	if tenantResponse.Availability.Valid {
		t.Fatalf("expected tenant availability to be invalid, got %+v", tenantResponse.Availability)
	}
	if !strings.Contains(tenantResponse.Availability.Reason, "platform-managed hostnames") {
		t.Fatalf("expected platform-managed hostname error, got %+v", tenantResponse.Availability)
	}

	adminRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/domains/availability?hostname=fugue.pro", platformAdminKey, nil)
	if adminRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, adminRecorder.Code, adminRecorder.Body.String())
	}
	var adminResponse struct {
		Availability appDomainAvailability `json:"availability"`
	}
	mustDecodeJSON(t, adminRecorder, &adminResponse)
	if !adminResponse.Availability.Valid || !adminResponse.Availability.Available {
		t.Fatalf("expected platform admin availability to be valid and available, got %+v", adminResponse.Availability)
	}
	if adminResponse.Availability.Hostname != "fugue.pro" {
		t.Fatalf("expected hostname %q, got %+v", "fugue.pro", adminResponse.Availability)
	}
}

func TestEdgeDomainsListsOnlyManagedVerifiedCustomDomains(t *testing.T) {
	t.Parallel()

	s, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	now := mustTime(t, "2026-03-31T00:00:00Z")
	domains := []model.AppDomain{
		{
			Hostname:    "www.example.com",
			AppID:       app.ID,
			TenantID:    app.TenantID,
			Status:      model.AppDomainStatusVerified,
			RouteTarget: server.primaryCustomDomainTarget(app),
			CreatedAt:   now,
			UpdatedAt:   now,
		},
		{
			Hostname:    "pending.example.com",
			AppID:       app.ID,
			TenantID:    app.TenantID,
			Status:      model.AppDomainStatusPending,
			RouteTarget: server.primaryCustomDomainTarget(app),
			CreatedAt:   now,
			UpdatedAt:   now,
		},
		{
			Hostname:    "fugue.pro",
			AppID:       app.ID,
			TenantID:    app.TenantID,
			Status:      model.AppDomainStatusVerified,
			RouteTarget: server.primaryCustomDomainTarget(app),
			CreatedAt:   now,
			UpdatedAt:   now,
		},
		{
			Hostname:    "d-abc.dns.fugue.pro",
			AppID:       app.ID,
			TenantID:    app.TenantID,
			Status:      model.AppDomainStatusVerified,
			RouteTarget: server.primaryCustomDomainTarget(app),
			CreatedAt:   now,
			UpdatedAt:   now,
		},
	}
	for _, domain := range domains {
		if _, err := s.PutAppDomain(domain); err != nil {
			t.Fatalf("put app domain %s: %v", domain.Hostname, err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/edge/domains?token=edge-secret", nil)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Domains []struct {
			Hostname string `json:"hostname"`
		} `json:"domains"`
	}
	mustDecodeJSON(t, recorder, &response)
	got := make([]string, 0, len(response.Domains))
	for _, domain := range response.Domains {
		got = append(got, domain.Hostname)
	}
	sort.Strings(got)
	want := []string{"fugue.pro", "www.example.com"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected managed custom domains %v, got %v", want, got)
	}
}

func TestEdgeDomainTLSReportUpdatesVerifiedDomainStatus(t *testing.T) {
	t.Parallel()

	s, server, apiKey, _, app, resolver := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	expectedTarget := server.primaryCustomDomainTarget(app)
	resolver.cname["www.example.com"] = expectedTarget + "."

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", apiKey, map[string]any{
		"hostname": "www.example.com",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	errorReport := performJSONRequest(t, server, http.MethodPost, "/v1/edge/domains/tls-report?token=edge-secret", "", map[string]any{
		"hostname":         "www.example.com",
		"tls_status":       model.AppDomainTLSStatusError,
		"tls_last_message": "certificate issuance failed",
	})
	if errorReport.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, errorReport.Code, errorReport.Body.String())
	}

	domain, err := s.GetAppDomain("www.example.com")
	if err != nil {
		t.Fatalf("get app domain after error report: %v", err)
	}
	if domain.TLSStatus != model.AppDomainTLSStatusError {
		t.Fatalf("expected error TLS status, got %+v", domain)
	}
	if domain.TLSLastMessage != "certificate issuance failed" {
		t.Fatalf("expected TLS error message to be stored, got %+v", domain)
	}
	if domain.TLSLastCheckedAt == nil {
		t.Fatalf("expected TLS last checked timestamp to be set, got %+v", domain)
	}
	if domain.TLSReadyAt != nil {
		t.Fatalf("expected TLS ready timestamp to stay empty on error, got %+v", domain)
	}

	certPEM, keyPEM, metadataJSON := generateTestTLSCertificateBundle(t, "www.example.com")
	readyReport := performJSONRequest(t, server, http.MethodPost, "/v1/edge/domains/tls-report?token=edge-secret", "", map[string]any{
		"hostname":         "www.example.com",
		"tls_status":       model.AppDomainTLSStatusReady,
		"tls_last_message": "ignored",
	})
	if readyReport.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, readyReport.Code, readyReport.Body.String())
	}

	domain, err = s.GetAppDomain("www.example.com")
	if err != nil {
		t.Fatalf("get app domain after ready report: %v", err)
	}
	if domain.TLSStatus != model.AppDomainTLSStatusPending {
		t.Fatalf("expected pending TLS status without a shared certificate, got %+v", domain)
	}
	if domain.TLSLastMessage != "ignored" {
		t.Fatalf("expected pending report to keep the TLS last message, got %+v", domain)
	}
	if domain.TLSLastCheckedAt == nil || domain.TLSReadyAt != nil {
		t.Fatalf("expected pending report timestamps to reflect the missing certificate, got %+v", domain)
	}

	readyWithCert := performJSONRequest(t, server, http.MethodPost, "/v1/edge/domains/tls-report?token=edge-secret", "", map[string]any{
		"hostname":        "www.example.com",
		"tls_status":      model.AppDomainTLSStatusReady,
		"certificate_pem": certPEM,
		"private_key_pem": keyPEM,
		"metadata_json":   metadataJSON,
		"issuer_storage":  "acme-v02.api.letsencrypt.org-directory",
	})
	if readyWithCert.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, readyWithCert.Code, readyWithCert.Body.String())
	}

	domain, err = s.GetAppDomain("www.example.com")
	if err != nil {
		t.Fatalf("get app domain after ready report with cert: %v", err)
	}
	if domain.TLSStatus != model.AppDomainTLSStatusReady {
		t.Fatalf("expected ready TLS status after shared certificate upload, got %+v", domain)
	}
	if domain.TLSLastMessage != "" {
		t.Fatalf("expected ready report to clear TLS last message, got %+v", domain)
	}
	if domain.TLSLastCheckedAt == nil || domain.TLSReadyAt == nil {
		t.Fatalf("expected ready report timestamps to be set, got %+v", domain)
	}

	pendingAfterCert := performJSONRequest(t, server, http.MethodPost, "/v1/edge/domains/tls-report?token=edge-secret", "", map[string]any{
		"hostname":         "www.example.com",
		"tls_status":       model.AppDomainTLSStatusPending,
		"tls_last_message": "warmup retry still pending",
	})
	if pendingAfterCert.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, pendingAfterCert.Code, pendingAfterCert.Body.String())
	}

	domain, err = s.GetAppDomain("www.example.com")
	if err != nil {
		t.Fatalf("get app domain after pending report with shared cert: %v", err)
	}
	if domain.TLSStatus != model.AppDomainTLSStatusReady {
		t.Fatalf("expected shared certificate to prevent TLS downgrade, got %+v", domain)
	}
	if domain.TLSLastMessage != "" || domain.TLSReadyAt == nil {
		t.Fatalf("expected pending report with shared cert to preserve ready state, got %+v", domain)
	}
}

func TestEdgeDomainTLSReportAllowsSubpathRouteOnSameHostname(t *testing.T) {
	t.Parallel()

	s, server, apiKey, _, app, resolver := setupAppDomainTestServer(t)
	expectedTarget := server.primaryCustomDomainTarget(app)
	resolver.cname["www.example.com"] = expectedTarget + "."

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", apiKey, map[string]any{
		"hostname": "www.example.com",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	if _, err := s.CreateAppWithRoute(app.TenantID, app.ProjectID, "api", "", model.AppSpec{
		Image:     "ghcr.io/example/api:latest",
		Ports:     []int{8000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppRoute{
		Hostname:    "www.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://www.example.com/v1",
		PathPrefix:  "/v1",
		ServicePort: 8000,
	}); err != nil {
		t.Fatalf("create subpath app route on custom domain hostname: %v", err)
	}

	report := performJSONRequest(t, server, http.MethodPost, "/v1/edge/domains/tls-report?token=edge-secret", "", map[string]any{
		"hostname":         "www.example.com",
		"tls_status":       model.AppDomainTLSStatusPending,
		"tls_last_message": "warmup check",
	})
	if report.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, report.Code, report.Body.String())
	}
	domain, err := s.GetAppDomain("www.example.com")
	if err != nil {
		t.Fatalf("get app domain after TLS report: %v", err)
	}
	if domain.TLSStatus != model.AppDomainTLSStatusPending || domain.TLSLastMessage != "warmup check" {
		t.Fatalf("expected TLS report to update the custom domain, got %+v", domain)
	}
}

func TestEdgeDomainTLSReportAcceptsPlatformRootCustomDomain(t *testing.T) {
	t.Parallel()

	s, server, _, platformAdminKey, app, resolver := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	expectedTarget := server.primaryCustomDomainTarget(app)
	resolver.cname["fugue.pro"] = expectedTarget + "."

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", platformAdminKey, map[string]any{
		"hostname": "fugue.pro",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	certPEM, keyPEM, metadataJSON := generateTestTLSCertificateBundle(t, "fugue.pro")
	report := performJSONRequest(t, server, http.MethodPost, "/v1/edge/domains/tls-report?token=edge-secret", "", map[string]any{
		"hostname":        "fugue.pro",
		"tls_status":      model.AppDomainTLSStatusReady,
		"certificate_pem": certPEM,
		"private_key_pem": keyPEM,
		"metadata_json":   metadataJSON,
		"issuer_storage":  "acme-v02.api.letsencrypt.org-directory",
	})
	if report.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, report.Code, report.Body.String())
	}

	domain, err := s.GetAppDomain("fugue.pro")
	if err != nil {
		t.Fatalf("get app domain after ready report: %v", err)
	}
	if domain.TLSStatus != model.AppDomainTLSStatusReady {
		t.Fatalf("expected ready TLS status, got %+v", domain)
	}
	if domain.TLSReadyAt == nil || domain.TLSLastCheckedAt == nil {
		t.Fatalf("expected ready timestamps to be set, got %+v", domain)
	}
}

func TestAppDomainDiagnosisAndRepairUseSharedTLSCertificate(t *testing.T) {
	t.Parallel()

	s, server, apiKey, _, app, resolver := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	expectedTarget := server.primaryCustomDomainTarget(app)
	resolver.cname["www.example.com"] = expectedTarget + "."

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", apiKey, map[string]any{
		"hostname": "www.example.com",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	diagnosisResponse := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/domains/diagnosis?hostname=www.example.com", apiKey, nil)
	if diagnosisResponse.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, diagnosisResponse.Code, diagnosisResponse.Body.String())
	}
	var diagnosisEnvelope struct {
		Diagnosis appDomainDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, diagnosisResponse, &diagnosisEnvelope)
	if diagnosisEnvelope.Diagnosis.DNSObservation.Verified != true {
		t.Fatalf("expected DNS to verify, got %+v", diagnosisEnvelope.Diagnosis)
	}
	if diagnosisEnvelope.Diagnosis.SharedTLSCertificate.Present {
		t.Fatalf("expected no shared certificate before upload, got %+v", diagnosisEnvelope.Diagnosis)
	}

	certPEM, keyPEM, metadataJSON := generateTestTLSCertificateBundle(t, "www.example.com")
	putBundle := performJSONRequest(t, server, http.MethodPut, "/v1/edge/domains/www.example.com/tls-bundle?token=edge-secret", "", map[string]any{
		"certificate_pem": certPEM,
		"private_key_pem": keyPEM,
		"metadata_json":   metadataJSON,
		"issuer_storage":  "acme-v02.api.letsencrypt.org-directory",
	})
	if putBundle.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, putBundle.Code, putBundle.Body.String())
	}
	var putBundleEnvelope struct {
		Certificate model.EdgeTLSCertificate `json:"certificate"`
		Domain      model.AppDomain          `json:"domain"`
	}
	mustDecodeJSON(t, putBundle, &putBundleEnvelope)
	if putBundleEnvelope.Domain.TLSStatus != model.AppDomainTLSStatusReady {
		t.Fatalf("expected shared bundle upload to mark TLS ready, got %+v", putBundleEnvelope.Domain)
	}

	getBundle := performJSONRequest(t, server, http.MethodGet, "/v1/edge/domains/www.example.com/tls-bundle?token=edge-secret", "", nil)
	if getBundle.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, getBundle.Code, getBundle.Body.String())
	}
	var getBundleEnvelope struct {
		Certificate model.EdgeTLSCertificate `json:"certificate"`
	}
	mustDecodeJSON(t, getBundle, &getBundleEnvelope)
	if getBundleEnvelope.Certificate.CertificateSHA256 == "" {
		t.Fatalf("expected stored certificate sha, got %+v", getBundleEnvelope.Certificate)
	}

	domain, err := s.GetAppDomain("www.example.com")
	if err != nil {
		t.Fatalf("get app domain after bundle upload: %v", err)
	}
	domain.TLSStatus = model.AppDomainTLSStatusPending
	domain.TLSReadyAt = nil
	domain.TLSLastCheckedAt = nil
	domain.TLSLastMessage = "stale"
	if _, err := s.PutAppDomain(domain); err != nil {
		t.Fatalf("make domain pending for repair: %v", err)
	}

	repairResponse := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains/repair", apiKey, map[string]any{
		"hostname": "www.example.com",
	})
	if repairResponse.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, repairResponse.Code, repairResponse.Body.String())
	}
	var repairEnvelope struct {
		Domain    model.AppDomain    `json:"domain"`
		Diagnosis appDomainDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, repairResponse, &repairEnvelope)
	if repairEnvelope.Domain.TLSStatus != model.AppDomainTLSStatusReady {
		t.Fatalf("expected repair to restore TLS ready, got %+v", repairEnvelope.Domain)
	}
	if !repairEnvelope.Diagnosis.SharedTLSCertificate.Present {
		t.Fatalf("expected repair diagnosis to report shared certificate, got %+v", repairEnvelope.Diagnosis)
	}
}

func TestPrimaryCustomDomainTargetDefaultsToDNSNamespace(t *testing.T) {
	t.Parallel()

	_, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	expectedLabel := stableCustomDomainTargetLabel(app)
	expectedTarget := expectedLabel + ".dns.fugue.pro"
	if got := server.primaryCustomDomainTarget(app); got != expectedTarget {
		t.Fatalf("expected primary custom-domain target %q, got %q", expectedTarget, got)
	}
}

func generateTestTLSCertificateBundle(t *testing.T, hostname string) (string, string, string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 62)
	serialNumber, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		t.Fatalf("generate serial: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: hostname,
		},
		DNSNames:              []string{hostname},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	metadata := `{"issuer_storage":"acme-v02.api.letsencrypt.org-directory"}`
	return strings.TrimSpace(string(certPEM)), strings.TrimSpace(string(keyPEM)), metadata
}

func setupAppDomainTestServer(t *testing.T) (*store.Store, *Server, string, string, model.App, *fakeAppDomainResolver) {
	t.Helper()
	return setupAppDomainTestServerWithDomains(t, "apps.example.com")
}

func setupAppDomainTestServerWithDomains(t *testing.T, appBaseDomain string) (*store.Store, *Server, string, string, model.App, *fakeAppDomainResolver) {
	t.Helper()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("App Domain Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	_, platformAdminKey, err := s.CreateAPIKey(tenant.ID, "platform-admin", []string{"platform.admin"})
	if err != nil {
		t.Fatalf("create platform admin key: %v", err)
	}
	routeHostname := "demo." + appBaseDomain
	app, err := s.CreateAppWithRoute(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppRoute{
		Hostname:    routeHostname,
		BaseDomain:  appBaseDomain,
		PublicURL:   "https://" + routeHostname,
		ServicePort: 8080,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain:          appBaseDomain,
		APIPublicDomain:        "api.example.com",
		EdgeQualityRankingMode: "active",
		EdgeTLSAskToken:        "edge-secret",
		AllowLegacyEdgeToken:   true,
	})
	resolver := &fakeAppDomainResolver{
		cname: map[string]string{},
		ip:    map[string][]net.IPAddr{},
	}
	server.dnsResolver = resolver
	return s, server, apiKey, platformAdminKey, app, resolver
}

type fakeAppDomainResolver struct {
	cname map[string]string
	ip    map[string][]net.IPAddr
}

func mustTime(t *testing.T, raw string) time.Time {
	t.Helper()
	value, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("parse time %q: %v", raw, err)
	}
	return value
}

func (f *fakeAppDomainResolver) LookupCNAME(_ context.Context, host string) (string, error) {
	if value, ok := f.cname[normalizeExternalAppDomain(host)]; ok {
		return value, nil
	}
	return "", &net.DNSError{IsNotFound: true}
}

func (f *fakeAppDomainResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	if values, ok := f.ip[normalizeExternalAppDomain(host)]; ok {
		return values, nil
	}
	return nil, &net.DNSError{IsNotFound: true}
}
