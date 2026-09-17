package api

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDomainCertificateValidityControlsDiagnosisRepairAndReports(t *testing.T) {
	for _, mode := range []string{"expired", "future", "wrong hostname", "wrong owner"} {
		t.Run(mode, func(t *testing.T) {
			state, server, key, _, app, resolver := setupAppDomainTestServerWithDomains(t, "example.test")
			host := "certificate.customer.test"
			resolver.cname[host] = server.primaryCustomDomainTarget(app) + "."
			created := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", key, map[string]any{"hostname": host})
			if created.Code != 200 {
				t.Fatal(created.Body.String())
			}
			now := time.Now().UTC()
			from, to := now.Add(-time.Hour), now.Add(time.Hour)
			certHost := host
			switch mode {
			case "expired":
				from, to = now.Add(-48*time.Hour), now.Add(-time.Hour)
			case "future":
				from, to = now.Add(time.Hour), now.Add(48*time.Hour)
			case "wrong hostname":
				certHost = "other.customer.test"
			}
			cert, private, metadata := generateTestTLSCertificateBundleAt(t, certHost, from, to)
			domain, err := state.GetAppDomain(host)
			if err != nil {
				t.Fatal(err)
			}
			original := now.Add(-time.Hour)
			domain.TLSStatus = "ready"
			domain.TLSReadyAt = &original
			domain.TLSLastCheckedAt = &original
			if _, err = state.PutAppDomain(domain); err != nil {
				t.Fatal(err)
			}
			// Forged future metadata cannot make an expired actual leaf usable.
			metadataExpiry := now.Add(24 * time.Hour)
			stored := model.EdgeTLSCertificate{Hostname: host, AppID: domain.AppID, TenantID: domain.TenantID, CertificatePEM: cert, PrivateKeyPEM: private, MetadataJSON: metadata, IssuerStorage: "issuer", NotAfter: &metadataExpiry}
			if mode == "wrong owner" {
				stored.TenantID = "foreign"
			}
			if _, err := state.PutEdgeTLSCertificate(stored); err != nil {
				t.Fatal(err)
			}
			diag := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/domains/diagnosis?hostname="+host, key, nil)
			if diag.Code != 200 {
				t.Fatal(diag.Body.String())
			}
			var body struct {
				Diagnosis appDomainDiagnosis `json:"diagnosis"`
			}
			mustDecodeJSON(t, diag, &body)
			if !body.Diagnosis.SharedTLSCertificate.Present {
				t.Fatal("diagnosis lost historical certificate presence")
			}
			for _, check := range body.Diagnosis.Checks {
				if check.Name == "shared_tls_certificate" || check.Name == "tls_ready" || check.Name == "route_active" {
					if check.Status == "pass" {
						t.Fatal("unusable certificate passed", mode, check)
					}
				}
			}
			after, _ := state.GetAppDomain(host)
			if after.TLSStatus != "ready" || !after.TLSReadyAt.Equal(original) {
				t.Fatal("read diagnosis rewrote historical readiness")
			}
			repair := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains/repair", key, map[string]any{"hostname": host})
			if repair.Code != 200 {
				t.Fatal(repair.Body.String())
			}
			after, _ = state.GetAppDomain(host)
			if after.TLSStatus == "ready" || after.TLSReadyAt != nil {
				t.Fatal("repair promoted unusable material")
			}
			report := performJSONRequest(t, server, http.MethodPost, "/v1/edge/domains/tls-report?token=edge-secret", "", map[string]any{"hostname": host, "tls_status": "ready"})
			if report.Code != 200 {
				t.Fatal(report.Body.String())
			}
			after, _ = state.GetAppDomain(host)
			if after.TLSStatus == "ready" || after.TLSReadyAt != nil {
				t.Fatal("report promoted unusable material")
			}
			if mode != "wrong owner" {
				upload := performJSONRequest(t, server, http.MethodPut, "/v1/edge/domains/"+host+"/tls-bundle?token=edge-secret", "", map[string]any{"certificate_pem": cert, "private_key_pem": private, "issuer_storage": "issuer"})
				if upload.Code != 400 || strings.Contains(upload.Body.String(), "BEGIN") {
					t.Fatal("invalid certificate upload accepted or exposed material")
				}
			}
			validCert, validKey, validMetadata := generateTestTLSCertificateBundle(t, host)
			upload := performJSONRequest(t, server, http.MethodPut, "/v1/edge/domains/"+host+"/tls-bundle?token=edge-secret", "", map[string]any{"certificate_pem": validCert, "private_key_pem": validKey, "metadata_json": validMetadata, "issuer_storage": "issuer"})
			if upload.Code != 200 {
				t.Fatal(upload.Body.String())
			}
			after, _ = state.GetAppDomain(host)
			if after.TLSStatus != "ready" || after.TLSReadyAt == nil {
				t.Fatal("valid renewed certificate not accepted")
			}
		})
	}
}
