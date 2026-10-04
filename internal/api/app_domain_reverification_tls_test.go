package api

import (
	"net/http"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDomainReverificationRestoresOnlyUsableOwnedSharedTLS(t *testing.T) {
	for _, mode := range []string{"valid", "expired", "future", "wrong hostname", "wrong owner", "missing", "dns unverified"} {
		t.Run(mode, func(t *testing.T) {
			state, server, key, _, app, resolver := setupAppDomainTestServerWithDomains(t, "example.test")
			host := "reverified.customer.test"
			resolver.cname[host] = server.primaryCustomDomainTarget(app) + "."
			created := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", key, map[string]any{"hostname": host})
			if created.Code != http.StatusOK {
				t.Fatal(created.Body.String())
			}
			domain, err := state.GetAppDomain(host)
			if err != nil {
				t.Fatal(err)
			}
			// A domain may temporarily lose ownership verification while its
			// positive, owner-bound certificate remains available for recovery.
			domain.Status = model.AppDomainStatusPending
			domain.DNSStatus = model.AppDomainDNSStatusPending
			if _, err := state.PutAppDomain(domain); err != nil {
				t.Fatal(err)
			}
			if mode != "missing" {
				now := time.Now().UTC()
				from, to := now.Add(-time.Hour), now.Add(time.Hour)
				certHost := host
				switch mode {
				case "expired":
					from, to = now.Add(-48*time.Hour), now.Add(-time.Hour)
				case "future":
					from, to = now.Add(time.Hour), now.Add(48*time.Hour)
				case "wrong hostname":
					certHost = "another.customer.test"
				}
				cert, private, metadata := generateTestTLSCertificateBundleAt(t, certHost, from, to)
				stored := model.EdgeTLSCertificate{Hostname: host, AppID: app.ID, TenantID: app.TenantID,
					CertificatePEM: cert, PrivateKeyPEM: private, MetadataJSON: metadata, IssuerStorage: "issuer", NotAfter: &to}
				if mode == "wrong owner" {
					stored.TenantID = "foreign"
				}
				if _, err := state.PutEdgeTLSCertificate(stored); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "dns unverified" {
				delete(resolver.cname, host)
			}
			verified := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains/verify", key, map[string]any{"hostname": host})
			if verified.Code != http.StatusOK {
				t.Fatal(verified.Body.String())
			}
			after, err := state.GetAppDomain(host)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "valid" {
				if after.Status != model.AppDomainStatusVerified || after.TLSStatus != model.AppDomainTLSStatusReady || after.TLSReadyAt == nil {
					t.Fatalf("reusable certificate still requires a legacy TLS report: %+v", after)
				}
				readyAt := *after.TLSReadyAt
				again := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains/verify", key, map[string]any{"hostname": host})
				after, _ = state.GetAppDomain(host)
				if again.Code != http.StatusOK || !after.TLSReadyAt.Equal(readyAt) {
					t.Fatal("idempotent verification rewrote the readiness timestamp")
				}
			} else if after.TLSStatus == model.AppDomainTLSStatusReady || after.TLSReadyAt != nil {
				t.Fatalf("unverified ownership or unusable certificate promoted readiness: %+v", after)
			}
		})
	}
}
