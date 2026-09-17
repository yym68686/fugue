package edge

import (
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestStoppedCustomDomainTLSRequiresSameBundleVerifiedOwner(t *testing.T) {
	for name, mutate := range map[string]func(*model.EdgeRouteBundle){
		"verified":        func(*model.EdgeRouteBundle) {},
		"missing":         func(b *model.EdgeRouteBundle) { b.TLSAllowlist = nil },
		"pending":         func(b *model.EdgeRouteBundle) { b.TLSAllowlist[0].Status = "pending" },
		"foreign app":     func(b *model.EdgeRouteBundle) { b.TLSAllowlist[0].AppID = "other" },
		"foreign tenant":  func(b *model.EdgeRouteBundle) { b.TLSAllowlist[0].TenantID = "other" },
		"duplicate":       func(b *model.EdgeRouteBundle) { b.TLSAllowlist = append(b.TLSAllowlist, b.TLSAllowlist[0]) },
		"foreign group":   func(b *model.EdgeRouteBundle) { b.Routes[0].EdgeGroupID = "other" },
		"platform":        func(b *model.EdgeRouteBundle) { b.Routes[0].RouteKind = model.EdgeRouteKindPlatform },
		"policy disabled": func(b *model.EdgeRouteBundle) { b.Routes[0].RoutePolicy = "route_a_only" },
		"origin":          func(b *model.EdgeRouteBundle) { b.Routes[0].UpstreamURL = "http://origin:8080" },
	} {
		t.Run(name, func(t *testing.T) {
			host := "stopped.example.test"
			b := model.EdgeRouteBundle{Version: "bundle", Routes: []model.EdgeRouteBinding{{Hostname: host, AppID: "app", TenantID: "tenant", Status: "disabled", RouteKind: model.EdgeRouteKindCustomDomain, TLSPolicy: model.EdgeRouteTLSPolicyCustomDomain, RoutePolicy: model.EdgeRoutePolicyEnabled, EdgeGroupID: "group-a"}}, TLSAllowlist: []model.EdgeTLSAllowlistEntry{{Hostname: host, AppID: "app", TenantID: "tenant", Status: "verified", TLSStatus: "ready"}}}
			mutate(&b)
			s := NewService(config.EdgeConfig{EdgeGroupID: "group-a"}, log.New(io.Discard, "", 0))
			s.recordSyncSuccess(b, "bundle", time.Now(), false)
			r := httptest.NewRequest(http.MethodGet, "/edge/tls/ask?domain="+host, nil)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			want := http.StatusForbidden
			if name == "verified" {
				want = http.StatusOK
			}
			if w.Code != want {
				t.Fatalf("permission=%d want=%d", w.Code, want)
			}
			if len(s.customDomainTLSHosts(b)) != map[bool]int{true: 1, false: 0}[name == "verified"] {
				t.Fatal("warmup and ask permissions disagree")
			}
			if name == "verified" {
				// Changing the caller's bundle after publication cannot alter the
				// immutable permission view or authorize a different tenant.
				b.TLSAllowlist[0].TenantID = "mutated"
				again := httptest.NewRecorder()
				s.Handler().ServeHTTP(again, r)
				if again.Code != 200 {
					t.Fatal("permission index retained mutable allowlist")
				}
				b.TLSAllowlist = nil
				s.recordSyncSuccess(b, "next", time.Now(), false)
				again = httptest.NewRecorder()
				s.Handler().ServeHTTP(again, r)
				if again.Code != 403 {
					t.Fatal("new bundle inherited old authorization")
				}
			}
		})
	}
}

func TestExpiredSharedTLSCannotOverwriteUsableLocalCertificate(t *testing.T) {
	host := "certificate.example.test"
	now := time.Now().UTC()
	cert, key := testCaddyTLSKeyPair(t, host)
	s := NewService(config.EdgeConfig{CaddyDataDir: t.TempDir(), CaddySharedTLSEnabled: true}, log.New(io.Discard, "", 0))
	good := caddyTLSCertificateBundle{CertificatePEM: cert, PrivateKeyPEM: key, MetadataJSON: `{}`, IssuerStorage: defaultCaddyIssuerStorage}
	if installed, err := s.installSharedCaddyTLSCertificate(host, good); err != nil || !installed {
		t.Fatal(err)
	}
	for _, dates := range [][2]time.Time{{now.Add(-48 * time.Hour), now.Add(-time.Hour)}, {now.Add(time.Hour), now.Add(48 * time.Hour)}} {
		cert, key := testCaddyTLSKeyPairAt(t, host, dates[0], dates[1])
		bad := good
		bad.CertificatePEM, bad.PrivateKeyPEM = cert, key
		if installed, err := s.installSharedCaddyTLSCertificate(host, bad); err == nil || installed {
			t.Fatal("invalid dated certificate installed")
		}
		local, err := s.readLocalCaddyTLSCertificate(host)
		if err != nil || !reflect.DeepEqual(normalizeCaddyTLSCertificateBundle(local), normalizeCaddyTLSCertificateBundle(good)) {
			t.Fatal("positive local certificate changed", err)
		}
	}
}
