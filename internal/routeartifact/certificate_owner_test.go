package routeartifact

import (
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestCustomDomainCertificateOwnerRequiresSignedReferenceAndLocalOwner(t *testing.T) {
	for _, scenario := range []string{"custom", "shared platform reference", "foreign hostname", "duplicate reference", "missing reference", "missing custom route", "missing platform path", "unverified domain", "foreign domain", "duplicate domain", "foreign custom path", "foreign reference", "disabled route"} {
		t.Run(scenario, func(t *testing.T) {
			host := "shared.example.test"
			refs := []platformconfig.TLSIntent{{Hostname: host, Policy: model.EdgeRouteTLSPolicyCustomDomain, AppID: "app-one", TenantID: "tenant-one"}}
			bundle := model.EdgeRouteBundle{Routes: []model.EdgeRouteBinding{{Hostname: host, TLSPolicy: model.EdgeRouteTLSPolicyCustomDomain, AppID: "app-one", TenantID: "tenant-one", RoutePolicy: model.EdgeRoutePolicyEnabled}, {Hostname: host, PathPrefix: "/api", TLSPolicy: model.EdgeRouteTLSPolicyPlatform, RoutePolicy: model.EdgeRoutePolicyEnabled}}, TLSAllowlist: []model.EdgeTLSAllowlistEntry{{Hostname: host, Status: model.AppDomainStatusVerified, AppID: "app-one", TenantID: "tenant-one"}}}
			switch scenario {
			case "shared platform reference", "missing platform path":
				refs[0].Policy, refs[0].AppID, refs[0].TenantID = model.EdgeRouteTLSPolicyPlatform, "", ""
				if scenario == "missing platform path" {
					bundle.Routes = bundle.Routes[:1]
				}
			case "foreign hostname":
				host = "other.example.test"
			case "duplicate reference":
				refs = append(refs, refs[0])
			case "missing reference":
				refs = nil
			case "missing custom route":
				bundle.Routes = bundle.Routes[1:]
			case "unverified domain":
				bundle.TLSAllowlist[0].Status = model.AppDomainStatusPending
			case "foreign domain":
				bundle.TLSAllowlist[0].AppID = "other-app"
			case "duplicate domain":
				bundle.TLSAllowlist = append(bundle.TLSAllowlist, bundle.TLSAllowlist[0])
			case "foreign custom path":
				route := bundle.Routes[0]
				route.PathPrefix, route.TenantID = "/other", "other-tenant"
				bundle.Routes = append(bundle.Routes, route)
			case "foreign reference":
				refs[0].TenantID = "other-tenant"
			case "disabled route":
				bundle.Routes[0].RoutePolicy = model.EdgeRoutePolicyRouteAOnly
			}
			owner, err := CustomDomainCertificateOwner(refs, bundle, host)
			allowed := scenario == "custom" || scenario == "shared platform reference"
			if allowed {
				if err != nil || owner != (CertificateOwner{Hostname: host, AppID: "app-one", TenantID: "tenant-one"}) {
					t.Fatal("signed owner unavailable", err)
				}
			} else if err == nil {
				t.Fatal("invalid certificate owner authorized")
			}
		})
	}
}
