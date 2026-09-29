package routeartifact

import (
	"errors"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

type CertificateOwner struct {
	Hostname string
	AppID    string
	TenantID string
}

// CustomDomainCertificateOwner joins a signed hostname reference to its local
// custom-domain routes and verified allowlist. A platform path can share the
// same SNI hostname, but cannot supply or replace the application's owner.
func CustomDomainCertificateOwner(refs []platformconfig.TLSIntent, bundle model.EdgeRouteBundle, host string) (CertificateOwner, error) {
	fail := errors.New("certificate hostname or custom-domain ownership differs from signed local routes")
	var ref platformconfig.TLSIntent
	count := 0
	for _, item := range refs {
		if item.Hostname == host {
			ref, count = item, count+1
		}
	}
	if count != 1 || ref.Policy != model.EdgeRouteTLSPolicyCustomDomain && ref.Policy != model.EdgeRouteTLSPolicyPlatform {
		return CertificateOwner{}, fail
	}
	owner := CertificateOwner{Hostname: host}
	matchedPolicy := false
	for _, route := range bundle.Routes {
		if route.Hostname != host {
			continue
		}
		matchedPolicy = matchedPolicy || route.TLSPolicy == ref.Policy
		if route.TLSPolicy != model.EdgeRouteTLSPolicyCustomDomain {
			continue
		}
		if route.AppID == "" || route.TenantID == "" || !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) || owner.AppID != "" && (route.AppID != owner.AppID || route.TenantID != owner.TenantID) {
			return CertificateOwner{}, fail
		}
		owner.AppID, owner.TenantID = route.AppID, route.TenantID
	}
	if owner.AppID == "" || !matchedPolicy || ref.Policy == model.EdgeRouteTLSPolicyCustomDomain && (ref.AppID != owner.AppID || ref.TenantID != owner.TenantID) || ref.Policy == model.EdgeRouteTLSPolicyPlatform && (ref.AppID != "" && ref.AppID != owner.AppID || ref.TenantID != "" && ref.TenantID != owner.TenantID) {
		return CertificateOwner{}, fail
	}
	count = 0
	for _, domain := range bundle.TLSAllowlist {
		if domain.Hostname != host {
			continue
		}
		if domain.Status != model.AppDomainStatusVerified || domain.AppID != owner.AppID || domain.TenantID != owner.TenantID {
			return CertificateOwner{}, fail
		}
		count++
	}
	if count != 1 {
		return CertificateOwner{}, fail
	}
	return owner, nil
}
