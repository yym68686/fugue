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
// custom-domain routes and verified allowlist. The verified hostname owner
// supplies the certificate; another signed path in its tenant may use it
// without becoming the owner or gaining access to the key.
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
	count = 0
	for _, domain := range bundle.TLSAllowlist {
		if domain.Hostname != host {
			continue
		}
		if domain.Status != model.AppDomainStatusVerified || domain.AppID == "" || domain.TenantID == "" {
			return CertificateOwner{}, fail
		}
		owner.AppID, owner.TenantID = domain.AppID, domain.TenantID
		count++
	}
	if count != 1 || ref.Policy == model.EdgeRouteTLSPolicyCustomDomain && (ref.AppID != owner.AppID || ref.TenantID != owner.TenantID) || ref.Policy == model.EdgeRouteTLSPolicyPlatform && (ref.AppID != "" && ref.AppID != owner.AppID || ref.TenantID != "" && ref.TenantID != owner.TenantID) {
		return CertificateOwner{}, fail
	}
	matchedPolicy := false
	ownerPresent := false
	for _, route := range bundle.Routes {
		if route.Hostname != host {
			continue
		}
		matchedPolicy = matchedPolicy || route.TLSPolicy == ref.Policy
		if route.TLSPolicy != model.EdgeRouteTLSPolicyCustomDomain {
			continue
		}
		if route.AppID == "" || route.TenantID != owner.TenantID || !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) {
			return CertificateOwner{}, fail
		}
		ownerPresent = ownerPresent || route.AppID == owner.AppID
	}
	if !ownerPresent || !matchedPolicy {
		return CertificateOwner{}, fail
	}
	return owner, nil
}
