// Package releasecontract defines side-effect-free release contracts that may
// be consumed by both Planner evidence producers and dormant runtime state.
package releasecontract

import "fmt"

// Domain is a release-time mutation boundary, not a runtime RBAC role.
type Domain string

const (
	DomainNodeLocal        Domain = "node-local"
	DomainAuthoritativeDNS Domain = "authoritative-dns"
	DomainControlPlane     Domain = "control-plane"
	DomainImageCache       Domain = "image-cache"
	DomainBackup           Domain = "backup"
)

var orderedDomains = []Domain{
	DomainNodeLocal,
	DomainAuthoritativeDNS,
	DomainControlPlane,
	DomainImageCache,
	DomainBackup,
}

var domainRank = map[Domain]int{
	DomainNodeLocal:        0,
	DomainAuthoritativeDNS: 1,
	DomainControlPlane:     2,
	DomainImageCache:       3,
	DomainBackup:           4,
}

var fixedAdapters = map[Domain]string{
	DomainNodeLocal:        "control_plane_release_adapter_node_local",
	DomainAuthoritativeDNS: "control_plane_release_adapter_authoritative_dns",
	DomainControlPlane:     "control_plane_release_adapter_control_plane",
	DomainImageCache:       "control_plane_release_adapter_image_cache",
	DomainBackup:           "control_plane_release_adapter_backup",
}

// KnownDomains returns the canonical order used by release contracts.
func KnownDomains() []Domain {
	return append([]Domain(nil), orderedDomains...)
}

// ParseDomain validates a release-domain name.
func ParseDomain(value string) (Domain, error) {
	domain := Domain(value)
	if _, ok := domainRank[domain]; !ok {
		return "", fmt.Errorf("unknown release domain %q", value)
	}
	return domain, nil
}

// DomainRank returns the stable rank used for canonical vectors.
func DomainRank(domain Domain) (int, bool) {
	rank, ok := domainRank[domain]
	return rank, ok
}

// AdapterForDomain returns the fixed production adapter for a domain.
func AdapterForDomain(domain Domain) (string, bool) {
	adapter, ok := fixedAdapters[domain]
	return adapter, ok
}
