package api

import (
	"fugue/internal/platformconfig"
	"sort"
)

type applicationDomainConfig platformconfig.ApplicationDomainsIntent

func (s *Server) legacyApplicationDomains() applicationDomainConfig {
	reserved := make([]string, 0, len(s.reservedAppHosts))
	for host := range s.reservedAppHosts {
		reserved = append(reserved, host)
	}
	sort.Strings(reserved)
	return applicationDomainConfig{AppBaseDomain: normalizeExternalAppDomain(s.appBaseDomain), CustomDomainBaseDomain: normalizeExternalAppDomain(s.customDomainBaseDomain), ReservedHostnames: reserved, DefaultDNSTTL: edgeDNSPolicyTTL(s.dnsBundleTTL)}
}

func (s *Server) applicationDomainsForProjection(in *platformconfig.ApplicationDomainsIntent) (applicationDomainConfig, error) {
	if in == nil {
		return s.legacyApplicationDomains(), nil
	}
	if err := platformconfig.ValidateApplicationDomains(in); err != nil {
		return applicationDomainConfig{}, err
	}
	return applicationDomainConfig(*platformconfig.CloneApplicationDomains(in)), nil
}

// The one-time importer records effective namespace settings, including derived
// reserved names. It does not activate a policy or alter a serving pointer.
func (s *Server) importPlatformEnvironment(env map[string]string, generation string) (platformconfig.EnvironmentImportResult, error) {
	result, err := platformconfig.ImportEnvironment(env, generation)
	if err != nil {
		return result, err
	}
	domains := platformconfig.ApplicationDomainsIntent(s.legacyApplicationDomains())
	if err := platformconfig.ValidateApplicationDomains(&domains); err != nil {
		return result, err
	}
	result.Intent.ApplicationDomains = &domains
	result.SourceDigest, err = platformconfig.Digest(struct {
		Environment string                                  `json:"environment_digest"`
		Domains     platformconfig.ApplicationDomainsIntent `json:"effective_application_domains"`
	}{result.SourceDigest, domains})
	result.ImportedKeys = append(result.ImportedKeys, "effective_application_domains")
	return result, err
}
